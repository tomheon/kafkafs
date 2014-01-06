package kafkafs

import (
	"fmt"
	"log"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

// Implements a FUSE filesystem backed by a Kafka installation.
//
// This version is read only, so it cannot post to topics, only read
// from them.
type KafkaRoFs struct {
	pathfs.FileSystem

	KafkaClient KafkaClient
	userFiles   map[string]bool
	userFilesM  sync.RWMutex
}

type parsedPath struct {
	IsValid   bool
	IsRoot    bool
	Topic     string
	Partition int32
	Offset    int64
}

func (fs *KafkaRoFs) parseAndValidate(name string) (parsedPath, error) {
	parsed := parsedPath{IsValid: true, IsRoot: false, Topic: "", Partition: -1,
		Offset: -1}
	slashed := filepath.ToSlash(name)
	re := regexp.MustCompile("/{2,}")
	normal := re.ReplaceAllString(slashed, "/")
	if normal == "" {
		parsed.IsRoot = true
		return parsed, nil
	}

	splits := strings.Split(normal, "/")

	if len(splits) > 3 {
		parsed.IsValid = false
		return parsed, nil
	}

	if len(splits) >= 1 {
		maybeTopic := splits[0]
		isTop, err := fs.isTopic(maybeTopic)
		if err != nil {
			return parsed, err
		}
		if !isTop {
			parsed.IsValid = false
			return parsed, nil
		}
		parsed.Topic = maybeTopic
	}

	if len(splits) >= 2 {
		maybePartition := splits[1]
		isPart, err := fs.isPartition(parsed.Topic, maybePartition)
		if err != nil {
			return parsed, err
		}
		if !isPart {
			parsed.IsValid = false
			return parsed, nil
		}
		// this should always succeed if isPartition returned true.
		partition64, _ := strconv.ParseInt(maybePartition, 10, 32)
		parsed.Partition = int32(partition64)
	}

	if len(splits) == 3 {
		maybeOffset := splits[2]
		offset, err := strconv.ParseInt(maybeOffset, 10, 64)
		if err != nil {
			parsed.IsValid = false
			return parsed, nil
		}
		parsed.Offset = offset
	}

	return parsed, nil
}

func (fs *KafkaRoFs) isTopic(maybeTopic string) (bool, error) {
	topics, err := fs.KafkaClient.GetTopics()
	if err != nil {
		return false, err
	}

	for _, topic := range topics {
		if topic == maybeTopic {
			return true, nil
		}
	}

	return false, nil
}

func (fs *KafkaRoFs) isPartition(topic string, maybePartition string) (bool, error) {
	maybePartition64, err := strconv.ParseInt(maybePartition, 10, 32)
	if err != nil {
		return false, nil
	}
	maybePartition32 := int32(maybePartition64)
	partitions, err := fs.KafkaClient.GetPartitions(topic)
	if err != nil {
		return false, err
	}

	for _, partition := range partitions {
		if partition == maybePartition32 {
			return true, nil
		}
	}

	return false, nil
}

func (fs *KafkaRoFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr,
	fuse.Status) {
	log.Printf("GetAttr name: %s", name)

	parsed, err := fs.parseAndValidate(name)

	if err != nil {
		return nil, fuse.EIO
	}

	if !parsed.IsValid {
		return nil, fuse.ENOENT
	}

	switch {
	// root or a topic
	case parsed.IsRoot, parsed.Offset == -1 && parsed.Partition == -1:
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0500}, fuse.OK
	// partition
	case parsed.Offset == -1:
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0700}, fuse.OK
	// offset / msg
	case true:
		msgBytes, ferr := fs.getMessage(parsed.Topic, parsed.Partition, parsed.Offset)
		if err != nil {
			return nil, ferr
		}
		fs.addUserFile(parsed.Topic, parsed.Partition, parsed.Offset)
		return &fuse.Attr{Mode: fuse.S_IFREG | 0400,
			Size: uint64(len(msgBytes))}, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (fs *KafkaRoFs) getMessage(topic string, partition int32, offset int64) ([]byte,
	fuse.Status) {
	msgBytes, err := fs.KafkaClient.GetMessage(topic, partition, offset)

	if err != nil {
		return nil, fuse.EIO
	}

	if msgBytes == nil {
		// this means the message doesn't exist (yet, possibly)
		return nil, fuse.ENOENT
	}

	err = fs.addUserFile(topic, partition, offset)
	if err != nil {
		return nil, fuse.EIO
	}

	return msgBytes, fuse.OK
}

func (fs *KafkaRoFs) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry,
	fuse.Status) {
	log.Printf("OpenDir name: %s", name)

	parsed, err := fs.parseAndValidate(name)
	if err != nil {
		return nil, fuse.EIO
	}

	if !parsed.IsValid {
		return nil, fuse.ENOENT
	}

	switch {
	case parsed.IsRoot:
		return fs.openRoot(context)
	case parsed.Partition == -1:
		return fs.openTopic(parsed.Topic, context)
	case parsed.Offset == -1:
		return fs.openPartition(parsed.Topic, parsed.Partition, context)
	}

	return nil, fuse.ENOENT
}

func (fs *KafkaRoFs) openPartition(topic string, partition int32,
	context *fuse.Context) ([]fuse.DirEntry,
	fuse.Status) {
	earliest, next, err := fs.KafkaClient.GetBoundingOffsets(topic, partition)
	if err != nil {
		return nil, fuse.EIO
	}
	if next == int64(0) {
		// this means an empty partition
		return []fuse.DirEntry{}, fuse.OK
	}

	entries := []fuse.DirEntry{fuse.DirEntry{Name: strconv.FormatInt(earliest, 10),
		Mode: fuse.S_IFREG}}
	if earliest != next-1 {
		entries = append(entries, fuse.DirEntry{Name: strconv.FormatInt(next-1, 10),
			Mode: fuse.S_IFREG})
	}

	var paths []string
	fs.userFilesM.RLock()
	for path, _ := range fs.userFiles {
		paths = append(paths, path)
	}
	fs.userFilesM.RUnlock()

	for _, path := range paths {
		parsed, err := fs.parseAndValidate(path)
		if err != nil {
			log.Panicf("Error was non-nil, bad user path %s", err)
		}
		if parsed.Partition == partition && parsed.Offset != earliest &&
			parsed.Offset != next-1 {
			entries = append(entries, fuse.DirEntry{
				Name: strconv.FormatInt(parsed.Offset, 10), Mode: fuse.S_IFREG})
		}
	}

	return entries, fuse.OK
}

func (fs *KafkaRoFs) openRoot(context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	// root, show all the topics
	var topicDirs []fuse.DirEntry
	topics, err := fs.KafkaClient.GetTopics()

	if err != nil {
		return nil, fuse.EIO
	}

	for _, topic := range topics {
		topicDirs = append(topicDirs, fuse.DirEntry{Name: topic, Mode: fuse.S_IFDIR})
	}

	return topicDirs, fuse.OK
}

func (fs *KafkaRoFs) openTopic(name string, context *fuse.Context) ([]fuse.DirEntry,
	fuse.Status) {
	var partitionDirs []fuse.DirEntry
	partitions, err := fs.KafkaClient.GetPartitions(name)

	if err != nil {
		return nil, fuse.EIO
	}

	for _, partition := range partitions {
		partitionDirs = append(partitionDirs,
			fuse.DirEntry{Name: strconv.FormatInt(int64(partition), 10),
				Mode: fuse.S_IFDIR})
	}

	return partitionDirs, fuse.OK
}

func (fs *KafkaRoFs) Open(name string, flags uint32,
	context *fuse.Context) (nodefs.File, fuse.Status) {
	parsed, err := fs.parseAndValidate(name)
	if err != nil {
		return nil, fuse.EIO
	}

	if !parsed.IsValid || parsed.Offset == -1 {
		return nil, fuse.ENOENT
	}

	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}

	msgBytes, ferr := fs.getMessage(parsed.Topic, parsed.Partition, parsed.Offset)

	if ferr != fuse.OK {
		return nil, ferr
	}

	return nodefs.NewDataFile(msgBytes), fuse.OK
}

func (fs *KafkaRoFs) addUserFile(topic string, partition int32, offset int64) error {
	path := fmt.Sprintf("%s/%d/%d", topic, partition, offset)
	fs.userFilesM.Lock()
	fs.userFiles[path] = true
	fs.userFilesM.Unlock()
	return nil
}

func (fs *KafkaRoFs) offsetNotExpired(topic string, partition int32,
	offset int64) (bool, error) {
	earliest, _, err := fs.KafkaClient.GetBoundingOffsets(topic, partition)
	if err != nil {
		return false, err
	}
	return offset >= earliest, nil
}

func (fs *KafkaRoFs) offsetIsFuture(topic string, partition int32,
	offset int64) (bool, error) {
	_, next, err := fs.KafkaClient.GetBoundingOffsets(topic, partition)
	if err != nil {
		return false, err
	}
	return offset >= next, nil
}

// just pretend we set the times to keep touch happy
func (fs *KafkaRoFs) Utimens(name string, Atime *time.Time, Mtime *time.Time,
	context *fuse.Context) fuse.Status {
	return fuse.OK
}

func NewKafkaRoFs(kClient KafkaClient) *KafkaRoFs {
	return &KafkaRoFs{FileSystem: pathfs.NewDefaultFileSystem(), KafkaClient: kClient,
		userFiles: make(map[string]bool)}
}

func (fs *KafkaRoFs) Unlink(name string, context *fuse.Context) fuse.Status {
	_, ok := fs.userFiles[name]
	if !ok {
		return fuse.EPERM
	}
	delete(fs.userFiles, name)
	return fuse.OK
}

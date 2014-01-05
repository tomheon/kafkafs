package kafkafs

import (
	"errors"
	"github.com/hanwen/go-fuse/fuse"
	"sort"
	"testing"
)

type MockKafkaClient struct {
	// a hacky topic => partition => offset => msg bytes structure
	messages map[string]map[int32]map[int64][]byte
}

func NewMockKafkaClient() *MockKafkaClient {
	return &MockKafkaClient{messages: make(map[string]map[int32]map[int64][]byte)}
}

func (mkc *MockKafkaClient) setMessage(topic string, partition int32, offset int64,
	bytes []byte) {
	mkc.createPartition(topic, partition)
	mkc.messages[topic][partition][offset] = bytes
}

func (mkc *MockKafkaClient) createPartition(topic string, partition int32) {
	_, tExists := mkc.messages[topic]
	if !tExists {
		mkc.messages[topic] = make(map[int32]map[int64][]byte)
	}
	_, pExists := mkc.messages[topic][partition]
	if !pExists {
		mkc.messages[topic][partition] = make(map[int64][]byte)
	}
}

func (mkc *MockKafkaClient) GetTopics() ([]string, error) {
	var topics []string

	for topic, _ := range mkc.messages {
		topics = append(topics, topic)
	}

	return topics, nil
}

func (mkc *MockKafkaClient) GetPartitions(topic string) ([]int32, error) {
	topicMap, ok := mkc.messages[topic]
	if !ok {
		return nil, errors.New("No such topic")
	}

	var partitions []int32
	for partition, _ := range topicMap {
		partitions = append(partitions, partition)
	}

	return partitions, nil
}

func (mkc *MockKafkaClient) GetBoundingOffsets(topic string, partition int32) (int64,
	int64, error) {
	topicMap, ok := mkc.messages[topic]
	if !ok {
		return -1, -1, errors.New("No such topic")
	}

	partMap, ok := topicMap[partition]
	if !ok {
		return -1, -1, errors.New("No such partition")
	}

	// because apparently Go is too cool to give native sorting for
	// int64, and the tests will never need offsets > 32 bits
	var offsets []int
	for offset, _ := range partMap {
		offsets = append(offsets, int(offset))
	}

	if len(offsets) == 0 {
		return 0, 0, nil
	}

	sort.Ints(offsets)
	return int64(offsets[0]), int64(offsets[len(offsets)-1]) + 1, nil
}

func (mkc *MockKafkaClient) GetMessage(topic string, partition int32,
	offset int64) ([]byte, error) {
	topicMap, ok := mkc.messages[topic]
	if !ok {
		return nil, errors.New("No such topic")
	}

	partMap, ok := topicMap[partition]
	if !ok {
		return nil, errors.New("No such partition")
	}

	bytes, ok := partMap[offset]
	if !ok {
		return nil, nil
	}

	return bytes, nil
}

func TestEmptyKafka(t *testing.T) {
	mkc := NewMockKafkaClient()
	fs := NewKafkaRoFs(mkc)
	rootDir, code := fs.OpenDir("", nil)

	if code != fuse.OK {
		t.Errorf("Fused Error code %s", code)
	}

	if len(rootDir) != 0 {
		t.Errorf("Non empty root %s", rootDir)
	}

	_, code = fs.OpenDir("topic", nil)

	if code != fuse.ENOENT {
		t.Error("Found topic in empty kafka")
	}
}

func TestWithMessages(t *testing.T) {
	mkc := NewMockKafkaClient()
	mkc.setMessage("topic", 0, 0, []byte("hi there"))
	fs := NewKafkaRoFs(mkc)
	rootDir, code := fs.OpenDir("", nil)

	if code != fuse.OK {
		t.Errorf("Fused Error code %s", code)
	}

	if len(rootDir) != 1 {
		t.Error("No topic dir")
	}

	topicDir, code := fs.OpenDir("topic", nil)

	if code != fuse.OK {
		t.Errorf("Fused Error code %s", code)
	}

	if len(topicDir) != 1 {
		t.Errorf("Wrong len %s", topicDir)
	}

	if topicDir[0].Name != "0" {
		t.Errorf("Wrong name for partition %s", topicDir[0].Name)
	}

	partDir, code := fs.OpenDir("topic/0", nil)

	if code != fuse.OK {
		t.Errorf("Fused Error code %s", code)
	}

	if len(partDir) != 1 {
		t.Errorf("Wrong len %s", partDir)
	}

	if partDir[0].Name != "0" {
		t.Errorf("Wrong name for partition %s", topicDir[0].Name)
	}

	mkc.setMessage("topic", 0, 1, []byte("bye there"))
	partDir, code = fs.OpenDir("topic/0", nil)

	if code != fuse.OK {
		t.Errorf("Fused Error code %s", code)
	}

	if len(partDir) != 2 {
		t.Errorf("Wrong len %s", partDir)
	}

	if partDir[0].Name != "0" {
		t.Errorf("Wrong name for partition %s", topicDir[0].Name)
	}

	if partDir[1].Name != "1" {
		t.Errorf("Wrong name for partition %s", topicDir[1].Name)
	}
}

func TestEmptyPartition(t *testing.T) {
	mkc := NewMockKafkaClient()
	mkc.createPartition("topic", 0)
	fs := NewKafkaRoFs(mkc)
	partitionDir, code := fs.OpenDir("topic/0", nil)

	if code != fuse.OK {
		t.Errorf("Fused Error code %s", code)
	}

	if len(partitionDir) != 0 {
		t.Errorf("Partition non-empty %s", partitionDir)
	}
}

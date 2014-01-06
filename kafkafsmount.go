package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	"github.com/tomheon/kafkafs/kafkafs"
)

func unmountOnInt(c chan os.Signal, server *fuse.Server, client *sarama.Client) {
	s := <-c
	fmt.Println("Got signal:", s)
	server.Unmount()
	client.Close()
}

func main() {

	var commandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var kafkaAddrs = commandLine.String("kafkaAddrs", "localhost:9092",
		"Kafka server addresses host1:port1[;host2:port2...]")
	var metadataRetries = commandLine.Int("metadataRetries", 10,
		"Max times to attempt metadata refresh from Kafka before failing")
	var waitForElectionMs = commandLine.Int("waitForElectionMs", 250,
		"Max milliseconds to wait for Kafka leader election before failing")
	var maxMsgBytes = commandLine.Int("maxMsgBytes", 1024*1024*10,
		"Max bytes to pull for a single message")

	commandLine.Parse(os.Args[1:])

	commandLine.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: kafkafs [options] mountpoint\n")
		commandLine.PrintDefaults()
		os.Exit(2)
	}

	if commandLine.NArg() != 1 {
		commandLine.Usage()
	}

	mountpoint := commandLine.Arg(0)

	addrs := strings.Split(*kafkaAddrs, ";")
	client, err := sarama.NewClient("kafkafs", addrs,
		&sarama.ClientConfig{MetadataRetries: *metadataRetries,
			WaitForElection: time.Duration(*waitForElectionMs) * time.Millisecond})
	if err != nil {
		log.Fatalf("Error from client %s", err)
	}

	kClient := kafkafs.NewKafkaClient(client, int32(*maxMsgBytes))

	nfs := pathfs.NewPathNodeFs(kafkafs.NewKafkaRoFs(kClient), nil)

	server, _, err := nodefs.MountFileSystem(mountpoint, nfs, nil)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go unmountOnInt(c, server, client)
	server.Serve()
}

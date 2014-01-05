package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
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
	addrs := []string{"localhost:9092"}
	client, err := sarama.NewClient("kafkafs", addrs, &sarama.ClientConfig{MetadataRetries: 10, WaitForElection: 250 * time.Millisecond})
	if err != nil {
		log.Fatalf("Error from client %s", err)
	}

	kClient := kafkafs.NewKafkaClient(client, 1024*1024)

	nfs := pathfs.NewPathNodeFs(kafkafs.NewKafkaRoFs(kClient), nil)

	server, _, err := nodefs.MountFileSystem("/home/edmund/hi", nfs, nil)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go unmountOnInt(c, server, client)
	server.Serve()
}

package kafkafs

import (
	"errors"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

// A much simplified interface for dealing with Kafka, restricted only
// to the operations that kafkafs needs and therefore easier to mock /
// control.
type KafkaClient interface {
	// list all topics
	GetTopics() ([]string, error)

	// list the partitions for a topic
	GetPartitions(topic string) ([]int32, error)

	// get the earliest and next offsets available for a topic /
	// partition.  Note that the "next" offset is for the next
	// message, that is, the one that currently doesn't exist yet
	// (e.g., 0 if the partition is empty)
	GetBoundingOffsets(topic string, partition int32) (int64, int64, error)

	// get the bytes for a given message
	GetMessage(topic string, partition int32, offset int64) ([]byte, error)
}

type kafkaClient struct {
	Client *sarama.Client

	MaxBytes int32
}

func (tsClient *kafkaClient) GetTopics() ([]string, error) {
	tsClient.Client.RefreshAllMetadata()
	topics, err := tsClient.Client.Topics()
	return topics, err
}

func (tsClient *kafkaClient) GetPartitions(topic string) ([]int32,
	error) {
	tsClient.Client.RefreshTopicMetadata(topic)
	partitions, err := tsClient.Client.Partitions(topic)
	return partitions, err
}

func (tsClient *kafkaClient) GetBoundingOffsets(topic string,
	partition int32) (int64, int64, error) {
	broker, err := tsClient.Client.Leader(topic, partition)

	if err != nil {
		return 0, 0, err
	}

	offsetRequest := &sarama.OffsetRequest{}
	offsetRequest.AddBlock(topic, partition, sarama.EarliestOffset, 100)
	offsetRequest.AddBlock(topic, partition, sarama.LatestOffsets, 100)
	offsetsResp, err := broker.GetAvailableOffsets("kafkafs", offsetRequest)

	if err != nil {
		return 0, 0, err
	}

	offsets := offsetsResp.Blocks[topic][partition].Offsets
	earliest := offsets[len(offsets) - 1]
	next := offsets[0]

	return earliest, next, nil
}

func (tsClient *kafkaClient) GetMessage(topic string, partition int32,
	offset int64) ([]byte, error) {
	log.Printf("GetMessage topic partition offset: %s %d %d", topic, partition,
		offset)
	broker, err := tsClient.Client.Leader(topic, partition)
	if err != nil {
		return nil, err
	}

	fetchRequest := &sarama.FetchRequest{}
	fetchRequest.AddBlock(topic, partition, offset, tsClient.MaxBytes)
	fetchResp, err := broker.Fetch("kafkafs", fetchRequest)
	if err != nil {
		return nil, err
	}
	responseBlock := fetchResp.Blocks[topic][partition]
	if responseBlock.Err != 0 {
		return nil, errors.New(fmt.Sprintf("Error code: %d", responseBlock.Err))
	}
	msgSet := responseBlock.MsgSet
	if len(msgSet.Messages) == 0 {
		// the offset didn't exist yet
		return nil, nil
	}
	msg := msgSet.Messages[0].Msg
	return msg.Value, nil
}

func NewKafkaClient(client *sarama.Client, maxBytes int32) KafkaClient {
    return &kafkaClient{Client: client, MaxBytes: maxBytes}
}
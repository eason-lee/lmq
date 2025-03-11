package store

import (
	pb "github.com/eason-lee/lmq/proto"
)

// Store 存储接口
type Store interface {
	Write(topic string, partition int, messages []*pb.Message) error
	Read(topic string, partition int, offset int64, maxMessages int) ([]*pb.Message, error)
	GetOffset(topic string, messageID string) (int64, error)
	Close() error
	CreatePartition(topic string, partID int) error
	GetLatestOffset(topic string, partition int) (int64, error)
	GetMessages(topic string) ([]*pb.Message, error)
	GetTopics() []string
	GetPartitions(topic string) []int
	CleanupSegments(topic string, partition int, policy CleanupPolicy) error
}

package store

import (
	"time"

	"github.com/eason-lee/lmq/pkg/protocol"
)

// Store 存储接口
type Store interface {
	Write(topic string, partition int, messages []*protocol.Message) error
	Read(topic string, partition int, offset int64, maxMessages int) ([]*protocol.Message, error)
	GetOffset(topic string, messageID string) (int64, error)
	Close() error
	CreatePartition(topic string, partID int) error
	GetLatestOffset(topic string, partition int) (int64, error)
	GetMessages(topic string) ([]*protocol.Message, error)
	GetTopics() []string
	GetPartitions(topic string) []int
	CleanupSegments(topic string, partition int, retention time.Duration) error 
}

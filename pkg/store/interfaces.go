package store

import (
	"github.com/eason-lee/lmq/pkg/protocol"
)

// Store 定义存储接口
type Store interface {
	Write(topic string, partition int, messages []*protocol.Message) error
	Read(topic string, partition int, offset int64, count int) ([]*protocol.Message, error)
	GetLatestOffset(topic string, partition int) (int64, error)
	Close() error
}

// PartitionStore 定义分区存储接口
type PartitionStore interface {
	Write(messages []*protocol.Message) error
	Read(offset int64, count int) ([]*protocol.Message, error)
	GetLatestOffset() (int64, error)
	Close() error
}

// SegmentStore 定义段存储接口
type SegmentStore interface {
	Write(msg *protocol.Message) error
	Read(offset int64, count int) ([]*protocol.Message, error)
	GetLatestOffset() (int64, error)
	Close() error
}

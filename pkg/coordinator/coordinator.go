package coordinator

import (
	"context"
	"time"
)

// ServiceInfo 表示一个服务实例的信息
type ServiceInfo struct {
	ID      string            `json:"id"`
	Name    string            `json:"name"`
	Address string            `json:"address"`
	Port    int               `json:"port"`
	Tags    []string          `json:"tags"`
	Meta    map[string]string `json:"meta"`
}

// PartitionInfo 分区信息
type PartitionInfo struct {
	ID        int      `json:"id"`
	Topic     string   `json:"topic"`
	Leader    string   `json:"leader"`
	ISR       []string `json:"isr"`       // In-Sync Replicas
	Replicas  []string `json:"replicas"`  // 所有副本
	Followers []string `json:"followers"` // 跟随者节点
}

// ConsumerGroupInfo 消费者组信息
type ConsumerGroupInfo struct {
	GroupID    string           `json:"group_id"`
	Topics     []string         `json:"topics"`
	Members    []string         `json:"members"`
	Offsets    map[string]int64 `json:"offsets"`    // topic -> offset
	Partitions map[string]int   `json:"partitions"` // topic -> partition
}

// Coordinator 协调器接口
type Coordinator interface {
	// 服务发现相关
	RegisterService(ctx context.Context, serviceName string, addr string) error
	UnregisterService(ctx context.Context, serviceName string, addr string) error
	GetService(ctx context.Context, serviceName string) ([]string, error)
	DiscoverService(ctx context.Context, serviceName string) ([]*ServiceInfo, error)

	// Topic 管理相关
	CreateTopic(ctx context.Context, topic string, partitions int) error
	DeleteTopic(ctx context.Context, topic string) error
	GetTopics(ctx context.Context) ([]string, error)
	TopicExists(ctx context.Context, topic string) (bool, error)

	// 分区管理相关
	GetPartition(ctx context.Context, topic string) (*PartitionInfo, error)
	GetPartitions(ctx context.Context, topic string) ([]*PartitionInfo, error)
	GetPartitionLeader(ctx context.Context, topic string, partitionID int) (string, error)
	GetPartitionReplicas(ctx context.Context, topic string, partitionID int) ([]string, error)

	// 消费者组管理相关
	RegisterConsumer(ctx context.Context, groupID string, topics []string) error
	UnregisterConsumer(ctx context.Context, groupID string) error
	GetConsumerOffset(ctx context.Context, groupID string, topic string) (int64, error)
	GetConsumerPartition(ctx context.Context, groupID string, topic string) (int, error)
	CommitOffset(ctx context.Context, groupID string, topic string, offset int64) error

	// 锁相关
	Lock(ctx context.Context, key string) error
	Unlock(ctx context.Context, key string) error

	// 领导者选举
	ElectLeader(ctx context.Context, key string) (bool, error)
	ResignLeader(ctx context.Context, key string) error
	IsLeader(ctx context.Context, key string) (bool, error)
	GetLeader(ctx context.Context, key string) (string, error)
	WatchLeader(ctx context.Context, key string) (<-chan string, error)

	// 配置管理
	PutConfig(ctx context.Context, key string, value []byte) error
	GetConfig(ctx context.Context, key string) ([]byte, error)
	WatchConfig(ctx context.Context, key string) (<-chan []byte, error)

	// 分布式锁
	AcquireLock(key string, ttl time.Duration) (bool, error)
	ReleaseLock(key string) error

	// 主题管理
	GetTopic(ctx context.Context, topic string) (map[string]interface{}, error)
	GetTopicPartitionCount(ctx context.Context, topic string) (int, error)
	WatchTopics(ctx context.Context) (<-chan []string, error)

	// 分区管理
	CreatePartition(partition *PartitionInfo) error
	UpdatePartition(partition *PartitionInfo) error
	DeletePartition(ctx context.Context, topic string, partitionID int) error
	WatchPartition(ctx context.Context, topic string, partitionID int) (<-chan *PartitionInfo, error)

	// ISR管理
	AddToISR(ctx context.Context, topic string, partitionID int, nodeID string) error
	RemoveFromISR(ctx context.Context, topic string, partitionID int, nodeID string) error
	GetISR(ctx context.Context, topic string, partitionID int) ([]string, error)
	WatchISR(ctx context.Context, topic string, partitionID int) (<-chan []string, error)

	// 消费者组管理
	GetConsumerGroup(ctx context.Context, groupID string) (*ConsumerGroupInfo, error)

	// 关闭
	Close() error
}

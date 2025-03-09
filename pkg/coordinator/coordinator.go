package coordinator

import (
	"context"
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

// Coordinator 定义了协调器接口
type Coordinator interface {
	// 服务注册相关
	RegisterService(ctx context.Context, nodeID string, addr string) error
	UnregisterService(ctx context.Context, nodeID string, addr string) error
	GetService(ctx context.Context, serviceName string) ([]string, error)
	DiscoverService(ctx context.Context, serviceName string) ([]*ServiceInfo, error)

	// 主题管理
	CreateTopic(ctx context.Context, topic string, partitionCount int) error
	DeleteTopic(ctx context.Context, topic string) error
	GetTopics(ctx context.Context) ([]string, error)
	GetTopic(ctx context.Context, topic string) (map[string]interface{}, error)
	TopicExists(ctx context.Context, topic string) (bool, error)
	GetTopicPartitionCount(ctx context.Context, topic string) (int, error)
	WatchTopics(ctx context.Context) (<-chan []string, error)

	// 分区管理
	GetPartition(ctx context.Context, topic string, partitionID int) (*PartitionInfo, error)
	GetPartitions(ctx context.Context, topic string) ([]*PartitionInfo, error)
	GetNodePartitions(ctx context.Context, nodeID string) ([]*PartitionInfo, error)
	GetLeaderPartitions(ctx context.Context, nodeID string) ([]*PartitionInfo, error)
	GetFollowerPartitions(ctx context.Context, nodeID string) ([]*PartitionInfo, error)
	GetPartitionLeader(ctx context.Context, topic string, partitionID int) (string, error)
	GetPartitionReplicas(ctx context.Context, topic string, partitionID int) ([]string, error)
	IsPartitionLeader(ctx context.Context, topic string, partitionID int, nodeID string) (bool, error)
	ElectPartitionLeader(ctx context.Context, topic string, partitionID int, nodeID string) (bool, error)

	// 消费者组管理
	RegisterConsumer(ctx context.Context, groupID string, topics []string) error
	UnregisterConsumer(ctx context.Context, groupID string) error
	GetConsumerGroup(ctx context.Context, groupID string) (*ConsumerGroupInfo, error)
	CommitOffset(ctx context.Context, groupID string, topic string, offset int64) error
	GetConsumerOffset(ctx context.Context, groupID string, topic string) (int64, error)
	GetConsumerPartition(ctx context.Context, groupID string, topic string) (int, error)

	// 副本状态管理
	RegisterReplicaStatus(ctx context.Context, topic string, partitionID int, nodeID string, offset int64) error
	GetReplicaStatus(ctx context.Context, topic string, partitionID int, nodeID string) (int64, error)
	GetAllReplicaStatus(ctx context.Context, topic string, partitionID int) (map[string]int64, error)

	// ISR管理
	GetISR(ctx context.Context, topic string, partitionID int) ([]string, error)
	UpdateISR(ctx context.Context, topic string, partitionID int, isr []string) error
	WatchISR(ctx context.Context, topic string, partitionID int) (<-chan []string, error)
	AddToISR(ctx context.Context, topic string, partitionID int, nodeID string) error
	RemoveFromISR(ctx context.Context, topic string, partitionID int, nodeID string) error

	// 分布式锁
	Lock(ctx context.Context, key string) error
	Unlock(ctx context.Context, key string) error

	// 关闭
	Close() error
}

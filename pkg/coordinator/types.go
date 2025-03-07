package coordinator

import "time"

// ServiceInfo 表示一个服务实例的信息
type ServiceInfo struct {
	ID      string            `json:"id"`
	Name    string            `json:"name"`
	Address string            `json:"address"`
	Port    int               `json:"port"`
	Tags    []string          `json:"tags"`
	Meta    map[string]string `json:"meta"`
}

// Coordinator 定义了集群协调所需的接口
type Coordinator interface {
	// 服务注册与发现
	RegisterService(service *ServiceInfo) error
	DeregisterService(serviceID string) error
	DiscoverService(serviceName string) ([]*ServiceInfo, error)
	
	// 领导者选举
	ElectLeader(serviceName string) (bool, error)
	ResignLeader(serviceName string) error
	IsLeader(serviceName string) bool
	WatchLeader(serviceName string) (<-chan string, error)
	
	// 配置管理
	PutConfig(key string, value []byte) error
	GetConfig(key string) ([]byte, error)
	WatchConfig(key string) (<-chan []byte, error)
	
	// 分布式锁
	AcquireLock(key string, ttl time.Duration) (bool, error)
	ReleaseLock(key string) error
	
	// 主题管理
	CreateTopic(topic string, partitionCount int) error
	GetTopic(topic string) (map[string]interface{}, error)
	GetAllTopics() ([]string, error)
	DeleteTopic(topic string) error
	GetTopicPartitionCount(topic string) (int, error)
	WatchTopics() (<-chan []string, error)
	
	// 分区管理
	CreatePartition(partition *PartitionInfo) error
	GetPartition(topic string, partitionID int) (*PartitionInfo, error)
	GetAllPartitions(topic string) ([]*PartitionInfo, error)
	UpdatePartition(partition *PartitionInfo) error
	DeletePartition(topic string, partitionID int) error
	WatchPartition(topic string, partitionID int) (<-chan *PartitionInfo, error)
	
	// 分区leader管理
	ElectPartitionLeader(topic string, partitionID int, nodeID string) (bool, error)
	ResignPartitionLeader(topic string, partitionID int) error
	IsPartitionLeader(topic string, partitionID int, nodeID string) (bool, error)
	GetPartitionLeader(topic string, partitionID int) (string, error)
	
	// ISR管理
	UpdateISR(topic string, partitionID int, isr []string) error
	AddToISR(topic string, partitionID int, nodeID string) error
	RemoveFromISR(topic string, partitionID int, nodeID string) error
	
	// Follower管理
	AddFollower(topic string, partitionID int, nodeID string) error
	RemoveFollower(topic string, partitionID int, nodeID string) error
	
	// 副本状态管理
	RegisterReplicaStatus(topic string, partitionID int, nodeID string, offset int64) error
	GetReplicaStatus(topic string, partitionID int, nodeID string) (int64, error)
	GetAllReplicaStatus(topic string, partitionID int) (map[string]int64, error)
	
	// 节点分区查询
	GetNodePartitions(nodeID string) ([]*PartitionInfo, error)
	GetLeaderPartitions(nodeID string) ([]*PartitionInfo, error)
	GetFollowerPartitions(nodeID string) ([]*PartitionInfo, error)
	
	// 关闭
	Close() error
}

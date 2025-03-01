package consul

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

	// 关闭和清理
	Close() error
}

package consul

import (
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

type ConsulCoordinator struct {
	client   *api.Client
	config   *api.Config
	sessions map[string]string // key -> session ID
	isLeader map[string]bool   // service -> is leader
	mu       sync.RWMutex
}

// NewCoordinator 创建一个新的 Consul 协调器
func NewCoordinator(address string) (*ConsulCoordinator, error) {
	config := api.DefaultConfig()
	config.Address = address

	client, err := api.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consul client: %w", err)
	}

	return &ConsulCoordinator{
		client:   client,
		config:   config,
		sessions: make(map[string]string),
		isLeader: make(map[string]bool),
	}, nil
}

// RegisterService 注册服务到 Consul
func (c *ConsulCoordinator) RegisterService(service *ServiceInfo) error {
	reg := &api.AgentServiceRegistration{
		ID:      service.ID,
		Name:    service.Name,
		Address: service.Address,
		Port:    service.Port,
		Tags:    service.Tags,
		Meta:    service.Meta,
		Check: &api.AgentServiceCheck{
			TCP:                            fmt.Sprintf("%s:%d", service.Address, service.Port),
			Interval:                       "10s",
			Timeout:                        "5s",
			DeregisterCriticalServiceAfter: "30s",
		},
	}

	return c.client.Agent().ServiceRegister(reg)
}

// DeregisterService 从 Consul 注销服务
func (c *ConsulCoordinator) DeregisterService(serviceID string) error {
	return c.client.Agent().ServiceDeregister(serviceID)
}

// DiscoverService 发现服务
func (c *ConsulCoordinator) DiscoverService(serviceName string) ([]*ServiceInfo, error) {
	services, _, err := c.client.Health().Service(serviceName, "", true, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to discover service: %w", err)
	}

	var result []*ServiceInfo
	for _, service := range services {
		result = append(result, &ServiceInfo{
			ID:      service.Service.ID,
			Name:    service.Service.Service,
			Address: service.Service.Address,
			Port:    service.Service.Port,
			Tags:    service.Service.Tags,
			Meta:    service.Service.Meta,
		})
	}

	return result, nil
}

// ElectLeader 尝试成为领导者
func (c *ConsulCoordinator) ElectLeader(serviceName string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 创建会话
	session, _, err := c.client.Session().Create(&api.SessionEntry{
		Name:     fmt.Sprintf("lmq-leader-%s", serviceName),
		TTL:      "15s",
		Behavior: "release",
	}, nil)

	if err != nil {
		return false, fmt.Errorf("failed to create session: %w", err)
	}

	key := fmt.Sprintf("lmq/leader/%s", serviceName)
	acquired, _, err := c.client.KV().Acquire(&api.KVPair{
		Key:     key,
		Value:   []byte(session),
		Session: session,
	}, nil)

	if err != nil {
		return false, err
	}

	if acquired {
		c.sessions[key] = session
		c.isLeader[serviceName] = true
	}

	return acquired, nil
}

// ResignLeader 放弃领导者身份
func (c *ConsulCoordinator) ResignLeader(serviceName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("lmq/leader/%s", serviceName)
	session, exists := c.sessions[key]
	if !exists {
		return nil
	}

	released, _, err := c.client.KV().Release(&api.KVPair{
		Key:     key,
		Session: session,
	}, nil)

	if err == nil && released{
		delete(c.sessions, key)
		delete(c.isLeader, serviceName)
	}

	return err
}

// IsLeader 检查是否是领导者
func (c *ConsulCoordinator) IsLeader(serviceName string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isLeader[serviceName]
}

// WatchLeader 监听领导者变化
func (c *ConsulCoordinator) WatchLeader(serviceName string) (<-chan string, error) {
	ch := make(chan string, 1)
	key := fmt.Sprintf("lmq/leader/%s", serviceName)

	go func() {
		var lastIndex uint64
		for {
			pair, meta, err := c.client.KV().Get(key, &api.QueryOptions{
				WaitIndex: lastIndex,
			})
			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			if meta.LastIndex > lastIndex {
				lastIndex = meta.LastIndex
				if pair != nil {
					ch <- string(pair.Value)
				}
			}
		}
	}()

	return ch, nil
}

// PutConfig 存储配置
func (c *ConsulCoordinator) PutConfig(key string, value []byte) error {
	_, err := c.client.KV().Put(&api.KVPair{
		Key:   fmt.Sprintf("lmq/config/%s", key),
		Value: value,
	}, nil)
	return err
}

// GetConfig 获取配置
func (c *ConsulCoordinator) GetConfig(key string) ([]byte, error) {
	pair, _, err := c.client.KV().Get(fmt.Sprintf("lmq/config/%s", key), nil)
	if err != nil {
		return nil, err
	}
	if pair == nil {
		return nil, nil
	}
	return pair.Value, nil
}

// WatchConfig 监听配置变化
func (c *ConsulCoordinator) WatchConfig(key string) (<-chan []byte, error) {
	ch := make(chan []byte, 1)

	go func() {
		var lastIndex uint64
		for {
			pair, meta, err := c.client.KV().Get(fmt.Sprintf("lmq/config/%s", key), &api.QueryOptions{
				WaitIndex: lastIndex,
			})
			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			if meta.LastIndex > lastIndex {
				lastIndex = meta.LastIndex
				if pair != nil {
					ch <- pair.Value
				}
			}
		}
	}()

	return ch, nil
}

// AcquireLock 获取分布式锁
func (c *ConsulCoordinator) AcquireLock(key string, ttl time.Duration) (bool, error) {
	session, _, err := c.client.Session().Create(&api.SessionEntry{
		Name:     fmt.Sprintf("lmq-lock-%s", key),
		TTL:      ttl.String(),
		Behavior: "release",
	}, nil)

	if err != nil {
		return false, err
	}

	acquired, _, err := c.client.KV().Acquire(&api.KVPair{
		Key:     fmt.Sprintf("lmq/locks/%s", key),
		Value:   []byte(session),
		Session: session,
	}, nil)

	if err == nil && acquired {
		c.mu.Lock()
		c.sessions[key] = session
		c.mu.Unlock()
	}

	return acquired, err
}

// ReleaseLock 释放分布式锁
func (c *ConsulCoordinator) ReleaseLock(key string) error {
	c.mu.Lock()
	session, exists := c.sessions[key]
	c.mu.Unlock()

	if !exists {
		return nil
	}

	released, _, err := c.client.KV().Release(&api.KVPair{
		Key:     fmt.Sprintf("lmq/locks/%s", key),
		Session: session,
	}, nil)

	if err == nil && released{
		c.mu.Lock()
		delete(c.sessions, key)
		c.mu.Unlock()
	}

	return err
}

// Close 关闭协调器
func (c *ConsulCoordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 注销所有会话
	for key, session := range c.sessions {
		c.client.Session().Destroy(session, nil)
		delete(c.sessions, key)
	}

	// 清理状态
	c.isLeader = make(map[string]bool)

	return nil
}

package coordinator

import (
	"encoding/json"
	"fmt"
	"strings"
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

// NewConsulCoordinator 创建一个新的 Consul 协调器
func NewConsulCoordinator(address string) (*ConsulCoordinator, error) {
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


// PartitionInfo 分区信息
type PartitionInfo struct {
	Topic     string   `json:"topic"`
	ID        int      `json:"id"`
	Leader    string   `json:"leader"`
	Followers []string `json:"followers"`
	ISR       []string `json:"isr"`
}

// CreatePartition 创建分区
func (c *ConsulCoordinator) CreatePartition(partition *PartitionInfo) error {
	data, err := json.Marshal(partition)
	if err != nil {
		return fmt.Errorf("序列化分区信息失败: %w", err)
	}

	key := fmt.Sprintf("lmq/partitions/%s/%d", partition.Topic, partition.ID)
	_, err = c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: data,
	}, nil)

	return err
}

// GetPartition 获取分区信息
func (c *ConsulCoordinator) GetPartition(topic string, partitionID int) (*PartitionInfo, error) {
	key := fmt.Sprintf("lmq/partitions/%s/%d", topic, partitionID)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, fmt.Errorf("获取分区信息失败: %w", err)
	}

	if pair == nil {
		return nil, nil // 分区不存在
	}

	var partition PartitionInfo
	if err := json.Unmarshal(pair.Value, &partition); err != nil {
		return nil, fmt.Errorf("解析分区信息失败: %w", err)
	}

	return &partition, nil
}

// GetAllPartitions 获取主题的所有分区
func (c *ConsulCoordinator) GetAllPartitions(topic string) ([]*PartitionInfo, error) {
	prefix := fmt.Sprintf("lmq/partitions/%s/", topic)
	pairs, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, fmt.Errorf("获取分区列表失败: %w", err)
	}

	var partitions []*PartitionInfo
	for _, pair := range pairs {
		var partition PartitionInfo
		if err := json.Unmarshal(pair.Value, &partition); err != nil {
			return nil, fmt.Errorf("解析分区信息失败: %w", err)
		}
		partitions = append(partitions, &partition)
	}

	return partitions, nil
}

// UpdatePartition 更新分区信息
func (c *ConsulCoordinator) UpdatePartition(partition *PartitionInfo) error {
	data, err := json.Marshal(partition)
	if err != nil {
		return fmt.Errorf("序列化分区信息失败: %w", err)
	}

	key := fmt.Sprintf("lmq/partitions/%s/%d", partition.Topic, partition.ID)
	_, err = c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: data,
	}, nil)

	return err
}

// DeletePartition 删除分区
func (c *ConsulCoordinator) DeletePartition(topic string, partitionID int) error {
	key := fmt.Sprintf("lmq/partitions/%s/%d", topic, partitionID)
	_, err := c.client.KV().Delete(key, nil)
	return err
}

// WatchPartition 监听分区变化
func (c *ConsulCoordinator) WatchPartition(topic string, partitionID int) (<-chan *PartitionInfo, error) {
	ch := make(chan *PartitionInfo, 1)
	key := fmt.Sprintf("lmq/partitions/%s/%d", topic, partitionID)

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
					var partition PartitionInfo
					if err := json.Unmarshal(pair.Value, &partition); err != nil {
						continue
					}
					ch <- &partition
				}
			}
		}
	}()

	return ch, nil
}

// CreateTopic 创建主题
func (c *ConsulCoordinator) CreateTopic(topic string, partitionCount int) error {
	// 检查主题是否已存在
	key := fmt.Sprintf("lmq/topics/%s", topic)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return fmt.Errorf("检查主题失败: %w", err)
	}

	if pair != nil {
		return fmt.Errorf("主题 %s 已存在", topic)
	}

	// 创建主题元数据
	topicInfo := map[string]interface{}{
		"name":            topic,
		"partition_count": partitionCount,
		"created_at":      time.Now().Unix(),
	}

	data, err := json.Marshal(topicInfo)
	if err != nil {
		return fmt.Errorf("序列化主题信息失败: %w", err)
	}

	_, err = c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: data,
	}, nil)

	if err != nil {
		return fmt.Errorf("创建主题失败: %w", err)
	}

	return nil
}

// GetTopic 获取主题信息
func (c *ConsulCoordinator) GetTopic(topic string) (map[string]interface{}, error) {
	key := fmt.Sprintf("lmq/topics/%s", topic)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, fmt.Errorf("获取主题信息失败: %w", err)
	}

	if pair == nil {
		return nil, nil // 主题不存在
	}

	var topicInfo map[string]interface{}
	if err := json.Unmarshal(pair.Value, &topicInfo); err != nil {
		return nil, fmt.Errorf("解析主题信息失败: %w", err)
	}

	return topicInfo, nil
}

// GetAllTopics 获取所有主题
func (c *ConsulCoordinator) GetAllTopics() ([]string, error) {
	prefix := "lmq/topics/"
	pairs, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, fmt.Errorf("获取主题列表失败: %w", err)
	}

	var topics []string
	for _, pair := range pairs {
		// 从键中提取主题名
		topic := strings.TrimPrefix(pair.Key, prefix)
		topics = append(topics, topic)
	}

	return topics, nil
}

// DeleteTopic 删除主题
func (c *ConsulCoordinator) DeleteTopic(topic string) error {
	// 删除主题元数据
	key := fmt.Sprintf("lmq/topics/%s", topic)
	_, err := c.client.KV().Delete(key, nil)
	if err != nil {
		return fmt.Errorf("删除主题元数据失败: %w", err)
	}

	// 删除主题的所有分区
	prefix := fmt.Sprintf("lmq/partitions/%s/", topic)
	_, err = c.client.KV().DeleteTree(prefix, nil)
	if err != nil {
		return fmt.Errorf("删除主题分区失败: %w", err)
	}

	return nil
}

// ElectPartitionLeader 为分区选举leader
func (c *ConsulCoordinator) ElectPartitionLeader(topic string, partitionID int, nodeID string) (bool, error) {
	// 获取分区信息
	partition, err := c.GetPartition(topic, partitionID)
	if err != nil {
		return false, err
	}

	if partition == nil {
		return false, fmt.Errorf("分区不存在: %s-%d", topic, partitionID)
	}

	// 创建会话
	session, _, err := c.client.Session().Create(&api.SessionEntry{
		Name:     fmt.Sprintf("lmq-partition-leader-%s-%d", topic, partitionID),
		TTL:      "15s",
		Behavior: "release",
	}, nil)

	if err != nil {
		return false, fmt.Errorf("创建会话失败: %w", err)
	}

	// 尝试获取分区leader锁
	key := fmt.Sprintf("lmq/partition-leaders/%s/%d", topic, partitionID)
	acquired, _, err := c.client.KV().Acquire(&api.KVPair{
		Key:     key,
		Value:   []byte(nodeID),
		Session: session,
	}, nil)

	if err != nil {
		return false, err
	}

	if acquired {
		// 更新分区元数据
		partition.Leader = nodeID
		
		// 如果节点在followers中，将其移除
		var newFollowers []string
		for _, f := range partition.Followers {
			if f != nodeID {
				newFollowers = append(newFollowers, f)
			}
		}
		partition.Followers = newFollowers
		
		// 确保节点在ISR中
		inISR := false
		for _, isr := range partition.ISR {
			if isr == nodeID {
				inISR = true
				break
			}
		}
		if !inISR {
			partition.ISR = append(partition.ISR, nodeID)
		}
		
		// 更新分区信息
		if err := c.UpdatePartition(partition); err != nil {
			// 如果更新失败，释放锁
			c.client.KV().Release(&api.KVPair{
				Key:     key,
				Session: session,
			}, nil)
			return false, fmt.Errorf("更新分区信息失败: %w", err)
		}
		
		// 保存会话ID
		c.mu.Lock()
		c.sessions[key] = session
		c.mu.Unlock()
	}

	return acquired, nil
}

// ResignPartitionLeader 放弃分区leader身份
func (c *ConsulCoordinator) ResignPartitionLeader(topic string, partitionID int) error {
	key := fmt.Sprintf("lmq/partition-leaders/%s/%d", topic, partitionID)
	
	c.mu.Lock()
	session, exists := c.sessions[key]
	c.mu.Unlock()
	
	if !exists {
		return nil
	}

	released, _, err := c.client.KV().Release(&api.KVPair{
		Key:     key,
		Session: session,
	}, nil)

	if err == nil && released {
		c.mu.Lock()
		delete(c.sessions, key)
		c.mu.Unlock()
	}

	return err
}

// IsPartitionLeader 检查节点是否是分区的leader
func (c *ConsulCoordinator) IsPartitionLeader(topic string, partitionID int, nodeID string) (bool, error) {
	key := fmt.Sprintf("lmq/partition-leaders/%s/%d", topic, partitionID)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return false, err
	}

	if pair == nil {
		return false, nil
	}

	return string(pair.Value) == nodeID, nil
}

// RegisterReplicaStatus 注册副本状态
func (c *ConsulCoordinator) RegisterReplicaStatus(topic string, partitionID int, nodeID string, offset int64) error {
	key := fmt.Sprintf("lmq/replica-status/%s/%d/%s", topic, partitionID, nodeID)
	data, err := json.Marshal(map[string]interface{}{
		"offset":     offset,
		"updated_at": time.Now().Unix(),
	})
	if err != nil {
		return err
	}

	_, err = c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: data,
	}, nil)

	return err
}

// GetReplicaStatus 获取副本状态
func (c *ConsulCoordinator) GetReplicaStatus(topic string, partitionID int, nodeID string) (int64, error) {
	key := fmt.Sprintf("lmq/replica-status/%s/%d/%s", topic, partitionID, nodeID)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return 0, err
	}

	if pair == nil {
		return 0, nil // 副本状态不存在
	}

	var status map[string]interface{}
	if err := json.Unmarshal(pair.Value, &status); err != nil {
		return 0, err
	}

	offset, ok := status["offset"].(float64)
	if !ok {
		return 0, fmt.Errorf("无效的偏移量格式")
	}

	return int64(offset), nil
}

// GetAllReplicaStatus 获取分区的所有副本状态
func (c *ConsulCoordinator) GetAllReplicaStatus(topic string, partitionID int) (map[string]int64, error) {
	prefix := fmt.Sprintf("lmq/replica-status/%s/%d/", topic, partitionID)
	pairs, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, err
	}

	result := make(map[string]int64)
	for _, pair := range pairs {
		// 从键中提取节点ID
		nodeID := strings.TrimPrefix(pair.Key, prefix)
		
		var status map[string]interface{}
		if err := json.Unmarshal(pair.Value, &status); err != nil {
			return nil, err
		}

		offset, ok := status["offset"].(float64)
		if !ok {
			return nil, fmt.Errorf("无效的偏移量格式")
		}

		result[nodeID] = int64(offset)
	}

	return result, nil
}

// GetPartitionLeader 获取分区的leader节点ID
func (c *ConsulCoordinator) GetPartitionLeader(topic string, partitionID int) (string, error) {
	key := fmt.Sprintf("lmq/partition-leaders/%s/%d", topic, partitionID)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return "", err
	}

	if pair == nil {
		return "", nil // 没有leader
	}

	return string(pair.Value), nil
}

// UpdateISR 更新分区的ISR列表
func (c *ConsulCoordinator) UpdateISR(topic string, partitionID int, isr []string) error {
	partition, err := c.GetPartition(topic, partitionID)
	if err != nil {
		return err
	}

	if partition == nil {
		return fmt.Errorf("分区不存在: %s-%d", topic, partitionID)
	}

	partition.ISR = isr
	return c.UpdatePartition(partition)
}

// AddToISR 将节点添加到ISR
func (c *ConsulCoordinator) AddToISR(topic string, partitionID int, nodeID string) error {
	partition, err := c.GetPartition(topic, partitionID)
	if err != nil {
		return err
	}

	if partition == nil {
		return fmt.Errorf("分区不存在: %s-%d", topic, partitionID)
	}

	// 检查节点是否已在ISR中
	for _, id := range partition.ISR {
		if id == nodeID {
			return nil // 已经在ISR中
		}
	}

	// 添加到ISR
	partition.ISR = append(partition.ISR, nodeID)
	return c.UpdatePartition(partition)
}

// RemoveFromISR 从ISR中移除节点
func (c *ConsulCoordinator) RemoveFromISR(topic string, partitionID int, nodeID string) error {
	partition, err := c.GetPartition(topic, partitionID)
	if err != nil {
		return err
	}

	if partition == nil {
		return fmt.Errorf("分区不存在: %s-%d", topic, partitionID)
	}

	// 从ISR中移除节点
	var newISR []string
	for _, id := range partition.ISR {
		if id != nodeID {
			newISR = append(newISR, id)
		}
	}

	// 如果ISR没有变化，直接返回
	if len(newISR) == len(partition.ISR) {
		return nil
	}

	partition.ISR = newISR
	return c.UpdatePartition(partition)
}

// AddFollower 添加follower节点
func (c *ConsulCoordinator) AddFollower(topic string, partitionID int, nodeID string) error {
	partition, err := c.GetPartition(topic, partitionID)
	if err != nil {
		return err
	}

	if partition == nil {
		return fmt.Errorf("分区不存在: %s-%d", topic, partitionID)
	}

	// 如果节点是leader，不能添加为follower
	if partition.Leader == nodeID {
		return fmt.Errorf("节点 %s 是分区 %s-%d 的leader，不能添加为follower", nodeID, topic, partitionID)
	}

	// 检查节点是否已是follower
	for _, id := range partition.Followers {
		if id == nodeID {
			return nil // 已经是follower
		}
	}

	// 添加为follower
	partition.Followers = append(partition.Followers, nodeID)
	return c.UpdatePartition(partition)
}

// RemoveFollower 移除follower节点
func (c *ConsulCoordinator) RemoveFollower(topic string, partitionID int, nodeID string) error {
	partition, err := c.GetPartition(topic, partitionID)
	if err != nil {
		return err
	}

	if partition == nil {
		return fmt.Errorf("分区不存在: %s-%d", topic, partitionID)
	}

	// 从followers中移除节点
	var newFollowers []string
	for _, id := range partition.Followers {
		if id != nodeID {
			newFollowers = append(newFollowers, id)
		}
	}

	// 如果followers没有变化，直接返回
	if len(newFollowers) == len(partition.Followers) {
		return nil
	}

	partition.Followers = newFollowers
	return c.UpdatePartition(partition)
}

// GetTopicPartitionCount 获取主题的分区数量
func (c *ConsulCoordinator) GetTopicPartitionCount(topic string) (int, error) {
	topicInfo, err := c.GetTopic(topic)
	if err != nil {
		return 0, err
	}

	if topicInfo == nil {
		return 0, fmt.Errorf("主题 %s 不存在", topic)
	}

	partitionCount, ok := topicInfo["partition_count"].(float64)
	if !ok {
		return 0, fmt.Errorf("无效的分区数量格式")
	}

	return int(partitionCount), nil
}

// WatchTopics 监听主题变化
func (c *ConsulCoordinator) WatchTopics() (<-chan []string, error) {
	ch := make(chan []string, 1)
	prefix := "lmq/topics/"

	go func() {
		var lastIndex uint64
		for {
			pairs, meta, err := c.client.KV().List(prefix, &api.QueryOptions{
				WaitIndex: lastIndex,
			})
			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			if meta.LastIndex > lastIndex {
				lastIndex = meta.LastIndex
				
				var topics []string
				for _, pair := range pairs {
					topic := strings.TrimPrefix(pair.Key, prefix)
					topics = append(topics, topic)
				}
				
				ch <- topics
			}
		}
	}()

	return ch, nil
}

// GetNodePartitions 获取节点负责的所有分区
func (c *ConsulCoordinator) GetNodePartitions(nodeID string) ([]*PartitionInfo, error) {
	// 获取所有主题
	topics, err := c.GetAllTopics()
	if err != nil {
		return nil, err
	}

	var result []*PartitionInfo
	
	// 遍历所有主题的所有分区
	for _, topic := range topics {
		partitions, err := c.GetAllPartitions(topic)
		if err != nil {
			return nil, err
		}

		// 查找节点负责的分区
		for _, partition := range partitions {
			// 如果节点是leader
			if partition.Leader == nodeID {
				result = append(result, partition)
				continue
			}

			// 如果节点是follower
			for _, follower := range partition.Followers {
				if follower == nodeID {
					result = append(result, partition)
					break
				}
			}
		}
	}

	return result, nil
}

// GetLeaderPartitions 获取节点作为leader的所有分区
func (c *ConsulCoordinator) GetLeaderPartitions(nodeID string) ([]*PartitionInfo, error) {
	// 获取所有主题
	topics, err := c.GetAllTopics()
	if err != nil {
		return nil, err
	}

	var result []*PartitionInfo
	
	// 遍历所有主题的所有分区
	for _, topic := range topics {
		partitions, err := c.GetAllPartitions(topic)
		if err != nil {
			return nil, err
		}

		// 查找节点作为leader的分区
		for _, partition := range partitions {
			if partition.Leader == nodeID {
				result = append(result, partition)
			}
		}
	}

	return result, nil
}

// GetFollowerPartitions 获取节点作为follower的所有分区
func (c *ConsulCoordinator) GetFollowerPartitions(nodeID string) ([]*PartitionInfo, error) {
	// 获取所有主题
	topics, err := c.GetAllTopics()
	if err != nil {
		return nil, err
	}

	var result []*PartitionInfo
	
	// 遍历所有主题的所有分区
	for _, topic := range topics {
		partitions, err := c.GetAllPartitions(topic)
		if err != nil {
			return nil, err
		}

		// 查找节点作为follower的分区
		for _, partition := range partitions {
			for _, follower := range partition.Followers {
				if follower == nodeID {
					result = append(result, partition)
					break
				}
			}
		}
	}

	return result, nil
}

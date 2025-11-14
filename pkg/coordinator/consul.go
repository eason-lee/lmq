package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
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

// RegisterService 注册服务
func (c *ConsulCoordinator) RegisterService(ctx context.Context, serviceName string, addr string) error {
	// 构建服务注册信息
	reg := &api.AgentServiceRegistration{
		ID:      serviceName,
		Name:    serviceName,
		Address: addr,
		Check: &api.AgentServiceCheck{
			TCP:      addr,
			Interval: "10s",
			Timeout:  "5s",
		},
	}

	return c.client.Agent().ServiceRegister(reg)
}

// UnregisterService 从 Consul 注销服务
func (c *ConsulCoordinator) UnregisterService(ctx context.Context, serviceName string, addr string) error {
	return c.client.Agent().ServiceDeregister(serviceName)
}

// DiscoverService 发现服务
func (c *ConsulCoordinator) DiscoverService(ctx context.Context, serviceName string) ([]*ServiceInfo, error) {
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

// GetService 获取服务地址列表
func (c *ConsulCoordinator) GetService(ctx context.Context, serviceName string) ([]string, error) {
	services, _, err := c.client.Health().Service(serviceName, "", true, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get service: %w", err)
	}

	var addresses []string
	for _, service := range services {
		addr := fmt.Sprintf("%s:%d", service.Service.Address, service.Service.Port)
		addresses = append(addresses, addr)
	}

	return addresses, nil
}

// ElectLeader 尝试成为领导者
func (c *ConsulCoordinator) ElectLeader(ctx context.Context, key string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 创建会话
	session, _, err := c.client.Session().Create(&api.SessionEntry{
		Name:     fmt.Sprintf("lmq-leader-%s", key),
		TTL:      "15s",
		Behavior: "release",
	}, nil)

	if err != nil {
		return false, fmt.Errorf("failed to create session: %w", err)
	}

	lockKey := fmt.Sprintf("lmq/leader/%s", key)
	acquired, _, err := c.client.KV().Acquire(&api.KVPair{
		Key:     lockKey,
		Value:   []byte(session),
		Session: session,
	}, nil)

	if err != nil {
		return false, err
	}

	if acquired {
		c.sessions[lockKey] = session
		c.isLeader[key] = true
	}

	return acquired, nil
}

// ResignLeader 放弃领导者身份
func (c *ConsulCoordinator) ResignLeader(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	lockKey := fmt.Sprintf("lmq/leader/%s", key)
	session, exists := c.sessions[lockKey]
	if !exists {
		return nil
	}

	released, _, err := c.client.KV().Release(&api.KVPair{
		Key:     lockKey,
		Session: session,
	}, nil)

	if err == nil && released {
		delete(c.sessions, lockKey)
		delete(c.isLeader, key)
	}

	return err
}

// IsLeader 检查是否是领导者
func (c *ConsulCoordinator) IsLeader(ctx context.Context, key string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.isLeader[key], nil
}

// GetLeader 获取当前的领导者
func (c *ConsulCoordinator) GetLeader(ctx context.Context, key string) (string, error) {
	lockKey := fmt.Sprintf("lmq/leader/%s", key)
	pair, _, err := c.client.KV().Get(lockKey, nil)
	if err != nil {
		return "", err
	}

	if pair == nil {
		return "", fmt.Errorf("no leader found for %s", key)
	}

	return string(pair.Value), nil
}

// WatchLeader 监听领导者变化
func (c *ConsulCoordinator) WatchLeader(ctx context.Context, key string) (<-chan string, error) {
	ch := make(chan string, 1)
	lockKey := fmt.Sprintf("lmq/leader/%s", key)

	go func() {
		defer close(ch)

		var lastIndex uint64
		for {
			select {
			case <-ctx.Done():
				return
			default:
				pair, meta, err := c.client.KV().Get(lockKey, &api.QueryOptions{
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
		}
	}()

	return ch, nil
}

// PutConfig 存储配置
func (c *ConsulCoordinator) PutConfig(ctx context.Context, key string, value []byte) error {
	_, err := c.client.KV().Put(&api.KVPair{
		Key:   fmt.Sprintf("lmq/config/%s", key),
		Value: value,
	}, nil)
	return err
}

// GetConfig 获取配置
func (c *ConsulCoordinator) GetConfig(ctx context.Context, key string) ([]byte, error) {
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
func (c *ConsulCoordinator) WatchConfig(ctx context.Context, key string) (<-chan []byte, error) {
	ch := make(chan []byte, 1)
	configKey := fmt.Sprintf("lmq/config/%s", key)

	go func() {
		defer close(ch)

		var lastIndex uint64
		for {
			select {
			case <-ctx.Done():
				return
			default:
				pair, meta, err := c.client.KV().Get(configKey, &api.QueryOptions{
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

	if err == nil && released {
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

// CreatePartition 创建分区
func (c *ConsulCoordinator) CreatePartition(ctx context.Context, partition *PartitionInfo) error {
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
func (c *ConsulCoordinator) GetPartition(ctx context.Context, topic string, partitionID int) (*PartitionInfo, error) {
	key := fmt.Sprintf("topics/%s/partitions/%d", topic, partitionID)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, fmt.Errorf("获取分区信息失败: %w", err)
	}

	if pair == nil {
		return nil, fmt.Errorf("分区不存在")
	}

	var partition PartitionInfo
	if err := json.Unmarshal(pair.Value, &partition); err != nil {
		return nil, fmt.Errorf("解析分区信息失败: %w", err)
	}

	return &partition, nil
}

// GetPartitions 获取主题的所有分区
func (c *ConsulCoordinator) GetPartitions(ctx context.Context, topic string) ([]*PartitionInfo, error) {
	prefix := fmt.Sprintf("topics/%s/partitions/", topic)
	pairs, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, fmt.Errorf("获取分区列表失败: %w", err)
	}

	partitions := make([]*PartitionInfo, 0, len(pairs))
	for _, pair := range pairs {
		var partition PartitionInfo
		if err := json.Unmarshal(pair.Value, &partition); err != nil {
			return nil, fmt.Errorf("解析分区信息失败: %w", err)
		}
		partitions = append(partitions, &partition)
	}

	return partitions, nil
}

// GetPartitionLeader 获取分区的leader节点
func (c *ConsulCoordinator) GetPartitionLeader(ctx context.Context, topic string, partitionID int) (string, error) {
	partition, err := c.GetPartition(ctx, topic, partitionID)
	if err != nil {
		return "", err
	}

	if partition == nil {
		return "", fmt.Errorf("分区不存在: %s-%d", topic, partitionID)
	}

	return partition.Leader, nil
}

// GetPartitionReplicas 获取分区的副本列表
func (c *ConsulCoordinator) GetPartitionReplicas(ctx context.Context, topic string, partitionID int) ([]string, error) {
	key := fmt.Sprintf("topics/%s/partitions/%d", topic, partitionID)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, fmt.Errorf("获取分区信息失败: %w", err)
	}

	if pair == nil {
		return nil, fmt.Errorf("分区不存在")
	}

	var partition PartitionInfo
	if err := json.Unmarshal(pair.Value, &partition); err != nil {
		return nil, fmt.Errorf("解析分区信息失败: %w", err)
	}

	return partition.Replicas, nil
}

// UpdatePartition 更新分区信息
func (c *ConsulCoordinator) UpdatePartition(ctx context.Context, partition *PartitionInfo) error {
	key := fmt.Sprintf("topics/%s/partitions/%d", partition.Topic, partition.ID)
	value, err := json.Marshal(partition)
	if err != nil {
		return fmt.Errorf("序列化分区信息失败: %w", err)
	}

	p := &api.KVPair{
		Key:   key,
		Value: value,
	}

	_, err = c.client.KV().Put(p, nil)
	if err != nil {
		return fmt.Errorf("更新分区信息失败: %w", err)
	}

	return nil
}

// DeletePartition 删除分区
func (c *ConsulCoordinator) DeletePartition(ctx context.Context, topic string, partitionID int) error {
	key := fmt.Sprintf("lmq/partitions/%s/%d", topic, partitionID)
	_, err := c.client.KV().Delete(key, nil)
	return err
}

// WatchPartition 监听分区变化
func (c *ConsulCoordinator) WatchPartition(ctx context.Context, topic string, partitionID int) (<-chan *PartitionInfo, error) {
	ch := make(chan *PartitionInfo, 1)
	key := fmt.Sprintf("lmq/partitions/%s/%d", topic, partitionID)

	go func() {
		defer close(ch)

		var lastIndex uint64
		for {
			select {
			case <-ctx.Done():
				return
			default:
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
		}
	}()

	return ch, nil
}

// CreateTopic 创建主题
func (c *ConsulCoordinator) CreateTopic(ctx context.Context, topic string, partitionCount int) error {
	// 检查主题是否已存在
	exists, err := c.TopicExists(ctx, topic)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("主题已存在: %s", topic)
	}

	// 创建主题配置
	topicConfig := map[string]interface{}{
		"partitions": partitionCount,
		"created_at": time.Now().Unix(),
	}

	// 将配置写入Consul
	data, err := json.Marshal(topicConfig)
	if err != nil {
		return fmt.Errorf("序列化主题配置失败: %w", err)
	}

	key := path.Join("topics", topic)
	p := &api.KVPair{
		Key:   key,
		Value: data,
	}

	_, err = c.client.KV().Put(p, nil)
	if err != nil {
		return fmt.Errorf("写入主题配置失败: %w", err)
	}

	return nil
}

// GetTopic 获取主题信息
func (c *ConsulCoordinator) GetTopic(ctx context.Context, topic string) (map[string]interface{}, error) {
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

// GetTopics 获取所有主题
func (c *ConsulCoordinator) GetTopics(ctx context.Context) ([]string, error) {
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
func (c *ConsulCoordinator) DeleteTopic(ctx context.Context, topic string) error {
	// 检查主题是否存在
	exists, err := c.TopicExists(ctx, topic)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("主题不存在: %s", topic)
	}

	// 删除主题配置
	key := path.Join("topics", topic)
	_, err = c.client.KV().Delete(key, nil)
	if err != nil {
		return fmt.Errorf("删除主题配置失败: %w", err)
	}

	return nil
}

// ElectPartitionLeader 为分区选举leader
func (c *ConsulCoordinator) ElectPartitionLeader(ctx context.Context, topic string, partitionID int, nodeID string) (bool, error) {
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
		// 获取分区信息
		partition, err := c.GetPartition(ctx, topic, partitionID)
		if err != nil {
			// 如果获取分区信息失败，释放锁
			c.client.KV().Release(&api.KVPair{
				Key:     key,
				Session: session,
			}, nil)
			return false, err
		}

		// 更新分区leader
		partition.Leader = nodeID
		if err := c.UpdatePartition(ctx, partition); err != nil {
			// 如果更新失败，释放锁
			c.client.KV().Release(&api.KVPair{
				Key:     key,
				Session: session,
			}, nil)
			return false, err
		}

		// 保存会话ID
		c.mu.Lock()
		c.sessions[key] = session
		c.mu.Unlock()
	}

	return acquired, nil
}

// ResignPartitionLeader 放弃分区leader身份
func (c *ConsulCoordinator) ResignPartitionLeader(ctx context.Context, topic string, partitionID int) error {
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
func (c *ConsulCoordinator) IsPartitionLeader(ctx context.Context, topic string, partitionID int, nodeID string) (bool, error) {
	partition, err := c.GetPartition(ctx, topic, partitionID)
	if err != nil {
		return false, err
	}

	if partition == nil {
		return false, fmt.Errorf("分区不存在: %s-%d", topic, partitionID)
	}

	return partition.Leader == nodeID, nil
}

// RegisterReplicaStatus 注册副本状态
func (c *ConsulCoordinator) RegisterReplicaStatus(ctx context.Context, topic string, partitionID int, nodeID string, offset int64) error {
	key := fmt.Sprintf("lmq/replica-status/%s/%d/%s", topic, partitionID, nodeID)
	_, err := c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: []byte(fmt.Sprintf("%d", offset)),
	}, nil)
	return err
}

// GetReplicaStatus 获取副本状态
func (c *ConsulCoordinator) GetReplicaStatus(ctx context.Context, topic string, partitionID int, nodeID string) (int64, error) {
	key := fmt.Sprintf("lmq/replica-status/%s/%d/%s", topic, partitionID, nodeID)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return 0, err
	}

	if pair == nil {
		return 0, nil
	}

	return strconv.ParseInt(string(pair.Value), 10, 64)
}

// GetAllReplicaStatus 获取所有副本状态
func (c *ConsulCoordinator) GetAllReplicaStatus(ctx context.Context, topic string, partitionID int) (map[string]int64, error) {
	prefix := fmt.Sprintf("lmq/replica-status/%s/%d/", topic, partitionID)
	pairs, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, err
	}

	result := make(map[string]int64)
	for _, pair := range pairs {
		nodeID := strings.TrimPrefix(pair.Key, prefix)
		offset, err := strconv.ParseInt(string(pair.Value), 10, 64)
		if err != nil {
			continue
		}
		result[nodeID] = offset
	}

	return result, nil
}

// GetTopicPartitionCount 获取主题的分区数
func (c *ConsulCoordinator) GetTopicPartitionCount(ctx context.Context, topic string) (int, error) {
	topicInfo, err := c.GetTopic(ctx, topic)
	if err != nil {
		return 0, err
	}

	if topicInfo == nil {
		return 0, fmt.Errorf("主题不存在: %s", topic)
	}

	partitions, ok := topicInfo["partitions"].(float64)
	if !ok {
		return 0, fmt.Errorf("主题分区数格式错误")
	}

	return int(partitions), nil
}

// WatchTopics 监听主题变化
func (c *ConsulCoordinator) WatchTopics(ctx context.Context) (<-chan []string, error) {
	ch := make(chan []string, 1)
	prefix := "lmq/topics/"

	go func() {
		defer close(ch)

		var lastIndex uint64
		for {
			select {
			case <-ctx.Done():
				return
			default:
				pairs, meta, err := c.client.KV().List(prefix, &api.QueryOptions{
					WaitIndex: lastIndex,
				})
				if err != nil {
					time.Sleep(time.Second)
					continue
				}

				if meta.LastIndex > lastIndex {
					lastIndex = meta.LastIndex
					topics := make([]string, 0, len(pairs))
					for _, pair := range pairs {
						topic := strings.TrimPrefix(pair.Key, prefix)
						topics = append(topics, topic)
					}
					ch <- topics
				}
			}
		}
	}()

	return ch, nil
}

// GetNodePartitions 获取节点的所有分区
func (c *ConsulCoordinator) GetNodePartitions(ctx context.Context, nodeID string) ([]*PartitionInfo, error) {
	// 获取所有主题
	topics, err := c.GetTopics(ctx)
	if err != nil {
		return nil, err
	}

	var partitions []*PartitionInfo
	for _, topic := range topics {
		// 获取主题的所有分区
		topicPartitions, err := c.GetPartitions(ctx, topic)
		if err != nil {
			continue
		}

		// 检查每个分区是否属于该节点
		for _, partition := range topicPartitions {
			for _, replica := range partition.Replicas {
				if replica == nodeID {
					partitions = append(partitions, partition)
					break
				}
			}
		}
	}

	return partitions, nil
}

// GetLeaderPartitions 获取节点作为leader的所有分区
func (c *ConsulCoordinator) GetLeaderPartitions(ctx context.Context, nodeID string) ([]*PartitionInfo, error) {
	// 获取所有主题
	topics, err := c.GetTopics(ctx)
	if err != nil {
		return nil, err
	}

	var partitions []*PartitionInfo
	for _, topic := range topics {
		// 获取主题的所有分区
		topicPartitions, err := c.GetPartitions(ctx, topic)
		if err != nil {
			continue
		}

		// 检查每个分区的leader是否是该节点
		for _, partition := range topicPartitions {
			if partition.Leader == nodeID {
				partitions = append(partitions, partition)
			}
		}
	}

	return partitions, nil
}

// GetFollowerPartitions 获取节点作为follower的所有分区
func (c *ConsulCoordinator) GetFollowerPartitions(ctx context.Context, nodeID string) ([]*PartitionInfo, error) {
	// 获取所有主题
	topics, err := c.GetTopics(ctx)
	if err != nil {
		return nil, err
	}

	var partitions []*PartitionInfo
	for _, topic := range topics {
		// 获取主题的所有分区
		topicPartitions, err := c.GetPartitions(ctx, topic)
		if err != nil {
			continue
		}

		// 检查每个分区的follower是否包含该节点
		for _, partition := range topicPartitions {
			for _, follower := range partition.Followers {
				if follower == nodeID {
					partitions = append(partitions, partition)
					break
				}
			}
		}
	}

	return partitions, nil
}

// RegisterConsumer 注册消费者
func (c *ConsulCoordinator) RegisterConsumer(ctx context.Context, groupID string, topics []string) error {
	group := &ConsumerGroupInfo{
		GroupID:    groupID,
		Topics:     topics,
		Offsets:    make(map[string]int64),
		Partitions: make(map[string]int),
	}

	data, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("序列化消费者组信息失败: %w", err)
	}

	key := fmt.Sprintf("lmq/consumer-groups/%s", groupID)
	_, err = c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: data,
	}, nil)

	return err
}

// UnregisterConsumer 注销消费者
func (c *ConsulCoordinator) UnregisterConsumer(ctx context.Context, groupID string) error {
	key := fmt.Sprintf("lmq/consumer-groups/%s", groupID)
	_, err := c.client.KV().Delete(key, nil)
	return err
}

// GetConsumerGroup 获取消费者组信息
func (c *ConsulCoordinator) GetConsumerGroup(ctx context.Context, groupID string) (*ConsumerGroupInfo, error) {
	key := fmt.Sprintf("lmq/consumer-groups/%s", groupID)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}

	if pair == nil {
		return nil, nil
	}

	var group ConsumerGroupInfo
	if err := json.Unmarshal(pair.Value, &group); err != nil {
		return nil, fmt.Errorf("解析消费者组信息失败: %w", err)
	}

	return &group, nil
}

// CommitOffset 提交消费位置
func (c *ConsulCoordinator) CommitOffset(ctx context.Context, groupID string, topic string, partition int, offset int64) error {
    key := fmt.Sprintf("lmq/consumer_groups/%s/offsets/%s/%d", groupID, topic, partition)
    _, err := c.client.KV().Put(&api.KVPair{
        Key:   key,
        Value: []byte(fmt.Sprintf("%d", offset)),
    }, nil)
    return err
}


// GetConsumerPartition 获取消费者分区
func (c *ConsulCoordinator) GetConsumerPartition(ctx context.Context, groupID string, topic string) (int, error) {
	group, err := c.GetConsumerGroup(ctx, groupID)
	if err != nil {
		return 0, err
	}

	if group == nil {
		return 0, fmt.Errorf("消费者组不存在: %s", groupID)
	}

	partition, ok := group.Partitions[topic]
	if !ok {
		return 0, nil
	}

	return partition, nil
}

// AddToISR 添加节点到 ISR 列表
func (c *ConsulCoordinator) AddToISR(ctx context.Context, topic string, partitionID int, nodeID string) error {
	partition, err := c.GetPartition(ctx, topic, partitionID)
	if err != nil {
		return err
	}

	// 检查节点是否已经在 ISR 中
	for _, id := range partition.ISR {
		if id == nodeID {
			return nil
		}
	}

	// 添加到 ISR
	partition.ISR = append(partition.ISR, nodeID)

	// 更新分区信息
	return c.UpdatePartition(ctx, partition)
}

// RemoveFromISR 从 ISR 列表中移除节点
func (c *ConsulCoordinator) RemoveFromISR(ctx context.Context, topic string, partitionID int, nodeID string) error {
	partition, err := c.GetPartition(ctx, topic, partitionID)
	if err != nil {
		return err
	}

	// 从 ISR 中移除节点
	newISR := make([]string, 0, len(partition.ISR))
	for _, id := range partition.ISR {
		if id != nodeID {
			newISR = append(newISR, id)
		}
	}
	partition.ISR = newISR

	// 更新分区信息
	return c.UpdatePartition(ctx, partition)
}

// GetISR 获取 ISR 列表
func (c *ConsulCoordinator) GetISR(ctx context.Context, topic string, partitionID int) ([]string, error) {
	partition, err := c.GetPartition(ctx, topic, partitionID)
	if err != nil {
		return nil, err
	}

	return partition.ISR, nil
}

// WatchISR 监听ISR列表变化
func (c *ConsulCoordinator) WatchISR(ctx context.Context, topic string, partitionID int) (<-chan []string, error) {
	ch := make(chan []string, 1)

	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				partition, err := c.GetPartition(ctx, topic, partitionID)
				if err != nil {
					time.Sleep(time.Second)
					continue
				}

				if partition != nil {
					ch <- partition.ISR
				}

				time.Sleep(time.Second)
			}
		}
	}()

	return ch, nil
}

// TopicExists 检查主题是否存在
func (c *ConsulCoordinator) TopicExists(ctx context.Context, topic string) (bool, error) {
	key := fmt.Sprintf("lmq/topics/%s", topic)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return false, err
	}
	return pair != nil, nil
}

// Lock 获取分布式锁
func (c *ConsulCoordinator) Lock(ctx context.Context, key string) error {
	acquired, err := c.AcquireLock(key, 15*time.Second)
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("failed to acquire lock: %s", key)
	}
	return nil
}

// Unlock 释放分布式锁
func (c *ConsulCoordinator) Unlock(ctx context.Context, key string) error {
	return c.ReleaseLock(key)
}

// UpdateISR 更新ISR列表
func (c *ConsulCoordinator) UpdateISR(ctx context.Context, topic string, partitionID int, isr []string) error {
	partition, err := c.GetPartition(ctx, topic, partitionID)
	if err != nil {
		return err
	}

	if partition == nil {
		return fmt.Errorf("分区不存在: %s-%d", topic, partitionID)
	}

	partition.ISR = isr
	return c.UpdatePartition(ctx, partition)
}

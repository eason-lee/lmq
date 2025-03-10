package coordinator

import (
	"context"
	"fmt"
	"log"

	"github.com/eason-lee/lmq/pkg/store"
)

// PartitionManager 分区管理器，负责分区的创建、分配和重平衡
type PartitionManager struct {
	coordinator Coordinator
	rebalancer  store.PartitionRebalancer
}

// NewPartitionManager 创建新的分区管理器
func NewPartitionManager(coordinator Coordinator, rebalancer store.PartitionRebalancer) *PartitionManager {
	// 如果没有指定重平衡器，使用默认的最小移动重平衡器
	if rebalancer == nil {
		rebalancer = store.DefaultRebalancer
	}

	return &PartitionManager{
		coordinator: coordinator,
		rebalancer:  rebalancer,
	}
}

// CreateTopicPartitions 创建主题分区并分配副本
func (pm *PartitionManager) CreateTopicPartitions(ctx context.Context, topic string, partitionCount int, replicationFactor int) error {
	// 获取可用的Broker列表
	services, err := pm.coordinator.DiscoverService(ctx, "lmq-broker")
	if err != nil {
		return fmt.Errorf("获取可用Broker失败: %w", err)
	}

	// 提取Broker ID列表
	brokers := make([]string, 0, len(services))
	for _, service := range services {
		brokers = append(brokers, service.ID)
	}

	// 检查副本因子是否合法
	if replicationFactor > len(brokers) {
		return fmt.Errorf("复制因子(%d)大于可用Broker数量(%d)", replicationFactor, len(brokers))
	}

	// 使用重平衡器生成分区分配方案
	log.Printf("使用%s重平衡器为主题%s创建%d个分区，复制因子为%d", pm.rebalancer.Name(), topic, partitionCount, replicationFactor)
	assignments, err := pm.rebalancer.Rebalance(topic, partitionCount, brokers, replicationFactor, nil)
	if err != nil {
		return fmt.Errorf("生成分区分配方案失败: %w", err)
	}

	// 创建分区并设置副本
	for _, assignment := range assignments {
		partition := &PartitionInfo{
			ID:       assignment.PartitionID,
			Topic:    assignment.Topic,
			Leader:   assignment.BrokerID,
			Replicas: assignment.Replicas,
			ISR:      assignment.Replicas, // 初始时ISR与副本列表相同
		}

		// 设置Followers
		followers := make([]string, 0, len(assignment.Replicas)-1)
		for _, replica := range assignment.Replicas {
			if replica != assignment.BrokerID {
				followers = append(followers, replica)
			}
		}
		partition.Followers = followers

		// 创建分区
		if err := pm.coordinator.CreatePartition(ctx, partition); err != nil {
			return fmt.Errorf("创建分区失败: %w", err)
		}
	}

	return nil
}

// RebalanceTopicPartitions 重新平衡主题的分区分配
func (pm *PartitionManager) RebalanceTopicPartitions(ctx context.Context, topic string, replicationFactor int) error {
	// 获取主题的分区数量
	partitionCount, err := pm.coordinator.GetTopicPartitionCount(ctx, topic)
	if err != nil {
		return fmt.Errorf("获取主题分区数量失败: %w", err)
	}

	// 获取当前的分区分配情况
	partitions, err := pm.coordinator.GetPartitions(ctx, topic)
	if err != nil {
		return fmt.Errorf("获取分区信息失败: %w", err)
	}

	// 转换为PartitionAssignment格式
	currentAssignments := make([]store.PartitionAssignment, 0, len(partitions))
	for _, p := range partitions {
		currentAssignments = append(currentAssignments, store.PartitionAssignment{
			Topic:       p.Topic,
			PartitionID: p.ID,
			BrokerID:    p.Leader,
			IsLeader:    true,
			Replicas:    p.Replicas,
		})
	}

	// 获取可用的Broker列表
	services, err := pm.coordinator.DiscoverService(ctx, "lmq-broker")
	if err != nil {
		return fmt.Errorf("获取可用Broker失败: %w", err)
	}

	// 提取Broker ID列表
	brokers := make([]string, 0, len(services))
	for _, service := range services {
		brokers = append(brokers, service.ID)
	}

	// 检查副本因子是否合法
	if replicationFactor > len(brokers) {
		return fmt.Errorf("复制因子(%d)大于可用Broker数量(%d)", replicationFactor, len(brokers))
	}

	// 使用重平衡器生成新的分区分配方案
	log.Printf("使用%s重平衡器为主题%s重新平衡%d个分区，复制因子为%d", pm.rebalancer.Name(), topic, partitionCount, replicationFactor)
	newAssignments, err := pm.rebalancer.Rebalance(topic, partitionCount, brokers, replicationFactor, currentAssignments)
	if err != nil {
		return fmt.Errorf("生成分区分配方案失败: %w", err)
	}

	// 更新分区分配
	for _, assignment := range newAssignments {
		partition, err := pm.coordinator.GetPartition(ctx, topic, assignment.PartitionID)
		if err != nil {
			return fmt.Errorf("获取分区信息失败: %w", err)
		}

		// 更新分区信息
		partition.Leader = assignment.BrokerID
		partition.Replicas = assignment.Replicas

		// 设置Followers
		followers := make([]string, 0, len(assignment.Replicas)-1)
		for _, replica := range assignment.Replicas {
			if replica != assignment.BrokerID {
				followers = append(followers, replica)
			}
		}
		partition.Followers = followers

		// 更新ISR（保留原有的ISR中仍然在副本列表中的节点）
		newISR := make([]string, 0, len(assignment.Replicas))
		newISR = append(newISR, assignment.BrokerID) // Leader总是在ISR中

		for _, nodeID := range partition.ISR {
			if nodeID != assignment.BrokerID { // 避免重复添加Leader
				// 检查节点是否仍在副本列表中
				for _, replica := range assignment.Replicas {
					if nodeID == replica {
						newISR = append(newISR, nodeID)
						break
					}
				}
			}
		}
		partition.ISR = newISR

		// 更新分区
		if err := pm.coordinator.UpdatePartition(ctx, partition); err != nil {
			return fmt.Errorf("更新分区失败: %w", err)
		}
	}

	return nil
}

// AddBrokerAndRebalance 添加新的Broker并重新平衡分区
func (pm *PartitionManager) AddBrokerAndRebalance(ctx context.Context, brokerID string, replicationFactor int) error {
	// 获取所有主题
	topics, err := pm.coordinator.GetTopics(ctx)
	if err != nil {
		return fmt.Errorf("获取主题列表失败: %w", err)
	}

	// 为每个主题重新平衡分区
	for _, topic := range topics {
		if err := pm.RebalanceTopicPartitions(ctx, topic, replicationFactor); err != nil {
			log.Printf("重新平衡主题%s的分区失败: %v", topic, err)
			// 继续处理其他主题
		}
	}

	return nil
}

// RemoveBrokerAndRebalance 移除Broker并重新平衡分区
func (pm *PartitionManager) RemoveBrokerAndRebalance(ctx context.Context, brokerID string, replicationFactor int) error {
	// 获取所有主题
	topics, err := pm.coordinator.GetTopics(ctx)
	if err != nil {
		return fmt.Errorf("获取主题列表失败: %w", err)
	}

	// 为每个主题重新平衡分区
	for _, topic := range topics {
		if err := pm.RebalanceTopicPartitions(ctx, topic, replicationFactor); err != nil {
			log.Printf("重新平衡主题%s的分区失败: %v", topic, err)
			// 继续处理其他主题
		}
	}

	return nil
}
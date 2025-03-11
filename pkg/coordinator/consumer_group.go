package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
)

// GetConsumersInGroup 获取消费者组中的所有消费者
func (c *ConsulCoordinator) GetConsumersInGroup(ctx context.Context, groupID string) ([]string, error) {
	prefix := fmt.Sprintf("lmq/consumer_groups/%s/members/", groupID)
	pairs, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, fmt.Errorf("获取消费者组成员失败: %w", err)
	}

	consumers := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		// 从键中提取消费者ID
		parts := strings.Split(pair.Key, "/")
		if len(parts) > 0 {
			consumerID := parts[len(parts)-1]
			consumers = append(consumers, consumerID)
		}
	}

	return consumers, nil
}

// GetTopicPartitions 获取主题的所有分区
func (c *ConsulCoordinator) GetTopicPartitions(ctx context.Context, topic string) ([]*PartitionInfo, error) {
	// 这个方法与GetPartitions功能相同，但保持接口一致性
	prefix := fmt.Sprintf("lmq/topics/%s/partitions/", topic)
	pairs, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, fmt.Errorf("获取主题分区失败: %w", err)
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

// AssignPartitionToConsumer 将分区分配给消费者
func (c *ConsulCoordinator) AssignPartitionToConsumer(ctx context.Context, topic string, partitionID int, groupID string, consumerID string) error {
	// 创建分区分配记录
	key := fmt.Sprintf("lmq/consumer_groups/%s/assignments/%s/%s/%d", groupID, consumerID, topic, partitionID)
	_, err := c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: []byte(strconv.Itoa(partitionID)),
	}, nil)

	if err != nil {
		return fmt.Errorf("分配分区失败: %w", err)
	}

	// 更新消费者组的分区映射
	groupKey := fmt.Sprintf("lmq/consumer_groups/%s/partitions/%s/%d", groupID, topic, partitionID)
	_, err = c.client.KV().Put(&api.KVPair{
		Key:   groupKey,
		Value: []byte(consumerID),
	}, nil)

	if err != nil {
		return fmt.Errorf("更新消费者组分区映射失败: %w", err)
	}

	return nil
}

// TriggerGroupRebalance 触发消费者组再平衡
func (c *ConsulCoordinator) TriggerGroupRebalance(ctx context.Context, groupID string) error {
	// 创建再平衡事件
	key := fmt.Sprintf("lmq/consumer_groups/%s/rebalance", groupID)
	_, err := c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: []byte(fmt.Sprintf("%d", time.Now().UnixNano())),
	}, nil)

	if err != nil {
		return fmt.Errorf("触发消费者组再平衡失败: %w", err)
	}

	return nil
}

// GetConsumerAssignedPartitions 获取分配给消费者的分区
func (c *ConsulCoordinator) GetConsumerAssignedPartitions(ctx context.Context, groupID string, consumerID string, topic string) ([]int, error) {
	prefix := fmt.Sprintf("lmq/consumer_groups/%s/assignments/%s/%s/", groupID, consumerID, topic)
	pairs, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, fmt.Errorf("获取消费者分配的分区失败: %w", err)
	}

	partitions := make([]int, 0, len(pairs))
	for _, pair := range pairs {
		// 从键中提取分区ID
		parts := strings.Split(pair.Key, "/")
		if len(parts) > 0 {
			partIDStr := parts[len(parts)-1]
			partID, err := strconv.Atoi(partIDStr)
			if err != nil {
				continue
			}
			partitions = append(partitions, partID)
		}
	}

	return partitions, nil
}

// GetConsumerOffset 获取消费者组在特定主题和分区的消费位置
func (c *ConsulCoordinator) GetConsumerOffset(ctx context.Context, groupID string, topic string, partition int) (int64, error) {
	key := fmt.Sprintf("lmq/consumer_groups/%s/offsets/%s/%d", groupID, topic, partition)
	pair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return 0, fmt.Errorf("获取消费位置失败: %w", err)
	}

	if pair == nil {
		// 如果没有记录，返回0表示从头开始消费
		return 0, nil
	}

	offset, err := strconv.ParseInt(string(pair.Value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("解析消费位置失败: %w", err)
	}

	return offset, nil
}
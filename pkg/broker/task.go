package broker

import (
	"log"
	"time"

	"github.com/eason-lee/lmq/pkg/store"
)

// StartCleanupTask 启动定期清理过期段的任务
func (b *Broker) StartCleanupTask(interval time.Duration, policy store.CleanupPolicy) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			b.cleanupSegments(policy)
		}
	}()
}

// cleanupSegments 使用指定的清理策略清理所有主题和分区的段
func (b *Broker) cleanupSegments(policy store.CleanupPolicy) {
	// 获取所有主题
	topics := b.getTopics()
	
	for _, topic := range topics {
		// 获取主题的所有分区
		partitions := b.getPartitions(topic)
		
		for _, partition := range partitions {
			if err := b.store.CleanupSegments(topic, partition, policy); err != nil {
				log.Printf("清理段失败: 主题=%s, 分区=%d, 策略=%s, 错误=%v", 
					topic, partition, policy.Name(), err)
			} else {
				log.Printf("成功清理主题 %s 分区 %d 的段，使用策略: %s", 
					topic, partition, policy.Name())
			}
		}
	}
}

// getTopics 获取所有主题
func (b *Broker) getTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	
	topics := make([]string, 0, len(b.subscribers))
	for topic := range b.subscribers {
		topics = append(topics, topic)
	}
	return topics
}

// getPartitions 获取主题的所有分区
func (b *Broker) getPartitions(topic string) []int {
	return b.store.GetPartitions(topic)
}

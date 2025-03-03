package broker

import (
	"log"
	"time"
)

// StartCleanupTask 启动定期清理过期段的任务
func (b *Broker) StartCleanupTask(interval time.Duration, retention time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			b.cleanupExpiredSegments(retention)
		}
	}()
}

// cleanupExpiredSegments 清理所有主题和分区的过期段
func (b *Broker) cleanupExpiredSegments(retention time.Duration) {
	// 获取所有主题
	topics := b.getTopics()
	
	for _, topic := range topics {
		// 获取主题的所有分区
		partitions := b.getPartitions(topic)
		
		for _, partition := range partitions {
			if err := b.store.CleanupSegments(topic, partition, retention); err != nil {
				log.Printf("清理过期段失败: 主题=%s, 分区=%d, 错误=%v", topic, partition, err)
			} else {
				log.Printf("成功清理主题 %s 分区 %d 的过期段", topic, partition)
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
	// 这里我们需要从 FileStore 中获取分区信息
	// 由于 FileStore 没有直接提供获取分区列表的方法，我们需要添加一个
	return b.store.GetPartitions(topic)
}

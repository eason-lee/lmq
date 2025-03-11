package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/eason-lee/lmq/proto"
)

const (
	// 默认段大小 - 64MB
	DefaultSegmentSize = 64 * 1024 * 1024
	// 索引文件后缀
	IndexFileSuffix = ".index"
	// 数据文件后缀
	DataFileSuffix = ".log"
)

// FileStore 文件存储实现
type FileStore struct {
	dataDir     string
	partitions  map[string]map[int]*Partition
	mu          sync.RWMutex
	segmentSize int64
}

// NewFileStore 创建新的文件存储实例
func NewFileStore(dataDir string) (*FileStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("创建数据目录失败: %w", err)
	}

	fs := &FileStore{
		dataDir:     dataDir,
		partitions:  make(map[string]map[int]*Partition),
		segmentSize: DefaultSegmentSize,
	}

	// 加载已存在的分区
	if err := fs.loadPartitions(); err != nil {
		return nil, err
	}

	return fs, nil
}

// loadPartitions 加载已存在的分区
func (fs *FileStore) loadPartitions() error {
	// 遍历主题目录
	topics, err := os.ReadDir(fs.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("读取数据目录失败: %w", err)
	}

	for _, topic := range topics {
		if !topic.IsDir() {
			continue
		}

		// 遍历分区目录
		partitionDirs, err := os.ReadDir(filepath.Join(fs.dataDir, topic.Name()))
		if err != nil {
			return fmt.Errorf("读取主题目录失败: %w", err)
		}

		for _, partitionDir := range partitionDirs {
			if !partitionDir.IsDir() {
				continue
			}

			// 解析分区ID
			partitionID := 0
			fmt.Sscanf(partitionDir.Name(), "%d", &partitionID)

			// 加载分区
			if err := fs.loadPartition(topic.Name(), partitionID); err != nil {
				return err
			}
		}
	}

	return nil
}

// loadPartition 加载分区
func (fs *FileStore) loadPartition(topic string, partitionID int) error {
	partitionDir := filepath.Join(fs.dataDir, topic, fmt.Sprintf("%d", partitionID))

	// 创建分区实例
	partition, err := NewPartition(partitionDir)
	if err != nil {
		return err
	}

	// 保存分区实例
	if _, ok := fs.partitions[topic]; !ok {
		fs.partitions[topic] = make(map[int]*Partition)
	}
	fs.partitions[topic][partitionID] = partition

	return nil
}

// Write 写入消息到指定分区
func (fs *FileStore) Write(topic string, partition int, messages []*pb.Message) error {
	fs.mu.RLock()
	p, ok := fs.partitions[topic][partition]
	fs.mu.RUnlock()

	if !ok {
		// 如果分区不存在，尝试创建
		if err := fs.CreatePartition(topic, partition); err != nil {
			return err
		}

		fs.mu.RLock()
		p = fs.partitions[topic][partition]
		fs.mu.RUnlock()
	}

	return p.Write(messages)
}

// Read 从指定分区读取消息
func (fs *FileStore) Read(topic string, partition int, offset int64, maxMessages int) ([]*pb.Message, error) {
	fs.mu.RLock()
	p, ok := fs.partitions[topic][partition]
	fs.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("分区不存在: %s-%d", topic, partition)
	}

	return p.Read(offset, maxMessages)
}

// GetOffset 获取指定消息的偏移量
func (fs *FileStore) GetOffset(topic string, messageID string) (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// 遍历所有分区查找消息
	if partitions, ok := fs.partitions[topic]; ok {
		for _, p := range partitions {
			offset, err := p.GetOffset(messageID)
			if err == nil {
				return offset, nil
			}
		}
	}

	return 0, fmt.Errorf("消息不存在: %s-%s", topic, messageID)
}

// Close 关闭存储
func (fs *FileStore) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for _, topicPartitions := range fs.partitions {
		for _, partition := range topicPartitions {
			if err := partition.Close(); err != nil {
				return err
			}
		}
	}

	return nil
}

// CreatePartition 创建分区
func (fs *FileStore) CreatePartition(topic string, partID int) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// 检查分区是否已存在
	if _, ok := fs.partitions[topic]; ok {
		if _, ok := fs.partitions[topic][partID]; ok {
			return fmt.Errorf("分区已存在: %s-%d", topic, partID)
		}
	}

	// 创建分区目录
	partitionDir := filepath.Join(fs.dataDir, topic, fmt.Sprintf("%d", partID))
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		return fmt.Errorf("创建分区目录失败: %w", err)
	}

	// 创建分区实例
	partition, err := NewPartition(partitionDir)
	if err != nil {
		return fmt.Errorf("创建分区实例失败: %w", err)
	}

	// 保存分区实例
	if _, ok := fs.partitions[topic]; !ok {
		fs.partitions[topic] = make(map[int]*Partition)
	}
	fs.partitions[topic][partID] = partition

	return nil
}



// GetMessages 获取指定主题和分区的所有消息
func (fs *FileStore) GetMessages(topic string) ([]*pb.Message, error) {
	fs.mu.RLock()
	partitions, ok := fs.partitions[topic]
	fs.mu.RUnlock()

	if !ok {
		return nil, nil // 主题不存在，返回空列表
	}

	var allMessages []*pb.Message
	for partID := range partitions {
		messages, err := fs.Read(topic, partID, 0, 1000) // 读取最多1000条消息
		if err != nil {
			return nil, err
		}
		allMessages = append(allMessages, messages...)
	}

	return allMessages, nil
}

// GetTopics 获取所有主题
func (fs *FileStore) GetTopics() []string {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	topics := make([]string, 0, len(fs.partitions))
	for topic := range fs.partitions {
		topics = append(topics, topic)
	}
	return topics
}

// GetPartitions 获取主题的所有分区
func (fs *FileStore) GetPartitions(topic string) []int {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if partitions, ok := fs.partitions[topic]; ok {
		result := make([]int, 0, len(partitions))
		for partID := range partitions {
			result = append(result, partID)
		}
		return result
	}

	return []int{}
}

// GetLatestOffset 获取指定分区的最新偏移量
func (fs *FileStore) GetLatestOffset(topic string, partition int) (int64, error) {
	fs.mu.RLock()
	p, ok := fs.partitions[topic][partition]
	fs.mu.RUnlock()

	if !ok {
		return 0, fmt.Errorf("分区不存在: %s-%d", topic, partition)
	}

	return p.GetLatestOffset()
}

func (fs *FileStore) mustGetPartition(topic string, partition int) (*Partition, error) {
	fs.mu.RLock()
	p, ok := fs.partitions[topic][partition]
	fs.mu.RUnlock()
	if!ok {
		return nil, fmt.Errorf("分区不存在: %s-%d", topic, partition)
	}
	return p, nil
}

// CleanupSegments 使用指定的清理策略清理段
func (fs *FileStore) CleanupSegments(topic string, partitionID int, policy CleanupPolicy) error {
	p, err := fs.mustGetPartition(topic, partitionID)
	if err!= nil {
		return err
	}

	// 应用清理策略
	return p.ApplyCleanupPolicy(policy)
}

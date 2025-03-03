package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/protocol"
)

const (
	// 默认段大小 - 64MB
	DefaultSegmentSize = 64 * 1024 * 1024
	// 索引文件后缀
	IndexFileSuffix = ".index"
	// 数据文件后缀
	DataFileSuffix = ".log"
)

// FileStore 实现基于文件的消息存储
type FileStore struct {
	baseDir     string
	mu          sync.RWMutex
	partitions  map[string]map[int]*Partition // topic -> partition -> Partition
	segmentSize int64                         // 每个段的最大大小
}

// NewFileStore 创建一个新的文件存储
func NewFileStore(baseDir string) (*FileStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("创建存储目录失败: %w", err)
	}

	fs := &FileStore{
		baseDir:     baseDir,
		partitions:  make(map[string]map[int]*Partition),
		segmentSize: DefaultSegmentSize,
	}

	// 加载现有分区
	if err := fs.loadPartitions(); err != nil {
		return nil, err
	}

	return fs, nil
}

// 加载现有分区
func (fs *FileStore) loadPartitions() error {
	// 遍历主题目录
	topics, err := os.ReadDir(fs.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("读取存储目录失败: %w", err)
	}

	for _, topicDir := range topics {
		if !topicDir.IsDir() {
			continue
		}
		topic := topicDir.Name()

		// 遍历分区目录
		partitionDirs, err := os.ReadDir(filepath.Join(fs.baseDir, topic))
		if err != nil {
			return fmt.Errorf("读取主题目录失败: %w", err)
		}

		for _, partDir := range partitionDirs {
			if !partDir.IsDir() || !strings.HasPrefix(partDir.Name(), "partition-") {
				continue
			}

			// 解析分区ID
			partIDStr := strings.TrimPrefix(partDir.Name(), "partition-")
			partID, err := strconv.Atoi(partIDStr)
			if err != nil {
				continue
			}

			// 加载分区
			if err := fs.loadPartition(topic, partID); err != nil {
				return err
			}
		}
	}

	return nil
}

// CreatePartition 创建新的分区
func (fs *FileStore) CreatePartition(topic string, partition int) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// 检查分区是否已存在
	if _, ok := fs.partitions[topic]; ok {
		if _, ok := fs.partitions[topic][partition]; ok {
			return nil // 分区已存在
		}
	} else {
		fs.partitions[topic] = make(map[int]*Partition)
	}

	// 创建分区目录
	partDir := filepath.Join(fs.baseDir, topic, fmt.Sprintf("partition-%d", partition))
	if err := os.MkdirAll(partDir, 0755); err != nil {
		return fmt.Errorf("创建分区目录失败: %w", err)
	}

	// 创建分区对象
	p := &Partition{
		topic:    topic,
		id:       partition,
		dir:      partDir,
		segments: make([]*Segment, 0),
	}

	// 创建初始段
	segment, err := p.createSegment(0, fs.segmentSize)
	if err != nil {
		return err
	}

	p.segments = append(p.segments, segment)
	p.activeSegment = segment
	fs.partitions[topic][partition] = p

	return nil
}

// 加载分区
func (fs *FileStore) loadPartition(topic string, partID int) error {
	partDir := filepath.Join(fs.baseDir, topic, fmt.Sprintf("partition-%d", partID))

	// 创建分区对象
	partition := &Partition{
		topic:    topic,
		id:       partID,
		dir:      partDir,
		segments: make([]*Segment, 0),
	}

	// 加载段文件
	files, err := os.ReadDir(partDir)
	if err != nil {
		return fmt.Errorf("读取分区目录失败: %w", err)
	}

	// 查找所有数据文件
	var baseOffsets []int64
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), DataFileSuffix) {
			// 解析基础偏移量
			baseOffsetStr := strings.TrimSuffix(file.Name(), DataFileSuffix)
			baseOffset, err := strconv.ParseInt(baseOffsetStr, 10, 64)
			if err != nil {
				continue
			}
			baseOffsets = append(baseOffsets, baseOffset)
		}
	}

	// 按基础偏移量排序
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// 加载每个段
	for _, baseOffset := range baseOffsets {
		segment, err := partition.loadSegment(baseOffset, fs.segmentSize)
		if err != nil {
			return err
		}
		partition.segments = append(partition.segments, segment)
	}

	// 如果没有段，创建初始段
	if len(partition.segments) == 0 {
		segment, err := partition.createSegment(0, fs.segmentSize)
		if err != nil {
			return err
		}
		partition.segments = append(partition.segments, segment)
	}

	// 设置活跃段
	partition.activeSegment = partition.segments[len(partition.segments)-1]

	// 保存分区
	fs.mu.Lock()
	if _, ok := fs.partitions[topic]; !ok {
		fs.partitions[topic] = make(map[int]*Partition)
	}
	fs.partitions[topic][partID] = partition
	fs.mu.Unlock()

	return nil
}

// Write 写入消息到指定分区
func (fs *FileStore) Write(topic string, partition int, messages []*protocol.Message) error {
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
func (fs *FileStore) Read(topic string, partition int, offset int64, count int) ([]*protocol.Message, error) {
	fs.mu.RLock()
	p, ok := fs.partitions[topic][partition]
	fs.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("分区不存在: %s-%d", topic, partition)
	}

	return p.Read(offset, count)
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

// CleanupSegments 清理过期的段
func (fs *FileStore) CleanupSegments(topic string, partition int, retention time.Duration) error {
	fs.mu.RLock()
	p, ok := fs.partitions[topic][partition]
	fs.mu.RUnlock()

	if !ok {
		return fmt.Errorf("分区不存在: %s-%d", topic, partition)
	}

	return p.CleanupSegments(retention)
}

// GetMessages 获取指定主题和分区的所有消息
func (fs *FileStore) GetMessages(topic string) ([]*protocol.Message, error) {
	fs.mu.RLock()
	partitions, ok := fs.partitions[topic]
	fs.mu.RUnlock()

	if !ok {
		return nil, nil // 主题不存在，返回空列表
	}

	var allMessages []*protocol.Message
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

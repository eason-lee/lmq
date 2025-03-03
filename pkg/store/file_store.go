package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
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

// Partition 表示一个分区
type Partition struct {
	topic        string
	id           int
	dir          string
	segments     []*Segment // 按基础偏移量排序的段列表
	activeSegment *Segment  // 当前活跃的段
	mu           sync.RWMutex
}

// Segment 表示一个日志段
type Segment struct {
	baseOffset int64     // 段的基础偏移量
	nextOffset int64     // 下一条消息的偏移量
	size       int64     // 当前段大小
	maxSize    int64     // 段的最大大小
	dataFile   *os.File  // 数据文件
	indexFile  *os.File  // 索引文件
	mu         sync.Mutex // 段级别的锁
}

// MessageIndex 消息索引记录
type MessageIndex struct {
	Offset    int64 // 消息偏移量
	Position  int64 // 在数据文件中的位置
	Size      int32 // 消息大小
	Timestamp int64 // 消息时间戳
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

// Write 写入消息到分区
func (p *Partition) Write(messages []*protocol.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, msg := range messages {
		// 检查当前段是否已满
		if p.activeSegment.size >= p.activeSegment.maxSize {
			// 创建新段
			newSegment, err := p.createSegment(p.activeSegment.nextOffset, p.activeSegment.maxSize)
			if err != nil {
				return err
			}
			p.segments = append(p.segments, newSegment)
			p.activeSegment = newSegment
		}

		// 写入消息到活跃段
		if err := p.activeSegment.Write(msg); err != nil {
			return err
		}
	}

	return nil
}

// Write 写入消息到段
func (s *Segment) Write(msg *protocol.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 序列化消息
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}

	// 获取当前文件位置
	position, err := s.dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("获取文件位置失败: %w", err)
	}

	// 写入消息数据
	if _, err := s.dataFile.Write(data); err != nil {
		return fmt.Errorf("写入消息数据失败: %w", err)
	}

	// 写入索引
	index := MessageIndex{
		Offset:    s.nextOffset,
		Position:  position,
		Size:      int32(len(data)),
		Timestamp: msg.Timestamp,
	}

	if err := s.writeIndex(&index); err != nil {
		return fmt.Errorf("写入索引失败: %w", err)
	}

	// 更新段状态
	s.size += int64(len(data))
	s.nextOffset++

	return nil
}

// 创建新的段
func (p *Partition) createSegment(baseOffset int64, maxSize int64) (*Segment, error) {
	dataPath := filepath.Join(p.dir, fmt.Sprintf("%020d%s", baseOffset, DataFileSuffix))
	indexPath := filepath.Join(p.dir, fmt.Sprintf("%020d%s", baseOffset, IndexFileSuffix))

	// 打开数据文件
	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("创建数据文件失败: %w", err)
	}

	// 打开索引文件
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("创建索引文件失败: %w", err)
	}

	// 获取文件大小
	dataInfo, err := dataFile.Stat()
	if err != nil {
		dataFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("获取文件信息失败: %w", err)
	}

	// 创建段对象
	segment := &Segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		size:       dataInfo.Size(),
		maxSize:    maxSize,
		dataFile:   dataFile,
		indexFile:  indexFile,
	}

	return segment, nil
}

// 加载现有段
func (p *Partition) loadSegment(baseOffset int64, maxSize int64) (*Segment, error) {
	dataPath := filepath.Join(p.dir, fmt.Sprintf("%020d%s", baseOffset, DataFileSuffix))
	indexPath := filepath.Join(p.dir, fmt.Sprintf("%020d%s", baseOffset, IndexFileSuffix))

	// 打开数据文件
	dataFile, err := os.OpenFile(dataPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开数据文件失败: %w", err)
	}

	// 打开索引文件
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("打开索引文件失败: %w", err)
	}

	// 获取文件大小
	dataInfo, err := dataFile.Stat()
	if err != nil {
		dataFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("获取文件信息失败: %w", err)
	}

	// 创建段对象
	segment := &Segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		size:       dataInfo.Size(),
		maxSize:    maxSize,
		dataFile:   dataFile,
		indexFile:  indexFile,
	}

	// 确定下一个偏移量
	indexInfo, err := indexFile.Stat()
	if err != nil {
		dataFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("获取索引文件信息失败: %w", err)
	}

	// 计算索引条目数
	indexCount := indexInfo.Size() / 28 // 每个索引条目28字节
	if indexCount > 0 {
		// 读取最后一个索引条目
		buf := make([]byte, 28)
		if _, err := indexFile.ReadAt(buf, (indexCount-1)*28); err != nil {
			dataFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("读取索引失败: %w", err)
		}

		lastOffset := int64(binary.BigEndian.Uint64(buf[0:8]))
		segment.nextOffset = lastOffset + 1
	}

	return segment, nil
}

// writeIndex 写入索引记录
func (s *Segment) writeIndex(index *MessageIndex) error {
	buf := make([]byte, 28) // 8 + 8 + 4 + 8 bytes
	binary.BigEndian.PutUint64(buf[0:8], uint64(index.Offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(index.Position))
	binary.BigEndian.PutUint32(buf[16:20], uint32(index.Size))
	binary.BigEndian.PutUint64(buf[20:28], uint64(index.Timestamp))

	if _, err := s.indexFile.Write(buf); err != nil {
		return err
	}

	return nil
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

// Read 从分区读取消息
func (p *Partition) Read(offset int64, count int) ([]*protocol.Message, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 找到包含指定偏移量的段
	var targetSegment *Segment
	for i := len(p.segments) - 1; i >= 0; i-- {
		if p.segments[i].baseOffset <= offset {
			targetSegment = p.segments[i]
			break
		}
	}

	if targetSegment == nil {
		return nil, fmt.Errorf("找不到包含偏移量 %d 的段", offset)
	}

	return targetSegment.Read(offset, count)
}

// Read 从段读取消息
func (s *Segment) Read(offset int64, count int) ([]*protocol.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查偏移量是否在段范围内
	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, fmt.Errorf("偏移量 %d 不在段范围内 [%d, %d)", offset, s.baseOffset, s.nextOffset)
	}

	// 读取索引
	indexes, err := s.readIndexes(offset, count)
	if err != nil {
		return nil, err
	}

	messages := make([]*protocol.Message, 0, len(indexes))
	for _, idx := range indexes {
		// 读取消息数据
		data := make([]byte, idx.Size)
		if _, err := s.dataFile.ReadAt(data, idx.Position); err != nil {
			return nil, fmt.Errorf("读取消息数据失败: %w", err)
		}

		var msg protocol.Message
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, fmt.Errorf("反序列化消息失败: %w", err)
		}

		messages = append(messages, &msg)
	}

	return messages, nil
}

// readIndexes 读取索引记录
func (s *Segment) readIndexes(offset int64, count int) ([]MessageIndex, error) {
	// 计算索引文件中的位置
	relativeOffset := offset - s.baseOffset
	position := relativeOffset * 28 // 每个索引条目28字节

	// 获取索引文件大小
	info, err := s.indexFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("获取索引文件信息失败: %w", err)
	}

	// 计算可读取的最大条目数
	maxEntries := (info.Size() - position) / 28
	if maxEntries <= 0 {
		return nil, nil
	}

	// 限制读取数量
	if count > int(maxEntries) {
		count = int(maxEntries)
	}

	// 读取索引数据
	buf := make([]byte, count*28)
	if _, err := s.indexFile.ReadAt(buf, position); err != nil {
		return nil, fmt.Errorf("读取索引数据失败: %w", err)
	}

	// 解析索引记录
	indexes := make([]MessageIndex, count)
	for i := 0; i < count; i++ {
		pos := i * 28
		indexes[i] = MessageIndex{
			Offset:    int64(binary.BigEndian.Uint64(buf[pos : pos+8])),
			Position:  int64(binary.BigEndian.Uint64(buf[pos+8 : pos+16])),
			Size:      int32(binary.BigEndian.Uint32(buf[pos+16 : pos+20])),
			Timestamp: int64(binary.BigEndian.Uint64(buf[pos+20 : pos+28])),
		}
	}

	return indexes, nil
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

// Close 关闭分区
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, segment := range p.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Close 关闭段
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.dataFile.Close(); err != nil {
		return err
	}

	if err := s.indexFile.Close(); err != nil {
		return err
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

// CleanupSegments 清理过期的段
func (p *Partition) CleanupSegments(retention time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.segments) <= 1 {
		// 至少保留一个段
		return nil
	}

	now := time.Now().UnixNano()
	cutoff := now - retention.Nanoseconds()

	// 找到可以删除的段
	var lastIndexToKeep int
	for i, segment := range p.segments {
		// 检查段的最后一条消息的时间戳
		lastIndex, err := segment.getLastMessageTimestamp()
		if err != nil {
			return err
		}

		if lastIndex > cutoff || i == len(p.segments)-1 {
			// 这个段包含未过期的消息或者是最后一个段，保留
			lastIndexToKeep = i
			break
		}
	}

	// 删除过期的段
	for i := 0; i < lastIndexToKeep; i++ {
		segment := p.segments[i]
		if err := segment.Close(); err != nil {
			return err
		}

		// 删除文件
		dataPath := filepath.Join(p.dir, fmt.Sprintf("%020d%s", segment.baseOffset, DataFileSuffix))
		indexPath := filepath.Join(p.dir, fmt.Sprintf("%020d%s", segment.baseOffset, IndexFileSuffix))

		if err := os.Remove(dataPath); err != nil {
			return fmt.Errorf("删除数据文件失败: %w", err)
		}

		if err := os.Remove(indexPath); err != nil {
			return fmt.Errorf("删除索引文件失败: %w", err)
		}
	}

	// 更新段列表
	p.segments = p.segments[lastIndexToKeep:]

	return nil
}

// getLastMessageTimestamp 获取段中最后一条消息的时间戳
func (s *Segment) getLastMessageTimestamp() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取索引文件大小
	info, err := s.indexFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("获取索引文件信息失败: %w", err)
	}

	// 如果没有消息，返回0
	if info.Size() == 0 {
		return 0, nil
	}

	// 读取最后一个索引条目
	buf := make([]byte, 28)
	if _, err := s.indexFile.ReadAt(buf, info.Size()-28); err != nil {
		return 0, fmt.Errorf("读取索引失败: %w", err)
	}

	// 解析时间戳
	timestamp := int64(binary.BigEndian.Uint64(buf[20:28]))
	return timestamp, nil
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

// Save 保存单条消息（兼容旧接口）
func (fs *FileStore) Save(topic string, msg *protocol.Message) error {
	// 默认使用分区0
	return fs.Write(topic, 0, []*protocol.Message{msg})
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

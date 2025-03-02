package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/eason-lee/lmq/pkg/protocol"
)

// FileStore 实现基于文件的消息存储
type FileStore struct {
	baseDir string
	mu      sync.RWMutex
	indexes map[string]map[int]*partitionIndex // topic -> partition -> index
}

type partitionIndex struct {
	offset    int64
	dataFile  *os.File
	indexFile *os.File
	mu        sync.RWMutex
}

// MessageIndex 消息索引记录
type MessageIndex struct {
	Offset    int64
	Position  int64
	Size      int32
	Timestamp int64
}

// NewFileStore 创建一个新的文件存储
func NewFileStore(baseDir string) (*FileStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("创建存储目录失败: %w", err)
	}

	return &FileStore{
		baseDir: baseDir,
		indexes: make(map[string]map[int]*partitionIndex),
	}, nil
}

// CreatePartition 创建新的分区
func (fs *FileStore) CreatePartition(topic string, partition int) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// 确保主题目录存在
	topicDir := filepath.Join(fs.baseDir, topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return fmt.Errorf("创建主题目录失败: %w", err)
	}

	// 创建分区目录
	partitionDir := filepath.Join(topicDir, fmt.Sprintf("partition-%d", partition))
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		return fmt.Errorf("创建分区目录失败: %w", err)
	}

	// 初始化分区索引
	if _, ok := fs.indexes[topic]; !ok {
		fs.indexes[topic] = make(map[int]*partitionIndex)
	}

	// 打开数据文件和索引文件
	dataFile, err := os.OpenFile(
		filepath.Join(partitionDir, "data.log"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return fmt.Errorf("打开数据文件失败: %w", err)
	}

	indexFile, err := os.OpenFile(
		filepath.Join(partitionDir, "index.log"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		dataFile.Close()
		return fmt.Errorf("打开索引文件失败: %w", err)
	}

	fs.indexes[topic][partition] = &partitionIndex{
		offset:    0,
		dataFile:  dataFile,
		indexFile: indexFile,
	}

	return nil
}

// Write 写入消息到指定分区
func (fs *FileStore) Write(topic string, partition int, messages []*protocol.Message) error {
	fs.mu.RLock()
	partIndex, ok := fs.indexes[topic][partition]
	fs.mu.RUnlock()

	if !ok {
		return fmt.Errorf("分区不存在: %s-%d", topic, partition)
	}

	partIndex.mu.Lock()
	defer partIndex.mu.Unlock()

	// 获取当前文件位置
	position, err := partIndex.dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("获取文件位置失败: %w", err)
	}

	// 批量写入消息
	for _, msg := range messages {
		data, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("序列化消息失败: %w", err)
		}

		// 写入消息数据
		if _, err := partIndex.dataFile.Write(data); err != nil {
			return fmt.Errorf("写入消息数据失败: %w", err)
		}

		// 写入索引
		index := MessageIndex{
			Offset:    partIndex.offset,
			Position:  position,
			Size:      int32(len(data)),
			Timestamp: msg.Timestamp,
		}

		if err := fs.writeIndex(partIndex.indexFile, &index); err != nil {
			return fmt.Errorf("写入索引失败: %w", err)
		}

		position += int64(len(data))
		partIndex.offset++
	}

	return nil
}

// Read 从指定分区读取消息
func (fs *FileStore) Read(topic string, partition int, offset int64, count int) ([]*protocol.Message, error) {
	fs.mu.RLock()
	partIndex, ok := fs.indexes[topic][partition]
	fs.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("分区不存在: %s-%d", topic, partition)
	}

	partIndex.mu.RLock()
	defer partIndex.mu.RUnlock()

	// 读取索引
	indexes, err := fs.readIndexes(partIndex.indexFile, offset, count)
	if err != nil {
		return nil, fmt.Errorf("读取索引失败: %w", err)
	}

	messages := make([]*protocol.Message, 0, len(indexes))
	for _, idx := range indexes {
		// 读取消息数据
		data := make([]byte, idx.Size)
		if _, err := partIndex.dataFile.ReadAt(data, idx.Position); err != nil {
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

// writeIndex 写入索引记录
func (fs *FileStore) writeIndex(file *os.File, index *MessageIndex) error {
	buf := make([]byte, 28) // 8 + 8 + 4 + 8 bytes
	binary.BigEndian.PutUint64(buf[0:8], uint64(index.Offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(index.Position))
	binary.BigEndian.PutUint32(buf[16:20], uint32(index.Size))
	binary.BigEndian.PutUint64(buf[20:28], uint64(index.Timestamp))

	_, err := file.Write(buf)
	return err
}

// readIndexes 读取索引记录
func (fs *FileStore) readIndexes(file *os.File, offset int64, count int) ([]MessageIndex, error) {
	// 定位到指定偏移量
	position := offset * 28 // 每个索引记录 28 字节
	if _, err := file.Seek(position, io.SeekStart); err != nil {
		return nil, err
	}

	indexes := make([]MessageIndex, 0, count)
	buf := make([]byte, 28)

	for i := 0; i < count; i++ {
		n, err := file.Read(buf)
		if err != nil {
			break // 文件结束或发生错误
		}
		if n != 28 {
			break // 不完整的记录
		}

		index := MessageIndex{
			Offset:    int64(binary.BigEndian.Uint64(buf[0:8])),
			Position:  int64(binary.BigEndian.Uint64(buf[8:16])),
			Size:      int32(binary.BigEndian.Uint32(buf[16:20])),
			Timestamp: int64(binary.BigEndian.Uint64(buf[20:28])),
		}
		indexes = append(indexes, index)
	}

	return indexes, nil
}

// Close 关闭存储
func (fs *FileStore) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var lastErr error
	for _, partitions := range fs.indexes {
		for _, idx := range partitions {
			if err := idx.dataFile.Close(); err != nil {
				lastErr = err
			}
			if err := idx.indexFile.Close(); err != nil {
				lastErr = err
			}
		}
	}

	return lastErr
}

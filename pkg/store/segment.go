package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/eason-lee/lmq/pkg/protocol"
)

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

// newSegment 创建新的段
func newSegment(dir string, baseOffset int64, maxSize int64) (*Segment, error) {
	dataPath := filepath.Join(dir, fmt.Sprintf("%020d%s", baseOffset, DataFileSuffix))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d%s", baseOffset, IndexFileSuffix))

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

	return &Segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		size:      dataInfo.Size(),
		maxSize:   maxSize,
		dataFile:  dataFile,
		indexFile: indexFile,
	}, nil
}

// Write 写入消息到段
func (s *Segment) Write(msg *protocol.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取当前文件位置
	position, err := s.dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("获取文件位置失败: %w", err)
	}

	// 使用二进制格式序列化消息
	data, err := serializeMessage(msg)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
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

		// 使用二进制格式反序列化消息
		msg, err := deserializeMessage(data)
		if err != nil {
			return nil, fmt.Errorf("反序列化消息失败: %w", err)
		}

		messages = append(messages, msg)
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
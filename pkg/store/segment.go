package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	pb "github.com/eason-lee/lmq/proto"
)

// 错误定义
var (
	ErrSegmentFull = fmt.Errorf("段已满")
)

// Segment 表示一个日志段
type Segment struct {
	baseOffset int64        // 段的基础偏移量
	nextOffset int64        // 下一条消息的偏移量
	size       int64        // 当前段大小
	maxSize    int64        // 段的最大大小
	dataFile   *os.File     // 数据文件
	indexFile  *os.File     // 索引文件
	mu         sync.RWMutex // 段级别的锁
	index      []MessageIndex
	dir        string
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
		size:       dataInfo.Size(),
		maxSize:    maxSize,
		dataFile:   dataFile,
		indexFile:  indexFile,
		dir:        dir,
	}, nil
}

// Write 写入消息到段
func (s *Segment) Write(msg *pb.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 序列化消息
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}

	// 添加消息长度前缀
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	// 组合长度和数据
	msgData := make([]byte, len(lenBuf)+len(data))
	copy(msgData[:4], lenBuf)
	copy(msgData[4:], data)

	// 检查段大小
	if s.size+int64(len(msgData)) > s.maxSize {
		return ErrSegmentFull
	}

	// 写入数据
	position, err := s.dataFile.Seek(0, os.SEEK_END)
	if err != nil {
		return fmt.Errorf("获取文件位置失败: %w", err)
	}

	if _, err := s.dataFile.Write(msgData); err != nil {
		return fmt.Errorf("写入数据失败: %w", err)
	}

	// 更新索引
	timestamp := time.Now().UnixNano()
	if msg.Timestamp != nil {
		timestamp = msg.Timestamp.AsTime().UnixNano()
	}

	index := MessageIndex{
		Offset:    s.baseOffset + int64(len(s.index)),
		Position:  position,
		Size:      int32(len(msgData)),
		Timestamp: timestamp,
	}
	s.index = append(s.index, index)
	s.size += int64(len(msgData))

	return nil
}

// Read 从段读取消息
func (s *Segment) Read(offset int64, count int) ([]*pb.Message, error) {
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

	messages := make([]*pb.Message, 0, len(indexes))
	for _, idx := range indexes {
		// 读取消息数据
		data := make([]byte, idx.Size)
		if _, err := s.dataFile.ReadAt(data, idx.Position); err != nil {
			return nil, fmt.Errorf("读取消息数据失败: %w", err)
		}

		// 读取消息长度
		if len(data) < 4 {
			return nil, fmt.Errorf("数据长度不足")
		}
		msgLen := binary.BigEndian.Uint32(data[:4])
		if len(data) < int(msgLen)+4 {
			return nil, fmt.Errorf("数据长度不匹配")
		}

		// 反序列化消息
		msg := &pb.Message{}
		if err := proto.Unmarshal(data[4:4+msgLen], msg); err != nil {
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

// GetLatestOffset 获取段的最新偏移量
func (s *Segment) GetLatestOffset() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 如果没有消息，返回基础偏移量
	if len(s.index) == 0 {
		return s.baseOffset, nil
	}

	// 返回最后一条消息的偏移量
	lastIndex := s.index[len(s.index)-1]
	return lastIndex.Offset, nil
}

// Delete 删除段文件
func (s *Segment) Delete() error {
	if err := os.Remove(filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, DataFileSuffix))); err != nil {
		return fmt.Errorf("删除数据文件失败: %w", err)
	}
	if err := os.Remove(filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, IndexFileSuffix))); err != nil {
		return fmt.Errorf("删除索引文件失败: %w", err)
	}
	return nil
}

// getLastModifiedTime 获取段的最后修改时间
func (s *Segment) getLastModifiedTime() (time.Time, error) {
	info, err := s.dataFile.Stat()
	if err != nil {
		return time.Time{}, fmt.Errorf("获取数据文件信息失败: %w", err)
	}
	return info.ModTime(), nil
}

// GetOffset 获取指定消息ID的偏移量
func (s *Segment) GetOffset(messageID string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 从索引中查找消息
	for i, entry := range s.index {
		// 读取消息
		message, err := s.readMessage(entry.Position, entry.Size)
		if err != nil {
			continue
		}

		// 检查消息ID
		if message.Id == messageID {
			return s.baseOffset + int64(i), nil
		}
	}

	return 0, fmt.Errorf("消息不存在: %s", messageID)
}

// readMessage 从指定位置读取消息
func (s *Segment) readMessage(position int64, size int32) (*pb.Message, error) {
	// 打开数据文件
	file := s.dataFile
	
	defer file.Close()

	// 定位到消息位置
	if _, err := file.Seek(position, 0); err != nil {
		return nil, err
	}

	// 读取消息数据
	data := make([]byte, size)
	if _, err := file.Read(data); err != nil {
		return nil, err
	}

	// 解析消息
	message := &pb.Message{}
	if err := proto.Unmarshal(data[4:], message); err != nil {
		return nil, err
	}

	return message, nil
}

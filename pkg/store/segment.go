package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/eason-lee/lmq/proto"
	"google.golang.org/protobuf/proto"
)

// 错误定义
var (
	ErrSegmentFull = fmt.Errorf("段已满")
)

// Segment 表示一个日志段
type Segment struct {
	baseOffset  int64        // 段的基础偏移量
	nextOffset  int64        // 下一条消息的偏移量
	size        int64        // 当前段大小
	maxSize     int64        // 段的最大大小
	dataFile    *os.File     // 数据文件
	indexFile   *os.File     // 索引文件
	mu          sync.RWMutex // 段级别的锁
	index       []MessageIndex
	dir         string
	ct          time.Time
	sparseIndex *SparseIndex // 稀疏索引
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
	sparseIndexPath := filepath.Join(dir, fmt.Sprintf("%020d.sparse", baseOffset))

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

	// 创建稀疏索引
	sparseIndex, err := NewSparseIndex(sparseIndexPath, DefaultSparseIndexConfig)
	if err != nil {
		dataFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("创建稀疏索引失败: %w", err)
	}

	return &Segment{
		baseOffset:  baseOffset,
		nextOffset:  baseOffset,
		size:        dataInfo.Size(),
		maxSize:     maxSize,
		dataFile:    dataFile,
		indexFile:   indexFile,
		dir:         dir,
		ct:          time.Now(),
		sparseIndex: sparseIndex,
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

	offset := s.baseOffset + int64(len(s.index))
	index := MessageIndex{
		Offset:    offset,
		Position:  position,
		Size:      int32(len(msgData)),
		Timestamp: timestamp,
	}
	s.index = append(s.index, index)
	s.size += int64(len(msgData))

	// 更新稀疏索引
	if err := s.sparseIndex.AddMessage(msg, offset, position); err != nil {
		return fmt.Errorf("更新稀疏索引失败: %w", err)
	}

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

	// 使用稀疏索引找到最近的位置
	position, err := s.sparseIndex.FindPosition(offset)
	if err != nil {
		return nil, fmt.Errorf("查找稀疏索引失败: %w", err)
	}

	// 从找到的位置开始读取消息
	messages := make([]*pb.Message, 0, count)
	currentPosition := position

	for len(messages) < count {
		// 读取消息长度
		lenBuf := make([]byte, 4)
		if _, err := s.dataFile.ReadAt(lenBuf, currentPosition); err != nil {
			break // 到达文件末尾
		}
		msgLen := binary.BigEndian.Uint32(lenBuf)

		// 读取消息数据
		data := make([]byte, msgLen+4)
		if _, err := s.dataFile.ReadAt(data, currentPosition); err != nil {
			break
		}

		// 反序列化消息
		msg := &pb.Message{}
		if err := proto.Unmarshal(data[4:], msg); err != nil {
			return nil, fmt.Errorf("反序列化消息失败: %w", err)
		}

		// 检查是否达到目标偏移量
		if currentPosition >= position {
			messages = append(messages, msg)
		}

		// 更新位置
		currentPosition += int64(len(data))
	}

	return messages, nil
}


// Close 关闭段
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 关闭稀疏索引
	if s.sparseIndex != nil {
		if err := s.sparseIndex.Close(); err != nil {
			return fmt.Errorf("关闭稀疏索引失败: %w", err)
		}
	}

	if err := s.dataFile.Close(); err != nil {
		return err
	}

	if err := s.indexFile.Close(); err != nil {
		return err
	}

	return nil
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

package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
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
	pageCache   *PageCache   // 页缓存
	asyncIO     *AsyncIO     // 异步I/O管理器
}

// MessageIndex 消息索引记录
type MessageIndex struct {
	Offset    int64 // 消息偏移量
	Position  int64 // 在数据文件中的位置
	Size      int32 // 消息大小
	Timestamp int64 // 消息时间戳
}

// NewSegment 创建新的段
func NewSegment(dir string, baseOffset int64, maxSize int64) (*Segment, error) {
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

	// 创建页缓存
	pageCache := NewPageCache(DefaultPageCacheConfig)

	// 创建异步I/O管理器
	asyncIO := NewAsyncIO(DefaultAsyncIOConfig, pageCache)

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
		pageCache:   pageCache,
		asyncIO:     asyncIO,
	}, nil
}

// Read 从段读取消息
func (s *Segment) Read(offset int64, count int) ([]*pb.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

	// 使用页缓存和异步I/O优化读取
	for len(messages) < count {
		// 尝试从页缓存读取消息长度
		cacheKey := fmt.Sprintf("%s:%d", s.dataFile.Name(), currentPosition)
		var lenBuf []byte
		var fromCache bool

		if s.pageCache != nil && s.pageCache.config.Enabled {
			lenBuf, fromCache = s.pageCache.Get(cacheKey)
		}

		// 如果缓存未命中，从文件读取
		if !fromCache {
			lenBuf = make([]byte, 4)

			// 使用异步I/O读取
			if s.asyncIO != nil && s.asyncIO.config.Enabled {
				// 创建同步通道
				ch := make(chan struct {
					data []byte
					err  error
				}, 1)

				// 异步读取
				s.asyncIO.AsyncRead(s.dataFile.Name(), currentPosition, 4, func(data []byte, err error) {
					ch <- struct {
						data []byte
						err  error
					}{data, err}
				})

				// 等待读取完成
				result := <-ch
				if result.err != nil {
					break // 到达文件末尾或读取错误
				}
				lenBuf = result.data
			} else {
				// 同步读取
				if _, err := s.dataFile.ReadAt(lenBuf, currentPosition); err != nil {
					break // 到达文件末尾
				}
			}

			// 更新页缓存
			if s.pageCache != nil && s.pageCache.config.Enabled {
				s.pageCache.Put(cacheKey, lenBuf, false)
			}
		} else if s.pageCache != nil {
			// 释放缓存引用
			defer s.pageCache.Release(cacheKey)
		}

		// 解析消息长度
		msgLen := binary.BigEndian.Uint32(lenBuf)

		// 使用页缓存和异步I/O读取消息数据
		var msgData []byte
		msgCacheKey := fmt.Sprintf("%s:%d:%d", s.dataFile.Name(), currentPosition, msgLen+4)
		var msgFromCache bool

		if s.pageCache != nil && s.pageCache.config.Enabled {
			msgData, msgFromCache = s.pageCache.Get(msgCacheKey)
		}

		if !msgFromCache {
			// 使用零拷贝方式读取消息数据
			msgData, err = s.ReadWithZeroCopy(currentPosition, int32(msgLen+4))
			if err != nil {
				return nil, fmt.Errorf("零拷贝读取消息失败: %w", err)
			}

			// 确保在函数返回前释放内存映射
			defer UnmapMemory(msgData)

			// 更新页缓存（如果消息大小适合缓存）
			if s.pageCache != nil && s.pageCache.config.Enabled && int(msgLen+4) <= s.pageCache.config.PageSize {
				// 创建副本以存储在缓存中
				cachedData := make([]byte, msgLen+4)
				copy(cachedData, msgData)
				s.pageCache.Put(msgCacheKey, cachedData, false)
			}
		} else if s.pageCache != nil {
			// 释放缓存引用
			defer s.pageCache.Release(msgCacheKey)
		}

		// 反序列化消息
		msg := &pb.Message{}
		if err := proto.Unmarshal(msgData[4:], msg); err != nil {
			return nil, fmt.Errorf("反序列化消息失败: %w", err)
		}

		// 检查是否达到目标偏移量
		if currentPosition >= position {
			messages = append(messages, msg)
		}

		// 更新位置
		currentPosition += int64(len(msgData))
	}

	return messages, nil
}

// Close 关闭段
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 关闭异步I/O管理器
	if s.asyncIO != nil {
		if err := s.asyncIO.Close(); err != nil {
			return fmt.Errorf("关闭异步I/O管理器失败: %w", err)
		}
	}

	// 关闭页缓存，刷新脏页
	if s.pageCache != nil && s.pageCache.config.Enabled {
		if err := s.pageCache.FlushDirtyPages(); err != nil {
			return fmt.Errorf("刷新页缓存失败: %w", err)
		}
		if err := s.pageCache.Clear(); err != nil {
			return fmt.Errorf("清空页缓存失败: %w", err)
		}
	}

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

// ReadWithZeroCopy 使用零拷贝方式读取数据
func (s *Segment) ReadWithZeroCopy(offset int64, size int32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 使用mmap实现零拷贝
	var data []byte
	var err error

	// 获取文件描述符
	fd := int(s.dataFile.Fd())

	// 使用syscall.Mmap进行内存映射
	data, err = syscall.Mmap(fd, offset, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("内存映射失败: %w", err)
	}

	// 注意：调用方需要在使用完毕后调用syscall.Munmap(data)释放映射
	return data, nil
}

// UnmapMemory 释放内存映射
func UnmapMemory(data []byte) error {
	if data == nil {
		return nil
	}
	return syscall.Munmap(data)
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
	// 使用零拷贝方式读取消息数据
	data, err := s.ReadWithZeroCopy(position, size)
	if err != nil {
		return nil, fmt.Errorf("零拷贝读取消息失败: %w", err)
	}

	// 确保在函数返回前释放内存映射
	defer UnmapMemory(data)

	// 解析消息
	message := &pb.Message{}
	if err := proto.Unmarshal(data[4:], message); err != nil {
		return nil, err
	}

	return message, nil
}

// WriteBatch 批量写入消息到段
func (s *Segment) WriteBatch(messages []*pb.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 预先计算总大小，以检查段容量
	totalSize := int64(0)
	msgDataList := make([][]byte, 0, len(messages))
	timestamps := make([]int64, 0, len(messages))

	// 第一步：序列化所有消息并计算总大小
	for _, msg := range messages {
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
		copy(msgData[:len(lenBuf)], lenBuf)
		copy(msgData[len(lenBuf):], data)

		// 累计大小
		totalSize += int64(len(msgData))
		msgDataList = append(msgDataList, msgData)

		// 记录时间戳
		timestamp := time.Now().UnixNano()
		if msg.Timestamp != nil {
			timestamp = msg.Timestamp.AsTime().UnixNano()
		}
		timestamps = append(timestamps, timestamp)
	}

	// 检查段是否有足够空间
	if s.size+totalSize > s.maxSize {
		return ErrSegmentFull
	}

	// 第二步：批量写入所有消息
	position := s.size

	// 合并所有消息数据为一个大块，减少I/O操作
	combinedData := make([]byte, 0, totalSize)
	for _, msgData := range msgDataList {
		combinedData = append(combinedData, msgData...)
	}

	// 使用页缓存
	if s.pageCache != nil && s.pageCache.config.Enabled {
		// 分块存储到页缓存
		currentPos := position
		for i := 0; i < len(combinedData); i += s.pageCache.config.PageSize {
			end := i + s.pageCache.config.PageSize
			if end > len(combinedData) {
				end = len(combinedData)
			}
			chunk := combinedData[i:end]
			cacheKey := fmt.Sprintf("%s:%d", s.dataFile.Name(), currentPos)
			s.pageCache.Put(cacheKey, chunk, true)
			currentPos += int64(len(chunk))
		}
	}

	// 使用异步I/O写入数据
	if s.asyncIO != nil && s.asyncIO.config.Enabled {
		// 创建同步通道
		ch := make(chan error, 1)

		// 异步写入
		s.asyncIO.AsyncWrite(s.dataFile.Name(), position, combinedData, func(_ []byte, err error) {
			ch <- err
		})

		// 等待写入完成
		if err := <-ch; err != nil {
			return fmt.Errorf("异步批量写入数据失败: %w", err)
		}
	} else {
		// 同步写入
		if _, err := s.dataFile.Write(combinedData); err != nil {
			return fmt.Errorf("写入消息数据失败: %w", err)
		}
	}

	// 更新索引和稀疏索引
	currentPos := position
	for i, msgData := range msgDataList {
		// 添加索引记录
		index := MessageIndex{
			Offset:    s.nextOffset,
			Position:  currentPos,
			Size:      int32(len(msgData)),
			Timestamp: timestamps[i],
		}
		s.index = append(s.index, index)

		// 更新稀疏索引
		if s.sparseIndex != nil {
			if err := s.sparseIndex.AddMessageByOffset(s.nextOffset, currentPos, timestamps[i]); err != nil {
				return fmt.Errorf("更新稀疏索引失败: %w", err)
			}
		}

		// 更新位置和偏移量
		currentPos += int64(len(msgData))
		s.nextOffset++
	}

	// 更新段大小
	s.size += totalSize

	return nil
}

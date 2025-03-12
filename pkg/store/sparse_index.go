package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	pb "github.com/eason-lee/lmq/proto"
)

// SparseIndexConfig 稀疏索引配置
type SparseIndexConfig struct {
	IndexInterval int  // 每隔多少条消息创建一个索引项
	Enabled       bool // 是否启用稀疏索引
}

// DefaultSparseIndexConfig 默认稀疏索引配置
var DefaultSparseIndexConfig = SparseIndexConfig{
	IndexInterval: 4096, // 默认每4096条消息创建一个索引项
	Enabled:       true,
}

// SparseIndex 稀疏索引实现
type SparseIndex struct {
	config       SparseIndexConfig
	indexFile    *os.File
	entries      []SparseIndexEntry
	messageCount int
	mu           sync.RWMutex
}

// SparseIndexEntry 稀疏索引条目
type SparseIndexEntry struct {
	Offset    int64 // 消息偏移量
	Position  int64 // 在数据文件中的位置
	Timestamp int64 // 消息时间戳
}

// NewSparseIndex 创建新的稀疏索引
func NewSparseIndex(indexFilePath string, config SparseIndexConfig) (*SparseIndex, error) {
	// 打开索引文件
	indexFile, err := os.OpenFile(indexFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开稀疏索引文件失败: %w", err)
	}

	// 加载现有索引条目
	entries, err := loadSparseIndexEntries(indexFile)
	if err != nil {
		indexFile.Close()
		return nil, err
	}

	return &SparseIndex{
		config:    config,
		indexFile: indexFile,
		entries:   entries,
	}, nil
}

// loadSparseIndexEntries 从文件加载稀疏索引条目
func loadSparseIndexEntries(file *os.File) ([]SparseIndexEntry, error) {
	// 获取文件大小
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("获取索引文件信息失败: %w", err)
	}

	// 计算条目数量
	entrySize := int64(24) // 每个条目24字节: 8(offset) + 8(position) + 8(timestamp)
	entryCount := info.Size() / entrySize
	if entryCount == 0 {
		return []SparseIndexEntry{}, nil
	}

	// 读取所有条目
	buf := make([]byte, info.Size())
	if _, err := file.ReadAt(buf, 0); err != nil {
		return nil, fmt.Errorf("读取索引文件失败: %w", err)
	}

	// 解析条目
	entries := make([]SparseIndexEntry, entryCount)
	for i := int64(0); i < entryCount; i++ {
		pos := i * entrySize
		entries[i] = SparseIndexEntry{
			Offset:    int64(binary.BigEndian.Uint64(buf[pos : pos+8])),
			Position:  int64(binary.BigEndian.Uint64(buf[pos+8 : pos+16])),
			Timestamp: int64(binary.BigEndian.Uint64(buf[pos+16 : pos+24])),
		}
	}

	return entries, nil
}

// AddMessage 添加消息到稀疏索引
func (si *SparseIndex) AddMessage(msg *pb.Message, offset int64, position int64) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	// 增加消息计数
	si.messageCount++

	// 检查是否需要创建索引项
	if !si.config.Enabled || si.messageCount%si.config.IndexInterval != 0 {
		return nil
	}

	// 创建新的索引条目
	timestamp := msg.Timestamp.Seconds
	entry := SparseIndexEntry{
		Offset:    offset,
		Position:  position,
		Timestamp: timestamp,
	}

	// 添加到内存中的索引
	si.entries = append(si.entries, entry)

	// 写入到文件
	return si.writeEntry(entry)
}

// writeEntry 将索引条目写入文件
func (si *SparseIndex) writeEntry(entry SparseIndexEntry) error {
	// 准备数据
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], uint64(entry.Offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(entry.Position))
	binary.BigEndian.PutUint64(buf[16:24], uint64(entry.Timestamp))

	// 写入文件
	_, err := si.indexFile.Write(buf)
	if err != nil {
		return fmt.Errorf("写入索引条目失败: %w", err)
	}

	return nil
}

// FindPosition 查找指定偏移量对应的位置
func (si *SparseIndex) FindPosition(offset int64) (int64, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	if len(si.entries) == 0 {
		return 0, fmt.Errorf("索引为空")
	}

	// 如果偏移量小于第一个索引条目，返回第一个条目的位置
	if offset <= si.entries[0].Offset {
		return si.entries[0].Position, nil
	}

	// 如果偏移量大于最后一个索引条目，返回最后一个条目的位置
	if offset >= si.entries[len(si.entries)-1].Offset {
		return si.entries[len(si.entries)-1].Position, nil
	}

	// 二分查找最接近但不大于目标偏移量的索引条目
	return si.binarySearch(offset)
}

// binarySearch 二分查找最接近但不大于目标偏移量的索引条目
func (si *SparseIndex) binarySearch(targetOffset int64) (int64, error) {
	left, right := 0, len(si.entries)-1
	var mid int

	for left <= right {
		mid = (left + right) / 2

		if si.entries[mid].Offset == targetOffset {
			// 精确匹配
			return si.entries[mid].Position, nil
		} else if si.entries[mid].Offset < targetOffset {
			// 检查下一个条目
			if mid+1 < len(si.entries) && si.entries[mid+1].Offset > targetOffset {
				// 找到最接近但不大于目标的条目
				return si.entries[mid].Position, nil
			}
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	// 如果没有找到精确匹配，返回最接近但不大于目标的条目
	if right >= 0 {
		return si.entries[right].Position, nil
	}

	return 0, fmt.Errorf("找不到合适的索引条目")
}

// GetEntries 获取所有索引条目
func (si *SparseIndex) GetEntries() []SparseIndexEntry {
	si.mu.RLock()
	defer si.mu.RUnlock()

	// 返回条目的副本，避免并发修改问题
	entries := make([]SparseIndexEntry, len(si.entries))
	copy(entries, si.entries)

	return entries
}

// Close 关闭索引
func (si *SparseIndex) Close() error {
	si.mu.Lock()
	defer si.mu.Unlock()

	return si.indexFile.Close()
}

// Sync 同步索引到磁盘
func (si *SparseIndex) Sync() error {
	si.mu.Lock()
	defer si.mu.Unlock()

	return si.indexFile.Sync()
}

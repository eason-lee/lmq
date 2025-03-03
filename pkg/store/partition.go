package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/protocol"
)

// Partition 表示一个分区
type Partition struct {
	topic        string
	id           int
	dir          string
	segments     []*Segment // 按基础偏移量排序的段列表
	activeSegment *Segment  // 当前活跃的段
	mu           sync.RWMutex
}

// newPartition 创建新的分区
func newPartition(topic string, id int, dir string) (*Partition, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建分区目录失败: %w", err)
	}

	return &Partition{
		topic:    topic,
		id:       id,
		dir:      dir,
		segments: make([]*Segment, 0),
	}, nil
}

// Write 写入消息到分区
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


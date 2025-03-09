package store

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/eason-lee/lmq/proto"
)

// Partition 分区
type Partition struct {
	dir            string
	activeSegment  *Segment
	segments       []*Segment
	maxSegmentSize int64
	mu             sync.RWMutex
}

// NewPartition 创建新的分区实例
func NewPartition(dir string) (*Partition, error) {
	// 创建分区目录
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建分区目录失败: %w", err)
	}

	p := &Partition{
		dir:            dir,
		maxSegmentSize: 1024 * 1024 * 1024, // 默认1GB
		segments:       make([]*Segment, 0),
	}

	// 加载现有的段
	if err := p.loadSegments(); err != nil {
		return nil, err
	}

	// 如果没有段,创建第一个段
	if len(p.segments) == 0 {
		segment, err := newSegment(dir, 0, p.maxSegmentSize)
		if err != nil {
			return nil, fmt.Errorf("创建初始段失败: %w", err)
		}
		p.segments = append(p.segments, segment)
		p.activeSegment = segment
	} else {
		// 使用最后一个段作为活动段
		p.activeSegment = p.segments[len(p.segments)-1]
	}

	return p, nil
}

// loadSegments 加载所有现有的段
func (p *Partition) loadSegments() error {
	files, err := os.ReadDir(p.dir)
	if err != nil {
		return fmt.Errorf("读取分区目录失败: %w", err)
	}

	// 找到所有数据文件
	var baseOffsets []int64
	for _, file := range files {
		if strings.HasSuffix(file.Name(), DataFileSuffix) {
			baseOffset, err := strconv.ParseInt(strings.TrimSuffix(file.Name(), DataFileSuffix), 10, 64)
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
		segment, err := newSegment(p.dir, baseOffset, p.maxSegmentSize)
		if err != nil {
			return fmt.Errorf("加载段失败 [%d]: %w", baseOffset, err)
		}
		p.segments = append(p.segments, segment)
	}

	return nil
}

// Write 写入消息
func (p *Partition) Write(messages []*pb.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, msg := range messages {
		// 检查当前段是否已满
		if p.activeSegment.size >= p.maxSegmentSize {
			// 创建新段
			nextOffset, err := p.activeSegment.GetLatestOffset()
			if err != nil {
				return fmt.Errorf("获取最新偏移量失败: %w", err)
			}

			newSegment, err := newSegment(p.dir, nextOffset, p.maxSegmentSize)
			if err != nil {
				return fmt.Errorf("创建新段失败: %w", err)
			}

			p.segments = append(p.segments, newSegment)
			p.activeSegment = newSegment
		}

		// 写入消息到活动段
		if err := p.activeSegment.Write(msg); err != nil {
			return fmt.Errorf("写入消息到段失败: %w", err)
		}
	}

	return nil
}

// Read 读取消息
func (p *Partition) Read(offset int64, maxMessages int) ([]*pb.Message, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var messages []*pb.Message
	remaining := maxMessages

	// 找到包含起始偏移量的段
	segmentIndex := p.findSegmentIndex(offset)
	if segmentIndex == -1 {
		return nil, fmt.Errorf("找不到包含偏移量 %d 的段", offset)
	}

	// 从找到的段开始读取
	for i := segmentIndex; i < len(p.segments) && remaining > 0; i++ {
		msgs, err := p.segments[i].Read(offset, remaining)
		if err != nil {
			return nil, fmt.Errorf("从段读取消息失败: %w", err)
		}

		messages = append(messages, msgs...)
		remaining -= len(msgs)

		// 更新下一个段的起始偏移量
		if i < len(p.segments)-1 {
			offset = p.segments[i+1].baseOffset
		}
	}

	return messages, nil
}

// findSegmentIndex 找到包含给定偏移量的段的索引
func (p *Partition) findSegmentIndex(offset int64) int {
	// 二分查找
	left, right := 0, len(p.segments)-1
	for left <= right {
		mid := (left + right) / 2
		segment := p.segments[mid]

		// 检查偏移量是否在当前段的范围内
		nextOffset := int64(^uint64(0) >> 1) // MaxInt64
		if mid < len(p.segments)-1 {
			nextOffset = p.segments[mid+1].baseOffset
		}

		if offset >= segment.baseOffset && offset < nextOffset {
			return mid
		}

		if offset < segment.baseOffset {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	return -1
}

// GetLatestOffset 获取最新偏移量
func (p *Partition) GetLatestOffset() (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.activeSegment == nil {
		return 0, nil
	}

	return p.activeSegment.GetLatestOffset()
}

// Close 关闭分区
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	for _, segment := range p.segments {
		if err := segment.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("关闭段失败: %v", errs)
	}

	return nil
}

// CleanupSegments 清理过期的段
func (p *Partition) CleanupSegments(retention time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	var newSegments []*Segment

	// 保留活动段和未过期的段
	for _, segment := range p.segments {
		// 获取段的最后修改时间
		lastModified, err := segment.getLastModifiedTime()
		if err != nil {
			continue
		}

		// 如果段未过期或是活动段,则保留
		if segment == p.activeSegment || now.Sub(lastModified) <= retention {
			newSegments = append(newSegments, segment)
			continue
		}

		// 关闭并删除过期段
		if err := segment.Close(); err != nil {
			log.Printf("关闭段失败: %v", err)
		}
		if err := segment.Delete(); err != nil {
			log.Printf("删除段失败: %v", err)
		}
	}

	p.segments = newSegments
	return nil
}

// GetOffset 获取指定消息ID的偏移量
func (p *Partition) GetOffset(messageID string) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 遍历所有段查找消息
	for _, segment := range p.segments {
		offset, err := segment.GetOffset(messageID)
		if err == nil {
			return offset, nil
		}
	}

	return 0, fmt.Errorf("消息不存在: %s", messageID)
}

package store

import (
	"time"
)

// CleanupPolicy 清理策略接口
// 用于决定哪些段应该被清理
type CleanupPolicy interface {
	// ShouldCleanup 判断哪些段应该被清理
	// 返回应该被清理的段列表
	ShouldCleanup(partition *Partition) []*Segment
	// Name 返回策略名称
	Name() string
}

// TimeBasedCleanupPolicy 基于时间的清理策略
// 清理超过保留时间的段
type TimeBasedCleanupPolicy struct {
	RetentionTime time.Duration // 数据保留时间
}

// ShouldCleanup 判断哪些段应该被清理
func (t *TimeBasedCleanupPolicy) ShouldCleanup(partition *Partition) []*Segment {
	now := time.Now()
	var segmentsToClean []*Segment

	// 获取所有段
	segments := partition.GetSegments()

	// 跳过活动段
	activeSegment := partition.GetActiveSegment()

	// 检查每个段的最后修改时间
	for _, segment := range segments {
		// 跳过活动段
		if segment == activeSegment {
			continue
		}

		// 获取段的最后修改时间
		lastModified, err := segment.getLastModifiedTime()
		if err != nil {
			continue
		}

		// 如果段超过保留时间，则标记为清理
		if now.Sub(lastModified) > t.RetentionTime {
			segmentsToClean = append(segmentsToClean, segment)
		}
	}

	return segmentsToClean
}

// Name 返回策略名称
func (t *TimeBasedCleanupPolicy) Name() string {
	return "time-based"
}

// SizeBasedCleanupPolicy 基于大小的清理策略
// 当分区总大小超过阈值时，清理最旧的段
type SizeBasedCleanupPolicy struct {
	MaxPartitionSize int64 // 分区最大大小（字节）
}

// ShouldCleanup 判断哪些段应该被清理
func (s *SizeBasedCleanupPolicy) ShouldCleanup(partition *Partition) []*Segment {
	var segmentsToClean []*Segment

	// 获取所有段
	segments := partition.GetSegments()

	// 跳过活动段
	activeSegment := partition.GetActiveSegment()

	// 计算分区总大小
	var totalSize int64
	for _, segment := range segments {
		totalSize += segment.size
	}

	// 如果总大小未超过阈值，则不需要清理
	if totalSize <= s.MaxPartitionSize {
		return segmentsToClean
	}

	// 按创建时间排序（假设段的基础偏移量反映了创建顺序）
	// 从最旧的段开始清理，直到总大小低于阈值
	for _, segment := range segments {
		// 跳过活动段
		if segment == activeSegment {
			continue
		}

		segmentsToClean = append(segmentsToClean, segment)
		totalSize -= segment.size

		// 如果总大小已经低于阈值，则停止清理
		if totalSize <= s.MaxPartitionSize {
			break
		}
	}

	return segmentsToClean
}

// Name 返回策略名称
func (s *SizeBasedCleanupPolicy) Name() string {
	return "size-based"
}

// OffsetBasedCleanupPolicy 基于偏移量的清理策略
// 清理小于指定偏移量的段
type OffsetBasedCleanupPolicy struct {
	MinOffset int64 // 最小偏移量
}

// ShouldCleanup 判断哪些段应该被清理
func (o *OffsetBasedCleanupPolicy) ShouldCleanup(partition *Partition) []*Segment {
	var segmentsToClean []*Segment

	// 获取所有段
	segments := partition.GetSegments()

	// 跳过活动段
	activeSegment := partition.GetActiveSegment()

	// 检查每个段的最大偏移量
	for _, segment := range segments {
		// 跳过活动段
		if segment == activeSegment {
			continue
		}

		// 获取段的最大偏移量
		maxOffset, err := segment.GetLatestOffset()
		if err != nil {
			continue
		}

		// 如果段的最大偏移量小于指定的最小偏移量，则标记为清理
		if maxOffset < o.MinOffset {
			segmentsToClean = append(segmentsToClean, segment)
		}
	}

	return segmentsToClean
}

// Name 返回策略名称
func (o *OffsetBasedCleanupPolicy) Name() string {
	return "offset-based"
}

// CompositeCleanupPolicy 组合清理策略
// 组合多个清理策略，取并集
type CompositeCleanupPolicy struct {
	policies []CleanupPolicy
}

// NewCompositeCleanupPolicy 创建新的组合清理策略
func NewCompositeCleanupPolicy(policies ...CleanupPolicy) *CompositeCleanupPolicy {
	return &CompositeCleanupPolicy{
		policies: policies,
	}
}

// ShouldCleanup 判断哪些段应该被清理
func (c *CompositeCleanupPolicy) ShouldCleanup(partition *Partition) []*Segment {
	// 使用map去重
	segmentMap := make(map[*Segment]struct{})

	// 应用每个策略
	for _, policy := range c.policies {
		segments := policy.ShouldCleanup(partition)
		for _, segment := range segments {
			segmentMap[segment] = struct{}{}
		}
	}

	// 转换为切片
	var segmentsToClean []*Segment
	for segment := range segmentMap {
		segmentsToClean = append(segmentsToClean, segment)
	}

	return segmentsToClean
}

// Name 返回策略名称
func (c *CompositeCleanupPolicy) Name() string {
	return "composite"
}

// DefaultCleanupPolicy 默认清理策略
// 使用基于时间的策略，默认保留7天
var DefaultCleanupPolicy = &TimeBasedCleanupPolicy{
	RetentionTime: 7 * 24 * time.Hour, // 7天
}
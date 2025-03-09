package store

import (
	"time"
)

// SegmentRollStrategy 分段策略接口
// 用于决定何时应该创建新的段
type SegmentRollStrategy interface {
	// ShouldRoll 判断是否应该创建新的段
	ShouldRoll(segment *Segment) bool
	// Name 返回策略名称
	Name() string
}

// SizeBasedRollStrategy 基于大小的分段策略
// 当段大小达到阈值时创建新段
type SizeBasedRollStrategy struct {
	MaxSize int64 // 最大段大小（字节）
}

// ShouldRoll 判断是否应该创建新的段
func (s *SizeBasedRollStrategy) ShouldRoll(segment *Segment) bool {
	return segment.size >= s.MaxSize
}

// Name 返回策略名称
func (s *SizeBasedRollStrategy) Name() string {
	return "size-based"
}

// TimeBasedRollStrategy 基于时间的分段策略
// 当段的时间跨度达到阈值时创建新段
type TimeBasedRollStrategy struct {
	MaxTimeSpan time.Duration // 最大时间跨度
}

// ShouldRoll 判断是否应该创建新的段
func (t *TimeBasedRollStrategy) ShouldRoll(segment *Segment) bool {

	// 计算时间跨度
	timeSpan := time.Since(segment.ct)
	return timeSpan >= t.MaxTimeSpan
}

// Name 返回策略名称
func (t *TimeBasedRollStrategy) Name() string {
	return "time-based"
}

// CountBasedRollStrategy 基于消息数量的分段策略
// 当段的消息数量达到阈值时创建新段
type CountBasedRollStrategy struct {
	MaxCount int // 最大消息数量
}

// ShouldRoll 判断是否应该创建新的段
func (c *CountBasedRollStrategy) ShouldRoll(segment *Segment) bool {
	return len(segment.index) >= c.MaxCount
}

// Name 返回策略名称
func (c *CountBasedRollStrategy) Name() string {
	return "count-based"
}

// CompositeRollStrategy 组合分段策略
// 当任一子策略返回true时创建新段
type CompositeRollStrategy struct {
	strategies []SegmentRollStrategy
}

// NewCompositeRollStrategy 创建新的组合分段策略
func NewCompositeRollStrategy(strategies ...SegmentRollStrategy) *CompositeRollStrategy {
	return &CompositeRollStrategy{
		strategies: strategies,
	}
}

// ShouldRoll 判断是否应该创建新的段
func (c *CompositeRollStrategy) ShouldRoll(segment *Segment) bool {
	for _, strategy := range c.strategies {
		if strategy.ShouldRoll(segment) {
			return true
		}
	}
	return false
}

// Name 返回策略名称
func (c *CompositeRollStrategy) Name() string {
	return "composite"
}

// DefaultRollStrategy 默认分段策略
// 使用基于大小的策略，默认64MB
var DefaultRollStrategy = &SizeBasedRollStrategy{
	MaxSize: 64 * 1024 * 1024, // 64MB
}
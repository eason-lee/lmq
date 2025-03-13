package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSizeBasedRollStrategy(t *testing.T) {
	// 创建测试段
	segment := &Segment{
		size:    50 * 1024 * 1024,  // 50MB
		maxSize: 100 * 1024 * 1024, // 100MB
	}

	// 测试小于阈值的情况
	strategy := &SizeBasedRollStrategy{
		MaxSize: 60 * 1024 * 1024, // 60MB
	}

	// 段大小小于策略阈值，不应该创建新段
	assert.False(t, strategy.ShouldRoll(segment))

	// 测试大于阈值的情况
	strategy = &SizeBasedRollStrategy{
		MaxSize: 40 * 1024 * 1024, // 40MB
	}

	// 段大小大于策略阈值，应该创建新段
	assert.True(t, strategy.ShouldRoll(segment))

	// 测试等于阈值的情况
	strategy = &SizeBasedRollStrategy{
		MaxSize: 50 * 1024 * 1024, // 50MB
	}

	// 段大小等于策略阈值，应该创建新段
	assert.True(t, strategy.ShouldRoll(segment))

	// 验证策略名称
	assert.Equal(t, "size-based", strategy.Name())
}

func TestTimeBasedRollStrategy(t *testing.T) {
	// 创建测试段
	segment := &Segment{
		ct: time.Now().Add(-2 * time.Hour), // 段创建于2小时前
	}

	// 测试时间跨度大于阈值的情况
	strategy := &TimeBasedRollStrategy{
		MaxTimeSpan: 1 * time.Hour, // 1小时
	}

	// 段时间跨度大于策略阈值，应该创建新段
	assert.True(t, strategy.ShouldRoll(segment))

	// 测试时间跨度小于阈值的情况
	strategy = &TimeBasedRollStrategy{
		MaxTimeSpan: 3 * time.Hour, // 3小时
	}

	// 段时间跨度小于策略阈值，不应该创建新段
	assert.False(t, strategy.ShouldRoll(segment))

	// 验证策略名称
	assert.Equal(t, "time-based", strategy.Name())
}

func TestCountBasedRollStrategy(t *testing.T) {
	// 创建测试段
	segment := &Segment{
		index: make([]MessageIndex, 100), // 100条消息
	}

	// 测试消息数量大于阈值的情况
	strategy := &CountBasedRollStrategy{
		MaxCount: 50, // 50条消息
	}

	// 段消息数量大于策略阈值，应该创建新段
	assert.True(t, strategy.ShouldRoll(segment))

	// 测试消息数量小于阈值的情况
	strategy = &CountBasedRollStrategy{
		MaxCount: 150, // 150条消息
	}

	// 段消息数量小于策略阈值，不应该创建新段
	assert.False(t, strategy.ShouldRoll(segment))

	// 测试消息数量等于阈值的情况
	strategy = &CountBasedRollStrategy{
		MaxCount: 100, // 100条消息
	}

	// 段消息数量等于策略阈值，应该创建新段
	assert.True(t, strategy.ShouldRoll(segment))

	// 验证策略名称
	assert.Equal(t, "count-based", strategy.Name())
}

func TestCompositeRollStrategy(t *testing.T) {
	// 创建测试段
	segment := &Segment{
		size:  30 * 1024 * 1024,                  // 30MB
		index: make([]MessageIndex, 80),          // 80条消息
		ct:    time.Now().Add(-30 * time.Minute), // 段创建于30分钟前
	}

	// 创建组合策略
	strategy := NewCompositeRollStrategy(
		&SizeBasedRollStrategy{MaxSize: 50 * 1024 * 1024},  // 50MB
		&CountBasedRollStrategy{MaxCount: 100},             // 100条消息
		&TimeBasedRollStrategy{MaxTimeSpan: 1 * time.Hour}, // 1小时
	)

	// 所有子策略都不满足条件，不应该创建新段
	assert.False(t, strategy.ShouldRoll(segment))

	// 修改段大小，使其满足大小策略
	segment.size = 60 * 1024 * 1024 // 60MB

	// 有一个子策略满足条件，应该创建新段
	assert.True(t, strategy.ShouldRoll(segment))

	// 重置段大小
	segment.size = 30 * 1024 * 1024 // 30MB

	// 修改段消息数量，使其满足数量策略
	segment.index = make([]MessageIndex, 120) // 120条消息

	// 有一个子策略满足条件，应该创建新段
	assert.True(t, strategy.ShouldRoll(segment))

	// 重置段消息数量
	segment.index = make([]MessageIndex, 80) // 80条消息

	// 修改段创建时间，使其满足时间策略
	segment.ct = time.Now().Add(-2 * time.Hour) // 段创建于2小时前

	// 有一个子策略满足条件，应该创建新段
	assert.True(t, strategy.ShouldRoll(segment))

	// 验证策略名称
	assert.Equal(t, "composite", strategy.Name())
}

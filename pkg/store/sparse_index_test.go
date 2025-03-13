package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/eason-lee/lmq/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSparseIndex(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "sparse-index-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 测试配置
	config := SparseIndexConfig{
		IndexInterval: 2, // 每2条消息创建一个索引项，方便测试
		Enabled:       true,
	}

	// 创建稀疏索引
	indexPath := filepath.Join(tempDir, "test.sparse")
	index, err := NewSparseIndex(indexPath, config)
	assert.NoError(t, err)
	assert.NotNil(t, index)

	// 测试添加消息
	testMessages := createTestMessages(10)
	for i, msg := range testMessages {
		err := index.AddMessage(msg, int64(i), int64(i*100))
		assert.NoError(t, err)
	}

	// 验证索引条目数量
	assert.Equal(t, 5, len(index.entries)) // 应该有5个条目 (10条消息，每2条创建一个索引项)

	// 测试查找位置
	position, err := index.FindPosition(4)
	assert.NoError(t, err)
	assert.Equal(t, int64(4*100), position)

	// 测试查找不存在的位置（应该返回最近的较小索引）
	position, err = index.FindPosition(5)
	assert.NoError(t, err)
	assert.Equal(t, int64(4*100), position) // 应该返回索引4的位置

	// 测试查找超出范围的位置
	_, err = index.FindPosition(20)
	assert.Error(t, err)

	// 测试关闭
	err = index.Close()
	assert.NoError(t, err)

	// 测试重新加载
	index2, err := NewSparseIndex(indexPath, config)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(index2.entries))

	// 验证加载的条目
	assert.Equal(t, int64(0), index2.entries[0].Offset)
	assert.Equal(t, int64(0), index2.entries[0].Position)
	assert.Equal(t, int64(2), index2.entries[1].Offset)
	assert.Equal(t, int64(200), index2.entries[1].Position)

	// 测试禁用稀疏索引
	config.Enabled = false
	index3, err := NewSparseIndex(indexPath, config)
	assert.NoError(t, err)

	// 添加消息不应该创建索引项
	msg := createTestMessages(1)[0]
	err = index3.AddMessage(msg, 100, 10000)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(index3.entries)) // 条目数量应该不变
}

// 测试二分查找
func TestSparseIndexBinarySearch(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "sparse-index-binary-search-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建稀疏索引
	indexPath := filepath.Join(tempDir, "test.sparse")
	config := SparseIndexConfig{IndexInterval: 1, Enabled: true} // 每条消息都创建索引项
	index, err := NewSparseIndex(indexPath, config)
	assert.NoError(t, err)

	// 添加有序的索引条目
	for i := 0; i < 100; i++ {
		msg := createTestMessages(1)[0]
		err := index.AddMessage(msg, int64(i), int64(i*100))
		assert.NoError(t, err)
	}

	// 测试二分查找
	testCases := []struct {
		offset      int64
		expectedPos int64
		expectedErr bool
	}{
		{0, 0, false},
		{50, 5000, false},
		{99, 9900, false},
		{25, 2500, false},
		{75, 7500, false},
		{-1, 0, true},  // 小于最小偏移量
		{100, 0, true}, // 大于最大偏移量
	}

	for _, tc := range testCases {
		pos, err := index.FindPosition(tc.offset)
		if tc.expectedErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedPos, pos)
		}
	}
}

// 创建测试消息
func createTestMessages(count int) []*pb.Message {
	messages := make([]*pb.Message, count)
	for i := 0; i < count; i++ {
		messages[i] = &pb.Message{
			Id:        fmt.Sprintf("msg-%d", i),
			Topic:     "test-topic",
			Body:      []byte(fmt.Sprintf("message body %d", i)),
			Timestamp: timestamppb.New(time.Now()),
			Partition: 0,
		}
	}
	return messages
}

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/eason-lee/lmq/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSegmentWriteBatch(t *testing.T) {
    // 创建临时根目录
    tempDir, err := os.MkdirTemp("", "segment-test")
    if err != nil {
        t.Fatalf("创建临时目录失败: %v", err)
    }
    defer os.RemoveAll(tempDir)

    // 测试批量写入（每个子测试使用独立段，避免相互影响）
    batchSizes := []int{1, 10, 100, 1000}
    for _, batchSize := range batchSizes {
        t.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(t *testing.T) {
            // 为子测试创建独立目录与段
            subDir := filepath.Join(tempDir, fmt.Sprintf("batch_%d", batchSize))
            if err := os.MkdirAll(subDir, 0755); err != nil {
                t.Fatalf("创建子目录失败: %v", err)
            }
            segment, err := NewSegment(subDir, 0, 1024*1024*10) // 10MB
            if err != nil {
                t.Fatalf("创建段失败: %v", err)
            }
            defer segment.Close()

            // 准备测试数据
            messages := generateTestMessages(batchSize)

            // 批量写入
            if err := segment.WriteBatch(messages); err != nil {
                t.Fatalf("批量写入失败: %v", err)
            }

            // 验证写入的消息数量：最新偏移量应为 batchSize-1
            latestOffset, err := segment.GetLatestOffset()
            if err != nil {
                t.Fatalf("获取最新偏移量失败: %v", err)
            }
            expectedOffset := int64(batchSize - 1)
            if latestOffset != expectedOffset {
                t.Errorf("最新偏移量不匹配，期望: %d, 实际: %d", expectedOffset, latestOffset)
            }
        })
    }
}

// 生成测试消息
func generateTestMessages(count int) []*pb.Message {
	messages := make([]*pb.Message, count)
	for i := 0; i < count; i++ {
		messages[i] = &pb.Message{
			Id:        fmt.Sprintf("msg-%d", i),
			Topic:     "test-topic",
			Body:      []byte(fmt.Sprintf("test-message-body-%d", i)),
			Timestamp: timestamppb.Now(),
			Type:      pb.MessageType_NORMAL,
			Partition: 0,
		}
	}
	return messages
}

func BenchmarkSegmentWrite(b *testing.B) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "segment-benchmark")
	if err != nil {
		b.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	b.Run("BatchWrite_100", func(b *testing.B) {
		// 创建段
		segmentPath := filepath.Join(tempDir, "batch100")
		os.MkdirAll(segmentPath, 0755)
		segment, err := NewSegment(segmentPath, 0, 1024*1024*100) // 100MB
		if err != nil {
			b.Fatalf("创建段失败: %v", err)
		}
		defer segment.Close()

		// 准备批量消息
		batchSize := 100
		messages := make([]*pb.Message, batchSize)
		for i := 0; i < batchSize; i++ {
			messages[i] = &pb.Message{
				Id:        fmt.Sprintf("benchmark-msg-%d", i),
				Topic:     "benchmark-topic",
				Body:      []byte(fmt.Sprintf("benchmark-message-body-%d", i)),
				Timestamp: timestamppb.Now(),
				Type:      pb.MessageType_NORMAL,
				Partition: 0,
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = segment.WriteBatch(messages)
		}
	})
}

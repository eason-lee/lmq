package main

import (
	"fmt"
	"log"
	"time"

	"github.com/eason-lee/lmq/pkg/protocol"
	"github.com/eason-lee/lmq/sdk"
)

func main() {
	// 创建 LMQ 客户端
	config := &lmq.Config{
		Brokers:        []string{"localhost:9000"},
		Timeout:        5 * time.Second,
		RetryTimes:     3,
		GroupID:        "example-group",
		AutoCommit:     true,
		CommitInterval: 5 * time.Second,
		MaxPullRecords: 100,
		PullInterval:    1 * time.Second,
	}

	client, err := lmq.NewClient(config)
	if err != nil {
		log.Fatalf("创建 LMQ 客户端失败: %v", err)
	}
	defer client.Close()

	// 订阅主题
	err = client.Subscribe([]string{"test-topic"}, func(messages []*protocol.Message) {
		for _, msg := range messages {
			fmt.Printf("收到消息: ID=%s, 主题=%s, 内容=%s, 时间=%v\n",
				msg.Id, msg.Topic, string(msg.Body), msg.Timestamp)
		}
	})
	if err != nil {
		log.Fatalf("订阅主题失败: %v", err)
	}

	// 发送消息
	result, err := client.Send("test-topic", []byte("Hello, LMQ SDK!"))
	if err != nil {
		log.Fatalf("发送消息失败: %v", err)
	}
	fmt.Printf("消息发送成功: ID=%s, 分区=%d\n", result.MessageID, result.Partition)

	// 异步发送消息
	client.SendAsync("test-topic", []byte("Async message"), func(result *lmq.SendResult) {
		if result.Error != nil {
			fmt.Printf("异步发送失败: %v\n", result.Error)
		} else {
			fmt.Printf("异步发送成功: ID=%s\n", result.MessageID)
		}
	})

	// 批量发送消息
	messages := [][]byte{
		[]byte("Batch message 1"),
		[]byte("Batch message 2"),
		[]byte("Batch message 3"),
	}
	results, err := client.SendBatch("test-topic", messages)
	if err != nil {
		log.Printf("批量发送部分失败: %v", err)
	}
	for i, res := range results {
		if res.Error != nil {
			fmt.Printf("消息 %d 发送失败: %v\n", i, res.Error)
		} else {
			fmt.Printf("消息 %d 发送成功: ID=%s\n", i, res.MessageID)
		}
	}

	// 等待一段时间，让消息被消费
	time.Sleep(10 * time.Second)
}
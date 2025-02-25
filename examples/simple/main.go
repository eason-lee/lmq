package main

import (
	"fmt"
	"log"
	"time"

	"github.com/eason-lee/lmq/client"
	"github.com/eason-lee/lmq/pkg/protocol"
)

func main() {
	// 创建客户端
	c, err := client.NewClient("localhost:9000")
	if err != nil {
		log.Fatalf("创建客户端失败: %v", err)
	}
	defer c.Close()

	// 创建一个通道用于同步
	done := make(chan bool)

	// 订阅主题
	err = c.Subscribe([]string{"test-topic"}, func(msg *protocol.Message) {
		fmt.Printf("收到推送消息: %s, 内容: %s\n", msg.ID, string(msg.Body))

		// 如果收到最后一条消息，发出完成信号
		if string(msg.Body) == "这是消息 #5" {
			done <- true
		}
	})
	if err != nil {
		log.Fatalf("订阅失败: %v", err)
	}

	fmt.Println("已订阅 test-topic，等待消息...")
	time.Sleep(1 * time.Second) // 确保订阅已完成

	// 发布消息
	for i := 0; i < 5; i++ {
		msg, err := c.Publish("test-topic", []byte(fmt.Sprintf("这是消息 #%d", i+1)))
		if err != nil {
			log.Printf("发布消息失败: %v", err)
			continue
		}
		fmt.Printf("发布消息成功: %s\n", msg.ID)
		time.Sleep(500 * time.Millisecond) // 减少间隔时间
	}

	// 等待最后一条消息被接收
	select {
	case <-done:
		fmt.Println("所有消息已接收")
	case <-time.After(10 * time.Second):
		fmt.Println("等待超时，可能有消息未被接收")
	}

	// 获取消息历史
	messages, err := c.Fetch("test-topic")
	if err != nil {
		log.Fatalf("获取消息失败: %v", err)
	}

	fmt.Printf("\n历史消息记录 (%d 条):\n", len(messages))
	for _, msg := range messages {
		fmt.Printf("- %s: %s\n", msg.ID, string(msg.Body))
	}
}

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

	// 订阅主题
	err = c.Subscribe([]string{"test-topic"}, func(msg *protocol.Message) {
		fmt.Printf("收到消息: %s, 内容: %s\n", msg.ID, string(msg.Body))
	})
	if err != nil {
		log.Fatalf("订阅失败: %v", err)
	}

	// 发布消息
	for i := 0; i < 5; i++ {
		msg, err := c.Publish("test-topic", []byte(fmt.Sprintf("这是消息 #%d", i+1)))
		if err != nil {
			log.Printf("发布消息失败: %v", err)
			continue
		}
		fmt.Printf("发布消息成功: %s\n", msg.ID)
		time.Sleep(1 * time.Second)
	}

	// 获取消息
	messages, err := c.Fetch("test-topic")
	if err != nil {
		log.Fatalf("获取消息失败: %v", err)
	}

	fmt.Printf("获取到 %d 条消息:\n", len(messages))
	for _, msg := range messages {
		fmt.Printf("- %s: %s\n", msg.ID, string(msg.Body))
	}

	// 等待一段时间以接收订阅的消息
	time.Sleep(5 * time.Second)
}

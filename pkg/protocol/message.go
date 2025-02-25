package protocol

import (
	"time"
)

// Message 表示一个消息
type Message struct {
	ID        string    // 消息唯一标识符
	Topic     string    // 消息主题
	Body      []byte    // 消息内容
	Timestamp time.Time // 消息创建时间
}

// NewMessage 创建一个新的消息
func NewMessage(topic string, body []byte) *Message {
	return &Message{
		ID:        generateID(), // 实现一个生成唯一ID的函数
		Topic:     topic,
		Body:      body,
		Timestamp: time.Now(),
	}
}

// 生成唯一ID
func generateID() string {
	// 简单实现，使用时间戳+随机数
	return time.Now().Format("20060102150405") + "-" + randomString(8)
}

// 生成随机字符串
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
		time.Sleep(1 * time.Nanosecond) // 确保随机性
	}
	return string(b)
} 
package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/protocol"
	"github.com/eason-lee/lmq/pkg/store"
)

// Subscriber 表示一个订阅者
type Subscriber struct {
	ID     string
	Topics []string
	Ch     chan *protocol.Message
}

// Broker 消息代理，负责消息的路由和分发
type Broker struct {
	store               *store.FileStore
	subscribers         map[string][]*Subscriber // 主题 -> 订阅者列表
	mu                  sync.RWMutex
	unackedMessages     map[string]*protocol.Message // 消息ID -> 消息
	unackedMu           sync.RWMutex
}

// NewBroker 创建一个新的消息代理
func NewBroker(storeDir string) (*Broker, error) {
	fileStore, err := store.NewFileStore(storeDir)
	if err != nil {
		return nil, err
	}

	return &Broker{
		store:               fileStore,
		subscribers:         make(map[string][]*Subscriber),
		unackedMessages:     make(map[string]*protocol.Message),
	}, nil
}

// Publish 发布消息到指定主题
func (b *Broker) Publish(topic string, data []byte) (*protocol.Message, error) {
	// 创建消息
	msg := protocol.NewMessage(topic, data)

	// 保存消息
	if err := b.store.Save(topic, msg); err != nil {
		return nil, err
	}

	// 分发消息给订阅者
	b.mu.RLock()
	subs := b.subscribers[topic]
	b.mu.RUnlock()

	for _, sub := range subs {
		// 非阻塞发送，避免一个慢订阅者阻塞整个系统
		select {
		case sub.Ch <- msg:
			// 消息发送成功
		default:
			// 订阅者的通道已满，可以记录日志或采取其他措施
			fmt.Printf("订阅者 %s 的通道已满，消息 %s 未能发送\n", sub.ID, msg.ID)
		}
	}

	return msg, nil
}

// Subscribe 订阅指定主题
func (b *Broker) Subscribe(subID string, topics []string, bufSize int) (*Subscriber, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 创建订阅者
	sub := &Subscriber{
		ID:     subID,
		Topics: topics,
		Ch:     make(chan *protocol.Message, bufSize),
	}

	// 添加到订阅列表
	for _, topic := range topics {
		b.subscribers[topic] = append(b.subscribers[topic], sub)
	}

	return sub, nil
}

// Unsubscribe 取消订阅
func (b *Broker) Unsubscribe(subID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 从所有主题中移除该订阅者
	for topic, subs := range b.subscribers {
		var newSubs []*Subscriber
		for _, sub := range subs {
			if sub.ID != subID {
				newSubs = append(newSubs, sub)
			}
		}
		b.subscribers[topic] = newSubs
	}
}

// GetMessages 获取指定主题的所有消息
func (b *Broker) GetMessages(topic string) ([]*protocol.Message, error) {
	return b.store.GetMessages(topic)
}

// AckMessage 添加确认消息的方法
func (b *Broker) AckMessage(messageID string) {
	b.unackedMu.Lock()
	defer b.unackedMu.Unlock()
	
	delete(b.unackedMessages, messageID)
}

// RetryUnackedMessages 添加重试未确认消息的方法
func (b *Broker) RetryUnackedMessages() {
	b.unackedMu.RLock()
	messages := make([]*protocol.Message, 0, len(b.unackedMessages))
	for _, msg := range b.unackedMessages {
		messages = append(messages, msg)
	}
	b.unackedMu.RUnlock()

	for _, msg := range messages {
		// 重新发布消息
		b.Publish(msg.Topic, msg.Body)
	}
}

// StartRetryTask 启动定时重试任务
func (b *Broker) StartRetryTask(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			b.RetryUnackedMessages()
		}
	}()
}

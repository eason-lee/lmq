package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/protocol"
	"github.com/eason-lee/lmq/pkg/replication"
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
	nodeID          string
	replicaMgr      *replication.ReplicaManager
	store           *store.FileStore
	subscribers     map[string][]*Subscriber // 主题 -> 订阅者列表
	mu              sync.RWMutex
	unackedMessages map[string]*protocol.Message // 消息ID -> 消息
	unackedMu       sync.RWMutex
}

// NewBroker 创建一个新的消息代理
// 修改 Broker 构造函数
func NewBroker(nodeID string, storeDir string, addr string) (*Broker, error) {
    fileStore, err := store.NewFileStore(storeDir)
    if err != nil {
        return nil, err
    }

    replicaMgr := replication.NewReplicaManager(nodeID, fileStore, addr)
    broker := &Broker{
        nodeID:          nodeID,
        store:          fileStore,
        replicaMgr:     replicaMgr,
        subscribers:    make(map[string][]*Subscriber),
        unackedMessages: make(map[string]*protocol.Message),
    }

    // 启动复制管理器
    if err := replicaMgr.Start(); err != nil {
        return nil, err
    }

    return broker, nil
}

// Publish 发布消息到指定主题
func (b *Broker) Publish(topic string, data []byte) (*protocol.Message, error) {
	// 创建消息
	msg := protocol.NewMessage(topic, data)
	
	// 选择分区
	partition := b.selectPartition(topic, msg.ID)
	
	// 保存消息到分区
	if err := b.store.Write(topic, partition, []*protocol.Message{msg}); err != nil {
		return nil, fmt.Errorf("保存消息失败: %w", err)
	}
	
	// 复制到其他节点
	if err := b.replicaMgr.ReplicateMessages(topic, partition, []*protocol.Message{msg}); err != nil {
		return nil, fmt.Errorf("复制消息失败: %w", err)
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

// 添加新的方法
func (b *Broker) HandleConnection(conn net.Conn) {
    defer conn.Close()

    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)

    for {
        // 读取消息
        data, err := reader.ReadBytes('\n')
        if err != nil {
            log.Printf("读取消息失败: %v", err)
            break
        }

        // 解析消息
        var msg protocol.Message
        if err := json.Unmarshal(data, &msg); err != nil {
            log.Printf("解析消息失败: %v", err)
            continue
        }

        // 根据消息类型处理
        switch msg.Type {
        case "publish":
            // 处理发布消息
            if err := b.handlePublish(&msg); err != nil {
                b.sendError(writer, err)
                continue
            }
            b.sendSuccess(writer, "消息发布成功")

        case "subscribe":
            // 处理订阅请求
            if err := b.handleSubscribe(conn, &msg); err != nil {
                b.sendError(writer, err)
                continue
            }
            b.sendSuccess(writer, "订阅成功")

        case "ack":
            // 处理消息确认
            if err := b.handleAck(&msg); err != nil {
                b.sendError(writer, err)
                continue
            }
            b.sendSuccess(writer, "确认成功")

        default:
            b.sendError(writer, fmt.Errorf("未知的消息类型: %s", msg.Type))
        }
    }
}

// 处理发布消息
func (b *Broker) handlePublish(msg *protocol.Message) error {
    // 选择分区
    partition := b.selectPartition(msg.Topic, msg.ID)
    
    // 写入存储
    if err := b.store.Write(msg.Topic, partition, []*protocol.Message{msg}); err != nil {
        return fmt.Errorf("存储消息失败: %w", err)
    }

    // 复制到其他节点
    if err := b.replicaMgr.ReplicateMessages(msg.Topic, partition, []*protocol.Message{msg}); err != nil {
        return fmt.Errorf("复制消息失败: %w", err)
    }

    return nil
}

// selectPartition 选择消息应该发送到的分区
func (b *Broker) selectPartition(topic string, messageID string) int {
    // 获取主题的分区数量
    partitionCount := b.getPartitionCount(topic)
    if partitionCount <= 0 {
        // 如果没有分区，默认使用分区0
        return 0
    }
    
    // 使用消息ID的哈希值来确定分区
    // 这是一个简单的哈希分区策略，可以根据需要改进
    hash := 0
    for _, c := range messageID {
        hash = 31*hash + int(c)
    }
    if hash < 0 {
        hash = -hash
    }
    return hash % partitionCount
}

// getPartitionCount 获取主题的分区数量
func (b *Broker) getPartitionCount(topic string) int {
    // 从复制管理器获取分区信息
    partitions := b.replicaMgr.GetPartitions(topic)
    return len(partitions)
}

// 处理订阅请求
func (b *Broker) handleSubscribe(conn net.Conn, msg *protocol.Message) error {
    // TODO: 实现订阅逻辑
    return nil
}

// 处理消息确认
func (b *Broker) handleAck(msg *protocol.Message) error {
    // TODO: 实现确认逻辑
    return nil
}

// 发送错误响应
func (b *Broker) sendError(writer *bufio.Writer, err error) {
    resp := &protocol.Response{
        Success: false,
        Message:   err.Error(),
    }
    b.sendResponse(writer, resp)
}

// 发送成功响应
func (b *Broker) sendSuccess(writer *bufio.Writer, message string) {
    resp := &protocol.Response{
        Success: true,
        Message: message,
    }
    b.sendResponse(writer, resp)
}

// 发送响应
func (b *Broker) sendResponse(writer *bufio.Writer, resp *protocol.Response) {
    data, _ := json.Marshal(resp)
    writer.Write(append(data, '\n'))
    writer.Flush()
}

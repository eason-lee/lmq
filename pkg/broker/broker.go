package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/protocol"
	"github.com/eason-lee/lmq/pkg/replication"
	"github.com/eason-lee/lmq/pkg/store"
     pb "github.com/eason-lee/lmq/proto"
     "google.golang.org/protobuf/proto"
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

    if err := broker.Start(); err != nil {
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

// Start 启动 Broker
func (b *Broker) Start() error {
	// 启动复制管理器
	if err := b.replicaMgr.Start(); err != nil {
		return err
	}
	
	// 启动清理任务，每小时执行一次，保留7天的数据
	b.StartCleanupTask(1*time.Hour, 7*24*time.Hour)
	
	// 启动重试任务
	b.StartRetryTask(5 * time.Second)
	
	return nil
}

// 添加新的方法
func (b *Broker) HandleConnection(conn net.Conn) {
    defer conn.Close()

    for {
        // 读取消息长度（4字节）
        lenBuf := make([]byte, 4)
        if _, err := io.ReadFull(conn, lenBuf); err != nil {
            if err != io.EOF {
                log.Printf("读取消息长度失败: %v", err)
            }
            break
        }
        
        // 解析消息长度
        msgLen := binary.BigEndian.Uint32(lenBuf)
        
        // 读取消息内容
        msgBuf := make([]byte, msgLen)
        if _, err := io.ReadFull(conn, msgBuf); err != nil {
            log.Printf("读取消息内容失败: %v", err)
            break
        }
        
        // 反序列化消息
        var pbMsg pb.Message
        if err := proto.Unmarshal(msgBuf, &pbMsg); err != nil {
            log.Printf("反序列化消息失败: %v", err)
            continue
        }
        
        // 转换为内部消息格式
        msg := &protocol.Message{
            ID:        pbMsg.Id,
            Topic:     pbMsg.Topic,
            Body:      pbMsg.Body,
            Timestamp: pbMsg.Timestamp,
            Type:      pbMsg.Type,
        }

        // 根据消息类型处理
        var err error
        switch msg.Type {
        case "publish":
            err = b.handlePublish(msg)
        case "subscribe":
            err = b.handleSubscribe(conn, msg)
        case "ack":
            err = b.handleAck(msg)
        default:
            err = fmt.Errorf("未知的消息类型: %s", msg.Type)
        }
        
        // 发送响应
        if err != nil {
            b.sendProtobufError(conn, err)
        } else {
            b.sendProtobufSuccess(conn, "操作成功")
        }
    }
}

// 发送 Protobuf 错误响应
func (b *Broker) sendProtobufError(conn net.Conn, err error) {
    resp := &pb.Response{
        Success: false,
        Message: err.Error(),
    }
    b.sendProtobufResponse(conn, resp)
}

// 发送 Protobuf 成功响应
func (b *Broker) sendProtobufSuccess(conn net.Conn, message string) {
    resp := &pb.Response{
        Success: true,
        Message: message,
    }
    b.sendProtobufResponse(conn, resp)
}

// 发送 Protobuf 响应
func (b *Broker) sendProtobufResponse(conn net.Conn, resp *pb.Response) {
    // 序列化响应
    data, err := proto.Marshal(resp)
    if err != nil {
        log.Printf("序列化响应失败: %v", err)
        return
    }
    
    // 准备长度前缀
    lenBuf := make([]byte, 4)
    binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
    
    // 发送长度和数据
    if _, err := conn.Write(lenBuf); err != nil {
        log.Printf("发送响应长度失败: %v", err)
        return
    }
    
    if _, err := conn.Write(data); err != nil {
        log.Printf("发送响应内容失败: %v", err)
        return
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

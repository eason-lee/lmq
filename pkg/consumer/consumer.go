package consumer

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/network"
	"github.com/eason-lee/lmq/pkg/protocol"
	pb "github.com/eason-lee/lmq/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	Brokers        []string      // broker 节点地址列表
	GroupID        string        // 消费者组ID
	Topics         []string      // 订阅的主题列表
	AutoCommit     bool          // 是否自动提交确认
	CommitInterval time.Duration // 自动提交间隔
	MaxPullRecords int           // 单次拉取的最大消息数
	PullTimeout    time.Duration // 拉取超时时间
}

// Consumer 消息消费者
type Consumer struct {
	config     *ConsumerConfig
	client     *network.Client
	subscribed bool
	messages   chan *protocol.Message
	stopCh     chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
}

// NewConsumer 创建一个新的消费者
func NewConsumer(config *ConsumerConfig) (*Consumer, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("至少需要一个 broker 地址")
	}

	if config.GroupID == "" {
		return nil, fmt.Errorf("必须指定消费者组ID")
	}

	if len(config.Topics) == 0 {
		return nil, fmt.Errorf("至少需要订阅一个主题")
	}

	if config.MaxPullRecords <= 0 {
		config.MaxPullRecords = 100
	}

	if config.PullTimeout <= 0 {
		config.PullTimeout = 5 * time.Second
	}

	// 创建到 broker 的连接
	client, err := network.NewClient(config.Brokers[0]) // 暂时只连接第一个 broker
	if err != nil {
		return nil, fmt.Errorf("连接 broker 失败: %w", err)
	}

	return &Consumer{
		config:   config,
		client:   client,
		messages: make(chan *protocol.Message, 1000),
		stopCh:   make(chan struct{}),
	}, nil
}

// Subscribe 订阅主题
func (c *Consumer) Subscribe() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subscribed {
		return nil // 已经订阅过了
	}

	// 创建订阅请求
	req := &protocol.Message{
		Message: &pb.Message{
			Type: pb.MessageType_NORMAL,
			Attributes: map[string]*anypb.Any{
				"group_id": mustPackAny(c.config.GroupID),
				"topics":   mustPackAny(strings.Join(c.config.Topics, ",")),
			},
		},
	}

	// 发送订阅请求
	resp, err := c.client.Send("subscribe", req)
	if err != nil {
		return fmt.Errorf("发送订阅请求失败: %w", err)
	}

	if resp.Status == pb.Status_ERROR {
		return fmt.Errorf("订阅失败: %s", resp.Message)
	}

	c.subscribed = true

	// 启动消息接收循环，接收服务器推送的消息
	c.wg.Add(1)
	go c.receiveMessages()

	// 如果启用了自动提交，启动自动提交循环
	if c.config.AutoCommit && c.config.CommitInterval > 0 {
		c.wg.Add(1)
		go c.autoCommitLoop()
	}

	return nil
}

// receiveMessages 接收消息
func (c *Consumer) receiveMessages() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopCh:
			return
		default:
			// 从客户端连接接收消息
			msg, err := c.client.Receive()
			if err != nil {
				log.Printf("接收消息失败: %v", err)
				time.Sleep(time.Second) // 出错后等待一段时间再重试
				continue
			}

			// 检查消息类型，确保是普通消息而不是控制消息
			if msg.Message.Type != pb.MessageType_NORMAL {
				continue
			}

			// 发送到消息通道
			select {
			case c.messages <- msg:
				// 消息成功发送到通道
			default:
				// 通道已满，记录日志
				log.Printf("消息通道已满，丢弃消息: %s", msg.Message.Id)
			}
		}
	}
}

// Pull 拉取消息
func (c *Consumer) Pull(timeout time.Duration) []*protocol.Message {
	if !c.subscribed {
		if err := c.Subscribe(); err != nil {
			log.Printf("自动订阅失败: %v", err)
			return nil
		}
	}

	var messages []*protocol.Message
	timeoutCh := time.After(timeout)

	// 收集消息直到超时或达到最大消息数
	for {
		select {
		case msg := <-c.messages:
			messages = append(messages, msg)
			if len(messages) >= c.config.MaxPullRecords {
				return messages
			}
		case <-timeoutCh:
			return messages
		}
	}
}

// Commit 提交消息确认
func (c *Consumer) Commit(messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	// 创建确认请求
	req := &protocol.Message{
		Message: &pb.Message{
			Type: pb.MessageType_NORMAL,
			Attributes: map[string]*anypb.Any{
				"group_id":    mustPackAny(c.config.GroupID),
				"message_ids": mustPackAny(strings.Join(messageIDs, ",")),
			},
		},
	}

	// 发送确认请求
	resp, err := c.client.Send("ack", req)
	if err != nil {
		return fmt.Errorf("发送确认请求失败: %w", err)
	}

	if resp.Status == pb.Status_ERROR {
		return fmt.Errorf("确认失败: %s", resp.Message)
	}

	return nil
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.subscribed {
		return nil
	}

	// 发送停止信号
	close(c.stopCh)

	// 等待所有协程退出
	c.wg.Wait()

	// 关闭消息通道
	close(c.messages)

	// 关闭客户端连接
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("关闭客户端连接失败: %w", err)
	}

	c.subscribed = false
	return nil
}

// autoCommitLoop 自动提交确认的循环
func (c *Consumer) autoCommitLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.CommitInterval)
	defer ticker.Stop()

	var pendingMessages []string

	for {
		select {
		case <-c.stopCh:
			// 在退出前提交所有待确认的消息
			if len(pendingMessages) > 0 {
				if err := c.Commit(pendingMessages); err != nil {
					log.Printf("自动提交失败: %v", err)
				}
			}
			return
		case <-ticker.C:
			// 定时提交
			if len(pendingMessages) > 0 {
				if err := c.Commit(pendingMessages); err != nil {
					log.Printf("自动提交失败: %v", err)
				} else {
					// 提交成功后清空待确认列表
					pendingMessages = nil
				}
			}
		case msg := <-c.messages:
			// 记录消息ID，等待自动提交
			pendingMessages = append(pendingMessages, msg.Message.Id)

			// 将消息放回通道，供 Pull 方法获取
			c.messages <- msg
		}
	}
}

// mustPackAny 将值打包为 Any 类型
func mustPackAny(value interface{}) *anypb.Any {
	var any *anypb.Any
	var err error

	switch v := value.(type) {
	case string:
		any, err = anypb.New(&pb.Response{Message: v})
	default:
		panic(fmt.Sprintf("unsupported type: %T", value))
	}

	if err != nil {
		panic(err)
	}

	return any
}

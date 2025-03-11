package consumer

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/network"
	pb "github.com/eason-lee/lmq/proto"
)

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	Brokers        []string      // broker 节点地址列表
	GroupID        string        // 消费者组ID
	Topics         []string      // 订阅的主题列表
	AutoCommit     bool          // 是否自动提交确认
	CommitInterval time.Duration // 自动提交间隔
	MaxPullRecords int           // 单次拉取的最大消息数
	PullInterval   time.Duration // 拉取间隔
}

// Consumer 消息消费者
type Consumer struct {
	config     *ConsumerConfig
	client     *network.Client
	subscribed bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
	handler    func([]*pb.Message)
	consumerID string       // 消费者唯一标识
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

	if config.PullInterval <= 0 {
		config.PullInterval = time.Second
	}

	// 创建到 broker 的连接
	client, err := network.NewClient(config.Brokers[0]) // 暂时只连接第一个 broker
	if err != nil {
		return nil, fmt.Errorf("连接 broker 失败: %w", err)
	}

	// 生成唯一的消费者ID
	consumerID := fmt.Sprintf("%s-%d", config.GroupID, time.Now().UnixNano())

	return &Consumer{
		config:     config,
		client:     client,
		stopCh:     make(chan struct{}),
		consumerID: consumerID,
	}, nil
}

// Subscribe 订阅主题并开始消费
func (c *Consumer) Subscribe(handler func([]*pb.Message)) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subscribed {
		return nil // 已经订阅过了
	}

	// 创建订阅请求
	req := &pb.SubscribeRequest{
		GroupId: c.config.GroupID,
		Topics:  c.config.Topics,
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
	c.handler = handler

	// 启动消息拉取循环
	c.wg.Add(1)
	go c.pullLoop()

	return nil
}

// pullLoop 消息拉取循环
func (c *Consumer) pullLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.PullInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			for _, topic := range c.config.Topics {
				messages, err := c.pull(topic)
				if err != nil {
					log.Printf("拉取消息失败: %v", err)
					continue
				}

				if len(messages) > 0 {
					// 处理消息
					c.handler(messages)

					// 如果启用了自动提交，提交消息确认
					if c.config.AutoCommit {
						lastMsg := messages[len(messages)-1]
						if err := c.Commit(topic, []string{lastMsg.Id}); err != nil {
							log.Printf("自动提交失败: %v", err)
						}
					}
				}
			}
		}
	}
}

// pull 从指定主题拉取消息
func (c *Consumer) pull(topic string) ([]*pb.Message, error) {
	// 创建拉取请求
	req := &pb.PullRequest{
		GroupId:     c.config.GroupID,
		Topic:       topic,
		MaxMessages: int32(c.config.MaxPullRecords),
		ConsumerId:  c.consumerID,
	}

	// 发送拉取请求
	resp, err := c.client.Send("pull", req)
	if err != nil {
		return nil, fmt.Errorf("发送拉取请求失败: %w", err)
	}

	if resp.Status == pb.Status_ERROR {
		return nil, fmt.Errorf("拉取消息失败: %s", resp.Message)
	}

	// 解析响应数据
	if resp.GetResponseData() == nil {
		return nil, fmt.Errorf("响应数据为空")
	}

	batchResp := resp.GetBatchMessagesData()
	if batchResp == nil {
		return nil, fmt.Errorf("响应数据类型错误")
	}

	return batchResp.Messages, nil
}

// Commit 提交消息确认
func (c *Consumer) Commit(topic string, messageIDs []string) error {
	// 创建确认请求
	req := &pb.AckRequest{
		GroupId:    c.config.GroupID,
		Topic:      topic,
		MessageIds: messageIDs,
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

	// 关闭客户端连接
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("关闭客户端连接失败: %w", err)
	}

	c.subscribed = false
	return nil
}

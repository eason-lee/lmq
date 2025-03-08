package lmq

import (
	"fmt"
	"time"

	"github.com/eason-lee/lmq/pkg/consumer"
	"github.com/eason-lee/lmq/pkg/producer"
)

// Config LMQ SDK 配置
type Config struct {
	// 连接配置
	Brokers []string      // broker 节点地址列表
	Timeout time.Duration // 连接超时时间

	// 生产者配置
	RetryTimes int // 发送重试次数

	// 消费者配置
	GroupID        string        // 消费者组ID
	AutoCommit     bool          // 是否自动提交确认
	CommitInterval time.Duration // 自动提交间隔
	MaxPullRecords int           // 单次拉取的最大消息数
	PullTimeout    time.Duration // 拉取超时时间
}

// Client LMQ 客户端
type Client struct {
	config   *Config
	producer *producer.Producer
	consumer *consumer.Consumer
}

// Message 消息
type Message struct {
	ID        string    // 消息ID
	Topic     string    // 主题
	Body      []byte    // 消息内容
	Timestamp time.Time // 时间戳
}

// SendResult 发送结果
type SendResult struct {
	MessageID string    // 消息ID
	Topic     string    // 主题
	Partition int       // 分区
	Timestamp time.Time // 发送时间
	Error     error     // 错误信息
}

// NewClient 创建一个新的 LMQ 客户端
func NewClient(config *Config) (*Client, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("至少需要一个 broker 地址")
	}

	// 设置默认值
	if config.Timeout <= 0 {
		config.Timeout = 5 * time.Second
	}

	if config.RetryTimes <= 0 {
		config.RetryTimes = 3
	}

	if config.MaxPullRecords <= 0 {
		config.MaxPullRecords = 100
	}

	if config.PullTimeout <= 0 {
		config.PullTimeout = 5 * time.Second
	}

	return &Client{
		config: config,
	}, nil
}

// Send 发送消息到指定主题
func (c *Client) Send(topic string, data []byte) (*SendResult, error) {
	p, err := c.getProducer()
	if err != nil {
		return nil, err
	}

	result, err := p.Send(topic, data, "")
	if err != nil {
		return nil, err
	}

	return &SendResult{
		MessageID: result.MessageID,
		Topic:     result.Topic,
		Partition: result.Partition,
		Timestamp: result.Timestamp,
		Error:     result.Error,
	}, nil
}

// SendWithKey 发送带分区键的消息到指定主题
func (c *Client) SendWithKey(topic string, data []byte, partitionKey string) (*SendResult, error) {
	p, err := c.getProducer()
	if err != nil {
		return nil, err
	}

	result, err := p.Send(topic, data, partitionKey)
	if err != nil {
		return nil, err
	}

	return &SendResult{
		MessageID: result.MessageID,
		Topic:     result.Topic,
		Partition: result.Partition,
		Timestamp: result.Timestamp,
		Error:     result.Error,
	}, nil
}

// SendAsync 异步发送消息
func (c *Client) SendAsync(topic string, data []byte, callback func(*SendResult)) {
	go func() {
		result, _ := c.Send(topic, data)
		if callback != nil {
			callback(result)
		}
	}()
}

// SendBatch 批量发送消息
func (c *Client) SendBatch(topic string, messages [][]byte) ([]*SendResult, error) {
	p, err := c.getProducer()
	if err != nil {
		return nil, err
	}

	results, err := p.SendBatch(topic, messages, "")
	if err != nil {
		return nil, err
	}

	sendResults := make([]*SendResult, len(results))
	for i, r := range results {
		sendResults[i] = &SendResult{
			MessageID: r.MessageID,
			Topic:     r.Topic,
			Partition: r.Partition,
			Timestamp: r.Timestamp,
			Error:     r.Error,
		}
	}

	return sendResults, nil
}

// Subscribe 订阅主题并消费消息
func (c *Client) Subscribe(topics []string, handler func([]*Message)) error {
	cons, err := c.getConsumer(topics)
	if err != nil {
		return err
	}

	// 订阅主题
	if err := cons.Subscribe(); err != nil {
		return fmt.Errorf("订阅主题失败: %w", err)
	}

	// 启动消费循环
	go func() {
		for {
			// 拉取消息
			messages := cons.Pull(c.config.PullTimeout)
			if len(messages) > 0 {
				// 转换消息格式
				sdkMessages := make([]*Message, len(messages))
				for i, msg := range messages {
					sdkMessages[i] = &Message{
						ID:        msg.Id,
						Topic:     msg.Topic,
						Body:      msg.Body,
						Timestamp: msg.Timestamp.AsTime(),
					}
				}
				// 处理消息
				handler(sdkMessages)
			}
		}
	}()

	return nil
}

// Close 关闭客户端
func (c *Client) Close() error {
	var lastErr error

	// 关闭生产者
	if c.producer != nil {
		if err := c.producer.Close(); err != nil {
			lastErr = err
		}
	}

	// 关闭消费者
	if c.consumer != nil {
		if err := c.consumer.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// getProducer 获取或创建生产者
func (c *Client) getProducer() (*producer.Producer, error) {
	if c.producer != nil {
		return c.producer, nil
	}

	// 创建生产者配置
	producerConfig := &producer.ProducerConfig{
		Brokers:    c.config.Brokers,
		RetryTimes: c.config.RetryTimes,
		Timeout:    c.config.Timeout,
	}

	// 创建生产者
	p, err := producer.NewProducer(producerConfig)
	if err != nil {
		return nil, fmt.Errorf("创建生产者失败: %w", err)
	}

	c.producer = p
	return p, nil
}

// getConsumer 获取或创建消费者
func (c *Client) getConsumer(topics []string) (*consumer.Consumer, error) {
	if c.consumer != nil {
		return c.consumer, nil
	}

	if c.config.GroupID == "" {
		return nil, fmt.Errorf("必须指定消费者组ID")
	}

	if len(topics) == 0 {
		return nil, fmt.Errorf("至少需要订阅一个主题")
	}

	// 创建消费者配置
	consumerConfig := &consumer.ConsumerConfig{
		Brokers:        c.config.Brokers,
		GroupID:        c.config.GroupID,
		Topics:         topics,
		AutoCommit:     c.config.AutoCommit,
		CommitInterval: c.config.CommitInterval,
		MaxPullRecords: c.config.MaxPullRecords,
		PullTimeout:    c.config.PullTimeout,
	}

	// 创建消费者
	cons, err := consumer.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("创建消费者失败: %w", err)
	}

	c.consumer = cons
	return cons, nil
}
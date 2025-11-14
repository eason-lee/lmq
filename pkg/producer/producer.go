package producer

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/network"
	pb "github.com/eason-lee/lmq/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProducerConfig 生产者配置
type ProducerConfig struct {
	Brokers    []string      // broker 节点地址列表
	RetryTimes int           // 发送重试次数
	Timeout    time.Duration // 发送超时时间
}

// Producer 消息生产者
type Producer struct {
	config  *ProducerConfig
	clients map[string]*network.Client // broker地址 -> 客户端连接
	mu      sync.RWMutex
}

// SendResult 发送结果
type SendResult struct {
	MessageID string    // 消息ID
	Topic     string    // 主题
	Partition int       // 分区
	Timestamp time.Time // 发送时间
	Error     error     // 错误信息
}

// NewProducer 创建一个新的生产者
func NewProducer(config *ProducerConfig) (*Producer, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("至少需要一个 broker 地址")
	}

	if config.RetryTimes <= 0 {
		config.RetryTimes = 3
	}

	if config.Timeout <= 0 {
		config.Timeout = 5 * time.Second
	}

	return &Producer{
		config:  config,
		clients: make(map[string]*network.Client),
	}, nil
}

func (p *Producer) newMessage(topic string, body []byte) *pb.Message {
	now := timestamppb.Now()
	return &pb.Message{
		Id:         uuid.New().String(),
		Topic:      topic,
		Body:       body,
		Timestamp:  now,
		Type:       pb.MessageType_NORMAL,
		Version:    1,
		Attributes: make(map[string]*anypb.Any),
	}
}

// Send 同步发送消息到指定主题
func (p *Producer) Send(topic string, data []byte, partitionKey string) (*SendResult, error) {
	// 选择一个可用的 broker
	broker := p.selectBroker(topic)

	// 获取或创建到 broker 的连接
	client, err := p.getClient(broker)
	if err != nil {
		return nil, fmt.Errorf("连接 broker 失败: %w", err)
	}

	// 创建消息
	msg := p.newMessage(topic, data)

	// 如果提供了分区键，设置到消息中
	if partitionKey != "" {
		msg.Attributes["partition_key"] = mustPackAny(partitionKey)
	}

	// 发送消息
	var lastErr error
	for i := 0; i < p.config.RetryTimes; i++ {
		resp, err := client.Send("publish", msg)
		if err == nil && resp.Status == pb.Status_OK {
			// 解析响应中的分区信息
			partition := 0
			if resp.Message != "" {
				partition, err = strconv.Atoi(resp.Message)
				if err != nil {
					return nil, fmt.Errorf("解析分区信息失败: %w", err)
				}
			}

			return &SendResult{
				MessageID: msg.Id,
				Topic:     topic,
				Partition: partition,
				Timestamp: msg.Timestamp.AsTime(),
				Error:     nil,
			}, nil
		}
		lastErr = err
		time.Sleep(time.Second * time.Duration(i+1)) // 指数退避
	}

	return &SendResult{
		MessageID: msg.Id,
		Topic:     topic,
		Error:     fmt.Errorf("发送消息失败，已重试 %d 次: %v", p.config.RetryTimes, lastErr),
	}, lastErr
}

// SendAsync 异步发送消息到指定主题
func (p *Producer) SendAsync(topic string, data []byte, partitionKey string, callback func(*SendResult)) {
	go func() {
		result, _ := p.Send(topic, data, partitionKey)
		if callback != nil {
			callback(result)
		}
	}()
}

// SendBatch 批量发送消息
func (p *Producer) SendBatch(topic string, messages [][]byte, partitionKey string) ([]*SendResult, error) {
	results := make([]*SendResult, len(messages))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var hasError bool

	for i, data := range messages {
		wg.Add(1)
		go func(idx int, msgData []byte) {
			defer wg.Done()
			result, err := p.Send(topic, msgData, partitionKey)

			mu.Lock()
			results[idx] = result
			if err != nil {
				hasError = true
			}
			mu.Unlock()
		}(i, data)
	}

	wg.Wait()

	if hasError {
		return results, fmt.Errorf("部分消息发送失败")
	}
	return results, nil
}

// selectBroker 选择一个 broker
func (p *Producer) selectBroker(topic string) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	brokers := p.config.Brokers
	if len(brokers) == 0 {
		return ""
	}

	// 简单策略：优先使用已建立连接的 broker，否则返回列表第一个
	for _, addr := range brokers {
		if _, ok := p.clients[addr]; ok {
			return addr
		}
	}
	return brokers[0]
}

// getClient 获取或创建到指定 broker 的连接
func (p *Producer) getClient(addr string) (*network.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if client, ok := p.clients[addr]; ok {
		return client, nil
	}

	client, err := network.NewClient(addr)
	if err != nil {
		return nil, err
	}

	p.clients[addr] = client
	return client, nil
}

// Close 关闭生产者
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for addr, client := range p.clients {
		if err := client.Close(); err != nil {
			lastErr = err
		}
		delete(p.clients, addr)
	}

	return lastErr
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

package producer

import (
    "fmt"
    "sync"
    "time"

    "github.com/eason-lee/lmq/pkg/protocol"
    "github.com/eason-lee/lmq/pkg/network"
)

type ProducerConfig struct {
    Brokers    []string      // broker 节点地址列表
    RetryTimes int           // 发送重试次数
    Timeout    time.Duration // 发送超时时间
}

type Producer struct {
    config  *ProducerConfig
    clients map[string]*network.Client // broker地址 -> 客户端连接
    mu      sync.RWMutex
}

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

// Send 发送消息到指定主题
func (p *Producer) Send(topic string, data []byte) error {
    // 选择一个可用的 broker
    broker := p.selectBroker(topic)
    
    // 获取或创建到 broker 的连接
    client, err := p.getClient(broker)
    if err != nil {
        return fmt.Errorf("连接 broker 失败: %w", err)
    }

    // 创建发送请求
    msg := protocol.NewMessage(topic, data)

    // 发送消息
    var lastErr error
    for i := 0; i < p.config.RetryTimes; i++ {
        resp, err := client.Send("publish", msg)
        if err == nil && resp.Success {
            return nil
        }
        lastErr = err
        time.Sleep(time.Second * time.Duration(i+1)) // 指数退避
    }

    return fmt.Errorf("发送消息失败，已重试 %d 次: %v", p.config.RetryTimes, lastErr)
}

// selectBroker 选择一个 broker（这里先简单实现，后续可以加入负载均衡策略）
func (p *Producer) selectBroker(topic string) string {
    // TODO: 实现更智能的 broker 选择策略
    return p.config.Brokers[0]
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

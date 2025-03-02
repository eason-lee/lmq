package consumer

import (
    "bufio"
    "encoding/json"
    "fmt"
    "net"
    "sync"
    "time"

    "github.com/eason-lee/lmq/pkg/protocol"
)

type ConsumerConfig struct {
    Brokers      []string      // broker 地址列表
    GroupID      string        // 消费者组 ID
    Topics       []string      // 订阅的主题列表
    AutoCommit   bool          // 是否自动提交
    MaxRetries   int          // 最大重试次数
    RetryTimeout time.Duration // 重试超时时间
}

type Consumer struct {
    config   *ConsumerConfig
    conn     net.Conn
    reader   *bufio.Reader
    writer   *bufio.Writer
    handlers map[string]MessageHandler
    mu       sync.RWMutex
    running  bool
    stopCh   chan struct{}
}

type MessageHandler func(*protocol.Message) error

func NewConsumer(config *ConsumerConfig) (*Consumer, error) {
    if len(config.Brokers) == 0 {
        return nil, fmt.Errorf("至少需要一个 broker 地址")
    }

    if config.GroupID == "" {
        return nil, fmt.Errorf("必须指定消费者组 ID")
    }

    if len(config.Topics) == 0 {
        return nil, fmt.Errorf("至少需要订阅一个主题")
    }

    return &Consumer{
        config:   config,
        handlers: make(map[string]MessageHandler),
        stopCh:   make(chan struct{}),
    }, nil
}

func (c *Consumer) Subscribe(topic string, handler MessageHandler) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if c.running {
        return fmt.Errorf("消费者已经在运行，不能添加新的订阅")
    }

    c.handlers[topic] = handler
    return nil
}

func (c *Consumer) Start() error {
    c.mu.Lock()
    if c.running {
        c.mu.Unlock()
        return fmt.Errorf("消费者已经在运行")
    }
    c.running = true
    c.mu.Unlock()

    // 连接 broker
    if err := c.connect(); err != nil {
        return err
    }

    // 发送订阅请求
    if err := c.sendSubscribe(); err != nil {
        return err
    }

    // 启动消息处理循环
    go c.consumeLoop()

    return nil
}

func (c *Consumer) Stop() error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if !c.running {
        return nil
    }

    close(c.stopCh)
    c.running = false
    return c.conn.Close()
}

func (c *Consumer) connect() error {
    // 简单起见，先连接第一个 broker
    conn, err := net.Dial("tcp", c.config.Brokers[0])
    if err != nil {
        return fmt.Errorf("连接 broker 失败: %w", err)
    }

    c.conn = conn
    c.reader = bufio.NewReader(conn)
    c.writer = bufio.NewWriter(conn)
    return nil
}

func (c *Consumer) sendSubscribe() error {
    req := &protocol.SubscribeRequest{
        GroupID: c.config.GroupID,
        Topics:  c.config.Topics,
    }

    data, err := json.Marshal(req)
    if err != nil {
        return err
    }

    if _, err := c.writer.Write(append(data, '\n')); err != nil {
        return err
    }

    return c.writer.Flush()
}

func (c *Consumer) consumeLoop() {
    for {
        select {
        case <-c.stopCh:
            return
        default:
            if err := c.handleMessage(); err != nil {
                fmt.Printf("处理消息失败: %v\n", err)
                time.Sleep(time.Second) // 简单的重试策略
            }
        }
    }
}

func (c *Consumer) handleMessage() error {
    // 读取消息
    line, err := c.reader.ReadBytes('\n')
    if err != nil {
        return err
    }

    var msg protocol.Message
    if err := json.Unmarshal(line, &msg); err != nil {
        return err
    }

    // 查找处理函数
    c.mu.RLock()
    handler, ok := c.handlers[msg.Topic]
    c.mu.RUnlock()

    if !ok {
        return fmt.Errorf("未找到主题 %s 的处理函数", msg.Topic)
    }

    // 处理消息
    if err := handler(&msg); err != nil {
        return err
    }

    // 如果配置了自动提交，发送确认
    if c.config.AutoCommit {
        return c.sendAck(msg.ID)
    }

    return nil
}

func (c *Consumer) sendAck(messageID string) error {
    ack := &protocol.AckRequest{
        GroupID:   c.config.GroupID,
        MessageID: messageID,
    }

    data, err := json.Marshal(ack)
    if err != nil {
        return err
    }

    if _, err := c.writer.Write(append(data, '\n')); err != nil {
        return err
    }

    return c.writer.Flush()
}
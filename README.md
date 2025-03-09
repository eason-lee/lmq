# LMQ - 分布式轻量级消息队列

LMQ 是一个用 Go 语言实现的分布式轻量级消息队列系统，旨在提供可扩展、高可用的消息传递功能。项目参考了Kafka的设计理念，实现了基本的消息发布-订阅模型，支持主题分区和消费者组等核心功能。

## 系统架构

```ascii
                                   ┌─────────────────┐
                                   │     Consul      │
                                   │  (服务发现&协调)  │
                                   └────────┬────────┘
                                           │
                     ┌────────────────────┼────────────────────┐
                     │                    │                    │
              ┌──────┴──────┐      ┌─────┴─────┐      ┌──────┴──────┐
              │   Broker-1  │      │  Broker-2 │      │   Broker-3  │
              │  (消息节点)  │◄────►│ (消息节点) │◄────►│  (消息节点)  │
              └──────┬──────┘      └─────┬─────┘      └──────┬──────┘
                     │                   │                    │
        ┌────────────┼───────────────────┼────────────────────┼────────────┐
        │            │                   │                    │            │
┌───────┴─────┐ ┌───┴───┐         ┌─────┴────┐         ┌────┴────┐ ┌─────┴─────┐
│ Producer-1  │ │Consumer│         │Producer-2│         │Consumer │ │Producer-3  │
└─────────────┘ └───────┘         └──────────┘         └─────────┘ └───────────┘
```

## 核心组件

### 1. 服务发现与协调 (Service Discovery & Coordination)

- 使用 Consul 进行服务注册、发现和健康检查
- 集群成员管理和领导者选举
- 配置中心，存储系统配置和元数据
- 分布式锁服务

### 2. Broker 节点 (Message Broker)

- 消息的接收、存储和投递
- 主题和分区管理
- 消息复制和同步
- 负载均衡
- 故障检测和恢复

### 3. 存储层 (Storage Layer)

- 基于文件系统的消息持久化
- 分区数据管理
- 索引和检索
- 数据压缩和清理

### 4. 客户端 SDK (Client SDK)

- 生产者和消费者接口
- 自动负载均衡
- 故障转移
- 消息确认和重试机制

## 核心特性

### 1. 高可用性

- Broker 节点多副本部署
- 自动故障转移
- 数据复制和同步
- 无单点故障设计

### 2. 可扩展性

- 水平扩展：动态增减节点
- 分区机制：主题可分多个分区
- 负载均衡：自动分配分区到节点
- 动态扩容：在线增加分区数

### 3. 可靠性

- 消息持久化
- 复制因子配置
- 消息确认机制
- 事务支持

### 4. 性能优化

- 批量处理
- 消息压缩
- 零拷贝传输
- 页面缓存优化


## 部署架构

### 最小部署

- 3 个 Consul 节点（保证高可用）
- 3 个 Broker 节点
- 每个主题默认 3 个分区
- 复制因子 2（1 个主副本 + 1 个从副本）


### 扩展建议

- 根据消息量水平扩展 Broker 节点
- 适当增加主题分区数
- 考虑跨数据中心部署
- 根据数据量调整存储配置

## 监控指标

### 1. 系统指标

- CPU 使用率
- 内存使用率
- 磁盘 I/O
- 网络 I/O

### 2. 业务指标

- 消息吞吐量
- 消息延迟
- 分区数量
- 副本同步状态

### 3. 客户端指标

- 生产者发送速率
- 消费者消费速率
- 连接数
- 错误率

## 参考Kafka设计的优化方向

基于对当前LMQ系统的分析，参照Kafka的设计理念，我们需要在以下几个方面进行优化：

### 1. 存储与分区优化

- **日志结构存储**：采用类似Kafka的日志结构存储格式，将消息顺序追加到文件中，提高写入性能
- **分段文件管理**：将分区数据拆分为多个分段文件，便于清理过期数据
- **索引机制改进**：实现稀疏索引和二分查找，加速消息检索
- **分区动态扩展**：支持在线增加分区数量，无需重启服务
- **分区重平衡**：实现自动分区重平衡算法，优化集群资源利用

### 2. 高可用与容错增强

- **ISR机制**：实现In-Sync Replicas机制，提高数据可靠性
- **Leader选举优化**：改进Leader选举算法，减少选举时间
- **副本同步策略**：支持同步和异步复制策略，平衡可靠性和性能
- **优雅降级**：在部分节点故障时保持服务可用性
- **数据一致性保证**：提供不同级别的一致性保证（至少一次、最多一次、精确一次）

### 3. 性能优化

- **批量处理增强**：优化批量发送和接收机制，减少网络开销
- **消息压缩**：支持多种压缩算法（Gzip, Snappy, LZ4），减少存储和网络带宽
- **零拷贝传输**：使用sendfile等系统调用实现零拷贝，提高传输效率
- **页缓存利用**：优化文件读写以充分利用操作系统页缓存
- **异步I/O**：使用异步I/O操作提高并发处理能力

### 4. 消费者组机制完善

- **再平衡协议**：实现完整的消费者组再平衡协议，支持动态加入/离开
- **偏移量管理**：改进消费偏移量的存储和管理机制
- **消费者心跳**：实现消费者心跳机制，及时检测消费者故障
- **静态成员**：支持静态成员ID，减少不必要的再平衡
- **增量再平衡**：实现增量再平衡算法，减少再平衡对消费的影响

### 5. 监控与运维

- **指标收集系统**：构建完整的指标收集系统，支持Prometheus集成
- **告警机制**：实现多级告警机制，及时发现系统异常
- **管理API**：提供完整的管理API，支持动态配置调整
- **可视化控制台**：开发Web控制台，简化集群管理和监控
- **配额管理**：实现生产者和消费者的配额管理，防止资源滥用

## 开发路线图

### 第一阶段：基础框架（已完成）

1. Consul 集成
2. Broker 节点服务
3. 基础存储引擎
4. 简单的客户端 SDK

### 第二阶段：核心功能（进行中）

1. 分区管理优化
2. 复制机制完善
3. 消息确认机制增强
4. 故障转移自动化

### 第三阶段：高级特性（计划中）

1. 事务支持
2. 消息过滤
3. 延迟队列增强
4. 死信队列
5. 消费者组再平衡协议

### 第四阶段：性能优化（计划中）

1. 批量处理增强
2. 多种压缩算法支持
3. 零拷贝传输实现
4. 页缓存优化
5. 异步I/O改进

## 使用示例

### 启动服务器

```go
func main() {
    // 配置 Broker
    config := &broker.BrokerConfig{
        DefaultPartitions: 3,
        addr:              "0.0.0.0:9000",
    }
    
    // 创建并启动 Broker
    broker, err := broker.NewBroker(config)
    if err != nil {
        log.Fatal(err)
    }
    
    // 使用上下文控制生命周期
    ctx := context.Background()
    if err := broker.Start(ctx); err != nil {
        log.Fatal(err)
    }
}
```

### 生产者示例

```go
func main() {
    // 创建LMQ客户端
    client, err := lmq.NewClient(&lmq.Config{
        Brokers:    []string{"localhost:9000"},
        RetryTimes: 3,
        Timeout:    5 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    // 订阅主题
    err = client.Subscribe([]string{"test-topic"}, func(messages []*protocol.Message) {
        for _, msg := range messages {
            fmt.Printf("收到消息: ID=%s, Topic=%s, Content=%s\n",
                msg.Message.Id, msg.Message.Topic, string(msg.Message.Body))
            
            // 如果未启用自动提交，需要手动确认消息
            if !client.config.AutoCommit {
                // 手动确认消息处理完成
                // 这里可以实现自定义的确认逻辑
            }
        }
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 保持程序运行，等待消息
    fmt.Println("消费者已启动，等待消息...")
    select {}
}
    
    // 发送消息
    result, err := client.Send("test-topic", []byte("Hello, LMQ!"))
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("消息已发送: ID=%s, Topic=%s, Partition=%d\n", 
        result.MessageID, result.Topic, result.Partition)
    
    // 批量发送消息
    messages := [][]byte{
        []byte("Message 1"),
        []byte("Message 2"),
        []byte("Message 3"),
    }
    results, err := client.SendBatch("test-topic", messages)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("已发送 %d 条消息\n", len(results))
}
```

### 消费者示例

```go
func main() {
    // 创建LMQ客户端
    client, err := lmq.NewClient(&lmq.Config{
        Brokers:        []string{"localhost:9000"},
        GroupID:        "test-group",
        AutoCommit:     true,
        CommitInterval: 5 * time.Second,
        MaxPullRecords: 100,
        PullInterval:   1 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    // 订阅主题
    err = client.Subscribe([]string{"test-topic"}, func(messages []*protocol.Message) {
        for _, msg := range messages {
            fmt.Printf("收到消息: ID=%s, Topic=%s, Content=%s\n",
                msg.Message.Id, msg.Message.Topic, string(msg.Message.Body))
            
            // 如果未启用自动提交，需要手动确认消息
            if !client.config.AutoCommit {
                // 手动确认消息处理完成
                // 这里可以实现自定义的确认逻辑
            }
        }
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 保持程序运行，等待消息
    fmt.Println("消费者已启动，等待消息...")
    select {}
}
    
    // 订阅主题
    err = consumer.Subscribe("test-topic", func(msg *lmq.Message) error {
        fmt.Printf("Received: %s\n", string(msg.Data))
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 开始消费
    consumer.Start()
}
```

## 贡献指南

1. Fork 项目
2. 创建特性分支
3. 提交变更
4. 发起 Pull Request

## 许可证

MIT License

# LMQ - 分布式轻量级消息队列

LMQ 是一个用 Go 语言实现的分布式轻量级消息队列系统，旨在提供可扩展、高可用的消息传递功能。

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

## 技术实现

### 1. 集群协调 (Consul)

```go
type ClusterCoordinator interface {
    // 服务注册与发现
    RegisterService(service *ServiceInfo) error
    DiscoverService(serviceName string) ([]*ServiceInfo, error)
    
    // 领导者选举
    ElectLeader(serviceName string) (bool, error)
    WatchLeader(serviceName string) (<-chan string, error)
    
    // 配置管理
    PutConfig(key string, value []byte) error
    GetConfig(key string) ([]byte, error)
    WatchConfig(key string) (<-chan []byte, error)
    
    // 分布式锁
    AcquireLock(key string) (bool, error)
    ReleaseLock(key string) error
}
```

### 2. 消息分区

```go
type PartitionManager interface {
    // 创建分区
    CreatePartition(topic string, partitionID int) error
    
    // 分配分区
    AssignPartition(partition string, broker string) error
    
    // 获取分区信息
    GetPartitionInfo(topic string, partitionID int) (*PartitionInfo, error)
    
    // 监控分区状态
    WatchPartitions() (<-chan PartitionEvent, error)
}
```

### 3. 复制机制

```go
type ReplicationManager interface {
    // 复制数据到副本
    Replicate(topic string, partition int, messages []*Message) error
    
    // 同步数据
    Sync(topic string, partition int) error
    
    // 检查复制状态
    CheckReplicationStatus(topic string, partition int) (*ReplicationStatus, error)
}
```

### 4. 存储引擎

```go
type StorageEngine interface {
    // 写入消息
    Write(partition string, messages []*Message) error
    
    // 读取消息
    Read(partition string, offset int64, count int) ([]*Message, error)
    
    // 获取分区信息
    GetPartitionMeta(partition string) (*PartitionMeta, error)
    
    // 清理过期数据
    Cleanup(partition string) error
}
```

## 部署架构

### 最小部署

- 3 个 Consul 节点（保证高可用）
- 3 个 Broker 节点
- 每个主题默认 3 个分区
- 复制因子 2（1 个主副本 + 1 个从副本）

### 推荐配置

- 硬件配置
  - CPU: 8+ 核
  - 内存: 16GB+
  - 磁盘: SSD
  - 网络: 1Gbps+

- 软件配置
  - 操作系统: Linux
  - 文件系统: ext4/xfs
  - 内核参数优化

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

## 开发路线图

### 第一阶段：基础框架

1. Consul 集成
2. Broker 节点服务
3. 基础存储引擎
4. 简单的客户端 SDK

### 第二阶段：核心功能

1. 分区管理
2. 复制机制
3. 消息确认
4. 故障转移

### 第三阶段：高级特性

1. 事务支持
2. 消息过滤
3. 延迟队列
4. 死信队列

### 第四阶段：性能优化

1. 批量处理
2. 消息压缩
3. 零拷贝
4. 缓存优化

## 使用示例

### 启动服务器

```go
func main() {
    // 配置 Broker
    config := &broker.Config{
        NodeID:    "broker-1",
        DataDir:   "/data/lmq",
        ConsulConfig: &consul.Config{
            Address: "localhost:8500",
        },
    }
    
    // 创建并启动 Broker
    broker, err := broker.NewBroker(config)
    if err != nil {
        log.Fatal(err)
    }
    
    broker.Start()
}
```

### 生产者示例

```go
func main() {
    // 创建生产者
    producer, err := lmq.NewProducer(&lmq.ProducerConfig{
        Brokers: []string{"localhost:9000"},
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 发送消息
    err = producer.Send("test-topic", []byte("Hello, LMQ!"))
    if err != nil {
        log.Fatal(err)
    }
}
```

### 消费者示例

```go
func main() {
    // 创建消费者
    consumer, err := lmq.NewConsumer(&lmq.ConsumerConfig{
        Brokers: []string{"localhost:9000"},
        Group:   "test-group",
    })
    if err != nil {
        log.Fatal(err)
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

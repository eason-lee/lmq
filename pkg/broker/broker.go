package broker

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/eason-lee/lmq/pkg/cluster"
	"github.com/eason-lee/lmq/pkg/coordinator"
	"github.com/eason-lee/lmq/pkg/network"
	"github.com/eason-lee/lmq/pkg/store"
	"github.com/eason-lee/lmq/pkg/ut"
	pb "github.com/eason-lee/lmq/proto"
    "github.com/google/uuid"
    "github.com/kevwan/mapreduce/v2"
    "google.golang.org/protobuf/types/known/timestamppb"
    "google.golang.org/protobuf/types/known/anypb"
)

// Subscriber 表示一个订阅者
type Subscriber struct {
	ID        string
	GroupID   string
	Topics    []string
	Filters   map[string]string
	BatchSize int32
	Timeout   time.Duration
	Ch        chan *pb.Message
}

// Broker 消息代理，负责消息的路由和分发
type Broker struct {
	nodeID          string
	store           store.Store
	subscribers     map[string][]*Subscriber // 主题 -> 订阅者列表
	mu              sync.RWMutex
	unackedMessages map[string]*pb.Message  // 消息ID -> 消息
	clusterMgr      *cluster.ClusterManager // 集群管理器
	coordinator     coordinator.Coordinator // 协调器
	stopCh          chan struct{}           // 停止信号
	server          *network.Server         // 网络服务器
	delayedMsgs     *DelayedMessageQueue    // 延迟消息队列
	config          *BrokerConfig           // 代理配置
}

// BrokerConfig 代理配置
type BrokerConfig struct {
    Addr              string
    DefaultPartitions int // 默认分区数量
    ConsulAddr        string // Consul 地址
}

// DelayedMessageQueue 延迟消息队列
type DelayedMessageQueue struct {
	messages map[string]*pb.Message // 消息ID -> 消息
	mu       sync.RWMutex
}

// NewDelayedMessageQueue 创建新的延迟消息队列
func NewDelayedMessageQueue() *DelayedMessageQueue {
	return &DelayedMessageQueue{
		messages: make(map[string]*pb.Message),
	}
}

// Add 添加延迟消息
func (q *DelayedMessageQueue) Add(msg *pb.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages[msg.Id] = msg
}

// Remove 移除延迟消息
func (q *DelayedMessageQueue) Remove(msgID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.messages, msgID)
}

// GetReadyMessages 获取准备投递的消息
func (q *DelayedMessageQueue) GetReadyMessages() []*pb.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	var readyMsgs []*pb.Message
	for id, msg := range q.messages {
		if shouldDeliver(msg) {
			readyMsgs = append(readyMsgs, msg)
			delete(q.messages, id)
		}
	}
	return readyMsgs
}

// generateNodeID 根据地址和时间戳生成唯一的节点ID
func generateNodeID(addr string) string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	cleanAddr := strings.ReplaceAll(addr, ":", "-")
	timestamp := time.Now().UnixNano()
	uniqueID := fmt.Sprintf("%s-%s-%d", hostname, cleanAddr, timestamp)

	hasher := md5.New()
	hasher.Write([]byte(uniqueID))
	hashBytes := hasher.Sum(nil)

	return fmt.Sprintf("%x", hashBytes)[:12]
}

// generateStoreDir 根据节点ID生成存储目录
func generateStoreDir(nodeID string) string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/tmp"
	}

	storeDir := filepath.Join(homeDir, ".lmq", "data", nodeID)

	if err := os.MkdirAll(storeDir, 0755); err != nil {
		log.Printf("创建存储目录失败: %v，将使用临时目录", err)
		storeDir = filepath.Join(os.TempDir(), "lmq-data-"+nodeID)
		os.MkdirAll(storeDir, 0755)
	}

	log.Printf("消息存储目录: %s", storeDir)
	return storeDir
}

// NewBroker 创建新的broker实例
func NewBroker(config *BrokerConfig) (*Broker, error) {
    // 使用默认配置
    if config == nil {
        config = &BrokerConfig{
            DefaultPartitions: 3, // 默认3个分区
            Addr:              "0.0.0.0:9000",
            ConsulAddr:        "127.0.0.1:8500",
        }
    }

    nodeID := generateNodeID(config.Addr)
    storeDir := generateStoreDir(nodeID)

	fileStore, err := store.NewFileStore(storeDir)
	if err != nil {
		return nil, err
	}

    consulCoord, err := coordinator.NewConsulCoordinator(config.ConsulAddr)
    if err != nil {
        return nil, fmt.Errorf("创建协调器失败: %w", err)
    }

	broker := &Broker{
		nodeID:          nodeID,
		store:           fileStore,
		subscribers:     make(map[string][]*Subscriber),
		unackedMessages: make(map[string]*pb.Message),
		coordinator:     consulCoord,
		stopCh:          make(chan struct{}),
		delayedMsgs:     NewDelayedMessageQueue(),
		config:          config,
	}

    server, err := network.NewServer(config.Addr, broker)
	if err != nil {
		return nil, fmt.Errorf("创建网络服务器失败: %w", err)
	}
	broker.server = server

    broker.clusterMgr = cluster.NewClusterManager(nodeID, config.Addr, fileStore, consulCoord)
	broker.clusterMgr.RegisterHandlers(server)

	return broker, nil
}

// StartStorageSyncTask 启动存储同步任务
func (b *Broker) StartStorageSyncTask(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := b.store.Sync(); err != nil {
					log.Printf("同步存储数据失败: %v", err)
				}
			case <-b.stopCh:
				return
			}
		}
	}()
}

// Start 启动broker服务
func (b *Broker) Start(ctx context.Context) error {
	if err := b.server.Start(); err != nil {
		return fmt.Errorf("启动网络服务器失败: %w", err)
	}

	if err := b.clusterMgr.Start(ctx); err != nil {
		return err
	}

	// 启动延迟消息处理器
	b.StartDelayedMessageProcessor(1 * time.Second)

	// 启动段清理任务，使用默认的清理策略
	b.StartCleanupTask(1*time.Hour, store.DefaultCleanupPolicy)

	// 启动存储同步任务，每5秒同步一次
	b.StartStorageSyncTask(5 * time.Second)

	// 启动节点同步任务
	go b.startNodeSyncTask(ctx, 10*time.Second)

    log.Printf("LMQ broker已启动，节点ID: %s, 监听地址: %s", b.nodeID, b.config.Addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	b.stopCh <- struct{}{}

	log.Println("收到退出信号，broker正在关闭...")

	return nil
}

// HandlePublish 处理发布请求
func (b *Broker) HandlePublish(ctx context.Context, req *pb.PublishRequest) error {
	if err := b.checkTopic(req.Topic); err != nil {
		return err
	}

	msg := &pb.Message{
		Id:         uuid.New().String(),
		Topic:      req.Topic,
		Body:       req.Body,
		Type:       req.Type,
		Attributes: req.Attributes,
		Timestamp:  timestamppb.New(time.Now()),
	}

    partitionID, err := b.SelectPartition(ctx, req.Topic, msg.Id)
    if err != nil {
        return fmt.Errorf("选择分区失败: %w", err)
    }

	// 获取分区信息
	partition, err := b.coordinator.GetPartition(ctx, req.Topic, partitionID)
	if err != nil {
		return fmt.Errorf("获取分区信息失败: %w", err)
	}

	if partition == nil {
		return fmt.Errorf("分区 %s-%d 不存在", req.Topic, partitionID)
	}

    // 写入消息
    msg.Partition = int32(partitionID)
    if err := b.store.Write(req.Topic, partitionID, []*pb.Message{msg}); err != nil {
        return fmt.Errorf("写入消息失败: %w", err)
    }

	// 检查当前节点是否是分区的leader
	isLeader := partition.Leader == b.nodeID

	// 如果当前节点是leader，则复制消息到follower节点
	if isLeader {
		// 获取分区的follower节点
		followers := partition.Followers
		if len(followers) == 0 {
			return nil
		}

		fn := func(nodeID string) func() error {
			return func() error {
				// 获取follower节点的客户端
				client, err := b.clusterMgr.GetOrCreateClient(nodeID)
				if err != nil {
					return err
				}

				// 构建复制请求
                replicationReq := &pb.Request{
                    Type: "replication",
                    RequestData: &pb.Request_PublishData{
                        PublishData: &pb.PublishRequest{
                            Topic:      req.Topic,
                            Body:       msg.Body,
                            Type:       msg.Type,
                            Attributes: addPartitionAttr(msg.Attributes, partitionID),
                        },
                    },
                }

				// 发送复制请求
				resp, err := client.Send("replication", replicationReq)
				if err != nil {
					return err
				}

                if resp.Status != pb.Status_OK {
                    return fmt.Errorf("复制失败: status=%v, message=%s", resp.Status, resp.Message)
                }
                return nil
			}
		}

		err := mapreduce.Finish(ut.Map(followers, fn)...)
		if err != nil {
			return err
		}

	}

	return nil
}

// HandleSubscribe 处理订阅请求
func (b *Broker) HandleSubscribe(ctx context.Context, req *pb.SubscribeRequest) error {
	// 检查 group_id 是否为空
	if req.GroupId == "" {
		return fmt.Errorf("group_id 不能为空")
	}

	// 检查 topics 是否为空
	if len(req.Topics) == 0 {
		return fmt.Errorf("topics 不能为空")
	}

	// 检查每个 topic 是否存在，如果不存在则创建
	for _, topic := range req.Topics {
		exists, err := b.coordinator.TopicExists(ctx, topic)
		if err != nil {
			return fmt.Errorf("检查主题是否存在失败: %w", err)
		}

		if !exists {
			// 主题不存在，自动创建
			log.Printf("主题 %s 不存在，自动创建", topic)

			// 创建主题元数据
			if err := b.coordinator.CreateTopic(ctx, topic, b.config.DefaultPartitions); err != nil {
				return fmt.Errorf("自动创建主题失败: %w", err)
			}

			// 使用分区管理器创建分区并应用重平衡策略
			partitionManager := coordinator.NewPartitionManager(b.coordinator, store.DefaultRebalancer)
			if err := partitionManager.CreateTopicPartitions(ctx, topic, b.config.DefaultPartitions, 1); err != nil { // 默认复制因子为1
				return fmt.Errorf("创建主题分区失败: %w", err)
			}
			log.Printf("已为主题 %s 创建 %d 个分区，使用 %s 重平衡策略", topic, b.config.DefaultPartitions, store.DefaultRebalancer.Name())
		}
	}

	// 注册消费者组
	if err := b.coordinator.RegisterConsumer(ctx, req.GroupId, req.Topics); err != nil {
		return fmt.Errorf("注册消费者组失败: %w", err)
	}

	// 为消费者组分配分区
	if err := b.assignPartitionsToConsumerGroup(ctx, req.GroupId, req.Topics); err != nil {
		return fmt.Errorf("分配分区失败: %w", err)
	}

	return nil
}

// assignPartitionsToConsumerGroup 为消费者组分配分区
func (b *Broker) assignPartitionsToConsumerGroup(ctx context.Context, groupID string, topics []string) error {
	// 获取消费者组中的所有消费者
	consumers, err := b.coordinator.GetConsumersInGroup(ctx, groupID)
	if err != nil {
		return fmt.Errorf("获取消费者组成员失败: %w", err)
	}

	// 如果没有消费者，不需要分配
	if len(consumers) == 0 {
		return nil
	}

	// 为每个主题分配分区
	for _, topic := range topics {
		// 获取主题的所有分区
		partitions, err := b.coordinator.GetTopicPartitions(ctx, topic)
		if err != nil {
			return fmt.Errorf("获取主题分区失败: %w", err)
		}

		// 使用轮询策略分配分区给消费者
		// 这里可以实现更复杂的分配策略，如粘性分配、范围分配等
		for i, partition := range partitions {
			consumerIndex := i % len(consumers)
			consumerID := consumers[consumerIndex]

			// 将分区分配给消费者
			if err := b.coordinator.AssignPartitionToConsumer(ctx, topic, partition.ID, groupID, consumerID); err != nil {
				return fmt.Errorf("分配分区失败: %w", err)
			}
		}
	}

	// 触发消费者组再平衡事件
	if err := b.coordinator.TriggerGroupRebalance(ctx, groupID); err != nil {
		return fmt.Errorf("触发消费者组再平衡失败: %w", err)
	}

	return nil
}

// HandlePull 处理拉取请求
func (b *Broker) HandlePull(ctx context.Context, req *pb.PullRequest) (*pb.Response, error) {
	// 检查 group_id 是否为空
	if req.GroupId == "" {
		return nil, fmt.Errorf("group_id 不能为空")
	}

	// 检查 topic 是否为空
	if req.Topic == "" {
		return nil, fmt.Errorf("topic 不能为空")
	}

	// 检查 topic 是否存在
	if err := b.checkTopic(req.Topic); err != nil {
		return nil, err
	}

	// 获取消费者组的分区
	partitions, err := b.coordinator.GetConsumerAssignedPartitions(ctx, req.GroupId, req.ConsumerId, req.Topic)
	if err != nil {
		return nil, fmt.Errorf("获取分配的分区失败: %w", err)
	}

	// 如果没有分配分区，返回空响应
	if len(partitions) == 0 {
		return &pb.Response{
			Status:  pb.Status_OK,
			Message: "没有分配的分区",
		}, nil
	}

	var allMessages []*pb.Message

	// 从每个分配的分区读取消息
	for _, partition := range partitions {
		// 获取该分区的消费位置
		offset, err := b.coordinator.GetConsumerOffset(ctx, req.GroupId, req.Topic, partition)
		if err != nil {
			return nil, fmt.Errorf("获取消费位置失败: %w", err)
		}

		// 读取消息
		messages, err := b.store.Read(req.Topic, partition, offset, int(req.MaxMessages)/len(partitions))
		if err != nil {
			return nil, fmt.Errorf("读取消息失败: %w", err)
		}

		allMessages = append(allMessages, messages...)
	}

	// 构造响应
	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "拉取消息成功",
		ResponseData: &pb.Response_BatchMessagesData{
			BatchMessagesData: &pb.BatchMessagesResponse{
				Messages: allMessages,
			},
		},
	}, nil
}

// HandleAck 处理确认请求
func (b *Broker) HandleAck(ctx context.Context, req *pb.AckRequest) error {
	// 检查 group_id 是否为空
	if req.GroupId == "" {
		return fmt.Errorf("group_id 不能为空")
	}

	// 检查 topic 是否为空
	if req.Topic == "" {
		return fmt.Errorf("topic 不能为空")
	}

	// 检查 message_ids 是否为空
	if len(req.MessageIds) == 0 {
		return fmt.Errorf("message_ids 不能为空")
	}

	// 检查 topic 是否存在
	if err := b.checkTopic(req.Topic); err != nil {
		return err
	}

	// 获取最后一条消息的 offset
	lastOffset, err := b.store.GetOffset(req.Topic, req.MessageIds[len(req.MessageIds)-1])
	if err != nil {
		return fmt.Errorf("获取消息 offset 失败: %w", err)
	}

	// 提交消费位置
	if err := b.coordinator.CommitOffset(ctx, req.GroupId, req.Topic, lastOffset+1); err != nil {
		return fmt.Errorf("提交消费位置失败: %w", err)
	}

	return nil
}

// checkTopic 检查 topic 是否存在
func (b *Broker) checkTopic(topic string) error {
	if topic == "" {
		return fmt.Errorf("topic 不能为空")
	}

	exists, err := b.coordinator.TopicExists(context.Background(), topic)
	if err != nil {
		return fmt.Errorf("检查 topic 是否存在失败: %w", err)
	}

	if !exists {
		return fmt.Errorf("topic %s 不存在", topic)
	}

	return nil
}

// StartDelayedMessageProcessor 启动延迟消息处理器
func (b *Broker) StartDelayedMessageProcessor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				readyMsgs := b.delayedMsgs.GetReadyMessages()
				for _, msg := range readyMsgs {
					req := &pb.PublishRequest{
						Topic: msg.Topic,
						Body:  msg.Body,
						Type:  pb.MessageType_NORMAL,
					}
					if err := b.HandlePublish(context.Background(), req); err != nil {
						log.Printf("投递延迟消息失败: %v", err)
						// 重新加入延迟队列
						b.delayedMsgs.Add(msg)
					}
				}
			case <-b.stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

// 从 Consul 发现其他节点并同步到集群管理器
func (b *Broker) syncNodesFromConsul(ctx context.Context) error {
	services, err := b.coordinator.DiscoverService(ctx, "lmq-broker")
	if err != nil {
		return fmt.Errorf("发现服务失败: %w", err)
	}

	// 构建节点地址映射
	nodes := make(map[string]string)
	for _, service := range services {
		// 跳过自己
		if service.ID == b.nodeID {
			continue
		}

		// 构建节点地址
		nodeAddr := fmt.Sprintf("%s:%d", service.Address, service.Port)
		nodes[service.ID] = nodeAddr
	}

	// 同步到集群管理器
	b.clusterMgr.SyncNodes(nodes)

	return nil
}

// 启动定期同步节点信息的任务
func (b *Broker) startNodeSyncTask(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			if err := b.syncNodesFromConsul(ctx); err != nil {
				log.Printf("同步节点信息失败: %v", err)
			}
		}
	}
}

// SelectPartition 根据消息ID选择分区
func (b *Broker) SelectPartition(ctx context.Context, topic string, messageID string) (int, error) {
	// 获取主题的分区数量
	partitionCount, err := b.coordinator.GetTopicPartitionCount(ctx, topic)
	if err != nil {
		return 0, err
	}

	// 简单的哈希分区
	hash := 0
	for _, c := range messageID {
		hash = 31*hash + int(c)
	}

	return (hash & 0x7FFFFFFF) % partitionCount, nil
}

// shouldDeliver 检查延迟消息是否应该投递
func shouldDeliver(msg *pb.Message) bool {
	if msg.Type != pb.MessageType_DELAYED {
		return true
	}

	// 获取延迟时间
	delayAttr, exists := msg.Attributes["delay"]
	if !exists {
		return true
	}

	var pubReq pb.PublishRequest
	if err := delayAttr.UnmarshalTo(&pubReq); err != nil {
		return true
	}

	deliveryTime := msg.Timestamp.AsTime().Add(time.Duration(pubReq.DelaySeconds) * time.Second)
	return time.Now().After(deliveryTime)
}
func addPartitionAttr(attrs map[string]*anypb.Any, partitionID int) map[string]*anypb.Any {
    if attrs == nil {
        attrs = make(map[string]*anypb.Any)
    }
    anyVal, _ := anypb.New(&pb.Response{Message: fmt.Sprintf("%d", partitionID)})
    attrs["partition_id"] = anyVal
    return attrs
}

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
	"github.com/eason-lee/lmq/pkg/protocol"
	"github.com/eason-lee/lmq/pkg/store"
	pb "github.com/eason-lee/lmq/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Subscriber 表示一个订阅者
type Subscriber struct {
	ID        string
	GroupID   string
	Topics    []string
	Filters   map[string]string
	BatchSize int32
	Timeout   time.Duration
	Ch        chan *protocol.Message
}

// Broker 消息代理，负责消息的路由和分发
type Broker struct {
	nodeID          string
	addr            string
	store           store.Store
	subscribers     map[string][]*Subscriber // 主题 -> 订阅者列表
	mu              sync.RWMutex
	unackedMessages map[string]*protocol.Message // 消息ID -> 消息
	unackedMu       sync.RWMutex
	clusterMgr      *cluster.ClusterManager // 集群管理器
	coordinator     coordinator.Coordinator // 协调器
	stopCh          chan struct{}           // 停止信号
	server          *network.Server         // 网络服务器
	delayedMsgs     *DelayedMessageQueue    // 延迟消息队列
}

// DelayedMessageQueue 延迟消息队列
type DelayedMessageQueue struct {
	messages map[string]*protocol.Message // 消息ID -> 消息
	mu       sync.RWMutex
}

// NewDelayedMessageQueue 创建新的延迟消息队列
func NewDelayedMessageQueue() *DelayedMessageQueue {
	return &DelayedMessageQueue{
		messages: make(map[string]*protocol.Message),
	}
}

// Add 添加延迟消息
func (q *DelayedMessageQueue) Add(msg *protocol.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages[msg.Message.Id] = msg
}

// Remove 移除延迟消息
func (q *DelayedMessageQueue) Remove(msgID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.messages, msgID)
}

// GetReadyMessages 获取准备投递的消息
func (q *DelayedMessageQueue) GetReadyMessages() []*protocol.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	var readyMsgs []*protocol.Message
	for id, msg := range q.messages {
		if msg.ShouldDeliver() {
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
func NewBroker(addr string) (*Broker, error) {
	if addr == "" {
		addr = "0.0.0.0:9000"
	}

	nodeID := generateNodeID(addr)
	storeDir := generateStoreDir(nodeID)

	fileStore, err := store.NewFileStore(storeDir)
	if err != nil {
		return nil, err
	}

	consulCoord, err := coordinator.NewConsulCoordinator("localhost:8500")
	if err != nil {
		return nil, fmt.Errorf("创建协调器失败: %w", err)
	}

	broker := &Broker{
		nodeID:          nodeID,
		addr:            addr,
		store:           fileStore,
		subscribers:     make(map[string][]*Subscriber),
		unackedMessages: make(map[string]*protocol.Message),
		coordinator:     consulCoord,
		stopCh:          make(chan struct{}),
		delayedMsgs:     NewDelayedMessageQueue(),
	}

	server, err := network.NewServer(addr, broker)
	if err != nil {
		return nil, fmt.Errorf("创建网络服务器失败: %w", err)
	}
	broker.server = server

	broker.clusterMgr = cluster.NewClusterManager(nodeID, addr, fileStore, consulCoord)
	broker.clusterMgr.RegisterHandlers(server)

	return broker, nil
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
	
	// 启动节点同步任务
	go b.startNodeSyncTask(ctx, 10*time.Second)

	log.Printf("LMQ broker已启动，节点ID: %s, 监听地址: %s", b.nodeID, b.addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	b.stopCh <- struct{}{}

	log.Println("收到退出信号，broker正在关闭...")

	return nil
}

// HandlePublish 处理发布请求
func (b *Broker) HandlePublish(ctx context.Context, req *pb.PublishRequest) error {
	// 检查 topic 是否存在
	if err := b.checkTopic(req.Topic); err != nil {
		return err
	}

	// 构造消息
	msg := &protocol.Message{
		Message: &pb.Message{
			Id:         uuid.New().String(),
			Topic:      req.Topic,
			Body:       req.Body,
			Type:       req.Type,
			Attributes: req.Attributes,
			Timestamp:  timestamppb.New(time.Now()),
		},
	}

	// 获取主题的所有分区
	partitions, err := b.coordinator.GetPartitions(ctx, req.Topic)
	if err != nil {
		return fmt.Errorf("获取分区列表失败: %w", err)
	}

	if len(partitions) == 0 {
		return fmt.Errorf("主题 %s 没有可用分区", req.Topic)
	}

	// 简单的分区选择策略：使用消息ID的哈希值对分区数量取模
	partitionIndex := int(crc32Hash(msg.Message.Id) % uint32(len(partitions)))
	partition := partitions[partitionIndex]

	// 写入消息
	if err := b.store.Write(req.Topic, partition.ID, []*protocol.Message{msg}); err != nil {
		return fmt.Errorf("写入消息失败: %w", err)
	}

	return nil
}

// crc32Hash 计算字符串的CRC32哈希值
func crc32Hash(s string) uint32 {
	var h uint32
	for i := 0; i < len(s); i++ {
		h = h*31 + uint32(s[i])
	}
	return h
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
			if err := b.coordinator.CreateTopic(ctx, topic, 1); err != nil { // 默认创建1个分区
				return fmt.Errorf("自动创建主题失败: %w", err)
			}
		}
	}

	// 注册消费者组
	if err := b.coordinator.RegisterConsumer(ctx, req.GroupId, req.Topics); err != nil {
		return fmt.Errorf("注册消费者组失败: %w", err)
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

	// 获取消费者组的消费位置
	offset, err := b.coordinator.GetConsumerOffset(ctx, req.GroupId, req.Topic)
	if err != nil {
		return nil, fmt.Errorf("获取消费位置失败: %w", err)
	}

	// 获取消费者组的分区
	partition, err := b.coordinator.GetConsumerPartition(ctx, req.GroupId, req.Topic)
	if err != nil {
		return nil, fmt.Errorf("获取分区失败: %w", err)
	}

	// 读取消息
	messages, err := b.store.Read(req.Topic, partition, offset, int(req.MaxMessages))
	if err != nil {
		return nil, fmt.Errorf("读取消息失败: %w", err)
	}

	// 如果没有消息，返回空响应
	if len(messages) == 0 {
		return &pb.Response{
			Status:  pb.Status_OK,
			Message: "没有新消息",
		}, nil
	}

	// 转换消息格式
	pbMessages := make([]*pb.Message, len(messages))
	for i, msg := range messages {
		pbMessages[i] = msg.Message
	}

	// 构造响应
	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "拉取消息成功",
		ResponseData: &pb.Response_BatchMessagesData{
			BatchMessagesData: &pb.BatchMessagesResponse{
				Messages: pbMessages,
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
						Topic: msg.Message.Topic,
						Body:  msg.Message.Body,
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

package broker

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"log"
	"net"
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
	"google.golang.org/protobuf/proto"
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
	Ch        chan *protocol.Message
}

// Broker 消息代理，负责消息的路由和分发
type Broker struct {
	nodeID          string
	addr            string
	store           *store.FileStore
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

	server := network.NewServer(addr)

	broker := &Broker{
		nodeID:          nodeID,
		addr:            addr,
		store:           fileStore,
		subscribers:     make(map[string][]*Subscriber),
		unackedMessages: make(map[string]*protocol.Message),
		coordinator:     consulCoord,
		stopCh:          make(chan struct{}),
		server:          server,
		delayedMsgs:     NewDelayedMessageQueue(),
	}

	broker.clusterMgr = cluster.NewClusterManager(nodeID, addr, fileStore, consulCoord)
	broker.clusterMgr.RegisterHandlers(server)

	return broker, nil
}

// Start 启动broker服务
func (b *Broker) Start() error {
	if err := b.server.Start(); err != nil {
		return fmt.Errorf("启动网络服务器失败: %w", err)
	}

	if err := b.clusterMgr.Start(); err != nil {
		return err
	}

	// 启动延迟消息处理器
	b.StartDelayedMessageProcessor(1 * time.Second)

	log.Printf("LMQ broker已启动，节点ID: %s, 监听地址: %s", b.nodeID, b.addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	b.stopCh <- struct{}{}

	log.Println("收到退出信号，broker正在关闭...")

	return nil
}

// HandlePublish 处理发布请求
func (b *Broker) HandlePublish(ctx context.Context, req *pb.PublishRequest) (*pb.Response, error) {
	var msg *protocol.Message

	// 根据请求类型创建消息
	if req.DelaySeconds > 0 {
		msg = protocol.NewDelayedMessage(req.Topic, req.Body, req.DelaySeconds)
	} else {
		msg = protocol.NewMessage(req.Topic, req.Body)
	}

	// 设置消息属性
	msg.Message.Type = req.Type
	for k, v := range req.Attributes {
		msg.Message.Attributes[k] = v
	}

	// 处理延迟消息
	if msg.Message.Type == pb.MessageType_DELAYED {
		b.delayedMsgs.Add(msg)
		return &pb.Response{
			Status:  pb.Status_OK,
			Message: "延迟消息已接收",
		}, nil
	}

	// 选择分区并保存消息
	partition, err := b.clusterMgr.SelectPartition(req.Topic, msg.Message.Id)
	if err != nil {
		return nil, fmt.Errorf("选择分区失败: %w", err)
	}

	if err := b.store.Write(req.Topic, partition, []*protocol.Message{msg}); err != nil {
		return nil, fmt.Errorf("保存消息失败: %w", err)
	}

	// 复制到其他节点
	if err := b.clusterMgr.ReplicateMessages(req.Topic, partition, []*protocol.Message{msg}); err != nil {
		return nil, fmt.Errorf("复制消息失败: %w", err)
	}

	// 分发消息给订阅者
	b.deliverMessage(msg)

	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "消息发布成功",
	}, nil
}

// HandleSubscribe 处理订阅请求
func (b *Broker) HandleSubscribe(ctx context.Context, req *pb.SubscribeRequest) (*pb.Response, error) {
	sub := &Subscriber{
		ID:        fmt.Sprintf("%s-%s", req.GroupId, generateNodeID("")),
		GroupID:   req.GroupId,
		Topics:    req.Topics,
		Filters:   req.Filters,
		BatchSize: req.BatchSize,
		Timeout:   time.Duration(req.TimeoutSeconds) * time.Second,
		Ch:        make(chan *protocol.Message, 100),
	}

	b.mu.Lock()
	for _, topic := range req.Topics {
		b.subscribers[topic] = append(b.subscribers[topic], sub)
	}
	b.mu.Unlock()

	// 启动消息投递协程
	go b.handleSubscriber(sub)

	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "订阅成功",
	}, nil
}

// handleSubscriber 处理订阅者的消息投递
func (b *Broker) handleSubscriber(sub *Subscriber) {
	var batch []*protocol.Message
	ticker := time.NewTicker(sub.Timeout)
	defer ticker.Stop()

	for {
		select {
		case msg := <-sub.Ch:
			// 检查消息是否符合过滤条件
			if !b.matchFilters(msg, sub.Filters) {
				continue
			}

			batch = append(batch, msg)
			if int32(len(batch)) >= sub.BatchSize {
				b.deliverBatch(sub, batch)
				batch = nil
			}

		case <-ticker.C:
			if len(batch) > 0 {
				b.deliverBatch(sub, batch)
				batch = nil
			}

		case <-b.stopCh:
			return
		}
	}
}

// matchFilters 检查消息是否符合过滤条件
func (b *Broker) matchFilters(msg *protocol.Message, filters map[string]string) bool {
	if len(filters) == 0 {
		return true
	}

	for key, value := range filters {
		attr, exists := msg.Message.Attributes[key]
		if !exists {
			return false
		}

		var resp pb.Response
		if err := attr.UnmarshalTo(&resp); err != nil {
			return false
		}

		if resp.Message != value {
			return false
		}
	}

	return true
}

// deliverBatch 投递一批消息
func (b *Broker) deliverBatch(sub *Subscriber, batch []*protocol.Message) {
	resp := &pb.BatchMessagesResponse{
		Messages: make([]*pb.Message, len(batch)),
	}

	for i, msg := range batch {
		resp.Messages[i] = msg.Message

		// 记录未确认的消息
		b.unackedMu.Lock()
		b.unackedMessages[msg.Message.Id] = msg
		b.unackedMu.Unlock()
	}

	// 将响应序列化并发送给订阅者
	if data, err := anypb.New(resp); err == nil {
		_ = &pb.Response{
			Status: pb.Status_OK,
			Data:   data,
		}
		// TODO: 通过网络发送response给订阅者
	}
}

// HandleAck 处理确认请求
func (b *Broker) HandleAck(ctx context.Context, req *pb.AckRequest) (*pb.Response, error) {
	b.unackedMu.Lock()
	defer b.unackedMu.Unlock()

	for _, msgID := range req.MessageIds {
		delete(b.unackedMessages, msgID)
	}

	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "消息确认成功",
	}, nil
}

// deliverMessage 投递消息给订阅者
func (b *Broker) deliverMessage(msg *protocol.Message) {
	b.mu.RLock()
	subs := b.subscribers[msg.Message.Topic]
	b.mu.RUnlock()

	for _, sub := range subs {
		select {
		case sub.Ch <- msg:
			// 消息发送成功
		default:
			// 如果订阅者的通道已满，将消息转为死信
			deadMsg := msg.ToDeadLetter("订阅者队列已满")
			if err := b.handleDeadLetter(deadMsg); err != nil {
				log.Printf("处理死信消息失败: %v", err)
			}
		}
	}
}

// handleDeadLetter 处理死信消息
func (b *Broker) handleDeadLetter(msg *protocol.Message) error {
	deadLetterTopic := fmt.Sprintf("%s.deadletter", msg.Message.Topic)

	partition, err := b.clusterMgr.SelectPartition(deadLetterTopic, msg.Message.Id)
	if err != nil {
		return fmt.Errorf("选择死信队列分区失败: %w", err)
	}

	if err := b.store.Write(deadLetterTopic, partition, []*protocol.Message{msg}); err != nil {
		return fmt.Errorf("保存死信消息失败: %w", err)
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
					if _, err := b.HandlePublish(context.Background(), req); err != nil {
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
func (b *Broker) syncNodesFromConsul() error {
	services, err := b.coordinator.DiscoverService("lmq-broker")
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
func (b *Broker) startNodeSyncTask(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			if err := b.syncNodesFromConsul(); err != nil {
				log.Printf("同步节点信息失败: %v", err)
			}
		}
	}
}

// Publish 发布消息到指定主题
func (b *Broker) Publish(topic string, data []byte) (*protocol.Message, error) {
	// 创建消息
	msg := protocol.NewMessage(topic, data)

	// 选择分区
	partition, err := b.clusterMgr.SelectPartition(topic, msg.Message.Id)
	if err != nil {
		return nil, fmt.Errorf("选择分区失败: %w", err)
	}

	// 保存消息到分区
	if err := b.store.Write(topic, partition, []*protocol.Message{msg}); err != nil {
		return nil, fmt.Errorf("保存消息失败: %w", err)
	}

	// 复制到其他节点
	if err := b.clusterMgr.ReplicateMessages(topic, partition, []*protocol.Message{msg}); err != nil {
		return nil, fmt.Errorf("复制消息失败: %w", err)
	}

	// 分发消息给订阅者
	b.mu.RLock()
	subs := b.subscribers[topic]
	b.mu.RUnlock()

	for _, sub := range subs {
		// 非阻塞发送，避免一个慢订阅者阻塞整个系统
		select {
		case sub.Ch <- msg:
			// 消息发送成功
		default:
			// 订阅者的通道已满，可以记录日志或采取其他措施
			fmt.Printf("订阅者 %s 的通道已满，消息 %s 未能发送\n", sub.ID, msg.Message.Id)
		}
	}

	return msg, nil
}

// Subscribe 订阅指定主题
func (b *Broker) Subscribe(subID string, topics []string, bufSize int) (*Subscriber, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 创建订阅者
	sub := &Subscriber{
		ID:     subID,
		Topics: topics,
		Ch:     make(chan *protocol.Message, bufSize),
	}

	// 添加到订阅列表
	for _, topic := range topics {
		b.subscribers[topic] = append(b.subscribers[topic], sub)
	}

	return sub, nil
}

// Unsubscribe 取消订阅
func (b *Broker) Unsubscribe(subID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 从所有主题中移除该订阅者
	for topic, subs := range b.subscribers {
		var newSubs []*Subscriber
		for _, sub := range subs {
			if sub.ID != subID {
				newSubs = append(newSubs, sub)
			}
		}
		b.subscribers[topic] = newSubs
	}
}

// AckMessage 添加确认消息的方法
func (b *Broker) AckMessage(messageID string) {
	b.unackedMu.Lock()
	defer b.unackedMu.Unlock()

	delete(b.unackedMessages, messageID)
}

// RetryUnackedMessages 添加重试未确认消息的方法
func (b *Broker) RetryUnackedMessages() {
	b.unackedMu.RLock()
	messages := make([]*protocol.Message, 0, len(b.unackedMessages))
	for _, msg := range b.unackedMessages {
		messages = append(messages, msg)
	}
	b.unackedMu.RUnlock()

	for _, msg := range messages {
		// 重新发布消息
		b.Publish(msg.Message.Topic, msg.Message.Body)
	}
}

// StartRetryTask 启动定时重试任务
func (b *Broker) StartRetryTask(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			b.RetryUnackedMessages()
		}
	}()
}

// handlePublish 处理发布消息
func (b *Broker) handlePublish(msg *protocol.Message) error {
	// 选择分区
	partition, err := b.clusterMgr.SelectPartition(msg.Message.Topic, msg.Message.Id)
	if err != nil {
		return fmt.Errorf("选择分区失败: %w", err)
	}

	// 写入存储
	if err := b.store.Write(msg.Message.Topic, partition, []*protocol.Message{msg}); err != nil {
		return fmt.Errorf("存储消息失败: %w", err)
	}

	// 复制到其他节点
	if err := b.clusterMgr.ReplicateMessages(msg.Message.Topic, partition, []*protocol.Message{msg}); err != nil {
		return fmt.Errorf("复制消息失败: %w", err)
	}

	return nil
}

// selectPartition 选择消息应该发送到的分区
func (b *Broker) selectPartition(topic string, messageID string) int {
	// 使用集群管理器选择分区
	partition, err := b.clusterMgr.SelectPartition(topic, messageID)
	if err != nil {
		log.Printf("选择分区失败: %v，使用默认分区0", err)
		return 0
	}
	return partition
}

// getPartitionCount 获取主题的分区数量
func (b *Broker) getPartitionCount(topic string) int {
	// 从协调器获取分区数量
	count, err := b.coordinator.GetTopicPartitionCount(topic)
	if err != nil {
		log.Printf("获取主题分区数量失败: %v", err)
		return 1 // 默认返回1个分区
	}
	return count
}

// handleSubscribe 处理订阅请求
func (b *Broker) handleSubscribe(conn net.Conn, msg *protocol.Message) error {
	// 从消息属性中提取订阅信息
	attrs := msg.Message.Attributes

	// 提取消费者组ID
	groupIDAttr, exists := attrs["group_id"]
	if !exists {
		return fmt.Errorf("必须提供有效的消费者组ID")
	}
	var groupIDResp pb.Response
	if err := groupIDAttr.UnmarshalTo(&groupIDResp); err != nil {
		return fmt.Errorf("解析消费者组ID失败: %w", err)
	}
	groupID := groupIDResp.Message

	// 提取主题列表
	topicsAttr, exists := attrs["topics"]
	if !exists {
		return fmt.Errorf("必须提供主题列表")
	}
	var topicsResp pb.Response
	if err := topicsAttr.UnmarshalTo(&topicsResp); err != nil {
		return fmt.Errorf("解析主题列表失败: %w", err)
	}
	topics := strings.Split(topicsResp.Message, ",")

	if len(topics) == 0 {
		return fmt.Errorf("至少需要订阅一个有效主题")
	}

	// 创建唯一的订阅者ID
	subID := fmt.Sprintf("%s-%s-%d", groupID, conn.RemoteAddr().String(), time.Now().UnixNano())

	// 创建订阅者
	sub, err := b.Subscribe(subID, topics, 1000) // 使用1000作为缓冲区大小
	if err != nil {
		return fmt.Errorf("创建订阅失败: %w", err)
	}

	// 启动一个协程来处理消息推送
	go func() {
		defer b.Unsubscribe(subID)

		for msg := range sub.Ch {
			// 将消息转换为Protobuf格式
			pbMsg := &pb.Message{
				Id:        msg.Message.Id,
				Topic:     msg.Message.Topic,
				Body:      msg.Message.Body,
				Timestamp: msg.Message.Timestamp,
				Type:      msg.Message.Type,
			}

			// 序列化消息
			data, err := proto.Marshal(pbMsg)
			if err != nil {
				log.Printf("序列化消息失败: %v", err)
				continue
			}

			// 准备长度前缀
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

			// 发送消息
			b.unackedMu.Lock()
			b.unackedMessages[msg.Message.Id] = msg // 添加到未确认消息列表
			b.unackedMu.Unlock()

			// 使用互斥锁保护写操作
			var mu sync.Mutex
			mu.Lock()
			if _, err := conn.Write(lenBuf); err != nil {
				mu.Unlock()
				log.Printf("发送消息长度失败: %v", err)
				return
			}

			if _, err := conn.Write(data); err != nil {
				mu.Unlock()
				log.Printf("发送消息内容失败: %v", err)
				return
			}
			mu.Unlock()
		}
	}()

	// 发送成功响应
	resp := &pb.Response{
		Status:  pb.Status_OK,
		Message: "订阅成功",
	}

	// 序列化响应
	respData, err := proto.Marshal(resp)
	if err != nil {
		return fmt.Errorf("序列化响应失败: %w", err)
	}

	// 准备长度前缀
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(respData)))

	// 发送响应
	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("发送响应长度失败: %w", err)
	}

	if _, err := conn.Write(respData); err != nil {
		return fmt.Errorf("发送响应内容失败: %w", err)
	}

	return nil
}

// handleAck 处理消息确认
func (b *Broker) handleAck(msg *protocol.Message) error {
	// 从消息属性中提取确认信息
	attrs := msg.Message.Attributes

	// 提取消费者组ID
	groupIDAttr, exists := attrs["group_id"]
	if !exists {
		return fmt.Errorf("必须提供有效的消费者组ID")
	}
	var groupIDResp pb.Response
	if err := groupIDAttr.UnmarshalTo(&groupIDResp); err != nil {
		return fmt.Errorf("解析消费者组ID失败: %w", err)
	}
	groupID := groupIDResp.Message

	// 提取要确认的消息ID列表
	messageIDsAttr, exists := attrs["message_ids"]
	if !exists {
		return fmt.Errorf("必须提供要确认的消息ID列表")
	}
	var messageIDsResp pb.Response
	if err := messageIDsAttr.UnmarshalTo(&messageIDsResp); err != nil {
		return fmt.Errorf("解析消息ID列表失败: %w", err)
	}
	messageIDs := strings.Split(messageIDsResp.Message, ",")

	if len(messageIDs) == 0 {
		return fmt.Errorf("至少需要确认一个有效消息ID")
	}

	// 记录确认信息
	log.Printf("消费者组 %s 确认了 %d 条消息", groupID, len(messageIDs))

	// 从未确认消息列表中移除这些消息
	b.unackedMu.Lock()
	for _, msgID := range messageIDs {
		delete(b.unackedMessages, msgID)
	}
	b.unackedMu.Unlock()

	return nil
}

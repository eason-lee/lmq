package broker

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
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
	"github.com/eason-lee/lmq/pkg/protocol"
	"github.com/eason-lee/lmq/pkg/replication"
	"github.com/eason-lee/lmq/pkg/store"
	pb "github.com/eason-lee/lmq/proto"
	"google.golang.org/protobuf/proto"
)

// Subscriber 表示一个订阅者
type Subscriber struct {
	ID     string
	Topics []string
	Ch     chan *protocol.Message
}

// Broker 消息代理，负责消息的路由和分发
type Broker struct {
	nodeID          string
	addr            string
	replicaMgr      *replication.ReplicaManager
	store           *store.FileStore
	subscribers     map[string][]*Subscriber // 主题 -> 订阅者列表
	mu              sync.RWMutex
	unackedMessages map[string]*protocol.Message // 消息ID -> 消息
	unackedMu       sync.RWMutex
    clusterMgr      *cluster.ClusterManager // 新增：集群管理器
	coordinator      coordinator.Coordinator
	stopCh      chan struct{}         // 停止信号
}

// NewBroker 创建一个新的消息代理
func NewBroker(  addr string) (*Broker, error) {
	if addr == "" {
		addr = "0.0.0.0:9000" // 默认监听所有网络接口的9000端口
	}

	// 自动生成唯一的节点ID
	nodeID := generateNodeID(addr)

	// 基于节点ID生成存储目录
	storeDir := generateStoreDir(nodeID)
	
	fileStore, err := store.NewFileStore(storeDir)
	if err != nil {
		return nil, err
	}

	replicaMgr := replication.NewReplicaManager(nodeID, fileStore, addr)
	broker := &Broker{
		nodeID:          nodeID,
		addr:            addr,
		store:           fileStore,
		replicaMgr:      replicaMgr,
		subscribers:     make(map[string][]*Subscriber),
		unackedMessages: make(map[string]*protocol.Message),
	}

	// 初始化集群管理器
	broker.clusterMgr = cluster.NewClusterManager(nodeID, addr)

	return broker, nil
}

//  generateNodeID 根据地址和时间戳生成唯一的节点ID
func generateNodeID(addr string) string {
	// 使用主机名+地址+时间戳的组合生成唯一ID
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	
	// 移除地址中的冒号，避免在文件名等场景中出现问题
	cleanAddr := strings.ReplaceAll(addr, ":", "-")
	
	// 生成唯一ID
	timestamp := time.Now().UnixNano()
	uniqueID := fmt.Sprintf("%s-%s-%d", hostname, cleanAddr, timestamp)
	
	// 使用MD5哈希生成固定长度的ID
	hasher := md5.New()
	hasher.Write([]byte(uniqueID))
	hashBytes := hasher.Sum(nil)
	
	// 返回16进制格式的前12个字符作为节点ID
	return fmt.Sprintf("%x", hashBytes)[:12]
}

// generateStoreDir 根据节点ID生成存储目录
func generateStoreDir(nodeID string) string {
	// 获取用户主目录
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/tmp" // 如果无法获取主目录，使用/tmp作为备选
	}
	
	// 创建基于节点ID的存储目录
	storeDir := filepath.Join(homeDir, ".lmq", "data", nodeID)
	
	// 确保目录存在
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		log.Printf("创建存储目录失败: %v，将使用临时目录", err)
		storeDir = filepath.Join(os.TempDir(), "lmq-data-"+nodeID)
		os.MkdirAll(storeDir, 0755)
	}
	
	log.Printf("消息存储目录: %s", storeDir)
	return storeDir
}

func (b *Broker) Start() error {
	// 启动复制管理器
	if err := b.replicaMgr.Start(); err != nil {
		return err
	}

	// 启动清理任务
	b.StartCleanupTask(1*time.Hour, 7*24*time.Hour)

	// 启动重试任务
	b.StartRetryTask(5 * time.Second)
    
    // 启动集群管理器
	if err := b.clusterMgr.Start(); err != nil {
		return err
	}
	// 启动节点同步任务
	go b.startNodeSyncTask(1 * time.Minute)

	// 启动 TCP 服务器
	listener, err := net.Listen("tcp", b.addr)
	if err != nil {
		log.Fatalf("启动服务器失败: %v", err)
	}
	defer listener.Close()

	log.Printf("LMQ broker已启动，节点ID: %s, 监听地址: %s", b.nodeID, b.addr)

	// 处理优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 接受连接
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("接受连接失败: %v", err)
				continue
			}
			go b.HandleConnection(conn)
		}
	}()

	// 等待退出信号
	<-sigCh
	b.stopCh <- struct{}{}

	log.Println("收到退出信号，broker正在关闭...")

	return nil
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
	partition := b.selectPartition(topic, msg.ID)

	// 保存消息到分区
	if err := b.store.Write(topic, partition, []*protocol.Message{msg}); err != nil {
		return nil, fmt.Errorf("保存消息失败: %w", err)
	}

	// 复制到其他节点
	if err := b.replicaMgr.ReplicateMessages(topic, partition, []*protocol.Message{msg}); err != nil {
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
			fmt.Printf("订阅者 %s 的通道已满，消息 %s 未能发送\n", sub.ID, msg.ID)
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
		b.Publish(msg.Topic, msg.Body)
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


func (b *Broker) HandleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// 读取消息长度（4字节）
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err != io.EOF {
				log.Printf("读取消息长度失败: %v", err)
			}
			break
		}

		// 解析消息长度
		msgLen := binary.BigEndian.Uint32(lenBuf)

		// 读取消息内容
		msgBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			log.Printf("读取消息内容失败: %v", err)
			break
		}

		// 反序列化消息
		var pbMsg pb.Message
		if err := proto.Unmarshal(msgBuf, &pbMsg); err != nil {
			log.Printf("反序列化消息失败: %v", err)
			continue
		}

		// 转换为内部消息格式
		msg := &protocol.Message{
			ID:        pbMsg.Id,
			Topic:     pbMsg.Topic,
			Body:      pbMsg.Body,
			Timestamp: pbMsg.Timestamp,
			Type:      pbMsg.Type,
		}

		// 根据消息类型处理
		var err error
		switch msg.Type {
		case "publish":
			err = b.handlePublish(msg)
		case "subscribe":
			err = b.handleSubscribe(conn, msg)
		case "ack":
			err = b.handleAck(msg)
		default:
			err = fmt.Errorf("未知的消息类型: %s", msg.Type)
		}

		// 发送响应
		if err != nil {
			b.sendProtobufError(conn, err)
		} else {
			b.sendProtobufSuccess(conn, "操作成功")
		}
	}
}

// 发送 Protobuf 错误响应
func (b *Broker) sendProtobufError(conn net.Conn, err error) {
	resp := &pb.Response{
		Success: false,
		Message: err.Error(),
	}
	b.sendProtobufResponse(conn, resp)
}

// 发送 Protobuf 成功响应
func (b *Broker) sendProtobufSuccess(conn net.Conn, message string) {
	resp := &pb.Response{
		Success: true,
		Message: message,
	}
	b.sendProtobufResponse(conn, resp)
}

// 发送 Protobuf 响应
func (b *Broker) sendProtobufResponse(conn net.Conn, resp *pb.Response) {
	// 序列化响应
	data, err := proto.Marshal(resp)
	if err != nil {
		log.Printf("序列化响应失败: %v", err)
		return
	}

	// 准备长度前缀
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	// 发送长度和数据
	if _, err := conn.Write(lenBuf); err != nil {
		log.Printf("发送响应长度失败: %v", err)
		return
	}

	if _, err := conn.Write(data); err != nil {
		log.Printf("发送响应内容失败: %v", err)
		return
	}
}

// 处理发布消息
func (b *Broker) handlePublish(msg *protocol.Message) error {
	// 选择分区
	partition := b.selectPartition(msg.Topic, msg.ID)

	// 写入存储
	if err := b.store.Write(msg.Topic, partition, []*protocol.Message{msg}); err != nil {
		return fmt.Errorf("存储消息失败: %w", err)
	}

	// 复制到其他节点
	if err := b.replicaMgr.ReplicateMessages(msg.Topic, partition, []*protocol.Message{msg}); err != nil {
		return fmt.Errorf("复制消息失败: %w", err)
	}

	return nil
}

// selectPartition 选择消息应该发送到的分区
func (b *Broker) selectPartition(topic string, messageID string) int {
	// 获取主题的分区数量
	partitionCount := b.getPartitionCount(topic)
	if partitionCount <= 0 {
		// 如果没有分区，默认使用分区0
		return 0
	}

	// 使用消息ID的哈希值来确定分区
	// 这是一个简单的哈希分区策略，可以根据需要改进
	hash := 0
	for _, c := range messageID {
		hash = 31*hash + int(c)
	}
	if hash < 0 {
		hash = -hash
	}
	return hash % partitionCount
}

// getPartitionCount 获取主题的分区数量
func (b *Broker) getPartitionCount(topic string) int {
	// 从复制管理器获取分区信息
	partitions := b.replicaMgr.GetPartitions(topic)
	return len(partitions)
}

// 处理订阅请求
func (b *Broker) handleSubscribe(conn net.Conn, msg *protocol.Message) error {
	// 从消息数据中提取订阅信息
	data := msg.Data

	// 提取消费者组ID
	groupID, ok := data["group_id"].(string)
	if !ok || groupID == "" {
		return fmt.Errorf("必须提供有效的消费者组ID")
	}

	// 提取主题列表
	topicsData, ok := data["topics"].([]interface{})
	if !ok {
		return fmt.Errorf("必须提供主题列表")
	}

	topics := make([]string, 0, len(topicsData))
	for _, t := range topicsData {
		if topic, ok := t.(string); ok && topic != "" {
			topics = append(topics, topic)
		}
	}

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
				Id:        msg.ID,
				Topic:     msg.Topic,
				Body:      msg.Body,
				Timestamp: msg.Timestamp,
				Type:      "message", // 标记为普通消息
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
			b.unackedMessages[msg.ID] = msg // 添加到未确认消息列表
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
		Success: true,
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

// 处理消息确认
func (b *Broker) handleAck(msg *protocol.Message) error {
	// 从消息数据中提取确认信息
	data := msg.Data
	
	// 提取消费者组ID
	groupID, ok := data["group_id"].(string)
	if !ok || groupID == "" {
		return fmt.Errorf("必须提供有效的消费者组ID")
	}
	
	// 提取要确认的消息ID列表
	messageIDsData, ok := data["message_ids"].([]interface{})
	if !ok {
		return fmt.Errorf("必须提供要确认的消息ID列表")
	}
	
	// 转换消息ID列表
	messageIDs := make([]string, 0, len(messageIDsData))
	for _, id := range messageIDsData {
		if msgID, ok := id.(string); ok && msgID != "" {
			messageIDs = append(messageIDs, msgID)
		}
	}
	
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
	
	// 更新消费者组的消费位置
	// 注意：这里需要根据实际存储实现来更新消费位置
	// 如果使用了持久化的消费位置，应该在这里更新
	
	return nil
}

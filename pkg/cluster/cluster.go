package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/coordinator"
	"github.com/eason-lee/lmq/pkg/network"
	"github.com/eason-lee/lmq/pkg/protocol"
	"github.com/eason-lee/lmq/pkg/store"
	pb "github.com/eason-lee/lmq/proto"
)

// NodeStatus 节点状态
type NodeStatus string

const (
	NodeStatusUp      NodeStatus = "up"      // 节点正常
	NodeStatusDown    NodeStatus = "down"    // 节点宕机
	NodeStatusUnknown NodeStatus = "unknown" // 节点状态未知
)

// Node 集群节点信息
type Node struct {
	ID       string     // 节点ID
	Address  string     // 节点地址
	Status   NodeStatus // 节点状态
	LastSeen time.Time  // 最后一次更新时间
}

// ClusterManager 集群管理器
type ClusterManager struct {
	localNodeID string                     // 本地节点ID
	localAddr   string                     // 本地地址
	nodes       map[string]*Node           // 所有节点
	clients     map[string]*network.Client // 节点ID -> 网络客户端
	mu          sync.RWMutex               // 保护nodes和clients
	stopCh      chan struct{}              // 停止信号

	// 存储和协调器
	store           *store.FileStore              // 存储引擎
	coordinator     coordinator.Coordinator       // 协调器
	partitionMgr    *coordinator.PartitionManager // 分区管理器
	replicationFactor int                         // 复制因子
}

// NewClusterManager 创建一个新的集群管理器
func NewClusterManager(nodeID string, localAddr string, storeFile *store.FileStore, coord coordinator.Coordinator) *ClusterManager {
	// 创建分区管理器，使用默认的最小移动重平衡器
	partitionMgr := coordinator.NewPartitionManager(coord, store.DefaultRebalancer)
	
	return &ClusterManager{
		localNodeID: nodeID,
		localAddr:   localAddr,
		nodes:       make(map[string]*Node),
		clients:     make(map[string]*network.Client),
		stopCh:      make(chan struct{}),
		store:       storeFile,
		coordinator: coord,
		partitionMgr: partitionMgr,
		replicationFactor: 1, // 默认复制因子为1
	}
}

// RegisterHandlers 注册消息处理器
func (cm *ClusterManager) RegisterHandlers(server *network.Server) {
	// 集群管理相关处理器
	server.RegisterHandler("heartbeat", cm.handleHeartbeatRequest)
	server.RegisterHandler("node_join", cm.handleNodeJoinRequest)
	server.RegisterHandler("node_leave", cm.handleNodeLeaveRequest)

	// 复制相关处理器
	server.RegisterHandler("replication", cm.handleReplicationRequest)
}

// handleHeartbeatRequest 处理心跳请求
func (cm *ClusterManager) handleHeartbeatRequest(req *pb.Request) *pb.Response {
	// 从 request_data 字段获取心跳数据
	if req.GetRequestData() == nil || req.GetHeartbeatData() == nil {
		return &pb.Response{
			Status:  pb.Status_ERROR,
			Message: "心跳请求缺少数据",
		}
	}

	heartbeat := req.GetHeartbeatData()
	nodeID := heartbeat.NodeId
	address := heartbeat.Address

	// 更新节点状态
	cm.mu.Lock()
	if node, exists := cm.nodes[nodeID]; exists {
		node.LastSeen = time.Now()
		node.Status = NodeStatusUp
	} else {
		// 新节点加入
		cm.nodes[nodeID] = &Node{
			ID:       nodeID,
			Address:  address,
			Status:   NodeStatusUp,
			LastSeen: time.Now(),
		}
	}
	cm.mu.Unlock()

	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "心跳成功",
	}
}

// handleNodeJoinRequest 处理节点加入请求
func (cm *ClusterManager) handleNodeJoinRequest(req *pb.Request) *pb.Response {
	if req.GetRequestData() == nil || req.GetJoinData() == nil {
		return &pb.Response{
			Status:  pb.Status_ERROR,
			Message: "加入请求缺少数据",
		}
	}

	join := req.GetJoinData()
	nodeID := join.NodeId
	address := join.Address

	// 添加节点
	cm.mu.Lock()
	cm.nodes[nodeID] = &Node{
		ID:       nodeID,
		Address:  address,
		Status:   NodeStatusUp,
		LastSeen: time.Now(),
	}
	cm.mu.Unlock()

	log.Printf("节点 %s (%s) 加入集群", nodeID, address)

	// 返回当前集群节点信息
	cm.mu.RLock()
	nodeAddrs := make(map[string]string)
	for id, node := range cm.nodes {
		nodeAddrs[id] = node.Address
	}
	cm.mu.RUnlock()

	// 构建响应数据
	nodesData := &pb.NodesResponse{
		Nodes: nodeAddrs,
	}

	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "节点加入成功",
		ResponseData: &pb.Response_NodesData{
			NodesData: nodesData,
		},
	}
}

// handleNodeLeaveRequest 处理节点离开请求
func (cm *ClusterManager) handleNodeLeaveRequest(req *pb.Request) *pb.Response {
	if req.GetRequestData() == nil || req.GetLeaveData() == nil {
		return &pb.Response{
			Status:  pb.Status_ERROR,
			Message: "离开请求缺少数据",
		}
	}

	leave := req.GetLeaveData()
	nodeID := leave.NodeId

	// 移除节点
	cm.mu.Lock()
	delete(cm.nodes, nodeID)
	if client, exists := cm.clients[nodeID]; exists {
		client.Close()
		delete(cm.clients, nodeID)
	}
	cm.mu.Unlock()

	log.Printf("节点 %s 离开集群", nodeID)

	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "节点离开成功",
	}
}

// handleReplicationRequest 处理复制请求
func (cm *ClusterManager) handleReplicationRequest(req *pb.Request) *pb.Response {
	ctx := context.Background()

	// 从 request_data 字段获取复制数据
	if req.GetRequestData() == nil || req.GetPublishData() == nil {
		return &pb.Response{
			Status:  pb.Status_ERROR,
			Message: "复制请求缺少数据",
		}
	}

	publish := req.GetPublishData()
	topic := publish.Topic
	pbMessages := []*pb.Message{
		{
			Id:         fmt.Sprintf("%d", time.Now().UnixNano()), // 生成唯一ID
			Topic:      topic,
			Body:       publish.Body,
			Type:       pb.MessageType_NORMAL,
			Attributes: publish.Attributes,
		},
	}

	if len(pbMessages) == 0 {
		return &pb.Response{
			Status:  pb.Status_ERROR,
			Message: "复制请求缺少消息",
		}
	}

	// 转换消息
	var messages []*protocol.Message
	for _, pbMsg := range pbMessages {
		msg := &protocol.Message{
			Message: pbMsg,
		}
		messages = append(messages, msg)
	}

	// 保存消息到本地存储
	partition := 0 // 默认使用分区0，实际应该从消息中获取
	if err := cm.store.Write(topic, partition, messages); err != nil {
		return &pb.Response{
			Status:  pb.Status_ERROR,
			Message: fmt.Sprintf("保存消息失败: %v", err),
		}
	}

	// 更新复制状态
	offset := int64(0) // 这里需要从存储中获取实际的偏移量

	// 注册副本状态
	if err := cm.coordinator.RegisterReplicaStatus(ctx, topic, partition, cm.localNodeID, offset); err != nil {
		log.Printf("注册副本状态失败: %v", err)
	}

	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "复制成功",
	}
}

// getClient 获取或创建客户端连接
func (cm *ClusterManager) getClient(nodeID string) (*network.Client, error) {
	cm.mu.RLock()
	client, exists := cm.clients[nodeID]
	node, nodeExists := cm.nodes[nodeID]
	cm.mu.RUnlock()

	if !nodeExists {
		return nil, fmt.Errorf("节点 %s 不存在", nodeID)
	}

	if exists && client != nil {
		return client, nil
	}

	// 创建新的客户端连接
	client, err := network.NewClient(node.Address)
	if err != nil {
		return nil, fmt.Errorf("连接节点 %s 失败: %w", nodeID, err)
	}

	cm.mu.Lock()
	cm.clients[nodeID] = client
	cm.mu.Unlock()

	return client, nil
}

// Start 启动集群管理器
func (cm *ClusterManager) Start(ctx context.Context) error {
	// 添加本地节点
	cm.mu.Lock()
	cm.nodes[cm.localNodeID] = &Node{
		ID:       cm.localNodeID,
		Address:  cm.localAddr,
		Status:   NodeStatusUp,
		LastSeen: time.Now(),
	}
	cm.mu.Unlock()

	// 注册服务
	err := cm.coordinator.RegisterService(ctx, cm.localNodeID, cm.localAddr)
	if err != nil {
		return fmt.Errorf("注册服务失败: %w", err)
	}

	// 启动心跳发送
	go cm.startHeartbeat()

	// 启动节点状态监控
	go cm.monitorNodeStatus()

	// 启动分区监控
	go cm.monitorPartitions()

	return nil
}

// Stop 停止集群管理器
func (cm *ClusterManager) Stop(ctx context.Context) error {
	close(cm.stopCh)

	// 注销服务
	if err := cm.coordinator.UnregisterService(ctx, cm.localNodeID, cm.localAddr); err != nil {
		log.Printf("注销服务失败: %v", err)
	}

	// 关闭所有客户端连接
	cm.mu.Lock()
	for _, client := range cm.clients {
		client.Close()
	}
	cm.mu.Unlock()

	return nil
}

// 启动心跳发送
func (cm *ClusterManager) startHeartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.sendHeartbeats()
		}
	}
}

// 发送心跳到所有节点
func (cm *ClusterManager) sendHeartbeats() {
	cm.mu.RLock()
	nodes := make([]*Node, 0, len(cm.nodes))
	for _, node := range cm.nodes {
		if node.ID != cm.localNodeID {
			nodes = append(nodes, node)
		}
	}
	cm.mu.RUnlock()

	for _, node := range nodes {
		go func(n *Node) {
			// 创建心跳请求
			heartbeatReq := &pb.Request{
				Type: "heartbeat",
				RequestData: &pb.Request_HeartbeatData{
					HeartbeatData: &pb.HeartbeatRequest{
						NodeId:  cm.localNodeID,
						Address: cm.localAddr,
						Status:  string(NodeStatusUp),
						Time:    time.Now().UnixNano(),
					},
				},
			}

			client, err := cm.getClient(n.ID)
			if err != nil {
				log.Printf("获取节点 %s 的客户端连接失败: %v", n.ID, err)
				return
			}

			resp, err := client.Send("heartbeat", heartbeatReq)
			if err != nil {
				log.Printf("向节点 %s 发送心跳失败: %v", n.ID, err)

				// 关闭失败的连接
				client.Close()

				cm.mu.Lock()
				delete(cm.clients, n.ID)
				cm.mu.Unlock()
				return
			}

			if resp.Status == pb.Status_ERROR {
				log.Printf("节点 %s 心跳响应错误: %s", n.ID, resp.Message)
			}
		}(node)
	}
}

// 监控节点状态
func (cm *ClusterManager) monitorNodeStatus() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.checkNodeStatus()
		}
	}
}

// 检查节点状态
func (cm *ClusterManager) checkNodeStatus() {
	now := time.Now()
	threshold := 30 * time.Second

	cm.mu.Lock()
	defer cm.mu.Unlock()

	for id, node := range cm.nodes {
		if id == cm.localNodeID {
			continue // 跳过自己
		}

		if now.Sub(node.LastSeen) > threshold {
			// 节点可能宕机
			if node.Status != NodeStatusDown {
				log.Printf("节点 %s 可能宕机，上次心跳时间: %v", id, node.LastSeen)
				node.Status = NodeStatusDown

				// 处理节点宕机
				go cm.handleNodeFailure(id)
			}
		}
	}
}

// handleNodeFailure 处理节点宕机
func (cm *ClusterManager) handleNodeFailure(nodeID string) {
	ctx := context.Background()

	// 获取该节点作为 leader 的所有分区
	partitions, err := cm.coordinator.GetLeaderPartitions(ctx, nodeID)
	if err != nil {
		log.Printf("获取节点 %s 的 leader 分区失败: %v", nodeID, err)
		return
	}

	// 对每个分区进行 leader 选举
	for _, p := range partitions {
		// 尝试成为新的 leader
		if cm.tryBecomeLeader(ctx, p.Topic, p.ID) {
			log.Printf("成为分区 %s-%d 的新 leader", p.Topic, p.ID)
		}
	}
}

// tryBecomeLeader 尝试成为分区的 leader
func (cm *ClusterManager) tryBecomeLeader(ctx context.Context, topic string, partitionID int) bool {
	// 检查当前节点是否在 ISR 中
	partition, err := cm.coordinator.GetPartition(ctx, topic, partitionID)
	if err != nil {
		log.Printf("获取分区 %s-%d 信息失败: %v", topic, partitionID, err)
		return false
	}

	if partition == nil {
		log.Printf("分区 %s-%d 不存在", topic, partitionID)
		return false
	}

	// 检查当前节点是否在 ISR 中
	inISR := false
	for _, nodeID := range partition.ISR {
		if nodeID == cm.localNodeID {
			inISR = true
			break
		}
	}

	if !inISR {
		log.Printf("当前节点不在分区 %s-%d 的 ISR 中，不能成为 leader", topic, partitionID)
		return false
	}

	// 尝试选举为 leader
	elected, err := cm.coordinator.ElectPartitionLeader(ctx, topic, partitionID, cm.localNodeID)
	if err != nil {
		log.Printf("选举分区 %s-%d 的 leader 失败: %v", topic, partitionID, err)
		return false
	}

	return elected
}

// 监控分区
func (cm *ClusterManager) monitorPartitions() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.syncPartitions()
		}
	}
}

// syncPartitions 同步分区信息
func (cm *ClusterManager) syncPartitions() {
	ctx := context.Background()

	// 获取当前节点负责的所有分区
	partitions, err := cm.coordinator.GetNodePartitions(ctx, cm.localNodeID)
	if err != nil {
		log.Printf("获取节点分区失败: %v", err)
		return
	}

	// 更新每个分区的复制状态
	for _, partition := range partitions {
		// 获取分区的最新消息ID
		offset := int64(0) // 这里需要实现获取最新消息ID的方法

		// 注册副本状态
		if err := cm.coordinator.RegisterReplicaStatus(ctx, partition.Topic, partition.ID, cm.localNodeID, offset); err != nil {
			log.Printf("注册副本状态失败: %v", err)
		}

		// 如果是 leader，检查 follower 的复制状态
		if partition.Leader == cm.localNodeID {
			cm.checkFollowerStatus(ctx, partition)
		}
	}
}

// checkFollowerStatus 检查 follower 的复制状态
func (cm *ClusterManager) checkFollowerStatus(ctx context.Context, partition *coordinator.PartitionInfo) {
	// 获取所有副本的状态
	replicaStatus, err := cm.coordinator.GetAllReplicaStatus(ctx, partition.Topic, partition.ID)
	if err != nil {
		log.Printf("获取分区 %s-%d 的副本状态失败: %v", partition.Topic, partition.ID, err)
		return
	}

	// 获取 leader 的偏移量
	leaderOffset, ok := replicaStatus[cm.localNodeID]
	if !ok {
		log.Printf("未找到 leader 的复制状态")
		return
	}

	// 检查每个 follower 的复制状态
	var newISR []string
	newISR = append(newISR, cm.localNodeID) // leader 总是在 ISR 中

	for _, follower := range partition.Followers {
		followerOffset, ok := replicaStatus[follower]
		if !ok {
			log.Printf("未找到 follower %s 的复制状态", follower)
			continue
		}

		// 如果 follower 的偏移量接近 leader，将其加入 ISR
		if leaderOffset-followerOffset <= 10 { // 假设允许的滞后是 10 条消息
			newISR = append(newISR, follower)
		}
	}

	// 更新 ISR
	if !equalStringSlices(partition.ISR, newISR) {
		log.Printf("更新分区 %s-%d 的 ISR: %v -> %v", partition.Topic, partition.ID, partition.ISR, newISR)
		if err := cm.coordinator.UpdateISR(ctx, partition.Topic, partition.ID, newISR); err != nil {
			log.Printf("更新 ISR 失败: %v", err)
		}
	}
}

// 比较两个字符串切片是否相等
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	// 创建 map 来检查元素
	aMap := make(map[string]bool)
	for _, s := range a {
		aMap[s] = true
	}

	// 检查 b 中的每个元素是否在 a 中
	for _, s := range b {
		if !aMap[s] {
			return false
		}
	}

	return true
}

// ReplicateMessages 复制消息到其他节点
func (cm *ClusterManager) ReplicateMessages(ctx context.Context, topic string, partition int, messages []*protocol.Message) error {
	// 检查当前节点是否是 leader
	isLeader, err := cm.coordinator.IsPartitionLeader(ctx, topic, partition, cm.localNodeID)
	if err != nil {
		return fmt.Errorf("检查 leader 状态失败: %w", err)
	}

	if !isLeader {
		return fmt.Errorf("当前节点不是分区 %s-%d 的 leader", topic, partition)
	}

	// 获取分区信息
	partitionInfo, err := cm.coordinator.GetPartition(ctx, topic, partition)
	if err != nil {
		return fmt.Errorf("获取分区信息失败: %w", err)
	}

	if partitionInfo == nil {
		return fmt.Errorf("分区 %s-%d 不存在", topic, partition)
	}

	// 保存消息到本地
	if err := cm.store.Write(topic, partition, messages); err != nil {
		return fmt.Errorf("保存消息到本地失败: %w", err)
	}

	// 获取最新偏移量
	offset := int64(0) // 这里需要实现获取最新消息ID的方法

	// 更新自己的复制状态
	if err := cm.coordinator.RegisterReplicaStatus(ctx, topic, partition, cm.localNodeID, offset); err != nil {
		log.Printf("注册副本状态失败: %v", err)
	}

	// 并行复制到所有 follower
	var wg sync.WaitGroup
	errors := make(chan error, len(partitionInfo.Followers))

	for _, followerID := range partitionInfo.Followers {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()

			err := cm.replicateToNode(ctx, nodeID, topic, partition, messages)
			if err != nil {
				errors <- fmt.Errorf("复制到节点 %s 失败: %w", nodeID, err)
			}
		}(followerID)
	}

	// 等待所有复制完成
	wg.Wait()
	close(errors)

	// 收集错误
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}

	// 如果有错误，但至少有一个复制成功，返回 nil
	if len(errs) > 0 && len(errs) < len(partitionInfo.Followers) {
		log.Printf("部分节点复制失败: %v", errs)
		return nil
	}

	// 如果所有复制都失败，返回错误
	if len(errs) > 0 && len(errs) == len(partitionInfo.Followers) {
		return fmt.Errorf("所有节点复制失败: %v", errs[0])
	}

	return nil
}

// replicateToNode 复制消息到指定节点
func (cm *ClusterManager) replicateToNode(ctx context.Context, nodeID string, topic string, partition int, messages []*protocol.Message) error {
	client, err := cm.getClient(nodeID)
	if err != nil {
		return err
	}

	// 转换消息为 protobuf 格式
	pbMessages := make([]*pb.Message, 0, len(messages))
	for _, msg := range messages {
		pbMsg := &pb.Message{
			Id:         msg.Message.Id,
			Topic:      msg.Message.Topic,
			Body:       msg.Message.Body,
			Type:       pb.MessageType_NORMAL,
			Attributes: msg.Message.Attributes,
		}
		pbMessages = append(pbMessages, pbMsg)
	}

	// 构建复制请求
	replicationReq := &pb.Request{
		Type: "replication",
		RequestData: &pb.Request_PublishData{
			PublishData: &pb.PublishRequest{
				Topic: topic,
				Body:  pbMessages[0].Body,
				Type:  pb.MessageType_NORMAL,
			},
		},
	}

	// 发送复制请求
	resp, err := client.Send("replication", replicationReq)
	if err != nil {
		return err
	}

	// 检查响应
	if resp.Status == pb.Status_ERROR {
		return fmt.Errorf("复制失败: %s", resp.Message)
	}

	return nil
}


// SyncNodes 从外部同步节点信息
func (cm *ClusterManager) SyncNodes(nodes map[string]string) {
	cm.mu.Lock()
	
	// 记录节点变化情况
	nodeAdded := false
	nodeRemoved := false
	
	// 更新现有节点
	for nodeID, addr := range nodes {
		if nodeID == cm.localNodeID {
			continue // 跳过本地节点
		}

		if node, exists := cm.nodes[nodeID]; exists {
			// 更新现有节点
			node.Address = addr
			node.LastSeen = time.Now()
		} else {
			// 添加新节点
			cm.nodes[nodeID] = &Node{
				ID:       nodeID,
				Address:  addr,
				Status:   NodeStatusUp,
				LastSeen: time.Now(),
			}
			log.Printf("添加新节点: %s (%s)", nodeID, addr)
			nodeAdded = true
		}
	}

	// 移除不再存在的节点
	for nodeID := range cm.nodes {
		if nodeID == cm.localNodeID {
			continue // 跳过本地节点
		}

		if _, exists := nodes[nodeID]; !exists {
			delete(cm.nodes, nodeID)

			// 关闭并移除客户端连接
			if client, exists := cm.clients[nodeID]; exists {
				client.Close()
				delete(cm.clients, nodeID)
			}

			log.Printf("移除节点: %s", nodeID)
			nodeRemoved = true
		}
	}
	cm.mu.Unlock()
	
	// 如果节点数量发生变化，触发分区重平衡
	if nodeAdded || nodeRemoved {
		log.Printf("集群节点发生变化，触发分区重平衡")
		ctx := context.Background()
		
		// 如果有新节点加入
		if nodeAdded {
			if err := cm.partitionMgr.AddBrokerAndRebalance(ctx, "", cm.replicationFactor); err != nil {
				log.Printf("添加节点后重平衡分区失败: %v", err)
			}
		}
		
		// 如果有节点移除
		if nodeRemoved {
			if err := cm.partitionMgr.RemoveBrokerAndRebalance(ctx, "", cm.replicationFactor); err != nil {
				log.Printf("移除节点后重平衡分区失败: %v", err)
			}
		}
	}
}

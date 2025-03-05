package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/network"
	"github.com/eason-lee/lmq/pkg/protocol"
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
	LastSeen time.Time  // 最后一次心跳时间
}

// MessageSender 消息发送接口，用于打破循环依赖
type MessageSender interface {
	SendMessage(nodeID string, msg *protocol.Message) error
}

// ClusterManager 集群管理器
// 修改 ClusterManager 结构体
type ClusterManager struct {
	localNodeID string                // 本地节点ID
	localAddr   string                // 本地地址
	nodes       map[string]*Node      // 所有节点
	clients     map[string]*network.Client // 节点ID -> 网络客户端
	mu          sync.RWMutex          // 保护nodes和clients
	stopCh      chan struct{}         // 停止信号
}

// 修改 NewClusterManager 函数
func NewClusterManager(nodeID string, localAddr string) *ClusterManager {
	return &ClusterManager{
		localNodeID: nodeID,
		localAddr:   localAddr,
		nodes:       make(map[string]*Node),
		clients:     make(map[string]*network.Client),
		stopCh:      make(chan struct{}),
	}
}

// 添加 getClient 方法获取或创建客户端连接
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

func (cm *ClusterManager) sendHeartbeat() {
	// 创建心跳消息
	heartbeatData := map[string]interface{}{
		"node_id": cm.localNodeID,
		"status":  string(NodeStatusUp),
	}

	// 向所有节点发送心跳
	cm.mu.RLock()
	nodeIDs := make([]string, 0, len(cm.nodes))
	for nodeID, node := range cm.nodes {
		if nodeID != cm.localNodeID && node.Status != NodeStatusDown {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	cm.mu.RUnlock()
	
	for _, nodeID := range nodeIDs {
		go func(id string) {
			client, err := cm.getClient(id)
			if err != nil {
				log.Printf("获取节点 %s 的客户端连接失败: %v", id, err)
				return
			}
			
			resp, err := client.Send("heartbeat", heartbeatData)
			if err != nil {
				log.Printf("向节点 %s 发送心跳失败: %v", id, err)
				
				// 关闭失败的连接
				client.Close()
				
				cm.mu.Lock()
				delete(cm.clients, id)
				cm.mu.Unlock()
				return
			}
			
			if !resp.Success {
				log.Printf("节点 %s 心跳响应错误: %s", id, resp.Error)
			}
		}(nodeID)
	}
}

// 添加 RegisterHandlers 方法注册处理器
func (cm *ClusterManager) RegisterHandlers(server *network.Server) {
	server.RegisterHandler("heartbeat", cm.handleHeartbeatRequest)
	server.RegisterHandler("node_join", cm.handleNodeJoinRequest)
	server.RegisterHandler("node_leave", cm.handleNodeLeaveRequest)
}

// 添加处理心跳请求的方法
func (cm *ClusterManager) handleHeartbeatRequest(req *network.Request) *network.Response {
	var heartbeatData map[string]interface{}
	if err := json.Unmarshal(req.Payload, &heartbeatData); err != nil {
		return &network.Response{
			Success: false,
			Error:   "解析心跳数据失败",
		}
	}
	
	nodeID, ok := heartbeatData["node_id"].(string)
	if !ok || nodeID == "" {
		return &network.Response{
			Success: false,
			Error:   "心跳消息缺少节点ID",
		}
	}
	
	statusStr, ok := heartbeatData["status"].(string)
	if !ok {
		return &network.Response{
			Success: false,
			Error:   "心跳消息缺少状态信息",
		}
	}
	
	status := NodeStatus(statusStr)
	cm.UpdateNodeStatus(nodeID, status)
	
	return &network.Response{
		Success: true,
		Data:    map[string]interface{}{"node_id": cm.localNodeID},
	}
}

// 添加处理节点加入请求的方法
func (cm *ClusterManager) handleNodeJoinRequest(req *network.Request) *network.Response {
	var joinData map[string]interface{}
	if err := json.Unmarshal(req.Payload, &joinData); err != nil {
		return &network.Response{
			Success: false,
			Error:   "解析节点加入数据失败",
		}
	}
	
	nodeID, ok := joinData["node_id"].(string)
	if !ok || nodeID == "" {
		return &network.Response{
			Success: false,
			Error:   "节点加入消息缺少节点ID",
		}
	}
	
	address, ok := joinData["address"].(string)
	if !ok || address == "" {
		return &network.Response{
			Success: false,
			Error:   "节点加入消息缺少地址信息",
		}
	}
	
	cm.AddNode(nodeID, address)
	
	// 返回当前所有节点信息，帮助新节点同步集群状态
	nodes := cm.GetNodes()
	nodeInfos := make([]map[string]interface{}, 0, len(nodes))
	for _, node := range nodes {
		nodeInfos = append(nodeInfos, map[string]interface{}{
			"id":      node.ID,
			"address": node.Address,
			"status":  string(node.Status),
		})
	}
	
	return &network.Response{
		Success: true,
		Data:    map[string]interface{}{"nodes": nodeInfos},
	}
}

// 添加处理节点离开请求的方法
func (cm *ClusterManager) handleNodeLeaveRequest(req *network.Request) *network.Response {
	var leaveData map[string]interface{}
	if err := json.Unmarshal(req.Payload, &leaveData); err != nil {
		return &network.Response{
			Success: false,
			Error:   "解析节点离开数据失败",
		}
	}
	
	nodeID, ok := leaveData["node_id"].(string)
	if !ok || nodeID == "" {
		return &network.Response{
			Success: false,
			Error:   "节点离开消息缺少节点ID",
		}
	}
	
	cm.RemoveNode(nodeID)
	
	return &network.Response{
		Success: true,
	}
}

// 添加 JoinCluster 方法
func (cm *ClusterManager) JoinCluster(seedNodeAddr string) error {
	if seedNodeAddr == "" {
		// 如果没有种子节点，则作为第一个节点启动
		log.Printf("作为集群的第一个节点启动")
		return nil
	}
	
	// 连接种子节点
	client, err := network.NewClient(seedNodeAddr)
	if err != nil {
		return fmt.Errorf("连接种子节点失败: %w", err)
	}
	defer client.Close()
	
	// 发送加入请求
	joinData := map[string]interface{}{
		"node_id": cm.localNodeID,
		"address": cm.localAddr,
	}
	
	resp, err := client.Send("node_join", joinData)
	if err != nil {
		return fmt.Errorf("发送加入请求失败: %w", err)
	}
	
	if !resp.Success {
		return fmt.Errorf("加入集群失败: %s", resp.Error)
	}
	
	// 处理响应，同步集群节点信息
	if resp.Data != nil {
		if data, ok := resp.Data.(map[string]interface{}); ok {
			if nodesData, ok := data["nodes"].([]interface{}); ok {
				for _, nodeData := range nodesData {
					if node, ok := nodeData.(map[string]interface{}); ok {
						nodeID := node["id"].(string)
						address := node["address"].(string)
						
						if nodeID != cm.localNodeID {
							cm.AddNode(nodeID, address)
						}
					}
				}
			}
		}
	}
	
	log.Printf("成功加入集群")
	return nil
}

// 修改 Stop 方法，关闭所有客户端连接
func (cm *ClusterManager) Stop() {
	close(cm.stopCh)
	
	// 关闭所有客户端连接
	cm.mu.Lock()
	for _, client := range cm.clients {
		client.Close()
	}
	cm.mu.Unlock()
}

// Start 启动集群管理器
func (cm *ClusterManager) Start() error {
	// 添加本地节点
	cm.mu.Lock()
	cm.nodes[cm.localNodeID] = &Node{
		ID:       cm.localNodeID,
		Address:  cm.localAddr,
		Status:   NodeStatusUp,
		LastSeen: time.Now(),
	}
	cm.mu.Unlock()

	// 启动心跳检测
	go cm.heartbeatLoop()

	// 启动故障检测
	go cm.failureDetectionLoop()

	return nil
}


// AddNode 添加节点到集群
func (cm *ClusterManager) AddNode(nodeID, address string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.nodes[nodeID] = &Node{
		ID:       nodeID,
		Address:  address,
		Status:   NodeStatusUnknown,
		LastSeen: time.Now(),
	}

	log.Printf("节点 %s 已添加到集群", nodeID)
}

// RemoveNode 从集群中移除节点
func (cm *ClusterManager) RemoveNode(nodeID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.nodes, nodeID)
	log.Printf("节点 %s 已从集群中移除", nodeID)
}

// GetNodes 获取所有节点
func (cm *ClusterManager) GetNodes() []*Node {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	nodes := make([]*Node, 0, len(cm.nodes))
	for _, node := range cm.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// UpdateNodeStatus 更新节点状态
func (cm *ClusterManager) UpdateNodeStatus(nodeID string, status NodeStatus) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if node, ok := cm.nodes[nodeID]; ok {
		node.Status = status
		node.LastSeen = time.Now()
		log.Printf("节点 %s 状态更新为 %s", nodeID, status)
	}
}

// HandleHeartbeat 处理收到的心跳消息
func (cm *ClusterManager) HandleHeartbeat(msg *protocol.Message) {
	data := msg.Data

	nodeID, ok := data["node_id"].(string)
	if !ok || nodeID == "" {
		log.Printf("心跳消息缺少节点ID")
		return
	}

	statusStr, ok := data["status"].(string)
	if !ok {
		log.Printf("心跳消息缺少状态信息")
		return
	}

	status := NodeStatus(statusStr)
	cm.UpdateNodeStatus(nodeID, status)
}

// heartbeatLoop 心跳循环
func (cm *ClusterManager) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.sendHeartbeat()
		}
	}
}


// failureDetectionLoop 故障检测循环
func (cm *ClusterManager) failureDetectionLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.detectFailures()
		}
	}
}

// detectFailures 检测节点故障
func (cm *ClusterManager) detectFailures() {
	now := time.Now()
	threshold := 15 * time.Second // 15秒没有心跳就认为节点故障

	cm.mu.Lock()
	defer cm.mu.Unlock()

	for nodeID, node := range cm.nodes {
		if nodeID != cm.localNodeID && node.Status != NodeStatusDown {
			if now.Sub(node.LastSeen) > threshold {
				node.Status = NodeStatusDown
				log.Printf("节点 %s 可能已宕机", nodeID)
				
				// 触发故障恢复流程
				go cm.handleNodeFailure(nodeID)
			}
		}
	}
}

// handleNodeFailure 处理节点故障
func (cm *ClusterManager) handleNodeFailure(nodeID string) {
	log.Printf("开始处理节点 %s 的故障恢复", nodeID)
	
	// 这里应该实现故障恢复逻辑
	// 例如：重新分配分区、选举新的主节点等
}

// RebalancePartitions 重新平衡分区
func (cm *ClusterManager) RebalancePartitions() {
	log.Printf("开始重新平衡分区")
	// TODO 
	
	// 这里应该实现分区重平衡逻辑
}

// HandleClusterMessage 处理集群相关消息
func (cm *ClusterManager) HandleClusterMessage(msg *protocol.Message) error {
	switch msg.Type {
	case "heartbeat":
		// 处理心跳消息
		cm.HandleHeartbeat(msg)
		return nil
	case "node_join":
		// 处理节点加入消息
		data := msg.Data
		
		nodeID, ok := data["node_id"].(string)
		if !ok || nodeID == "" {
			return fmt.Errorf("节点加入消息缺少节点ID")
		}
		
		address, ok := data["address"].(string)
		if !ok || address == "" {
			return fmt.Errorf("节点加入消息缺少地址信息")
		}
		
		cm.AddNode(nodeID, address)
		return nil
	case "node_leave":
		// 处理节点离开消息
		data := msg.Data
		
		nodeID, ok := data["node_id"].(string)
		if !ok || nodeID == "" {
			return fmt.Errorf("节点离开消息缺少节点ID")
		}
		
		cm.RemoveNode(nodeID)
		return nil
	default:
		return fmt.Errorf("未知的集群消息类型: %s", msg.Type)
	}
}
package cluster

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/eason-lee/lmq/pkg/network"
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
	localNodeID string                // 本地节点ID
	localAddr   string                // 本地地址
	nodes       map[string]*Node      // 所有节点
	clients     map[string]*network.Client // 节点ID -> 网络客户端
	mu          sync.RWMutex          // 保护nodes和clients
	stopCh      chan struct{}         // 停止信号
}

// NewClusterManager 创建一个新的集群管理器
func NewClusterManager(nodeID string, localAddr string) *ClusterManager {
	return &ClusterManager{
		localNodeID: nodeID,
		localAddr:   localAddr,
		nodes:       make(map[string]*Node),
		clients:     make(map[string]*network.Client),
		stopCh:      make(chan struct{}),
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

	return nil
}

// Stop 停止集群管理器
func (cm *ClusterManager) Stop() error {
	close(cm.stopCh)
	
	// 关闭所有客户端连接
	cm.mu.Lock()
	for _, client := range cm.clients {
		client.Close()
	}
	cm.mu.Unlock()
	
	return nil
}

// SyncNodes 从外部同步节点信息
func (cm *ClusterManager) SyncNodes(nodes map[string]string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
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
		}
	}
}

// AddNode 添加节点到集群
func (cm *ClusterManager) AddNode(nodeID, address string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.nodes[nodeID] = &Node{
		ID:       nodeID,
		Address:  address,
		Status:   NodeStatusUp,
		LastSeen: time.Now(),
	}

	log.Printf("节点 %s 已添加到集群", nodeID)
}

// RemoveNode 从集群中移除节点
func (cm *ClusterManager) RemoveNode(nodeID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.nodes, nodeID)
	
	// 关闭并移除客户端连接
	if client, exists := cm.clients[nodeID]; exists {
		client.Close()
		delete(cm.clients, nodeID)
	}
	
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

// GetNodeAddresses 获取所有节点地址
func (cm *ClusterManager) GetNodeAddresses() map[string]string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	addresses := make(map[string]string, len(cm.nodes))
	for id, node := range cm.nodes {
		addresses[id] = node.Address
	}
	return addresses
}

// SendMessageToNode 向指定节点发送消息
func (cm *ClusterManager) SendMessageToNode(nodeID string, msgType string, data map[string]interface{}) (*network.Response, error) {
	client, err := cm.getClient(nodeID)
	if err != nil {
		return nil, err
	}
	
	return client.Send(msgType, data)
}

// RebalancePartitions 重新平衡分区
func (cm *ClusterManager) RebalancePartitions() {
	log.Printf("开始重新平衡分区")
	// TODO: 实现分区重平衡逻辑
}
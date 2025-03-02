package replication

import (
	"fmt"
	"log"
	"sync"
	"time"

	"encoding/json" // 添加 json 包

	"github.com/eason-lee/lmq/pkg/network"
	"github.com/eason-lee/lmq/pkg/protocol"
	"github.com/eason-lee/lmq/pkg/store"
)

// 添加节点状态常量
const (
    NodeStateNormal  = "normal"   // 正常状态
    NodeStateDown    = "down"     // 节点宕机
    NodeStateSyncing = "syncing"  // 正在同步
)

// ReplicationRequest 复制请求
type ReplicationRequest struct {
    Topic     string              `json:"topic"`
    Partition int                 `json:"partition"`
    Messages  []*protocol.Message `json:"messages"`
    Offset    int64              `json:"offset"`
}

// ReplicationResponse 复制响应
type ReplicationResponse struct {
    Success bool   `json:"success"`
    Error   string `json:"error,omitempty"`
    Offset  int64  `json:"offset"`
}

// PartitionMeta 分区元数据
type PartitionMeta struct {
    Topic     string
    ID        int
    Leader    string   // leader broker ID
    Followers []string // follower broker IDs
    ISR       []string // in-sync replicas
}

// NodeInfo 节点信息
type NodeInfo struct {
    ID       string    // 节点ID
    Address  string    // 节点地址
    State    string    // 节点状态
    LastSeen int64     // 最后一次心跳时间
}

// 在 ReplicaManager 结构体中添加节点管理相关字段
type ReplicaManager struct {
    nodeID            string // 当前节点 ID
    store             *store.FileStore
    partitions        map[string]map[int]*PartitionMeta // topic -> partitionID -> meta
    mu                sync.RWMutex
    server            *network.Server
    clients           map[string]*network.Client // nodeID -> client
    nodes             map[string]*NodeInfo       // 所有节点信息
    nodesMu           sync.RWMutex               // 节点信息锁
    heartbeatInterval time.Duration              // 心跳间隔
    electionTimeout   time.Duration              // 选举超时
}

// 修改构造函数，添加节点管理初始化
func NewReplicaManager(nodeID string, store *store.FileStore, addr string) *ReplicaManager {
    rm := &ReplicaManager{
        nodeID:     nodeID,
        store:      store,
        partitions: make(map[string]map[int]*PartitionMeta),
        server:     network.NewServer(addr),
        clients:    make(map[string]*network.Client),
        nodes:      make(map[string]*NodeInfo),
        heartbeatInterval: 5 * time.Second,
        electionTimeout:   15 * time.Second,
    }

    // 注册复制请求处理器
    rm.server.RegisterHandler("replication", rm.handleReplicationRequest)
    
    // 注册心跳处理器
    rm.server.RegisterHandler("heartbeat", rm.handleHeartbeat)
    
    // 添加自身节点信息
    rm.nodes[nodeID] = &NodeInfo{
        ID:       nodeID,
        Address:  addr,
        State:    NodeStateNormal,
        LastSeen: time.Now().UnixNano(),
    }
    
    return rm
}

// 增强 Start 方法，启动节点状态管理
func (rm *ReplicaManager) Start() error {
    // 启动服务器
    if err := rm.server.Start(); err != nil {
        return err
    }
    
    // 启动心跳发送
    go rm.startHeartbeat()
    
    // 启动节点状态监控
    go rm.monitorNodeStatus()
    
    return nil
}

// 添加心跳发送方法
func (rm *ReplicaManager) startHeartbeat() {
    ticker := time.NewTicker(rm.heartbeatInterval)
    defer ticker.Stop()
    
    for range ticker.C {
        rm.sendHeartbeats()
    }
}

// 发送心跳到所有节点
func (rm *ReplicaManager) sendHeartbeats() {
    rm.nodesMu.RLock()
    nodes := make([]*NodeInfo, 0, len(rm.nodes))
    for _, node := range rm.nodes {
        if node.ID != rm.nodeID {
            nodes = append(nodes, node)
        }
    }
    rm.nodesMu.RUnlock()
    
    for _, node := range nodes {
        go func(n *NodeInfo) {
            if err := rm.sendHeartbeat(n.ID, n.Address); err != nil {
                log.Printf("发送心跳到节点 %s 失败: %v", n.ID, err)
            }
        }(node)
    }
}

// 发送心跳到指定节点
func (rm *ReplicaManager) sendHeartbeat(nodeID, addr string) error {
    client, err := rm.getOrCreateClient(nodeID, addr)
    if err != nil {
        return err
    }
    
    heartbeat := &protocol.Heartbeat{
        NodeID:    rm.nodeID,
        Timestamp: time.Now().UnixNano(),
    }
    
    resp, err := client.Send("heartbeat", heartbeat)
    if err != nil {
        return err
    }
    
    if !resp.Success {
        return fmt.Errorf("心跳响应失败: %s", resp.Error)
    }
    
    return nil
}

// 处理心跳请求
func (rm *ReplicaManager) handleHeartbeat(req *network.Request) *network.Response {
    var heartbeat protocol.Heartbeat
    if err := json.Unmarshal(req.Payload, &heartbeat); err != nil {
        return &network.Response{Success: false, Error: err.Error()}
    }
    
    // 更新节点状态
    rm.updateNodeStatus(heartbeat.NodeID, heartbeat.Timestamp)
    
    return &network.Response{Success: true}
}

// 更新节点状态
func (rm *ReplicaManager) updateNodeStatus(nodeID string, timestamp int64) {
    rm.nodesMu.Lock()
    defer rm.nodesMu.Unlock()
    
    if node, ok := rm.nodes[nodeID]; ok {
        node.LastSeen = timestamp
        node.State = NodeStateNormal
    } else {
        // 新节点加入
        rm.nodes[nodeID] = &NodeInfo{
            ID:       nodeID,
            State:    NodeStateNormal,
            LastSeen: timestamp,
        }
    }
}

// 监控节点状态
func (rm *ReplicaManager) monitorNodeStatus() {
    ticker := time.NewTicker(rm.heartbeatInterval)
    defer ticker.Stop()
    
    for range ticker.C {
        rm.checkNodeStatus()
    }
}

// 检查节点状态
func (rm *ReplicaManager) checkNodeStatus() {
    now := time.Now().UnixNano()
    timeout := now - int64(rm.electionTimeout)
    
    rm.nodesMu.Lock()
    defer rm.nodesMu.Unlock()
    
    for id, node := range rm.nodes {
        if id == rm.nodeID {
            continue // 跳过自己
        }
        
        if node.LastSeen < timeout {
            // 节点可能宕机
            if node.State != NodeStateDown {
                log.Printf("节点 %s 可能宕机，上次心跳时间: %v", id, time.Unix(0, node.LastSeen))
                node.State = NodeStateDown
                
                // 处理节点宕机
                go rm.handleNodeDown(id)
            }
        }
    }
}

// 处理节点宕机
func (rm *ReplicaManager) handleNodeDown(nodeID string) {
    // 获取该节点作为 leader 的所有分区
    partitions := rm.getLeaderPartitions(nodeID)
    
    // 对每个分区进行 leader 选举
    for _, p := range partitions {
        go rm.electNewLeader(p.Topic, p.ID)
    }
}

// 获取指定节点作为 leader 的所有分区
func (rm *ReplicaManager) getLeaderPartitions(nodeID string) []*PartitionMeta {
    rm.mu.RLock()
    defer rm.mu.RUnlock()
    
    var result []*PartitionMeta
    
    for _, topicPartitions := range rm.partitions {
        for _, partition := range topicPartitions {
            if partition.Leader == nodeID {
                result = append(result, partition)
            }
        }
    }
    
    return result
}

// 选举新的 leader
func (rm *ReplicaManager) electNewLeader(topic string, partition int) {
    rm.mu.Lock()
    defer rm.mu.Unlock()
    
    meta := rm.partitions[topic][partition]
    if meta == nil {
        log.Printf("分区不存在: %s-%d", topic, partition)
        return
    }
    
    // 从 ISR 中选择一个新的 leader
    for _, nodeID := range meta.ISR {
        if nodeID != meta.Leader {
            rm.nodesMu.RLock()
            node, ok := rm.nodes[nodeID]
            rm.nodesMu.RUnlock()
            
            if ok && node.State == NodeStateNormal {
                log.Printf("为分区 %s-%d 选举新的 leader: %s", topic, partition, nodeID)
                meta.Leader = nodeID
                
                // 更新 followers
                var newFollowers []string
                for _, f := range meta.Followers {
                    if f != nodeID {
                        newFollowers = append(newFollowers, f)
                    }
                }
                if meta.Leader != rm.nodeID {
                    newFollowers = append(newFollowers, rm.nodeID)
                }
                meta.Followers = newFollowers
                
                // 通知其他节点
                go rm.notifyLeaderChange(topic, partition, nodeID)
                return
            }
        }
    }
    
    log.Printf("分区 %s-%d 没有可用的 follower 作为新的 leader", topic, partition)
}

// 通知 leader 变更
func (rm *ReplicaManager) notifyLeaderChange(topic string, partition int, newLeader string) {
    // 实现通知逻辑
}

// 获取或创建客户端连接
func (rm *ReplicaManager) getOrCreateClient(nodeID, addr string) (*network.Client, error) {
    rm.mu.Lock()
    defer rm.mu.Unlock()
    
    if client, ok := rm.clients[nodeID]; ok {
        return client, nil
    }
    
    client, err := network.NewClient(addr)
    if err != nil {
        return nil, err
    }
    
    rm.clients[nodeID] = client
    return client, nil
}

// 修改发送到follower的方法
func (rm *ReplicaManager) sendToFollower(nodeID string, topic string, partition int, messages []*protocol.Message) error {
	client, ok := rm.clients[nodeID]
	if !ok {
		// TODO: 从配置或服务发现获取节点地址
		addr := fmt.Sprintf("localhost:%d", 9000) // 示例地址
		var err error
		client, err = network.NewClient(addr)
		if err != nil {
			return err
		}
		rm.clients[nodeID] = client
	}

	req := ReplicationRequest{
		Topic:     topic,
		Partition: partition,
		Messages:  messages,
	}

	resp, err := client.Send("replication", req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("复制失败: %s", resp.Error)
	}

	return nil
}

// 添加处理复制请求的方法
func (rm *ReplicaManager) handleReplicationRequest(req *network.Request) *network.Response {
	var replicationReq ReplicationRequest
	if err := json.Unmarshal(req.Payload, &replicationReq); err != nil {
		return &network.Response{Success: false, Error: err.Error()}
	}

	resp := rm.HandleReplicationRequest(&replicationReq)
	return &network.Response{
		Success: resp.Success,
		Error:   resp.Error,
		Data:    resp,
	}
}

// AddPartition 添加分区
func (rm *ReplicaManager) AddPartition(meta *PartitionMeta) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, ok := rm.partitions[meta.Topic]; !ok {
		rm.partitions[meta.Topic] = make(map[int]*PartitionMeta)
	}
	rm.partitions[meta.Topic][meta.ID] = meta
	return nil
}

// IsLeader 检查当前节点是否是指定分区的 leader
func (rm *ReplicaManager) IsLeader(topic string, partition int) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if p, ok := rm.partitions[topic][partition]; ok {
		return p.Leader == rm.nodeID
	}
	return false
}

// GetPartitions 获取主题的所有分区
func (rm *ReplicaManager) GetPartitions(topic string) []int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if partitions, ok := rm.partitions[topic]; ok {
		result := make([]int, 0, len(partitions))
		for id := range partitions {
			result = append(result, id)
		}
		return result
	}

	// 如果主题不存在，创建默认分区
	rm.mu.RUnlock()
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, ok := rm.partitions[topic]; !ok {
		rm.partitions[topic] = make(map[int]*PartitionMeta)
		// 创建默认分区
		meta := &PartitionMeta{
			Topic:     topic,
			ID:        0,
			Leader:    rm.nodeID,
			Followers: []string{},
			ISR:       []string{rm.nodeID},
		}
		rm.partitions[topic][0] = meta

		// 创建存储分区
		if err := rm.store.CreatePartition(topic, 0); err != nil {
			log.Printf("创建分区失败: %v", err)
		}
	}

	return []int{0}
}

// ReplicateMessages leader 复制消息到 follower
func (rm *ReplicaManager) ReplicateMessages(topic string, partition int, messages []*protocol.Message) error {
	meta := rm.getPartitionMeta(topic, partition)
	if meta == nil {
		return fmt.Errorf("分区不存在: %s-%d", topic, partition)
	}

	// 只有 leader 才能复制消息
	if meta.Leader != rm.nodeID {
		return fmt.Errorf("当前节点不是 leader")
	}

	// 写入本地存储
	if err := rm.store.Write(topic, partition, messages); err != nil {
		return err
	}

	// 并行复制到所有 follower
	var wg sync.WaitGroup
	errors := make(chan error, len(meta.Followers))

	for _, follower := range meta.Followers {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if err := rm.sendToFollower(nodeID, topic, partition, messages); err != nil {
				errors <- err
			}
		}(follower)
	}

	wg.Wait()
	close(errors)

	// 检查复制错误
	for err := range errors {
		if err != nil {
			return fmt.Errorf("复制失败: %w", err)
		}
	}

	return nil
}

// HandleReplicationRequest follower 处理复制请求
func (rm *ReplicaManager) HandleReplicationRequest(req *ReplicationRequest) *ReplicationResponse {
	meta := rm.getPartitionMeta(req.Topic, req.Partition)
	if meta == nil {
		return &ReplicationResponse{
			Success: false,
			Error:   "分区不存在",
		}
	}

	// 写入本地存储
	if err := rm.store.Write(req.Topic, req.Partition, req.Messages); err != nil {
		return &ReplicationResponse{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &ReplicationResponse{
		Success: true,
		Offset:  req.Offset + int64(len(req.Messages)),
	}
}

// 获取分区元数据
func (rm *ReplicaManager) getPartitionMeta(topic string, partition int) *PartitionMeta {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if p, ok := rm.partitions[topic]; ok {
		return p[partition]
	}
	return nil
}

package replication

import (
	"fmt"
	"log"
	"sync"

	"encoding/json" // 添加 json 包

	"github.com/eason-lee/lmq/pkg/network"
	"github.com/eason-lee/lmq/pkg/protocol"
	"github.com/eason-lee/lmq/pkg/store"
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

// ReplicaManager 复制管理器
// 添加新的字段和方法
type ReplicaManager struct {
    nodeID    string           // 当前节点 ID
    store     *store.FileStore
    partitions map[string]map[int]*PartitionMeta // topic -> partitionID -> meta
    mu        sync.RWMutex
    server  *network.Server
    clients map[string]*network.Client // nodeID -> client
}

// 修改构造函数
func NewReplicaManager(nodeID string, store *store.FileStore, addr string) *ReplicaManager {
    rm := &ReplicaManager{
        nodeID:     nodeID,
        store:      store,
        partitions: make(map[string]map[int]*PartitionMeta),
        server:     network.NewServer(addr),
        clients:    make(map[string]*network.Client),
    }

    // 注册复制请求处理器
    rm.server.RegisterHandler("replication", rm.handleReplicationRequest)
    
    return rm
}

// 添加启动方法
func (rm *ReplicaManager) Start() error {
    return rm.server.Start()
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

package replication

import (
	"fmt"
	"sync"

	"github.com/eason-lee/lmq/pkg/protocol"
	"github.com/eason-lee/lmq/pkg/store"
)

// PartitionMeta 分区元数据
type PartitionMeta struct {
    Topic     string
    ID        int
    Leader    string   // leader broker ID
    Followers []string // follower broker IDs
    ISR       []string // in-sync replicas
}

// ReplicaManager 复制管理器
type ReplicaManager struct {
    nodeID    string           // 当前节点 ID
    store     *store.FileStore
    partitions map[string]map[int]*PartitionMeta // topic -> partitionID -> meta
    mu        sync.RWMutex
}

// ReplicationRequest 复制请求
type ReplicationRequest struct {
    Topic     string
    Partition int
    Messages  []*protocol.Message
    Offset    int64
}

// ReplicationResponse 复制响应
type ReplicationResponse struct {
    Success bool
    Offset  int64
    Error   string
}

// NewReplicaManager 创建复制管理器
func NewReplicaManager(nodeID string, store *store.FileStore) *ReplicaManager {
    return &ReplicaManager{
        nodeID:     nodeID,
        store:      store,
        partitions: make(map[string]map[int]*PartitionMeta),
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

// 发送消息到 follower
func (rm *ReplicaManager) sendToFollower(nodeID string, topic string, partition int, messages []*protocol.Message) error {
    // TODO: 实现通过网络发送消息到 follower
    // 1. 建立连接
    // 2. 发送复制请求
    // 3. 等待响应
    // 4. 处理错误
    return nil
}
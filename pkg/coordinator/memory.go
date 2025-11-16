package coordinator

import (
    "context"
    "fmt"
    "sort"
    "strconv"
    "strings"
    "sync"
)

type MemoryCoordinator struct {
    mu sync.RWMutex

    services map[string][]*ServiceInfo // name -> services
    topics   map[string]int            // topic -> partitionCount
    parts    map[string]map[int]*PartitionInfo

    consumerGroups map[string]*ConsumerGroupInfo
    assignments    map[string]map[string]map[string][]int // group -> consumer -> topic -> partitions
    offsets        map[string]map[string]map[int]int64    // group -> topic -> partition -> offset
    replicaStatus  map[string]map[int]map[string]int64    // topic -> partition -> node -> offset
    isr            map[string]map[int][]string            // topic -> partition -> ISR
}

func NewMemoryCoordinator() *MemoryCoordinator {
    return &MemoryCoordinator{
        services:       make(map[string][]*ServiceInfo),
        topics:         make(map[string]int),
        parts:          make(map[string]map[int]*PartitionInfo),
        consumerGroups: make(map[string]*ConsumerGroupInfo),
        assignments:    make(map[string]map[string]map[string][]int),
        offsets:        make(map[string]map[string]map[int]int64),
        replicaStatus:  make(map[string]map[int]map[string]int64),
        isr:            make(map[string]map[int][]string),
    }
}

func (m *MemoryCoordinator) RegisterService(ctx context.Context, nodeID string, addr string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    host, port := splitHostPort(addr)
    m.services["lmq-broker"] = append(m.services["lmq-broker"], &ServiceInfo{
        ID:      nodeID,
        Name:    "lmq-broker",
        Address: host,
        Port:    port,
    })
    return nil
}

func (m *MemoryCoordinator) UnregisterService(ctx context.Context, nodeID string, addr string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    list := m.services["lmq-broker"]
    idx := -1
    for i, s := range list {
        if s.ID == nodeID {
            idx = i
            break
        }
    }
    if idx >= 0 {
        m.services["lmq-broker"] = append(list[:idx], list[idx+1:]...)
    }
    return nil
}

func (m *MemoryCoordinator) GetService(ctx context.Context, serviceName string) ([]string, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    var out []string
    for _, s := range m.services[serviceName] {
        out = append(out, fmt.Sprintf("%s:%d", s.Address, s.Port))
    }
    return out, nil
}

func (m *MemoryCoordinator) DiscoverService(ctx context.Context, serviceName string) ([]*ServiceInfo, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return append([]*ServiceInfo(nil), m.services[serviceName]...), nil
}

func (m *MemoryCoordinator) CreateTopic(ctx context.Context, topic string, partitionCount int) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    if _, ok := m.topics[topic]; ok {
        return fmt.Errorf("主题已存在: %s", topic)
    }
    m.topics[topic] = partitionCount
    return nil
}

func (m *MemoryCoordinator) DeleteTopic(ctx context.Context, topic string) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    delete(m.topics, topic)
    delete(m.parts, topic)
    return nil
}

func (m *MemoryCoordinator) GetTopics(ctx context.Context) ([]string, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    var ts []string
    for t := range m.topics {
        ts = append(ts, t)
    }
    sort.Strings(ts)
    return ts, nil
}

func (m *MemoryCoordinator) GetTopic(ctx context.Context, topic string) (map[string]interface{}, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    if pc, ok := m.topics[topic]; ok {
        return map[string]interface{}{"partitions": pc}, nil
    }
    return nil, nil
}

func (m *MemoryCoordinator) TopicExists(ctx context.Context, topic string) (bool, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    _, ok := m.topics[topic]
    return ok, nil
}

func (m *MemoryCoordinator) GetTopicPartitionCount(ctx context.Context, topic string) (int, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    pc, ok := m.topics[topic]
    if !ok {
        return 0, fmt.Errorf("主题不存在: %s", topic)
    }
    return pc, nil
}

func (m *MemoryCoordinator) WatchTopics(ctx context.Context) (<-chan []string, error) { ch := make(chan []string); close(ch); return ch, nil }

func (m *MemoryCoordinator) CreatePartition(ctx context.Context, partition *PartitionInfo) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    if _, ok := m.parts[partition.Topic]; !ok {
        m.parts[partition.Topic] = make(map[int]*PartitionInfo)
    }
    cp := *partition
    m.parts[partition.Topic][partition.ID] = &cp
    return nil
}

func (m *MemoryCoordinator) UpdatePartition(ctx context.Context, partition *PartitionInfo) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    if _, ok := m.parts[partition.Topic]; !ok { return fmt.Errorf("主题不存在") }
    cp := *partition
    m.parts[partition.Topic][partition.ID] = &cp
    return nil
}

func (m *MemoryCoordinator) GetPartition(ctx context.Context, topic string, partitionID int) (*PartitionInfo, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    if pmap, ok := m.parts[topic]; ok {
        if p, ok := pmap[partitionID]; ok {
            cp := *p
            return &cp, nil
        }
    }
    return nil, fmt.Errorf("分区不存在")
}

func (m *MemoryCoordinator) GetPartitions(ctx context.Context, topic string) ([]*PartitionInfo, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    var out []*PartitionInfo
    if pmap, ok := m.parts[topic]; ok {
        for _, p := range pmap {
            cp := *p
            out = append(out, &cp)
        }
    }
    sort.Slice(out, func(i,j int) bool { return out[i].ID < out[j].ID })
    return out, nil
}

func (m *MemoryCoordinator) GetNodePartitions(ctx context.Context, nodeID string) ([]*PartitionInfo, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    var out []*PartitionInfo
    for _, pmap := range m.parts {
        for _, p := range pmap {
            for _, r := range p.Replicas {
                if r == nodeID {
                    cp := *p
                    out = append(out, &cp)
                    break
                }
            }
        }
    }
    return out, nil
}

func (m *MemoryCoordinator) GetLeaderPartitions(ctx context.Context, nodeID string) ([]*PartitionInfo, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    var out []*PartitionInfo
    for _, pmap := range m.parts {
        for _, p := range pmap {
            if p.Leader == nodeID {
                cp := *p
                out = append(out, &cp)
            }
        }
    }
    return out, nil
}

func (m *MemoryCoordinator) GetFollowerPartitions(ctx context.Context, nodeID string) ([]*PartitionInfo, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    var out []*PartitionInfo
    for _, pmap := range m.parts {
        for _, p := range pmap {
            for _, f := range p.Followers {
                if f == nodeID {
                    cp := *p
                    out = append(out, &cp)
                    break
                }
            }
        }
    }
    return out, nil
}

func (m *MemoryCoordinator) GetPartitionLeader(ctx context.Context, topic string, partitionID int) (string, error) {
    p, err := m.GetPartition(ctx, topic, partitionID); if err != nil { return "", err }
    return p.Leader, nil
}

func (m *MemoryCoordinator) GetPartitionReplicas(ctx context.Context, topic string, partitionID int) ([]string, error) {
    p, err := m.GetPartition(ctx, topic, partitionID); if err != nil { return nil, err }
    return append([]string(nil), p.Replicas...), nil
}

func (m *MemoryCoordinator) IsPartitionLeader(ctx context.Context, topic string, partitionID int, nodeID string) (bool, error) {
    p, err := m.GetPartition(ctx, topic, partitionID); if err != nil { return false, err }
    return p.Leader == nodeID, nil
}

func (m *MemoryCoordinator) ElectPartitionLeader(ctx context.Context, topic string, partitionID int, nodeID string) (bool, error) {
    m.mu.Lock(); defer m.mu.Unlock()
    if pmap, ok := m.parts[topic]; ok {
        if p, ok := pmap[partitionID]; ok {
            p.Leader = nodeID
            return true, nil
        }
    }
    return false, fmt.Errorf("分区不存在")
}

func (m *MemoryCoordinator) RegisterConsumer(ctx context.Context, groupID string, topics []string) error {
    m.mu.Lock(); defer m.mu.Unlock()
    m.consumerGroups[groupID] = &ConsumerGroupInfo{GroupID: groupID, Topics: topics, Offsets: make(map[string]int64), Partitions: make(map[string]int)}
    if _, ok := m.assignments[groupID]; !ok { m.assignments[groupID] = make(map[string]map[string][]int) }
    if _, ok := m.offsets[groupID]; !ok { m.offsets[groupID] = make(map[string]map[int]int64) }
    return nil
}

func (m *MemoryCoordinator) UnregisterConsumer(ctx context.Context, groupID string) error {
    m.mu.Lock(); defer m.mu.Unlock()
    delete(m.consumerGroups, groupID)
    delete(m.assignments, groupID)
    delete(m.offsets, groupID)
    return nil
}

func (m *MemoryCoordinator) GetConsumerGroup(ctx context.Context, groupID string) (*ConsumerGroupInfo, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    if g, ok := m.consumerGroups[groupID]; ok { cp := *g; return &cp, nil }
    return nil, nil
}

func (m *MemoryCoordinator) CommitOffset(ctx context.Context, groupID string, topic string, partition int, offset int64) error {
    m.mu.Lock(); defer m.mu.Unlock()
    if _, ok := m.offsets[groupID]; !ok { m.offsets[groupID] = make(map[string]map[int]int64) }
    if _, ok := m.offsets[groupID][topic]; !ok { m.offsets[groupID][topic] = make(map[int]int64) }
    m.offsets[groupID][topic][partition] = offset
    return nil
}

func (m *MemoryCoordinator) GetConsumerOffset(ctx context.Context, groupID string, topic string, partition int) (int64, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    if tp, ok := m.offsets[groupID]; ok {
        if mp, ok := tp[topic]; ok {
            if off, ok := mp[partition]; ok { return off, nil }
        }
    }
    return 0, nil
}

func (m *MemoryCoordinator) GetConsumerPartition(ctx context.Context, groupID string, topic string) (int, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    if g, ok := m.consumerGroups[groupID]; ok { if p, ok := g.Partitions[topic]; ok { return p, nil } }
    return 0, nil
}

func (m *MemoryCoordinator) GetConsumersInGroup(ctx context.Context, groupID string) ([]string, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    var cs []string
    if grp, ok := m.assignments[groupID]; ok {
        for c := range grp { cs = append(cs, c) }
    }
    // 如果没有显式记录成员，返回一个默认消费者ID，允许分配
    if len(cs) == 0 { cs = append(cs, fmt.Sprintf("%s-consumer", groupID)) }
    sort.Strings(cs)
    return cs, nil
}

func (m *MemoryCoordinator) GetTopicPartitions(ctx context.Context, topic string) ([]*PartitionInfo, error) {
    return m.GetPartitions(ctx, topic)
}

func (m *MemoryCoordinator) AssignPartitionToConsumer(ctx context.Context, topic string, partitionID int, groupID string, consumerID string) error {
    m.mu.Lock(); defer m.mu.Unlock()
    if _, ok := m.assignments[groupID]; !ok { m.assignments[groupID] = make(map[string]map[string][]int) }
    if _, ok := m.assignments[groupID][consumerID]; !ok { m.assignments[groupID][consumerID] = make(map[string][]int) }
    m.assignments[groupID][consumerID][topic] = append(m.assignments[groupID][consumerID][topic], partitionID)
    return nil
}

func (m *MemoryCoordinator) TriggerGroupRebalance(ctx context.Context, groupID string) error { return nil }

func (m *MemoryCoordinator) GetConsumerAssignedPartitions(ctx context.Context, groupID string, consumerID string, topic string) ([]int, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    if grp, ok := m.assignments[groupID]; ok {
        if ct, ok := grp[consumerID]; ok {
            return append([]int(nil), ct[topic]...), nil
        }
    }
    return []int{}, nil
}

func (m *MemoryCoordinator) RegisterReplicaStatus(ctx context.Context, topic string, partitionID int, nodeID string, offset int64) error {
    m.mu.Lock(); defer m.mu.Unlock()
    if _, ok := m.replicaStatus[topic]; !ok { m.replicaStatus[topic] = make(map[int]map[string]int64) }
    if _, ok := m.replicaStatus[topic][partitionID]; !ok { m.replicaStatus[topic][partitionID] = make(map[string]int64) }
    m.replicaStatus[topic][partitionID][nodeID] = offset
    return nil
}

func (m *MemoryCoordinator) GetReplicaStatus(ctx context.Context, topic string, partitionID int, nodeID string) (int64, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    if tp, ok := m.replicaStatus[topic]; ok {
        if mp, ok := tp[partitionID]; ok {
            if off, ok := mp[nodeID]; ok { return off, nil }
        }
    }
    return 0, nil
}

func (m *MemoryCoordinator) GetAllReplicaStatus(ctx context.Context, topic string, partitionID int) (map[string]int64, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    out := make(map[string]int64)
    if tp, ok := m.replicaStatus[topic]; ok {
        if mp, ok := tp[partitionID]; ok {
            for n, off := range mp { out[n] = off }
        }
    }
    return out, nil
}

func (m *MemoryCoordinator) GetISR(ctx context.Context, topic string, partitionID int) ([]string, error) {
    m.mu.RLock(); defer m.mu.RUnlock()
    if tp, ok := m.isr[topic]; ok {
        if isr, ok := tp[partitionID]; ok { return append([]string(nil), isr...), nil }
    }
    return []string{}, nil
}

func (m *MemoryCoordinator) UpdateISR(ctx context.Context, topic string, partitionID int, isr []string) error {
    m.mu.Lock(); defer m.mu.Unlock()
    if _, ok := m.isr[topic]; !ok { m.isr[topic] = make(map[int][]string) }
    m.isr[topic][partitionID] = append([]string(nil), isr...)
    return nil
}

func (m *MemoryCoordinator) WatchISR(ctx context.Context, topic string, partitionID int) (<-chan []string, error) { ch := make(chan []string); close(ch); return ch, nil }
func (m *MemoryCoordinator) AddToISR(ctx context.Context, topic string, partitionID int, nodeID string) error { return nil }
func (m *MemoryCoordinator) RemoveFromISR(ctx context.Context, topic string, partitionID int, nodeID string) error { return nil }

func (m *MemoryCoordinator) Lock(ctx context.Context, key string) error { return nil }
func (m *MemoryCoordinator) Unlock(ctx context.Context, key string) error { return nil }
func (m *MemoryCoordinator) Close() error { return nil }

func splitHostPort(addr string) (string, int) {
    parts := strings.Split(addr, ":")
    if len(parts) == 2 {
        p, _ := strconv.Atoi(parts[1])
        return parts[0], p
    }
    return addr, 0
}


package store

import (
	"fmt"
	"sort"
	"sync"
)

// PartitionAssignment 分区分配信息
type PartitionAssignment struct {
	Topic       string   // 主题名称
	PartitionID int      // 分区ID
	BrokerID    string   // Broker ID
	IsLeader    bool     // 是否为主副本
	Replicas    []string // 所有副本的Broker ID列表
}

// PartitionRebalancer 分区重平衡器接口
type PartitionRebalancer interface {
	// Rebalance 重新平衡分区分配
	// 参数:
	//   - topic: 主题名称
	//   - partitionCount: 分区数量
	//   - brokers: 可用的Broker列表
	//   - replicationFactor: 复制因子
	//   - currentAssignments: 当前的分区分配情况
	// 返回:
	//   - 新的分区分配方案
	//   - 错误信息
	Rebalance(topic string, partitionCount int, brokers []string, replicationFactor int, currentAssignments []PartitionAssignment) ([]PartitionAssignment, error)
	// Name 返回重平衡器名称
	Name() string
}

// RoundRobinRebalancer 轮询分区重平衡器
// 使用轮询算法分配分区到Broker
type RoundRobinRebalancer struct {
	mu sync.Mutex
}

// NewRoundRobinRebalancer 创建新的轮询分区重平衡器
func NewRoundRobinRebalancer() *RoundRobinRebalancer {
	return &RoundRobinRebalancer{}
}

// Rebalance 重新平衡分区分配
func (r *RoundRobinRebalancer) Rebalance(topic string, partitionCount int, brokers []string, replicationFactor int, currentAssignments []PartitionAssignment) ([]PartitionAssignment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 检查参数
	if len(brokers) == 0 {
		return nil, fmt.Errorf("没有可用的Broker")
	}

	if replicationFactor > len(brokers) {
		return nil, fmt.Errorf("复制因子(%d)大于可用Broker数量(%d)", replicationFactor, len(brokers))
	}

	// 创建新的分配方案
	assignments := make([]PartitionAssignment, partitionCount)

	// 对Broker进行排序，确保结果的确定性
	sortedBrokers := make([]string, len(brokers))
	copy(sortedBrokers, brokers)
	sort.Strings(sortedBrokers)

	// 为每个分区分配Broker
	for partID := 0; partID < partitionCount; partID++ {
		// 为当前分区选择副本
		replicas := make([]string, replicationFactor)
		for i := 0; i < replicationFactor; i++ {
			// 使用轮询算法选择Broker
			brokerIndex := (partID + i) % len(sortedBrokers)
			replicas[i] = sortedBrokers[brokerIndex]
		}

		// 创建分区分配
		assignments[partID] = PartitionAssignment{
			Topic:       topic,
			PartitionID: partID,
			BrokerID:    replicas[0], // 第一个副本为主副本
			IsLeader:    true,
			Replicas:    replicas,
		}
	}

	return assignments, nil
}

// Name 返回重平衡器名称
func (r *RoundRobinRebalancer) Name() string {
	return "round-robin"
}

// RackAwareRebalancer 机架感知分区重平衡器
// 考虑Broker所在的机架，尽量将副本分布在不同机架上
type RackAwareRebalancer struct {
	mu sync.Mutex
}

// BrokerInfo Broker信息
type BrokerInfo struct {
	ID   string // Broker ID
	Rack string // 机架ID
}

// NewRackAwareRebalancer 创建新的机架感知分区重平衡器
func NewRackAwareRebalancer() *RackAwareRebalancer {
	return &RackAwareRebalancer{}
}

// Rebalance 重新平衡分区分配
func (r *RackAwareRebalancer) Rebalance(topic string, partitionCount int, brokers []string, replicationFactor int, currentAssignments []PartitionAssignment) ([]PartitionAssignment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 检查参数
	if len(brokers) == 0 {
		return nil, fmt.Errorf("没有可用的Broker")
	}

	if replicationFactor > len(brokers) {
		return nil, fmt.Errorf("复制因子(%d)大于可用Broker数量(%d)", replicationFactor, len(brokers))
	}

	// 假设我们已经获取了每个Broker的机架信息
	// 在实际生产环境中，这些信息应该从配置或服务发现系统获取
	brokerRackMap := make(map[string]string)

	// 模拟获取机架信息
	// 在实际实现中，这部分应该替换为从配置或元数据服务获取真实的机架信息
	rackCount := 3 // 假设有3个机架
	for i, broker := range brokers {
		rackID := fmt.Sprintf("rack-%d", i%rackCount)
		brokerRackMap[broker] = rackID
	}

	// 按机架对Broker进行分组
	rackBrokers := make(map[string][]string)
	for broker, rack := range brokerRackMap {
		rackBrokers[rack] = append(rackBrokers[rack], broker)
	}

	// 创建新的分配方案
	assignments := make([]PartitionAssignment, partitionCount)

	// 对Broker进行排序，确保结果的确定性
	sortedBrokers := make([]string, len(brokers))
	copy(sortedBrokers, brokers)
	sort.Strings(sortedBrokers)

	// 获取所有机架ID并排序
	rackIDs := make([]string, 0, len(rackBrokers))
	for rack := range rackBrokers {
		rackIDs = append(rackIDs, rack)
	}
	sort.Strings(rackIDs)

	// 为每个分区分配Broker
	for partID := 0; partID < partitionCount; partID++ {
		// 为当前分区选择副本，尽量从不同机架选择
		replicas := make([]string, 0, replicationFactor)
		selectedRacks := make(map[string]bool)

		// 首先尝试从每个机架选择一个Broker
		for i := 0; i < len(rackIDs) && len(replicas) < replicationFactor; i++ {
			rackIndex := (partID + i) % len(rackIDs)
			rack := rackIDs[rackIndex]

			// 如果该机架有可用的Broker
			if len(rackBrokers[rack]) > 0 {
				// 选择该机架中的一个Broker
				brokerIndex := partID % len(rackBrokers[rack])
				replicas = append(replicas, rackBrokers[rack][brokerIndex])
				selectedRacks[rack] = true
			}
		}

		// 如果还需要更多副本，从所有Broker中选择，但尽量避免选择同一机架的
		for i := 0; len(replicas) < replicationFactor && i < len(sortedBrokers); i++ {
			broker := sortedBrokers[(partID+i)%len(sortedBrokers)]
			rack := brokerRackMap[broker]

			// 如果该机架已经选择了Broker，且还有其他机架可选，则跳过
			if selectedRacks[rack] && len(selectedRacks) < len(rackIDs) {
				continue
			}

			// 检查该Broker是否已经被选择
			alreadySelected := false
			for _, r := range replicas {
				if r == broker {
					alreadySelected = true
					break
				}
			}

			if !alreadySelected {
				replicas = append(replicas, broker)
				selectedRacks[rack] = true
			}
		}

		// 如果副本数量不足，可能是因为机架数量少于复制因子
		// 这种情况下，允许在同一机架上选择多个Broker
		if len(replicas) < replicationFactor {
			for i := 0; len(replicas) < replicationFactor && i < len(sortedBrokers); i++ {
				broker := sortedBrokers[(partID+i)%len(sortedBrokers)]

				// 检查该Broker是否已经被选择
				alreadySelected := false
				for _, r := range replicas {
					if r == broker {
						alreadySelected = true
						break
					}
				}

				if !alreadySelected {
					replicas = append(replicas, broker)
				}
			}
		}

		// 创建分区分配
		assignments[partID] = PartitionAssignment{
			Topic:       topic,
			PartitionID: partID,
			BrokerID:    replicas[0], // 第一个副本为主副本
			IsLeader:    true,
			Replicas:    replicas,
		}
	}

	return assignments, nil
}

// Name 返回重平衡器名称
func (r *RackAwareRebalancer) Name() string {
	return "rack-aware"
}

// MinimumMovementRebalancer 最小移动分区重平衡器
// 在保证负载均衡的前提下，尽量减少分区移动
type MinimumMovementRebalancer struct {
	mu sync.Mutex
}

// NewMinimumMovementRebalancer 创建新的最小移动分区重平衡器
func NewMinimumMovementRebalancer() *MinimumMovementRebalancer {
	return &MinimumMovementRebalancer{}
}

// Rebalance 重新平衡分区分配
func (m *MinimumMovementRebalancer) Rebalance(topic string, partitionCount int, brokers []string, replicationFactor int, currentAssignments []PartitionAssignment) ([]PartitionAssignment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查参数
	if len(brokers) == 0 {
		return nil, fmt.Errorf("没有可用的Broker")
	}

	if replicationFactor > len(brokers) {
		return nil, fmt.Errorf("复制因子(%d)大于可用Broker数量(%d)", replicationFactor, len(brokers))
	}

	// 如果没有当前分配或分区数量变化，使用轮询算法创建新的分配
	if len(currentAssignments) == 0 || len(currentAssignments) != partitionCount {
		rr := NewRoundRobinRebalancer()
		return rr.Rebalance(topic, partitionCount, brokers, replicationFactor, currentAssignments)
	}

	// 创建Broker集合
	brokerSet := make(map[string]struct{})
	for _, broker := range brokers {
		brokerSet[broker] = struct{}{}
	}

	// 计算每个Broker当前的分区数量
	brokerPartitionCount := make(map[string]int)
	for _, broker := range brokers {
		brokerPartitionCount[broker] = 0
	}

	// 创建新的分配方案，尽量保持现有分配
	newAssignments := make([]PartitionAssignment, partitionCount)
	for i, assignment := range currentAssignments {
		// 检查当前主副本是否仍然可用
		_, leaderAvailable := brokerSet[assignment.BrokerID]

		// 检查当前副本是否仍然可用
		availableReplicas := make([]string, 0, replicationFactor)
		for _, replica := range assignment.Replicas {
			if _, ok := brokerSet[replica]; ok {
				availableReplicas = append(availableReplicas, replica)
			}
		}

		// 如果主副本可用且有足够的副本，保持现有分配
		if leaderAvailable && len(availableReplicas) >= replicationFactor {
			// 如果有多余的副本，只保留需要的数量
			if len(availableReplicas) > replicationFactor {
				availableReplicas = availableReplicas[:replicationFactor]
			}

			newAssignments[i] = PartitionAssignment{
				Topic:       topic,
				PartitionID: assignment.PartitionID,
				BrokerID:    assignment.BrokerID,
				IsLeader:    true,
				Replicas:    availableReplicas,
			}

			// 更新Broker分区计数
			for _, replica := range availableReplicas {
				brokerPartitionCount[replica]++
			}
		} else {
			// 需要重新分配
			// 使用轮询算法为这个分区分配新的副本
			sortedBrokers := make([]string, len(brokers))
			copy(sortedBrokers, brokers)
			sort.Strings(sortedBrokers)

			// 按照当前负载排序Broker
			sort.SliceStable(sortedBrokers, func(i, j int) bool {
				return brokerPartitionCount[sortedBrokers[i]] < brokerPartitionCount[sortedBrokers[j]]
			})

			// 选择负载最小的Broker作为副本
			replicas := make([]string, replicationFactor)
			for j := 0; j < replicationFactor; j++ {
				replicas[j] = sortedBrokers[j]
				brokerPartitionCount[sortedBrokers[j]]++
			}

			newAssignments[i] = PartitionAssignment{
				Topic:       topic,
				PartitionID: assignment.PartitionID,
				BrokerID:    replicas[0],
				IsLeader:    true,
				Replicas:    replicas,
			}
		}
	}

	return newAssignments, nil
}

// Name 返回重平衡器名称
func (m *MinimumMovementRebalancer) Name() string {
	return "minimum-movement"
}

// DefaultRebalancer 默认分区重平衡器
// 使用最小移动策略
var DefaultRebalancer PartitionRebalancer = NewMinimumMovementRebalancer()

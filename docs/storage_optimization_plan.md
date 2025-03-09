# LMQ 存储与分区优化设计方案

## 背景

当前 LMQ 已经实现了基本的消息存储功能，包括分区和段的管理。但是，为了提高系统的性能、可靠性和可维护性，我们需要参考 Kafka 的设计理念，对存储层进行优化。

## 当前存储实现分析

### 现有架构

目前的存储层实现了以下组件：

1. **Store 接口**：定义了存储层的基本操作
2. **FileStore**：基于文件系统的存储实现
3. **Partition**：分区管理，每个分区包含多个段
4. **Segment**：段管理，每个段包含数据文件和索引文件

### 存在的问题

1. **索引效率低**：当前索引机制需要遍历所有索引记录，查找效率低
2. **段管理不够灵活**：段的创建和切换机制不够完善
3. **数据压缩缺失**：没有实现消息压缩功能
4. **清理策略简单**：仅基于时间的清理策略，不支持基于大小的清理
5. **分区动态扩展不支持**：不支持在线增加分区数量

## 优化方案

### 1. 日志结构存储优化

#### 1.1 消息格式重构

优化消息格式，采用更紧凑的二进制格式：

```
消息格式：
+----------------+----------------+----------------+----------------+
|  消息长度(4B)   |  消息头部长度(1B) |     消息头部     |     消息体      |
+----------------+----------------+----------------+----------------+

消息头部格式：
+----------------+----------------+----------------+----------------+
|   消息魔数(1B)  |   版本号(1B)   |   属性标志(1B)  |   CRC32(4B)    |
+----------------+----------------+----------------+----------------+
|   消息ID长度    |     消息ID     |   主题长度      |      主题       |
+----------------+----------------+----------------+----------------+
|     时间戳(8B)  |   分区ID(4B)   |   保留字段      |                |
+----------------+----------------+----------------+----------------+
```

#### 1.2 批量写入优化

实现批量写入机制，减少磁盘 I/O 操作：

```go
// 批量写入接口
func (s *Segment) WriteBatch(messages []*pb.Message) error {
    // 预分配缓冲区
    // 批量序列化消息
    // 一次性写入磁盘
    // 批量更新索引
}
```

#### 1.3 零拷贝实现

利用 `sendfile` 系统调用实现零拷贝传输：

```go
// 零拷贝读取接口
func (s *Segment) ReadWithZeroCopy(offset int64, size int32) ([]byte, error) {
    // 使用 mmap 或 sendfile 实现零拷贝
}
```

### 2. 分段文件管理优化

#### 2.1 分段策略优化

增加多种分段策略：

1. **基于大小的分段**：当段大小达到阈值时创建新段
2. **基于时间的分段**：当段的时间跨度达到阈值时创建新段
3. **基于消息数量的分段**：当段的消息数量达到阈值时创建新段

```go
// 分段策略接口
type SegmentRollStrategy interface {
    ShouldRoll(segment *Segment) bool
}

// 大小分段策略
type SizeBasedRollStrategy struct {
    MaxSize int64
}

// 时间分段策略
type TimeBasedRollStrategy struct {
    MaxTimeSpan time.Duration
}

// 消息数量分段策略
type CountBasedRollStrategy struct {
    MaxCount int
}
```

#### 2.2 清理策略优化

增加多种清理策略：

1. **基于时间的清理**：删除超过保留时间的段
2. **基于大小的清理**：当分区总大小超过阈值时，删除最旧的段
3. **基于偏移量的清理**：删除小于指定偏移量的段

```go
// 清理策略接口
type CleanupPolicy interface {
    ShouldCleanup(partition *Partition) []*Segment
}

// 时间清理策略
type TimeBasedCleanupPolicy struct {
    RetentionTime time.Duration
}

// 大小清理策略
type SizeBasedCleanupPolicy struct {
    MaxPartitionSize int64
}

// 偏移量清理策略
type OffsetBasedCleanupPolicy struct {
    MinOffset int64
}
```

### 3. 索引机制改进

#### 3.1 稀疏索引实现

实现稀疏索引，减少索引文件大小，提高查找效率：

```go
// 稀疏索引配置
type SparseIndexConfig struct {
    IndexInterval int // 每隔多少条消息创建一个索引项
}

// 稀疏索引实现
func (s *Segment) WriteSparseIndex(msg *pb.Message, position int64, size int32) error {
    // 判断是否需要创建索引项
    // 写入索引
}
```

#### 3.2 索引文件格式优化

优化索引文件格式，支持快速查找：

```
索引文件格式：
+----------------+----------------+----------------+----------------+
|   偏移量(8B)    |   位置(8B)     |   大小(4B)     |   时间戳(8B)    |
+----------------+----------------+----------------+----------------+
```

#### 3.3 二分查找实现

实现二分查找算法，加速消息检索：

```go
// 二分查找实现
func (s *Segment) BinarySearch(targetOffset int64) (int, error) {
    // 二分查找算法实现
}
```

### 4. 分区动态扩展

#### 4.1 分区创建接口

实现在线增加分区的接口：

```go
// 创建分区接口
func (fs *FileStore) CreatePartition(topic string, partitionID int) error {
    // 创建分区目录
    // 初始化分区
    // 更新元数据
}
```

#### 4.2 分区重平衡算法

实现分区重平衡算法，优化集群资源利用：

```go
// 分区重平衡接口
func (c *Coordinator) RebalancePartitions(topic string, brokers []string) (map[string][]int, error) {
    // 计算分区分配方案
    // 返回 broker -> 分区列表 的映射
}
```

### 5. 消息压缩

#### 5.1 压缩算法支持

支持多种压缩算法：

1. Gzip
2. Snappy
3. LZ4

```go
// 压缩类型
type CompressionType int

const (
    NoCompression CompressionType = iota
    GzipCompression
    SnappyCompression
    LZ4Compression
)

// 压缩接口
func Compress(data []byte, compressionType CompressionType) ([]byte, error) {
    // 根据压缩类型选择压缩算法
}

// 解压接口
func Decompress(data []byte, compressionType CompressionType) ([]byte, error) {
    // 根据压缩类型选择解压算法
}
```

## 实施计划

### 第一阶段：基础优化

1. 消息格式重构
2. 批量写入优化
3. 稀疏索引实现

### 第二阶段：高级特性

1. 分段策略优化
2. 清理策略优化
3. 消息压缩支持

### 第三阶段：性能优化

1. 零拷贝实现
2. 页缓存优化
3. 异步 I/O 改进

### 第四阶段：可扩展性增强

1. 分区动态扩展
2. 分区重平衡算法

## 预期收益

1. **性能提升**：通过批量处理、零拷贝、压缩等技术，提高系统吞吐量
2. **存储效率**：通过压缩和优化的存储格式，减少存储空间占用
3. **可靠性增强**：通过完善的分段和清理策略，提高系统稳定性
4. **可扩展性提升**：通过动态分区和重平衡，支持系统动态扩展

## 风险与挑战

1. **兼容性**：新的存储格式需要考虑与旧版本的兼容
2. **迁移成本**：现有数据需要迁移到新的存储格式
3. **测试复杂度**：需要全面测试各种场景下的性能和稳定性

## 下一步行动

1. 详细设计消息格式和索引格式
2. 实现批量写入和稀疏索引
3. 编写单元测试和性能测试
4. 进行小规模测试和验证
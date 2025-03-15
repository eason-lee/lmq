package store

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

// AsyncIOConfig 异步I/O配置
type AsyncIOConfig struct {
	WorkerCount   int           // 工作协程数量
	QueueSize     int           // 队列大小
	FlushInterval time.Duration // 刷新间隔
	Enabled       bool          // 是否启用异步I/O
	MaxBatchSize  int           // 最大批处理大小
	MaxWaitTime   time.Duration // 最大等待时间
}

// DefaultAsyncIOConfig 默认异步I/O配置
var DefaultAsyncIOConfig = AsyncIOConfig{
	WorkerCount:   4,               // 默认4个工作协程
	QueueSize:     1024,            // 默认队列大小1024
	FlushInterval: 1 * time.Second, // 默认1秒刷新一次
	Enabled:       true,
	MaxBatchSize:  100,                    // 默认最大批处理100个请求
	MaxWaitTime:   100 * time.Millisecond, // 默认最大等待100ms
}

// IOOperation 表示I/O操作类型
type IOOperation int

const (
	IORead IOOperation = iota
	IOWrite
	IOSync
)

// IORequest 表示I/O请求
type IORequest struct {
	Op       IOOperation         // 操作类型
	FilePath string              // 文件路径
	Offset   int64               // 偏移量
	Data     []byte              // 数据（写入时使用）
	Size     int                 // 大小（读取时使用）
	Callback func([]byte, error) // 回调函数
}

// AsyncIO 异步I/O管理器
type AsyncIO struct {
	config    AsyncIOConfig
	queue     chan *IORequest
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	pageCache *PageCache
	stats     AsyncIOStats
}

// AsyncIOStats 异步I/O统计信息
type AsyncIOStats struct {
	TotalRequests   int64 // 总请求数
	ReadRequests    int64 // 读请求数
	WriteRequests   int64 // 写请求数
	SyncRequests    int64 // 同步请求数
	BatchedRequests int64 // 批处理请求数
	Errors          int64 // 错误数
}

// NewAsyncIO 创建新的异步I/O管理器
func NewAsyncIO(config AsyncIOConfig, pageCache *PageCache) *AsyncIO {
	ctx, cancel := context.WithCancel(context.Background())

	asyncIO := &AsyncIO{
		config:    config,
		queue:     make(chan *IORequest, config.QueueSize),
		ctx:       ctx,
		cancel:    cancel,
		pageCache: pageCache,
	}

	// 启动工作协程
	if config.Enabled {
		asyncIO.start()
	}

	return asyncIO
}

// start 启动异步I/O处理
func (a *AsyncIO) start() {
	// 启动工作协程
	for i := 0; i < a.config.WorkerCount; i++ {
		a.wg.Add(1)
		go a.worker()
	}

	// 启动定时刷新协程
	a.wg.Add(1)
	go a.flushWorker()
}

// worker 工作协程
func (a *AsyncIO) worker() {
	defer a.wg.Done()

	// 批处理缓冲区
	writeBatch := make(map[string][]*IORequest)
	readBatch := make(map[string][]*IORequest)

	processBatch := func() {
		// 处理写批次
		for filePath, requests := range writeBatch {
			a.processBatchWrite(filePath, requests)
		}
		// 清空写批次
		for k := range writeBatch {
			delete(writeBatch, k)
		}

		// 处理读批次
		for filePath, requests := range readBatch {
			a.processBatchRead(filePath, requests)
		}
		// 清空读批次
		for k := range readBatch {
			delete(readBatch, k)
		}
	}

	batchTimer := time.NewTimer(a.config.MaxWaitTime)
	batchSize := 0

	for {
		select {
		case <-a.ctx.Done():
			// 处理剩余的批次
			processBatch()
			return

		case req := <-a.queue:
			a.mu.Lock()
			a.stats.TotalRequests++
			switch req.Op {
			case IORead:
				a.stats.ReadRequests++
				readBatch[req.FilePath] = append(readBatch[req.FilePath], req)
			case IOWrite:
				a.stats.WriteRequests++
				writeBatch[req.FilePath] = append(writeBatch[req.FilePath], req)
			case IOSync:
				a.stats.SyncRequests++
				// 同步请求立即处理
				a.mu.Unlock()
				a.processSync(req)
				a.mu.Lock()
			}
			batchSize++
			a.mu.Unlock()

			// 如果批次已满，立即处理
			if batchSize >= a.config.MaxBatchSize {
				processBatch()
				batchSize = 0
				// 重置定时器
				if !batchTimer.Stop() {
					<-batchTimer.C
				}
				batchTimer.Reset(a.config.MaxWaitTime)
			}

		case <-batchTimer.C:
			// 超时，处理当前批次
			if batchSize > 0 {
				processBatch()
				batchSize = 0
			}
			batchTimer.Reset(a.config.MaxWaitTime)
		}
	}
}

// flushWorker 定时刷新工作协程
func (a *AsyncIO) flushWorker() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			// 刷新页缓存中的脏页
			if a.pageCache != nil {
				a.pageCache.FlushDirtyPages()
			}
		}
	}
}

// processBatchWrite 处理批量写请求
func (a *AsyncIO) processBatchWrite(filePath string, requests []*IORequest) {
	// 按偏移量排序请求
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].Offset < requests[j].Offset
	})

	// 尝试合并连续的写请求
	mergedRequests := make([]*IORequest, 0)
	currentReq := requests[0]
	mergedData := make([]byte, len(currentReq.Data))
	copy(mergedData, currentReq.Data)

	for i := 1; i < len(requests); i++ {
		req := requests[i]
		// 如果当前请求与上一个请求连续，则合并
		if currentReq.Offset+int64(len(currentReq.Data)) == req.Offset {
			// 扩展合并数据
			mergedData = append(mergedData, req.Data...)
			// 更新当前请求的数据
			currentReq.Data = mergedData
		} else {
			// 不连续，添加当前请求到合并列表
			mergedRequests = append(mergedRequests, currentReq)
			// 开始新的合并
			currentReq = req
			mergedData = make([]byte, len(req.Data))
			copy(mergedData, req.Data)
		}
	}

	// 添加最后一个合并请求
	mergedRequests = append(mergedRequests, currentReq)

	// 处理合并后的请求
	for _, req := range mergedRequests {
		// 尝试从页缓存获取
		cacheKey := fmt.Sprintf("%s:%d", filePath, req.Offset)

		// 更新页缓存
		if a.pageCache != nil {
			a.pageCache.Put(cacheKey, req.Data, true)
		}

		// 执行实际写入
		err := a.doWrite(filePath, req.Offset, req.Data)

		// 调用回调函数
		if req.Callback != nil {
			req.Callback(nil, err)
		}

		// 更新统计信息
		a.mu.Lock()
		a.stats.BatchedRequests++
		if err != nil {
			a.stats.Errors++
		}
		a.mu.Unlock()
	}
}

// processBatchRead 处理批量读请求
func (a *AsyncIO) processBatchRead(filePath string, requests []*IORequest) {
	// 按偏移量排序请求
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].Offset < requests[j].Offset
	})

	// 处理每个读请求
	for _, req := range requests {
		// 尝试从页缓存获取
		cacheKey := fmt.Sprintf("%s:%d", filePath, req.Offset)
		var data []byte
		var err error
		var fromCache bool

		if a.pageCache != nil {
			data, fromCache = a.pageCache.Get(cacheKey)
		}

		// 如果缓存未命中，从文件读取
		if !fromCache {
			data, err = a.doRead(filePath, req.Offset, req.Size)

			// 更新页缓存
			if err == nil && a.pageCache != nil && len(data) <= a.pageCache.config.PageSize {
				a.pageCache.Put(cacheKey, data, false)
			}
		} else if a.pageCache != nil {
			// 释放缓存引用
			defer a.pageCache.Release(cacheKey)
		}

		// 调用回调函数
		if req.Callback != nil {
			req.Callback(data, err)
		}

		// 更新统计信息
		a.mu.Lock()
		a.stats.BatchedRequests++
		if err != nil {
			a.stats.Errors++
		}
		a.mu.Unlock()
	}
}

// processSync 处理同步请求
func (a *AsyncIO) processSync(req *IORequest) {
	// 执行文件同步
	err := a.doSync(req.FilePath)

	// 调用回调函数
	if req.Callback != nil {
		req.Callback(nil, err)
	}

	// 更新统计信息
	a.mu.Lock()
	if err != nil {
		a.stats.Errors++
	}
	a.mu.Unlock()
}

// doWrite 执行实际的写操作
func (a *AsyncIO) doWrite(filePath string, offset int64, data []byte) error {
	// 打开文件
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	// 写入数据
	_, err = file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("写入数据失败: %w", err)
	}

	return nil
}

// doRead 执行实际的读操作
func (a *AsyncIO) doRead(filePath string, offset int64, size int) ([]byte, error) {
	// 打开文件
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	// 读取数据
	data := make([]byte, size)
	_, err = file.ReadAt(data, offset)
	if err != nil {
		return nil, fmt.Errorf("读取数据失败: %w", err)
	}

	return data, nil
}

// doSync 执行实际的同步操作
func (a *AsyncIO) doSync(filePath string) error {
	// 打开文件
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	// 同步数据
	if err := file.Sync(); err != nil {
		return fmt.Errorf("同步文件失败: %w", err)
	}

	return nil
}

// AsyncRead 异步读取数据
func (a *AsyncIO) AsyncRead(filePath string, offset int64, size int, callback func([]byte, error)) {
	if !a.config.Enabled {
		// 如果异步I/O未启用，直接同步读取
		data, err := a.doRead(filePath, offset, size)
		if callback != nil {
			callback(data, err)
		}
		return
	}

	// 创建读请求
	req := &IORequest{
		Op:       IORead,
		FilePath: filePath,
		Offset:   offset,
		Size:     size,
		Callback: callback,
	}

	// 将请求加入队列
	select {
	case a.queue <- req:
		// 请求已加入队列
	default:
		// 队列已满，直接同步处理
		data, err := a.doRead(filePath, offset, size)
		if callback != nil {
			callback(data, err)
		}
	}
}

// AsyncWrite 异步写入数据
func (a *AsyncIO) AsyncWrite(filePath string, offset int64, data []byte, callback func([]byte, error)) {
	if !a.config.Enabled {
		// 如果异步I/O未启用，直接同步写入
		err := a.doWrite(filePath, offset, data)
		if callback != nil {
			callback(nil, err)
		}
		return
	}

	// 创建写请求
	req := &IORequest{
		Op:       IOWrite,
		FilePath: filePath,
		Offset:   offset,
		Data:     data,
		Callback: callback,
	}

	// 将请求加入队列
	select {
	case a.queue <- req:
		// 请求已加入队列
	default:
		// 队列已满，直接同步处理
		err := a.doWrite(filePath, offset, data)
		if callback != nil {
			callback(nil, err)
		}
	}
}

// AsyncSync 异步同步文件
func (a *AsyncIO) AsyncSync(filePath string, callback func([]byte, error)) {
	if !a.config.Enabled {
		// 如果异步I/O未启用，直接同步处理
		err := a.doSync(filePath)
		if callback != nil {
			callback(nil, err)
		}
		return
	}

	// 创建同步请求
	req := &IORequest{
		Op:       IOSync,
		FilePath: filePath,
		Callback: callback,
	}

	// 将请求加入队列
	select {
	case a.queue <- req:
		// 请求已加入队列
	default:
		// 队列已满，直接同步处理
		err := a.doSync(filePath)
		if callback != nil {
			callback(nil, err)
		}
	}
}

// Close 关闭异步I/O管理器
func (a *AsyncIO) Close() error {
	// 取消上下文
	a.cancel()

	// 等待所有工作协程退出
	a.wg.Wait()

	return nil
}

// GetStats 获取异步I/O统计信息
func (a *AsyncIO) GetStats() AsyncIOStats {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.stats
}

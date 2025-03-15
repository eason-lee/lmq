package store

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// PageCacheConfig 页缓存配置
type PageCacheConfig struct {
	MaxPages      int           // 最大缓存页数
	PageSize      int           // 页大小（字节）
	EvictionDelay time.Duration // 驱逐延迟时间
	Enabled       bool          // 是否启用页缓存
}

// DefaultPageCacheConfig 默认页缓存配置
var DefaultPageCacheConfig = PageCacheConfig{
	MaxPages:      1024,            // 默认缓存1024页
	PageSize:      4 * 1024,        // 默认页大小4KB
	EvictionDelay: 5 * time.Minute, // 默认5分钟后驱逐
	Enabled:       true,
}

// CachedPage 缓存页
type CachedPage struct {
	Key       string    // 缓存键（文件路径+偏移量）
	Data      []byte    // 页数据
	LastUsed  time.Time // 最后使用时间
	Dirty     bool      // 是否脏页
	Reference int       // 引用计数
}

// PageCache 页缓存实现
type PageCache struct {
	config    PageCacheConfig
	cache     map[string]*list.Element
	evictList *list.List
	mu        sync.RWMutex
	stats     PageCacheStats
}

// PageCacheStats 页缓存统计信息
type PageCacheStats struct {
	Hits        int64 // 缓存命中次数
	Misses      int64 // 缓存未命中次数
	Evictions   int64 // 缓存驱逐次数
	DirtyWrites int64 // 脏页写入次数
}

// NewPageCache 创建新的页缓存
func NewPageCache(config PageCacheConfig) *PageCache {
	return &PageCache{
		config:    config,
		cache:     make(map[string]*list.Element),
		evictList: list.New(),
	}
}

// Get 从缓存获取页
func (pc *PageCache) Get(key string) ([]byte, bool) {
	if !pc.config.Enabled {
		return nil, false
	}

	pc.mu.RLock()
	elem, ok := pc.cache[key]
	pc.mu.RUnlock()

	if !ok {
		pc.stats.Misses++
		return nil, false
	}

	// 更新LRU位置
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// 再次检查，因为可能在获取写锁期间被其他goroutine修改
	elem, ok = pc.cache[key]
	if !ok {
		pc.stats.Misses++
		return nil, false
	}

	pc.evictList.MoveToFront(elem)
	page := elem.Value.(*CachedPage)
	page.LastUsed = time.Now()
	page.Reference++

	pc.stats.Hits++
	return page.Data, true
}

// Put 将页放入缓存
func (pc *PageCache) Put(key string, data []byte, dirty bool) error {
	if !pc.config.Enabled {
		return nil
	}

	if len(data) > pc.config.PageSize {
		return fmt.Errorf("数据大小超过页大小限制: %d > %d", len(data), pc.config.PageSize)
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	// 检查是否已存在
	if elem, ok := pc.cache[key]; ok {
		pc.evictList.MoveToFront(elem)
		page := elem.Value.(*CachedPage)
		page.Data = data
		page.LastUsed = time.Now()
		page.Dirty = dirty
		return nil
	}

	// 检查是否需要驱逐
	if pc.evictList.Len() >= pc.config.MaxPages {
		if err := pc.evictPage(); err != nil {
			return err
		}
	}

	// 创建新页
	page := &CachedPage{
		Key:      key,
		Data:     data,
		LastUsed: time.Now(),
		Dirty:    dirty,
	}

	// 添加到缓存
	elem := pc.evictList.PushFront(page)
	pc.cache[key] = elem

	return nil
}

// evictPage 驱逐一个页
func (pc *PageCache) evictPage() error {
	// 从尾部开始查找可驱逐的页
	for e := pc.evictList.Back(); e != nil; e = e.Prev() {
		page := e.Value.(*CachedPage)

		// 跳过引用中的页
		if page.Reference > 0 {
			continue
		}

		// 跳过最近使用的页
		if time.Since(page.LastUsed) < pc.config.EvictionDelay {
			continue
		}

		// 处理脏页
		if page.Dirty {
			// 在实际实现中，这里应该将脏页写回磁盘
			// 为简化实现，这里只增加统计计数
			pc.stats.DirtyWrites++
		}

		// 从缓存中移除
		delete(pc.cache, page.Key)
		pc.evictList.Remove(e)
		pc.stats.Evictions++

		return nil
	}

	// 如果没有可驱逐的页，强制驱逐最老的页
	if elem := pc.evictList.Back(); elem != nil {
		page := elem.Value.(*CachedPage)

		// 处理脏页
		if page.Dirty {
			pc.stats.DirtyWrites++
		}

		// 从缓存中移除
		delete(pc.cache, page.Key)
		pc.evictList.Remove(elem)
		pc.stats.Evictions++

		return nil
	}

	return fmt.Errorf("无法驱逐页：缓存为空")
}

// Release 释放页引用
func (pc *PageCache) Release(key string) {
	if !pc.config.Enabled {
		return
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if elem, ok := pc.cache[key]; ok {
		page := elem.Value.(*CachedPage)
		if page.Reference > 0 {
			page.Reference--
		}
	}
}

// MarkDirty 标记页为脏页
func (pc *PageCache) MarkDirty(key string) {
	if !pc.config.Enabled {
		return
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if elem, ok := pc.cache[key]; ok {
		page := elem.Value.(*CachedPage)
		page.Dirty = true
	}
}

// FlushDirtyPages 刷新所有脏页
func (pc *PageCache) FlushDirtyPages() error {
	if !pc.config.Enabled {
		return nil
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	// 遍历所有页，处理脏页
	for e := pc.evictList.Front(); e != nil; e = e.Next() {
		page := e.Value.(*CachedPage)
		if page.Dirty {
			// 在实际实现中，这里应该将脏页写回磁盘
			// 为简化实现，这里只增加统计计数
			pc.stats.DirtyWrites++
			page.Dirty = false
		}
	}

	return nil
}

// GetStats 获取缓存统计信息
func (pc *PageCache) GetStats() PageCacheStats {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	return pc.stats
}

// Clear 清空缓存
func (pc *PageCache) Clear() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// 处理所有脏页
	for e := pc.evictList.Front(); e != nil; e = e.Next() {
		page := e.Value.(*CachedPage)
		if page.Dirty {
			pc.stats.DirtyWrites++
		}
	}

	// 清空缓存
	pc.cache = make(map[string]*list.Element)
	pc.evictList.Init()

	return nil
}

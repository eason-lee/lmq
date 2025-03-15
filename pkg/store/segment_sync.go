package store

import (
	"fmt"
)

// Sync 同步段数据到磁盘
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 如果启用了页缓存，先刷新脏页
	if s.pageCache != nil && s.pageCache.config.Enabled {
		if err := s.pageCache.FlushDirtyPages(); err != nil {
			return fmt.Errorf("刷新页缓存失败: %w", err)
		}
	}

	// 同步稀疏索引
	if s.sparseIndex != nil {
		if err := s.sparseIndex.Sync(); err != nil {
			return fmt.Errorf("同步稀疏索引失败: %w", err)
		}
	}

	// 使用异步I/O同步文件
	if s.asyncIO != nil && s.asyncIO.config.Enabled {
		// 创建同步通道
		dataCh := make(chan error, 1)
		indexCh := make(chan error, 1)

		// 异步同步数据文件
		s.asyncIO.AsyncSync(s.dataFile.Name(), func(_ []byte, err error) {
			dataCh <- err
		})

		// 异步同步索引文件
		s.asyncIO.AsyncSync(s.indexFile.Name(), func(_ []byte, err error) {
			indexCh <- err
		})

		// 等待同步完成
		if err := <-dataCh; err != nil {
			return fmt.Errorf("同步数据文件失败: %w", err)
		}

		if err := <-indexCh; err != nil {
			return fmt.Errorf("同步索引文件失败: %w", err)
		}
	} else {
		// 同步数据文件
		if err := s.dataFile.Sync(); err != nil {
			return fmt.Errorf("同步数据文件失败: %w", err)
		}

		// 同步索引文件
		if err := s.indexFile.Sync(); err != nil {
			return fmt.Errorf("同步索引文件失败: %w", err)
		}
	}

	return nil
}

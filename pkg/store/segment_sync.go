package store

import (
	"fmt"
)

// Sync 同步段数据到磁盘
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 同步稀疏索引
	if s.sparseIndex != nil {
		if err := s.sparseIndex.Sync(); err != nil {
			return fmt.Errorf("同步稀疏索引失败: %w", err)
		}
	}

	// 同步数据文件
	if err := s.dataFile.Sync(); err != nil {
		return fmt.Errorf("同步数据文件失败: %w", err)
	}

	// 同步索引文件
	if err := s.indexFile.Sync(); err != nil {
		return fmt.Errorf("同步索引文件失败: %w", err)
	}

	return nil
}

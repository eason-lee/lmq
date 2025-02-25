package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/eason-lee/lmq/pkg/protocol"
)

// FileStore 实现基于文件的消息存储
type FileStore struct {
	baseDir string
	mu      sync.RWMutex
}

// NewFileStore 创建一个新的文件存储
func NewFileStore(baseDir string) (*FileStore, error) {
	// 确保目录存在
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("创建存储目录失败: %w", err)
	}

	return &FileStore{
		baseDir: baseDir,
	}, nil
}

// Save 保存消息到文件
func (fs *FileStore) Save(topic string, msg *protocol.Message) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// 确保主题目录存在
	topicDir := filepath.Join(fs.baseDir, topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return fmt.Errorf("创建主题目录失败: %w", err)
	}

	// 将消息序列化为JSON
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}

	// 创建文件名
	fileName := filepath.Join(topicDir, msg.ID+".json")

	// 写入文件
	if err := os.WriteFile(fileName, data, 0644); err != nil {
		return fmt.Errorf("写入消息文件失败: %w", err)
	}

	return nil
}

// GetMessages 获取指定主题的所有消息
func (fs *FileStore) GetMessages(topic string) ([]*protocol.Message, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	topicDir := filepath.Join(fs.baseDir, topic)

	// 检查目录是否存在
	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		return []*protocol.Message{}, nil
	}

	// 读取目录中的所有文件
	files, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, fmt.Errorf("读取主题目录失败: %w", err)
	}

	var messages []*protocol.Message
	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
			continue
		}

		// 读取文件内容
		data, err := os.ReadFile(filepath.Join(topicDir, file.Name()))
		if err != nil {
			return nil, fmt.Errorf("读取消息文件失败: %w", err)
		}

		// 反序列化消息
		var msg protocol.Message
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, fmt.Errorf("反序列化消息失败: %w", err)
		}

		messages = append(messages, &msg)
	}

	return messages, nil
}

// DeleteMessage 删除指定消息
func (fs *FileStore) DeleteMessage(topic string, messageID string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fileName := filepath.Join(fs.baseDir, topic, messageID+".json")

	// 检查文件是否存在
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil // 文件不存在，视为已删除
	}

	// 删除文件
	if err := os.Remove(fileName); err != nil {
		return fmt.Errorf("删除消息文件失败: %w", err)
	}

	return nil
}

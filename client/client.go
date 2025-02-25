package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/eason-lee/lmq/pkg/protocol"
)

// Client LMQ客户端
type Client struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	addr     string
	mu       sync.Mutex
	handlers map[string]MessageHandler
}

// MessageHandler 消息处理函数
type MessageHandler func(*protocol.Message)

// NewClient 创建一个新的客户端
func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("连接服务器失败: %w", err)
	}

	client := &Client{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
		addr:     addr,
		handlers: make(map[string]MessageHandler),
	}

	// 启动响应处理
	go client.handleResponses()

	return client, nil
}

// Close 关闭客户端连接
func (c *Client) Close() error {
	return c.conn.Close()
}

// Publish 发布消息
func (c *Client) Publish(topic string, data []byte) (*protocol.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 创建消息
	msg := protocol.NewMessage(topic, data)

	// 创建命令
	cmd := &protocol.Command{
		Type:    protocol.CmdPublish,
		Topic:   topic,
		Message: msg,
	}

	// 发送命令
	if err := c.sendCommand(cmd); err != nil {
		return nil, err
	}

	// 读取响应
	resp, err := c.readResponse()
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("发布失败: %s", resp.Message)
	}

	// 解析返回的消息
	var returnedMsg protocol.Message
	if err := json.Unmarshal(resp.Data, &returnedMsg); err != nil {
		return nil, fmt.Errorf("解析响应消息失败: %w", err)
	}

	return &returnedMsg, nil
}

// Subscribe 订阅主题
func (c *Client) Subscribe(topics []string, handler MessageHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 注册处理函数
	for _, topic := range topics {
		c.handlers[topic] = handler
	}

	// 创建命令
	cmd := &protocol.Command{
		Type:   protocol.CmdSubscribe,
		Topics: topics,
	}

	// 发送命令
	if err := c.sendCommand(cmd); err != nil {
		return err
	}

	// 读取响应
	resp, err := c.readResponse()
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("订阅失败: %s", resp.Message)
	}

	return nil
}

// Fetch 获取指定主题的消息
func (c *Client) Fetch(topic string) ([]*protocol.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 创建命令
	cmd := &protocol.Command{
		Type:  protocol.CmdFetch,
		Topic: topic,
	}

	// 发送命令
	if err := c.sendCommand(cmd); err != nil {
		return nil, err
	}

	// 读取响应
	resp, err := c.readResponse()
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("获取消息失败: %s", resp.Message)
	}

	return resp.Messages, nil
}

// 发送命令
func (c *Client) sendCommand(cmd *protocol.Command) error {
	cmdBytes, err := protocol.EncodeCommand(cmd)
	if err != nil {
		return fmt.Errorf("编码命令失败: %w", err)
	}

	if _, err := c.writer.Write(append(cmdBytes, '\n')); err != nil {
		return fmt.Errorf("发送命令失败: %w", err)
	}

	return c.writer.Flush()
}

// 读取响应
func (c *Client) readResponse() (*protocol.Response, error) {
	respBytes, err := c.reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	resp, err := protocol.DecodeResponse(respBytes)
	if err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	return resp, nil
}

// 添加确认消息的方法
func (c *Client) AckMessage(messageID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 创建命令
	cmd := &protocol.Command{
		Type:      protocol.CmdAck,
		MessageID: messageID,
	}

	// 发送命令
	if err := c.sendCommand(cmd); err != nil {
		return err
	}

	// 读取响应
	resp, err := c.readResponse()
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("确认消息失败: %s", resp.Message)
	}

	return nil
}

// 修改 handleResponses 方法，增加自动确认
func (c *Client) handleResponses() {
	for {
		respBytes, err := c.reader.ReadBytes('\n')
		if err != nil {
			// 连接已关闭或出错
			break
		}

		resp, err := protocol.DecodeResponse(respBytes)
		if err != nil {
			// 解析响应失败
			continue
		}

		// 处理消息
		if resp.Success && len(resp.Messages) > 0 {
			for _, msg := range resp.Messages {
				if handler, ok := c.handlers[msg.Topic]; ok {
					go func(m *protocol.Message) {
						handler(m)
						// 自动确认消息
						if err := c.AckMessage(m.ID); err != nil {
							fmt.Printf("确认消息 %s 失败: %v\n", m.ID, err)
						}
					}(msg)
				} else {
					fmt.Printf("没有找到主题 %s 的处理函数\n", msg.Topic)
				}
			}
		}
	}
}

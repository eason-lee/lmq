package protocol

import (
	"encoding/json"
)

// CommandType 表示命令类型
type CommandType string

const (
	CmdPublish   CommandType = "PUBLISH"   // 发布消息
	CmdSubscribe CommandType = "SUBSCRIBE" // 订阅主题
	CmdFetch     CommandType = "FETCH"     // 获取消息
	CmdAck       CommandType = "ACK"       // 确认消息
)

// Command 表示客户端发送的命令
type Command struct {
	Type    CommandType     `json:"type"`              // 命令类型
	Topic   string          `json:"topic,omitempty"`   // 主题
	Message *Message        `json:"message,omitempty"` // 消息内容
	Topics  []string        `json:"topics,omitempty"`  // 订阅的主题列表
	Data    json.RawMessage `json:"data,omitempty"`    // 其他数据
}

// Response 表示服务器的响应
type Response struct {
	Success  bool            `json:"success"`           // 是否成功
	Message  string          `json:"message,omitempty"` // 响应消息
	Data     json.RawMessage `json:"data,omitempty"`    // 响应数据
	Messages []*Message      `json:"messages,omitempty"` // 消息列表
}

// EncodeCommand 将命令编码为JSON
func EncodeCommand(cmd *Command) ([]byte, error) {
	return json.Marshal(cmd)
}

// DecodeCommand 将JSON解码为命令
func DecodeCommand(data []byte) (*Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return &cmd, err
}

// EncodeResponse 将响应编码为JSON
func EncodeResponse(resp *Response) ([]byte, error) {
	return json.Marshal(resp)
}

// DecodeResponse 将JSON解码为响应
func DecodeResponse(data []byte) (*Response, error) {
	var resp Response
	err := json.Unmarshal(data, &resp)
	return &resp, err
} 
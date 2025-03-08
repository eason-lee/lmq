package protocol

import (
	"time"

	pb "github.com/eason-lee/lmq/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Message 表示一个消息，包装proto.Message
type Message struct {
	*pb.Message
}

// NewMessage 创建一个新的消息
func NewMessage(topic string, body []byte) *Message {
	now := timestamppb.Now()
	msg := &pb.Message{
		Id:         uuid.New().String(),
		Topic:      topic,
		Body:       body,
		Timestamp:  now,
		Type:       pb.MessageType_NORMAL,
		Version:    1,
		Attributes: make(map[string]*anypb.Any),
	}
	return &Message{Message: msg}
}

// NewDelayedMessage 创建一个延迟消息
func NewDelayedMessage(topic string, body []byte, delaySeconds int32) *Message {
	msg := NewMessage(topic, body)
	msg.Type = pb.MessageType_DELAYED
	// 将延迟时间添加到属性中
	if delayValue, err := anypb.New(&pb.PublishRequest{DelaySeconds: delaySeconds}); err == nil {
		msg.Attributes["delay"] = delayValue
	}
	return msg
}

// IsExpired 检查消息是否过期
func (m *Message) IsExpired() bool {
	if m.Timestamp == nil {
		return false
	}
	// 默认消息有效期为7天
	return time.Since(m.Timestamp.AsTime()) > 7*24*time.Hour
}

// ShouldDeliver 检查延迟消息是否应该投递
func (m *Message) ShouldDeliver() bool {
	if m.Type != pb.MessageType_DELAYED {
		return true
	}

	// 获取延迟时间
	delayAttr, exists := m.Attributes["delay"]
	if !exists {
		return true
	}

	var pubReq pb.PublishRequest
	if err := delayAttr.UnmarshalTo(&pubReq); err != nil {
		return true
	}

	deliveryTime := m.Timestamp.AsTime().Add(time.Duration(pubReq.DelaySeconds) * time.Second)
	return time.Now().After(deliveryTime)
}

// ToDeadLetter 将消息转换为死信消息
func (m *Message) ToDeadLetter(reason string) *Message {
	m.Type = pb.MessageType_DEAD_LETTER
	if reasonValue, err := anypb.New(&pb.Response{Message: reason}); err == nil {
		m.Attributes["dead_letter_reason"] = reasonValue
	}
	return m
}

// GetDeadLetterReason 获取死信原因
func (m *Message) GetDeadLetterReason() string {
	if m.Type != pb.MessageType_DEAD_LETTER {
		return ""
	}

	reasonAttr, exists := m.Attributes["dead_letter_reason"]
	if !exists {
		return ""
	}

	var resp pb.Response
	if err := reasonAttr.UnmarshalTo(&resp); err != nil {
		return ""
	}

	return resp.Message
}

// GetID 获取消息ID
func (m *Message) GetID() string {
	return m.Id
}

// GetTopic 获取消息主题
func (m *Message) GetTopic() string {
	return m.Topic
}

// GetBody 获取消息内容
func (m *Message) GetBody() []byte {
	return m.Body
}

// GetTimestamp 获取消息时间戳
func (m *Message) GetTimestamp() time.Time {
	if m.Timestamp == nil {
		return time.Time{}
	}
	return m.Timestamp.AsTime()
}

// GetType 获取消息类型
func (m *Message) GetType() pb.MessageType {
	return m.Type
}

// GetAttributes 获取消息属性
func (m *Message) GetAttributes() map[string]*anypb.Any {
	return m.Attributes
}

// SetAttribute 设置消息属性
func (m *Message) SetAttribute(key string, value *anypb.Any) {
	m.Attributes[key] = value
}

// GetVersion 获取消息版本
func (m *Message) GetVersion() int32 {
	return m.Version
}

// Clone 克隆消息
func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}
	return &Message{
		Message: proto.Clone(m.Message).(*pb.Message),
	}
}

type PublishResponse struct {
	Success   bool   `json:"success"`
	MessageID string `json:"message_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

// 添加以下结构体
type SubscribeRequest struct {
	GroupID string   `json:"group_id"`
	Topics  []string `json:"topics"`
}

type AckRequest struct {
	GroupID   string `json:"group_id"`
	MessageID string `json:"message_id"`
}

// 添加心跳相关结构体
type Heartbeat struct {
	NodeID    string `json:"node_id"`
	Timestamp int64  `json:"timestamp"`
}

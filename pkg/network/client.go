package network

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/eason-lee/lmq/pkg/protocol"
	pb "github.com/eason-lee/lmq/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Client struct {
	addr string
	conn net.Conn
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		addr: addr,
		conn: conn,
	}, nil
}

// buildPublishRequest 构建发布请求
func buildPublishRequest(topic string, body []byte, msgType pb.MessageType, attrs map[string]*anypb.Any) *pb.Request {
	return &pb.Request{
		Type: "publish",
		RequestData: &pb.Request_PublishData{
			PublishData: &pb.PublishRequest{
				Topic:      topic,
				Body:       body,
				Type:       msgType,
				Attributes: attrs,
			},
		},
	}
}

// buildSubscribeRequest 构建订阅请求
func buildSubscribeRequest(groupID string, topics []string) *pb.Request {
	return &pb.Request{
		Type: "subscribe",
		RequestData: &pb.Request_SubscribeData{
			SubscribeData: &pb.SubscribeRequest{
				GroupId: groupID,
				Topics:  topics,
			},
		},
	}
}

// buildPullRequest 构建拉取请求
func buildPullRequest(groupID string, topic string, maxMessages int32) *pb.Request {
	return &pb.Request{
		Type: "pull",
		RequestData: &pb.Request_PullData{
			PullData: &pb.PullRequest{
				GroupId:     groupID,
				Topic:       topic,
				MaxMessages: maxMessages,
			},
		},
	}
}

// buildAckRequest 构建确认请求
func buildAckRequest(groupID string, topic string, messageIDs []string) *pb.Request {
	return &pb.Request{
		Type: "ack",
		RequestData: &pb.Request_AckData{
			AckData: &pb.AckRequest{
				GroupId:    groupID,
				Topic:      topic,
				MessageIds: messageIDs,
			},
		},
	}
}

// Send 发送请求并等待响应
func (c *Client) Send(reqType string, reqMsg interface{}) (*pb.Response, error) {
	var pbReq *pb.Request

	switch reqType {
	case "publish":
		msg := reqMsg.(*pb.PublishRequest)
		pbReq = buildPublishRequest(msg.Topic, msg.Body, msg.Type, msg.Attributes)
	case "subscribe":
		msg := reqMsg.(*pb.SubscribeRequest)
		pbReq = buildSubscribeRequest(msg.GroupId, msg.Topics)
	case "pull":
		msg := reqMsg.(*pb.PullRequest)
		pbReq = buildPullRequest(msg.GroupId, msg.Topic, msg.MaxMessages)
	case "ack":
		msg := reqMsg.(*pb.AckRequest)
		pbReq = buildAckRequest(msg.GroupId, msg.Topic, msg.MessageIds)
	default:
		return nil, fmt.Errorf("未知的请求类型: %s", reqType)
	}

	// 序列化请求
	reqData, err := proto.Marshal(pbReq)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %w", err)
	}

	// 发送请求长度
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(reqData)))
	if _, err := c.conn.Write(lenBuf); err != nil {
		return nil, fmt.Errorf("发送请求长度失败: %w", err)
	}

	// 发送请求数据
	if _, err := c.conn.Write(reqData); err != nil {
		return nil, fmt.Errorf("发送请求数据失败: %w", err)
	}

	// 读取响应长度
	if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
		return nil, fmt.Errorf("读取响应长度失败: %w", err)
	}
	respLen := binary.BigEndian.Uint32(lenBuf)

	// 读取响应数据
	respData := make([]byte, respLen)
	if _, err := io.ReadFull(c.conn, respData); err != nil {
		return nil, fmt.Errorf("读取响应数据失败: %w", err)
	}

	// 反序列化响应
	var pbResp pb.Response
	if err := proto.Unmarshal(respData, &pbResp); err != nil {
		return nil, fmt.Errorf("反序列化响应失败: %w", err)
	}

	return &pbResp, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Receive 接收服务器推送的消息
func (c *Client) Receive() (*protocol.Message, error) {
	// 读取消息长度
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
		return nil, fmt.Errorf("读取消息长度失败: %w", err)
	}

	// 解析消息长度
	msgLen := binary.BigEndian.Uint32(lenBuf)

	// 读取消息内容
	msgBuf := make([]byte, msgLen)
	if _, err := io.ReadFull(c.conn, msgBuf); err != nil {
		return nil, fmt.Errorf("读取消息内容失败: %w", err)
	}

	// 反序列化消息
	var pbMsg pb.Message
	if err := proto.Unmarshal(msgBuf, &pbMsg); err != nil {
		return nil, fmt.Errorf("反序列化消息失败: %w", err)
	}

	// 转换为内部消息格式
	return &protocol.Message{
		Message: &pbMsg,
	}, nil
}

package network

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/eason-lee/lmq/pkg/protocol"
	pb "github.com/eason-lee/lmq/proto"
	"google.golang.org/protobuf/proto"
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

func (c *Client) Send(reqType string, payload interface{}) (*pb.Response, error) {
	// 使用 protobuf 序列化请求
	pbReq := &pb.Request{
		Type: reqType,
	}

	// 根据不同的请求类型，设置不同的 payload
	if err := setRequestPayload(pbReq, reqType, payload); err != nil {
		return nil, fmt.Errorf("设置请求负载失败: %w", err)
	}

	// 序列化请求
	reqData, err := proto.Marshal(pbReq)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %w", err)
	}

	// 发送长度前缀
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
	respLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, respLenBuf); err != nil {
		return nil, fmt.Errorf("读取响应长度失败: %w", err)
	}

	// 解析响应长度
	respLen := binary.BigEndian.Uint32(respLenBuf)

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

// setRequestPayload 根据请求类型设置不同的负载
func setRequestPayload(req *pb.Request, reqType string, payload interface{}) error {
	switch reqType {
	case "publish":
		if msg, ok := payload.(*protocol.Message); ok {
			req.PublishData = &pb.PublishRequest{
				Topic:      msg.Message.Topic,
				Body:       msg.Message.Body,
				Type:       msg.Message.Type,
				Attributes: msg.Message.Attributes,
			}
		}
	case "subscribe":
		if msg, ok := payload.(*protocol.Message); ok {
			groupID := ""
			topics := ""
			if msg.Message.Attributes != nil {
				if attr, ok := msg.Message.Attributes["group_id"]; ok {
					var resp pb.Response
					if err := attr.UnmarshalTo(&resp); err == nil {
						groupID = resp.Message
					}
				}
				if attr, ok := msg.Message.Attributes["topics"]; ok {
					var resp pb.Response
					if err := attr.UnmarshalTo(&resp); err == nil {
						topics = resp.Message
					}
				}
			}
			req.SubscribeData = &pb.SubscribeRequest{
				GroupId: groupID,
				Topics:  []string{topics},
			}
		}
	case "ack":
		if msg, ok := payload.(*protocol.Message); ok {
			groupId := ""
			messageIDs := ""
			if msg.Message.Attributes != nil {
				if attr, ok := msg.Message.Attributes["group_id"]; ok {
					var resp pb.Response
					if err := attr.UnmarshalTo(&resp); err == nil {
						groupId = resp.Message
					}
				}
				if attr, ok := msg.Message.Attributes["message_ids"]; ok {
					var resp pb.Response
					if err := attr.UnmarshalTo(&resp); err == nil {
						messageIDs = resp.Message
					}
				}
			}
			req.AckData = &pb.AckRequest{
				GroupId:    groupId,
				MessageIds: []string{messageIDs},
			}
		}
	default:
		return fmt.Errorf("不支持的请求类型: %s", reqType)
	}
	return nil
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

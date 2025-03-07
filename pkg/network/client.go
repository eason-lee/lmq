package network

import (
    "encoding/binary"
    "fmt"
    "io"
    "net"
    
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

func (c *Client) Send(reqType string, payload interface{}) (*Response, error) {
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
    
    // 转换为内部响应格式
    resp := &Response{
        Success: pbResp.Success,
        Error:   pbResp.Error,
    }
    
    // 根据不同的响应类型，解析不同的数据
    if pbResp.Success && pbResp.Data != nil {
        resp.Data = pbResp.Data
    }
    
    return resp, nil
}

// setRequestPayload 根据请求类型设置不同的负载
func setRequestPayload(req *pb.Request, reqType string, payload interface{}) error {
    switch reqType {
    case "node_join", "node_leave":
        if data, ok := payload.(map[string]interface{}); ok {
            req.Data = make(map[string]string)
            for k, v := range data {
                req.Data[k] = fmt.Sprintf("%v", v)
            }
        }
    case "publish":
        if pubData, ok := payload.(map[string]interface{}); ok {
            topic, _ := pubData["topic"].(string)
            body, _ := pubData["body"].([]byte)
            
            req.PublishData = &pb.PublishRequest{
                Topic: topic,
                Body:  body,
            }
        }
    case "subscribe":
        if subData, ok := payload.(map[string]interface{}); ok {
            groupID, _ := subData["group_id"].(string)
            topics, _ := subData["topics"].([]string)
            
            req.SubscribeData = &pb.SubscribeRequest{
                GroupId: groupID,
                Topics:  topics,
            }
        }
    case "ack":
        if ackData, ok := payload.(map[string]interface{}); ok {
            groupID, _ := ackData["group_id"].(string)
            messageIDs, _ := ackData["message_ids"].([]string)
            
            req.AckData = &pb.AckRequest{
                GroupId:    groupID,
                MessageIds: messageIDs,
            }
        }
    default:
        // 对于未知类型，尝试通用处理
        if data, ok := payload.(map[string]interface{}); ok {
            req.Data = make(map[string]string)
            for k, v := range data {
                req.Data[k] = fmt.Sprintf("%v", v)
            }
        }
    }
    
    return nil
}

func (c *Client) Close() error {
    return c.conn.Close()
}
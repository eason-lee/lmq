package network

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	pb "github.com/eason-lee/lmq/proto"
	"google.golang.org/protobuf/proto"
)

// BrokerInterface 定义了 broker 需要实现的接口
type BrokerInterface interface {
	HandlePublish(ctx context.Context, req *pb.PublishRequest) error
	HandleSubscribe(ctx context.Context, req *pb.SubscribeRequest) error
	HandlePull(ctx context.Context, req *pb.PullRequest) (*pb.Response, error)
	HandleAck(ctx context.Context, req *pb.AckRequest) error
}

// Server 网络服务器
type Server struct {
	addr     string
	listener net.Listener
	handlers map[string]ProtoHandlerFunc
	mu       sync.RWMutex
	broker   BrokerInterface
}

// 使用 protobuf 请求和响应的处理函数
type ProtoHandlerFunc func(req *pb.Request) *pb.Response

// NewServer 创建新的服务器实例
func NewServer(addr string, b BrokerInterface) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("创建监听器失败: %w", err)
	}

	return &Server{
		addr:     addr,
		listener: listener,
		handlers: make(map[string]ProtoHandlerFunc),
		broker:   b,
	}, nil
}

func (s *Server) Start() error {
	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Printf("接受连接失败: %v", err)
			continue
		}
		go s.handleRequest(conn)
	}
}

// handleRequest 处理客户端请求
func (s *Server) handleRequest(conn net.Conn) {
	defer conn.Close()

	for {
		// 读取请求长度
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err != io.EOF {
				log.Printf("读取请求长度失败: %v", err)
			}
			return
		}
		reqLen := binary.BigEndian.Uint32(lenBuf)

		// 读取请求数据
		reqData := make([]byte, reqLen)
		if _, err := io.ReadFull(conn, reqData); err != nil {
			log.Printf("读取请求数据失败: %v", err)
			return
		}

		// 反序列化请求
		var pbReq pb.Request
		if err := proto.Unmarshal(reqData, &pbReq); err != nil {
			log.Printf("反序列化请求失败: %v", err)
			return
		}

        // 处理请求
        var pbResp *pb.Response
        var err error

        switch pbReq.Type {
        case "publish":
            if pubData := pbReq.GetPublishData(); pubData != nil {
                pbResp, err = s.handlePublish(pubData)
            } else {
                err = fmt.Errorf("发布请求数据为空")
            }
        case "subscribe":
            if subData := pbReq.GetSubscribeData(); subData != nil {
                pbResp, err = s.handleSubscribe(subData)
            } else {
                err = fmt.Errorf("订阅请求数据为空")
            }
        case "pull":
            if pullData := pbReq.GetPullData(); pullData != nil {
                pbResp, err = s.handlePull(pullData)
            } else {
                err = fmt.Errorf("拉取请求数据为空")
            }
        case "ack":
            if ackData := pbReq.GetAckData(); ackData != nil {
                pbResp, err = s.handleAck(ackData)
            } else {
                err = fmt.Errorf("确认请求数据为空")
            }
        case "ping":
            pbResp = &pb.Response{Status: pb.Status_OK, Message: "pong"}
        default:
            // 尝试使用注册的处理器
            s.mu.RLock()
            handler := s.handlers[pbReq.Type]
            s.mu.RUnlock()
            if handler != nil {
                pbResp = handler(&pbReq)
            } else {
                err = fmt.Errorf("未知的请求类型: %s", pbReq.Type)
            }
        }

		// 如果处理出错，构造错误响应
		if err != nil {
			pbResp = &pb.Response{
				Status:  pb.Status_ERROR,
				Message: err.Error(),
			}
		}

		// 序列化响应
		respData, err := proto.Marshal(pbResp)
		if err != nil {
			log.Printf("序列化响应失败: %v", err)
			return
		}

		// 发送响应长度
		binary.BigEndian.PutUint32(lenBuf, uint32(len(respData)))
		if _, err := conn.Write(lenBuf); err != nil {
			log.Printf("发送响应长度失败: %v", err)
			return
		}

		// 发送响应数据
		if _, err := conn.Write(respData); err != nil {
			log.Printf("发送响应数据失败: %v", err)
			return
		}
	}
}

// handlePublish 处理发布请求
func (s *Server) handlePublish(req *pb.PublishRequest) (*pb.Response, error) {
	// 调用 broker 处理发布请求
	err := s.broker.HandlePublish(context.Background(), req)
	if err != nil {
		return nil, err
	}

	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "消息发布成功",
	}, nil
}

// handleSubscribe 处理订阅请求
func (s *Server) handleSubscribe(req *pb.SubscribeRequest) (*pb.Response, error) {
	// 调用 broker 处理订阅请求
	err := s.broker.HandleSubscribe(context.Background(), req)
	if err != nil {
		return nil, err
	}

	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "订阅成功",
	}, nil
}

// handlePull 处理拉取请求
func (s *Server) handlePull(req *pb.PullRequest) (*pb.Response, error) {
	// 调用 broker 处理拉取请求
	resp, err := s.broker.HandlePull(context.Background(), req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// handleAck 处理确认请求
func (s *Server) handleAck(req *pb.AckRequest) (*pb.Response, error) {
	// 调用 broker 处理确认请求
	err := s.broker.HandleAck(context.Background(), req)
	if err != nil {
		return nil, err
	}

	return &pb.Response{
		Status:  pb.Status_OK,
		Message: "消息确认成功",
	}, nil
}

// 注册处理器
func (s *Server) RegisterHandler(reqType string, handler ProtoHandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[reqType] = handler
}

// 关闭服务器
func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

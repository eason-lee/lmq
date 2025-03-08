package network

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"sync"

	pb "github.com/eason-lee/lmq/proto"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	addr     string
	listener net.Listener
	handlers map[string]ProtoHandlerFunc
	mu       sync.RWMutex
}

// 使用 protobuf 请求和响应的处理函数
type ProtoHandlerFunc func(req *pb.Request) *pb.Response

func NewServer(addr string) *Server {
	return &Server{
		addr:     addr,
		handlers: make(map[string]ProtoHandlerFunc),
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = listener

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
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// 读取消息长度
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err != io.EOF {
				log.Printf("读取消息长度失败: %v", err)
			}
			break
		}
		
		// 解析消息长度
		msgLen := binary.BigEndian.Uint32(lenBuf)
		
		// 读取消息内容
		msgBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			log.Printf("读取消息内容失败: %v", err)
			break
		}
		
		// 反序列化请求
		var pbReq pb.Request
		if err := proto.Unmarshal(msgBuf, &pbReq); err != nil {
			log.Printf("反序列化请求失败: %v", err)
			sendErrorResponse(conn, "无效的请求格式")
			continue
		}
		
		// 处理请求
		s.mu.RLock()
		handler, exists := s.handlers[pbReq.Type]
		s.mu.RUnlock()
		
		var pbResp *pb.Response
		if !exists {
			pbResp = &pb.Response{Status: pb.Status_ERROR, Message: "未知的请求类型"}
		} else {
			pbResp = handler(&pbReq)
		}
		
		// 序列化响应
		respData, err := proto.Marshal(pbResp)
		if err != nil {
			log.Printf("序列化响应失败: %v", err)
			continue
		}
		
		// 发送响应长度
		respLenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(respLenBuf, uint32(len(respData)))
		if _, err := conn.Write(respLenBuf); err != nil {
			log.Printf("发送响应长度失败: %v", err)
			break
		}
		
		// 发送响应内容
		if _, err := conn.Write(respData); err != nil {
			log.Printf("发送响应内容失败: %v", err)
			break
		}
	}
}
// 发送错误响应
func sendErrorResponse(conn net.Conn, errMsg string) {
	resp := &pb.Response{
		Status:  pb.Status_ERROR,
		Message: errMsg,
	}
	
	respData, err := proto.Marshal(resp)
	if err != nil {
		log.Printf("序列化错误响应失败: %v", err)
		return
	}
	
	// 发送响应长度
	respLenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(respLenBuf, uint32(len(respData)))
	if _, err := conn.Write(respLenBuf); err != nil {
		log.Printf("发送错误响应长度失败: %v", err)
		return
	}
	
	// 发送响应内容
	if _, err := conn.Write(respData); err != nil {
		log.Printf("发送错误响应内容失败: %v", err)
	}
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

package network

import (
	"encoding/json"
	"net"
	"sync"
)

type Server struct {
	addr     string
	listener net.Listener
	handlers map[string]HandlerFunc
	mu       sync.RWMutex
}

type HandlerFunc func(req *Request) *Response

type Request struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Response struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func NewServer(addr string) *Server {
	return &Server{
		addr:     addr,
		handlers: make(map[string]HandlerFunc),
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
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var req Request
		if err := decoder.Decode(&req); err != nil {
			break
		}

		s.mu.RLock()
		handler, exists := s.handlers[req.Type]
		s.mu.RUnlock()

		var resp *Response
		if !exists {
			resp = &Response{Success: false, Error: "未知的请求类型"}
		} else {
			resp = handler(&req)
		}

		if err := encoder.Encode(resp); err != nil {
			break
		}
	}
}

func (s *Server) RegisterHandler(reqType string, handler HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[reqType] = handler
}

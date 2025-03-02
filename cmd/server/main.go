package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/eason-lee/lmq/pkg/broker"
)

var (
	nodeID   = flag.String("node", "broker-1", "broker节点ID")
	port     = flag.String("port", "9000", "服务器监听端口")
	storeDir = flag.String("store", "./data", "消息存储目录")
)

func main() {
	flag.Parse()

	// 创建 broker
	addr := fmt.Sprintf(":%s", *port)
	b, err := broker.NewBroker(*nodeID, *storeDir, addr)
	if err != nil {
		log.Fatalf("创建broker失败: %v", err)
	}

	// 启动 TCP 服务器
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("启动服务器失败: %v", err)
	}
	defer listener.Close()

	log.Printf("LMQ broker已启动，节点ID: %s, 监听地址: %s", *nodeID, addr)

	// 处理优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 接受连接
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("接受连接失败: %v", err)
				continue
			}
			go b.HandleConnection(conn)
		}
	}()

	// 等待退出信号
	<-sigCh
	log.Println("收到退出信号，broker正在关闭...")
}

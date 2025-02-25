package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/eason-lee/lmq/pkg/broker"
	"github.com/eason-lee/lmq/pkg/protocol"
)

var (
	port     = flag.String("port", "9000", "服务器监听端口")
	storeDir = flag.String("store", "./data", "消息存储目录")
)

func main() {
	flag.Parse()

	// 创建消息代理
	b, err := broker.NewBroker(*storeDir)
	if err != nil {
		log.Fatalf("创建消息代理失败: %v", err)
	}

	// 启动TCP服务器
	addr := fmt.Sprintf(":%s", *port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("启动服务器失败: %v", err)
	}
	defer listener.Close()

	log.Printf("LMQ 服务器已启动，监听地址: %s", addr)

	// 处理优雅退出
	go handleSignals()

	// 接受连接
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("接受连接失败: %v", err)
			continue
		}

		// 为每个连接创建一个处理协程
		go handleConnection(conn, b)
	}
}

// 处理客户端连接
func handleConnection(conn net.Conn, b *broker.Broker) {
	defer conn.Close()
	log.Printf("客户端已连接: %s", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// 客户端ID，用于订阅
	clientID := fmt.Sprintf("client-%s", conn.RemoteAddr())
	var subscriber *broker.Subscriber

	for {
		// 读取命令
		cmdBytes, err := reader.ReadBytes('\n')
		if err != nil {
			log.Printf("读取命令失败: %v", err)
			break
		}

		// 解析命令
		cmd, err := protocol.DecodeCommand(cmdBytes)
		if err != nil {
			log.Printf("解析命令失败: %v", err)
			sendResponse(writer, &protocol.Response{
				Success: false,
				Message: fmt.Sprintf("解析命令失败: %v", err),
			})
			continue
		}

		// 处理命令
		switch cmd.Type {
		case protocol.CmdPublish:
			// 发布消息
			msg, err := b.Publish(cmd.Topic, cmd.Message.Body)
			if err != nil {
				sendResponse(writer, &protocol.Response{
					Success: false,
					Message: fmt.Sprintf("发布消息失败: %v", err),
				})
				continue
			}

			sendResponse(writer, &protocol.Response{
				Success: true,
				Message: "消息发布成功",
				Data:    mustMarshal(msg),
			})

		case protocol.CmdSubscribe:
			// 订阅主题
			if subscriber != nil {
				// 如果已经订阅，先取消之前的订阅
				b.Unsubscribe(clientID)
				close(subscriber.Ch)
			}

			subscriber, err = b.Subscribe(clientID, cmd.Topics, 100)
			if err != nil {
				sendResponse(writer, &protocol.Response{
					Success: false,
					Message: fmt.Sprintf("订阅失败: %v", err),
				})
				continue
			}

			sendResponse(writer, &protocol.Response{
				Success: true,
				Message: "订阅成功",
			})

			// 启动一个协程来处理订阅消息
			go func() {
				for msg := range subscriber.Ch {
					resp := &protocol.Response{
						Success:  true,
						Message:  "收到新消息",
						Messages: []*protocol.Message{msg},
					}
					respBytes, _ := protocol.EncodeResponse(resp)
					writer.Write(append(respBytes, '\n'))
					writer.Flush()
				}
			}()

		case protocol.CmdFetch:
			// 获取指定主题的消息
			messages, err := b.GetMessages(cmd.Topic)
			if err != nil {
				sendResponse(writer, &protocol.Response{
					Success: false,
					Message: fmt.Sprintf("获取消息失败: %v", err),
				})
				continue
			}

			sendResponse(writer, &protocol.Response{
				Success:  true,
				Message:  fmt.Sprintf("获取到 %d 条消息", len(messages)),
				Messages: messages,
			})

		default:
			sendResponse(writer, &protocol.Response{
				Success: false,
				Message: fmt.Sprintf("未知命令: %s", cmd.Type),
			})
		}
	}

	// 连接关闭，取消订阅
	if subscriber != nil {
		b.Unsubscribe(clientID)
		close(subscriber.Ch)
	}
	log.Printf("客户端断开连接: %s", conn.RemoteAddr())
}

// 发送响应
func sendResponse(writer *bufio.Writer, resp *protocol.Response) {
	respBytes, _ := protocol.EncodeResponse(resp)
	writer.Write(append(respBytes, '\n'))
	writer.Flush()
}

// 处理信号
func handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	log.Println("收到退出信号，服务器正在关闭...")
	os.Exit(0)
}

// 将对象转换为JSON
func mustMarshal(v interface{}) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}

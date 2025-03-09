package main

import (
	"context"
	"log"

	"github.com/eason-lee/lmq/pkg/broker"
)


func main() {
	ctx := context.Background()

	// 创建 broker
	b, err := broker.NewBroker(nil)
	if err != nil {
		log.Fatalf("创建broker失败: %v", err)
	}
	if err:=  b.Start(ctx); err != nil {
		log.Fatalf("启动broker失败: %v", err)
	}

}

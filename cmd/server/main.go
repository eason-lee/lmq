package main

import (
	"flag"
	"log"

	"github.com/eason-lee/lmq/pkg/broker"
)


func main() {
	flag.Parse()

	// 创建 broker
	b, err := broker.NewBroker("")
	if err != nil {
		log.Fatalf("创建broker失败: %v", err)
	}
	if err:=  b.Start(); err != nil {
		log.Fatalf("启动broker失败: %v", err)
	}

}

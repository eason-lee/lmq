package main

import (
    "context"
    "log"
    "os"
    "strconv"

    "github.com/eason-lee/lmq/pkg/broker"
)


func main() {
    ctx := context.Background()

    addr := getenv("LMQ_ADDR", "0.0.0.0:9000")
    consul := getenv("LMQ_CONSUL_ADDR", "127.0.0.1:8500")
    parts := getenvInt("LMQ_DEFAULT_PARTITIONS", 3)

    cfg := &broker.BrokerConfig{
        DefaultPartitions: parts,
        ConsulAddr:        consul,
        Addr:              addr,
    }

    b, err := broker.NewBroker(cfg)
    if err != nil {
        log.Fatalf("创建broker失败: %v", err)
    }
    if err:=  b.Start(ctx); err != nil {
        log.Fatalf("启动broker失败: %v", err)
    }

}

func getenv(key, def string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return def
}

func getenvInt(key string, def int) int {
    if v := os.Getenv(key); v != "" {
        if n, err := strconv.Atoi(v); err == nil {
            return n
        }
    }
    return def
}

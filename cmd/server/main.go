package main

import (
    "context"
    "flag"
    "log"
    "os"
    "strconv"

    "github.com/eason-lee/lmq/pkg/broker"
)


func main() {
    ctx := context.Background()

    envAddr := getenv("LMQ_ADDR", "0.0.0.0:9000")
    envConsul := getenv("LMQ_CONSUL_ADDR", "127.0.0.1:8500")
    envParts := getenvInt("LMQ_DEFAULT_PARTITIONS", 3)

    flagAddr := flag.String("addr", envAddr, "broker listen address")
    flagConsul := flag.String("consul", envConsul, "consul address")
    flagParts := flag.Int("partitions", envParts, "default partitions")
    flag.Parse()

    cfg := &broker.BrokerConfig{
        DefaultPartitions: *flagParts,
        ConsulAddr:        *flagConsul,
        Addr:              *flagAddr,
    }

    log.Printf("Starting LMQ: addr=%s consul=%s default_partitions=%d", cfg.Addr, cfg.ConsulAddr, cfg.DefaultPartitions)

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

package integration

import (
    "context"
    "testing"
    "time"

    "github.com/eason-lee/lmq/pkg/broker"
    "github.com/eason-lee/lmq/pkg/coordinator"
    sdk "github.com/eason-lee/lmq/sdk"
    pb "github.com/eason-lee/lmq/proto"
)

func TestEndToEndPublishSubscribeAck(t *testing.T) {
    coord := coordinator.NewMemoryCoordinator()
    cfg := &broker.BrokerConfig{Addr: "127.0.0.1:9320", DefaultPartitions: 3, ConsulAddr: "", MetricsAddr: "127.0.0.1:9310"}
    b, err := broker.NewBrokerWithCoordinator(cfg, coord)
    if err != nil { t.Fatalf("new broker failed: %v", err) }
    ctx := context.Background()
    if err := b.StartNoBlock(ctx); err != nil { t.Fatalf("start broker failed: %v", err) }

    // Prepare consumer with auto-commit
    client, err := sdk.NewClient(&sdk.Config{Brokers: []string{cfg.Addr}, Timeout: time.Second * 5, GroupID: "g1", AutoCommit: true, CommitInterval: time.Second, MaxPullRecords: 100, PullInterval: time.Millisecond * 200})
    if err != nil { t.Fatalf("new client failed: %v", err) }

    recv := make(chan *pb.Message, 10)
    done := make(chan struct{})

    // Subscribe first to create topic and partitions
    if err := client.Subscribe([]string{"e2e-topic"}, func(msgs []*pb.Message) {
        for _, m := range msgs { recv <- m }
        if len(msgs) > 0 { close(done) }
    }); err != nil {
        t.Fatalf("subscribe failed: %v", err)
    }

    // Send one message
    if _, err := client.Send("e2e-topic", []byte("hello")); err != nil {
        t.Fatalf("send failed: %v", err)
    }

    select {
    case <-done:
        // ok
    case <-time.After(5 * time.Second):
        t.Fatalf("timeout waiting for message")
    }

    // wait a moment for auto-commit
    time.Sleep(300 * time.Millisecond)
    // Verify offset advanced on any partition (auto-commit)
    success := false
    for pid := 0; pid < 3; pid++ {
        off, err := coord.GetConsumerOffset(ctx, "g1", "e2e-topic", pid)
        if err == nil && off > 0 { success = true; break }
    }
    if !success { t.Fatalf("expected committed offset on some partition") }

    client.Close()
}

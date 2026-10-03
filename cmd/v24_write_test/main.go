package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "raftkv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	totalSent    atomic.Int64
	totalSuccess atomic.Int64
	totalFail    atomic.Int64
)

type latTracker struct {
	mu   sync.Mutex
	data []time.Duration
}

func (lt *latTracker) add(d time.Duration) {
	lt.mu.Lock()
	if len(lt.data) < 100000 {
		lt.data = append(lt.data, d)
	}
	lt.mu.Unlock()
}

func (lt *latTracker) p99() time.Duration {
	lt.mu.Lock()
	if len(lt.data) == 0 {
		lt.mu.Unlock()
		return 0
	}
	sorted := make([]time.Duration, len(lt.data))
	copy(sorted, lt.data)
	lt.mu.Unlock()
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)) * 0.99)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func main() {
	target := flag.String("target", "127.0.0.1:9801", "gRPC目标地址")
	total := flag.Int("total", 1000, "总写入请求数")
	concurrency := flag.Int("concurrency", 10, "并发goroutine数")
	leaderID := flag.String("leader", "node-2", "Leader节点ID")
	term := flag.Int("term", 1, "当前term")
	flag.Parse()

	fmt.Printf("=== V2.4 gRPC写入测试 (target=%s, total=%d, concurrency=%d, leader=%s, term=%d) ===\n", *target, *total, *concurrency, *leaderID, *term)
	fmt.Printf("开始: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "gRPC连接失败: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewRaftServiceClient(conn)

	lat := &latTracker{}
	var wg sync.WaitGroup
	var seq atomic.Int64
	t0 := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				n := seq.Add(1)
				if n > int64(*total) {
					return
				}
				entry := &pb.LogEntry{
					Term:    int64(*term),
					Index:   n,
					Command: []byte(fmt.Sprintf(`{"src":"v24-write-%d","idx":%d,"data":"consistency-test-%d"}`, id, n, n)),
				}
				req := &pb.AppendEntriesRequest{
					Term:         int64(*term),
					LeaderId:     *leaderID,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []*pb.LogEntry{entry},
					LeaderCommit: n,
				}
				st0 := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				resp, err := client.AppendEntries(ctx, req)
				cancel()
				lat.add(time.Since(st0))
				totalSent.Add(1)
				if err != nil || resp == nil || !resp.Success {
					totalFail.Add(1)
					if totalFail.Load() <= 3 {
						fmt.Printf("[FAIL] #%d err=%v resp=%v\n", n, err, resp)
					}
				} else {
					totalSuccess.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	dur := time.Since(t0)

	fmt.Printf("\n=== 写入结果 ===\n")
	fmt.Printf("总发送: %d\n", totalSent.Load())
	fmt.Printf("成功: %d (%.2f%%)\n", totalSuccess.Load(), float64(totalSuccess.Load())/float64(totalSent.Load())*100)
	fmt.Printf("失败: %d (%.2f%%)\n", totalFail.Load(), float64(totalFail.Load())/float64(totalSent.Load())*100)
	fmt.Printf("平均TPS: %.0f req/s\n", float64(totalSent.Load())/dur.Seconds())
	fmt.Printf("P99延迟: %v\n", lat.p99())
	fmt.Printf("耗时: %v\n", dur)
	fmt.Printf("结束: %s\n", time.Now().Format("2006-01-02 15:04:05"))
}

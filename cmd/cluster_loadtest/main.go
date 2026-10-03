package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "raftkv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type nodeInfo struct {
	httpPort string
	grpcPort string
}

type leaderConn struct {
	mu       sync.Mutex
	conn     *grpc.ClientConn
	client   pb.RaftServiceClient
	term     int64
	leaderID string
	grpcAddr string
}

func (lc *leaderConn) get() (pb.RaftServiceClient, int64, string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.client, lc.term, lc.leaderID
}

func (lc *leaderConn) reconnect(nodes []nodeInfo) bool {
	for _, n := range nodes {
		url := fmt.Sprintf("http://localhost:%s/raft/status", n.httpPort)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var status map[string]interface{}
		if json.Unmarshal(body, &status) != nil {
			continue
		}
		state, _ := status["state"].(string)
		if state != "Leader" {
			continue
		}
		term := int64(0)
		if t, ok := status["term"].(float64); ok {
			term = int64(t)
		}
		leaderID, _ := status["leader_id"].(string)
		grpcAddr := fmt.Sprintf("localhost:%s", n.grpcPort)

		lc.mu.Lock()
		if lc.leaderID == leaderID && lc.conn != nil {
			lc.mu.Unlock()
			return true
		}
		lc.mu.Unlock()

		conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		lc.mu.Lock()
		if lc.conn != nil {
			lc.conn.Close()
		}
		oldLeader := lc.leaderID
		lc.conn = conn
		lc.client = pb.NewRaftServiceClient(conn)
		lc.term = term
		lc.leaderID = leaderID
		lc.grpcAddr = grpcAddr
		lc.mu.Unlock()
		if oldLeader != "" {
			fmt.Printf("[重连] Leader 切换: %s → %s (term=%d)\n", oldLeader, leaderID, term)
		}
		return true
	}
	return false
}

var (
	totalSent    atomic.Int64
	totalSuccess atomic.Int64
	totalFail    atomic.Int64
	totalBytes   atomic.Int64
)

type latTracker struct {
	mu   sync.Mutex
	data []time.Duration
}

func (lt *latTracker) add(d time.Duration) {
	lt.mu.Lock()
	if len(lt.data) < 50000 {
		lt.data = append(lt.data, d)
	} else {
		lt.data = lt.data[1:]
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
	duration := flag.Duration("duration", 60*time.Second, "压测持续时间")
	concurrency := flag.Int("concurrency", 30, "并发 goroutine 数")
	nodesRaw := flag.String("nodes", "9001:9500,9002:9501,9003:9502,9104:9604,9105:9605", "节点列表 HTTP:gRPC")
	flag.Parse()

	var nodes []nodeInfo
	for _, pair := range strings.Split(*nodesRaw, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
		if len(parts) == 2 {
			nodes = append(nodes, nodeInfo{httpPort: parts[0], grpcPort: parts[1]})
		}
	}

	fmt.Printf("╔══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  RaftKV 分布式集群压测 — 故障自愈验证                    ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  并发: %-3d goroutine    持续: %-6v              ║\n", *concurrency, *duration)
	fmt.Printf("║  节点: %d 个  模式: gRPC → Leader → Pipeline → MySQL     ║\n", len(nodes))
	fmt.Printf("╚══════════════════════════════════════════════════════════╝\n\n")

	lc := &leaderConn{}
	if !lc.reconnect(nodes) {
		fmt.Fprintln(os.Stderr, "无法找到 Leader")
		os.Exit(1)
	}
	lc.mu.Lock()
	fmt.Printf("初始 Leader: %s (term=%d, grpc=%s)\n\n", lc.leaderID, lc.term, lc.grpcAddr)
	lc.mu.Unlock()

	lat := &latTracker{}
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	var seq atomic.Int64
	reconnectCh := make(chan struct{}, 1)

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			localSeq := 0
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				localSeq++
				n := seq.Add(1)

				client, term, leaderID := lc.get()
				if client == nil {
					totalFail.Add(1)
					select {
					case reconnectCh <- struct{}{}:
					default:
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				entry := &pb.LogEntry{
					Term:    term,
					Index:   n,
					Command: []byte(fmt.Sprintf(`{"src":"dist-load-%d","idx":%d,"ts":%d}`, id, localSeq, time.Now().UnixNano())),
				}
				req := &pb.AppendEntriesRequest{
					Term:         term,
					LeaderId:     leaderID,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []*pb.LogEntry{entry},
					LeaderCommit: n,
				}

				t0 := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, err := client.AppendEntries(ctx, req)
				cancel()
				elapsed := time.Since(t0)

				totalSent.Add(1)
				totalBytes.Add(int64(len(entry.Command)))
				lat.add(elapsed)

				if err != nil || resp == nil || !resp.Success {
					totalFail.Add(1)
					select {
					case reconnectCh <- struct{}{}:
					default:
					}
				} else {
					totalSuccess.Add(1)
				}
			}
		}(i)
	}

	go func() {
		for {
			select {
			case <-stopCh:
				return
			case <-reconnectCh:
				if lc.reconnect(nodes) {
					lc.mu.Lock()
					fmt.Printf("[重连] 新 Leader: %s (term=%d, grpc=%s)\n", lc.leaderID, lc.term, lc.grpcAddr)
					lc.mu.Unlock()
				}
			}
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	timeout := time.NewTimer(*duration)
	defer timeout.Stop()

	fmt.Printf("%-6s  %10s  %10s  %8s  %8s  %8s\n", "时间", "总发送", "TPS", "成功", "失败", "P99延迟")
	fmt.Println("──────────────────────────────────────────────────────────────")

	var lastSent int64
	sec := 0
	for {
		select {
		case <-ticker.C:
			sec++
			nowSent := totalSent.Load()
			tps := nowSent - lastSent
			lastSent = nowSent
			succ := totalSuccess.Load()
			fail := totalFail.Load()
			p99 := lat.p99()
			fmt.Printf("%-6s  %10d  %10d  %8d  %8d  %6.0fµs\n",
				fmt.Sprintf("%ds", sec), nowSent, tps, succ, fail, float64(p99.Microseconds()))
		case <-timeout.C:
			close(stopCh)
			goto DONE
		}
	}

DONE:
	wg.Wait()

	totalS := totalSent.Load()
	totalSu := totalSuccess.Load()
	totalF := totalFail.Load()
	dur := *duration
	avgTPS := float64(totalS) / dur.Seconds()

	fmt.Println("──────────────────────────────────────────────────────────────")
	fmt.Printf("\n╔══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                    分布式压测最终结果                     ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  总发送:     %-12d                        ║\n", totalS)
	fmt.Printf("║  成功:       %-12d                        ║\n", totalSu)
	fmt.Printf("║  失败:       %-12d                        ║\n", totalF)
	fmt.Printf("║  平均 TPS:   %-12.0f                        ║\n", avgTPS)
	fmt.Printf("║  P99 延迟:   %-12v                        ║\n", lat.p99())
	fmt.Printf("║  数据量:     %-10.2f MB                        ║\n", float64(totalBytes.Load())/1024/1024)
	fmt.Printf("║  耗时:       %-12v                        ║\n", dur)
	fmt.Printf("╚══════════════════════════════════════════════════════════╝\n")
}

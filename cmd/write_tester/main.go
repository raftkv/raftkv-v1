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
	totalBytes   atomic.Int64
	totalDurUs   atomic.Int64
	minDurUs     atomic.Int64
	maxDurUs     atomic.Int64
)

type latTracker struct {
	mu   sync.Mutex
	data []time.Duration
}

func (lt *latTracker) add(d time.Duration) {
	lt.mu.Lock()
	if len(lt.data) < 100000 {
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

var httpToGrpc = map[string]string{
	"9001": "9500", "9002": "9501", "9003": "9502", "9104": "9604", "9105": "9605",
}

func findLeader(host string) (string, int64, string, error) {
	ports := []string{"9001", "9002", "9003", "9104", "9105"}
	for _, p := range ports {
		url := fmt.Sprintf("http://%s:%s/raft/status", host, p)
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
		grpcPort := httpToGrpc[p]
		return grpcPort, term, leaderID, nil
	}
	return "", 0, "", fmt.Errorf("no leader found among 5 nodes")
}

func main() {
	target := flag.String("target", "172.38.3.147", "目标主机IP(用于找Leader)")
	grpcPort := flag.String("grpc", "", "gRPC端口(留空则自动从Leader状态获取)")
	total := flag.Int("total", 100000, "总写入请求数")
	concurrency := flag.Int("concurrency", 50, "并发goroutine数")
	flag.Parse()

	minDurUs.Store(1 << 62)

	fmt.Printf("============================================================\n")
	fmt.Printf("  RaftKV gRPC AppendEntries 写入压测工具\n")
	fmt.Printf("============================================================\n")
	fmt.Printf("  目标主机     : %s\n", *target)
	fmt.Printf("  总请求数     : %d\n", *total)
	fmt.Printf("  并发数       : %d goroutines\n", *concurrency)
	fmt.Printf("  启动时间     : %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf("============================================================\n\n")

	port := *grpcPort
	term := int64(1)
	leaderID := "write-tester"

	fmt.Printf("[探测] 正在扫描 %s 的5个节点查找 Leader...\n", *target)
	_, t, l, err := findLeader(*target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[失败] 无法找到 Leader: %v\n", err)
		os.Exit(1)
	}
	term = t
	leaderID = l
	fmt.Printf("[探测] Leader=%s, term=%d\n", leaderID, term)

	if port == "" {
		port = "9500"
	}
	fmt.Printf("[目标] 向 gRPC 端口 %s 发送写入(模拟Leader→Follower复制)\n\n", port)

	grpcAddr := fmt.Sprintf("%s:%s", *target, port)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[失败] gRPC连接失败: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewRaftServiceClient(conn)
	fmt.Printf("[连接] gRPC已连接: %s\n\n", grpcAddr)

	lat := &latTracker{}
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	var seq atomic.Int64

	fmt.Printf("%-8s  %10s  %10s  %10s  %10s  %10s\n", "进度", "已发送", "成功", "失败", "TPS", "P99延迟")
	fmt.Println("──────────────────────────────────────────────────────────────────────")

	progressTicker := time.NewTicker(2 * time.Second)
	defer progressTicker.Stop()
	go func() {
		var lastSent int64
		for {
			select {
			case <-stopCh:
				return
			case <-progressTicker.C:
				nowSent := totalSent.Load()
				tps := (nowSent - lastSent) / 2
				lastSent = nowSent
				succ := totalSuccess.Load()
				fail := totalFail.Load()
				p99 := lat.p99()
				fmt.Printf("%-8s  %10d  %10d  %10d  %10d  %8.0fµs\n",
					fmt.Sprintf("%d/%d", nowSent, *total), nowSent, succ, fail, tps, float64(p99.Microseconds()))
			}
		}
	}()

	t0 := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			localSeq := 0
			for {
				n := seq.Add(1)
				if n > int64(*total) {
					return
				}
				localSeq++

				entry := &pb.LogEntry{
					Term:    term,
					Index:   n,
					Command: []byte(fmt.Sprintf(`{"src":"write-tester-%d","idx":%d,"ts":%d,"data":"gRPC-AppendEntries-payload-%d"}`, id, localSeq, time.Now().UnixNano(), n)),
				}
				req := &pb.AppendEntriesRequest{
					Term:         term,
					LeaderId:     leaderID,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []*pb.LogEntry{entry},
					LeaderCommit: n,
				}

				st0 := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				resp, err := client.AppendEntries(ctx, req)
				cancel()

				totalSent.Add(1)
				totalBytes.Add(int64(len(entry.Command)))
				lat.add(time.Since(st0))

				us := time.Since(st0).Microseconds()
				totalDurUs.Add(us)
				if us < minDurUs.Load() {
					minDurUs.Store(us)
				}
				if us > maxDurUs.Load() {
					maxDurUs.Store(us)
				}

				if err != nil || resp == nil || !resp.Success {
					totalFail.Add(1)
					if totalFail.Load() <= 5 {
						if err != nil {
							fmt.Printf("[DEBUG] 请求#%d 失败: err=%v\n", n, err)
						} else if resp == nil {
							fmt.Printf("[DEBUG] 请求#%d 失败: resp=nil\n", n)
						} else {
							fmt.Printf("[DEBUG] 请求#%d 失败: resp.Term=%d resp.Success=%v\n", n, resp.Term, resp.Success)
						}
					}
				} else {
					totalSuccess.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()
	close(stopCh)
	dur := time.Since(t0)

	totalS := totalSent.Load()
	totalSu := totalSuccess.Load()
	totalF := totalFail.Load()
	avgTPS := float64(totalS) / dur.Seconds()
	avgDur := float64(totalDurUs.Load()) / float64(totalS)

	fmt.Println("──────────────────────────────────────────────────────────────────────")
	fmt.Printf("\n============================================================\n")
	fmt.Printf("  gRPC AppendEntries 写入压测最终结果\n")
	fmt.Printf("============================================================\n")
	fmt.Printf("  总发送       : %d\n", totalS)
	fmt.Printf("  成功         : %d (%.2f%%)\n", totalSu, float64(totalSu)/float64(totalS)*100)
	fmt.Printf("  失败         : %d (%.2f%%)\n", totalF, float64(totalF)/float64(totalS)*100)
	fmt.Printf("  平均 TPS     : %.0f req/s\n", avgTPS)
	fmt.Printf("  P99 延迟     : %v\n", lat.p99())
	fmt.Printf("  平均延迟     : %.0fµs\n", avgDur)
	fmt.Printf("  最小延迟     : %dµs\n", minDurUs.Load())
	fmt.Printf("  最大延迟     : %dµs\n", maxDurUs.Load())
	fmt.Printf("  数据量       : %.2f MB\n", float64(totalBytes.Load())/1024/1024)
	fmt.Printf("  耗时         : %v\n", dur)
	fmt.Printf("============================================================\n")
}

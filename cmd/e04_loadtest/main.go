package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	totalSent    atomic.Int64
	totalSuccess atomic.Int64
	totalFail    atomic.Int64
	writeReqs    atomic.Int64
	readReqs     atomic.Int64
	redirectCnt  atomic.Int64 // 修复E: leader重定向次数
	retryCnt     atomic.Int64 // 修复E: 写失败重试次数
	retryOkCnt   atomic.Int64 // 修复E: 重试后转为成功次数

	leaderMu      sync.RWMutex
	currentLeader string // 修复E: 当前leader端口(共享状态)
)

var backoffDelays = []time.Duration{10 * time.Millisecond, 50 * time.Millisecond, 200 * time.Millisecond}

var nodeIDRe = regexp.MustCompile(`node-(\d+)`)

func getLeader() string {
	leaderMu.RLock()
	defer leaderMu.RUnlock()
	return currentLeader
}

func setLeader(port string) {
	leaderMu.Lock()
	currentLeader = port
	leaderMu.Unlock()
}

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

func (lt *latTracker) percentile(p float64) time.Duration {
	lt.mu.Lock()
	if len(lt.data) == 0 {
		lt.mu.Unlock()
		return 0
	}
	sorted := make([]time.Duration, len(lt.data))
	copy(sorted, lt.data)
	lt.mu.Unlock()
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (lt *latTracker) max() time.Duration {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if len(lt.data) == 0 {
		return 0
	}
	m := lt.data[0]
	for _, d := range lt.data[1:] {
		if d > m {
			m = d
		}
	}
	return m
}

func findLeader(nodes string) string {
	for _, n := range splitNodes(nodes) {
		url := fmt.Sprintf("http://localhost:%s/raft/status", n)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if bytes.Contains(body, []byte(`"state":"Leader"`)) {
			return n
		}
	}
	return ""
}

func splitNodes(nodes string) []string {
	var result []string
	for _, n := range bytes.Split([]byte(nodes), []byte(",")) {
		result = append(result, string(n))
	}
	return result
}

// 修复E: 从"not leader: current leader is node-N"错误信息解析新leader端口
func parseLeaderFromError(respBody []byte, nodesRaw string) string {
	m := nodeIDRe.FindSubmatch(respBody)
	if len(m) < 2 {
		return ""
	}
	port := "900" + string(m[1])
	for _, n := range splitNodes(nodesRaw) {
		if n == port {
			return port
		}
	}
	return ""
}

// 修复E: 写请求（带NotLeader重定向 + 最多3次重试 + 退避）
// 返回 true 表示最终成功，false 表示3次尝试后仍失败
func doWrite(client *http.Client, nodesRaw string, id int, n int64) bool {
	body := fmt.Sprintf(`{"src":"e04-%d","idx":%d,"ts":%d}`, id, n, time.Now().UnixNano())
	bodyBytes := []byte(body)

	for attempt := 0; attempt <= 3; attempt++ {
		leaderPort := getLeader()
		resp, err := client.Post(
			fmt.Sprintf("http://localhost:%s/raft/propose", leaderPort),
			"application/json",
			bytes.NewBuffer(bodyBytes),
		)
		if err == nil {
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 && !bytes.Contains(respBody, []byte(`"success":false`)) {
				if attempt > 0 {
					retryOkCnt.Add(1)
				}
				return true
			}
			// 失败响应：NotLeader 触发重定向
			if bytes.Contains(respBody, []byte("not leader")) {
				newPort := parseLeaderFromError(respBody, nodesRaw)
				if newPort == "" {
					newPort = findLeader(nodesRaw)
				}
				if newPort != "" && newPort != leaderPort {
					setLeader(newPort)
					redirectCnt.Add(1)
				}
			}
		}
		// 重试（上限3次，逐次退避）
		if attempt < 3 {
			retryCnt.Add(1)
			time.Sleep(backoffDelays[attempt])
		}
	}
	return false
}
func main() {
	duration := flag.Duration("duration", 120*time.Second, "压测持续时间")
	concurrency := flag.Int("concurrency", 500, "并发goroutine数")
	nodesRaw := flag.String("nodes", "9001,9002,9003,9004,9005", "节点HTTP端口列表")
	writeRatio := flag.Int("write-ratio", 20, "写入百分比(0-100)")
	flag.Parse()

	initLeader := findLeader(*nodesRaw)
	if initLeader == "" {
		fmt.Fprintln(os.Stderr, "无法找到Leader")
		os.Exit(1)
	}
	setLeader(initLeader)
	fmt.Printf("Leader: localhost:%s\n", initLeader)
	fmt.Printf("并发: %d  持续: %v  写入比例: %d%%\n\n", *concurrency, *duration, *writeRatio)

	lat := &latTracker{}
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	var seq atomic.Int64

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := &http.Client{
				Timeout:   5 * time.Second,
				Transport: &http.Transport{MaxIdleConns: 200, MaxIdleConnsPerHost: 200},
			}
			localSeq := 0
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				localSeq++
				n := seq.Add(1)
				isWrite := (localSeq*100 / *writeRatio)%100 == 0
				if *writeRatio == 100 {
					isWrite = true
				}

				var resp *http.Response
				var err error
				t0 := time.Now()

				if isWrite {
					writeReqs.Add(1)
					ok := doWrite(client, *nodesRaw, id, n)
					elapsed := time.Since(t0)
					lat.add(elapsed)
					totalSent.Add(1)
					if ok {
						totalSuccess.Add(1)
					} else {
						totalFail.Add(1)
					}
					continue
				}

				resp, err = client.Get(fmt.Sprintf("http://localhost:%s/raft/status", getLeader()))
				readReqs.Add(1)
				elapsed := time.Since(t0)
				lat.add(elapsed)
				totalSent.Add(1)

				if err != nil {
					totalFail.Add(1)
					continue
				}
				_, _ = io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 200 {
					totalSuccess.Add(1)
				} else {
					totalFail.Add(1)
				}
			}
		}(i)
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	timeout := time.NewTimer(*duration)
	defer timeout.Stop()

	fmt.Printf("%-8s  %10s  %10s  %8s  %8s  %10s  %10s  %10s\n",
		"时间", "总发送", "TPS", "成功", "失败", "P50", "P99", "Max")
	fmt.Println("──────────────────────────────────────────────────────────────────────────────────")

	var lastSent int64
	sec := 0
	for {
		select {
		case <-ticker.C:
			sec += 5
			nowSent := totalSent.Load()
			tps := (nowSent - lastSent) / 5
			lastSent = nowSent
			succ := totalSuccess.Load()
			fail := totalFail.Load()
			p50 := lat.percentile(0.50)
			p99 := lat.percentile(0.99)
			mx := lat.max()
			fmt.Printf("%-8s  %10d  %10d  %8d  %8d  %8.1fms  %8.1fms  %8.1fms\n",
				fmt.Sprintf("%ds", sec), nowSent, tps, succ, fail,
				float64(p50.Milliseconds()), float64(p99.Milliseconds()), float64(mx.Milliseconds()))
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
	wr := writeReqs.Load()
	rr := readReqs.Load()
	dur := *duration
	avgTPS := float64(totalS) / dur.Seconds()
	successRate := float64(0)
	if totalS > 0 {
		successRate = float64(totalSu) / float64(totalS) * 100
	}

	fmt.Println("──────────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("\n=== E04 最终结果 ===\n")
	fmt.Printf("总发送:     %d (写=%d 读=%d)\n", totalS, wr, rr)
	fmt.Printf("成功:       %d\n", totalSu)
	fmt.Printf("失败:       %d\n", totalF)
	fmt.Printf("成功率:     %.2f%%\n", successRate)
	fmt.Printf("平均TPS:    %.0f req/s\n", avgTPS)
	fmt.Printf("重定向:     %d\n", redirectCnt.Load())
	fmt.Printf("重试:       %d (重试成功=%d)\n", retryCnt.Load(), retryOkCnt.Load())
	fmt.Printf("P50:        %v\n", lat.percentile(0.50))
	fmt.Printf("P99:        %v\n", lat.percentile(0.99))
	fmt.Printf("Max:        %v\n", lat.max())
	fmt.Printf("耗时:       %v\n", dur)
}

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"

	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type LatencyHistogram struct {
	mu      sync.Mutex
	buckets []int64
	bounds  []float64
	total   int64
}

func NewLatencyHistogram() *LatencyHistogram {
	bounds := []float64{0.1, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000}
	return &LatencyHistogram{
		buckets: make([]int64, len(bounds)+1),
		bounds:  bounds,
	}
}

func (h *LatencyHistogram) Record(latencyMs float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, b := range h.bounds {
		if latencyMs <= b {
			h.buckets[i]++
			h.total++
			return
		}
	}
	h.buckets[len(h.bounds)]++
	h.total++
}

func (h *LatencyHistogram) Percentile(p float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	target := int64(float64(h.total) * p / 100)
	var cum int64
	for i, count := range h.buckets {
		cum += count
		if cum >= target {
			if i < len(h.bounds) {
				return h.bounds[i]
			}
			return 99999
		}
	}
	return 99999
}

func (h *LatencyHistogram) Print() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("  total=%d\n", h.total))
	for i, count := range h.buckets {
		if count == 0 {
			continue
		}
		if i < len(h.bounds) {
			sb.WriteString(fmt.Sprintf("  <=%.1fms: %d\n", h.bounds[i], count))
		} else {
			sb.WriteString(fmt.Sprintf("  >%.1fms: %d\n", h.bounds[i-1], count))
		}
	}
	return sb.String()
}

type WorkerStats struct {
	success   int64
	fail      int64
	shed      int64
	histogram *LatencyHistogram
}

type LoadGen struct {
	concurrency  int
	duration     time.Duration
	endpoints    []string
	mode         string
	writeRatio   int
	client       *http.Client
	stats        []*WorkerStats
	totalReq     int64
	totalSuccess int64
	totalFail    int64
	leaderEP     string
	leaderAt     time.Time
	leaderMu     sync.Mutex
}

func NewLoadGen(concurrency int, duration time.Duration, endpoints []string, mode string, writeRatio int) *LoadGen {
	transport := &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}
	stats := make([]*WorkerStats, concurrency)
	for i := range stats {
		stats[i] = &WorkerStats{histogram: NewLatencyHistogram()}
	}
	return &LoadGen{
		concurrency: concurrency,
		duration:    duration,
		endpoints:   endpoints,
		mode:        mode,
		writeRatio:  writeRatio,
		client:      client,
		stats:       stats,
	}
}

func (lg *LoadGen) findLeader() string {
	lg.leaderMu.Lock()
	defer lg.leaderMu.Unlock()
	if lg.leaderEP != "" && time.Since(lg.leaderAt) < 1*time.Second {
		return lg.leaderEP
	}
	for _, ep := range lg.endpoints {
		resp, err := lg.client.Get(fmt.Sprintf("http://%s/raft/stats", ep))
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if strings.Contains(string(body), "state=Leader") {
			lg.leaderEP = ep
			lg.leaderAt = time.Now()
			return ep
		}
	}
	lg.leaderEP = lg.endpoints[0]
	lg.leaderAt = time.Now()
	return lg.endpoints[0]
}

func (lg *LoadGen) invalidateLeader() {
	lg.leaderMu.Lock()
	lg.leaderEP = ""
	lg.leaderAt = time.Time{}
	lg.leaderMu.Unlock()
}

func (lg *LoadGen) doWrite(workerID int, keyIdx int64) bool {
	body := fmt.Sprintf(`{"key":"loadgen_%d_%d","value":"v%d"}`, workerID, keyIdx, keyIdx)
	start := time.Now()
	ep := lg.findLeader()

	for attempt := 0; attempt < 3; attempt++ {
		req, _ := http.NewRequest("POST", fmt.Sprintf("http://%s/raft/propose", ep), bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := lg.client.Do(req)
		if err != nil {
			lg.invalidateLeader()
			ep = lg.findLeader()
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 429 || resp.StatusCode == 503 {
			atomic.AddInt64(&lg.stats[workerID].shed, 1)
			return false
		}
		if resp.StatusCode == 200 && !bytes.Contains(respBody, []byte(`"success":false`)) {
			latency := float64(time.Since(start).Microseconds()) / 1000.0
			lg.stats[workerID].histogram.Record(latency)
			atomic.AddInt64(&lg.stats[workerID].success, 1)
			return true
		}
		lg.invalidateLeader()
		ep = lg.findLeader()
	}

	latency := float64(time.Since(start).Microseconds()) / 1000.0
	lg.stats[workerID].histogram.Record(latency)
	atomic.AddInt64(&lg.stats[workerID].fail, 1)
	return false
}

func (lg *LoadGen) doRead(workerID int) bool {
	ep := lg.endpoints[workerID%len(lg.endpoints)]
	start := time.Now()
	resp, err := lg.client.Get(fmt.Sprintf("http://%s/raft/stats", ep))
	latency := float64(time.Since(start).Microseconds()) / 1000.0
	lg.stats[workerID].histogram.Record(latency)
	if err != nil {
		atomic.AddInt64(&lg.stats[workerID].fail, 1)
		return false
	}
	resp.Body.Close()
	if resp.StatusCode == 200 {
		atomic.AddInt64(&lg.stats[workerID].success, 1)
		return true
	}
	atomic.AddInt64(&lg.stats[workerID].fail, 1)
	return false
}

func (lg *LoadGen) worker(id int, wg *sync.WaitGroup, stopCh <-chan struct{}) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(int64(id)))
	var keyIdx int64
	for {
		select {
		case <-stopCh:
			return
		default:
		}
		if lg.mode == "mixed-rw" {
			if rng.Intn(100) < lg.writeRatio {
				lg.doWrite(id, atomic.AddInt64(&keyIdx, 1))
			} else {
				lg.doRead(id)
			}
		} else {
			lg.doWrite(id, atomic.AddInt64(&keyIdx, 1))
		}
	}
}

func (lg *LoadGen) Run() Result {
	leader := lg.findLeader()
	fmt.Fprintf(os.Stderr, "[loadgen] leader=%s, concurrency=%d, duration=%v, mode=%s\n", leader, lg.concurrency, lg.duration, lg.mode)

	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < lg.concurrency; i++ {
		wg.Add(1)
		go lg.worker(i, &wg, stopCh)
	}

	time.Sleep(lg.duration)
	close(stopCh)
	wg.Wait()

	elapsed := time.Since(start)
	elapsedSec := elapsed.Seconds()

	var totalShed int64
	for i := range lg.stats {
		s := atomic.LoadInt64(&lg.stats[i].success)
		f := atomic.LoadInt64(&lg.stats[i].fail)
		h := atomic.LoadInt64(&lg.stats[i].shed)
		atomic.AddInt64(&lg.totalReq, s+f+h)
		atomic.AddInt64(&lg.totalSuccess, s)
		atomic.AddInt64(&lg.totalFail, f)
		totalShed += h
	}

	mergedHist := NewLatencyHistogram()
	for i := range lg.stats {
		lg.stats[i].histogram.mu.Lock()
		for j, count := range lg.stats[i].histogram.buckets {
			mergedHist.buckets[j] += count
		}
		mergedHist.total += lg.stats[i].histogram.total
		lg.stats[i].histogram.mu.Unlock()
	}

	tps := float64(lg.totalSuccess) / elapsedSec
	successRate := 100.0
	shedRate := 0.0
	if lg.totalReq > 0 {
		successRate = float64(lg.totalSuccess) * 100.0 / float64(lg.totalReq)
		shedRate = float64(totalShed) * 100.0 / float64(lg.totalReq)
	}

	return Result{
		Concurrency: lg.concurrency,
		Duration:    elapsedSec,
		TotalReq:    lg.totalReq,
		Success:     lg.totalSuccess,
		Fail:        lg.totalFail,
		Shed:        totalShed,
		TPS:         tps,
		SuccessRate: successRate,
		ShedRate:    shedRate,
		P50:         mergedHist.Percentile(50),
		P95:         mergedHist.Percentile(95),
		P99:         mergedHist.Percentile(99),
		MaxMs:       mergedHist.Percentile(100),
		Histogram:   mergedHist,
	}
}

type Result struct {
	Concurrency int               `json:"concurrency"`
	Duration    float64           `json:"duration_sec"`
	TotalReq    int64             `json:"total_req"`
	Success     int64             `json:"success"`
	Fail        int64             `json:"fail"`
	Shed        int64             `json:"shed"`
	TPS         float64           `json:"tps"`
	SuccessRate float64           `json:"success_rate"`
	ShedRate    float64           `json:"shed_rate"`
	P50         float64           `json:"p50_ms"`
	P95         float64           `json:"p95_ms"`
	P99         float64           `json:"p99_ms"`
	MaxMs       float64           `json:"max_ms"`
	Histogram   *LatencyHistogram `json:"-"`
}

func (r Result) Summary() string {
	return fmt.Sprintf("concurrency=%d  duration=%.1fs  total=%d  success=%d  shed=%d  fail=%d  TPS=%.1f  successRate=%.2f%%  shedRate=%.2f%%  P50=%.1fms  P95=%.1fms  P99=%.1fms  Max=%.1fms",
		r.Concurrency, r.Duration, r.TotalReq, r.Success, r.Shed, r.Fail, r.TPS, r.SuccessRate, r.ShedRate, r.P50, r.P95, r.P99, r.MaxMs)
}

func (r Result) JSON() string {
	b, _ := json.Marshal(struct {
		Concurrency int     `json:"concurrency"`
		Duration    float64 `json:"duration_sec"`
		TotalReq    int64   `json:"total_req"`
		Success     int64   `json:"success"`
		Shed        int64   `json:"shed"`
		Fail        int64   `json:"fail"`
		TPS         float64 `json:"tps"`
		SuccessRate float64 `json:"success_rate"`
		ShedRate    float64 `json:"shed_rate"`
		P50         float64 `json:"p50_ms"`
		P95         float64 `json:"p95_ms"`
		P99         float64 `json:"p99_ms"`
		MaxMs       float64 `json:"max_ms"`
	}{
		r.Concurrency, r.Duration, r.TotalReq, r.Success, r.Shed, r.Fail, r.TPS, r.SuccessRate, r.ShedRate, r.P50, r.P95, r.P99, r.MaxMs,
	})
	return string(b)
}

func main() {
	debug.SetMaxThreads(50000)
	concurrency := flag.Int("concurrency", 64, "并发 goroutine 数")
	duration := flag.Duration("duration", 30*time.Second, "测试时长")
	endpointsStr := flag.String("endpoints", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9004,127.0.0.1:9005", "目标端点列表(逗号分隔)")
	mode := flag.String("mode", "write", "负载模式: write | mixed-rw")
	writeRatio := flag.Int("write-ratio", 70, "mixed-rw 模式下写入比例(%)")
	output := flag.String("output", "", "结果输出文件路径(可选)")
	showHist := flag.Bool("hist", false, "打印延迟直方图")
	flag.Parse()

	endpoints := strings.Split(*endpointsStr, ",")
	lg := NewLoadGen(*concurrency, *duration, endpoints, *mode, *writeRatio)
	result := lg.Run()

	summary := result.Summary()
	fmt.Println(summary)

	if *showHist {
		fmt.Println("延迟直方图:")
		fmt.Print(result.Histogram.Print())
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	fmt.Printf("客户端内存: Alloc=%dKB Sys=%dKB  NumGoroutine=%d\n",
		memStats.Alloc/1024, memStats.Sys/1024, runtime.NumGoroutine())

	if *output != "" {
		f, err := os.Create(*output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "输出文件创建失败: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		fmt.Fprintf(f, "%s\n", result.JSON())
		if *showHist {
			fmt.Fprintf(f, "\n直方图:\n%s", result.Histogram.Print())
		}
	}
}

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	totalReqs   atomic.Int64
	totalErrors atomic.Int64
	totalBytes  atomic.Int64
	writeReqs   atomic.Int64
	readReqs    atomic.Int64
	totalDurUs  atomic.Int64
	minDurUs    atomic.Int64
	maxDurUs    atomic.Int64
	status200   atomic.Int64
	status404   atomic.Int64
	statusOther atomic.Int64
)

func init() {
	minDurUs.Store(int64(^uint64(0) >> 1))
}

func worker(id int, target string, wg *sync.WaitGroup, stop <-chan struct{}) {
	defer wg.Done()
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConns: 200, MaxIdleConnsPerHost: 200, MaxConnsPerHost: 0},
	}
	seq := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		seq++
		var resp *http.Response
		var err error
		start := time.Now()
		isWrite := (seq%5 == 0)
		if isWrite {
			body := fmt.Sprintf(`{"term":1,"leader_id":"load-tester","prev_log_index":0,"prev_log_term":0,"leader_commit":0,"entries":[{"term":1,"index":%d,"command":"write-payload-%d"}]}`, seq, seq)
			resp, err = client.Post(target+"/raft/append_entries", "application/json", bytes.NewBufferString(body))
		} else {
			paths := []string{"/raft/status", "/health/live", "/health/ready", "/latency/stats", "/pipeline/stats"}
			path := paths[seq%len(paths)]
			resp, err = client.Get(target + path)
		}
		dur := time.Since(start)
		durUs := dur.Microseconds()
		totalDurUs.Add(durUs)
		for {
			old := minDurUs.Load()
			if durUs >= old || minDurUs.CompareAndSwap(old, durUs) {
				break
			}
		}
		for {
			old := maxDurUs.Load()
			if durUs <= old || maxDurUs.CompareAndSwap(old, durUs) {
				break
			}
		}
		if err != nil {
			totalErrors.Add(1)
			statusOther.Add(1)
		} else {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			totalBytes.Add(int64(len(body)))
			switch resp.StatusCode {
			case 200:
				status200.Add(1)
			case 404:
				status404.Add(1)
			default:
				statusOther.Add(1)
			}
			if resp.StatusCode >= 400 {
				totalErrors.Add(1)
			}
		}
		totalReqs.Add(1)
		if isWrite {
			writeReqs.Add(1)
		} else {
			readReqs.Add(1)
		}
	}
}

func main() {
	duration := flag.String("duration", "60s", "压测持续时间")
	concurrency := flag.Int("concurrency", 100, "并发goroutine数")
	target := flag.String("target", "http://127.0.0.1:9001", "目标地址")
	flag.Parse()

	d, err := time.ParseDuration(*duration)
	if err != nil {
		fmt.Fprintf(os.Stderr, "无效的duration: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("============================================================\n")
	fmt.Printf("  RaftKV 工业级极限负载压测工具\n")
	fmt.Printf("============================================================\n")
	fmt.Printf("  目标地址     : %s\n", *target)
	fmt.Printf("  并发数       : %d goroutines\n", *concurrency)
	fmt.Printf("  持续时间     : %s\n", d)
	fmt.Printf("  写入比例     : 20%% (POST /raft/append_entries)\n")
	fmt.Printf("  读取比例     : 80%% (GET /raft/status, /health/*, /latency/*)\n")
	fmt.Printf("  启动时间     : %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf("============================================================\n\n")

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(i, *target, &wg, stop)
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	startTime := time.Now()

	go func() {
		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(startTime)
				reqs := totalReqs.Load()
				errs := totalErrors.Load()
				wr := writeReqs.Load()
				rr := readReqs.Load()
				rate := float64(reqs) / elapsed.Seconds()
				fmt.Printf("[进度] %s | 总请求=%d (写=%d 读=%d) | 吞吐=%.0f req/s | 错误=%d | 错误率=%.4f%%\n",
					elapsed.Round(time.Second), reqs, wr, rr, rate, errs, float64(errs)/float64(reqs)*100)
			case <-stop:
				return
			}
		}
	}()

	time.Sleep(d)
	close(stop)
	wg.Wait()

	elapsed := time.Since(startTime)
	reqs := totalReqs.Load()
	errs := totalErrors.Load()
	wr := writeReqs.Load()
	rr := readReqs.Load()
	bytesTotal := totalBytes.Load()
	rate := float64(reqs) / elapsed.Seconds()

	avgDurUs := int64(0)
	if reqs > 0 {
		avgDurUs = totalDurUs.Load() / reqs
	}
	minD := minDurUs.Load()
	maxD := maxDurUs.Load()

	fmt.Printf("\n============================================================\n")
	fmt.Printf("  压测结果汇总\n")
	fmt.Printf("============================================================\n")
	fmt.Printf("  总持续时间   : %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  总请求数     : %d\n", reqs)
	fmt.Printf("    - 写入请求 : %d (%.1f%%)\n", wr, float64(wr)/float64(reqs)*100)
	fmt.Printf("    - 读取请求 : %d (%.1f%%)\n", rr, float64(rr)/float64(reqs)*100)
	fmt.Printf("  吞吐量       : %.0f req/s (%.2f K req/s)\n", rate, rate/1000)
	fmt.Printf("  错误数       : %d (%.4f%%)\n", errs, float64(errs)/float64(reqs)*100)
	fmt.Printf("  总传输字节   : %d (%.2f MB)\n", bytesTotal, float64(bytesTotal)/1024/1024)
	fmt.Printf("  延迟统计     :\n")
	fmt.Printf("    - 最小     : %d us\n", minD)
	fmt.Printf("    - 平均     : %d us\n", avgDurUs)
	fmt.Printf("    - 最大     : %d us\n", maxD)
	fmt.Printf("  状态码分布   :\n")
	fmt.Printf("    - HTTP 200 : %d 次\n", status200.Load())
	fmt.Printf("    - HTTP 404 : %d 次\n", status404.Load())
	fmt.Printf("    - 其他     : %d 次\n", statusOther.Load())
	fmt.Printf("============================================================\n")

	if errs > 0 && float64(errs)/float64(reqs) > 0.01 {
		os.Exit(1)
	}
}

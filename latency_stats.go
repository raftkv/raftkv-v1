// =========================================================================
// RaftKV 确定性引擎 — gRPC 请求延迟统计（源头级 Histogram）
//
// 设计原则：
//   1. 纯标准库，零外部依赖（不用 prometheus client）
//   2. 内存直方图：固定桶 + 原子计数，O(1) 更新
//   3. 通过 gRPC UnaryInterceptor 无侵入统计所有 RPC 耗时
//   4. HTTP 端点 /latency/stats 输出 JSON，供监控桥读取
//
// 集成：
//   grpc.NewServer(grpc.ChainUnaryInterceptor(licenseGuardInterceptor, latencyInterceptor))
//   httpMux.HandleFunc("/latency/stats", handleLatencyStats)
// =========================================================================

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

// latencyBuckets 直方图桶边界（秒）—— 与仪表盘 le 标签对齐
// 覆盖 0.1ms ~ 1s + Inf，含 500ms 慢请求阈值
var latencyBuckets = []float64{
	0.0001, 0.0005, 0.001, 0.002, 0.005,
	0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
}

// latencyStats 全局延迟统计（原子计数，无锁）
type latencyStats struct {
	buckets []int64 // 每桶计数（含 +Inf 桶）
	count   int64   // 总请求数
	sumUs   int64   // 总耗时（微秒）
}

var globalLatency = newLatencyStats()

func newLatencyStats() *latencyStats {
	return &latencyStats{
		buckets: make([]int64, len(latencyBuckets)+1),
	}
}

// observe 记录一次请求耗时
func (ls *latencyStats) observe(d time.Duration) {
	sec := d.Seconds()
	// 定位桶
	idx := len(latencyBuckets) // +Inf 桶
	for i, b := range latencyBuckets {
		if sec < b {
			idx = i
			break
		}
	}
	atomic.AddInt64(&ls.buckets[idx], 1)
	atomic.AddInt64(&ls.count, 1)
	atomic.AddInt64(&ls.sumUs, d.Microseconds())
}

// snapshot 导出当前快照（JSON 友好）
type latencySnapshot struct {
	Buckets map[string]int64 `json:"buckets"`
	Count   int64            `json:"count"`
	SumUs   int64            `json:"sum_us"`
}

func (ls *latencyStats) snapshot() latencySnapshot {
	snap := latencySnapshot{
		Buckets: make(map[string]int64, len(latencyBuckets)+1),
		Count:   atomic.LoadInt64(&ls.count),
		SumUs:   atomic.LoadInt64(&ls.sumUs),
	}
	for i, b := range latencyBuckets {
		snap.Buckets[fmt.Sprintf("%g", b)] = atomic.LoadInt64(&ls.buckets[i])
	}
	snap.Buckets["+Inf"] = atomic.LoadInt64(&ls.buckets[len(latencyBuckets)])
	return snap
}

// latencyInterceptor gRPC 一元拦截器：统计每次 RPC 耗时
func latencyInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	globalLatency.observe(time.Since(start))
	return resp, err
}

// handleLatencyStats HTTP 端点：输出延迟统计 JSON
func handleLatencyStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(globalLatency.snapshot())
}

// latencyPrometheus 将快照转换为 Prometheus 文本格式（供桥接器/直接采集）
func (ls *latencyStats) prometheusText() string {
	snap := ls.snapshot()
	var out string
	out += "# HELP grpc_request_duration_seconds gRPC 请求耗时分布\n"
	out += "# TYPE grpc_request_duration_seconds histogram\n"
	cumulative := int64(0)
	for _, b := range latencyBuckets {
		cumulative += snap.Buckets[fmt.Sprintf("%g", b)]
		out += fmt.Sprintf("grpc_request_duration_seconds_bucket{le=\"%g\"} %d\n", b, cumulative)
	}
	cumulative += snap.Buckets["+Inf"]
	out += fmt.Sprintf("grpc_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", cumulative)
	out += fmt.Sprintf("grpc_request_duration_seconds_sum %d\n", snap.SumUs)
	out += fmt.Sprintf("grpc_request_duration_seconds_count %d\n", snap.Count)
	return out
}

// handleLatencyPrometheus HTTP 端点：直接输出 Prometheus 格式（备用）
func handleLatencyPrometheus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprint(w, globalLatency.prometheusText())
}

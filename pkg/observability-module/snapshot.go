// =========================================================================
// RaftKV Module04 — 微秒级原生可观测性底座（Latency 直采引擎）
//
// snapshot.go — 快照导出（文本 + JSON 格式）
//
// 设计要点：
//   1. HistogramSnapshot：单 Histogram 的快照（count/sum/min/max/P50/P90/P99/P999/buckets）
//   2. CollectorSnapshot：整个 MetricsCollector 的快照（所有 name+labels 组合）
//   3. TextFormat：人类可读文本（带分位数与桶分布）
//   4. JSONFormat：机器可读 JSON（供监控桥/前端消费）
//   5. 纯标准库 encoding/json + fmt，零外部依赖
// =========================================================================

package observability

import (
	"encoding/json"
	"fmt"
	"strings"
)

// -------------------------------------------------------------------------
// HistogramSnapshot — 单 Histogram 快照
// -------------------------------------------------------------------------

// HistogramSnapshot 单 Histogram 快照。
type HistogramSnapshot struct {
	Name    string            `json:"name"`
	Labels  map[string]string `json:"labels,omitempty"`
	Count   int64             `json:"count"`
	SumNS   int64             `json:"sum_ns"`
	MeanNS  int64             `json:"mean_ns"`
	MinNS   int64             `json:"min_ns"`
	MaxNS   int64             `json:"max_ns"`
	P50NS   int64             `json:"p50_ns"`
	P90NS   int64             `json:"p90_ns"`
	P99NS   int64             `json:"p99_ns"`
	P999NS  int64             `json:"p999_ns"`
	Buckets []BucketEntry     `json:"buckets"`
}

// BucketEntry 单桶条目。
type BucketEntry struct {
	UpperBoundNS int64 `json:"upper_ns"`
	Count        int64 `json:"count"`
}

// Snapshot 生成 Histogram 快照。
func (h *Histogram) Snapshot(name string, labels []Label) HistogramSnapshot {
	snap := HistogramSnapshot{
		Name:    name,
		Labels:  labelsToMap(labels),
		Count:   h.Count(),
		SumNS:   h.SumNS(),
		MeanNS:  h.MeanNS(),
		MinNS:   h.MinNS(),
		MaxNS:   h.MaxNS(),
		P50NS:   h.P50(),
		P90NS:   h.P90(),
		P99NS:   h.P99(),
		P999NS:  h.P999(),
		Buckets: make([]BucketEntry, numBuckets),
	}
	bounds := BucketBounds()
	counts := h.Buckets()
	for i := 0; i < numBuckets; i++ {
		snap.Buckets[i] = BucketEntry{UpperBoundNS: bounds[i], Count: counts[i]}
	}
	return snap
}

// labelsToMap labels 转 map。
func labelsToMap(labels []Label) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	m := make(map[string]string, len(labels))
	for _, l := range labels {
		m[l.Key] = l.Value
	}
	return m
}

// -------------------------------------------------------------------------
// CollectorSnapshot — 整个 MetricsCollector 快照
// -------------------------------------------------------------------------

// CollectorSnapshot MetricsCollector 快照。
type CollectorSnapshot struct {
	GeneratedAtNS int64               `json:"generated_at_ns"`
	TotalMetrics  int                 `json:"total_metrics"`
	Histograms    []HistogramSnapshot `json:"histograms"`
}

// Snapshot 生成整个 MetricsCollector 快照。
func (c *MetricsCollector) Snapshot() CollectorSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := CollectorSnapshot{
		GeneratedAtNS: NowNS(),
		TotalMetrics:  len(c.histograms),
		Histograms:    make([]HistogramSnapshot, 0, len(c.histograms)),
	}
	for k, h := range c.histograms {
		mk := c.keys[k]
		snap.Histograms = append(snap.Histograms, h.Snapshot(mk.Name, mk.Labels))
	}
	return snap
}

// -------------------------------------------------------------------------
// JSON 导出
// -------------------------------------------------------------------------

// JSON 返回 JSON 格式快照（带缩进）。
func (s CollectorSnapshot) JSON() ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}

// JSONCompact 返回紧凑 JSON。
func (s CollectorSnapshot) JSONCompact() ([]byte, error) {
	return json.Marshal(s)
}

// -------------------------------------------------------------------------
// 文本导出
// -------------------------------------------------------------------------

// Text 返回人类可读文本格式快照。
func (s CollectorSnapshot) Text() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "=== Observability Snapshot ===\n")
	fmt.Fprintf(&sb, "GeneratedAt: %d ns\n", s.GeneratedAtNS)
	fmt.Fprintf(&sb, "TotalMetrics: %d\n", s.TotalMetrics)
	for _, h := range s.Histograms {
		fmt.Fprintf(&sb, "\n--- %s", h.Name)
		if len(h.Labels) > 0 {
			sb.WriteString(" {")
			first := true
			for k, v := range h.Labels {
				if !first {
					sb.WriteString(", ")
				}
				first = false
				fmt.Fprintf(&sb, "%s=%s", k, v)
			}
			sb.WriteString("}")
		}
		sb.WriteString(" ---\n")
		fmt.Fprintf(&sb, "  Count: %d\n", h.Count)
		fmt.Fprintf(&sb, "  Sum:   %d ns (%.3f ms)\n", h.SumNS, float64(h.SumNS)/1e6)
		fmt.Fprintf(&sb, "  Mean:  %d ns (%.3f μs)\n", h.MeanNS, float64(h.MeanNS)/1e3)
		fmt.Fprintf(&sb, "  Min:   %d ns\n", h.MinNS)
		fmt.Fprintf(&sb, "  Max:   %d ns\n", h.MaxNS)
		fmt.Fprintf(&sb, "  P50:   %d ns (%.3f μs)\n", h.P50NS, float64(h.P50NS)/1e3)
		fmt.Fprintf(&sb, "  P90:   %d ns (%.3f μs)\n", h.P90NS, float64(h.P90NS)/1e3)
		fmt.Fprintf(&sb, "  P99:   %d ns (%.3f μs)\n", h.P99NS, float64(h.P99NS)/1e3)
		fmt.Fprintf(&sb, "  P99.9: %d ns (%.3f μs)\n", h.P999NS, float64(h.P999NS)/1e3)
		// 桶分布（仅打印非空桶）
		nonEmpty := 0
		for _, b := range h.Buckets {
			if b.Count > 0 {
				nonEmpty++
			}
		}
		if nonEmpty > 0 {
			fmt.Fprintf(&sb, "  Buckets (non-empty %d/%d):\n", nonEmpty, len(h.Buckets))
			cum := int64(0)
			for _, b := range h.Buckets {
				if b.Count > 0 {
					cum += b.Count
					ub := "Inf"
					if b.UpperBoundNS != 9223372036854775807 {
						ub = fmt.Sprintf("%dns", b.UpperBoundNS)
					}
					fmt.Fprintf(&sb, "    <%s: %d (cum %d)\n", ub, b.Count, cum)
				}
			}
		}
	}
	return sb.String()
}

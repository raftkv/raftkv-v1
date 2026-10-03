// =========================================================================
// RaftKV Module04 — 微秒级原生可观测性底座（Latency 直采引擎）
//
// metrics_collector.go — 多标签维度采集器
//
// 设计要点：
//   1. 按 metric name + labels 组合分桶，每个唯一组合一个 *Histogram
//   2. labelKey 采用 "k1=v1,k2=v2" 排序后拼接，保证 (a,b) 与 (b,a) 命中同一桶
//   3. hot path：一次 labelsKey 计算 + 一次 map 查找 + Histogram.ObserveNS
//   4. 读路径（Snapshot/Quantile）加 RLock，写路径（创建 Histogram）加 Lock
//   5. 提供 ForEach 遍历所有 (name, labels) → Histogram，便于聚合导出
//   6. 纯标准库，零外部依赖
//
// 与前序模块协调：
//   - Pipeline：ObserveNS("pipeline_stage", ns, L("stage","process"), L("pipeline_id","p1"))
//   - Raft：ObserveNS("raft_rpc", ns, L("rpc","append_entries"), L("peer","node2"))
//   - WAL：ObserveNS("wal_append", ns, L("storage","sm4"), L("batch","256"))
// =========================================================================

package observability

import (
	"sort"
	"strings"
	"sync"
)

// -------------------------------------------------------------------------
// MetricsCollector — 多标签维度采集器
// -------------------------------------------------------------------------

// MetricsCollector 多标签维度 Latency 采集器。
//
// 行为：
//   - ObserveNS(name, ns, labels...)：记录一次耗时到 (name, labels) 对应的 Histogram
//   - Get(name, labels...)：获取对应 Histogram（只读视图）
//   - Snapshot()：导出所有 (name, labels) → Histogram 快照
//   - ForEach(fn)：遍历所有 (name, labels) → Histogram
type MetricsCollector struct {
	mu         sync.RWMutex
	histograms map[string]*Histogram // key = name + "|" + labelsKey
	keys       map[string]MetricKey  // key → (name, labels) 反查
}

// MetricKey 一个 metric 的唯一标识（name + labels）。
type MetricKey struct {
	Name   string
	Labels []Label
}

// NewMetricsCollector 创建采集器。
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		histograms: make(map[string]*Histogram),
		keys:       make(map[string]MetricKey),
	}
}

// labelsKey 计算 labels 的规范化 key（按 Key 排序后拼接）。
func labelsKey(labels []Label) string {
	if len(labels) == 0 {
		return ""
	}
	sorted := make([]Label, len(labels))
	copy(sorted, labels)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
	var sb strings.Builder
	for i, l := range sorted {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(l.Key)
		sb.WriteByte('=')
		sb.WriteString(l.Value)
	}
	return sb.String()
}

// compositeKey 拼接 name + labelsKey。
func compositeKey(name string, labels []Label) string {
	lk := labelsKey(labels)
	if lk == "" {
		return name
	}
	return name + "|" + lk
}

// ObserveNS 记录一次耗时（纳秒）到 (name, labels) 对应的 Histogram。
func (c *MetricsCollector) ObserveNS(name string, ns int64, labels ...Label) {
	key := compositeKey(name, labels)
	c.mu.RLock()
	h, ok := c.histograms[key]
	c.mu.RUnlock()
	if !ok {
		c.mu.Lock()
		h, ok = c.histograms[key]
		if !ok {
			h = NewHistogram()
			c.histograms[key] = h
			c.keys[key] = MetricKey{Name: name, Labels: append([]Label(nil), labels...)}
		}
		c.mu.Unlock()
	}
	h.ObserveNS(ns)
}

// ObserveUS 记录微秒耗时。
func (c *MetricsCollector) ObserveUS(name string, us int64, labels ...Label) {
	c.ObserveNS(name, us*1000, labels...)
}

// Get 获取 (name, labels) 对应的 Histogram，不存在则返回 nil。
func (c *MetricsCollector) Get(name string, labels ...Label) *Histogram {
	key := compositeKey(name, labels)
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.histograms[key]
}

// ForEach 遍历所有 (name, labels) → Histogram。
//
//	遍历期间持 RLock，fn 内不应修改 collector。
func (c *MetricsCollector) ForEach(fn func(key MetricKey, h *Histogram)) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for k, h := range c.histograms {
		fn(c.keys[k], h)
	}
}

// Size 返回 (name, labels) 组合数。
func (c *MetricsCollector) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.histograms)
}

// Reset 重置所有 Histogram（保留分桶结构）。
func (c *MetricsCollector) Reset() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, h := range c.histograms {
		h.Reset()
	}
}

// -------------------------------------------------------------------------
// 聚合查询
// -------------------------------------------------------------------------

// QuantileOf 返回 (name, labels) 对应 Histogram 的分位数（纳秒）。
//
// 不存在返回 -1。
func (c *MetricsCollector) QuantileOf(name string, q float64, labels ...Label) int64 {
	h := c.Get(name, labels...)
	if h == nil {
		return -1
	}
	return h.Quantile(q)
}

// AggregateByName 按 name 聚合所有 labels 的耗时，返回合并后的 Histogram 快照。
//
// 用于查询某 metric 的全局分位数（忽略 label 维度）。
func (c *MetricsCollector) AggregateByName(name string) *Histogram {
	agg := NewHistogram()
	c.mu.RLock()
	defer c.mu.RUnlock()
	for k, h := range c.histograms {
		mk := c.keys[k]
		if mk.Name != name {
			continue
		}
		// 把 h 的 reservoir 样本灌入 agg
		samples, _ := h.reservoir.snapshot()
		for _, s := range samples {
			agg.ObserveNS(s)
		}
	}
	return agg
}

// Names 返回所有 metric name（去重）。
func (c *MetricsCollector) Names() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	seen := make(map[string]bool)
	for _, mk := range c.keys {
		seen[mk.Name] = true
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

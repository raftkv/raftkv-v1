// =========================================================================
// RaftKV Module04 — 微秒级原生可观测性底座（Latency 直采引擎）
//
// trace.go — 轻量级 Trace 埋点（Span 树）
//
// 设计要点：
//   1. 轻量级 Span：仅记录 name + startNS + endNS + parent，无分布式追踪复杂度
//   2. 支持父子 Span（Children 列表），可构建调用链
//   3. Span.End() 自动上报耗时到 MetricsCollector（若绑定）
//   4. 纯内存，无网络/序列化开销，适合微秒级直采
//   5. 纯标准库，零外部依赖
//
// 与前序模块协调：
//   - Pipeline 端到端追踪：root=Begin("pipeline_e2e") → 各 Stage Begin/End → root.End()
//   - Raft RPC 追踪：root=Begin("raft_propose") → replicateAll → advanceCommit
//   - WAL 追踪：root=Begin("wal_append_batch") → encrypt → fsync
// =========================================================================

package observability

import (
	"sync"
	"time"
)

// -------------------------------------------------------------------------
// Span — 轻量级追踪 Span
// -------------------------------------------------------------------------

// Span 轻量级追踪 Span（单 goroutine 使用，非线程安全；End 后不可修改）。
type Span struct {
	Name     string
	StartNS  int64
	EndNS    int64
	Parent   *Span
	Children []*Span

	collector *MetricsCollector
	labels    []Label
	mu        sync.Mutex
	ended     bool
}

// BeginSpan 创建并启动一个 Span。
//
// 若 parent 非 nil，自动加入 parent.Children。
// 若 collector 非 nil，End() 时自动上报耗时。
func BeginSpan(name string, parent *Span, collector *MetricsCollector, labels ...Label) *Span {
	s := &Span{
		Name:      name,
		StartNS:   time.Now().UnixNano(),
		Parent:    parent,
		collector: collector,
		labels:    labels,
	}
	if parent != nil {
		parent.mu.Lock()
		parent.Children = append(parent.Children, s)
		parent.mu.Unlock()
	}
	return s
}

// End 结束 Span，记录 EndNS 并上报到 collector。
//
// 返回耗时纳秒。多次调用仅第一次生效。
func (s *Span) End() int64 {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return s.EndNS - s.StartNS
	}
	s.EndNS = time.Now().UnixNano()
	s.ended = true
	s.mu.Unlock()
	if s.collector != nil {
		s.collector.ObserveNS(s.Name, s.EndNS-s.StartNS, s.labels...)
	}
	return s.EndNS - s.StartNS
}

// DurationNS 返回耗时纳秒（End 后调用）。
func (s *Span) DurationNS() int64 { return s.EndNS - s.StartNS }

// DurationUS 返回耗时微秒。
func (s *Span) DurationUS() int64 { return US(s.DurationNS()) }

// -------------------------------------------------------------------------
// TraceTree — Span 树打印
// -------------------------------------------------------------------------

// TraceTree Span 树的文本表示。
type TraceTree struct {
	Root *Span
}

// NewTraceTree 创建 Span 树。
func NewTraceTree(root *Span) *TraceTree { return &TraceTree{Root: root} }

// Format 文本格式化 Span 树（带缩进）。
func (t *TraceTree) Format() string {
	if t.Root == nil {
		return ""
	}
	var sb []byte
	formatSpan(t.Root, 0, &sb)
	return string(sb)
}

func formatSpan(s *Span, depth int, sb *[]byte) {
	for i := 0; i < depth; i++ {
		*sb = append(*sb, ' ', ' ')
	}
	*sb = append(*sb, s.Name...)
	*sb = append(*sb, " ["...)
	*sb = appendInt64(*sb, s.DurationNS())
	*sb = append(*sb, "ns]"...)
	*sb = append(*sb, '\n')
	for _, c := range s.Children {
		formatSpan(c, depth+1, sb)
	}
}

// appendInt64 简单 int64 → string 追加（避免 strconv 引入，虽然 strconv 是标准库）。
func appendInt64(b []byte, n int64) []byte {
	if n == 0 {
		return append(b, '0')
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return append(b, buf[i:]...)
}

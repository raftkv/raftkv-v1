// =========================================================================
// RaftKV Module04 — 微秒级原生可观测性底座（Latency 直采引擎）
//
// latency.go — 微秒级 Latency 直采器
//
// 设计要点：
//   1. 基于 time.Now().UnixNano() 采集纳秒级时间戳，精度达微秒级（≤1μs 量级误差）
//   2. 单次采集开销 < 100ns（仅一次 UnixNano 调用 + int64 减法）
//   3. 零内存分配 hot path（Now/Since 均无 alloc）
//   4. 提供 Timer 闭包返回式采集，方便 Pipeline/Raft/WAL 各 Stage 埋点
//   5. 纯标准库，零外部依赖（不依赖 prometheus/opentelemetry）
//
// 与前序模块协调：
//   - Module03 Pipeline：可在 Stage.proc 前后调用 Since/ObserveNS 采集端到端延迟
//   - Module01 Raft：可在 ProposeSync/AppendEntries 前后采集共识耗时
//   - Module02 WAL+SM4：可在 Append/fsync 前后采集持久化耗时
// =========================================================================

package observability

import (
	"sync/atomic"
	"time"
)

// -------------------------------------------------------------------------
// 纳秒级时间戳原语
// -------------------------------------------------------------------------

// NowNS 返回当前纳秒级时间戳（等价于 time.Now().UnixNano()，但语义更明确）。
//
// 开销：单次约 20-40ns（Windows/Linux），无内存分配。
func NowNS() int64 {
	return time.Now().UnixNano()
}

// SinceNS 返回从 start 到当前的纳秒耗时。
//
// 开销：一次 UnixNano + 一次 int64 减法，约 25-50ns，无内存分配。
func SinceNS(start int64) int64 {
	return time.Now().UnixNano() - start
}

// US 返回纳秒 → 微秒（向上取整）。
func US(ns int64) int64 {
	if ns < 0 {
		return 0
	}
	return (ns + 999) / 1000
}

// MS 返回纳秒 → 毫秒（向上取整）。
func MS(ns int64) int64 {
	if ns < 0 {
		return 0
	}
	return (ns + 999_999) / 1_000_000
}

// -------------------------------------------------------------------------
// LatencySampler — 微秒级延迟直采器
// -------------------------------------------------------------------------

// LatencySampler 微秒级延迟直采器。
//
// 行为：
//   - Start() 返回纳秒时间戳（hot path，无 alloc）
//   - Stop(start) 返回纳秒耗时并累计计数（hot path，无 alloc）
//   - Count() 返回累计采样次数
//   - 单实例线程安全（原子计数）
//
// 使用示例：
//
//	s := NewLatencySampler()
//	t0 := s.Start()
//	... do work ...
//	elapsed := s.Stop(t0)  // 纳纳秒
type LatencySampler struct {
	count atomic.Int64 // 累计采样次数
	maxNS atomic.Int64 // 历史最大耗时（纳秒）
	minNS atomic.Int64 // 历史最小耗时（纳秒），0 表示未初始化
}

// NewLatencySampler 创建延迟直采器。
func NewLatencySampler() *LatencySampler {
	return &LatencySampler{}
}

// Start 返回纳秒时间戳（hot path）。
func (s *LatencySampler) Start() int64 {
	return time.Now().UnixNano()
}

// Stop 记录耗时并返回纳秒值（hot path）。
func (s *LatencySampler) Stop(startNS int64) int64 {
	elapsed := time.Now().UnixNano() - startNS
	if elapsed < 0 {
		elapsed = 0
	}
	s.count.Add(1)
	// 维护 min/max（无锁，可能轻微竞争，但可观测性场景可接受）
	for {
		cur := s.maxNS.Load()
		if elapsed <= cur || s.maxNS.CompareAndSwap(cur, elapsed) {
			break
		}
	}
	for {
		cur := s.minNS.Load()
		if cur != 0 && elapsed >= cur {
			break
		}
		if s.minNS.CompareAndSwap(cur, elapsed) {
			break
		}
	}
	return elapsed
}

// Count 返回累计采样次数。
func (s *LatencySampler) Count() int64 { return s.count.Load() }

// MaxNS 返回历史最大耗时（纳秒）。
func (s *LatencySampler) MaxNS() int64 { return s.maxNS.Load() }

// MinNS 返回历史最小耗时（纳秒）。
func (s *LatencySampler) MinNS() int64 { return s.minNS.Load() }

// Reset 重置计数器与 min/max，返回旧 count。
func (s *LatencySampler) Reset() int64 {
	old := s.count.Swap(0)
	s.maxNS.Store(0)
	s.minNS.Store(0)
	return old
}

// -------------------------------------------------------------------------
// Timer — 闭包返回式采集（方便 Pipeline Stage 埋点）
// -------------------------------------------------------------------------

// Timer 一次性计时器，方便在函数入口创建、defer End() 采集。
//
// 使用示例：
//
//	defer observability.NewTimer(collector, "endpoint", "/api/foo").End()
type Timer struct {
	startNS   int64
	collector *MetricsCollector
	name      string
	labels    []Label
}

// Label 单个标签键值对。
type Label struct {
	Key   string
	Value string
}

// L 构造 Label 的便捷函数。
//
//	L("endpoint", "/api/foo")
func L(key, val string) Label { return Label{Key: key, Value: val} }

// NewTimer 创建 Timer，绑定到 collector 与 name/labels。
//
// 若 collector 为 nil，End() 将无副作用（仅计时）。
func NewTimer(collector *MetricsCollector, name string, labels ...Label) *Timer {
	return &Timer{
		startNS:   time.Now().UnixNano(),
		collector: collector,
		name:      name,
		labels:    labels,
	}
}

// End 记录耗时到 collector 并返回纳秒值。
func (t *Timer) End() int64 {
	elapsed := time.Now().UnixNano() - t.startNS
	if elapsed < 0 {
		elapsed = 0
	}
	if t.collector != nil {
		t.collector.ObserveNS(t.name, elapsed, t.labels...)
	}
	return elapsed
}

// EndUS 返回微秒耗时。
func (t *Timer) EndUS() int64 { return US(t.End()) }

// =========================================================================
// RaftKV Module03 — 高吞吐数据流转 Pipeline
//
// Metrics 吞吐量统计器：实时计算 TPS（每秒处理条数）。
//
// 设计要点：
//   1. 基于 atomic 计数器，零锁高并发友好
//   2. 后台 goroutine 周期性快照，计算窗口 TPS
//   3. 提供累计 TPS 与瞬时 TPS 两种口径
//   4. 可挂载到 Pipeline 任意 Stage（计数 in/out/drop）
//
// 零外部依赖：仅使用 Go 标准库
// =========================================================================

package pipeline

import (
	"sync/atomic"
	"time"
)

// -------------------------------------------------------------------------
// Counter 原子计数器
// -------------------------------------------------------------------------

// Counter 原子 int64 计数器。
type Counter struct {
	v atomic.Int64
}

// Inc 自增 1。
func (c *Counter) Inc() int64 { return c.v.Add(1) }

// Add 增加delta。
func (c *Counter) Add(delta int64) int64 { return c.v.Add(delta) }

// Load 当前值。
func (c *Counter) Load() int64 { return c.v.Load() }

// Reset 重置为 0，返回旧值。
func (c *Counter) Reset() int64 { return c.v.Swap(0) }

// -------------------------------------------------------------------------
// TPSMeter 吞吐量统计器
// -------------------------------------------------------------------------

// TPSMeter 吞吐量统计器。
//
// 行为：
//   - 通过 Add(n) 累计处理条数
//   - 后台 goroutine 每 window 周期快照，计算瞬时 TPS
//   - Stop 后停止采样
type TPSMeter struct {
	total   atomic.Int64
	last    atomic.Int64 // 上次采样时的 total
	instTPS atomic.Int64 // 瞬时 TPS（最近一个 window）

	window time.Duration
	stopCh chan struct{}
}

// NewTPSMeter 创建吞吐量统计器。
//
//   - window: 采样窗口（如 1s），<1ms 时修正为 1ms
func NewTPSMeter(window time.Duration) *TPSMeter {
	if window < time.Millisecond {
		window = time.Millisecond
	}
	return &TPSMeter{
		window: window,
		stopCh: make(chan struct{}),
	}
}

// Start 启动后台采样 goroutine。
func (m *TPSMeter) Start() {
	go m.run()
}

// Add 累计处理条数（线程安全）。
func (m *TPSMeter) Add(n int64) { m.total.Add(n) }

// Inc 累计 +1。
func (m *TPSMeter) Inc() { m.total.Add(1) }

// Total 返回累计总数。
func (m *TPSMeter) Total() int64 { return m.total.Load() }

// InstantTPS 返回最近一个 window 的瞬时 TPS。
func (m *TPSMeter) InstantTPS() int64 { return m.instTPS.Load() }

// Stop 停止采样。
func (m *TPSMeter) Stop() { close(m.stopCh) }

func (m *TPSMeter) run() {
	ticker := time.NewTicker(m.window)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			now := m.total.Load()
			prev := m.last.Swap(now)
			m.instTPS.Store(now - prev)
		}
	}
}

// -------------------------------------------------------------------------
// E2EStats 端到端统计
// -------------------------------------------------------------------------

// E2EStats 端到端统计：用于校验零丢失与顺序性。
//
// 访问模式约定：
//   - RecordSent/RecordRecv 由生产/消费 goroutine 调用
//   - Finalize/Pass 由主 goroutine 在所有 worker 退出后调用（happens-after）
//   - 因此非原子字段在 wg.Wait() 后读取是安全的
type E2EStats struct {
	Sent       int64 // 生产端发送数
	Received   int64 // 消费端接收数
	Lost       int64 // 丢失数（Sent - Received，应=0）
	Duplicated int64 // 重复数
	OutOfOrder int64 // 乱序数

	// 顺序性校验：记录最后一个收到的 ID，下一个应 = lastID+1
	lastID    int64
	orderInit bool
}

// RecordSent 生产端记录发送。
func (s *E2EStats) RecordSent() { s.Sent++ }

// RecordRecv 消费端记录接收，并校验顺序性（按 ID 递增）。
//
//   - 若 item.ID != lastID+1，记为乱序
//   - 首次调用不校验
func (s *E2EStats) RecordRecv(item Item) {
	s.Received++
	if !s.orderInit {
		s.lastID = int64(item.ID)
		s.orderInit = true
		return
	}
	if int64(item.ID) != s.lastID+1 {
		if int64(item.ID) <= s.lastID {
			s.Duplicated++
		} else {
			s.OutOfOrder++
		}
	}
	s.lastID = int64(item.ID)
}

// Finalize 计算 Lost。
func (s *E2EStats) Finalize() {
	s.Lost = s.Sent - s.Received
}

// Pass 是否通过（零丢失、零重复、零乱序）。
func (s *E2EStats) Pass() bool {
	return s.Lost == 0 && s.Duplicated == 0 && s.OutOfOrder == 0
}

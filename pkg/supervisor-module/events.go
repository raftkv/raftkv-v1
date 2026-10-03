// =========================================================================
// RaftKV Module05 — 工业级自愈与进程守护系统
//
// 文件：events.go
// 职责：EventListener 接口（扩展点）+ noopEventListener 空实现 +
//       fireOnXxx 回调触发（独立 goroutine + recover 兜底）
//
// 设计要点：
//   - EventListener 由 Module04 可观测性底座实现，本模块仅调用，不 import Module04
//   - 所有回调在独立 goroutine 中执行，defer recover() 兜底
//   - 回调 panic 不影响守护系统自身（spec.md 4.2.1）
//   - 不在持锁状态调用回调（design.md 2.5.5 死锁防护）
//
// 与前序模块协调：沿用接口/回调衔接约定（Transport / Pipeline.Output）
// 零依赖声明：仅 sync、time
// =========================================================================

package selfheal

import (
	"sync"
	"time"
)

// EventListener 自愈事件监听器接口。
//
// 由 Module04 可观测性底座实现，本模块仅调用，不 import Module04。
type EventListener interface {
	// OnWorkerCrash Worker 崩溃事件回调。
	OnWorkerCrash(evt CrashEvent)
	// OnWorkerRestart Worker 重启事件回调。
	OnWorkerRestart(evt RestartEvent)
	// OnHeartbeatTimeout 心跳超时事件回调。
	OnHeartbeatTimeout(evt TimeoutEvent)
	// OnPermanentStop 永久停机告警事件回调。
	OnPermanentStop(evt StopEvent)
	// OnStateTransition 状态流转事件回调。
	OnStateTransition(evt TransitionEvent)
}

// noopEventListener 空实现，作为默认 listener。
type noopEventListener struct{}

func (noopEventListener) OnWorkerCrash(evt CrashEvent)          {}
func (noopEventListener) OnWorkerRestart(evt RestartEvent)      {}
func (noopEventListener) OnHeartbeatTimeout(evt TimeoutEvent)   {}
func (noopEventListener) OnPermanentStop(evt StopEvent)         {}
func (noopEventListener) OnStateTransition(evt TransitionEvent) {}

// getListener 原子读取当前 listener，若未设置则返回 noop。
func (s *Supervisor) getListener() EventListener {
	v := s.listener.Load()
	if v == nil {
		return noopEventListener{}
	}
	l, ok := v.(EventListener)
	if !ok || l == nil {
		return noopEventListener{}
	}
	return l
}

// fireAsync 在独立 goroutine 中执行回调，recover 兜底防止 panic 影响守护系统。
func fireAsync(fn func()) {
	go func() {
		defer func() { _ = recover() }()
		fn()
	}()
}

// -------------------------------------------------------------------------
// fireOnXxx 回调触发方法
// -------------------------------------------------------------------------

// fireOnWorkerCrash 触发 Worker 崩溃事件回调。
func (s *Supervisor) fireOnWorkerCrash(w *Worker, reason CrashReason) {
	evt := CrashEvent{
		WorkerName:   w.def.Name,
		Reason:       reason,
		RestartCount: w.restartCount.Load(),
		Timestamp:    time.Now(),
	}
	l := s.getListener()
	fireAsync(func() { l.OnWorkerCrash(evt) })
}

// fireOnWorkerRestart 触发 Worker 重启事件回调。
func (s *Supervisor) fireOnWorkerRestart(w *Worker, reason CrashReason, backoff time.Duration) {
	evt := RestartEvent{
		WorkerName:   w.def.Name,
		Reason:       reason,
		RestartCount: w.restartCount.Load(),
		Backoff:      backoff,
		Timestamp:    time.Now(),
	}
	l := s.getListener()
	fireAsync(func() { l.OnWorkerRestart(evt) })
}

// fireOnHeartbeatTimeout 触发心跳超时事件回调。
func (s *Supervisor) fireOnHeartbeatTimeout(w *Worker, lastHB time.Time, elapsed time.Duration) {
	evt := TimeoutEvent{
		WorkerName:      w.def.Name,
		LastHeartbeatAt: lastHB,
		Elapsed:         elapsed,
		RestartCount:    w.restartCount.Load(),
		Timestamp:       time.Now(),
	}
	l := s.getListener()
	fireAsync(func() { l.OnHeartbeatTimeout(evt) })
}

// fireOnPermanentStop 触发永久停机告警事件回调。
func (s *Supervisor) fireOnPermanentStop(w *Worker, reason CrashReason) {
	evt := StopEvent{
		WorkerName:   w.def.Name,
		Reason:       reason,
		RestartCount: w.restartCount.Load(),
		Timestamp:    time.Now(),
	}
	l := s.getListener()
	fireAsync(func() { l.OnPermanentStop(evt) })
}

// fireOnStateTransition 触发状态流转事件回调。
func (s *Supervisor) fireOnStateTransition(w *Worker, from, to WorkerState) {
	evt := TransitionEvent{
		WorkerName: w.def.Name,
		From:       from,
		To:         to,
		Timestamp:  time.Now(),
	}
	l := s.getListener()
	fireAsync(func() { l.OnStateTransition(evt) })
}

// _ 确保 sync 包被引用（未来可能用 WaitGroup 等待回调完成）
var _ = sync.WaitGroup{}

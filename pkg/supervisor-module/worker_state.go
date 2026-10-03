// =========================================================================
// RaftKV Module05 — 工业级自愈与进程守护系统
//
// 文件：worker_state.go
// 职责：Worker 五态状态机 — String / canTransition 合法流转矩阵 /
//       applyTransition CAS 原子流转
//
// 设计要点：
//   - 五态：Running / Crashed / Restarting / Unhealthy / Stopped
//   - Stopped 为终态，封闭不再流转
//   - 禁止跳转：Running→Restarting、Crashed→Running、Unhealthy→Running、Stopped→任意
//   - 用 atomic.CompareAndSwapInt32 实现无锁 CAS，并发不撕裂（spec.md 5.6.3）
//
// 与前序模块协调：沿用 raft-module NodeState + String 模式
// 零依赖声明：仅 sync/atomic、fmt
// =========================================================================

package selfheal

import (
	"fmt"
)

// String 返回 WorkerState 的可读字符串。
func (s WorkerState) String() string {
	switch s {
	case StateRunning:
		return "Running"
	case StateCrashed:
		return "Crashed"
	case StateRestarting:
		return "Restarting"
	case StateUnhealthy:
		return "Unhealthy"
	case StateStopped:
		return "Stopped"
	default:
		return "Unknown"
	}
}

// canTransition 根据 design.md 2.1.3.1 流转矩阵校验 from → to 是否合法。
//
// 合法流转矩阵（true 表示允许）：
//
//	from \ to  | Running | Crashed | Restarting | Unhealthy | Stopped
//	-----------|---------|---------|------------|-----------|--------
//	Running    |   -     |  true   |   false    |   true    |  true
//	Crashed    |  false  |   -     |   true     |  false    |  true
//	Restarting |  true   |  true   |   -        |  false    |  true
//	Unhealthy  |  false  |  false  |   true     |   -       |  true
//	Stopped    |  false  |  false  |   false    |  false    |   -
func canTransition(from, to WorkerState) bool {
	switch from {
	case StateRunning:
		return to == StateCrashed || to == StateUnhealthy || to == StateStopped
	case StateCrashed:
		return to == StateRestarting || to == StateStopped
	case StateRestarting:
		return to == StateRunning || to == StateCrashed || to == StateStopped
	case StateUnhealthy:
		return to == StateRestarting || to == StateStopped
	case StateStopped:
		return false // 终态封闭
	default:
		return false
	}
}

// applyTransition 执行带校验的原子状态流转。
//
// 使用 atomic.CompareAndSwapInt32 实现无锁 CAS：
//   - 流转前调用 canTransition 校验，非法流转返回 ErrIllegalTransition
//   - CAS 失败（并发流转）则重读当前状态重试
//   - 流转成功后通过 Worker.sup 发布 TransitionEvent（若 listener 非空）
//
// 满足 spec.md 5.6.3 并发状态查询不撕裂。
func (w *Worker) applyTransition(to WorkerState) error {
	for {
		old := WorkerState(w.state.Load())
		if !canTransition(old, to) {
			return fmt.Errorf("%w: %s → %s", ErrIllegalTransition, old, to)
		}
		if w.state.CompareAndSwap(int32(old), int32(to)) {
			// CAS 成功，发布状态流转事件
			if w.sup != nil {
				w.sup.fireOnStateTransition(w, old, to)
			}
			return nil
		}
		// CAS 失败（其他 goroutine 并发流转），重试
	}
}

// tryTransition 尝试一次 CAS 流转，不重试。返回是否成功。
// 用于 Watchdog 巡检中避免阻塞：若并发冲突则放弃本轮处理。
func (w *Worker) tryTransition(to WorkerState) bool {
	old := WorkerState(w.state.Load())
	if !canTransition(old, to) {
		return false
	}
	if w.state.CompareAndSwap(int32(old), int32(to)) {
		if w.sup != nil {
			w.sup.fireOnStateTransition(w, old, to)
		}
		return true
	}
	return false
}

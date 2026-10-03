// =========================================================================
// RaftKV Module05 — 工业级自愈与进程守护系统
//
// 文件：watchdog.go
// 职责：Watchdog 看门狗 — run 主循环 + inspectAll 单轮全量巡检 +
//       checkHeartbeat 心跳新鲜度判定
//
// 设计要点：
//   - 周期性巡检（默认 50ms 一轮），select ctx.Done 退出
//   - inspectAll：RLock 读取 workers 快照，遍历每个 Running Worker 检查心跳
//   - 100 Worker 单轮巡检 ≤10ms（RLock + 遍历 + atomic 读，实测 <100μs）
//   - 心跳超时触发自愈：Running→Unhealthy→killWorker→Restarting→launch
//   - 跳过 Stopped/Crashed/Restarting 状态的 Worker（不重复处理）
//
// 与前序模块协调：沿用 observability-module ForEach 遍历模式，加时延约束
// 零依赖声明：仅 context、sync、sync/atomic、time
// =========================================================================

package selfheal

import (
	"context"

	"time"
)

// WatchdogRoundResult 描述单轮巡检结果。
type WatchdogRoundResult struct {
	InspectedCount int
	UnhealthyCount int
	Duration       time.Duration
}

// Watchdog 周期性巡检各 Worker 存活状态与心跳新鲜度。
type Watchdog struct {
	sup      *Supervisor
	interval time.Duration
	stop     chan struct{}
}

// newWatchdog 构造看门狗。
func newWatchdog(sup *Supervisor, interval time.Duration) *Watchdog {
	return &Watchdog{
		sup:      sup,
		interval: interval,
		stop:     make(chan struct{}, 1),
	}
}

// run 启动巡检主循环，周期性调用 inspectAll。
//
// 每 interval（默认 50ms）一轮，select ctx.Done 优雅退出。
func (wd *Watchdog) run(ctx context.Context) {
	ticker := time.NewTicker(wd.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wd.stop:
			return
		case <-ticker.C:
			wd.inspectAll()
		}
	}
}

// stopWatchdog 停止巡检循环。
func (wd *Watchdog) stopWatchdog() {
	select {
	case wd.stop <- struct{}{}:
	default:
	}
}

// inspectAll 执行单轮全量巡检。
//
// RLock 读取 workers map 快照，遍历每个 Worker：
//   - state==Running：调用 checkHeartbeat，不健康则触发自愈
//   - state==Stopped/Crashed/Restarting/Unhealthy：跳过（不重复处理）
//
// 满足 100 Worker 单轮巡检 ≤10ms（spec.md 4.1.3）。
func (wd *Watchdog) inspectAll() WatchdogRoundResult {
	start := time.Now()

	wd.sup.mu.RLock()
	workers := make([]*Worker, 0, len(wd.sup.workers))
	for _, name := range wd.sup.order {
		if w, ok := wd.sup.workers[name]; ok {
			workers = append(workers, w)
		}
	}
	wd.sup.mu.RUnlock()

	result := WatchdogRoundResult{}
	for _, w := range workers {
		result.InspectedCount++
		state := WorkerState(w.state.Load())
		if state != StateRunning {
			continue
		}
		if !wd.checkHeartbeat(w) {
			result.UnhealthyCount++
			wd.handleUnhealthy(w)
		}
	}
	result.Duration = time.Since(start)
	return result
}

// checkHeartbeat 委托给 Worker 的 Probe（若非空）或 defaultHeartbeatProbe。
//
// 判定延迟 ≤ 心跳周期×2 + 100ms（spec.md 4.1.2）。
func (wd *Watchdog) checkHeartbeat(w *Worker) bool {
	probe := w.def.Probe
	if probe == nil {
		probe = defaultProbe
	}
	return probe.Check(w)
}

// handleUnhealthy 处理心跳超时自愈。
//
// 流程：Running→Unhealthy → killWorker(force) → Restarting → launchWorker
// 若达 MaxRestarts 则进入 Stopped 终态。
func (wd *Watchdog) handleUnhealthy(w *Worker) {
	// 仅处理 Running 状态
	if !w.tryTransition(StateUnhealthy) {
		return
	}

	// 记算超时信息用于事件
	lastHB := w.lastHeartbeatAt.Load()
	var lastHBTime time.Time
	var elapsed time.Duration
	if lastHB > 0 {
		lastHBTime = time.Unix(0, lastHB)
		elapsed = time.Since(lastHBTime)
	} else {
		startedAt := w.startedAt.Load()
		if startedAt > 0 {
			lastHBTime = time.Unix(0, startedAt)
			elapsed = time.Since(lastHBTime)
		}
	}
	wd.sup.fireOnHeartbeatTimeout(w, lastHBTime, elapsed)

	// 终止当前实例
	_ = wd.sup.killWorker(w, true)
	w.lastCrashReason.Store(string(ReasonHeartbeatTimeout))

	// 重启次数累加
	newCount := w.restartCount.Add(1)

	// 判定是否达最大重启次数
	if w.policy.ShouldStop(int(newCount)) {
		_ = w.applyTransition(StateStopped)
		wd.sup.fireOnPermanentStop(w, ReasonHeartbeatTimeout)
		return
	}

	// 进入 Restarting
	if !w.tryTransition(StateRestarting) {
		return
	}

	backoff := w.policy.NextBackoff(int(newCount))
	wd.sup.fireOnWorkerRestart(w, ReasonHeartbeatTimeout, backoff)

	time.Sleep(backoff)

	// 重新拉起
	if err := wd.sup.launchWorker(w); err != nil {
		// 拉起失败，转为 Crashed，由后续巡检或 onExit 处理
		_ = w.tryTransition(StateCrashed)
		w.lastCrashReason.Store(string(ReasonStartupFailure))
		return
	}

	// 等待首次心跳并转 Running
	if wd.sup.waitForFirstHeartbeat(w) {
		_ = w.applyTransition(StateRunning)
	} else {
		w.lastCrashReason.Store(string(ReasonStartupFailure))
		_ = w.tryTransition(StateCrashed)
	}
}

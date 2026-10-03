// =========================================================================
// RaftKV Module05 — 工业级自愈与进程守护系统
//
// 文件：supervisor.go
// 职责：Supervisor 核心 — New/Start/Stop 生命周期 + AddWorker/RemoveWorker 注册 +
//       Snapshot 快照 + ReportHeartbeat 心跳 + SetEventListener +
//       onExit 崩溃自愈编排 + shutdown 优雅关闭
//
// 设计要点：
//   - New：校验 cfg，构造 Supervisor，state=Active，workers 空 map
//   - Start：启动 Watchdog + SignalHandler goroutine
//   - Stop：CAS state→ShuttingDown，并发停止所有 Worker，返回 GracefulShutdownResult
//   - AddWorker：校验唯一性，构造 Worker + RestartPolicy，launchWorker，转 Running
//   - onExit：崩溃自愈编排 Running→Crashed→Restarting→Running 或 Stopped
//   - shutdown：并发停止，超时强杀，检查残留=0
//
// 与前序模块协调：沿用 pipeline-module Close 排空 + raft-module 状态机模式
// 零依赖声明：仅 context、sync、sync/atomic、time、errors、fmt
// =========================================================================

package selfheal

import (
	"context"
	"fmt"
	"sync"

	"time"
)

// New 构造守护器。
//
// 前置条件：cfg 经 validate() 校验通过。
// 后置条件：返回未启动的 Supervisor，supervisorState=Active，workers 为空。
func New(cfg SupervisorConfig) (*Supervisor, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &Supervisor{
		cfg:     cfg,
		workers: make(map[string]*Worker),
		order:   make([]string, 0),
	}
	s.state.Store(int32(StateSupervisorActive))
	s.listener.Store(noopEventListener{})
	return s, nil
}

// Start 启动守护器，开始监听信号与运行 Watchdog。
//
// 前置条件：Supervisor 未启动。
// 后置条件：Watchdog goroutine 启动，SignalHandler goroutine 启动。
func (s *Supervisor) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}

	// 内部 ctx，用于控制 Watchdog 和 SignalHandler 生命周期
	internalCtx, cancel := context.WithCancel(ctx)
	s.cancelInternal = cancel

	// 启动 Watchdog
	s.watchdog = newWatchdog(s, s.cfg.WatchdogInterval)
	go s.watchdog.run(internalCtx)

	// 启动 SignalHandler
	s.sigHandler = newSignalHandler()
	go s.sigHandler.listenSignal(internalCtx, func() {
		_, _ = s.Stop(context.Background())
	})

	return nil
}

// Stop 发起优雅关闭，逐个停止所有 Worker 并返回关闭结果。
//
// 前置条件：Supervisor 已启动。
// 后置条件：supervisorState=Stopped，所有 Worker 已退出，residualCount=0。
func (s *Supervisor) Stop(ctx context.Context) (GracefulShutdownResult, error) {
	if !s.started.Load() {
		return GracefulShutdownResult{}, ErrNotStarted
	}

	var result GracefulShutdownResult
	var err error
	s.stopOnce.Do(func() {
		result = s.shutdown(ctx)
		// 停止内部 goroutine
		if s.cancelInternal != nil {
			s.cancelInternal()
		}
		if s.watchdog != nil {
			s.watchdog.stopWatchdog()
		}
		s.state.Store(int32(StateSupervisorStopped))
	})
	if result.ResidualCount > 0 {
		err = fmt.Errorf("selfheal: %d residual workers after shutdown", result.ResidualCount)
	}
	return result, err
}

// AddWorker 注册并立即拉起 Worker。
//
// 前置条件：supervisorState=Active；def.Name 唯一；def 经 validate() 通过。
// 后置条件：Worker 在 100ms 内进入 Running 并开始上报心跳。
func (s *Supervisor) AddWorker(def WorkerDef) error {
	if SupervisorState(s.state.Load()) != StateSupervisorActive {
		return ErrShuttingDown
	}
	if !s.started.Load() {
		return ErrNotStarted
	}

	// 填充默认值
	if def.HeartbeatInterval == 0 {
		def.HeartbeatInterval = s.cfg.DefaultHeartbeatInterval
	}
	if def.HeartbeatTimeout == 0 {
		def.HeartbeatTimeout = s.cfg.DefaultHeartbeatTimeout
	}
	if def.MaxRestarts == 0 && s.cfg.DefaultMaxRestarts > 0 {
		def.MaxRestarts = s.cfg.DefaultMaxRestarts
	}
	if def.BackoffBase == 0 {
		def.BackoffBase = s.cfg.DefaultBackoffBase
	}
	if def.BackoffMax == 0 {
		def.BackoffMax = s.cfg.DefaultBackoffMax
	}
	if def.ShutdownTimeout == 0 {
		def.ShutdownTimeout = s.cfg.DefaultShutdownTimeout
	}
	if def.Mode < 0 {
		def.Mode = s.cfg.WorkerMode
	}

	if err := def.validate(); err != nil {
		return err
	}

	s.mu.Lock()
	if _, exists := s.workers[def.Name]; exists {
		s.mu.Unlock()
		return ErrWorkerExists
	}

	w := &Worker{
		def:    def,
		policy: newRestartPolicy(def),
		sup:    s,
	}
	w.state.Store(int32(StateRunning))
	s.workers[def.Name] = w
	s.order = append(s.order, def.Name)
	s.mu.Unlock()

	if err := s.launchWorker(w); err != nil {
		// 拉起失败，移除并返回错误
		s.mu.Lock()
		delete(s.workers, def.Name)
		if len(s.order) > 0 && s.order[len(s.order)-1] == def.Name {
			s.order = s.order[:len(s.order)-1]
		}
		s.mu.Unlock()
		return err
	}
	return nil
}

// RemoveWorker 移除并停止指定 Worker。
func (s *Supervisor) RemoveWorker(name string) error {
	s.mu.Lock()
	w, ok := s.workers[name]
	if !ok {
		s.mu.Unlock()
		return ErrWorkerNotFound
	}
	delete(s.workers, name)
	// 从 order 移除
	for i, n := range s.order {
		if n == name {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	// 标记优雅退出，避免 onExit 触发重启
	w.graceful.Store(true)
	_ = s.killWorker(w, false)
	_ = w.applyTransition(StateStopped)
	return nil
}

// Snapshot 生成守护状态快照。
//
// 无锁撕裂：RLock 读取 map，每 Worker 用 atomic 读状态。
func (s *Supervisor) Snapshot() SupervisorSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := SupervisorSnapshot{
		SupervisorState: SupervisorState(s.state.Load()).String(),
		SnapshotAt:      time.Now(),
		Workers:         make([]WorkerSnapshot, 0, len(s.workers)),
	}

	for _, name := range s.order {
		w := s.workers[name]
		if w == nil {
			continue
		}
		ws := WorkerSnapshot{
			Name:             w.def.Name,
			State:            WorkerState(w.state.Load()).String(),
			RestartCount:     w.restartCount.Load(),
			StartedAt:        time.Unix(0, w.startedAt.Load()),
			CumulativeUptime: time.Duration(w.cumulativeUptime.Load()),
		}
		lastHB := w.lastHeartbeatAt.Load()
		if lastHB > 0 {
			ws.LastHeartbeatAt = time.Unix(0, lastHB)
		}
		if r := w.lastCrashReason.Load(); r != nil {
			ws.LastCrashReason = r.(string)
		}
		snap.Workers = append(snap.Workers, ws)
		snap.TotalRestarts += ws.RestartCount
		if ws.State == "Unhealthy" {
			snap.UnhealthyCount++
		}
		if ws.State == "Stopped" {
			snap.StoppedCount++
		}
	}
	return snap
}

// ReportHeartbeat Worker 心跳上报入口。
//
// atomic 写入 lastHeartbeatAt = now。未注册名静默忽略。
func (s *Supervisor) ReportHeartbeat(name string) {
	s.mu.RLock()
	w, ok := s.workers[name]
	s.mu.RUnlock()
	if !ok {
		return
	}
	w.lastHeartbeatAt.Store(time.Now().UnixNano())
}

// SetEventListener 注入事件监听器（衔接 Module04）。
func (s *Supervisor) SetEventListener(listener EventListener) {
	if listener == nil {
		listener = noopEventListener{}
	}
	s.listener.Store(listener)
}

// -------------------------------------------------------------------------
// onExit：崩溃自愈编排（design.md 2.1.3.2）
// -------------------------------------------------------------------------

// onExit 处理 Worker 退出事件，执行崩溃自愈编排。
//
// 流程：
//  1. 若 supervisor 非 Active 或 Worker 已 Stopped 或 graceful → 不处理
//  2. 若状态非 Running/Restarting → 不处理（已被 Watchdog 处理）
//  3. applyTransition(Crashed)，restartCount++，fireOnWorkerCrash
//  4. 若 ShouldStop → applyTransition(Stopped) + fireOnPermanentStop，返回
//  5. applyTransition(Restarting) → backoff → fireOnWorkerRestart → sleep → launch
//  6. 等待首次心跳：收到 → Running；否则 → Crashed 递归
func (s *Supervisor) onExit(w *Worker, reason CrashReason) {
	// 关闭模式或终态，不处理
	if SupervisorState(s.state.Load()) != StateSupervisorActive {
		return
	}
	if WorkerState(w.state.Load()) == StateStopped {
		return
	}
	if w.graceful.Load() {
		return
	}

	// 仅处理 Running / Restarting 状态的退出
	curState := WorkerState(w.state.Load())
	if curState != StateRunning && curState != StateRestarting {
		return
	}

	// 流转到 Crashed
	if err := w.applyTransition(StateCrashed); err != nil {
		return
	}
	w.lastCrashReason.Store(string(reason))

	// 重启次数累加
	newCount := w.restartCount.Add(1)
	s.fireOnWorkerCrash(w, reason)

	// 判定是否达最大重启次数
	if w.policy.ShouldStop(int(newCount)) {
		_ = w.applyTransition(StateStopped)
		s.fireOnPermanentStop(w, reason)
		return
	}

	// 进入 Restarting
	if err := w.applyTransition(StateRestarting); err != nil {
		return
	}

	backoff := w.policy.NextBackoff(int(newCount))
	s.fireOnWorkerRestart(w, reason, backoff)

	time.Sleep(backoff)

	// 检查 supervisor 是否仍在 Active（防止关闭期间继续重启）
	if SupervisorState(s.state.Load()) != StateSupervisorActive {
		return
	}

	// 重新拉起
	if err := s.launchWorker(w); err != nil {
		// 拉起失败，转为 Crashed 递归处理
		w.lastCrashReason.Store(string(ReasonStartupFailure))
		s.onExit(w, ReasonStartupFailure)
		return
	}

	// 等待首次心跳（≤HeartbeatTimeout）
	if s.waitForFirstHeartbeat(w) {
		_ = w.applyTransition(StateRunning)
	} else {
		// 未收到首次心跳，视为启动失败
		w.lastCrashReason.Store(string(ReasonStartupFailure))
		s.onExit(w, ReasonStartupFailure)
	}
}

// waitForFirstHeartbeat 等待 Worker 上报首次心跳，超时返回 false。
//
// 轮询检查 lastHeartbeatAt 是否非零，超时返回 false。
func (s *Supervisor) waitForFirstHeartbeat(w *Worker) bool {
	deadline := time.Now().Add(w.def.HeartbeatTimeout)
	for time.Now().Before(deadline) {
		if w.lastHeartbeatAt.Load() > 0 {
			return true
		}
		// 检查 Worker 是否已退出（done 已关闭）
		select {
		case <-w.done:
			return false
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	return w.lastHeartbeatAt.Load() > 0
}

// -------------------------------------------------------------------------
// shutdown：优雅关闭流程（design.md 2.1.3.4）
// -------------------------------------------------------------------------

// shutdown 执行优雅关闭。
//
// 流程：
//  1. CAS state Active→ShuttingDown（拒绝新注册）
//  2. RLock 获取所有 Worker 快照（按 order 顺序）
//  3. 并发停止：每 Worker 一个 goroutine，cancel + 等待 done 或 shutdownTimeout 超时强杀
//  4. 收集 PerWorker 详情
//  5. 检查残留（必须为 0）
//  6. 返回 GracefulShutdownResult
func (s *Supervisor) shutdown(ctx context.Context) GracefulShutdownResult {
	start := time.Now()

	// CAS state → ShuttingDown
	s.state.CompareAndSwap(int32(StateSupervisorActive), int32(StateSupervisorShuttingDown))

	// 获取所有 Worker 快照
	s.mu.RLock()
	workers := make([]*Worker, 0, len(s.workers))
	for _, name := range s.order {
		if w, ok := s.workers[name]; ok {
			workers = append(workers, w)
		}
	}
	s.mu.RUnlock()

	result := GracefulShutdownResult{
		PerWorker: make([]WorkerStopDetail, 0, len(workers)),
	}

	if len(workers) == 0 {
		result.TotalDuration = time.Since(start)
		return result
	}

	// 并发停止所有 Worker
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, w := range workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			wStart := time.Now()

			// 标记优雅退出
			w.graceful.Store(true)

			// 正常停止
			_ = s.killWorker(w, false)

			elapsed := time.Since(wStart)
			exitWay := "graceful"

			// 检查是否被强杀（killWorker 超时强杀时设置 lastCrashReason=ReasonForceKilled）
			if r := w.lastCrashReason.Load(); r != nil {
				if reason, ok := r.(string); ok && reason == string(ReasonForceKilled) {
					exitWay = "force_killed"
				}
			}

			_ = w.applyTransition(StateStopped)

			mu.Lock()
			result.PerWorker = append(result.PerWorker, WorkerStopDetail{
				Name:     w.def.Name,
				ExitWay:  exitWay,
				Duration: elapsed,
			})
			if exitWay == "graceful" {
				result.StoppedCount++
			} else {
				result.ForceKilledCount++
			}
			mu.Unlock()
		}(w)
	}
	wg.Wait()

	// 检查残留
	s.mu.RLock()
	for _, w := range s.workers {
		state := WorkerState(w.state.Load())
		if state != StateStopped {
			result.ResidualCount++
		}
	}
	s.mu.RUnlock()

	result.TotalDuration = time.Since(start)
	return result
}

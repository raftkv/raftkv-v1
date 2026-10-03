// =========================================================================
// RaftKV Module05 — 工业级自愈与进程守护系统
//
// 文件：process_manager.go
// 职责：ProcessManager — launchWorker 双模式拉起 + waitExit 退出归因 +
//       killWorker 终止（正常 + 强杀）
//
// 设计要点：
//   - 双模式：ModeGoroutine（goroutine + Entry）/ ModeProcess（os/exec 子进程）
//   - goroutine 模式：创建 ctx + cancel，启动 goroutine 执行 Entry(ctx, hb)
//     defer close(done) + defer recover 捕获 panic
//   - 子进程模式：os/exec.CommandContext + cmd.Start + goroutine 调 cmd.Wait
//   - waitExit：阻塞等待退出，返回 (exitCode, reason)
//   - killWorker：force=false 调 cancel 等待 done；force=true 子进程调 Process.Kill
//   - 拉起后 atomic.Store startedAt = now，lastHeartbeatAt = 0（等待首次心跳）
//
// 与前序模块协调：沿用 pipeline-module context 取消传播 + raft-module goroutine 模式
// 零依赖声明：仅 os/exec、context、sync、sync/atomic、time、syscall
// =========================================================================

package selfheal

import (
	"context"
	"os/exec"

	"time"
)

// launchWorker 拉起 Worker 实例（双模式）。
//
// 拉起后 atomic.Store startedAt = now，lastHeartbeatAt = 0（等待首次心跳）。
// 启动 waitExit goroutine，退出后调用 s.onExit 触发自愈编排。
func (s *Supervisor) launchWorker(w *Worker) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 重置退出通道
	w.done = make(chan struct{}, 1)
	w.exitCh = make(chan struct{}, 1)

	now := time.Now().UnixNano()
	w.startedAt.Store(now)
	w.lastHeartbeatAt.Store(0) // 等待首次心跳

	switch w.def.Mode {
	case ModeGoroutine:
		return s.launchGoroutine(w)
	case ModeProcess:
		return s.launchProcess(w)
	default:
		return s.launchGoroutine(w)
	}
}

// launchGoroutine 以 goroutine 模式拉起 Worker。
func (s *Supervisor) launchGoroutine(w *Worker) error {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	hb := func() {
		w.lastHeartbeatAt.Store(time.Now().UnixNano())
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// panic 退出
				w.exitReason.Store(string(ReasonPanic))
				w.exitCode.Store(-1)
			}
			// 记录累计运行时长
			startedAt := w.startedAt.Load()
			if startedAt > 0 {
				uptime := time.Now().UnixNano() - startedAt
				if uptime > 0 {
					w.cumulativeUptime.Add(uptime)
				}
			}
			close(w.done)
		}()
		w.def.Entry(ctx, hb)
		// 正常返回：若非优雅关闭，视为非零退出
		if !w.graceful.Load() {
			w.exitReason.Store(string(ReasonNonZeroExit))
			w.exitCode.Store(1)
		} else {
			w.exitReason.Store(string(ReasonGracefulExit))
			w.exitCode.Store(0)
		}
	}()

	// 启动 waitExit 监督 goroutine
	go func() {
		<-w.done
		reason := ReasonNonZeroExit
		if r := w.exitReason.Load(); r != nil {
			reason = CrashReason(r.(string))
		}
		s.onExit(w, reason)
	}()

	return nil
}

// launchProcess 以子进程模式拉起 Worker。
func (s *Supervisor) launchProcess(w *Worker) error {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	cmd := exec.CommandContext(ctx, w.def.CmdPath, w.def.Args...)
	w.cmd = cmd

	if err := cmd.Start(); err != nil {
		// 启动失败
		w.exitReason.Store(string(ReasonStartupFailure))
		w.exitCode.Store(-1)
		close(w.done)
		return err
	}

	// 启动 waitExit 监督 goroutine
	go func() {
		err := cmd.Wait()
		// 记录累计运行时长
		startedAt := w.startedAt.Load()
		if startedAt > 0 {
			uptime := time.Now().UnixNano() - startedAt
			if uptime > 0 {
				w.cumulativeUptime.Add(uptime)
			}
		}

		exitCode := 0
		if cmd.ProcessState != nil {
			exitCode = cmd.ProcessState.ExitCode()
		}
		if err != nil {
			exitCode = -1
		}
		w.exitCode.Store(int32(exitCode))

		reason := ReasonNonZeroExit
		if w.graceful.Load() {
			reason = ReasonGracefulExit
		} else if exitCode != 0 {
			reason = ReasonNonZeroExit
		} else {
			// ExitCode == 0 但非优雅关闭，视为异常退出
			reason = ReasonNonZeroExit
		}
		w.exitReason.Store(string(reason))
		close(w.done)

		s.onExit(w, reason)
	}()

	return nil
}

// waitExit 等待 Worker 退出并返回退出码与原因。
//
// goroutine 模式：<-w.done
// 子进程模式：由 launchProcess 的 goroutine 已处理，此处仅等待 done
// 链路耗时 <10ms（chan 收发 + atomic 读）。
func (s *Supervisor) waitExit(w *Worker) (exitCode int, reason CrashReason) {
	<-w.done
	exitCode = int(w.exitCode.Load())
	r := w.exitReason.Load()
	if r == nil {
		reason = ReasonNonZeroExit
	} else {
		reason = CrashReason(r.(string))
	}
	return
}

// killWorker 终止 Worker。
//
// force=false（正常停止）：调用 cancel 取消 ctx，等待 done 或 shutdownTimeout
// force=true（强杀）：goroutine 模式再次 cancel + 等待 done；子进程模式调用 Process.Kill
//
// 标记 lastCrashReason = ReasonForceKilled（强杀时）。
func (s *Supervisor) killWorker(w *Worker, force bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.cancel == nil {
		return nil
	}

	if force {
		w.lastCrashReason.Store(string(ReasonForceKilled))
		// 子进程模式：直接 Kill
		if w.def.Mode == ModeProcess && w.cmd != nil && w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
			return nil
		}
		// goroutine 模式：cancel + 等待 done（带超时）
		w.cancel()
		select {
		case <-w.done:
		case <-time.After(w.def.ShutdownTimeout):
			// 超时放弃（goroutine 泄漏，Go 固有限制）
		}
		return nil
	}

	// 正常停止
	w.cancel()
	select {
	case <-w.done:
	case <-time.After(w.def.ShutdownTimeout):
		// 超时，强杀
		w.lastCrashReason.Store(string(ReasonForceKilled))
		if w.def.Mode == ModeProcess && w.cmd != nil && w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
		}
	}
	return nil
}

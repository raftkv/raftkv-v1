// =========================================================================
// RaftKV Module05 — 工业级自愈与进程守护系统
//
// 文件：signal_handler.go
// 职责：SignalHandler — 监听 SIGTERM / SIGINT，触发优雅关闭回调
//
// 设计要点：
//   - 仅注册 SIGTERM 与 SIGINT，忽略其他信号（spec.md 4.3.1）
//   - 两种信号触发同一 onShutdown 回调，行为完全一致（spec.md 4.5.2）
//   - ctx 取消时优雅退出，不阻塞
//
// 与前序模块协调：沿用 pipeline-module context 取消传播约定
// 零依赖声明：仅 os/signal、syscall、context
// =========================================================================

package selfheal

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// SignalHandler 负责监听操作系统终止信号。
type SignalHandler struct {
	stop chan struct{}
}

// newSignalHandler 构造信号处理器。
func newSignalHandler() *SignalHandler {
	return &SignalHandler{
		stop: make(chan struct{}, 1),
	}
}

// listenSignal 监听 SIGTERM / SIGINT 信号。
//
// 收到信号后调用 onShutdown 回调（即 Supervisor.Stop），随后返回。
// ctx 取消时直接返回。仅注册 SIGTERM 与 SIGINT，其他信号不捕获。
func (sh *SignalHandler) listenSignal(ctx context.Context, onShutdown func()) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case <-sigCh:
		// 收到 SIGTERM 或 SIGINT，触发优雅关闭
		if onShutdown != nil {
			onShutdown()
		}
		// 通知停止完成
		select {
		case sh.stop <- struct{}{}:
		default:
		}
	case <-ctx.Done():
		// 上下文取消，直接退出
	}
}

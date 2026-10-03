// =========================================================================
// RaftKV Module05 — 工业级自愈与进程守护系统
//
// 文件：restart_policy.go
// 职责：RestartPolicy — 指数退避计算 NextBackoff（含溢出防护）+
//       最大重启次数判定 ShouldStop（风暴抑制）
//
// 设计要点：
//   - NextBackoff(n) = BackoffBase × 2^(n-1)，封顶 BackoffMax
//   - 溢出防护：shift > 30 直接返回 BackoffMax；结果 < 0 返回 BackoffMax
//   - ShouldStop(n) = n >= MaxRestarts，达阈值后停止重启进入 Stopped 终态
//
// 与前序模块协调：沿用 raft-module 随机选举超时思路，改为指数退避
// 零依赖声明：仅 time
// =========================================================================

package selfheal

import "time"

// RestartPolicy 控制 Worker 崩溃后的重启行为。
type RestartPolicy struct {
	// MaxRestarts 最大重启次数，达此值后进入永久停机。
	MaxRestarts int
	// BackoffBase 指数退避基准间隔。
	BackoffBase time.Duration
	// BackoffMax 指数退避上限。
	BackoffMax time.Duration
}

// newRestartPolicy 从 WorkerDef 构造不可变重启策略。
func newRestartPolicy(def WorkerDef) RestartPolicy {
	return RestartPolicy{
		MaxRestarts: def.MaxRestarts,
		BackoffBase: def.BackoffBase,
		BackoffMax:  def.BackoffMax,
	}
}

// NextBackoff 计算第 restartCount 次重启的退避间隔。
//
// 序列（base=100ms, max=30s）：
//
//	第 1 次：100ms × 2^0 = 100ms
//	第 2 次：100ms × 2^1 = 200ms
//	第 3 次：100ms × 2^2 = 400ms
//	...
//	第 9 次：100ms × 2^8 = 25.6s
//	第 10 次：100ms × 2^9 = 51.2s → 封顶 30s
//
// 溢出防护：shift > 30 直接返回 BackoffMax，避免 int64 溢出。
func (p RestartPolicy) NextBackoff(restartCount int) time.Duration {
	if restartCount <= 0 {
		return p.BackoffBase
	}
	shift := restartCount - 1
	if shift > 30 {
		return p.BackoffMax
	}
	backoff := p.BackoffBase * time.Duration(uint64(1)<<uint(shift))
	if backoff > p.BackoffMax || backoff < 0 {
		return p.BackoffMax
	}
	return backoff
}

// ShouldStop 判断是否达到最大重启次数，应停止重启进入永久停机。
//
// 满足 spec.md 5.4.1 风暴抑制：达阈值后不再产生任何重启动作。
func (p RestartPolicy) ShouldStop(restartCount int) bool {
	return restartCount >= p.MaxRestarts
}

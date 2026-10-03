// =========================================================================
// RaftKV Module05 — 工业级自愈与进程守护系统
//
// 文件：health_check.go
// 职责：HealthProbe 接口（扩展点）+ defaultHeartbeatProbe 默认心跳探针实现
//
// 设计要点：
//   - HealthProbe 接口供运维侧注入自定义探针（TCP/HTTP）
//   - defaultHeartbeatProbe 仅做心跳新鲜度判定：
//       * 未上报心跳（lastHB==0）：检查启动宽限期（startedAt + HeartbeatTimeout > now 则健康）
//       * 已上报：elapsed = now - lastHB，返回 elapsed <= HeartbeatTimeout
//   - 判定延迟 ≤ 心跳周期×2 + 100ms（spec.md 4.1.2）
//
// 与前序模块协调：沿用接口解耦约定（Transport / Pipeline.Output）
// 零依赖声明：仅 time、sync/atomic
// =========================================================================

package selfheal

import (
	"time"
)

// HealthProbe 健康探针扩展接口。
//
// 默认实现为心跳探针；运维侧可注入自定义探针（如 TCP 端口探活、HTTP /healthz）。
// 在 WorkerDef 中通过 Probe 字段注入，未指定时用 defaultHeartbeatProbe。
type HealthProbe interface {
	// Check 返回 true 表示健康，false 表示不健康。
	Check(w *Worker) bool
}

// defaultHeartbeatProbe 默认心跳新鲜度探针。
type defaultHeartbeatProbe struct{}

// defaultProbe 是默认探针单例。
var defaultProbe HealthProbe = defaultHeartbeatProbe{}

// Check 实现心跳新鲜度判定。
//
// 算法（design.md 2.4.3）：
//
//	if lastHB == 0:
//	    // 未上报过心跳：检查启动宽限期
//	    return time.Since(startedAt) <= HeartbeatTimeout
//	else:
//	    return time.Since(lastHB) <= HeartbeatTimeout
func (defaultHeartbeatProbe) Check(w *Worker) bool {
	lastHB := w.lastHeartbeatAt.Load()
	timeout := w.def.HeartbeatTimeout
	if lastHB == 0 {
		startedAt := w.startedAt.Load()
		if startedAt == 0 {
			return true // 尚未启动，视为健康（避免误判）
		}
		return time.Since(time.Unix(0, startedAt)) <= timeout
	}
	return time.Since(time.Unix(0, lastHB)) <= timeout
}

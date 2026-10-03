// =========================================================================
// RaftKV Module05 — 工业级自愈与进程守护系统
//
// 文件：types.go
// 职责：枚举（WorkerMode / WorkerState / CrashReason / SupervisorState）+
//       结构体（WorkerDef / Worker / SupervisorConfig / Supervisor）+
//       快照与事件结构体 + DefaultConfig + validate 校验
//
// 设计要点：
//   - 零外部依赖，仅用标准库 time / sync / sync/atomic / context / os/exec / errors / fmt
//   - Worker 状态用 atomic.Int32 存储，满足并发状态查询不撕裂（spec.md 5.6.3）
//   - 心跳时间戳用 atomic.Int64（UnixNano）无锁高频写入
//   - workers map 用 sync.RWMutex 保护，读多写少
//
// 与前序模块协调：
//   - 沿用 raft-module / pipeline-module 的 Config + Default<Name>Config 模式
//   - 沿用 observability-module 的 atomic 计数约定
//   - 不直接 import 前序模块，仅通过 EventListener 接口由调用方注入
//
// 零依赖声明：仅 Go 标准库
// =========================================================================

package selfheal

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// -------------------------------------------------------------------------
// 枚举：Worker 运行模式
// -------------------------------------------------------------------------

// WorkerMode 表示 Worker 的运行模式。
type WorkerMode int32

const (
	// ModeGoroutine 以 goroutine 模拟 Worker（测试友好，无进程开销）。
	ModeGoroutine WorkerMode = 0
	// ModeProcess 以操作系统子进程运行 Worker（生产模式）。
	ModeProcess WorkerMode = 1
)

// String 返回可读字符串。
func (m WorkerMode) String() string {
	switch m {
	case ModeGoroutine:
		return "Goroutine"
	case ModeProcess:
		return "Process"
	default:
		return "Unknown"
	}
}

// -------------------------------------------------------------------------
// 枚举：Worker 五态状态机
// -------------------------------------------------------------------------

// WorkerState 表示 Worker 的五态状态机。
type WorkerState int32

const (
	// StateRunning 运行中。
	StateRunning WorkerState = 0
	// StateCrashed 已崩溃。
	StateCrashed WorkerState = 1
	// StateRestarting 重启中。
	StateRestarting WorkerState = 2
	// StateUnhealthy 心跳不健康。
	StateUnhealthy WorkerState = 3
	// StateStopped 永久停机（终态）。
	StateStopped WorkerState = 4
)

// -------------------------------------------------------------------------
// 枚举：Supervisor 守护器状态
// -------------------------------------------------------------------------

// SupervisorState 表示守护器自身状态。
type SupervisorState int32

const (
	// StateSupervisorActive 守护器活跃，接受注册。
	StateSupervisorActive SupervisorState = 0
	// StateSupervisorShuttingDown 守护器关闭中，拒绝新注册。
	StateSupervisorShuttingDown SupervisorState = 1
	// StateSupervisorStopped 守护器已停止（终态）。
	StateSupervisorStopped SupervisorState = 2
)

// String 返回可读字符串。
func (s SupervisorState) String() string {
	switch s {
	case StateSupervisorActive:
		return "Active"
	case StateSupervisorShuttingDown:
		return "ShuttingDown"
	case StateSupervisorStopped:
		return "Stopped"
	default:
		return "Unknown"
	}
}

// -------------------------------------------------------------------------
// 枚举：崩溃原因标签（满足 spec.md 4.4.3 链路追踪关联）
// -------------------------------------------------------------------------

// CrashReason 标识 Worker 退出/崩溃的原因。
type CrashReason string

const (
	// ReasonPanic Entry panic 退出。
	ReasonPanic CrashReason = "panic"
	// ReasonNonZeroExit 非零退出码。
	ReasonNonZeroExit CrashReason = "non_zero_exit"
	// ReasonHeartbeatTimeout 心跳超时。
	ReasonHeartbeatTimeout CrashReason = "heartbeat_timeout"
	// ReasonStartupFailure 启动阶段失败。
	ReasonStartupFailure CrashReason = "startup_failure"
	// ReasonForceKilled 被强杀。
	ReasonForceKilled CrashReason = "force_killed"
	// ReasonGracefulExit 优雅关闭正常退出（非崩溃）。
	ReasonGracefulExit CrashReason = "graceful_exit"
)

// -------------------------------------------------------------------------
// 错误变量
// -------------------------------------------------------------------------

var (
	// ErrInvalidConfig SupervisorConfig 校验失败。
	ErrInvalidConfig = errors.New("selfheal: invalid supervisor config")
	// ErrInvalidWorkerDef WorkerDef 校验失败。
	ErrInvalidWorkerDef = errors.New("selfheal: invalid worker def")
	// ErrWorkerExists 同名 Worker 已注册。
	ErrWorkerExists = errors.New("selfheal: worker already exists")
	// ErrWorkerNotFound 指定 Worker 未注册。
	ErrWorkerNotFound = errors.New("selfheal: worker not found")
	// ErrShuttingDown 守护器关闭中，拒绝新注册。
	ErrShuttingDown = errors.New("selfheal: supervisor is shutting down")
	// ErrAlreadyStarted 守护器已启动，重复 Start。
	ErrAlreadyStarted = errors.New("selfheal: supervisor already started")
	// ErrNotStarted 守护器未启动。
	ErrNotStarted = errors.New("selfheal: supervisor not started")
	// ErrIllegalTransition 非法状态流转。
	ErrIllegalTransition = errors.New("selfheal: illegal state transition")
)

// -------------------------------------------------------------------------
// WorkerDef：注册输入（对应 spec.md 6.1）
// -------------------------------------------------------------------------

// WorkerDef 描述受守护 Worker 的定义，由调用方在 AddWorker 时提交。
type WorkerDef struct {
	Name              string                                      `json:"name"`
	Mode              WorkerMode                                  `json:"mode"`
	Entry             func(ctx context.Context, heartbeat func()) `json:"-"`        // goroutine 模式入口
	CmdPath           string                                      `json:"cmd_path"` // 子进程模式可执行文件路径
	Args              []string                                    `json:"args"`     // 子进程参数
	HeartbeatInterval time.Duration                               `json:"heartbeat_interval"`
	HeartbeatTimeout  time.Duration                               `json:"heartbeat_timeout"`
	MaxRestarts       int                                         `json:"max_restarts"`
	BackoffBase       time.Duration                               `json:"backoff_base"`
	BackoffMax        time.Duration                               `json:"backoff_max"`
	ShutdownTimeout   time.Duration                               `json:"shutdown_timeout"`
	Probe             HealthProbe                                 `json:"-"` // 可选，nil 用默认
}

// -------------------------------------------------------------------------
// Worker：运行时实例（内部，字段不导出）
// -------------------------------------------------------------------------

// Worker 表示一个受守护的工作单元运行时实例。
type Worker struct {
	def              WorkerDef
	policy           RestartPolicy
	state            atomic.Int32 // WorkerState
	restartCount     atomic.Int64
	lastHeartbeatAt  atomic.Int64 // UnixNano，0 表示未上报
	startedAt        atomic.Int64 // UnixNano
	cumulativeUptime atomic.Int64 // 累计运行纳秒
	lastCrashReason  atomic.Value // CrashReason 字符串
	exitReason       atomic.Value // CrashReason 退出原因（供 waitExit 读取）
	exitCode         atomic.Int32 // 退出码（子进程模式）
	cancel           context.CancelFunc
	cmd              *exec.Cmd // 子进程模式
	done             chan struct{}
	exitCh           chan struct{} // 退出事件通知（缓冲 1）
	mu               sync.Mutex    // 保护 launch/kill 串行
	sup              *Supervisor   // 反向引用，用于事件回调
	// graceful 标志：为 true 表示当前退出由优雅关闭触发，不应视为崩溃
	graceful atomic.Bool
}

// State 返回 Worker 当前状态（atomic 读，无锁）。
func (w *Worker) State() WorkerState {
	return WorkerState(w.state.Load())
}

// RestartCount 返回累计重启次数。
func (w *Worker) RestartCount() int64 {
	return w.restartCount.Load()
}

// -------------------------------------------------------------------------
// SupervisorConfig：守护器配置
// -------------------------------------------------------------------------

// SupervisorConfig 描述守护器的全局配置。
type SupervisorConfig struct {
	WatchdogInterval         time.Duration `json:"watchdog_interval"`
	DefaultHeartbeatInterval time.Duration `json:"default_heartbeat_interval"`
	DefaultHeartbeatTimeout  time.Duration `json:"default_heartbeat_timeout"`
	DefaultMaxRestarts       int           `json:"default_max_restarts"`
	DefaultBackoffBase       time.Duration `json:"default_backoff_base"`
	DefaultBackoffMax        time.Duration `json:"default_backoff_max"`
	DefaultShutdownTimeout   time.Duration `json:"default_shutdown_timeout"`
	WorkerMode               WorkerMode    `json:"worker_mode"`
}

// -------------------------------------------------------------------------
// Supervisor：守护器核心（内部，字段不导出）
// -------------------------------------------------------------------------

// Supervisor 是工业级自愈与进程守护系统的核心实体。
type Supervisor struct {
	cfg            SupervisorConfig
	workers        map[string]*Worker
	order          []string
	mu             sync.RWMutex
	state          atomic.Int32 // SupervisorState
	listener       atomic.Value // EventListener
	watchdog       *Watchdog
	sigHandler     *SignalHandler
	started        atomic.Bool
	stopOnce       sync.Once
	cancelInternal context.CancelFunc
}

// -------------------------------------------------------------------------
// 快照结构体（对应 spec.md 6.3）
// -------------------------------------------------------------------------

// WorkerSnapshot 是单个 Worker 的状态快照。
type WorkerSnapshot struct {
	Name             string        `json:"name"`
	State            string        `json:"state"`
	RestartCount     int64         `json:"restart_count"`
	LastHeartbeatAt  time.Time     `json:"last_heartbeat_at"`
	StartedAt        time.Time     `json:"started_at"`
	CumulativeUptime time.Duration `json:"cumulative_uptime"`
	LastCrashReason  string        `json:"last_crash_reason"`
}

// SupervisorSnapshot 是守护器全量状态快照。
type SupervisorSnapshot struct {
	Workers         []WorkerSnapshot `json:"workers"`
	SupervisorState string           `json:"supervisor_state"`
	TotalRestarts   int64            `json:"total_restarts"`
	UnhealthyCount  int              `json:"unhealthy_count"`
	StoppedCount    int              `json:"stopped_count"`
	SnapshotAt      time.Time        `json:"snapshot_at"`
}

// -------------------------------------------------------------------------
// 优雅关闭结果（对应 spec.md 6.4）
// -------------------------------------------------------------------------

// WorkerStopDetail 描述单个 Worker 的关闭详情。
type WorkerStopDetail struct {
	Name     string        `json:"name"`
	ExitWay  string        `json:"exit_way"` // graceful | force_killed
	Duration time.Duration `json:"duration"`
}

// GracefulShutdownResult 是优雅关闭的汇总结果。
type GracefulShutdownResult struct {
	StoppedCount     int                `json:"stopped_count"`
	ForceKilledCount int                `json:"force_killed_count"`
	ResidualCount    int                `json:"residual_count"`
	TotalDuration    time.Duration      `json:"total_duration"`
	PerWorker        []WorkerStopDetail `json:"per_worker"`
}

// -------------------------------------------------------------------------
// 事件结构体（对应 spec.md 4.4.1 结构化日志 + 4.4.3 链路追踪关联）
// -------------------------------------------------------------------------

// CrashEvent 崩溃事件。
type CrashEvent struct {
	WorkerName   string      `json:"worker_name"`
	Reason       CrashReason `json:"reason"`
	RestartCount int64       `json:"restart_count"`
	Timestamp    time.Time   `json:"timestamp"`
}

// RestartEvent 重启事件。
type RestartEvent struct {
	WorkerName   string        `json:"worker_name"`
	Reason       CrashReason   `json:"reason"`
	RestartCount int64         `json:"restart_count"`
	Backoff      time.Duration `json:"backoff"`
	Timestamp    time.Time     `json:"timestamp"`
}

// TimeoutEvent 心跳超时事件。
type TimeoutEvent struct {
	WorkerName      string        `json:"worker_name"`
	LastHeartbeatAt time.Time     `json:"last_heartbeat_at"`
	Elapsed         time.Duration `json:"elapsed"`
	RestartCount    int64         `json:"restart_count"`
	Timestamp       time.Time     `json:"timestamp"`
}

// StopEvent 永久停机告警事件。
type StopEvent struct {
	WorkerName   string      `json:"worker_name"`
	Reason       CrashReason `json:"reason"`
	RestartCount int64       `json:"restart_count"`
	Timestamp    time.Time   `json:"timestamp"`
}

// TransitionEvent 状态流转事件。
type TransitionEvent struct {
	WorkerName string      `json:"worker_name"`
	From       WorkerState `json:"from"`
	To         WorkerState `json:"to"`
	Timestamp  time.Time   `json:"timestamp"`
}

// -------------------------------------------------------------------------
// DefaultSupervisorConfig / DefaultWorkerDef
// -------------------------------------------------------------------------

// DefaultSupervisorConfig 返回带默认值的守护器配置。
func DefaultSupervisorConfig() SupervisorConfig {
	return SupervisorConfig{
		WatchdogInterval:         50 * time.Millisecond,
		DefaultHeartbeatInterval: 1 * time.Second,
		DefaultHeartbeatTimeout:  2 * time.Second,
		DefaultMaxRestarts:       5,
		DefaultBackoffBase:       100 * time.Millisecond,
		DefaultBackoffMax:        30 * time.Second,
		DefaultShutdownTimeout:   5 * time.Second,
		WorkerMode:               ModeGoroutine,
	}
}

// DefaultWorkerDef 返回带默认值的 WorkerDef 模板，调用方可覆盖字段。
func DefaultWorkerDef(name string) WorkerDef {
	return WorkerDef{
		Name:              name,
		Mode:              ModeGoroutine,
		HeartbeatInterval: 1 * time.Second,
		HeartbeatTimeout:  2 * time.Second,
		MaxRestarts:       5,
		BackoffBase:       100 * time.Millisecond,
		BackoffMax:        30 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}
}

// -------------------------------------------------------------------------
// validate 校验函数（满足 spec.md 4.3.2 安全性约束）
// -------------------------------------------------------------------------

// validate 校验 SupervisorConfig 非负有限值与范围。
func (cfg SupervisorConfig) validate() error {
	if cfg.WatchdogInterval < 10*time.Millisecond || cfg.WatchdogInterval > 1*time.Second {
		return fmt.Errorf("%w: watchdog_interval %s 超出 [10ms,1s]", ErrInvalidConfig, cfg.WatchdogInterval)
	}
	if cfg.DefaultHeartbeatInterval < 100*time.Millisecond || cfg.DefaultHeartbeatInterval > 60*time.Second {
		return fmt.Errorf("%w: default_heartbeat_interval %s 超出 [100ms,60s]", ErrInvalidConfig, cfg.DefaultHeartbeatInterval)
	}
	if cfg.DefaultHeartbeatTimeout < 2*cfg.DefaultHeartbeatInterval {
		return fmt.Errorf("%w: default_heartbeat_timeout %s < 2×heartbeat_interval", ErrInvalidConfig, cfg.DefaultHeartbeatTimeout)
	}
	if cfg.DefaultMaxRestarts < 0 || cfg.DefaultMaxRestarts > 100 {
		return fmt.Errorf("%w: default_max_restarts %d 超出 [0,100]", ErrInvalidConfig, cfg.DefaultMaxRestarts)
	}
	if cfg.DefaultBackoffBase < 10*time.Millisecond || cfg.DefaultBackoffBase > 10*time.Second {
		return fmt.Errorf("%w: default_backoff_base %s 超出 [10ms,10s]", ErrInvalidConfig, cfg.DefaultBackoffBase)
	}
	if cfg.DefaultBackoffMax < cfg.DefaultBackoffBase {
		return fmt.Errorf("%w: default_backoff_max %s < backoff_base", ErrInvalidConfig, cfg.DefaultBackoffMax)
	}
	if cfg.DefaultShutdownTimeout < 1*time.Second || cfg.DefaultShutdownTimeout > 60*time.Second {
		return fmt.Errorf("%w: default_shutdown_timeout %s 超出 [1s,60s]", ErrInvalidConfig, cfg.DefaultShutdownTimeout)
	}
	return nil
}

// validate 校验 WorkerDef 字段合法。
func (def WorkerDef) validate() error {
	if len(def.Name) == 0 || len(def.Name) > 64 {
		return fmt.Errorf("%w: name 长度 %d 超出 [1,64]", ErrInvalidWorkerDef, len(def.Name))
	}
	if def.Mode == ModeGoroutine && def.Entry == nil {
		return fmt.Errorf("%w: goroutine 模式 Entry 不能为 nil", ErrInvalidWorkerDef)
	}
	if def.Mode == ModeProcess && len(def.CmdPath) == 0 {
		return fmt.Errorf("%w: process 模式 CmdPath 不能为空", ErrInvalidWorkerDef)
	}
	if def.HeartbeatInterval < 100*time.Millisecond || def.HeartbeatInterval > 60*time.Second {
		return fmt.Errorf("%w: heartbeat_interval %s 超出 [100ms,60s]", ErrInvalidWorkerDef, def.HeartbeatInterval)
	}
	if def.HeartbeatTimeout < 2*def.HeartbeatInterval {
		return fmt.Errorf("%w: heartbeat_timeout %s < 2×heartbeat_interval", ErrInvalidWorkerDef, def.HeartbeatTimeout)
	}
	if def.MaxRestarts < 0 || def.MaxRestarts > 100 {
		return fmt.Errorf("%w: max_restarts %d 超出 [0,100]", ErrInvalidWorkerDef, def.MaxRestarts)
	}
	if def.BackoffBase < 10*time.Millisecond || def.BackoffBase > 10*time.Second {
		return fmt.Errorf("%w: backoff_base %s 超出 [10ms,10s]", ErrInvalidWorkerDef, def.BackoffBase)
	}
	if def.BackoffMax < def.BackoffBase {
		return fmt.Errorf("%w: backoff_max %s < backoff_base", ErrInvalidWorkerDef, def.BackoffMax)
	}
	if def.ShutdownTimeout < 1*time.Second || def.ShutdownTimeout > 60*time.Second {
		return fmt.Errorf("%w: shutdown_timeout %s 超出 [1s,60s]", ErrInvalidWorkerDef, def.ShutdownTimeout)
	}
	return nil
}

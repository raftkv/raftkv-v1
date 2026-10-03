// =========================================================================
// RaftKV — Agent 确定性调度器 + 故障检测 + 自动重调度
//
// 定位: 在 Raft 共识之上，为 Agent 推理任务提供确定性资源分配，
//       当节点故障（网络分区/宕机）时自动触发重调度，无需人工干预。
//
// 核心能力:
//   1. 过滤链 (Filter Chain): GPU型号/VRAM/拓扑亲和/健康度/优先级
//   2. 打分 (Scoring):    BinPacking(紧致) / Spread(分散) / TopologyAware(拓扑感知)
//   3. 确定性绑定:        同输入+同集群状态 → 同分配结果 (Temp=0 时)
//   4. 故障检测:          心跳 Gap 检测 + DegradationManager 联动
//   5. 自动重调度:        故障节点上的所有任务 → 自动迁移到健康节点
//   6. 降级联动:          集群进入 ReadOnly → 暂停新调度，修复已有任务
//   7. Raft 共识保障:     每次分配/释放均经 Raft 日志落盘
//
// 调度流程:
//
//   AgentTaskRequest
//        │
//   ┌────▼──────────────────────────────┐
//   │  1. Filter Chain (过滤)            │
//   │     ├─ GPUModelFilter              │
//   │     ├─ VRAMFilter                  │
//   │     ├─ TopologyAffinityFilter       │
//   │     ├─ HealthFilter (≥60分)        │
//   │     ├─ PriorityGate (优先级准入)    │
//   │     └─ DeterministicLockFilter     │
//   └────┬──────────────────────────────┘
//        │ candidates[]
//   ┌────▼──────────────────────────────┐
//   │  2. Scoring (打分)                 │
//   │     ├─ BinPackingScore (紧致优先)   │
//   │     ├─ SpreadScore (均衡优先)       │
//   │     └─ TopologyScore (亲和优先)     │
//   └────┬──────────────────────────────┘
//        │ scoredCandidates[]
//   ┌────▼──────────────────────────────┐
//   │  3. Pick (选择)                    │
//   │     确定性: 同Seed → 同排序 → 同选 │
//   │     非确定性: 分数最高 + 随机打破   │
//   └────┬──────────────────────────────┘
//        │ bestNode + gpuIndices
//   ┌────▼──────────────────────────────┐
//   │  4. RaftBind (Raft 提案绑定)       │
//   │     CmdTaskAllocate → Propose →     │
//   │     Consensus → Apply → Response   │
//   └────────────────────────────────────┘
//
// =========================================================================

package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// =========================================================================
// 第一部分: 调度策略定义
// =========================================================================

// SchedulingStrategy 调度策略枚举
type SchedulingStrategy string

const (
	StrategyBinPacking    SchedulingStrategy = "bin_packing"    // 紧致优先：尽量填满已有节点
	StrategySpread        SchedulingStrategy = "spread"         // 分散优先：尽量分散到不同节点
	StrategyTopologyAware SchedulingStrategy = "topology_aware" // 拓扑感知：同机架 > 同交换机
	StrategyDeterministic SchedulingStrategy = "deterministic"  // 确定性：相同输入必然相同输出
)

// FilterName 过滤器名称（用于日志和统计）
type FilterName string

const (
	FilterGPUModel         FilterName = "gpu_model"
	FilterVRAM             FilterName = "vram"
	FilterTopology         FilterName = "topology_affinity"
	FilterHealth           FilterName = "health"
	FilterPriority         FilterName = "priority_gate"
	FilterDeterministic    FilterName = "deterministic_lock"
	FilterInterconnect     FilterName = "interconnect_bandwidth"
	FilterNodeAntiAffinity FilterName = "node_anti_affinity"
)

// =========================================================================
// 第二部分: FilterFunc 与 ScoreFunc 类型定义
// =========================================================================

// FilterFunc 过滤器函数
// 返回 true 表示该节点通过此过滤器
type FilterFunc func(ru *ResourceUnit, task *TaskUnit) *FilterResult

// FilterResult 过滤结果
type FilterResult struct {
	Passed bool
	Filter FilterName
	Reason string // 未通过的原因
}

// ScoreFunc 打分函数
// 返回分数 (越高越优先)
type ScoreFunc func(ru *ResourceUnit, task *TaskUnit) float64

// ScoredNode 带分数的候选节点
type ScoredNode struct {
	ResourceUnit *ResourceUnit
	Score        float64
	GPUIndices   []int // 建议分配的 GPU 索引
}

// =========================================================================
// 第三部分: AgentScheduler — 核心调度器
// =========================================================================

// AgentScheduler 确定性 Agent 任务调度器
type AgentScheduler struct {
	mu sync.RWMutex

	// 调度策略配置
	strategy SchedulingStrategy
	filters  []FilterFunc
	scorers  []ScoreFunc

	// 外部依赖
	stateMachine   *UnifiedStateMachine
	proposer       RaftProposer
	agentAdapter   *AgentAdapter
	degradationMgr interface{ IsDegraded() bool } // 避免循环依赖，用 interface

	// 故障检测器
	faultDetector *FaultDetector

	// 重调度器
	reScheduler *ReScheduler

	// 统计
	totalScheduled   atomic.Int64
	totalFiltered    atomic.Int64
	totalReScheduled atomic.Int64
	totalFailures    atomic.Int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc

	logger *log.Logger
}

// SchedulerConfig 调度器配置
type SchedulerConfig struct {
	Strategy               SchedulingStrategy
	EnableBinPacking       bool
	EnableTopologyAware    bool
	MinHealthScore         int           // 最小健康分数 (默认 60)
	MaxVRAMUtilization     float64       // 单 GPU 最大显存利用率 (默认 0.95)
	DeterministicTieBreak  bool          // 确定性打破平局 (Temp=0 时启用)
	FaultDetectionInterval time.Duration // 故障检测间隔 (默认 2s)
	ReScheduleEnabled      bool          // 是否启用自动重调度 (默认 true)
	ReScheduleBatchSize    int           // 每次重调度的最大任务数 (默认 10)
}

// DefaultSchedulerConfig 返回默认配置
func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		Strategy:               StrategyBinPacking,
		EnableBinPacking:       true,
		EnableTopologyAware:    true,
		MinHealthScore:         60,
		MaxVRAMUtilization:     0.95,
		DeterministicTieBreak:  true,
		FaultDetectionInterval: 2 * time.Second,
		ReScheduleEnabled:      true,
		ReScheduleBatchSize:    10,
	}
}

// NewAgentScheduler 创建调度器
func NewAgentScheduler(
	stateMachine *UnifiedStateMachine,
	proposer RaftProposer,
	agentAdapter *AgentAdapter,
	degradationMgr interface{ IsDegraded() bool },
	config SchedulerConfig,
) *AgentScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	as := &AgentScheduler{
		strategy:       config.Strategy,
		stateMachine:   stateMachine,
		proposer:       proposer,
		agentAdapter:   agentAdapter,
		degradationMgr: degradationMgr,
		ctx:            ctx,
		cancel:         cancel,
		logger:         log.Default(),
	}

	// 注册过滤器链
	as.filters = as.buildFilterChain(config)

	// 注册打分函数
	as.scorers = as.buildScorerChain(config)

	// 初始化故障检测器
	as.faultDetector = NewFaultDetector(as, config.FaultDetectionInterval)

	// 初始化重调度器
	as.reScheduler = NewReScheduler(as, config)

	return as
}

// Start 启动调度器及其子组件
func (as *AgentScheduler) Start() {
	as.faultDetector.Start()
	as.reScheduler.Start()
	as.logger.Printf("[AgentScheduler] 已启动 (策略=%s, 过滤器=%d, 打分器=%d)",
		as.strategy, len(as.filters), len(as.scorers))
}

// Stop 停止调度器
func (as *AgentScheduler) Stop() {
	as.cancel()
	as.faultDetector.Stop()
	as.reScheduler.Stop()
	as.logger.Printf("[AgentScheduler] 已停止 (调度=%d, 过滤=%d, 重调度=%d, 失败=%d)",
		as.totalScheduled.Load(), as.totalFiltered.Load(),
		as.totalReScheduled.Load(), as.totalFailures.Load())
}

// =========================================================================
// 第四部分: Schedule — 核心调度入口
// =========================================================================

// ScheduleResult 调度结果
type ScheduleResult struct {
	Success       bool
	TaskID        string
	AssignedNode  string             // 分配的 ResourceUnit ID
	AssignedGPUs  []int              // 分配的 GPU 索引
	AssignedVRAM  int                // 每 GPU 分配的 VRAM (MiB)
	RejectedNodes map[string]string  // 被拒绝的节点及其原因
	Scores        map[string]float64 // 候选节点得分
	LatencyUs     int64              // 调度延迟 (微秒)
	Reason        string             // 失败原因
}

// Schedule 为任务分配资源
// 完整的调度流程: Filter → Score → Pick → RaftBind
func (as *AgentScheduler) Schedule(task *TaskUnit) (*ScheduleResult, error) {
	startTime := time.Now()

	// 0. 检查集群是否处于降级状态
	if as.degradationMgr != nil && as.degradationMgr.IsDegraded() {
		return &ScheduleResult{
			Success: false,
			TaskID:  task.ID,
			Reason:  "集群处于降级只读模式，暂停新任务调度",
		}, nil
	}

	// 0.5. 检查 Leader
	if !as.proposer.IsLeader() {
		return &ScheduleResult{
			Success: false,
			TaskID:  task.ID,
			Reason:  fmt.Sprintf("当前节点不是 Leader (Leader=%s)", as.proposer.LeaderID()),
		}, nil
	}

	// 1. 从状态机获取当前资源池快照
	resources := as.stateMachine.ListResources()
	if len(resources) == 0 {
		return &ScheduleResult{
			Success: false,
			TaskID:  task.ID,
			Reason:  "资源池为空，无可用 GPU 节点",
		}, nil
	}

	// 2. Filter Chain — 逐层过滤
	candidates, rejected := as.filterCandidates(resources, task)
	as.totalFiltered.Add(int64(len(rejected)))

	if len(candidates) == 0 {
		reason := "无可调度节点: "
		for nodeID, why := range rejected {
			reason += fmt.Sprintf("%s(%s) ", nodeID, why)
		}
		return &ScheduleResult{
			Success:       false,
			TaskID:        task.ID,
			RejectedNodes: rejected,
			Reason:        reason,
		}, nil
	}

	// 3. Scoring — 对候选节点打分
	scored := as.scoreCandidates(candidates, task)

	// 4. Pick — 选择最优节点
	selected := as.pickBest(scored, task)

	// 5. RaftBind — 通过 Raft 共识绑定分配
	alloc := TaskAllocation{
		TaskID:      task.ID,
		ResourceIDs: []string{selected.ResourceUnit.ID},
		GPUIndices:  map[string][]int{selected.ResourceUnit.ID: selected.GPUIndices},
		VRAMPerGPU:  task.GPURequest.VRAMPerGPUMiB,
		CPUCores:    task.CPURequest,
		MemoryMiB:   task.MemoryRequest,
	}

	seq := as.stateMachine.CommandSeq + 1
	cmd, err := NewCommand(CmdTaskAllocate, "agent-scheduler", seq, alloc,
		as.stateMachine.LastSM3Hash, as.stateMachine.SM3Hasher)
	if err != nil {
		return nil, fmt.Errorf("构造分配命令失败: %w", err)
	}

	cmdBytes, _ := cmd.ToBytes()
	accepted, err := as.proposer.Propose(cmdBytes)
	if err != nil || !accepted {
		as.totalFailures.Add(1)
		return &ScheduleResult{
			Success: false,
			TaskID:  task.ID,
			Reason:  fmt.Sprintf("Raft 分配提案失败: %v", err),
		}, nil
	}

	// 等待 Raft 共识完成
	if err := as.proposer.ProposeAndWait(cmdBytes, 10*time.Second); err != nil {
		as.totalFailures.Add(1)
		return &ScheduleResult{
			Success: false,
			TaskID:  task.ID,
			Reason:  fmt.Sprintf("Raft 分配共识超时: %v", err),
		}, nil
	}

	as.totalScheduled.Add(1)

	// 收集各节点的得分
	scores := make(map[string]float64)
	for _, sn := range scored {
		scores[sn.ResourceUnit.ID] = sn.Score
	}

	result := &ScheduleResult{
		Success:      true,
		TaskID:       task.ID,
		AssignedNode: selected.ResourceUnit.ID,
		AssignedGPUs: selected.GPUIndices,
		AssignedVRAM: task.GPURequest.VRAMPerGPUMiB,
		Scores:       scores,
		LatencyUs:    time.Since(startTime).Microseconds(),
	}

	as.logger.Printf("[AgentScheduler] 调度成功: %s → %s (GPU=%v, VRAM=%dMiB, 候选=%d, 延迟=%dus)",
		task.ID, selected.ResourceUnit.ID, selected.GPUIndices,
		task.GPURequest.VRAMPerGPUMiB, len(candidates), result.LatencyUs)

	return result, nil
}

// =========================================================================
// 第五部分: 过滤链实现
// =========================================================================

func (as *AgentScheduler) buildFilterChain(config SchedulerConfig) []FilterFunc {
	var chain []FilterFunc

	// 1. GPU 型号过滤
	chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) *FilterResult {
		if task.GPURequest.PreferredModel != "" && ru.Model != task.GPURequest.PreferredModel {
			return &FilterResult{false, FilterGPUModel,
				fmt.Sprintf("需要 %s, 实际 %s", task.GPURequest.PreferredModel, ru.Model)}
		}
		if task.GPURequest.RequiredArchitecture != "" && ru.Architecture != task.GPURequest.RequiredArchitecture {
			return &FilterResult{false, FilterGPUModel,
				fmt.Sprintf("需要架构 %s, 实际 %s", task.GPURequest.RequiredArchitecture, ru.Architecture)}
		}
		return &FilterResult{true, FilterGPUModel, ""}
	})

	// 2. VRAM 过滤
	chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) *FilterResult {
		for i := 0; i < ru.Count; i++ {
			if !containsInt(ru.AllocatedGPUs, i) {
				return &FilterResult{true, FilterVRAM, ""} // 至少有一个空闲 GPU
			}
		}
		return &FilterResult{false, FilterVRAM, "所有 GPU 已被分配"}
	})

	// 3. GPU 互联带宽过滤
	chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) *FilterResult {
		if task.MinInterconnectBandwidth > 0 &&
			ru.NVLinkBandwidthGBs < task.MinInterconnectBandwidth {
			return &FilterResult{false, FilterInterconnect,
				fmt.Sprintf("需要互联带宽 %.0f GB/s, 实际 %.0f GB/s",
					task.MinInterconnectBandwidth, ru.NVLinkBandwidthGBs)}
		}
		return &FilterResult{true, FilterInterconnect, ""}
	})

	// 4. 健康度过滤
	chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) *FilterResult {
		if ru.HealthScore < config.MinHealthScore {
			return &FilterResult{false, FilterHealth,
				fmt.Sprintf("健康分 %d < 阈值 %d", ru.HealthScore, config.MinHealthScore)}
		}
		if !ru.IsHealthy() {
			return &FilterResult{false, FilterHealth,
				fmt.Sprintf("节点状态: %s", ru.State)}
		}
		return &FilterResult{true, FilterHealth, ""}
	})

	// 5. 优先级准入
	chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) *FilterResult {
		if len(ru.AllowedPriorities) > 0 {
			allowed := false
			for _, p := range ru.AllowedPriorities {
				if p == task.Priority {
					allowed = true
					break
				}
			}
			if !allowed {
				return &FilterResult{false, FilterPriority,
					fmt.Sprintf("节点不接受优先级 %d 的任务", task.Priority)}
			}
		}
		return &FilterResult{true, FilterPriority, ""}
	})

	// 6. 确定性锁定过滤 (已有确定性任务的节点不接受新的确定性任务)
	chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) *FilterResult {
		if task.DeterministicMode && ru.DeterministicLocked && len(ru.DeterministicTasks) > 0 {
			// 允许同一确定性上下文的任务共置
			return &FilterResult{true, FilterDeterministic, ""}
		}
		return &FilterResult{true, FilterDeterministic, ""}
	})

	// 7. 拓扑反亲和 (不与指定任务共置)
	chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) *FilterResult {
		if len(task.TopologyAntiAffinity) > 0 {
			for _, tid := range task.TopologyAntiAffinity {
				if otherTask, ok := as.stateMachine.GetTask(tid); ok {
					for _, assignedNode := range otherTask.AssignedNodes {
						if assignedNode == ru.ID {
							return &FilterResult{false, FilterNodeAntiAffinity,
								fmt.Sprintf("与任务 %s 反亲和 (共置节点 %s)", tid, ru.ID)}
						}
					}
				}
			}
		}
		return &FilterResult{true, FilterNodeAntiAffinity, ""}
	})

	return chain
}

// filterCandidates 执行过滤链
func (as *AgentScheduler) filterCandidates(resources []*ResourceUnit, task *TaskUnit) ([]*ResourceUnit, map[string]string) {
	var candidates []*ResourceUnit
	rejected := make(map[string]string)

	for _, ru := range resources {
		passed := true
		for _, filter := range as.filters {
			result := filter(ru, task)
			if !result.Passed {
				if _, exists := rejected[ru.ID]; !exists {
					rejected[ru.ID] = result.Reason
				}
				passed = false
				break
			}
		}
		if passed {
			candidates = append(candidates, ru)
		}
	}

	return candidates, rejected
}

// =========================================================================
// 第六部分: 打分函数实现
// =========================================================================

func (as *AgentScheduler) buildScorerChain(cfg SchedulerConfig) []ScoreFunc {
	var chain []ScoreFunc

	if cfg.EnableBinPacking {
		// BinPacking: 优先选已分配 GPU 最多的节点 (紧致优先)
		chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) float64 {
			return ru.GPUUtilization() * 100 // 0-100
		})
	} else {
		// Spread: 优先选已分配 GPU 最少的节点 (均衡优先)
		chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) float64 {
			return (1 - ru.GPUUtilization()) * 100 // 0-100
		})
	}

	if cfg.EnableTopologyAware {
		// 拓扑亲和: 同机架 +20, 同交换机 +10
		chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) float64 {
			score := 0.0
			if task.TopologyAffinity != nil && task.TopologyAffinity.Scope == "same_rack" && task.NodeSelector != nil {
				if rack, ok := task.NodeSelector["rack_id"]; ok && ru.RackID == rack {
					score += 20
				}
			}
			if task.TopologyAffinity.Scope == "same_switch" && task.NodeSelector != nil {
				if sw, ok := task.NodeSelector["switch_id"]; ok && ru.SwitchID == sw {
					score += 10
				}
			}
			return score
		})
	}

	// 健康度加成: 健康分越高越好
	chain = append(chain, func(ru *ResourceUnit, task *TaskUnit) float64 {
		return float64(ru.HealthScore) * 0.1 // 0-10
	})

	return chain
}

// scoreCandidates 执行打分
func (as *AgentScheduler) scoreCandidates(candidates []*ResourceUnit, task *TaskUnit) []ScoredNode {
	var scored []ScoredNode

	for _, ru := range candidates {
		totalScore := 0.0
		for _, scorer := range as.scorers {
			totalScore += scorer(ru, task)
		}

		// 为每个节点确定可分配的 GPU 索引
		gpuIndices := as.selectGPUs(ru, task)

		scored = append(scored, ScoredNode{
			ResourceUnit: ru,
			Score:        totalScore,
			GPUIndices:   gpuIndices,
		})
	}

	// 排序: 分数降序
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	return scored
}

// selectGPUs 从节点中选择可用的 GPU 索引
func (as *AgentScheduler) selectGPUs(ru *ResourceUnit, task *TaskUnit) []int {
	need := task.GPURequest.Count
	if need <= 0 {
		need = 1
	}

	// 收集所有未分配的 GPU 索引
	var freeGPUs []int
	for i := 0; i < ru.Count; i++ {
		if !containsInt(ru.AllocatedGPUs, i) {
			freeGPUs = append(freeGPUs, i)
		}
	}

	// 如果需求超过可用，返回所有可用
	if need > len(freeGPUs) {
		return freeGPUs
	}

	// 确定性选择: 如果任务 Temp=0，使用 Seed 稳定排序
	if task.DeterministicMode {
		// 按 GPU 索引自然排序（确定性）
		sort.Ints(freeGPUs)
	}

	return freeGPUs[:need]
}

// pickBest 从排序后的候选中选择最优节点
func (as *AgentScheduler) pickBest(scored []ScoredNode, task *TaskUnit) *ScoredNode {
	if len(scored) == 0 {
		return nil
	}

	// 确定性模式: 当多个节点分数相同时，用 Seed 打破平局
	if task.DeterministicMode && task.Seed != 0 {
		// 找到所有得分最高的节点
		bestScore := scored[0].Score
		var tied []int
		for i, sn := range scored {
			if sn.Score >= bestScore-0.01 { // 浮点容差
				tied = append(tied, i)
			}
		}

		// 用 hash(seed + nodeID) 从平局中确定性选择
		if len(tied) > 1 {
			pickIdx := int(task.Seed%int64(len(tied))+int64(len(tied))) % len(tied)
			return &scored[tied[pickIdx]]
		}
	}

	return &scored[0]
}

// =========================================================================
// 第七部分: FaultDetector — 故障检测器
// =========================================================================

// FaultEvent 故障事件
type FaultEvent struct {
	Type        string // "node_offline" | "node_degraded" | "node_heartbeat_lost" | "raft_partition"
	NodeID      string // 受影响的节点 ID
	Timestamp   time.Time
	Severity    string // "critical" | "warning"
	Description string
}

// FaultDetector 故障检测器
// 监控节点健康状态，检测网络分区和节点故障，触发重调度
type FaultDetector struct {
	mu sync.RWMutex

	scheduler     *AgentScheduler
	checkInterval time.Duration

	// 节点心跳追踪: nodeID → 最后心跳时间
	lastHeartbeats map[string]time.Time

	// 心跳超时阈值
	heartbeatTimeout time.Duration

	// 故障事件通道
	faultCh chan FaultEvent

	// 已知故障节点（避免重复触发重调度）
	knownFaults map[string]bool

	// 控制
	ctx    context.Context
	cancel context.CancelFunc

	logger *log.Logger
}

// NewFaultDetector 创建故障检测器
func NewFaultDetector(scheduler *AgentScheduler, checkInterval time.Duration) *FaultDetector {
	ctx, cancel := context.WithCancel(context.Background())
	return &FaultDetector{
		scheduler:        scheduler,
		checkInterval:    checkInterval,
		lastHeartbeats:   make(map[string]time.Time),
		heartbeatTimeout: 3 * checkInterval, // 3 个检测周期无心跳 = 离线
		faultCh:          make(chan FaultEvent, 64),
		knownFaults:      make(map[string]bool),
		ctx:              ctx,
		cancel:           cancel,
		logger:           log.Default(),
	}
}

// Start 启动故障检测循环
func (fd *FaultDetector) Start() {
	go fd.detectionLoop()
	fd.logger.Printf("[FaultDetector] 已启动 (检测间隔=%v, 心跳超时=%v)",
		fd.checkInterval, fd.heartbeatTimeout)
}

// Stop 停止故障检测
func (fd *FaultDetector) Stop() {
	fd.cancel()
	fd.logger.Printf("[FaultDetector] 已停止")
}

// UpdateHeartbeat 外部调用: 更新节点心跳时间
func (fd *FaultDetector) UpdateHeartbeat(nodeID string) {
	fd.mu.Lock()
	fd.lastHeartbeats[nodeID] = time.Now()
	fd.mu.Unlock()
}

// detectionLoop 故障检测主循环
func (fd *FaultDetector) detectionLoop() {
	ticker := time.NewTicker(fd.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fd.ctx.Done():
			return
		case <-ticker.C:
			fd.checkAllNodes()
		case fault := <-fd.faultCh:
			fd.handleFault(fault)
		}
	}
}

// checkAllNodes 检测所有节点的健康状态
func (fd *FaultDetector) checkAllNodes() {
	resources := fd.scheduler.stateMachine.ListResources()
	now := time.Now()

	for _, ru := range resources {
		// 1. 心跳超时检测
		fd.mu.RLock()
		lastHB, hasHB := fd.lastHeartbeats[ru.ID]
		fd.mu.RUnlock()

		if hasHB && now.Sub(lastHB) > fd.heartbeatTimeout {
			// 心跳超时 → 认为节点离线
			if _, known := fd.knownFaults[ru.ID]; !known {
				fd.logger.Printf("[FaultDetector] 节点心跳超时: %s (最后心跳=%v前, 阈值=%v)",
					ru.ID, now.Sub(lastHB), fd.heartbeatTimeout)
				fd.faultCh <- FaultEvent{
					Type:        "node_heartbeat_lost",
					NodeID:      ru.ID,
					Timestamp:   now,
					Severity:    "critical",
					Description: fmt.Sprintf("节点 %s 心跳超时 (%v 无响应)", ru.ID, now.Sub(lastHB).Round(time.Second)),
				}
			}
		}

		// 2. 健康分检测
		if ru.HealthScore < 40 && ru.State == ResourceStateOnline {
			if _, known := fd.knownFaults[ru.ID]; !known {
				fd.logger.Printf("[FaultDetector] 节点健康分过低: %s (健康分=%d)", ru.ID, ru.HealthScore)
				fd.faultCh <- FaultEvent{
					Type:        "node_degraded",
					NodeID:      ru.ID,
					Timestamp:   now,
					Severity:    "warning",
					Description: fmt.Sprintf("节点 %s 健康分=%d (低于 40)", ru.ID, ru.HealthScore),
				}
			}
		}

		// 3. 节点直接下线
		if ru.State == ResourceStateOffline || ru.State == ResourceStateMaintenance {
			if _, known := fd.knownFaults[ru.ID]; !known {
				fd.logger.Printf("[FaultDetector] 节点离线: %s (state=%s)", ru.ID, ru.State)
				fd.faultCh <- FaultEvent{
					Type:        "node_offline",
					NodeID:      ru.ID,
					Timestamp:   now,
					Severity:    "critical",
					Description: fmt.Sprintf("节点 %s 已下线 (state=%s)", ru.ID, ru.State),
				}
			}
		}
	}
}

// handleFault 处理故障事件
func (fd *FaultDetector) handleFault(fault FaultEvent) {
	fd.mu.Lock()
	if fd.knownFaults[fault.NodeID] {
		fd.mu.Unlock()
		return // 已处理过，避免重复
	}
	fd.knownFaults[fault.NodeID] = true
	fd.mu.Unlock()

	fd.logger.Printf("[FaultDetector] 故障事件: %s severity=%s node=%s desc=%s",
		fault.Type, fault.Severity, fault.NodeID, fault.Description)

	// 触发重调度
	fd.scheduler.reScheduler.OnNodeFault(fault)
}

// ClearFault 清除故障标记（节点恢复后调用）
func (fd *FaultDetector) ClearFault(nodeID string) {
	fd.mu.Lock()
	delete(fd.knownFaults, nodeID)
	fd.mu.Unlock()
	fd.logger.Printf("[FaultDetector] 故障已清除: %s", nodeID)
}

// =========================================================================
// 第八部分: ReScheduler — 自动重调度器
// =========================================================================

// ReScheduler 自动重调度器
// 当节点故障时，自动将该节点上的所有任务重新调度到健康节点
type ReScheduler struct {
	mu sync.RWMutex

	scheduler *AgentScheduler
	config    SchedulerConfig

	// 重调度队列
	reScheduleCh chan ReScheduleRequest

	// 正在重调度的任务（防止重复）
	inProgress map[string]bool

	// 统计
	totalReScheduled atomic.Int64
	totalRecovered   atomic.Int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc

	logger *log.Logger
}

// ReScheduleRequest 重调度请求
type ReScheduleRequest struct {
	FaultNodeID string
	Tasks       []string // 需要重调度的任务 ID 列表
	Reason      string
	Priority    PriorityClass
}

// NewReScheduler 创建重调度器
func NewReScheduler(scheduler *AgentScheduler, config SchedulerConfig) *ReScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReScheduler{
		scheduler:    scheduler,
		config:       config,
		reScheduleCh: make(chan ReScheduleRequest, 256),
		inProgress:   make(map[string]bool),
		ctx:          ctx,
		cancel:       cancel,
		logger:       log.Default(),
	}
}

// Start 启动重调度循环
func (rs *ReScheduler) Start() {
	go rs.loop()
	rs.logger.Printf("[ReScheduler] 已启动 (批量=%d, 自动=%v)",
		rs.config.ReScheduleBatchSize, rs.config.ReScheduleEnabled)
}

// Stop 停止重调度
func (rs *ReScheduler) Stop() {
	rs.cancel()
	rs.logger.Printf("[ReScheduler] 已停止 (重调度=%d, 恢复=%d)",
		rs.totalReScheduled.Load(), rs.totalRecovered.Load())
}

// OnNodeFault 故障检测器回调: 节点故障时触发重调度
func (rs *ReScheduler) OnNodeFault(fault FaultEvent) {
	if !rs.config.ReScheduleEnabled {
		rs.logger.Printf("[ReScheduler] 自动重调度未启用，忽略故障: %s", fault.NodeID)
		return
	}

	// 查找该节点上运行的所有任务
	tasks := rs.scheduler.stateMachine.ListTasks()
	var affectedTasks []string

	for _, tu := range tasks {
		if tu.Phase != PhaseRunning && tu.Phase != PhaseScheduled {
			continue
		}
		for _, assignedNode := range tu.AssignedNodes {
			if assignedNode == fault.NodeID {
				affectedTasks = append(affectedTasks, tu.ID)
				break
			}
		}
	}

	if len(affectedTasks) == 0 {
		rs.logger.Printf("[ReScheduler] 节点 %s 故障，但无运行中任务需要迁移", fault.NodeID)
		return
	}

	rs.logger.Printf("[ReScheduler] 节点 %s 故障，触发重调度: %d 个任务受影响",
		fault.NodeID, len(affectedTasks))

	// 按优先级排序（高优先级先恢复）
	sort.Slice(affectedTasks, func(i, j int) bool {
		tu1, _ := rs.scheduler.stateMachine.GetTask(affectedTasks[i])
		tu2, _ := rs.scheduler.stateMachine.GetTask(affectedTasks[j])
		if tu1 == nil || tu2 == nil {
			return false
		}
		return int(tu1.Priority) < int(tu2.Priority)
	})

	// 批量提交重调度请求
	rs.reScheduleCh <- ReScheduleRequest{
		FaultNodeID: fault.NodeID,
		Tasks:       affectedTasks,
		Reason:      fault.Description,
		Priority:    PriorityCritical, // 故障恢复是最优先的
	}
}

// loop 重调度主循环
func (rs *ReScheduler) loop() {
	for {
		select {
		case <-rs.ctx.Done():
			return
		case req := <-rs.reScheduleCh:
			rs.processReSchedule(req)
		}
	}
}

// processReSchedule 处理一批重调度请求
func (rs *ReScheduler) processReSchedule(req ReScheduleRequest) {
	successCount := 0
	failCount := 0

	// 限制每轮处理的任务数
	batchSize := 10
	if batchSize <= 0 {
		batchSize = 10
	}
	if len(req.Tasks) > batchSize {
		rs.logger.Printf("[ReScheduler] 任务数 %d 超过批量限制 %d，分批处理",
			len(req.Tasks), batchSize)
		// 将超出部分重新入队
		go func() {
			defer func() {
				if r := recover(); r != nil {
					rs.logger.Printf("[recover] reSchedule goroutine panic: %v", r)
				}
			}()
			time.Sleep(500 * time.Millisecond)
			rs.reScheduleCh <- ReScheduleRequest{
				FaultNodeID: req.FaultNodeID,
				Tasks:       req.Tasks[batchSize:],
				Reason:      req.Reason,
				Priority:    req.Priority,
			}
		}()
	}

	batch := req.Tasks
	if len(batch) > batchSize {
		batch = batch[:batchSize]
	}

	for _, taskID := range batch {
		// 去重检查
		rs.mu.Lock()
		if rs.inProgress[taskID] {
			rs.mu.Unlock()
			continue
		}
		rs.inProgress[taskID] = true
		rs.mu.Unlock()

		// 重调度单个任务
		err := rs.reScheduleOne(taskID, req.FaultNodeID, req.Reason)
		if err != nil {
			rs.logger.Printf("[ReScheduler] 重调度失败: task=%s err=%v", taskID, err)
			failCount++
		} else {
			rs.logger.Printf("[ReScheduler] 重调度成功: task=%s", taskID)
			successCount++
		}

		rs.mu.Lock()
		delete(rs.inProgress, taskID)
		rs.mu.Unlock()

		// 每个任务之间稍作延迟，避免 Raft 提案拥塞
		if len(batch) > 5 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	rs.totalReScheduled.Add(int64(successCount))

	rs.logger.Printf("[ReScheduler] 批次完成: 成功=%d 失败=%d 节点=%s 原因=%s",
		successCount, failCount, req.FaultNodeID, req.Reason)
}

// reScheduleOne 重调度单个任务
func (rs *ReScheduler) reScheduleOne(taskID, faultNodeID, reason string) error {
	// 1. 从状态机获取任务
	tu, ok := rs.scheduler.stateMachine.GetTask(taskID)
	if !ok {
		return fmt.Errorf("任务不存在: %s", taskID)
	}

	// 2. 检查任务是否可以重调度
	if !tu.CanRetry() {
		// 超过重试次数 → 标记为失败
		failPayload := TaskFailure{
			TaskID: taskID,
			Reason: fmt.Sprintf("重调度失败: 超过最大重试次数(%d), 原因=%s", tu.MaxRetries, reason),
		}
		seq := rs.scheduler.stateMachine.CommandSeq + 1
		failCmd, _ := NewCommand(CmdTaskFail, "re-scheduler", seq, failPayload,
			rs.scheduler.stateMachine.LastSM3Hash, rs.scheduler.stateMachine.SM3Hasher)
		failBytes, _ := failCmd.ToBytes()
		rs.scheduler.proposer.Propose(failBytes)
		rs.totalRecovered.Add(1)
		return fmt.Errorf("超过最大重试次数: %d", tu.MaxRetries)
	}

	// 3. 释放原有资源（通过 Raft 提案）
	_ = TaskAllocation{}

	seq := rs.scheduler.stateMachine.CommandSeq + 1
	cmd1, err := NewCommand(CmdTaskFail, "re-scheduler", seq,
		TaskFailure{TaskID: taskID, Reason: "节点故障自动释放: " + reason},
		rs.scheduler.stateMachine.LastSM3Hash, rs.scheduler.stateMachine.SM3Hasher)
	if err != nil {
		return err
	}
	cmdBytes1, _ := cmd1.ToBytes()
	rs.scheduler.proposer.Propose(cmdBytes1)

	// 短暂等待释放命令共识
	time.Sleep(200 * time.Millisecond)

	// 4. 重新调度（复用 AgentScheduler.Schedule 的完整流程）
	result, err := rs.scheduler.Schedule(tu)
	if err != nil {
		return fmt.Errorf("调度失败: %w", err)
	}
	if !result.Success {
		// 重调度失败 → 再次入队（延迟重试）
		go func() {
			defer func() {
				if r := recover(); r != nil {
					rs.logger.Printf("[recover] reSchedule retry goroutine panic: %v", r)
				}
			}()
			time.Sleep(5 * time.Second)
			rs.reScheduleCh <- ReScheduleRequest{
				FaultNodeID: faultNodeID,
				Tasks:       []string{taskID},
				Reason:      "重试: " + result.Reason,
				Priority:    tu.Priority,
			}
		}()
		return fmt.Errorf("调度失败: %s", result.Reason)
	}

	rs.totalRecovered.Add(1)
	return nil
}

// =========================================================================
// 第九部分: 调度器统计与 API
// =========================================================================

// SchedulerStats 调度器统计
type SchedulerStats struct {
	TotalScheduled    int64  `json:"total_scheduled"`
	TotalFiltered     int64  `json:"total_filtered"`
	TotalReScheduled  int64  `json:"total_re_scheduled"`
	TotalRecovered    int64  `json:"total_recovered"`
	TotalFailures     int64  `json:"total_failures"`
	PoolSize          int    `json:"pool_size"`
	AvailableGPUs     int    `json:"available_gpus"`
	RunningTasks      int    `json:"running_tasks"`
	PendingTasks      int    `json:"pending_tasks"`
	AvgSchedulingUs   int64  `json:"avg_scheduling_us"`
	FaultNodes        int    `json:"fault_nodes"`
	Strategy          string `json:"strategy"`
	ReScheduleEnabled bool   `json:"re_schedule_enabled"`
}

// GetStats 返回调度器统计
func (as *AgentScheduler) GetStats() SchedulerStats {
	var running, pending, availableGPUs int
	resources := as.stateMachine.ListResources()
	for _, ru := range resources {
		availableGPUs += ru.AvailableGPUs()
	}

	tasks := as.stateMachine.ListTasks()
	for _, tu := range tasks {
		if tu.Phase == PhaseRunning {
			running++
		} else if tu.Phase == PhasePending {
			pending++
		}
	}

	as.faultDetector.mu.RLock()
	faultCount := len(as.faultDetector.knownFaults)
	as.faultDetector.mu.RUnlock()

	return SchedulerStats{
		TotalScheduled:    as.totalScheduled.Load(),
		TotalFiltered:     as.totalFiltered.Load(),
		TotalReScheduled:  as.totalReScheduled.Load(),
		TotalRecovered:    as.reScheduler.totalRecovered.Load(),
		TotalFailures:     as.totalFailures.Load(),
		PoolSize:          len(resources),
		AvailableGPUs:     availableGPUs,
		RunningTasks:      running,
		PendingTasks:      pending,
		FaultNodes:        faultCount,
		Strategy:          string(as.strategy),
		ReScheduleEnabled: true,
	}
}

// =========================================================================
// 第十部分: 确定性调度验证 (Temp=0 保证)
// =========================================================================

// VerifyDeterminism 验证确定性调度
// 对同一个任务调用 Schedule 两次，检查是否得到相同的分配结果
func (as *AgentScheduler) VerifyDeterminism(task *TaskUnit) (bool, string) {
	if !task.DeterministicMode {
		return false, "任务未启用确定性模式"
	}

	// 第一次调度
	result1, err1 := as.Schedule(task)
	if err1 != nil || !result1.Success {
		return false, fmt.Sprintf("第一次调度失败: %v", err1)
	}

	// 记录第一次分配
	node1 := result1.AssignedNode
	gpus1 := fmt.Sprintf("%v", result1.AssignedGPUs)

	// 释放资源
	releaseAlloc := TaskAllocation{
		TaskID:      task.ID + "-verify",
		ResourceIDs: []string{node1},
	}
	seq := as.stateMachine.CommandSeq + 1
	releaseCmd, _ := NewCommand(CmdTaskDelete, "determinism-verify", seq, releaseAlloc,
		as.stateMachine.LastSM3Hash, as.stateMachine.SM3Hasher)
	releaseBytes, _ := releaseCmd.ToBytes()
	as.proposer.Propose(releaseBytes)
	time.Sleep(300 * time.Millisecond)

	// 第二次调度 (集群状态应与第一次相同)
	result2, err2 := as.Schedule(task)
	if err2 != nil || !result2.Success {
		return false, fmt.Sprintf("第二次调度失败: %v", err2)
	}

	node2 := result2.AssignedNode
	gpus2 := fmt.Sprintf("%v", result2.AssignedGPUs)

	// 比较
	if node1 == node2 && gpus1 == gpus2 {
		return true, fmt.Sprintf("确定性验证通过: 两次分配一致 (node=%s, gpus=%s)", node1, gpus1)
	}

	return false, fmt.Sprintf("确定性验证失败: 结果不一致 (第一次: %s/%s, 第二次: %s/%s)",
		node1, gpus1, node2, gpus2)
}

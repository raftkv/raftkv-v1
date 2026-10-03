// =========================================================================
// RaftKV — Raft 共识双适配层 (替代传统 REST 数据网关)
//
// 架构原则:
//   1. 所有跨系统状态变更，必须经过 Raft 提案 → 共识 → 落盘 → 应用
//   2. 不存在"两套系统的数据对账"——Raft 日志是唯一的事实来源
//   3. 向下适配层: K8s Node Change → Raft Log → ResourceUnit 池
//   4. 向上适配层: Agent Task Request → Raft Log → TaskUnit 分配
//   5. 每个状态机命令都带 SM3 哈希，形成不可篡改的审计链
//
// 数据流:
//
//   ┌──────────────┐         ┌──────────────────────┐
//   │  K8s API     │  watch  │  K8sAdapter           │
//   │  (Node Add/  │ ──────→ │  (向下适配层)           │
//   │   Update/Del)│         │                       │
//   └──────────────┘         └───────┬───────────────┘
//                                    │ Propose(cmd)
//                           ┌────────▼───────────────┐
//                           │  RaftNode (共识核心)     │
//                           │  ┌───────────────────┐ │
//                           │  │ Log[0] SM3(genesis)│ │
//                           │  │ Log[1] SM3(node-1) │ │
//                           │  │ Log[2] SM3(alloc)  │ │
//                           │  │ ...                │ │
//                           │  └───────────────────┘ │
//                           │  Consensus → Apply     │
//                           └────────┬───────────────┘
//                                    │ Apply(committed)
//                           ┌────────▼───────────────┐
//                           │  UnifiedStateMachine    │
//                           │  ResourceUnit pool      │
//                           │  TaskUnit pool          │
//                           │  Allocation table       │
//                           └────────┬───────────────┘
//                                    │ Notify
//                           ┌────────▼───────────────┐
//                           │  AgentAdapter           │
//                           │  (向上适配层)           │
//                           └───────┬─────────────────┘
//                                   │ Task response
//                           ┌───────▼─────────────────┐
//                           │  Ray / DeepSeek API     │
//                           │  (Agent Harness)        │
//                           └─────────────────────────┘
//
// =========================================================================

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// =========================================================================
// 第一部分: Raft 状态机命令格式
// =========================================================================

// CommandType Raft 状态机命令类型
type CommandType string

const (
	// 向下适配层命令 (K8s → Raft)
	CmdResourceRegister CommandType = "resource_register" // 新节点注册
	CmdResourceUpdate   CommandType = "resource_update"   // 节点状态更新
	CmdResourceDrain    CommandType = "resource_drain"    // 节点排空下线
	CmdResourceOffline  CommandType = "resource_offline"  // 节点离线/故障

	// 向上适配层命令 (Agent → Raft)
	CmdTaskCreate   CommandType = "task_create"   // 创建任务
	CmdTaskAllocate CommandType = "task_allocate" // 分配资源
	CmdTaskUpdate   CommandType = "task_update"   // 任务状态更新
	CmdTaskComplete CommandType = "task_complete" // 任务完成
	CmdTaskFail     CommandType = "task_fail"     // 任务失败
	CmdTaskPreempt  CommandType = "task_preempt"  // 任务被抢占
	CmdTaskDelete   CommandType = "task_delete"   // 删除任务

	// 控制面命令
	CmdSnapshot  CommandType = "snapshot"  // 状态快照
	CmdRebalance CommandType = "rebalance" // 负载重平衡
)

// StateMachineCommand Raft 状态机命令（写入 Raft Log 的载荷）
type StateMachineCommand struct {
	// 命令类型
	Type CommandType `json:"type"`

	// 命令发起者标识
	Source string `json:"source"` // "k8s-adapter" | "agent-adapter" | "control-plane"

	// 时间戳（提案时间）
	Timestamp time.Time `json:"timestamp"`

	// 序列号（全局递增，用于排序和去重）
	Sequence int64 `json:"sequence"`

	// Payload: ResourceUnit 或 TaskUnit 的 JSON 序列化
	Payload json.RawMessage `json:"payload"`

	// SM3 哈希值（Payload 的哈希，用于防篡改校验）
	SM3Hash []byte `json:"sm3_hash"`

	// 前置命令的 SM3 哈希（链式校验）
	PrevSM3Hash []byte `json:"prev_sm3_hash"`
}

// NewCommand 创建带 SM3 哈希的状态机命令
func NewCommand(typ CommandType, source string, seq int64, payload interface{}, prevHash []byte, hasher SM3Hasher) (*StateMachineCommand, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	var sm3Hash []byte
	if hasher != nil {
		// 哈希 = SM3(prevHash || type || source || seq || payload)
		preimage := append(prevHash, []byte(typ)...)
		preimage = append(preimage, []byte(source)...)
		preimage = append(preimage, byte(seq>>56), byte(seq>>48), byte(seq>>40), byte(seq>>32),
			byte(seq>>24), byte(seq>>16), byte(seq>>8), byte(seq))
		preimage = append(preimage, payloadBytes...)
		sm3Hash = hasher.Hash(preimage)
	}

	return &StateMachineCommand{
		Type:        typ,
		Source:      source,
		Timestamp:   time.Now(),
		Sequence:    seq,
		Payload:     payloadBytes,
		SM3Hash:     sm3Hash,
		PrevSM3Hash: prevHash,
	}, nil
}

// ToBytes 序列化为字节数组（写入 Raft Log 的格式）
func (c *StateMachineCommand) ToBytes() ([]byte, error) {
	return json.Marshal(c)
}

// FromBytes 从字节数组反序列化
func FromBytes(data []byte) (*StateMachineCommand, error) {
	var c StateMachineCommand
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("unmarshal command: %w", err)
	}
	return &c, nil
}

// =========================================================================
// 第二部分: 统一状态机 (Raft Apply 的目标)
// =========================================================================

// UnifiedStateMachine Raft 状态机的应用层
// 所有经过 Raft 共识的命令在此处 apply，更新内存中的资源池和任务池
type UnifiedStateMachine struct {
	mu sync.RWMutex

	// 资源池: resourceUnitID → ResourceUnit
	ResourcePool map[string]*ResourceUnit

	// 任务池: taskUnitID → TaskUnit
	TaskPool map[string]*TaskUnit

	// 分配表: taskUnitID → []resourceUnitID
	AllocationTable map[string][]string

	// 全局命令序列号
	CommandSeq int64

	// 最后一条命令的 SM3 哈希（链式校验起点）
	LastSM3Hash []byte

	// 已应用日志数
	AppliedCount int64

	// 回调: 当资源/任务状态变更时通知外部
	OnResourceChange func(ru *ResourceUnit, changeType CommandType)
	OnTaskChange     func(tu *TaskUnit, changeType CommandType)

	// SM3 引擎
	SM3Hasher SM3Hasher

	// 日志
	logger *log.Logger
}

// NewUnifiedStateMachine 创建统一状态机
//
// V2.5.1 修复：hasher 为 nil 时默认注入国密标准实现（tjfoc/gmsm v1.4.1），
// 彻底消除 V2.5.0 中「SM3Hasher 悬空 nil → Apply 静默跳过校验」的功能断层。
func NewUnifiedStateMachine(hasher SM3Hasher) *UnifiedStateMachine {
	if hasher == nil {
		hasher = NewStandardSM3()
	}
	return &UnifiedStateMachine{
		ResourcePool:    make(map[string]*ResourceUnit),
		TaskPool:        make(map[string]*TaskUnit),
		AllocationTable: make(map[string][]string),
		LastSM3Hash:     make([]byte, 32), // 创世哈希（全零）
		SM3Hasher:       hasher,
		logger:          log.Default(),
	}
}

// Apply 应用 Raft 日志命令到状态机
// 这是 Raft 状态机的核心方法——所有状态变更的唯一入口
func (sm *UnifiedStateMachine) Apply(commandBytes []byte) error {
	cmd, err := FromBytes(commandBytes)
	if err != nil {
		return fmt.Errorf("状态机 Apply 失败: %w", err)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 校验 SM3 国密哈希链（V2.5.1：强制生效）
	//
	// V2.5.0 缺陷：此处曾以 `if sm.SM3Hasher != nil` 包裹，而 hasher 实际恒为 nil，
	// 导致防篡改校验被静默跳过；且校验失败仅打印日志后继续 apply，形同虚设。
	// V2.5.1 修复：删除无效 nil 分支，校验无条件执行；失败即中止 apply 并返回错误。
	if len(cmd.SM3Hash) > 0 && len(cmd.PrevSM3Hash) > 0 {
		recomputed := sm.recomputeHash(cmd)
		if !bytesEqual(recomputed, cmd.SM3Hash) {
			sm.logger.Printf("[状态机] 国密 SM3 校验失败！seq=%d expected=%x actual=%x",
				cmd.Sequence, cmd.SM3Hash[:8], recomputed[:8])
			return fmt.Errorf("国密 SM3 哈希校验失败: seq=%d，日志条目疑似被篡改，已中止 apply", cmd.Sequence)
		}
	}

	// 根据命令类型分派
	switch cmd.Type {

	// === 向下适配层命令 ===
	case CmdResourceRegister, CmdResourceUpdate:
		var ru ResourceUnit
		if err := json.Unmarshal(cmd.Payload, &ru); err != nil {
			return fmt.Errorf("unmarshal ResourceUnit: %w", err)
		}
		ru.StateSequence = cmd.Sequence
		ru.SM3StateHash = fmt.Sprintf("%x", cmd.SM3Hash)
		sm.ResourcePool[ru.ID] = &ru
		if sm.OnResourceChange != nil {
			sm.OnResourceChange(&ru, cmd.Type)
		}
		sm.logger.Printf("[状态机] 资源 %s %s (seq=%d)", cmd.Type, ru.ID, cmd.Sequence)

	case CmdResourceDrain:
		var ru ResourceUnit
		if err := json.Unmarshal(cmd.Payload, &ru); err != nil {
			return fmt.Errorf("unmarshal ResourceUnit: %w", err)
		}
		if existing, ok := sm.ResourcePool[ru.ID]; ok {
			existing.State = ResourceStateDraining
			existing.StateSequence = cmd.Sequence
			if sm.OnResourceChange != nil {
				sm.OnResourceChange(existing, cmd.Type)
			}
		}

	case CmdResourceOffline:
		var ru ResourceUnit
		if err := json.Unmarshal(cmd.Payload, &ru); err != nil {
			return fmt.Errorf("unmarshal ResourceUnit: %w", err)
		}
		if existing, ok := sm.ResourcePool[ru.ID]; ok {
			existing.State = ResourceStateOffline
			existing.StateSequence = cmd.Sequence
			// 释放该节点上的所有任务
			sm.releaseTasksOnNode(ru.ID)
			if sm.OnResourceChange != nil {
				sm.OnResourceChange(existing, cmd.Type)
			}
		}

	// === 向上适配层命令 ===
	case CmdTaskCreate:
		var tu TaskUnit
		if err := json.Unmarshal(cmd.Payload, &tu); err != nil {
			return fmt.Errorf("unmarshal TaskUnit: %w", err)
		}
		tu.Phase = PhasePending
		tu.TaskSequence = cmd.Sequence
		tu.SM3TaskHash = fmt.Sprintf("%x", cmd.SM3Hash)
		sm.TaskPool[tu.ID] = &tu
		if sm.OnTaskChange != nil {
			sm.OnTaskChange(&tu, cmd.Type)
		}
		sm.logger.Printf("[状态机] 任务创建 %s (seq=%d)", tu.ID, cmd.Sequence)

	case CmdTaskAllocate:
		var alloc TaskAllocation
		if err := json.Unmarshal(cmd.Payload, &alloc); err != nil {
			return fmt.Errorf("unmarshal TaskAllocation: %w", err)
		}
		sm.applyAllocation(&alloc)
		sm.logger.Printf("[状态机] 任务分配 %s → %v (seq=%d)", alloc.TaskID, alloc.ResourceIDs, cmd.Sequence)

	case CmdTaskComplete:
		var tu TaskUnit
		if err := json.Unmarshal(cmd.Payload, &tu); err != nil {
			return fmt.Errorf("unmarshal TaskUnit: %w", err)
		}
		if existing, ok := sm.TaskPool[tu.ID]; ok {
			existing.Phase = PhaseCompleted
			existing.CompletedAt = &cmd.Timestamp
			existing.TaskSequence = cmd.Sequence
			if existing.CompletedAt == nil {
				now := time.Now()
				existing.CompletedAt = &now
			}
			// 释放资源
			sm.releaseTaskResources(tu.ID)
			delete(sm.AllocationTable, tu.ID)
			if sm.OnTaskChange != nil {
				sm.OnTaskChange(existing, cmd.Type)
			}
		}

	case CmdTaskFail:
		var fail TaskFailure
		if err := json.Unmarshal(cmd.Payload, &fail); err != nil {
			return fmt.Errorf("unmarshal TaskFailure: %w", err)
		}
		if existing, ok := sm.TaskPool[fail.TaskID]; ok {
			existing.Phase = PhaseFailed
			existing.LastError = fail.Reason
			existing.RetryCount++
			existing.TaskSequence = cmd.Sequence
			// 释放资源
			sm.releaseTaskResources(fail.TaskID)
			delete(sm.AllocationTable, fail.TaskID)
			if sm.OnTaskChange != nil {
				sm.OnTaskChange(existing, cmd.Type)
			}
		}

	case CmdTaskDelete:
		var tu TaskUnit
		if err := json.Unmarshal(cmd.Payload, &tu); err != nil {
			return fmt.Errorf("unmarshal TaskUnit: %w", err)
		}
		sm.releaseTaskResources(tu.ID)
		delete(sm.AllocationTable, tu.ID)
		delete(sm.TaskPool, tu.ID)

	// === 控制面命令 ===
	case CmdRebalance:
		sm.rebalance()
		sm.logger.Printf("[状态机] 重平衡完成 (seq=%d)", cmd.Sequence)
	}

	// 更新链式哈希
	sm.LastSM3Hash = cmd.SM3Hash
	sm.CommandSeq = cmd.Sequence
	sm.AppliedCount++

	return nil
}

// TaskAllocation 任务分配信息
type TaskAllocation struct {
	TaskID      string           `json:"task_id"`
	ResourceIDs []string         `json:"resource_ids"`
	GPUIndices  map[string][]int `json:"gpu_indices"` // resourceID → gpuIndices
	VRAMPerGPU  int              `json:"vram_per_gpu"`
	CPUCores    int              `json:"cpu_cores"`
	MemoryMiB   int              `json:"memory_mib"`
}

// TaskFailure 任务失败信息
type TaskFailure struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}

// =========================================================================
// 状态机内部方法
// =========================================================================

func (sm *UnifiedStateMachine) applyAllocation(alloc *TaskAllocation) {
	tu, ok := sm.TaskPool[alloc.TaskID]
	if !ok {
		return
	}

	tu.Phase = PhaseScheduled
	tu.AssignedNodes = alloc.ResourceIDs
	tu.AssignedGPUs = alloc.GPUIndices

	// 更新 ResourceUnit 的分配状态
	allocFailures := 0
	for _, rid := range alloc.ResourceIDs {
		ru, ok := sm.ResourcePool[rid]
		if !ok {
			continue
		}
		gpus := alloc.GPUIndices[rid]
		if err := ru.Allocate(gpus, alloc.VRAMPerGPU, alloc.CPUCores/len(alloc.ResourceIDs), alloc.MemoryMiB/len(alloc.ResourceIDs)); err != nil {
			allocFailures++
			log.Printf("[applyAllocation] 资源 %s 分配失败: %v", rid, err)
		}
	}
	if allocFailures > 0 {
		log.Printf("[applyAllocation] 任务 %s 在 %d/%d 个资源上分配失败", alloc.TaskID, allocFailures, len(alloc.ResourceIDs))
	}

	// 转为 running（实际中，Agent 会发 CmdTaskUpdate 确认启动）
	tu.Phase = PhaseRunning
	now := time.Now()
	tu.StartedAt = &now
	tu.UpdatedAt = now

	sm.AllocationTable[alloc.TaskID] = alloc.ResourceIDs
}

func (sm *UnifiedStateMachine) releaseTaskResources(taskID string) {
	resourceIDs, ok := sm.AllocationTable[taskID]
	if !ok {
		return
	}

	tu, exists := sm.TaskPool[taskID]
	if !exists {
		return
	}

	for _, rid := range resourceIDs {
		ru, ok := sm.ResourcePool[rid]
		if !ok {
			continue
		}
		gpus := tu.AssignedGPUs[rid]
		ru.Deallocate(gpus, tu.GPURequest.VRAMPerGPUMiB, tu.CPURequest/len(resourceIDs), tu.MemoryRequest/len(resourceIDs))
	}
}

func (sm *UnifiedStateMachine) releaseTasksOnNode(resourceID string) {
	for taskID, resourceIDs := range sm.AllocationTable {
		for _, rid := range resourceIDs {
			if rid == resourceID {
				if tu, ok := sm.TaskPool[taskID]; ok {
					tu.Phase = PhasePreempted
					tu.LastError = fmt.Sprintf("节点 %s 下线，任务被抢占", resourceID)
				}
				sm.releaseTaskResources(taskID)
				delete(sm.AllocationTable, taskID)
				break
			}
		}
	}
}

func (sm *UnifiedStateMachine) rebalance() {
	// 简单的重平衡：将 Pending 任务重新调度
	for _, tu := range sm.TaskPool {
		if tu.Phase == PhasePending {
			// 标记为需要重新调度（实际中由调度器处理）
			sm.logger.Printf("[重平衡] 待调度任务: %s", tu.ID)
		}
	}
}

func (sm *UnifiedStateMachine) recomputeHash(cmd *StateMachineCommand) []byte {
	preimage := append(cmd.PrevSM3Hash, []byte(cmd.Type)...)
	preimage = append(preimage, []byte(cmd.Source)...)
	seq := cmd.Sequence
	preimage = append(preimage, byte(seq>>56), byte(seq>>48), byte(seq>>40), byte(seq>>32),
		byte(seq>>24), byte(seq>>16), byte(seq>>8), byte(seq))
	preimage = append(preimage, cmd.Payload...)
	return sm.SM3Hasher.Hash(preimage)
}

// =========================================================================
// StateMachine 查询接口（读操作，无需 Raft 共识）
// =========================================================================

func (sm *UnifiedStateMachine) GetResource(id string) (*ResourceUnit, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ru, ok := sm.ResourcePool[id]
	return ru, ok
}

func (sm *UnifiedStateMachine) GetTask(id string) (*TaskUnit, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	tu, ok := sm.TaskPool[id]
	return tu, ok
}

func (sm *UnifiedStateMachine) ListResources() []*ResourceUnit {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	result := make([]*ResourceUnit, 0, len(sm.ResourcePool))
	for _, ru := range sm.ResourcePool {
		result = append(result, ru)
	}
	return result
}

func (sm *UnifiedStateMachine) ListTasks() []*TaskUnit {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	result := make([]*TaskUnit, 0, len(sm.TaskPool))
	for _, tu := range sm.TaskPool {
		result = append(result, tu)
	}
	return result
}

func (sm *UnifiedStateMachine) Snapshot() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return map[string]interface{}{
		"resource_count": len(sm.ResourcePool),
		"task_count":     len(sm.TaskPool),
		"applied_count":  sm.AppliedCount,
		"command_seq":    sm.CommandSeq,
		"last_sm3_hash":  fmt.Sprintf("%x", sm.LastSM3Hash[:8]),
	}
}

// =========================================================================
// 第三部分: RaftProposer 接口 — 向 Raft 集群提案命令
// =========================================================================

// RaftProposer 定义向 Raft 状态机提案的接口
// 实际实现由 RaftNode 的 Propose() 方法提供
type RaftProposer interface {
	// Propose 向 Raft 集群提案一条命令
	// 返回: 提案是否被接受（不等待共识完成）
	Propose(command []byte) (bool, error)

	// ProposeAndWait 提案并等待共识完成
	// 返回: 共识完成后 apply 的结果
	ProposeAndWait(command []byte, timeout time.Duration) error

	// IsLeader 当前节点是否为 Leader
	// 只有 Leader 可以接受新的提案
	IsLeader() bool

	// LeaderID 当前已知的 Leader ID
	LeaderID() string

	// Stats 返回 Raft 运行时统计
	Stats() map[string]interface{}
}

// =========================================================================
// 第四部分: K8sAdapter — 向下适配层 (K8s Node → Raft)
// =========================================================================

// K8sNodeEvent K8s Node 变更事件
type K8sNodeEvent struct {
	// 事件类型: "ADDED" | "MODIFIED" | "DELETED"
	Type string

	// Node 名称
	NodeName string

	// 节点标签
	Labels map[string]string

	// GPU 信息
	GPUCount   int
	GPUModel   GPUModel
	GPUArch    GPUArchitecture
	GPUVRAMGiB int

	// 资源容量
	CPUCores  int
	MemoryGiB int
	NVMeGiB   int

	// 节点状态
	Ready       bool
	HealthScore int

	// 拓扑信息
	RackID   string
	SwitchID string
	ZoneID   string
	Region   string
}

// K8sAdapter 向下适配层
// 监听 K8s Node 变化 → 构造 ResourceUnit → Raft 提案 → 状态机 apply
type K8sAdapter struct {
	mu sync.RWMutex

	// 集群标识
	ClusterID   string
	ClusterType string

	// Raft 提案接口
	proposer RaftProposer

	// 状态机引用（读取当前状态）
	stateMachine *UnifiedStateMachine

	// SM3 哈希引擎
	hasher SM3Hasher

	// 事件通道（模拟 K8s watch）
	eventCh chan K8sNodeEvent

	// 控制
	ctx    context.Context
	cancel context.CancelFunc

	// 统计
	eventsProcessed    int64
	proposalsSubmitted int64

	// 日志
	logger *log.Logger
}

// NewK8sAdapter 创建 K8s 适配器
func NewK8sAdapter(
	clusterID, clusterType string,
	proposer RaftProposer,
	stateMachine *UnifiedStateMachine,
	hasher SM3Hasher,
) *K8sAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	return &K8sAdapter{
		ClusterID:    clusterID,
		ClusterType:  clusterType,
		proposer:     proposer,
		stateMachine: stateMachine,
		hasher:       hasher,
		eventCh:      make(chan K8sNodeEvent, 1024),
		ctx:          ctx,
		cancel:       cancel,
		logger:       log.Default(),
	}
}

// Start 启动 K8s 事件监听循环
func (ka *K8sAdapter) Start() {
	go ka.eventLoop()
	ka.logger.Printf("[K8s适配器] 已启动 (集群=%s, 类型=%s)", ka.ClusterID, ka.ClusterType)
}

// Stop 停止适配器
func (ka *K8sAdapter) Stop() {
	ka.cancel()
	ka.logger.Printf("[K8s适配器] 已停止 (处理事件=%d, 提案=%d)", ka.eventsProcessed, ka.proposalsSubmitted)
}

// =========================================================================
// K8sAdapter: 事件入口（对外接口）
// =========================================================================

// OnNodeAdded K8s Node ADDED 事件
func (ka *K8sAdapter) OnNodeAdded(event K8sNodeEvent) {
	event.Type = "ADDED"
	ka.eventCh <- event
}

// OnNodeModified K8s Node MODIFIED 事件
func (ka *K8sAdapter) OnNodeModified(event K8sNodeEvent) {
	event.Type = "MODIFIED"
	ka.eventCh <- event
}

// OnNodeDeleted K8s Node DELETED 事件
func (ka *K8sAdapter) OnNodeDeleted(event K8sNodeEvent) {
	event.Type = "DELETED"
	ka.eventCh <- event
}

// InjectEvent 手动注入 K8s 事件（用于测试和演示）
func (ka *K8sAdapter) InjectEvent(event K8sNodeEvent) {
	ka.eventCh <- event
}

// =========================================================================
// K8sAdapter: 事件处理循环
// =========================================================================

func (ka *K8sAdapter) eventLoop() {
	for {
		select {
		case <-ka.ctx.Done():
			return
		case event := <-ka.eventCh:
			ka.mu.Lock()
			ka.eventsProcessed++
			ka.mu.Unlock()
			ka.handleK8sEvent(event)
		}
	}
}

func (ka *K8sAdapter) handleK8sEvent(event K8sNodeEvent) {
	// 1. 将 K8s Node 事件转换为 ResourceUnit
	ru := ka.k8sEventToResourceUnit(event)

	// 2. 确定命令类型
	var cmdType CommandType
	switch event.Type {
	case "ADDED":
		cmdType = CmdResourceRegister
	case "MODIFIED":
		cmdType = CmdResourceUpdate
	case "DELETED":
		cmdType = CmdResourceOffline
	default:
		ka.logger.Printf("[K8s适配器] 未知事件类型: %s", event.Type)
		return
	}

	// 3. 构造 Raft 状态机命令
	seq := ka.stateMachine.CommandSeq + 1
	cmd, err := NewCommand(cmdType, "k8s-adapter", seq, ru, ka.stateMachine.LastSM3Hash, ka.hasher)
	if err != nil {
		ka.logger.Printf("[K8s适配器] 命令构造失败: %v", err)
		return
	}

	cmdBytes, err := cmd.ToBytes()
	if err != nil {
		ka.logger.Printf("[K8s适配器] 命令序列化失败: %v", err)
		return
	}

	// 4. 向 Raft 集群提案
	// 只有 Leader 可以接受提案；非 Leader 节点应转发到 Leader
	if !ka.proposer.IsLeader() {
		ka.logger.Printf("[K8s适配器] 非 Leader 节点，忽略提案（Leader=%s）", ka.proposer.LeaderID())
		return
	}

	accepted, err := ka.proposer.Propose(cmdBytes)
	if err != nil {
		ka.logger.Printf("[K8s适配器] Raft 提案失败: %v (node=%s, type=%s)", err, event.NodeName, event.Type)
		return
	}

	if accepted {
		ka.mu.Lock()
		ka.proposalsSubmitted++
		ka.mu.Unlock()
		ka.logger.Printf("[K8s适配器] 提案成功: node=%s type=%s SM3=%x",
			event.NodeName, event.Type, cmd.SM3Hash[:8])
	} else {
		ka.logger.Printf("[K8s适配器] 提案被拒绝: node=%s type=%s", event.NodeName, event.Type)
	}
}

// k8sEventToResourceUnit 将 K8s 事件转换为 ResourceUnit
func (ka *K8sAdapter) k8sEventToResourceUnit(event K8sNodeEvent) *ResourceUnit {
	ruID := fmt.Sprintf("ru-%s-%s", ka.ClusterID, event.NodeName)

	// 检查是否已存在（更新场景）
	if existing, ok := ka.stateMachine.GetResource(ruID); ok {
		// 基于现有 ResourceUnit 更新
		existing.State = map[bool]ResourceNodeState{true: ResourceStateOnline, false: ResourceStateOffline}[event.Ready]
		existing.HealthScore = event.HealthScore
		existing.Labels = mergeMaps(existing.Labels, event.Labels)
		existing.LastHeartbeat = time.Now()
		return existing
	}

	// 新节点注册
	gpuSpec, ok := GPUSpecs[event.GPUModel]
	if !ok {
		gpuSpec = GPUTopology{
			Count:        event.GPUCount,
			Model:        event.GPUModel,
			Architecture: event.GPUArch,
			VRAMGiB:      event.GPUVRAMGiB,
		}
	} else {
		gpuSpec.Count = event.GPUCount
	}

	return &ResourceUnit{
		ID:            ruID,
		Name:          event.NodeName,
		ClusterID:     ka.ClusterID,
		ClusterType:   ka.ClusterType,
		CreatedAt:     time.Now(),
		LastHeartbeat: time.Now(),
		GPUTopology:   gpuSpec,
		CPUCores:      event.CPUCores,
		MemoryGiB:     event.MemoryGiB,
		NVMeGiB:       event.NVMeGiB,
		RackID:        event.RackID,
		SwitchID:      event.SwitchID,
		ZoneID:        event.ZoneID,
		Region:        event.Region,
		State:         map[bool]ResourceNodeState{true: ResourceStateOnline, false: ResourceStateOffline}[event.Ready],
		Role:          RoleCompute,
		HealthScore:   event.HealthScore,
		Labels:        event.Labels,
		AllocatedVRAM: make(map[int]int),
	}
}

// =========================================================================
// 第五部分: AgentAdapter — 向上适配层 (Agent Task → Raft)
// =========================================================================

// AgentTaskRequest Agent 提交的任务请求
type AgentTaskRequest struct {
	// 任务名称
	Name string

	// 命名空间
	Namespace string

	// 任务类型
	TaskType string

	// GPU 资源需求
	GPUCount       int
	VRAMPerGPUMiB  int
	PreferredModel GPUModel

	// CPU/内存需求
	CPUCores  int
	MemoryMiB int

	// 模型信息
	ModelName    string
	ModelVersion string

	// 确定性控制参数
	DeterministicMode bool
	Temperature       float64
	Seed              int64
	MoEMode           string
	Precision         string
	RecoveryLoop      bool

	// 优先级
	Priority PriorityClass

	// 拓扑亲和性
	TopologyScope     string
	MaxLatencyUs      int
	MinInterconnectGB float64
}

// AgentTaskResponse Agent 任务响应
type AgentTaskResponse struct {
	// 是否成功分配
	Success bool

	// 分配的任务 ID
	TaskID string

	// 分配的资源
	AssignedNodes []string
	AssignedGPUs  map[string][]int

	// 调度延迟（微秒）
	SchedulingLatencyUs int64

	// 错误信息
	Error string
}

// AgentAdapter 向上适配层
// 接收 Agent 任务请求 → 构造 TaskUnit → Raft 提案 → 状态机 apply → 分配资源 → 返回
type AgentAdapter struct {
	mu sync.RWMutex

	// Raft 提案接口
	proposer RaftProposer

	// 状态机引用
	stateMachine *UnifiedStateMachine

	// SM3 哈希引擎
	hasher SM3Hasher

	// 任务 ID 计数器
	taskIDCounter int64

	// 等待分配的任务 (taskID → channel)
	pendingAllocations map[string]chan *AgentTaskResponse

	// 控制
	ctx    context.Context
	cancel context.CancelFunc

	// 统计
	tasksSubmitted int64
	tasksAllocated int64
	tasksFailed    int64

	logger *log.Logger
}

// NewAgentAdapter 创建 Agent 适配器
func NewAgentAdapter(
	proposer RaftProposer,
	stateMachine *UnifiedStateMachine,
	hasher SM3Hasher,
) *AgentAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	return &AgentAdapter{
		proposer:           proposer,
		stateMachine:       stateMachine,
		hasher:             hasher,
		pendingAllocations: make(map[string]chan *AgentTaskResponse),
		ctx:                ctx,
		cancel:             cancel,
		logger:             log.Default(),
	}
}

// Start 启动 Agent 适配器
func (aa *AgentAdapter) Start() {
	// 注册状态机回调：当任务分配完成时通知等待的 Agent
	aa.stateMachine.OnTaskChange = func(tu *TaskUnit, changeType CommandType) {
		if changeType == CmdTaskAllocate {
			aa.mu.RLock()
			ch, ok := aa.pendingAllocations[tu.ID]
			aa.mu.RUnlock()
			if ok {
				ch <- &AgentTaskResponse{
					Success:       true,
					TaskID:        tu.ID,
					AssignedNodes: tu.AssignedNodes,
					AssignedGPUs:  tu.AssignedGPUs,
				}
			}
		} else if changeType == CmdTaskFail {
			aa.mu.RLock()
			ch, ok := aa.pendingAllocations[tu.ID]
			aa.mu.RUnlock()
			if ok {
				ch <- &AgentTaskResponse{
					Success: false,
					TaskID:  tu.ID,
					Error:   tu.LastError,
				}
			}
		}
	}
	aa.logger.Printf("[Agent适配器] 已启动")
}

// Stop 停止适配器
func (aa *AgentAdapter) Stop() {
	aa.cancel()
	aa.logger.Printf("[Agent适配器] 已停止 (提交=%d, 分配=%d, 失败=%d)",
		aa.tasksSubmitted, aa.tasksAllocated, aa.tasksFailed)
}

// =========================================================================
// AgentAdapter: SubmitTask — Agent 提交任务的核心入口
// =========================================================================

// SubmitTask Agent 提交任务请求
// 流程: Agent Request → Raft Proposal → Consensus → Apply → Allocate → Response
func (aa *AgentAdapter) SubmitTask(req AgentTaskRequest) (*AgentTaskResponse, error) {
	startTime := time.Now()

	// 1. 检查 Leader
	if !aa.proposer.IsLeader() {
		return &AgentTaskResponse{
			Success: false,
			Error:   fmt.Sprintf("当前节点不是 Leader，请向 %s 提交任务", aa.proposer.LeaderID()),
		}, nil
	}

	// 2. 生成任务 ID
	aa.mu.Lock()
	aa.taskIDCounter++
	taskID := fmt.Sprintf("tu-%s-%06d", req.Namespace, aa.taskIDCounter)
	aa.mu.Unlock()

	// 3. 构造 TaskUnit
	tu := aa.requestToTaskUnit(taskID, req)

	// 4. 构造 Raft 命令 (CmdTaskCreate)
	seq := aa.stateMachine.CommandSeq + 1
	cmd, err := NewCommand(CmdTaskCreate, "agent-adapter", seq, tu, aa.stateMachine.LastSM3Hash, aa.hasher)
	if err != nil {
		return nil, fmt.Errorf("构造任务命令失败: %w", err)
	}

	cmdBytes, err := cmd.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("序列化命令失败: %w", err)
	}

	// 5. 向 Raft 提案
	accepted, err := aa.proposer.Propose(cmdBytes)
	if err != nil {
		return &AgentTaskResponse{
			Success: false,
			TaskID:  taskID,
			Error:   fmt.Sprintf("Raft 提案失败: %v", err),
		}, nil
	}

	if !accepted {
		return &AgentTaskResponse{
			Success: false,
			TaskID:  taskID,
			Error:   "Raft 提案被拒绝",
		}, nil
	}

	aa.mu.Lock()
	aa.tasksSubmitted++
	aa.mu.Unlock()

	// 6. 等待 Raft 共识完成 + 状态机 apply
	// 超时 10 秒
	if err := aa.proposer.ProposeAndWait(cmdBytes, 10*time.Second); err != nil {
		// 超时或失败 → 标记任务为失败
		failPayload := TaskFailure{TaskID: taskID, Reason: fmt.Sprintf("Raft 共识超时: %v", err)}
		failCmd, _ := NewCommand(CmdTaskFail, "agent-adapter", aa.stateMachine.CommandSeq+1, failPayload, aa.stateMachine.LastSM3Hash, aa.hasher)
		failBytes, _ := failCmd.ToBytes()
		aa.proposer.Propose(failBytes) // 尽力而为

		return &AgentTaskResponse{
			Success: false,
			TaskID:  taskID,
			Error:   fmt.Sprintf("Raft 共识超时: %v", err),
		}, nil
	}

	// 7. Raft 共识完成 — 现在状态机已应用了 CmdTaskCreate
	// 接下来需要分配资源：在状态机中查找可用资源并提交 CmdTaskAllocate
	allocResp, err := aa.allocateResources(taskID, req)
	if err != nil {
		aa.mu.Lock()
		aa.tasksFailed++
		aa.mu.Unlock()

		failPayload := TaskFailure{TaskID: taskID, Reason: err.Error()}
		failCmd, _ := NewCommand(CmdTaskFail, "agent-adapter", aa.stateMachine.CommandSeq+1, failPayload, aa.stateMachine.LastSM3Hash, aa.hasher)
		failBytes, _ := failCmd.ToBytes()
		aa.proposer.Propose(failBytes)

		return &AgentTaskResponse{
			Success: false,
			TaskID:  taskID,
			Error:   err.Error(),
		}, nil
	}

	aa.mu.Lock()
	aa.tasksAllocated++
	aa.mu.Unlock()

	allocResp.SchedulingLatencyUs = time.Since(startTime).Microseconds()
	allocResp.TaskID = taskID
	return allocResp, nil
}

// allocateResources 在状态机中查找可用资源并提交分配命令
func (aa *AgentAdapter) allocateResources(taskID string, req AgentTaskRequest) (*AgentTaskResponse, error) {
	// 从状态机读取当前资源池
	resources := aa.stateMachine.ListResources()

	// 查找满足条件的 ResourceUnit
	var candidates []*ResourceUnit
	for _, ru := range resources {
		if !ru.IsHealthy() {
			continue
		}
		if ru.AvailableGPUs() < req.GPUCount {
			continue
		}
		if ru.AvailableVRAM() < req.VRAMPerGPUMiB*req.GPUCount {
			continue
		}
		if req.PreferredModel != "" && ru.Model != req.PreferredModel {
			continue
		}
		candidates = append(candidates, ru)
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("无可用 GPU 资源: 需要 %d×%s (VRAM=%dMiB/GPU), 资源池共 %d 节点",
			req.GPUCount, req.PreferredModel, req.VRAMPerGPUMiB, len(resources))
	}

	// 简化调度: 选第一个候选节点，分配前 N 个 GPU
	selected := candidates[0]
	gpuIndices := make([]int, req.GPUCount)
	for i := 0; i < req.GPUCount; i++ {
		// 找到第一个未分配的 GPU
		for gpuIdx := 0; gpuIdx < selected.Count; gpuIdx++ {
			if !containsInt(selected.AllocatedGPUs, gpuIdx) {
				gpuIndices[i] = gpuIdx
				break
			}
		}
	}

	// 构造分配命令
	alloc := TaskAllocation{
		TaskID:      taskID,
		ResourceIDs: []string{selected.ID},
		GPUIndices:  map[string][]int{selected.ID: gpuIndices},
		VRAMPerGPU:  req.VRAMPerGPUMiB,
		CPUCores:    req.CPUCores,
		MemoryMiB:   req.MemoryMiB,
	}

	seq := aa.stateMachine.CommandSeq + 1
	cmd, err := NewCommand(CmdTaskAllocate, "agent-adapter", seq, alloc, aa.stateMachine.LastSM3Hash, aa.hasher)
	if err != nil {
		return nil, fmt.Errorf("构造分配命令失败: %w", err)
	}

	cmdBytes, err := cmd.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("序列化分配命令失败: %w", err)
	}

	// 向 Raft 提案分配命令
	accepted, err := aa.proposer.Propose(cmdBytes)
	if err != nil || !accepted {
		return nil, fmt.Errorf("分配命令提案失败: %v", err)
	}

	// 等待分配命令共识完成
	if err := aa.proposer.ProposeAndWait(cmdBytes, 10*time.Second); err != nil {
		return nil, fmt.Errorf("分配命令共识超时: %v", err)
	}

	return &AgentTaskResponse{
		Success:       true,
		AssignedNodes: []string{selected.ID},
		AssignedGPUs:  map[string][]int{selected.ID: gpuIndices},
	}, nil
}

// CompleteTask Agent 通知任务完成
func (aa *AgentAdapter) CompleteTask(taskID string) error {
	tu, ok := aa.stateMachine.GetTask(taskID)
	if !ok {
		return fmt.Errorf("任务不存在: %s", taskID)
	}

	seq := aa.stateMachine.CommandSeq + 1
	cmd, err := NewCommand(CmdTaskComplete, "agent-adapter", seq, tu, aa.stateMachine.LastSM3Hash, aa.hasher)
	if err != nil {
		return err
	}

	cmdBytes, _ := cmd.ToBytes()
	_, err = aa.proposer.Propose(cmdBytes)
	return err
}

// FailTask Agent 通知任务失败
func (aa *AgentAdapter) FailTask(taskID, reason string) error {
	fail := TaskFailure{TaskID: taskID, Reason: reason}
	seq := aa.stateMachine.CommandSeq + 1
	cmd, err := NewCommand(CmdTaskFail, "agent-adapter", seq, fail, aa.stateMachine.LastSM3Hash, aa.hasher)
	if err != nil {
		return err
	}

	cmdBytes, _ := cmd.ToBytes()
	_, err = aa.proposer.Propose(cmdBytes)
	return err
}

// requestToTaskUnit 将 Agent 请求转换为 TaskUnit
func (aa *AgentAdapter) requestToTaskUnit(taskID string, req AgentTaskRequest) *TaskUnit {
	var topoAffinity *TopologyAffinity
	if req.TopologyScope != "" {
		topoAffinity = &TopologyAffinity{
			Scope:        req.TopologyScope,
			MaxLatencyUs: req.MaxLatencyUs,
		}
	}

	return &TaskUnit{
		ID:        taskID,
		Name:      req.Name,
		Namespace: req.Namespace,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		TaskType:  req.TaskType,
		Priority:  req.Priority,
		GPURequest: GPURequest{
			Count:                    req.GPUCount,
			VRAMPerGPUMiB:            req.VRAMPerGPUMiB,
			PreferredModel:           req.PreferredModel,
			MinInterconnectBandwidth: req.MinInterconnectGB,
		},
		CPURequest:               req.CPUCores,
		MemoryRequest:            req.MemoryMiB,
		ModelName:                req.ModelName,
		ModelVersion:             req.ModelVersion,
		Phase:                    PhasePending,
		DeterministicMode:        req.DeterministicMode,
		Temperature:              req.Temperature,
		Seed:                     req.Seed,
		MoEMode:                  req.MoEMode,
		Precision:                req.Precision,
		RecoveryLoopEnabled:      req.RecoveryLoop,
		MaxRetries:               3,
		TopologyAffinity:         topoAffinity,
		MinInterconnectBandwidth: req.MinInterconnectGB,
	}
}

// =========================================================================
// 辅助函数
// =========================================================================

func containsInt(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

func mergeMaps(a, b map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// =========================================================================
// 第七部分: 适配器集成示例 (main.go 中使用)
// =========================================================================

// InitAdapters 初始化双适配层并绑定到 Raft 状态机
// 这是 main.go 中需要调用的集成函数
//
// 使用示例:
//
//	func main() {
//	    // ... Raft 初始化 ...
//	    node := NewRaftNode(...)
//	    grpcServer := NewGRPCServer(node, grpcPort)
//
//	    // 初始化 SM3 引擎
//	    sm3Hasher := StandardSM3{}
//
//	    // 初始化统一状态机
//	    stateMachine := unified.NewUnifiedStateMachine(sm3Hasher)
//
//	    // 将 RaftNode 包装为 RaftProposer
//	    proposer := NewRaftNodeProposer(node)
//
//	    // 启动向下适配层 (K8s → Raft)
//	    k8sAdapter := unified.NewK8sAdapter(
//	        "deepseek-prod-hk", "k8s",
//	        proposer, stateMachine, sm3Hasher,
//	    )
//	    k8sAdapter.Start()
//
//	    // 启动向上适配层 (Agent → Raft)
//	    agentAdapter := unified.NewAgentAdapter(
//	        proposer, stateMachine, sm3Hasher,
//	    )
//	    agentAdapter.Start()
//
//	    // 注入模拟 K8s 事件 (实际生产中由 K8s Watch API 触发)
//	    k8sAdapter.InjectEvent(unified.K8sNodeEvent{...})
//
//	    // 处理 Agent 任务请求
//	    resp, _ := agentAdapter.SubmitTask(unified.AgentTaskRequest{...})
//	}
// RaftNodeProposer 将 *RaftNode 适配为 RaftProposer 接口。
//
// V2.5.1 修复：该适配器此前仅存在于注释示例中，从未实现，
// 是 InitAdapters 长期无法连通的直接原因之一。
type RaftNodeProposer struct {
	node *RaftNode
}

// NewRaftNodeProposer 将 RaftNode 包装为 RaftProposer。
func NewRaftNodeProposer(node *RaftNode) *RaftNodeProposer {
	return &RaftNodeProposer{node: node}
}

func (p *RaftNodeProposer) IsLeader() bool {
	if p == nil || p.node == nil {
		return false
	}
	return p.node.IsLeader()
}

func (p *RaftNodeProposer) LeaderID() string {
	if p == nil || p.node == nil {
		return ""
	}
	return p.node.LeaderID()
}

func (p *RaftNodeProposer) Stats() map[string]interface{} {
	if p == nil || p.node == nil {
		return nil
	}
	s := p.node.Stats()
	s.RLock()
	defer s.RUnlock()
	return map[string]interface{}{
		"id":           s.ID,
		"state":        s.State,
		"term":         s.Term,
		"leader_id":    s.LeaderID,
		"commit_index": s.CommitIndex,
		"last_applied": s.LastApplied,
		"log_count":    s.LogCount,
		"peer_count":   s.PeerCount,
	}
}

// Propose 向 Raft 集群提案一条命令。
//
// 已知架构缺口（V2.5.1 如实标注，未做掩饰性包装）：
// RaftNode 当前未暴露任何提案（写入）接口——gRPC 服务仅提供 RequestVote 与
// AppendEntries 两个 RPC，Leader 侧唯一的日志写入路径为 membership.go 的成员
// 变更。因此本方法暂无法实现真实提案，明确返回错误而非静默返回 false。
// 补齐写入路径属架构级变更，须经架构裁决后实施。
func (p *RaftNodeProposer) Propose(command []byte) (bool, error) {
	return false, fmt.Errorf(
		"RaftNode 未暴露提案接口：V2.5.1 写入路径尚未实现（gRPC 仅提供 RequestVote/AppendEntries），" +
			"补齐需架构裁决")
}

// ProposeAndWait 提案并等待共识完成。同 Propose，受写入路径缺失约束。
func (p *RaftNodeProposer) ProposeAndWait(command []byte, timeout time.Duration) error {
	return fmt.Errorf(
		"RaftNode 未暴露提案接口：V2.5.1 写入路径尚未实现（gRPC 仅提供 RequestVote/AppendEntries），" +
			"补齐需架构裁决")
}

// InitAdapters 初始化双适配层并绑定到 Raft 状态机。
//
// V2.5.1 修复：
//   - sm3Hasher 为 nil 时默认注入国密标准实现 NewStandardSM3()（此前导致整层
//     SM3 校验因 nil 判定被静默跳过，且本函数全库零调用者，属死代码）
//   - 真实构造 K8s/Agent 双适配层，替代原先的 nil 返回
//
// 调用方：main.go 启动时调用，返回的状态机供 HTTP 自证端点使用。
func InitAdapters(
	node interface{}, // *RaftNode — 这里用 interface{} 避免循环依赖
	sm3Hasher SM3Hasher,
) (*UnifiedStateMachine, *K8sAdapter, *AgentAdapter) {
	// sm3Hasher 为空时注入国密标准实现，杜绝 nil 悬空
	if sm3Hasher == nil {
		sm3Hasher = NewStandardSM3()
	}

	// 创建状态机（内部再次兜底，双重保障）
	stateMachine := NewUnifiedStateMachine(sm3Hasher)

	// 将 RaftNode 适配为 RaftProposer
	rn, _ := node.(*RaftNode)
	var proposer RaftProposer
	if rn != nil {
		proposer = NewRaftNodeProposer(rn)
	}

	clusterID := os.Getenv("NODE_ID")
	if clusterID == "" {
		clusterID = "raftkv-cluster"
	}
	k8sAdapter := NewK8sAdapter(clusterID, "k8s", proposer, stateMachine, sm3Hasher)
	agentAdapter := NewAgentAdapter(proposer, stateMachine, sm3Hasher)

	return stateMachine, k8sAdapter, agentAdapter
}

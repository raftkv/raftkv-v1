// =========================================================================
// RaftKV 统一资源模型 — DeepSeek 集群 + Agent Harness 双系统控制面
//
// 设计原则:
//   1. ResourceUnit 统一描述异构 GPU 计算节点（K8s Node / Slurm / YARN NodeManager）
//   2. TaskUnit 统一描述推理/训练任务（Ray Actor / Volcano Pod / Slurm Job）
//   3. 两者通过 RaftKV 确定性引擎调度器桥接
//   4. 支持 Temp=0 / MoE=Fixed / Seed=Locked 确定性约束注入
//
// 架构:
//
//   ┌─────────────────────────────────────────────────────┐
//   │              RaftKV 确定性引擎 (控制面)               │
//   │   Raft 强共识 + SM3 哈希链 + DegradationManager     │
//   └──────────┬────────────────────┬─────────────────────┘
//              │                    │
//     ┌────────▼────────┐  ┌───────▼──────────┐
//     │  ResourceUnit   │  │    TaskUnit       │
//     │  (GPU 节点池)    │  │  (推理任务队列)    │
//     └────────┬────────┘  └───────┬──────────┘
//              │                    │
//   ┌──────────▼────────────────────▼─────────────────────┐
//   │         统一调度器 (Unified Scheduler)               │
//   │   Volume-aware + Gang-scheduling + Deterministic    │
//   └──────────┬────────────────────┬─────────────────────┘
//              │                    │
//   ┌──────────▼────────┐  ┌───────▼──────────────────────┐
//   │ DeepSeek 集群      │  │ Agent Harness (Ray/Dist.)    │
//   │ K8s + Volcano      │  │ Ray Core + Ray Serve        │
//   │ A100/H800 × 10K    │  │ Actor Pool + Placement Grp  │
//   └───────────────────┘  └──────────────────────────────┘
//
// =========================================================================

package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// =========================================================================
// 第一部分: 资源能力枚举与常量
// =========================================================================

// GPUArchitecture GPU 微架构代际
type GPUArchitecture string

const (
	GPUArchAmpere    GPUArchitecture = "Ampere"    // A100 (SM80)
	GPUArchHopper    GPUArchitecture = "Hopper"    // H100/H800 (SM90)
	GPUArchBlackwell GPUArchitecture = "Blackwell" // B100/B200 (SM100)
	GPUArchAscend    GPUArchitecture = "Ascend"    // 华为昇腾 910B
	GPUArchCambricon GPUArchitecture = "Cambricon" // 寒武纪 MLU
)

// GPUModel GPU 具体型号
type GPUModel string

const (
	GPU_A100_40GB  GPUModel = "A100-40GB"
	GPU_A100_80GB  GPUModel = "A100-80GB"
	GPU_H800_80GB  GPUModel = "H800-80GB"
	GPU_H100_80GB  GPUModel = "H100-80GB"
	GPU_B200_192GB GPUModel = "B200-192GB"
	GPU_Ascend910B GPUModel = "Ascend-910B"
	GPU_MLU590     GPUModel = "MLU590"
)

// GPUInterconnect GPU 互联拓扑类型
type GPUInterconnect string

const (
	InterconnectNVLink     GPUInterconnect = "NVLink"     // NVIDIA NVLink 4.0 / 5.0
	InterconnectPCIe       GPUInterconnect = "PCIe"       // PCIe 4.0 / 5.0
	InterconnectHCCS       GPUInterconnect = "HCCS"       // 华为 HCCS
	InterconnectInfiniBand GPUInterconnect = "InfiniBand" // IB HDR/NDR
	InterconnectRoCE       GPUInterconnect = "RoCE"       // RDMA over Converged Ethernet
)

// NodeRole 节点在集群中的角色
type NodeRole string

const (
	RoleCompute NodeRole = "compute" // 纯计算节点（GPU Worker）
	RoleControl NodeRole = "control" // 控制面节点（RaftKV 引擎实例）
	RoleStorage NodeRole = "storage" // 存储节点（分布式文件系统）
	RoleEdge    NodeRole = "edge"    // 边缘推理节点
)

// ResourceNodeState 节点运行状态
type ResourceNodeState string

const (
	ResourceStateOnline      ResourceNodeState = "online"      // 正常在线
	ResourceStateDraining    ResourceNodeState = "draining"    // 排空中（准备下线）
	ResourceStateMaintenance ResourceNodeState = "maintenance" // 维护中
	ResourceStateOffline     ResourceNodeState = "offline"     // 离线/故障
	ResourceStateDegraded    ResourceNodeState = "degraded"    // 降级运行（部分 GPU 故障）
)

// TaskPhase 任务生命周期阶段
type TaskPhase string

const (
	PhasePending       TaskPhase = "pending"       // 等待调度
	PhaseScheduled     TaskPhase = "scheduled"     // 已分配节点
	PhaseRunning       TaskPhase = "running"       // 执行中
	PhaseCheckpointing TaskPhase = "checkpointing" // 检查点保存中
	PhaseCompleted     TaskPhase = "completed"     // 成功完成
	PhaseFailed        TaskPhase = "failed"        // 执行失败
	PhasePreempted     TaskPhase = "preempted"     // 被抢占
	PhaseRollingBack   TaskPhase = "rolling_back"  // 回滚中（确定性引擎触发）
)

// PriorityClass 任务优先级
type PriorityClass int

const (
	PriorityCritical   PriorityClass = 0 // P0: 控制面指令
	PriorityHigh       PriorityClass = 1 // P1: 实时推理
	PriorityNormal     PriorityClass = 2 // P2: 批量推理
	PriorityLow        PriorityClass = 3 // P3: 离线训练
	PriorityBestEffort PriorityClass = 4 // P4: 尽力而为
)

// =========================================================================
// 第二部分: GPU 拓扑描述（统一 GPU 资源描述符）
// =========================================================================

// GPUTopology 描述单节点内 GPU 的物理拓扑
type GPUTopology struct {
	// GPU 总数
	Count int `json:"count"`

	// GPU 型号
	Model GPUModel `json:"model"`

	// GPU 架构代际
	Architecture GPUArchitecture `json:"architecture"`

	// 每 GPU 显存 (GiB)
	VRAMGiB int `json:"vram_gib"`

	// GPU-GPU 互联类型
	Interconnect GPUInterconnect `json:"interconnect"`

	// NVLink 带宽 (GB/s)，仅 NVLink 互联有效
	NVLinkBandwidthGBs float64 `json:"nvlink_bandwidth_gbs,omitempty"`

	// NVSwitch 是否可用（全互联拓扑）
	NVSwitch bool `json:"nvswitch"`

	// NUMA 亲和性: GPU 到 NUMA 节点的映射
	// map[gpuIndex]numaNodeID
	GpuToNUMA map[int]int `json:"gpu_to_numa,omitempty"`

	// NVLink 邻接矩阵: 描述 GPU 之间的互联拓扑
	// [i][j] = true 表示 GPU i 和 GPU j 之间直连
	NVLinkTopology [][]bool `json:"nvlink_topology,omitempty"`

	// GPU 物理频率 (MHz)
	ClockMHz int `json:"clock_mhz"`

	// Tensor Core 数量 (每 SM)
	TensorCoresPerSM int `json:"tensor_cores_per_sm"`

	// FP8 / FP16 / BF16 算力 (TFLOPS)
	FP8TFLOPS  float64 `json:"fp8_tflops,omitempty"`
	FP16TFLOPS float64 `json:"fp16_tflops"`
	BF16TFLOPS float64 `json:"bf16_tflops,omitempty"`

	// 是否支持 MIG (Multi-Instance GPU)
	MIGEnabled bool `json:"mig_enabled"`

	// MIG 实例列表（如果启用）
	MIGInstances []MIGSlice `json:"mig_instances,omitempty"`
}

// MIGSlice MIG 实例切片
type MIGSlice struct {
	ID        int `json:"id"`
	GPUIndex  int `json:"gpu_index"`
	MemoryGiB int `json:"memory_gib"`
	SMCount   int `json:"sm_count"`
}

// GPUSpec 统一 GPU 规格查询表
var GPUSpecs = map[GPUModel]GPUTopology{
	GPU_A100_80GB: {
		Count: 8, Model: GPU_A100_80GB, Architecture: GPUArchAmpere,
		VRAMGiB: 80, Interconnect: InterconnectNVLink,
		NVLinkBandwidthGBs: 600, NVSwitch: true,
		FP16TFLOPS: 312, BF16TFLOPS: 312,
		TensorCoresPerSM: 4, MIGEnabled: true,
	},
	GPU_H800_80GB: {
		Count: 8, Model: GPU_H800_80GB, Architecture: GPUArchHopper,
		VRAMGiB: 80, Interconnect: InterconnectNVLink,
		NVLinkBandwidthGBs: 400, NVSwitch: false, // H800 NVLink 带宽被限制
		FP16TFLOPS: 990, FP8TFLOPS: 1980,
		TensorCoresPerSM: 4, MIGEnabled: false,
	},
	GPU_H100_80GB: {
		Count: 8, Model: GPU_H100_80GB, Architecture: GPUArchHopper,
		VRAMGiB: 80, Interconnect: InterconnectNVLink,
		NVLinkBandwidthGBs: 900, NVSwitch: true,
		FP16TFLOPS: 990, FP8TFLOPS: 1980,
		TensorCoresPerSM: 4, MIGEnabled: true,
	},
	GPU_Ascend910B: {
		Count: 8, Model: GPU_Ascend910B, Architecture: GPUArchAscend,
		VRAMGiB: 64, Interconnect: InterconnectHCCS,
		FP16TFLOPS: 320, BF16TFLOPS: 320,
	},
}

// =========================================================================
// 第三部分: ResourceUnit — 统一的算力资源单元 CRD
// =========================================================================

// ResourceUnit 统一算力资源单元
// 映射关系:
//
//	K8s:         1 ResourceUnit ≈ 1 Node (含所有 GPU)
//	Slurm:       1 ResourceUnit ≈ 1 ComputeNode
//	YARN:        1 ResourceUnit ≈ 1 NodeManager
//	Ray:         1 ResourceUnit ≈ 1 Worker Node
type ResourceUnit struct {
	// === CRD 元数据 ===
	// 全局唯一标识符，格式: "ru-{cluster}-{nodeID}"
	ID string `json:"id"`

	// 人类可读名称，如 "prod-gpu-a100-node-042"
	Name string `json:"name"`

	// 所属集群标识
	ClusterID string `json:"cluster_id"`

	// 集群类型: "k8s" | "slurm" | "yarn" | "ray"
	ClusterType string `json:"cluster_type"`

	// 创建时间
	CreatedAt time.Time `json:"created_at"`

	// 最后心跳时间
	LastHeartbeat time.Time `json:"last_heartbeat"`

	// === 硬件规格 ===
	// GPU 拓扑信息
	GPUTopology `json:",inline"`

	// CPU 核数
	CPUCores int `json:"cpu_cores"`

	// 系统内存 (GiB)
	MemoryGiB int `json:"memory_gib"`

	// 本地 NVMe 存储 (GiB)
	NVMeGiB int `json:"nvme_gib"`

	// 网络带宽 (Gbps)
	NetworkBandwidthGbps int `json:"network_bandwidth_gbps"`

	// 网络延迟 (微秒, 到最近交换机)
	NetworkLatencyUs int `json:"network_latency_us"`

	// === 拓扑位置 ===
	// 物理机架 ID
	RackID string `json:"rack_id"`

	// 交换机 ID (TOR)
	SwitchID string `json:"switch_id"`

	// 数据中心可用区
	ZoneID string `json:"zone_id"`

	// 地域
	Region string `json:"region"`

	// === 运行时状态 ===
	// 节点当前状态
	State ResourceNodeState `json:"state"`

	// 节点角色
	Role NodeRole `json:"role"`

	// 健康评分 (0-100)
	HealthScore int `json:"health_score"`

	// 已分配的 GPU 索引列表
	AllocatedGPUs []int `json:"allocated_gpus"`

	// 已分配的 GPU 显存 (MiB)，按 GPU 索引
	AllocatedVRAM map[int]int `json:"allocated_vram"`

	// 已分配的 CPU 核数
	AllocatedCPUs int `json:"allocated_cpus"`

	// 已分配的系统内存 (MiB)
	AllocatedMemory int `json:"allocated_memory"`

	// === RaftKV 确定性控制字段 ===
	// 节点是否被确定性引擎锁定（不允许非确定性任务调度到此节点）
	DeterministicLocked bool `json:"deterministic_locked"`

	// 确定性上下文: 当前节点上运行的确定性任务 ID 列表
	DeterministicTasks []string `json:"deterministic_tasks"`

	// SM3 哈希值（节点状态的链式存证）
	SM3StateHash string `json:"sm3_state_hash"`

	// 节点状态变更序列号（用于链式哈希）
	StateSequence int64 `json:"state_sequence"`

	// === 调度约束 ===
	// 自定义标签 (K8s labels / Slurm features)
	Labels map[string]string `json:"labels"`

	// 节点亲和性污点
	Taints []NodeTaint `json:"taints,omitempty"`

	// 节点允许的优先级范围
	AllowedPriorities []PriorityClass `json:"allowed_priorities"`

	// 互斥锁（用于并发安全的状态更新）
	mu sync.RWMutex `json:"-"`
}

// NodeTaint 节点污点（排斥规则）
type NodeTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Effect string `json:"effect"` // "NoSchedule" | "PreferNoSchedule" | "NoExecute"
}

// =========================================================================
// ResourceUnit 方法集
// =========================================================================

// AvailableGPUs 返回可用 GPU 数量
func (ru *ResourceUnit) AvailableGPUs() int {
	return ru.Count - len(ru.AllocatedGPUs)
}

// AvailableVRAM 返回可用显存总量 (MiB)
func (ru *ResourceUnit) AvailableVRAM() int {
	total := ru.VRAMGiB * 1024
	allocated := 0
	for _, v := range ru.AllocatedVRAM {
		allocated += v
	}
	return total - allocated
}

// GPUUtilization 返回 GPU 利用率 (0.0 - 1.0)
func (ru *ResourceUnit) GPUUtilization() float64 {
	if ru.Count == 0 {
		return 0
	}
	return float64(len(ru.AllocatedGPUs)) / float64(ru.Count)
}

// IsHealthy 判断节点是否健康（可被调度）
func (ru *ResourceUnit) IsHealthy() bool {
	return ru.State == ResourceStateOnline && ru.HealthScore >= 60
}

// CanSchedule 判断是否可以部署指定 GPU 需求的任务
func (ru *ResourceUnit) CanSchedule(gpuCount int, vramPerGPU int, cpuCores int, memoryMiB int) bool {
	if !ru.IsHealthy() {
		return false
	}
	if ru.AvailableGPUs() < gpuCount {
		return false
	}
	availableVRAM := ru.AvailableVRAM()
	if availableVRAM < vramPerGPU*gpuCount {
		return false
	}
	if ru.CPUCores-ru.AllocatedCPUs < cpuCores {
		return false
	}
	if (ru.MemoryGiB*1024)-ru.AllocatedMemory < memoryMiB {
		return false
	}
	return true
}

// Allocate 在节点上分配资源
func (ru *ResourceUnit) Allocate(gpuIndices []int, vramPerGPU int, cpus int, memoryMiB int) error {
	ru.mu.Lock()
	defer ru.mu.Unlock()

	for _, idx := range gpuIndices {
		if contains(ru.AllocatedGPUs, idx) {
			return fmt.Errorf("GPU %d 已被分配", idx)
		}
		ru.AllocatedGPUs = append(ru.AllocatedGPUs, idx)
		if ru.AllocatedVRAM == nil {
			ru.AllocatedVRAM = make(map[int]int)
		}
		ru.AllocatedVRAM[idx] += vramPerGPU
	}
	ru.AllocatedCPUs += cpus
	ru.AllocatedMemory += memoryMiB
	return nil
}

// Deallocate 释放节点上的资源
func (ru *ResourceUnit) Deallocate(gpuIndices []int, vramPerGPU int, cpus int, memoryMiB int) {
	ru.mu.Lock()
	defer ru.mu.Unlock()

	for _, idx := range gpuIndices {
		ru.AllocatedGPUs = removeInt(ru.AllocatedGPUs, idx)
		if ru.AllocatedVRAM != nil {
			ru.AllocatedVRAM[idx] -= vramPerGPU
			if ru.AllocatedVRAM[idx] <= 0 {
				delete(ru.AllocatedVRAM, idx)
			}
		}
	}
	ru.AllocatedCPUs -= cpus
	if ru.AllocatedCPUs < 0 {
		ru.AllocatedCPUs = 0
	}
	ru.AllocatedMemory -= memoryMiB
	if ru.AllocatedMemory < 0 {
		ru.AllocatedMemory = 0
	}
}

// ToJSON 序列化为 JSON
func (ru *ResourceUnit) ToJSON() ([]byte, error) {
	ru.mu.RLock()
	defer ru.mu.RUnlock()
	return json.Marshal(ru)
}

// FromK8sNode 从 K8s Node 对象映射为 ResourceUnit
func FromK8sNode(nodeName, clusterID string, gpuTopo GPUTopology, cpuCores, memoryGiB int, labels map[string]string) *ResourceUnit {
	return &ResourceUnit{
		ID:            fmt.Sprintf("ru-%s-%s", clusterID, nodeName),
		Name:          nodeName,
		ClusterID:     clusterID,
		ClusterType:   "k8s",
		CreatedAt:     time.Now(),
		LastHeartbeat: time.Now(),
		GPUTopology:   gpuTopo,
		CPUCores:      cpuCores,
		MemoryGiB:     memoryGiB,
		NVMeGiB:       0, // 从 K8s Node status 提取 ephemeral-storage
		State:         ResourceStateOnline,
		Role:          RoleCompute,
		HealthScore:   100,
		Labels:        labels,
		AllocatedVRAM: make(map[int]int),
	}
}

// =========================================================================
// 第四部分: TaskUnit — 统一的推理/训练任务单元 CRD
// =========================================================================

// TaskUnit 统一推理/训练任务单元
// 映射关系:
//
//	Ray:         1 TaskUnit ≈ 1 Actor / 1 Ray Task
//	K8s:         1 TaskUnit ≈ 1 Pod
//	Slurm:       1 TaskUnit ≈ 1 Job Step
//	Volcano:     1 TaskUnit ≈ 1 PodGroup 成员
type TaskUnit struct {
	// === CRD 元数据 ===
	// 全局唯一标识符，格式: "tu-{namespace}-{taskID}"
	ID string `json:"id"`

	// 人类可读名称
	Name string `json:"name"`

	// 所属命名空间/项目
	Namespace string `json:"namespace"`

	// 创建时间
	CreatedAt time.Time `json:"created_at"`

	// 更新时间
	UpdatedAt time.Time `json:"updated_at"`

	// === 任务规格 ===
	// 任务类型: "inference" | "training" | "checkpoint" | "control"
	TaskType string `json:"task_type"`

	// 优先级
	Priority PriorityClass `json:"priority"`

	// GPU 需求
	GPURequest GPURequest `json:"gpu_request"`

	// CPU 需求 (核数)
	CPURequest int `json:"cpu_request"`

	// 内存需求 (MiB)
	MemoryRequest int `json:"memory_request"`

	// === 模型信息 ===
	// 模型名称，如 "DeepSeek-V3" "DeepSeek-R1"
	ModelName string `json:"model_name"`

	// 模型版本/Checkpoint ID
	ModelVersion string `json:"model_version"`

	// 模型大小 (参数量)
	ModelSizeParams string `json:"model_size"` // "671B" "236B" "7B"

	// 模型分片策略
	// "tp8_pp16" = Tensor Parallel 8 + Pipeline Parallel 16
	ParallelismStrategy string `json:"parallelism_strategy"`

	// 推理框架: "vLLM" | "SGLang" | "TRT-LLM" | "DeepSpeed"
	InferenceFramework string `json:"inference_framework"`

	// === 调度约束 ===
	// 必须满足的节点选择器
	NodeSelector map[string]string `json:"node_selector,omitempty"`

	// 拓扑亲和性
	TopologyAffinity *TopologyAffinity `json:"topology_affinity,omitempty"`

	// 拓扑反亲和性（不和哪些任务共置）
	TopologyAntiAffinity []string `json:"topology_anti_affinity,omitempty"`

	// 需要的 GPU 互联带宽 (GB/s)
	MinInterconnectBandwidth float64 `json:"min_interconnect_bandwidth"`

	// === 运行时状态 ===
	// 当前阶段
	Phase TaskPhase `json:"phase"`

	// 已分配的 ResourceUnit ID 列表
	AssignedNodes []string `json:"assigned_nodes"`

	// 已分配的具体 GPU 索引 map[nodeID][]gpuIndex
	AssignedGPUs map[string][]int `json:"assigned_gpus"`

	// 开始执行时间
	StartedAt *time.Time `json:"started_at,omitempty"`

	// 完成时间
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// 重试次数
	RetryCount int `json:"retry_count"`

	// 最大重试次数
	MaxRetries int `json:"max_retries"`

	// 最后一次错误信息
	LastError string `json:"last_error,omitempty"`

	// === 性能指标 ===
	// Token 生成速率 (tokens/s)
	TokensPerSecond float64 `json:"tokens_per_second"`

	// 首 Token 延迟 (ms)
	FirstTokenLatencyMs float64 `json:"first_token_latency_ms"`

	// P99 延迟 (ms)
	P99LatencyMs float64 `json:"p99_latency_ms"`

	// GPU 利用率 (0.0 - 1.0)
	GPUUtilization float64 `json:"gpu_utilization"`

	// === RaftKV 确定性控制字段 ===
	// 是否启用确定性推理 (Temp=0)
	DeterministicMode bool `json:"deterministic_mode"`

	// 推理温度参数 (0.0 = 确定性, 1.0 = 随机)
	Temperature float64 `json:"temperature"`

	// 随机种子 (确定性模式下固定)
	Seed int64 `json:"seed"`

	// MoE 路由模式: "fixed" | "balanced" | "adaptive"
	MoEMode string `json:"moe_mode"`

	// FP 精度: "fp8" | "fp16" | "bf16" | "fp32" | "fp64"
	Precision string `json:"precision"`

	// 单步成功率 (%)
	StepSuccessRate float64 `json:"step_success_rate"`

	// 精度评分 (%)
	PrecisionScore float64 `json:"precision_score"`

	// 准确率 (%)
	AccuracyScore float64 `json:"accuracy_score"`

	// E2E 确定性评分
	E2EDeterminismScore float64 `json:"e2e_determinism_score"`

	// Recovery Loop 是否启用
	RecoveryLoopEnabled bool `json:"recovery_loop_enabled"`

	// 恢复循环增益 (%)
	RecoveryGainPercent float64 `json:"recovery_gain_percent"`

	// SM3 哈希值（任务参数的链式存证）
	SM3TaskHash string `json:"sm3_task_hash"`

	// 任务参数变更序列号
	TaskSequence int64 `json:"task_sequence"`

	// === 元数据 ===
	// 自定义注解
	Annotations map[string]string `json:"annotations,omitempty"`

	// 互斥锁
	mu sync.RWMutex `json:"-"`
}

// GPURequest GPU 资源请求规格
type GPURequest struct {
	// 需要的 GPU 数量
	Count int `json:"count"`

	// 每 GPU 需要的显存 (MiB)
	VRAMPerGPUMiB int `json:"vram_per_gpu_mib"`

	// 优先的 GPU 型号（空表示不限制）
	PreferredModel GPUModel `json:"preferred_model,omitempty"`

	// 必须满足的 GPU 架构（空表示不限制）
	RequiredArchitecture GPUArchitecture `json:"required_architecture,omitempty"`

	// GPU 间需要的最小互联带宽 (GB/s)
	MinInterconnectBandwidth float64 `json:"min_interconnect_bandwidth,omitempty"`

	// 是否要求 NVSwitch 全互联
	RequireNVSwitch bool `json:"require_nvswitch"`
}

// TopologyAffinity 拓扑亲和性约束
type TopologyAffinity struct {
	// 亲和策略: "same_rack" | "same_switch" | "same_zone" | "any"
	Scope string `json:"scope"`

	// 最大跨节点延迟 (微秒)
	MaxLatencyUs int `json:"max_latency_us"`
}

// =========================================================================
// TaskUnit 方法集
// =========================================================================

// IsDeterministic 判断是否为确定性任务
func (tu *TaskUnit) IsDeterministic() bool {
	return tu.DeterministicMode && tu.Temperature == 0 && tu.MoEMode == "fixed"
}

// TransitionPhase 状态转换
func (tu *TaskUnit) TransitionPhase(newPhase TaskPhase, errMsg string) {
	tu.mu.Lock()
	defer tu.mu.Unlock()

	oldPhase := tu.Phase
	tu.Phase = newPhase
	tu.UpdatedAt = time.Now()

	switch newPhase {
	case PhaseRunning:
		now := time.Now()
		tu.StartedAt = &now
	case PhaseCompleted, PhaseFailed:
		now := time.Now()
		tu.CompletedAt = &now
		if newPhase == PhaseFailed {
			tu.LastError = errMsg
			tu.RetryCount++
		}
	}

	// 日志: 阶段转换
	fmt.Printf("[TaskUnit] %s: %s → %s (err=%s)\n", tu.ID, oldPhase, newPhase, errMsg)
}

// CanRetry 判断是否可以重试
func (tu *TaskUnit) CanRetry() bool {
	return tu.RetryCount < tu.MaxRetries
}

// ToRayPlacementGroup 转换为 Ray PlacementGroup 资源描述
func (tu *TaskUnit) ToRayPlacementGroup() map[string]interface{} {
	bundles := make([]map[string]interface{}, tu.GPURequest.Count)
	for i := 0; i < tu.GPURequest.Count; i++ {
		bundles[i] = map[string]interface{}{
			"GPU":    1,
			"CPU":    tu.CPURequest / tu.GPURequest.Count,
			"memory": tu.MemoryRequest / tu.GPURequest.Count / (1024 * 1024), // 转换为 GB
		}
	}
	return map[string]interface{}{
		"bundles":          bundles,
		"strategy":         "STRICT_PACK",
		"name":             tu.ID,
		"detached":         false,
		"max_cpu_fraction": 1.0,
	}
}

// ToVolcanoPodSpec 转换为 Volcano Pod 资源规格 (JSON)
func (tu *TaskUnit) ToVolcanoPodSpec() map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":        tu.ID,
			"namespace":   tu.Namespace,
			"annotations": tu.Annotations,
		},
		"spec": map[string]interface{}{
			"schedulerName": "volcano",
			"containers": []map[string]interface{}{
				{
					"name":  "inference",
					"image": "deepseek/v3-inference:latest",
					"resources": map[string]interface{}{
						"limits": map[string]interface{}{
							"nvidia.com/gpu": tu.GPURequest.Count,
							"cpu":            fmt.Sprintf("%d", tu.CPURequest),
							"memory":         fmt.Sprintf("%dMi", tu.MemoryRequest),
						},
					},
					"env": []map[string]interface{}{
						{"name": "TEMPERATURE", "value": fmt.Sprintf("%.1f", tu.Temperature)},
						{"name": "SEED", "value": fmt.Sprintf("%d", tu.Seed)},
						{"name": "MOE_MODE", "value": tu.MoEMode},
						{"name": "PRECISION", "value": tu.Precision},
					},
				},
			},
		},
	}
}

// ToJSON 序列化
func (tu *TaskUnit) ToJSON() ([]byte, error) {
	tu.mu.RLock()
	defer tu.mu.RUnlock()
	return json.Marshal(tu)
}

// =========================================================================
// 第五部分: 统一调度器接口
// =========================================================================

// SchedulerDecision 调度决策结果
type SchedulerDecision struct {
	// 是否调度成功
	Success bool `json:"success"`

	// 分配方案: map[resourceUnitID][]gpuIndex
	Allocation map[string][]int `json:"allocation"`

	// 失败原因
	Reason string `json:"reason,omitempty"`

	// 备选 ResourceUnit ID 列表（按优先级排序）
	Alternatives []string `json:"alternatives,omitempty"`

	// 调度延迟 (微秒)
	LatencyUs int64 `json:"latency_us"`

	// SM3 哈希值（调度决策的链式存证）
	DecisionHash string `json:"decision_hash"`
}

// UnifiedScheduler 统一调度器接口
type UnifiedScheduler interface {
	// Schedule 将 TaskUnit 调度到 ResourceUnit 池
	Schedule(task *TaskUnit, pool []*ResourceUnit) (*SchedulerDecision, error)

	// Preempt 抢占低优先级任务
	Preempt(victimTaskID string) error

	// Drain 排空指定节点的所有任务
	Drain(nodeID string) ([]string, error)

	// Rebalance 重新平衡集群负载
	Rebalance() (int, error)

	// GetPlacement 查询指定任务的当前分配
	GetPlacement(taskID string) (map[string][]int, error)
}

// =========================================================================
// 第六部分: 双系统映射函数 (DeepSeek ↔ Agent Harness)
// =========================================================================

// MapResourceUnitToK8sNode 将 ResourceUnit 映射为 K8s Node 状态描述
func MapResourceUnitToK8sNode(ru *ResourceUnit) map[string]interface{} {
	return map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":   ru.Name,
			"labels": ru.Labels,
		},
		"status": map[string]interface{}{
			"capacity": map[string]interface{}{
				"nvidia.com/gpu": fmt.Sprintf("%d", ru.Count),
				"cpu":            fmt.Sprintf("%d", ru.CPUCores),
				"memory":         fmt.Sprintf("%dGi", ru.MemoryGiB),
			},
			"allocatable": map[string]interface{}{
				"nvidia.com/gpu": fmt.Sprintf("%d", ru.AvailableGPUs()),
				"cpu":            fmt.Sprintf("%d", ru.CPUCores-ru.AllocatedCPUs),
				"memory":         fmt.Sprintf("%dGi", (ru.MemoryGiB*1024-ru.AllocatedMemory)/1024),
			},
			"conditions": []map[string]interface{}{
				{
					"type":   "Ready",
					"status": map[bool]string{true: "True", false: "False"}[ru.IsHealthy()],
				},
			},
		},
	}
}

// MapResourceUnitToRayNode 将 ResourceUnit 映射为 Ray Node 描述
func MapResourceUnitToRayNode(ru *ResourceUnit) map[string]interface{} {
	return map[string]interface{}{
		"NodeID":   ru.ID,
		"NodeName": ru.Name,
		"Alive":    ru.IsHealthy(),
		"Resources": map[string]float64{
			"GPU":              float64(ru.AvailableGPUs()),
			"CPU":              float64(ru.CPUCores - ru.AllocatedCPUs),
			"memory":           float64(ru.AvailableVRAM() * 1024 * 1024), // bytes
			"accelerator_type": 0,                                         // 使用自定义资源标签
		},
		"Labels": ru.Labels,
	}
}

// MapTaskUnitToRayActorSpec 将 TaskUnit 映射为 Ray Actor 规格
func MapTaskUnitToRayActorSpec(tu *TaskUnit) map[string]interface{} {
	spec := map[string]interface{}{
		"num_gpus":     tu.GPURequest.Count,
		"num_cpus":     float64(tu.CPURequest),
		"memory":       float64(tu.MemoryRequest * 1024 * 1024), // bytes
		"max_restarts": tu.MaxRetries,
		"runtime_env": map[string]interface{}{
			"env_vars": map[string]string{
				"TEMPERATURE":   fmt.Sprintf("%.1f", tu.Temperature),
				"SEED":          fmt.Sprintf("%d", tu.Seed),
				"MOE_MODE":      tu.MoEMode,
				"PRECISION":     tu.Precision,
				"DETERMINISTIC": fmt.Sprintf("%v", tu.DeterministicMode),
				"MODEL_NAME":    tu.ModelName,
				"MODEL_VERSION": tu.ModelVersion,
				"PARALLELISM":   tu.ParallelismStrategy,
			},
		},
	}
	if tu.TopologyAffinity != nil {
		spec["placement_group"] = tu.TopologyAffinity.Scope
	}
	return spec
}

// MapTaskUnitToDeepSeekRequest 将 TaskUnit 映射为 DeepSeek 推理请求
func MapTaskUnitToDeepSeekRequest(tu *TaskUnit, prompt string, maxTokens int) map[string]interface{} {
	return map[string]interface{}{
		"model":             tu.ModelName,
		"messages":          []map[string]string{{"role": "user", "content": prompt}},
		"temperature":       tu.Temperature,
		"seed":              tu.Seed,
		"max_tokens":        maxTokens,
		"top_p":             1.0,
		"frequency_penalty": 0,
		"presence_penalty":  0,
		"stream":            false,
		// RaftKV 控制面扩展字段
		"__raftkv__": map[string]interface{}{
			"task_id":            tu.ID,
			"deterministic_mode": tu.DeterministicMode,
			"moe_mode":           tu.MoEMode,
			"precision":          tu.Precision,
			"sm3_task_hash":      tu.SM3TaskHash,
			"recovery_loop":      tu.RecoveryLoopEnabled,
		},
	}
}

// =========================================================================
// 第七部分: 辅助函数
// =========================================================================

func contains(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

func removeInt(slice []int, val int) []int {
	result := make([]int, 0, len(slice))
	for _, v := range slice {
		if v != val {
			result = append(result, v)
		}
	}
	return result
}

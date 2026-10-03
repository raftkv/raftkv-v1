// =========================================================================
// RaftKV — 国产算力优先弹性池化策略
//
// 核心设计:
//   1. ChipOrigin 标签化: 所有 ResourceUnit 标注国产/海外 + 供应商
//   2. DeterministicConstraint: TaskUnit 携带确定性芯片约束
//   3. PoolingStrategy 可插拔接口: DomesticFirst / Balanced / OverseasOnly
//   4. 平滑降级: 国产不足 → 国产异构 → 海外同型号 → 海外异构
//   5. 全配置驱动: SchedulerConfig 控制所有行为，无需改代码
//
// 降级阶梯 (Tiered Fallback):
//
//   Tier 1: 国产同型号  (e.g. Agent→Ascend910B, Node→Ascend910B)
//   Tier 2: 国产异构     (e.g. Agent→Ascend910B, Node→MLU590)
//   Tier 3: 海外同型号  (e.g. Agent→Ascend910B, Node→H800)
//   Tier 4: 海外异构     (e.g. Agent→Ascend910B, Node→A100)
//   Tier 5: 拒绝分配    (无可用资源)
//
// 使用方式:
//
//   config := DefaultSchedulerConfig()
//   config.PoolingStrategy = "domestic_first"
//   config.DomesticFirst = DomesticFirstConfig{
//       Enabled:    true,
//       StrictMode: false,  // false = 国产不足时自动降级到海外
//       PreferredVendors: []ChipVendor{VendorAscend, VendorCambricon},
//       MaxFallbackTier:   4,   // 最多降到 Tier 4
//   }
//
// =========================================================================

package main

import (
	"fmt"
	"sort"
)

// =========================================================================
// 第一部分: 芯片来源与供应商枚举
// =========================================================================

// ChipOrigin 芯片来源（国产 / 海外）
type ChipOrigin string

const (
	OriginDomestic ChipOrigin = "domestic" // 国产芯片
	OriginOverseas ChipOrigin = "overseas" // 海外芯片
)

// ChipVendor 芯片供应商
type ChipVendor string

const (
	// 国产
	VendorAscend    ChipVendor = "Huawei_Ascend"  // 华为昇腾
	VendorCambricon ChipVendor = "Cambricon_MLU"  // 寒武纪 MLU
	VendorHygon     ChipVendor = "Hygon_DCU"      // 海光 DCU
	VendorIluvatar  ChipVendor = "Iluvatar_Corex" // 天数智芯
	VendorBiren     ChipVendor = "Biren_BR100"    // 壁仞科技
	VendorEnflame   ChipVendor = "Enflame_DTU"    // 燧原科技

	// 海外
	VendorNVIDIA ChipVendor = "NVIDIA" // NVIDIA
	VendorAMD    ChipVendor = "AMD"    // AMD Instinct
	VendorIntel  ChipVendor = "Intel"  // Intel Gaudi
)

// GetChipOrigin 根据供应商返回来源
func GetChipOrigin(vendor ChipVendor) ChipOrigin {
	switch vendor {
	case VendorAscend, VendorCambricon, VendorHygon, VendorIluvatar, VendorBiren, VendorEnflame:
		return OriginDomestic
	default:
		return OriginOverseas
	}
}

// GetVendorForModel 根据 GPU 型号返回供应商
func GetVendorForModel(model GPUModel) ChipVendor {
	switch model {
	case GPU_Ascend910B:
		return VendorAscend
	case GPU_MLU590:
		return VendorCambricon
	case GPU_A100_40GB, GPU_A100_80GB, GPU_H800_80GB, GPU_H100_80GB, GPU_B200_192GB:
		return VendorNVIDIA
	default:
		return VendorNVIDIA // 默认 NVIDIA
	}
}

// IsDomesticModel 判断 GPU 型号是否为国产
func IsDomesticModel(model GPUModel) bool {
	return GetChipOrigin(GetVendorForModel(model)) == OriginDomestic
}

// =========================================================================
// 第二部分: ResourceUnit 扩展 — ChipMetadata
// =========================================================================

// ChipMetadata 芯片元数据（附加到 ResourceUnit）
type ChipMetadata struct {
	// 芯片来源
	Origin ChipOrigin `json:"origin"`

	// 供应商
	Vendor ChipVendor `json:"vendor"`

	// 国产信创兼容性级别 (0-100)
	// 100 = 完全信创兼容 (鲲鹏+openEuler+达梦+SM系列)
	// 50  = 部分兼容
	// 0   = 不涉及信创
	XinchuangScore int `json:"xinchuang_score"`

	// 是否通过了国密 SM2/SM3/SM4 硬件加速认证
	SMAccelerated bool `json:"sm_accelerated"`

	// 国产算子库支持: "CANN" (昇腾) / "BANG" (寒武纪) / "DTK" (海光) / ""
	OperatorSDK string `json:"operator_sdk"`

	// 等效算力比 (相对于 NVIDIA H800 的 FP16 TFLOPS 比值)
	// H800 = 1.0, Ascend910B ≈ 0.32 (320/990), A100 ≈ 0.32 (312/990)
	ComputeEquivalence float64 `json:"compute_equivalence"`
}

// DefaultChipMetadata 根据 GPU 型号获取默认芯片元数据
func DefaultChipMetadata(model GPUModel) ChipMetadata {
	switch model {
	case GPU_Ascend910B:
		return ChipMetadata{
			Origin: OriginDomestic, Vendor: VendorAscend,
			XinchuangScore: 95, SMAccelerated: true,
			OperatorSDK: "CANN", ComputeEquivalence: 0.32,
		}
	case GPU_MLU590:
		return ChipMetadata{
			Origin: OriginDomestic, Vendor: VendorCambricon,
			XinchuangScore: 85, SMAccelerated: true,
			OperatorSDK: "BANG", ComputeEquivalence: 0.25,
		}
	case GPU_H800_80GB:
		return ChipMetadata{
			Origin: OriginOverseas, Vendor: VendorNVIDIA,
			XinchuangScore: 0, SMAccelerated: false,
			ComputeEquivalence: 1.0,
		}
	case GPU_H100_80GB:
		return ChipMetadata{
			Origin: OriginOverseas, Vendor: VendorNVIDIA,
			XinchuangScore: 0, SMAccelerated: false,
			ComputeEquivalence: 1.0,
		}
	case GPU_A100_80GB:
		return ChipMetadata{
			Origin: OriginOverseas, Vendor: VendorNVIDIA,
			XinchuangScore: 0, SMAccelerated: false,
			ComputeEquivalence: 0.32,
		}
	default:
		return ChipMetadata{
			Origin: OriginOverseas, Vendor: VendorNVIDIA,
			ComputeEquivalence: 0.5,
		}
	}
}

// =========================================================================
// 第三部分: DeterministicConstraint — 任务级确定性芯片约束
// =========================================================================

// DeterministicConstraint Agent 任务携带的确定性芯片约束
// 附加到 TaskUnit 上，影响调度器的 PoolingStrategy 决策
type DeterministicConstraint struct {
	// 是否优先国产芯片
	PreferDomestic bool `json:"prefer_domestic"`

	// 允许的国产供应商列表（空 = 不限制）
	AllowedDomesticVendors []ChipVendor `json:"allowed_domestic_vendors,omitempty"`

	// 允许的海外供应商列表（空 = 不限制）
	AllowedOverseasVendors []ChipVendor `json:"allowed_overseas_vendors,omitempty"`

	// 严格模式: true = 绝不用海外芯片
	StrictMode bool `json:"strict_mode"`

	// 最大降级层数 (1-5)
	// 1 = 仅国产同型号
	// 2 = 国产同型号 → 国产异构
	// 3 = 国产同型号 → 国产异构 → 海外同型号
	// 4 = 国产同型号 → 国产异构 → 海外同型号 → 海外异构
	// 5 = 任意可用
	MaxFallbackTier int `json:"max_fallback_tier"`

	// 最大可接受的等效算力损失比例
	// 0.30 = 最多接受 30% 性能损失, 即等效算力比 ≥ 0.70
	MaxComputeLoss float64 `json:"max_compute_loss"`

	// 是否要求 SM 国密硬件加速
	RequireSMAcceleration bool `json:"require_sm_acceleration"`

	// 最小信创兼容分数 (0-100)
	MinXinchuangScore int `json:"min_xinchuang_score"`

	// 是否允许跨供应商混合调度（一个任务的不同分片用不同供应商的 GPU）
	AllowCrossVendor bool `json:"allow_cross_vendor"`

	// 任务对国产芯片的兼容性评分 (0-100)
	// 100 = 算子已适配 CANN / BANG / DTK，性能与 CUDA 等价
	// 50  = 算子部分适配，可能存在性能损失
	// 0   = 仅支持 CUDA，无法在国产芯片上运行
	DomesticCompatibility int `json:"domestic_compatibility"`
}

// DefaultConstraint 默认约束（国产优先，允许降级）
func DefaultConstraint() DeterministicConstraint {
	return DeterministicConstraint{
		PreferDomestic:         true,
		AllowedDomesticVendors: []ChipVendor{VendorAscend, VendorCambricon},
		StrictMode:             false,
		MaxFallbackTier:        4,
		MaxComputeLoss:         0.50,
		RequireSMAcceleration:  false,
		MinXinchuangScore:      0,
		AllowCrossVendor:       true,
		DomesticCompatibility:  100,
	}
}

// StrictDomesticConstraint 严格国产约束（不降级到海外）
func StrictDomesticConstraint() DeterministicConstraint {
	c := DefaultConstraint()
	c.StrictMode = true
	c.MaxFallbackTier = 2 // 仅国产同型号 + 国产异构
	c.MaxComputeLoss = 0.40
	return c
}

// =========================================================================
// 第四部分: PoolingStrategy — 可插拔池化策略接口
// =========================================================================

// FallbackTier 降级层级
type FallbackTier int

const (
	TierExactDomestic FallbackTier = 1 // 国产同供应商同型号
	TierCrossDomestic FallbackTier = 2 // 国产跨供应商 (异构)
	TierExactOverseas FallbackTier = 3 // 海外同型号
	TierCrossOverseas FallbackTier = 4 // 海外跨型号 (异构)
	TierAny           FallbackTier = 5 // 任意可用
)

func (t FallbackTier) String() string {
	switch t {
	case TierExactDomestic:
		return "国产同型号"
	case TierCrossDomestic:
		return "国产异构"
	case TierExactOverseas:
		return "海外同型号"
	case TierCrossOverseas:
		return "海外异构"
	case TierAny:
		return "任意"
	default:
		return "未知"
	}
}

// PoolingDecision 池化决策
type PoolingDecision struct {
	// 最终分配的层级
	Tier FallbackTier

	// 是否为国产芯片
	IsDomestic bool

	// 芯片供应商
	Vendor ChipVendor

	// 等效算力比
	ComputeEquivalence float64

	// 是否发生了降级（分配的芯片不是首选）
	Degraded bool

	// 降级原因
	Reason string
}

// PoolingStrategy 池化策略接口
type PoolingStrategy interface {
	// Name 策略名称
	Name() string

	// Evaluate 评估一个候选节点对于给定任务的适合度
	// 返回 0-100 的分数（越高越优先）和决策元数据
	Evaluate(ru *ResourceUnit, task *TaskUnit) (float64, *PoolingDecision)

	// DetermineTier 根据任务约束和节点属性确定降级层级
	DetermineTier(ru *ResourceUnit, constraint DeterministicConstraint) FallbackTier

	// IsAcceptable 判断该节点是否可以被接受（在给定的降级层级内）
	IsAcceptable(ru *ResourceUnit, constraint DeterministicConstraint) bool
}

// =========================================================================
// 第五部分: DomesticFirstPoolingStrategy — 国产优先策略
// =========================================================================

// DomesticFirstConfig 国产优先策略配置
type DomesticFirstConfig struct {
	// 是否启用
	Enabled bool

	// 优先的国产供应商列表
	PreferredVendors []ChipVendor

	// 是否严格模式（国产不足时拒绝）
	StrictMode bool

	// 最大降级层级
	MaxFallbackTier FallbackTier

	// 国产优先的额外分数加成 (加到 Score 上)
	DomesticBonus float64

	// 海外芯片的分数惩罚
	OverseasPenalty float64

	// 信创分数阈值（低于此分数的节点降级处理）
	XinchuangThreshold int
}

// DefaultDomesticFirstConfig 默认国产优先配置
func DefaultDomesticFirstConfig() DomesticFirstConfig {
	return DomesticFirstConfig{
		Enabled:            true,
		PreferredVendors:   []ChipVendor{VendorAscend, VendorCambricon, VendorHygon},
		StrictMode:         false,
		MaxFallbackTier:    TierCrossOverseas,
		DomesticBonus:      30.0, // 国产节点额外 +30 分
		OverseasPenalty:    0.0,  // 不惩罚海外（仅降级时使用）
		XinchuangThreshold: 70,
	}
}

// DomesticFirstPoolingStrategy 国产优先池化策略
type DomesticFirstPoolingStrategy struct {
	config DomesticFirstConfig
}

// NewDomesticFirstPoolingStrategy 创建国产优先策略
func NewDomesticFirstPoolingStrategy(config DomesticFirstConfig) *DomesticFirstPoolingStrategy {
	return &DomesticFirstPoolingStrategy{config: config}
}

func (s *DomesticFirstPoolingStrategy) Name() string {
	return "domestic_first"
}

// Evaluate 评估节点适合度
func (s *DomesticFirstPoolingStrategy) Evaluate(ru *ResourceUnit, task *TaskUnit) (float64, *PoolingDecision) {
	if !s.config.Enabled {
		return 0, &PoolingDecision{Tier: TierAny, Reason: "国产优先策略未启用"}
	}

	constraint := DefaultConstraint()
	if constraint.MaxFallbackTier == 0 {
		constraint = DefaultConstraint()
	}

	// 获取芯片元数据
	chip := DefaultChipMetadata(ru.Model)
	tier := s.DetermineTier(ru, constraint)

	score := 0.0
	decision := &PoolingDecision{
		Tier:               tier,
		IsDomestic:         chip.Origin == OriginDomestic,
		Vendor:             chip.Vendor,
		ComputeEquivalence: chip.ComputeEquivalence,
	}

	// 分数计算
	switch tier {
	case TierExactDomestic:
		// 国产同型号: 最高优先级
		score = 80 + s.config.DomesticBonus
		decision.Degraded = false

	case TierCrossDomestic:
		// 国产异构: 次优先级，有轻微损失
		score = 50 + s.config.DomesticBonus*0.7
		decision.Degraded = true
		decision.Reason = fmt.Sprintf("国产异构降级: 首选 %v, 可用 %s(%s)",
			s.config.PreferredVendors, ru.Model, chip.Vendor)

	case TierExactOverseas:
		// 海外同型号: 降级到海外
		score = 20
		decision.Degraded = true
		decision.Reason = fmt.Sprintf("国产资源不足，降级到海外同型号: %s", ru.Model)

	case TierCrossOverseas:
		// 海外异构: 最远降级
		score = 5
		decision.Degraded = true
		decision.Reason = fmt.Sprintf("国产不足+海外同型号不足，降级到海外异构: %s", ru.Model)

	default:
		score = 0
		decision.Reason = "超出最大降级层级"
	}

	// 等效算力调整
	if chip.ComputeEquivalence > 0 {
		score *= chip.ComputeEquivalence
	}

	// 信创加成分
	score += float64(chip.XinchuangScore) * 0.1

	return score, decision
}

// DetermineTier 确定降级层级
func (s *DomesticFirstPoolingStrategy) DetermineTier(ru *ResourceUnit, constraint DeterministicConstraint) FallbackTier {
	chip := DefaultChipMetadata(ru.Model)
	vendor := chip.Vendor
	origin := chip.Origin

	// 1. 严格模式: 仅国产
	if constraint.StrictMode && origin != OriginDomestic {
		return FallbackTier(999) // 不可接受
	}

	// 2. 国产同型号检查
	if origin == OriginDomestic {
		isPreferred := false
		for _, pv := range constraint.AllowedDomesticVendors {
			if vendor == pv {
				isPreferred = true
				break
			}
		}
		if isPreferred {
			return TierExactDomestic
		}
		// 国产但非首选供应商 → 国产异构
		return TierCrossDomestic
	}

	// 3. 海外芯片 → 检查是否允许降级
	if constraint.StrictMode {
		return FallbackTier(999) // 严格模式拒绝
	}

	// 4. 海外同型号: 检查型号是否在允许列表中
	isAllowedOverseas := len(constraint.AllowedOverseasVendors) == 0
	for _, ov := range constraint.AllowedOverseasVendors {
		if vendor == ov {
			isAllowedOverseas = true
			break
		}
	}

	if isAllowedOverseas {
		// 海外型号与期望型号匹配 → Tier 3
		// 这里简化：如果 ComputeEquivalence >= 0.5 视为"同型号"
		if chip.ComputeEquivalence >= 0.3 {
			return TierExactOverseas
		}
		return TierCrossOverseas
	}

	return TierCrossOverseas
}

// IsAcceptable 判断节点是否可接受
func (s *DomesticFirstPoolingStrategy) IsAcceptable(ru *ResourceUnit, constraint DeterministicConstraint) bool {
	if !s.config.Enabled {
		return true // 未启用池化策略时，接受所有节点
	}

	tier := s.DetermineTier(ru, constraint)
	maxTier := constraint.MaxFallbackTier
	if maxTier == 0 {
		maxTier = int(s.config.MaxFallbackTier)
	}

	return int(tier) <= maxTier
}

// =========================================================================
// 第六部分: BalancedPoolingStrategy — 均衡策略（不区分国产/海外）
// =========================================================================

// BalancedPoolingStrategy 均衡池化策略（仅基于算力/利用率，不考虑来源）
type BalancedPoolingStrategy struct{}

func NewBalancedPoolingStrategy() *BalancedPoolingStrategy {
	return &BalancedPoolingStrategy{}
}

func (s *BalancedPoolingStrategy) Name() string { return "balanced" }

func (s *BalancedPoolingStrategy) Evaluate(ru *ResourceUnit, task *TaskUnit) (float64, *PoolingDecision) {
	chip := DefaultChipMetadata(ru.Model)
	score := (1 - ru.GPUUtilization()) * 100 * chip.ComputeEquivalence
	return score, &PoolingDecision{
		Tier: TierAny, IsDomestic: chip.Origin == OriginDomestic,
		Vendor: chip.Vendor, ComputeEquivalence: chip.ComputeEquivalence,
	}
}

func (s *BalancedPoolingStrategy) DetermineTier(ru *ResourceUnit, constraint DeterministicConstraint) FallbackTier {
	return TierAny
}

func (s *BalancedPoolingStrategy) IsAcceptable(ru *ResourceUnit, constraint DeterministicConstraint) bool {
	return true
}

// =========================================================================
// 第七部分: OverseasOnlyPoolingStrategy — 仅海外策略（对比基准）
// =========================================================================

type OverseasOnlyPoolingStrategy struct{}

func NewOverseasOnlyPoolingStrategy() *OverseasOnlyPoolingStrategy {
	return &OverseasOnlyPoolingStrategy{}
}

func (s *OverseasOnlyPoolingStrategy) Name() string { return "overseas_only" }

func (s *OverseasOnlyPoolingStrategy) Evaluate(ru *ResourceUnit, task *TaskUnit) (float64, *PoolingDecision) {
	chip := DefaultChipMetadata(ru.Model)
	if chip.Origin == OriginDomestic {
		return -999, &PoolingDecision{Reason: "仅海外策略，拒绝国产芯片"}
	}
	return (1 - ru.GPUUtilization()) * 100, &PoolingDecision{
		Tier: TierExactOverseas, IsDomestic: false, Vendor: chip.Vendor,
	}
}

func (s *OverseasOnlyPoolingStrategy) DetermineTier(ru *ResourceUnit, constraint DeterministicConstraint) FallbackTier {
	return TierExactOverseas
}

func (s *OverseasOnlyPoolingStrategy) IsAcceptable(ru *ResourceUnit, constraint DeterministicConstraint) bool {
	return false
}

// =========================================================================
// 第八部分: PoolingStrategyFactory — 策略工厂
// =========================================================================

// PoolingStrategyFactory 池化策略工厂
type PoolingStrategyFactory struct{}

// Create 根据名称创建池化策略
func (f *PoolingStrategyFactory) Create(name string, config interface{}) (PoolingStrategy, error) {
	switch name {
	case "domestic_first":
		cfg, ok := config.(DomesticFirstConfig)
		if !ok {
			cfg = DefaultDomesticFirstConfig()
		}
		return NewDomesticFirstPoolingStrategy(cfg), nil

	case "balanced":
		return NewBalancedPoolingStrategy(), nil

	case "overseas_only":
		return NewOverseasOnlyPoolingStrategy(), nil

	default:
		return nil, fmt.Errorf("未知池化策略: %s (可用: domestic_first, balanced, overseas_only)", name)
	}
}

// =========================================================================
// 第九部分: PoolingAwareScheduler — 池化感知调度器扩展
// =========================================================================

// PoolingAwareScheduler 在原有 AgentScheduler 基础上增加池化策略层
// 这是一个包装器，注入到 ScoreFunc 链中
type PoolingAwareScheduler struct {
	base     *AgentScheduler
	strategy PoolingStrategy
	config   DomesticFirstConfig
}

// NewPoolingAwareScheduler 创建池化感知调度器
func NewPoolingAwareScheduler(base *AgentScheduler, config DomesticFirstConfig) *PoolingAwareScheduler {
	factory := &PoolingStrategyFactory{}
	strategy, err := factory.Create("domestic_first", config)
	if err != nil {
		strategy = NewBalancedPoolingStrategy()
	}

	return &PoolingAwareScheduler{
		base:     base,
		strategy: strategy,
		config:   config,
	}
}

// PoolingScoreFunc 返回一个可注入到 AgentScheduler.scorers 链的打分函数
// 使用方法:
//
//	scheduler.AddScorer(poolingScheduler.PoolingScoreFunc())
func (pas *PoolingAwareScheduler) PoolingScoreFunc() ScoreFunc {
	return func(ru *ResourceUnit, task *TaskUnit) float64 {
		if !pas.config.Enabled {
			return 0
		}

		score, decision := pas.strategy.Evaluate(ru, task)

		// 如果发生了降级，记录日志
		if decision.Degraded {
			// 实际生产中这里应该写入审计日志
			_ = decision.Reason
		}

		return score
	}
}

// PoolingFilterFunc 返回一个可注入到 AgentScheduler.filters 链的过滤函数
// 在严格模式下，直接过滤掉海外芯片
func (pas *PoolingAwareScheduler) PoolingFilterFunc() FilterFunc {
	return func(ru *ResourceUnit, task *TaskUnit) *FilterResult {
		if !pas.config.Enabled {
			return &FilterResult{true, "pooling", ""}
		}

		if !pas.strategy.IsAcceptable(ru, DefaultConstraint()) {
			return &FilterResult{false, "pooling",
				fmt.Sprintf("池化策略拒绝: 节点 %s 不满足确定性约束 (策略=%s)", ru.ID, pas.strategy.Name())}
		}

		return &FilterResult{true, "pooling", ""}
	}
}

// GetStrategy 返回当前策略
func (pas *PoolingAwareScheduler) GetStrategy() PoolingStrategy {
	return pas.strategy
}

// SwitchStrategy 热切换策略（无需重启）
func (pas *PoolingAwareScheduler) SwitchStrategy(name string, config interface{}) error {
	factory := &PoolingStrategyFactory{}
	strategy, err := factory.Create(name, config)
	if err != nil {
		return err
	}
	pas.strategy = strategy
	return nil
}

// =========================================================================
// 第十部分: 弹性池化统计
// =========================================================================

// PoolingStats 池化统计（用于大屏展示）
type PoolingStats struct {
	// 策略名称
	Strategy string `json:"strategy"`

	// 各层级分配计数
	Tier1Count int `json:"tier1_domestic_exact"` // 国产同型号
	Tier2Count int `json:"tier2_domestic_cross"` // 国产异构
	Tier3Count int `json:"tier3_overseas_exact"` // 海外同型号
	Tier4Count int `json:"tier4_overseas_cross"` // 海外异构

	// 国产芯片利用率
	DomesticUtilization float64 `json:"domestic_utilization"` // 0-1

	// 海外芯片利用率
	OverseasUtilization float64 `json:"overseas_utilization"` // 0-1

	// 降级率 (分配到非首选芯片的比例)
	DegradationRate float64 `json:"degradation_rate"`

	// 总分配次数
	TotalAllocations int `json:"total_allocations"`

	// 国产芯片总 GPU 数
	DomesticGPUs int `json:"domestic_gpus"`

	// 海外芯片总 GPU 数
	OverseasGPUs int `json:"overseas_gpus"`

	// 信创兼容率（分配到信创分数≥70 节点的比例）
	XinchuangRate float64 `json:"xinchuang_rate"`
}

// ComputePoolingStats 从状态机计算池化统计
func ComputePoolingStats(stateMachine *UnifiedStateMachine) PoolingStats {
	resources := stateMachine.ListResources()
	stats := PoolingStats{Strategy: "domestic_first"}

	var domesticUtilSum, overseasUtilSum float64
	var domesticGPUCount, overseasGPUCount int
	var xinchuangCount int

	for _, ru := range resources {
		chip := DefaultChipMetadata(ru.Model)
		if chip.Origin == OriginDomestic {
			domesticGPUCount += ru.Count
			domesticUtilSum += ru.GPUUtilization() * float64(ru.Count)
			if chip.XinchuangScore >= 70 {
				xinchuangCount += ru.Count
			}
		} else {
			overseasGPUCount += ru.Count
			overseasUtilSum += ru.GPUUtilization() * float64(ru.Count)
		}
	}

	if domesticGPUCount > 0 {
		stats.DomesticUtilization = domesticUtilSum / float64(domesticGPUCount)
	}
	if overseasGPUCount > 0 {
		stats.OverseasUtilization = overseasUtilSum / float64(overseasGPUCount)
	}

	stats.DomesticGPUs = domesticGPUCount
	stats.OverseasGPUs = overseasGPUCount
	stats.TotalAllocations = domesticGPUCount + overseasGPUCount

	totalGPUs := domesticGPUCount + overseasGPUCount
	if totalGPUs > 0 {
		stats.XinchuangRate = float64(xinchuangCount) / float64(totalGPUs)
	}

	return stats
}

// =========================================================================
// 第十一部分: 辅助函数
// =========================================================================

// TagResourceUnit 为 ResourceUnit 打上芯片元数据标签
func TagResourceUnit(ru *ResourceUnit) {
	chip := DefaultChipMetadata(ru.Model)
	ru.Labels["chip_origin"] = string(chip.Origin)
	ru.Labels["chip_vendor"] = string(chip.Vendor)
	ru.Labels["xinchuang_score"] = fmt.Sprintf("%d", chip.XinchuangScore)
	ru.Labels["compute_equivalence"] = fmt.Sprintf("%.2f", chip.ComputeEquivalence)
	if chip.SMAccelerated {
		ru.Labels["sm_accelerated"] = "true"
	}
	if chip.OperatorSDK != "" {
		ru.Labels["operator_sdk"] = chip.OperatorSDK
	}
}

// TagAllResources 批量为所有节点打标签
func TagAllResources(stateMachine *UnifiedStateMachine) {
	for _, ru := range stateMachine.ListResources() {
		TagResourceUnit(ru)
	}
}

// IsDomesticNode 判断节点是否为国产
func IsDomesticNode(ru *ResourceUnit) bool {
	return IsDomesticModel(ru.Model)
}

// GetDomesticNodes 过滤出国产节点
func GetDomesticNodes(resources []*ResourceUnit) []*ResourceUnit {
	var result []*ResourceUnit
	for _, ru := range resources {
		if IsDomesticModel(ru.Model) {
			result = append(result, ru)
		}
	}
	return result
}

// GetOverseasNodes 过滤出海外节点
func GetOverseasNodes(resources []*ResourceUnit) []*ResourceUnit {
	var result []*ResourceUnit
	for _, ru := range resources {
		if !IsDomesticModel(ru.Model) {
			result = append(result, ru)
		}
	}
	return result
}

// SortByDomesticFirst 国产优先排序
func SortByDomesticFirst(resources []*ResourceUnit) {
	sort.SliceStable(resources, func(i, j int) bool {
		iDomestic := IsDomesticModel(resources[i].Model)
		jDomestic := IsDomesticModel(resources[j].Model)
		if iDomestic && !jDomestic {
			return true
		}
		if !iDomestic && jDomestic {
			return false
		}
		// 同为国产或同为海外，按利用率排序（优先分配空闲节点）
		return resources[i].GPUUtilization() < resources[j].GPUUtilization()
	})
}

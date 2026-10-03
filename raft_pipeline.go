// =========================================================================
// RaftKV 确定性引擎 — Raft 日志处理管线
//
// 串联完整数据流转闭环:
//   Raft commit → SM4-CTR 加密 WAL → TiDB/MySQL 异步落盘
//
// 设计原则:
//   1. 不修改 raft.go 核心逻辑，通过 onCommit 回调钩子接入
//   2. WAL 持久化: EncryptedStorage (SM4-CTR + 1GB 预分配 + 批量 fsync)
//   3. 金融落盘: TiDBAdapter (非阻塞 Write + 三级背压 + 故障自愈)
//   4. 启动时 WAL 回放恢复日志，关闭时刷新所有缓冲
//
// 集成方式:
//   pipeline, _ := NewRaftPipeline(cfg)
//   node.SetOnCommit(pipeline.OnCommit)
//   defer pipeline.Close()
// =========================================================================

package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/pkg/adapters"
)

// =========================================================================
// 管线配置
// =========================================================================

// PipelineConfig Raft 管线配置
type PipelineConfig struct {
	// WAL 加密存储
	EnableWAL bool
	WALPath   string // WAL 文件路径
	SM4Key    []byte // 16 字节 SM4 密钥

	// TiDB/MySQL 异步落盘
	EnableSink bool
	SinkConfig adapters.SinkConfig
}

// loadSM4KeyFromEnv 从环境变量 SM4_KEY 读取 16 字节密钥（fail-closed）。
// 未设置或非 32 位 hex 编码即拒绝启动；错误信息不包含密钥值。
func loadSM4KeyFromEnv() []byte {
	v := os.Getenv("SM4_KEY")
	if v == "" {
		log.Fatalf("[pipeline] SM4_KEY 未设置，拒绝启动 (fail-closed): 请通过环境变量 SM4_KEY 提供 32 位 hex 编码的 16 字节密钥")
	}
	key, err := hex.DecodeString(v)
	if err != nil || len(key) != 16 {
		log.Fatalf("[pipeline] SM4_KEY 非法，拒绝启动 (fail-closed): 必须为 32 位 hex 编码的 16 字节密钥")
	}
	return key
}

// DefaultPipelineConfig 默认管线配置
func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		EnableWAL:  true,
		WALPath:    filepath.Join("/app/wal-data", "raftkv.wal"),
		SM4Key:     loadSM4KeyFromEnv(),
		EnableSink: false,
		SinkConfig: adapters.DefaultSinkConfig(),
	}
}

// =========================================================================
// RaftPipeline — Raft 日志处理管线
// =========================================================================

// RaftPipeline 串联 WAL 加密持久化 + TiDB 异步落盘
//
// 数据流:
//
//	Raft commit → OnCommit(log)
//	                ├→ EncryptedStorage.AppendRaftLog (SM4-CTR → WAL 批量 fsync)
//	                └→ TiDBAdapter.Write (JSON → 通道 → 批量 INSERT → MySQL/TiDB)
type RaftPipeline struct {
	storage *EncryptedStorage     // SM4-CTR 加密 WAL（可选）
	sink    *adapters.TiDBAdapter // TiDB/MySQL 异步落盘（可选）

	enableWAL  bool
	walPath    string
	enableSink bool
	logger     *log.Logger

	// 统计
	totalCommitted atomic.Int64
	walErrors      int64
	sinkErrors     int64

	// 快照压缩
	snapshotThreshold   int64
	snapshotMinInterval time.Duration // 快照最小间隔（节流，防止选举风暴期间频繁快照）
	lastSnapshotTime    time.Time     // 上次快照时间
	snapshotMu          sync.Mutex
	onSnapshotCompact   func(int64)        // 快照后日志压缩回调（参数=快照包含的最大 Index）
	scheduler           *SnapshotScheduler // 异步快照调度器
}

// NewRaftPipeline 创建 Raft 处理管线
func NewRaftPipeline(cfg PipelineConfig) (*RaftPipeline, error) {
	p := &RaftPipeline{
		enableWAL:  cfg.EnableWAL,
		walPath:    cfg.WALPath,
		enableSink: cfg.EnableSink,
		logger:     log.New(os.Stderr, "[pipeline] ", log.LstdFlags),
	}

	p.snapshotThreshold = 50000
	if v := os.Getenv("WAL_SNAPSHOT_THRESHOLD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			p.snapshotThreshold = int64(n)
		}
	}

	p.snapshotMinInterval = 10 * time.Second
	if v := os.Getenv("WAL_SNAPSHOT_MIN_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			p.snapshotMinInterval = time.Duration(n) * time.Millisecond
		}
	}

	// 初始化加密 WAL 存储
	if cfg.EnableWAL {
		if len(cfg.SM4Key) != 16 {
			return nil, fmt.Errorf("SM4 密钥必须 16 字节，当前 %d 字节", len(cfg.SM4Key))
		}
		if cfg.WALPath == "" {
			return nil, fmt.Errorf("WAL 路径不能为空")
		}

		// 确保目录存在
		dir := filepath.Dir(cfg.WALPath)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("创建 WAL 目录失败: %w", err)
			}
		}

		es, err := NewEncryptedStorage(cfg.WALPath, cfg.SM4Key)
		if err != nil {
			return nil, fmt.Errorf("创建加密存储失败: %w", err)
		}
		p.storage = es
		p.logger.Printf("WAL 加密存储已启用: %s", cfg.WALPath)
	}

	// 初始化 TiDB/MySQL 异步落盘
	if cfg.EnableSink && cfg.SinkConfig.Enable {
		adapter, err := adapters.NewTiDBAdapter(cfg.SinkConfig)
		if err != nil {
			// Sink 创建失败不应阻止 Raft 启动
			p.logger.Printf("⚠ TiDB 适配器创建失败（降级运行）: %v", err)
		} else {
			// 幂等建表
			if err := adapter.CreateTable(); err != nil {
				p.logger.Printf("⚠ 建表警告: %v（适配器仍可运行）", err)
			}
			p.sink = adapter
			p.logger.Printf("TiDB/MySQL 异步落盘已启用: %s", cfg.SinkConfig.DSN)
		}
	}

	return p, nil
}

// =========================================================================
// WAL 回放（启动恢复）
// =========================================================================

// ReplayWAL 从 WAL 回放日志，用于节点重启后恢复已提交日志
func (p *RaftPipeline) ReplayWAL() ([]RaftLog, error) {
	if p.storage == nil {
		return nil, nil
	}
	logs, err := p.storage.ReplayAll()
	if err != nil {
		return nil, fmt.Errorf("WAL 回放失败: %w", err)
	}
	p.logger.Printf("WAL 回放完成: %d 条日志", len(logs))
	return logs, nil
}

// WALReplayStats WAL 重放统计信息
type WALReplayStats struct {
	SegmentCount    int           `json:"segment_count"`
	LogCount        int           `json:"log_count"`
	BeforeCommitIdx int64         `json:"before_commit_idx"`
	AfterCommitIdx  int64         `json:"after_commit_idx"`
	Duration        time.Duration `json:"duration"`
	Integrity       string        `json:"integrity"`
	Error           error         `json:"-"`
}

// ReplayWALWithStats 带统计的 WAL 回放，返回日志、重放统计与错误
func (p *RaftPipeline) ReplayWALWithStats() ([]RaftLog, WALReplayStats, error) {
	stats := WALReplayStats{
		BeforeCommitIdx: 0,
		Integrity:       "完整",
	}
	start := time.Now()

	if p.storage == nil {
		stats.Duration = time.Since(start)
		stats.Integrity = "无WAL"
		return nil, stats, nil
	}

	segments, _ := filepath.Glob(filepath.Join(filepath.Dir(p.walPath), "wal-*"))
	stats.SegmentCount = len(segments)

	logs, err := p.storage.ReplayAll()
	stats.Duration = time.Since(start)
	stats.LogCount = len(logs)

	if err != nil {
		stats.Integrity = "损坏"
		stats.Error = err
		return nil, stats, fmt.Errorf("WAL 回放失败: %w", err)
	}

	if len(logs) == 0 {
		stats.Integrity = "空"
	} else {
		for i := 1; i < len(logs); i++ {
			if logs[i].Index != logs[i-1].Index+1 {
				if logs[i].Index == logs[i-1].Index {
					stats.Integrity = "损坏"
				} else {
					stats.Integrity = "截断"
				}
				break
			}
		}
		stats.AfterCommitIdx = logs[len(logs)-1].Index
	}

	if stats.Duration > 10*time.Second {
		log.Printf("WAL重放超时告警: 耗时 %v, 日志 %d 条", stats.Duration, stats.LogCount)
	}

	log.Printf("WAL回放完成: 恢复 %d 条日志, commitIdx %d -> %d, 耗时 %v, 完整性 %s",
		stats.LogCount, stats.BeforeCommitIdx, stats.AfterCommitIdx, stats.Duration, stats.Integrity)

	return logs, stats, nil
}

// =========================================================================
// OnCommit — Raft 日志提交回调
// =========================================================================

// SetOnSnapshotCompact 设置快照后日志压缩回调
// 快照成功后调用，参数为快照包含的最大日志 Index
func (p *RaftPipeline) SetOnSnapshotCompact(cb func(int64)) {
	p.onSnapshotCompact = cb
}

// SetScheduler 注入异步快照调度器
func (p *RaftPipeline) SetScheduler(s *SnapshotScheduler) {
	p.scheduler = s
}

// OnCommit Raft 日志提交回调
//
// 当 Raft 日志被提交（commitIdx 前进）时由 RaftNode 调用:
//  1. 加密写入 WAL（SM4-CTR → 批量 fsync）
//  2. 序列化写入 TiDB/MySQL（非阻塞 → 批量 INSERT）
//
// 注意: 此函数必须快速返回，不阻塞 Raft 主循环
//   - WAL Append 是缓冲写入（批量 fsync 在后台 goroutine）
//   - Sink Write 是非阻塞通道写入（满时降级 fallback 队列）
func (p *RaftPipeline) OnCommit(log RaftLog) {
	p.totalCommitted.Add(1)

	// 1. 加密写入 WAL
	if p.storage != nil {
		if err := p.storage.AppendRaftLog(log); err != nil {
			p.walErrors++
			p.logger.Printf("WAL 写入失败 index=%d: %v", log.Index, err)
		}

		if p.snapshotThreshold > 0 && p.totalCommitted.Load() >= p.snapshotThreshold {
			p.snapshotMu.Lock()
			if p.totalCommitted.Load() >= p.snapshotThreshold && time.Since(p.lastSnapshotTime) >= p.snapshotMinInterval {
				p.lastSnapshotTime = time.Now()
				if p.scheduler != nil {
					p.scheduler.Request(&snapshotRequest{
						lastIdx:  log.Index,
						lastTerm: log.Term,
						term:     0,
					})
					p.totalCommitted.Store(0)
				} else {
					n, oldBytes, err := p.storage.Snapshot()
					if err != nil {
						p.walErrors++
						p.logger.Printf("快照失败: %v", err)
						slogError("snapshot_failed", "快照失败", map[string]interface{}{
							"error": err.Error(),
						})
					} else {
						p.logger.Printf("快照触发: %d 条, WAL 释放 %d 字节", n, oldBytes)
						slogInfo("snapshot_compact", "快照触发", map[string]interface{}{
							"entries":   n,
							"wal_bytes": oldBytes,
						})
						p.totalCommitted.Store(0)
						if p.onSnapshotCompact != nil {
							p.onSnapshotCompact(log.Index)
						}
					}
				}
			}
			p.snapshotMu.Unlock()
		}
	}

	// 2. 序列化写入 TiDB/MySQL
	if p.sink != nil {
		data, err := json.Marshal(log)
		if err != nil {
			p.sinkErrors++
			p.logger.Printf("序列化失败 index=%d: %v", log.Index, err)
			return
		}
		if err := p.sink.Write(data); err != nil {
			p.sinkErrors++
			p.logger.Printf("Sink 写入失败 index=%d: %v", log.Index, err)
		}
	}
}

// OnCommitBatch 批量提交回调（多条日志同时提交时使用）
func (p *RaftPipeline) OnCommitBatch(logs []RaftLog) {
	for _, log := range logs {
		p.OnCommit(log)
	}
}

// =========================================================================
// 生命周期管理
// =========================================================================

// Sync 强制刷新所有缓冲到下游
func (p *RaftPipeline) Sync() error {
	if p.sink != nil {
		if err := p.sink.Sync(); err != nil {
			return fmt.Errorf("sink sync: %w", err)
		}
	}
	return nil
}

// Close 关闭管线，刷新所有缓冲
func (p *RaftPipeline) Close() error {
	var errs []error

	// 先刷新 sink
	if p.sink != nil {
		if err := p.sink.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("sink sync: %w", err))
		}
		if err := p.sink.Close(); err != nil {
			errs = append(errs, fmt.Errorf("sink close: %w", err))
		}
	}

	// 再关闭 WAL
	if p.storage != nil {
		if err := p.storage.Close(); err != nil {
			errs = append(errs, fmt.Errorf("storage close: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("管线关闭错误: %v", errs)
	}
	p.logger.Printf("管线已安全关闭 (committed=%d, walErrors=%d, sinkErrors=%d)",
		p.totalCommitted.Load(), p.walErrors, p.sinkErrors)
	return nil
}

// =========================================================================
// 统计与状态
// =========================================================================

// PipelineStats 管线运行时统计
type PipelineStats struct {
	Committed   int64 `json:"committed"`    // 总提交条数
	WALErrors   int64 `json:"wal_errors"`   // WAL 错误数
	SinkErrors  int64 `json:"sink_errors"`  // Sink 错误数
	WALEnabled  bool  `json:"wal_enabled"`  // WAL 是否启用
	SinkEnabled bool  `json:"sink_enabled"` // Sink 是否启用

	// Sink 详细统计（如果启用）
	SinkWritten     int64 `json:"sink_written,omitempty"`
	SinkFailed      int64 `json:"sink_failed,omitempty"`
	SinkRetried     int64 `json:"sink_retried,omitempty"`
	SinkFallback    int64 `json:"sink_fallback,omitempty"`
	SinkFallbackLen int   `json:"sink_fallback_len,omitempty"`
	SinkChannelLen  int   `json:"sink_channel_len,omitempty"`
	SinkDBHealthy   bool  `json:"sink_db_healthy,omitempty"`
}

// Stats 返回管线统计
func (p *RaftPipeline) Stats() PipelineStats {
	stats := PipelineStats{
		Committed:   p.totalCommitted.Load(),
		WALErrors:   p.walErrors,
		SinkErrors:  p.sinkErrors,
		WALEnabled:  p.storage != nil,
		SinkEnabled: p.sink != nil,
	}

	if p.sink != nil {
		s := p.sink.Stats()
		stats.SinkWritten = s.Written
		stats.SinkFailed = s.Failed
		stats.SinkRetried = s.Retried
		stats.SinkFallback = s.Fallback
		stats.SinkFallbackLen = s.FallbackLen
		stats.SinkChannelLen = s.ChannelLen
		stats.SinkDBHealthy = s.DBHealthy
	}

	return stats
}

// Healthy 返回管线健康状态
func (p *RaftPipeline) Healthy() bool {
	if p.sink != nil && !p.sink.Healthy() {
		return false
	}
	return true
}

// SinkAdapter 返回底层 TiDB 适配器（用于直接操作，如建表）
func (p *RaftPipeline) SinkAdapter() *adapters.TiDBAdapter {
	return p.sink
}

// Storage 返回底层加密存储
func (p *RaftPipeline) Storage() *EncryptedStorage {
	return p.storage
}

// =========================================================================
// 辅助: 从环境变量构建配置
// =========================================================================

// PipelineConfigFromEnv 从环境变量构建管线配置
//
// 环境变量:
//
//	WAL_ENABLE=true|false          WAL 开关
//	WAL_PATH=/data/raft.wal        WAL 路径
//	SM4_KEY=<hex>                  SM4 密钥（16 字节，hex 编码）
//	SINK_ENABLE=true|false         Sink 开关
//	SINK_DSN=root:@tcp(host)/db    数据库连接串
//	SINK_TABLE=raft_logs           目标表名
//	SINK_BATCH=1000                批量大小
//	SINK_FLUSH=500ms               刷新间隔
func PipelineConfigFromEnv() PipelineConfig {
	cfg := DefaultPipelineConfig()

	if v := os.Getenv("WAL_ENABLE"); v == "false" || v == "0" {
		cfg.EnableWAL = false
	}
	walPathEnv := os.Getenv("WAL_PATH")
	if walPathEnv != "" {
		cfg.WALPath = walPathEnv
	} else if cfg.EnableWAL {
		log.Fatalf("[pipeline] WAL_PATH 未设置，拒绝静默回落到默认路径 %q（多节点共享 WAL 将导致数据损坏），请显式设置 WAL_PATH 环境变量", cfg.WALPath)
	}

	if v := os.Getenv("SINK_ENABLE"); v == "true" || v == "1" {
		cfg.EnableSink = true
		cfg.SinkConfig.Enable = true
	}
	if v := os.Getenv("SINK_DSN"); v != "" {
		cfg.SinkConfig.DSN = v
	}
	if v := os.Getenv("SINK_TABLE"); v != "" {
		cfg.SinkConfig.TableName = v
	}

	// 数据库连接串必须由环境变量 SINK_DSN 提供，禁止硬编码凭据
	if cfg.EnableSink && cfg.SinkConfig.DSN == "" {
		fmt.Printf("[DataSink] 警告: 未设置 SINK_DSN，异步落盘功能已禁用\n")
		cfg.EnableSink = false
		cfg.SinkConfig.Enable = false
	}

	return cfg
}

// =========================================================================
// SnapshotScheduler — 异步快照调度器
// =========================================================================

// snapshotRequest 快照请求
type snapshotRequest struct {
	lastIdx  int64 // 快照包含的最大日志 Index
	lastTerm int64 // 快照包含的最大日志 Term
	term     int64 // 发起快照时的 Raft term（用于校验 leader 身份）
}

// SnapshotScheduler 异步快照调度器
// 将同步快照改为异步执行，消除 OnCommit 阻塞导致的 TPS 退化
type SnapshotScheduler struct {
	snapshotCh       chan *snapshotRequest
	storage          *EncryptedStorage
	onCompact        func(int64)
	node             *RaftNode
	wg               sync.WaitGroup
	stopCh           chan struct{}
	logger           *log.Logger
	compactionCount  atomic.Int64
	snapshotProgress atomic.Int32
}

// NewSnapshotScheduler 创建异步快照调度器
func NewSnapshotScheduler(storage *EncryptedStorage, onCompact func(int64), node *RaftNode, logger *log.Logger) *SnapshotScheduler {
	return &SnapshotScheduler{
		snapshotCh: make(chan *snapshotRequest, 1),
		storage:    storage,
		onCompact:  onCompact,
		node:       node,
		stopCh:     make(chan struct{}),
		logger:     logger,
	}
}

// Start 启动异步快照消费 goroutine
func (s *SnapshotScheduler) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.stopCh:
				return
			case req := <-s.snapshotCh:
				s.executeSnapshot(req)
			}
		}
	}()
	s.logger.Printf("异步快照调度器已启动")
}

// Stop 停止调度器，等待消费 goroutine 退出
func (s *SnapshotScheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
	s.logger.Printf("异步快照调度器已停止")
}

// Request 非阻塞投递快照请求（channel 满时跳过，不阻塞调用方）
func (s *SnapshotScheduler) Request(req *snapshotRequest) {
	select {
	case s.snapshotCh <- req:
	default:
		s.logger.Printf("快照请求跳过（前次快照仍在执行）")
	}
}

// executeSnapshot 执行快照（在消费 goroutine 中调用）
// T034: 增加 logCount > snapshotThreshold 联动触发检查 + lastIncludedIndex ≤ commitIdx 前置校验
func (s *SnapshotScheduler) executeSnapshot(req *snapshotRequest) {
	// 快照是本地状态机持久化操作，leader 和 follower 均可执行
	// req.term > 0 时校验 term 未变更（防止旧 term 的过期快照请求）
	if req.term > 0 && s.node.Term() != req.term {
		s.logger.Printf("快照跳过: term 变更 (reqTerm=%d, curTerm=%d)", req.term, s.node.Term())
		return
	}

	// T034: 前置校验 — lastIncludedIndex 必须 ≤ commitIdx（不可快照未提交的日志）
	commitIdx := s.node.getCommitIdx()
	if req.lastIdx > commitIdx {
		s.logger.Printf("快照拒绝: lastIncludedIndex=%d > commitIdx=%d（不可快照未提交日志）",
			req.lastIdx, commitIdx)
		return
	}

	n, oldBytes, err := s.storage.Snapshot()
	if err != nil {
		s.logger.Printf("异步快照失败: %v", err)
		return
	}

	s.logger.Printf("异步快照完成: %d 条, WAL 释放 %d 字节", n, oldBytes)
	slogInfo("snapshot_async_done", "异步快照完成", map[string]interface{}{
		"entries":   n,
		"wal_bytes": oldBytes,
		"last_idx":  req.lastIdx,
	})

	if s.onCompact != nil {
		s.onCompact(req.lastIdx)
	}
	s.compactionCount.Add(1)
	s.snapshotProgress.Store(1000)
}

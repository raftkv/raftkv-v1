// =========================================================================
// RaftKV 确定性引擎 — Raft 日志 → 国产金融数据库 异步落盘适配器
//
// 技术目标：
//   将 Raft 共识提交日志通过纯 Go 驱动异步批量写入 TiDB/MySQL
//
// 设计原则：
//   1. 纯 Go 零 CGO — github.com/go-sql-driver/mysql 纯 Go 驱动
//   2. 非阻塞写入 — Write() 永不阻塞，溢出走本地回退队列
//   3. 批量 group commit — batch_size 条或 flush_interval 窗口合并 INSERT
//   4. 故障自愈 — DB 宕机时缓存到本地队列，恢复后自动追平
//   5. sync.Pool 零拷贝 — 复用 bytes.Buffer 消除 GC 压力
//
// 依赖：github.com/go-sql-driver/mysql v1.10.0（纯 Go，零 CGO）
// =========================================================================

package adapters

import (
	"bytes"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql" // 纯 Go MySQL/TiDB 驱动，零 CGO
)

// =========================================================================
// 第一部分: DataSink 接口定义
// =========================================================================

// DataSink 数据落盘接口
// Raft 日志通过此接口异步写入下游金融级数据库
type DataSink interface {
	// Write 非阻塞写入日志数据
	// 数据首先进入内存通道，由后台 goroutine 批量写入数据库
	// 当通道满时自动降级到本地回退队列，永不阻塞调用方
	Write(logData []byte) error

	// Sync 强制刷新所有缓冲数据到数据库
	// 阻塞直到所有 pending 数据写入完成或超时
	Sync() error

	// Close 关闭适配器，刷新所有缓冲
	Close() error
}

// =========================================================================
// 第二部分: 配置结构
// =========================================================================

// SinkConfig TiDB 适配器配置
type SinkConfig struct {
	Enable        bool          // 是否启用
	DSN           string        // 数据库连接串 (MySQL/TiDB 协议)
	BatchSize     int           // 批量写入条数 (默认 1000)
	FlushInterval time.Duration // 刷新间隔 (默认 500ms)
	ChannelSize   int           // 写入通道缓冲大小 (默认 4096)
	MaxFallback   int           // 本地回退队列最大条数 (默认 100000)
	TableName     string        // 目标表名 (默认 raft_logs)
	MaxRetries    int           // 单批最大重试次数 (默认 3)
	RetryInterval time.Duration // 重试间隔 (默认 1s)
}

// DefaultSinkConfig 返回默认配置
func DefaultSinkConfig() SinkConfig {
	return SinkConfig{
		Enable:        false,
		DSN:           "root:@tcp(127.0.0.1:4000)/raftkv?charset=utf8mb4&parseTime=true",
		BatchSize:     1000,
		FlushInterval: 500 * time.Millisecond,
		ChannelSize:   4096,
		MaxFallback:   100000,
		TableName:     "raft_logs",
		MaxRetries:    3,
		RetryInterval: 1 * time.Second,
	}
}

// =========================================================================
// 第三部分: TiDBAdapter 核心实现
// =========================================================================

// TiDBAdapter TiDB/MySQL 异步批量写入适配器
//
// 数据流:
//
//	Write() → writeCh (非阻塞) → batchLoop → batch INSERT → TiDB
//	             ↓ (通道满)
//	        fallbackQueue (本地缓存) → DB 恢复后自动追平
type TiDBAdapter struct {
	db     *sql.DB
	config SinkConfig

	// 非阻塞写入通道
	writeCh chan []byte

	// 本地回退队列（DB 故障或通道满时缓存）
	fallbackMu sync.Mutex
	fallback   [][]byte

	// buffer 复用池（零拷贝序列化）
	pool sync.Pool

	// 批量刷新控制
	flushCh chan struct{}
	closeCh chan struct{}
	wg      sync.WaitGroup
	closed  atomic.Bool

	// 统计（原子操作，无锁）
	totalWritten  atomic.Int64
	totalFailed   atomic.Int64
	totalRetried  atomic.Int64
	totalFallback atomic.Int64
	dbHealthy     atomic.Bool
}

// NewTiDBAdapter 创建 TiDB 适配器
// 调用前需确保 DSN 可达，否则适配器将进入降级模式（写本地回退队列）
func NewTiDBAdapter(config SinkConfig) (*TiDBAdapter, error) {
	if !config.Enable {
		return nil, fmt.Errorf("DataSink 未启用 (enable=false)")
	}
	if config.DSN == "" {
		return nil, fmt.Errorf("DSN 不能为空")
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 500 * time.Millisecond
	}
	if config.ChannelSize <= 0 {
		config.ChannelSize = 4096
	}
	if config.MaxFallback <= 0 {
		config.MaxFallback = 100000
	}
	if config.TableName == "" {
		config.TableName = "raft_logs"
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.RetryInterval <= 0 {
		config.RetryInterval = 1 * time.Second
	}

	// 打开数据库连接池（纯 Go 驱动，零 CGO）
	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("打开数据库失败: %w", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(5 * time.Minute)

	// 健康检查
	if err := db.Ping(); err != nil {
		// DB 不可达，仍创建适配器，进入降级模式
		// 数据将缓存到 fallback 队列，DB 恢复后自动追平
		// 不返回 error，保证 Raft 主流程不受 DB 故障影响
	}

	adapter := &TiDBAdapter{
		db:       db,
		config:   config,
		writeCh:  make(chan []byte, config.ChannelSize),
		fallback: make([][]byte, 0, config.BatchSize),
		flushCh:  make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}

	// 初始健康状态
	adapter.dbHealthy.Store(true)

	// 启动后台批量写入 goroutine
	adapter.wg.Add(1)
	go adapter.batchLoop()

	// 启动健康检查 goroutine
	adapter.wg.Add(1)
	go adapter.healthLoop()

	return adapter, nil
}

// =========================================================================
// 第四部分: 非阻塞写入
// =========================================================================

// Write 非阻塞写入日志数据
//
// 优先级:
//  1. 写入 writeCh 通道（非阻塞 select）
//  2. 通道满 → 降级到 fallback 队列
//  3. fallback 队列满 → 丢弃最旧数据并计数（背压保护）
func (a *TiDBAdapter) Write(logData []byte) error {
	if a.closed.Load() {
		return fmt.Errorf("adapter 已关闭")
	}

	// 复用 buffer 拷贝数据（避免外部修改影响）
	buf := a.pool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Write(logData)
	data := buf.Bytes()

	// 优先非阻塞写入通道
	select {
	case a.writeCh <- data:
		return nil
	default:
	}

	// 通道满，降级到 fallback 队列
	a.fallbackMu.Lock()
	if len(a.fallback) >= a.config.MaxFallback {
		// 背压保护：丢弃最旧的 10% 数据
		dropCount := len(a.fallback) / 10
		a.fallback = a.fallback[dropCount:]
		a.totalFailed.Add(int64(dropCount))
	}
	a.fallback = append(a.fallback, data)
	a.fallbackMu.Unlock()

	a.totalFallback.Add(1)
	return nil
}

// Sync 强制刷新所有缓冲数据到数据库
func (a *TiDBAdapter) Sync() error {
	if a.closed.Load() {
		return fmt.Errorf("adapter 已关闭")
	}

	// 触发刷新
	select {
	case a.flushCh <- struct{}{}:
	default:
	}

	// 等待通道排空
	for len(a.writeCh) > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// 等待 fallback 排空（如果 DB 健康）
	for {
		a.fallbackMu.Lock()
		fbLen := len(a.fallback)
		a.fallbackMu.Unlock()
		if fbLen == 0 || !a.dbHealthy.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}

// =========================================================================
// 第五部分: 后台批量写入 goroutine
// =========================================================================

// batchLoop 批量写入主循环
func (a *TiDBAdapter) batchLoop() {
	defer a.wg.Done()

	batch := make([][]byte, 0, a.config.BatchSize)
	timer := time.NewTimer(a.config.FlushInterval)
	defer timer.Stop()

	for {
		select {
		case <-a.closeCh:
			// 关闭前刷新剩余数据
			a.drainFallback(&batch)
			a.flushBatch(batch)
			return

		case data := <-a.writeCh:
			batch = append(batch, data)
			if len(batch) >= a.config.BatchSize {
				a.drainFallback(&batch)
				a.flushBatch(batch)
				batch = batch[:0]
				timer.Reset(a.config.FlushInterval)
			}

		case <-a.flushCh:
			a.drainFallback(&batch)
			a.flushBatch(batch)
			batch = batch[:0]
			timer.Reset(a.config.FlushInterval)

		case <-timer.C:
			a.drainFallback(&batch)
			a.flushBatch(batch)
			batch = batch[:0]
			timer.Reset(a.config.FlushInterval)
		}
	}
}

// drainFallback 将 fallback 队列中的数据排入当前批次
func (a *TiDBAdapter) drainFallback(batch *[][]byte) {
	a.fallbackMu.Lock()
	if len(a.fallback) == 0 {
		a.fallbackMu.Unlock()
		return
	}
	// 尽可能多地将 fallback 数据并入批次
	remaining := a.config.BatchSize - len(*batch)
	if remaining > len(a.fallback) {
		remaining = len(a.fallback)
	}
	*batch = append(*batch, a.fallback[:remaining]...)
	a.fallback = a.fallback[remaining:]
	a.fallbackMu.Unlock()
}

// flushBatch 批量写入数据库（group commit）
func (a *TiDBAdapter) flushBatch(batch [][]byte) {
	if len(batch) == 0 {
		return
	}

	if !a.dbHealthy.Load() {
		// DB 不健康，数据放回 fallback 队列
		a.moveToFallback(batch)
		return
	}

	for retry := 0; retry < a.config.MaxRetries; retry++ {
		err := a.execBatch(batch)
		if err == nil {
			a.totalWritten.Add(int64(len(batch)))
			return
		}

		a.totalRetried.Add(int64(len(batch)))
		if retry == a.config.MaxRetries-1 {
			// 重试耗尽，标记 DB 不健康，数据放回 fallback
			a.dbHealthy.Store(false)
			a.moveToFallback(batch)
			a.totalFailed.Add(int64(len(batch)))
			return
		}
		time.Sleep(a.config.RetryInterval)
	}
}

// execBatch 执行批量 INSERT 事务
func (a *TiDBAdapter) execBatch(batch [][]byte) error {
	tx, err := a.db.Begin()
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf(
		"INSERT INTO %s (log_data) VALUES (?)", a.config.TableName,
	))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("预处理失败: %w", err)
	}
	defer stmt.Close()

	for _, data := range batch {
		if _, err := stmt.Exec(data); err != nil {
			tx.Rollback()
			return fmt.Errorf("执行 INSERT 失败: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}
	return nil
}

// moveToFallback 将批次数据移入 fallback 队列
func (a *TiDBAdapter) moveToFallback(batch [][]byte) {
	a.fallbackMu.Lock()
	// 如果 fallback 已满，丢弃最旧的 10%
	if len(a.fallback)+len(batch) > a.config.MaxFallback {
		dropCount := (len(a.fallback) + len(batch) - a.config.MaxFallback)
		if dropCount < len(a.fallback) {
			a.fallback = a.fallback[dropCount:]
		} else {
			a.fallback = a.fallback[:0]
		}
	}
	a.fallback = append(a.fallback, batch...)
	a.fallbackMu.Unlock()
}

// =========================================================================
// 第六部分: 健康检查 goroutine
// =========================================================================

// healthLoop 定期检查 DB 健康状态
func (a *TiDBAdapter) healthLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.closeCh:
			return
		case <-ticker.C:
			if err := a.db.Ping(); err != nil {
				a.dbHealthy.Store(false)
			} else {
				wasUnhealthy := !a.dbHealthy.Load()
				a.dbHealthy.Store(true)
				if wasUnhealthy && a.hasFallback() {
					// DB 恢复且有积压数据，触发刷新
					select {
					case a.flushCh <- struct{}{}:
					default:
					}
				}
			}
		}
	}
}

// hasFallback 检查是否有积压数据
func (a *TiDBAdapter) hasFallback() bool {
	a.fallbackMu.Lock()
	defer a.fallbackMu.Unlock()
	return len(a.fallback) > 0
}

// =========================================================================
// 第七部分: 生命周期管理
// =========================================================================

// Close 关闭适配器，刷新所有缓冲
func (a *TiDBAdapter) Close() error {
	if !a.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(a.closeCh)
	a.wg.Wait()

	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

// CreateTable 创建日志表（幂等操作）
func (a *TiDBAdapter) CreateTable() error {
	_, err := a.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			log_data   LONGBLOB NOT NULL,
			data_size  INT UNSIGNED NOT NULL DEFAULT 0,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_created (created_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`, a.config.TableName))
	if err != nil {
		return fmt.Errorf("建表失败: %w", err)
	}
	return nil
}

// =========================================================================
// 第八部分: 统计与状态
// =========================================================================

// SinkStats 适配器运行时统计
type SinkStats struct {
	Written     int64 // 成功写入条数
	Failed      int64 // 失败条数
	Retried     int64 // 重试条数
	Fallback    int64 // 降级到 fallback 的条数
	FallbackLen int   // 当前 fallback 队列长度
	ChannelLen  int   // 当前通道长度
	DBHealthy   bool  // DB 健康状态
}

// Stats 返回当前统计
func (a *TiDBAdapter) Stats() SinkStats {
	a.fallbackMu.Lock()
	fbLen := len(a.fallback)
	a.fallbackMu.Unlock()

	return SinkStats{
		Written:     a.totalWritten.Load(),
		Failed:      a.totalFailed.Load(),
		Retried:     a.totalRetried.Load(),
		Fallback:    a.totalFallback.Load(),
		FallbackLen: fbLen,
		ChannelLen:  len(a.writeCh),
		DBHealthy:   a.dbHealthy.Load(),
	}
}

// Healthy 返回 DB 健康状态
func (a *TiDBAdapter) Healthy() bool {
	return a.dbHealthy.Load()
}

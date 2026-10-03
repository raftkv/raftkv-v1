package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "raftkv/proto"
)

type BatchSyncState int32

const (
	BatchSyncIdle BatchSyncState = iota
	BatchSyncInProgress
	BatchSyncCompleted
	BatchSyncFailed
	BatchSyncRetrying
)

type BatchSyncTask struct {
	TargetFollowerID string
	StartIdx         int64
	EndIdx           int64
	BatchSize        int64
	State            int32
	RetryCount       int32
}

type BatchSyncConfig struct {
	Enable       bool
	LagThreshold int64
	MaxBatchSize int64
	MaxRetries   int32
	SyncInterval time.Duration
}

func DefaultBatchSyncConfig() BatchSyncConfig {
	return BatchSyncConfig{
		Enable:       true,
		LagThreshold: 100,
		MaxBatchSize: 4096,
		MaxRetries:   5,
		SyncInterval: 200 * time.Millisecond,
	}
}

func BatchSyncConfigFromEnv() BatchSyncConfig {
	cfg := DefaultBatchSyncConfig()
	if os.Getenv("BATCH_SYNC_ENABLE") == "false" || os.Getenv("BATCH_SYNC_ENABLE") == "0" {
		cfg.Enable = false
	}
	if v := osGetenvInt("BATCH_SYNC_LAG_THRESHOLD"); v > 0 {
		cfg.LagThreshold = int64(v)
	}
	if v := osGetenvInt("BATCH_SYNC_MAX_BATCH"); v > 0 {
		cfg.MaxBatchSize = int64(v)
	}
	return cfg
}

type BatchSyncManager struct {
	mu         sync.Mutex
	tasks      map[string]*BatchSyncTask
	config     BatchSyncConfig
	node       *RaftNode
	stopCh     chan struct{}
	wg         sync.WaitGroup
	inProgress sync.Map // 刀三: 并发护栏 — key=peerID, value=struct{}{}
}

func NewBatchSyncManager(node *RaftNode, config BatchSyncConfig) *BatchSyncManager {
	return &BatchSyncManager{
		tasks:  make(map[string]*BatchSyncTask),
		config: config,
		node:   node,
		stopCh: make(chan struct{}),
	}
}

func (m *BatchSyncManager) Start() {
	m.wg.Add(1)
	go m.SyncLoop()
}

func (m *BatchSyncManager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

func (m *BatchSyncManager) SyncLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			if !m.config.Enable {
				continue
			}
			m.node.CheckGapAlerts() // R-04修复C: gap>阈值持续10s告警
			lagging := m.node.IdentifyLaggingFollowers()
			if len(lagging) == 0 {
				continue
			}

			var wg sync.WaitGroup
			for _, f := range lagging {
				wg.Add(1)
				go func(lf LaggingFollower) {
					defer wg.Done()
					m.SyncFollower(lf)
				}(f)
			}
			wg.Wait()
		}
	}
}

func (m *BatchSyncManager) SyncFollower(f LaggingFollower) {
	// 刀三: 并发护栏 — 每 follower 同时只有 1 个 sync 或 snapshot 操作
	if _, loaded := m.inProgress.LoadOrStore(f.PeerID, struct{}{}); loaded {
		return
	}
	defer m.inProgress.Delete(f.PeerID)

	client, ok := m.node.GetPeerClient(f.PeerID)
	if !ok {
		return
	}

	batchSize := f.BatchSize
	if batchSize < 128 {
		batchSize = 128
	}

	startIdx := f.StartIdx
	retryCount := int32(0)

	for startIdx <= f.EndIdx {
		endIdx := startIdx + batchSize - 1
		if endIdx > f.EndIdx {
			endIdx = f.EndIdx
		}

		entries, errEntries := m.node.GetLogEntries(startIdx, endIdx)
		if errEntries != nil {
			m.node.logf("[SYNC] leader=%s follower=%s startIdx=%d endIdx=%d ErrCompacted — 触发快照兜底",
				m.node.id, f.PeerID, startIdx, endIdx)
			m.sendSnapshot(f.PeerID)
			return
		}
		if len(entries) == 0 {
			break
		}

		// 探针1(M1): batch sync 发送侧观测
		m.node.logf("[SYNC] leader=%s follower=%s startIdx=%d endIdx=%d entries=%d",
			m.node.id, f.PeerID, startIdx, endIdx, len(entries))
		// 探针2(M2): 序列化缓冲大小观测
		bufBytes := 0
		for _, e := range entries {
			bufBytes += 16 + len(e.Command) + len(e.Sm3Hash)
		}
		m.node.logf("[SYNCBUF] leader=%s follower=%s entries=%d bufferBytes=%d",
			m.node.id, f.PeerID, len(entries), bufBytes)

		prevLogIdx := startIdx - 1
		prevLogTerm := int64(0)
		if prevLogIdx > 0 {
			prevLogTerm = m.node.GetLogTerm(prevLogIdx)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req := &pb.AppendEntriesRequest{
			Term:         m.node.Term(),
			LeaderId:     m.node.ID(),
			PrevLogIndex: prevLogIdx,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: m.node.getCommitIdx(),
		}

		resp, err := client.AppendEntries(ctx, req)
		cancel()

		if err != nil {
			retryCount++
			if retryCount > m.config.MaxRetries {
				m.node.logf("[raft/%s] 批量同步失败: follower=%s, startIdx=%d, 重试%d次后放弃",
					m.node.id, f.PeerID, startIdx, retryCount)
				m.node.MarkFollowerDegraded(f.PeerID) // R-04修复B: 标记降级
				return
			}
			batchSize = batchSize / 2
			if batchSize < 128 {
				batchSize = 128
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if resp.Success {
			m.node.UpdateFollowerProgress(f.PeerID, endIdx)
			m.node.ClearFollowerDegraded(f.PeerID) // R-04修复B: 同步成功清除降级
			startIdx = endIdx + 1
		} else {
			// 刀一: 标准Raft回退探测 — follower拒绝时递减startIdx和nextIdx，禁止跳回1
			if startIdx > 1 {
				startIdx--
			}
			m.node.DecrementNextIdx(f.PeerID)
			retryCount++
			if retryCount > m.config.MaxRetries {
				m.node.MarkFollowerDegraded(f.PeerID) // R-04修复B: 标记降级
				return
			}
			batchSize = batchSize / 2
			if batchSize < 128 {
				batchSize = 128
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	atomic.StoreInt32(&m.node.consecutiveSuccess, 0)
}

func (m *BatchSyncManager) String() string {
	return fmt.Sprintf("BatchSyncManager(enabled=%v, lagThreshold=%d, maxBatch=%d, tasks=%d)",
		m.config.Enable, m.config.LagThreshold, m.config.MaxBatchSize, len(m.tasks))
}

// sendSnapshot 刀三: 快照兜底路径 — 通过 HTTP 将快照发送给 follower
func (m *BatchSyncManager) sendSnapshot(peerID string) {
	if m.node.getSnapshotData == nil {
		m.node.logf("[SYNC] leader=%s follower=%s getSnapshotData 回调未设置, 跳过快照兜底", m.node.id, peerID)
		return
	}

	snapshotData, lastIdx, lastTerm, err := m.node.getSnapshotData()
	if err != nil {
		m.node.logf("[SYNC] leader=%s follower=%s 读取快照失败: %v", m.node.id, peerID, err)
		return
	}

	httpAddr, ok := m.node.GetPeerHttpAddr(peerID)
	if !ok {
		m.node.logf("[SYNC] leader=%s follower=%s HTTP 地址未知, 跳过快照兜底", m.node.id, peerID)
		return
	}

	req := struct {
		SnapshotData      []byte `json:"snapshot_data"`
		LastIncludedIndex int64  `json:"last_included_index"`
		LastIncludedTerm  int64  `json:"last_included_term"`
		LeaderCommit      int64  `json:"leader_commit"`
	}{
		SnapshotData:      snapshotData,
		LastIncludedIndex: lastIdx,
		LastIncludedTerm:  lastTerm,
		LeaderCommit:      m.node.getCommitIdx(),
	}

	body, err := json.Marshal(req)
	if err != nil {
		m.node.logf("[SYNC] leader=%s follower=%s 快照请求序列化失败: %v", m.node.id, peerID, err)
		return
	}

	url := fmt.Sprintf("http://%s/raft/install-snapshot", httpAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		m.node.logf("[SYNC] leader=%s follower=%s HTTP 请求构造失败: %v", m.node.id, peerID, err)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		m.node.logf("[SYNC] leader=%s follower=%s 快照传输失败: %v", m.node.id, peerID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		m.node.UpdateFollowerProgress(peerID, lastIdx)
		m.node.ClearFollowerDegraded(peerID)
		m.node.logf("[SYNC] leader=%s follower=%s 快照安装成功: lastIncludedIndex=%d, nextIdx=%d",
			m.node.id, peerID, lastIdx, lastIdx+1)
	} else {
		m.node.logf("[SYNC] leader=%s follower=%s 快照安装失败: HTTP %d", m.node.id, peerID, resp.StatusCode)
	}
}

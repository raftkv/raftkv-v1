// =========================================================================
// RaftKV V2.3 — Raft Joint Consensus 动态成员变更
//
// 实现原理（单节点变更法，Diego Ongaro 博士论文 §4.3）：
//   1. C_old → C_old,new → C_new 三阶段变更
//   2. C_old,new 状态下，需要 C_old 和 C_new 各自的多数派同时同意
//   3. 配置变更作为特殊日志条目通过 Raft 复制
//   4. 新节点先追赶日志，再进入联合共识阶段
// =========================================================================

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	pb "raftkv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConfigChangeType int

const (
	ConfigAdd    ConfigChangeType = 1
	ConfigRemove ConfigChangeType = 2
)

type ConfigChange struct {
	Type     ConfigChangeType `json:"type"`
	NodeID   string           `json:"node_id"`
	Address  string           `json:"address"`
	OldPeers []string         `json:"old_peers"`
	NewPeers []string         `json:"new_peers"`
}

type ClusterConfig struct {
	mu      sync.RWMutex
	current []string
	old     []string
	joint   bool
	pending *ConfigChange
	logger  Logger
}

func NewClusterConfig(initialPeers []string, logger Logger) *ClusterConfig {
	peers := make([]string, len(initialPeers))
	copy(peers, initialPeers)
	return &ClusterConfig{
		current: peers,
		logger:  logger,
	}
}

const configEntryMarker byte = 0x01

func encodeConfigChange(cc ConfigChange) []byte {
	data, _ := json.Marshal(cc)
	result := make([]byte, 1, 1+len(data))
	result[0] = configEntryMarker
	result = append(result, data...)
	return result
}

func decodeConfigChange(cmd []byte) (ConfigChange, bool) {
	if len(cmd) == 0 || cmd[0] != configEntryMarker {
		return ConfigChange{}, false
	}
	var cc ConfigChange
	if err := json.Unmarshal(cmd[1:], &cc); err != nil {
		return ConfigChange{}, false
	}
	return cc, true
}

func isConfigEntry(cmd []byte) bool {
	return len(cmd) > 0 && cmd[0] == configEntryMarker
}

func stringInSlice(s string, list []string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func (cc *ClusterConfig) quorumSize() int {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	if cc.joint && len(cc.old) > 0 {
		oldQuorum := len(cc.old)/2 + 1
		newPeers := cc.current
		if cc.pending != nil {
			newPeers = cc.pending.NewPeers
		}
		newQuorum := len(newPeers)/2 + 1
		return oldQuorum + newQuorum
	}
	return len(cc.current)/2 + 1
}

func (cc *ClusterConfig) oldPeers() []string {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	if !cc.joint || len(cc.old) == 0 {
		return nil
	}
	peers := make([]string, len(cc.old))
	copy(peers, cc.old)
	return peers
}

func (cc *ClusterConfig) newPeers() []string {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	if cc.joint && cc.pending != nil {
		peers := make([]string, len(cc.pending.NewPeers))
		copy(peers, cc.pending.NewPeers)
		return peers
	}
	peers := make([]string, len(cc.current))
	copy(peers, cc.current)
	return peers
}

func (cc *ClusterConfig) allNodes() []string {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	seen := make(map[string]bool)
	nodes := make([]string, 0)
	for _, n := range cc.current {
		if !seen[n] {
			seen[n] = true
			nodes = append(nodes, n)
		}
	}
	if cc.joint {
		for _, n := range cc.old {
			if !seen[n] {
				seen[n] = true
				nodes = append(nodes, n)
			}
		}
	}
	return nodes
}

func (cc *ClusterConfig) beginJointConsensus(change ConfigChange) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.old = make([]string, len(cc.current))
	copy(cc.old, cc.current)
	cc.joint = true
	cc.pending = &change
	if cc.logger != nil {
		cc.logger.Printf("[membership] 进入联合共识: C_old=%v, 变更类型=%d 节点=%s",
			cc.old, change.Type, change.NodeID)
	}
}

func (cc *ClusterConfig) commitConfigChange(change ConfigChange) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.current = make([]string, len(change.NewPeers))
	copy(cc.current, change.NewPeers)
	cc.old = nil
	cc.joint = false
	cc.pending = nil
	if cc.logger != nil {
		cc.logger.Printf("[membership] 配置变更已提交: C_new=%v", cc.current)
	}
}

func (cc *ClusterConfig) currentPeers() []string {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	peers := make([]string, len(cc.current))
	copy(peers, cc.current)
	return peers
}

func (cc *ClusterConfig) isInConfig(nodeID string) bool {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	for _, n := range cc.current {
		if n == nodeID {
			return true
		}
	}
	return false
}

// AddNode 向集群动态添加新节点
func (rn *RaftNode) AddNode(nodeID, address string) error {
	rn.mu.Lock()
	if rn.state != StateLeader {
		rn.mu.Unlock()
		return fmt.Errorf("只有 Leader 才能执行成员变更")
	}
	for _, p := range rn.peers {
		if p.ID == nodeID {
			rn.mu.Unlock()
			return fmt.Errorf("节点 %s 已存在", nodeID)
		}
	}
	oldPeers := rn.config.currentPeers()
	newPeers := make([]string, 0, len(oldPeers)+1)
	newPeers = append(newPeers, oldPeers...)
	newPeers = append(newPeers, nodeID)
	change := ConfigChange{
		Type:     ConfigAdd,
		NodeID:   nodeID,
		Address:  address,
		OldPeers: oldPeers,
		NewPeers: newPeers,
	}
	rn.config.beginJointConsensus(change)
	rn.addPeerDynamic(nodeID, address)
	newIdx := int64(len(rn.logs)) + 1
	addCmd := encodeConfigChange(change)
	rn.logs = append(rn.logs, RaftLog{
		Index:   newIdx,
		Term:    rn.term,
		Command: addCmd,
		// V2.5.1: 国密 SM3 链式防篡改摘要（真实计算，修复 V2.5.0 透传断层）
		SM3Hash: ComputeEntrySM3(PrevEntryHash(rn.logs, newIdx), rn.term, newIdx, addCmd),
	})
	rn.nextIdx[nodeID] = 1 // V2.3: 新节点从头接收所有日志（含配置条目）
	rn.matchIdx[nodeID] = 0
	rn.logf("[raft/%s] [membership] AddNode: %s @ %s, 配置条目 index=%d, C_old=%v → C_new=%v",
		rn.id, nodeID, address, newIdx, oldPeers, newPeers)
	rn.updateStats()
	rn.mu.Unlock()
	return nil
}

// RemoveNode 从集群动态移除节点
func (rn *RaftNode) RemoveNode(nodeID string) error {
	rn.mu.Lock()
	if rn.state != StateLeader {
		rn.mu.Unlock()
		return fmt.Errorf("只有 Leader 才能执行成员变更")
	}
	found := false
	for _, p := range rn.peers {
		if p.ID == nodeID {
			found = true
			break
		}
	}
	if !found {
		rn.mu.Unlock()
		return fmt.Errorf("节点 %s 不在集群中", nodeID)
	}
	if nodeID == rn.id {
		rn.mu.Unlock()
		return fmt.Errorf("不能移除 Leader 节点，请先转移 Leadership")
	}
	oldPeers := rn.config.currentPeers()
	newPeers := make([]string, 0, len(oldPeers)-1)
	for _, p := range oldPeers {
		if p != nodeID {
			newPeers = append(newPeers, p)
		}
	}
	change := ConfigChange{
		Type:     ConfigRemove,
		NodeID:   nodeID,
		OldPeers: oldPeers,
		NewPeers: newPeers,
	}
	rn.config.beginJointConsensus(change)
	newIdx := int64(len(rn.logs)) + 1
	rmCmd := encodeConfigChange(change)
	rn.logs = append(rn.logs, RaftLog{
		Index:   newIdx,
		Term:    rn.term,
		Command: rmCmd,
		// V2.5.1: 国密 SM3 链式防篡改摘要（真实计算，修复 V2.5.0 透传断层）
		SM3Hash: ComputeEntrySM3(PrevEntryHash(rn.logs, newIdx), rn.term, newIdx, rmCmd),
	})
	rn.logf("[raft/%s] [membership] RemoveNode: %s, 配置条目 index=%d, C_old=%v → C_new=%v",
		rn.id, nodeID, newIdx, oldPeers, newPeers)
	rn.updateStats()
	rn.mu.Unlock()
	return nil
}

func (rn *RaftNode) addPeerDynamic(nodeID, address string) {
	rn.peers = append(rn.peers, PeerInfo{ID: nodeID, Address: address})
	rn.peerAddrs[nodeID] = address
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(16*1024*1024),
			grpc.MaxCallSendMsgSize(16*1024*1024),
		),
	)
	if err != nil {
		rn.logf("[raft/%s] [membership] 连接新节点 %s 失败: %v", rn.id, nodeID, err)
		return
	}
	client := pb.NewRaftServiceClient(conn)
	rn.peerClients[nodeID] = client
	rn.logf("[raft/%s] [membership] 已连接新节点: %s @ %s", rn.id, nodeID, address)
}

func (rn *RaftNode) removePeerDynamic(nodeID string) {
	newPeers := make([]PeerInfo, 0, len(rn.peers))
	for _, p := range rn.peers {
		if p.ID != nodeID {
			newPeers = append(newPeers, p)
		}
	}
	rn.peers = newPeers
	delete(rn.peerAddrs, nodeID)
	delete(rn.nextIdx, nodeID)
	delete(rn.matchIdx, nodeID)
	delete(rn.peerClients, nodeID)
	rn.logf("[raft/%s] [membership] 已移除节点: %s", rn.id, nodeID)
}

func (rn *RaftNode) applyCommittedConfig() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	for i := rn.lastApplied + 1; i <= rn.commitIdx; i++ {
		if int(i-1) < 0 || int(i-1) >= len(rn.logs) {
			break
		}
		log := rn.logs[i-1]
		if isConfigEntry(log.Command) {
			change, ok := decodeConfigChange(log.Command)
			if !ok {
				continue
			}
			rn.config.commitConfigChange(change)
			if change.Type == ConfigRemove && rn.state == StateLeader {
				rn.removePeerDynamic(change.NodeID)
			}
			rn.logf("[raft/%s] [membership] 配置变更已应用: index=%d, type=%d, node=%s, C_new=%v",
				rn.id, i, change.Type, change.NodeID, change.NewPeers)
		}
		rn.lastApplied = i
	}
}

func HandleAddNode(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if degraded, reason := IsDegradedMode(); degraded {
			http.Error(w, fmt.Sprintf("降级只读模式，禁止增删节点。原因: %s", reason), http.StatusForbidden)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		nodeID := r.URL.Query().Get("node_id")
		address := r.URL.Query().Get("address")
		if nodeID == "" || address == "" {
			http.Error(w, "Missing node_id or address", http.StatusBadRequest)
			return
		}
		if err := node.AddNode(nodeID, address); err != nil {
			http.Error(w, fmt.Sprintf("AddNode failed: %v", err), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Node %s added at %s\n", nodeID, address)
	}
}

func HandleRemoveNode(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if degraded, reason := IsDegradedMode(); degraded {
			http.Error(w, fmt.Sprintf("降级只读模式，禁止增删节点。原因: %s", reason), http.StatusForbidden)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		nodeID := r.URL.Query().Get("node_id")
		if nodeID == "" {
			http.Error(w, "Missing node_id", http.StatusBadRequest)
			return
		}
		if err := node.RemoveNode(nodeID); err != nil {
			http.Error(w, fmt.Sprintf("RemoveNode failed: %v", err), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Node %s removed\n", nodeID)
	}
}

func HandleClusterMembers(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		node.mu.RLock()
		members := node.config.currentPeers()
		allNodes := node.config.allNodes()
		quorum := node.config.quorumSize()
		joint := node.config.joint
		node.mu.RUnlock()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"current_members": members,
			"all_nodes":       allNodes,
			"quorum_size":     quorum,
			"joint_consensus": joint,
			"peer_count":      len(members),
		})
	}
}

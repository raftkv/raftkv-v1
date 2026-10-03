package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type WALGateAuditRecord struct {
	TriggerTime    int64  `json:"trigger_time"`
	NodeID         string `json:"node_id"`
	Reason         string `json:"reason"`
	PreviousRole   string `json:"previous_role"`
	RejectedWrites int64  `json:"rejected_writes"`
	RecoverTime    int64  `json:"recover_time"`
}

type WALGateAuditLog struct {
	mu       sync.Mutex
	filePath string
	nodeID   string
}

func NewWALGateAuditLog(nodeID string, dataDir string) *WALGateAuditLog {
	path := fmt.Sprintf("%s/wal_gate_audit-%s.log", dataDir, nodeID)
	return &WALGateAuditLog{
		filePath: path,
		nodeID:   nodeID,
	}
}

func (a *WALGateAuditLog) Record(r WALGateAuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if r.TriggerTime == 0 {
		r.TriggerTime = time.Now().Unix()
	}
	r.NodeID = a.nodeID

	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("审计记录序列化失败: %w", err)
	}

	f, err := os.OpenFile(a.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("审计日志文件打开失败: %w", err)
	}
	defer f.Close()

	_, err = f.Write(append(data, '\n'))
	return err
}

func (a *WALGateAuditLog) Query() []WALGateAuditRecord {
	a.mu.Lock()
	defer a.mu.Unlock()

	data, err := os.ReadFile(a.filePath)
	if err != nil {
		return nil
	}

	var records []WALGateAuditRecord
	for _, line := range splitLines(data) {
		if len(line) == 0 {
			continue
		}
		var r WALGateAuditRecord
		if json.Unmarshal(line, &r) == nil {
			records = append(records, r)
		}
	}
	return records
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}

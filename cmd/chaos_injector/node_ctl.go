package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

type NodeController struct {
	containerPrefix string
	httpPortBase    int
	httpClient      *http.Client
}

func NewNodeController(prefix string, portBase int) *NodeController {
	return &NodeController{
		containerPrefix: prefix,
		httpPortBase:    portBase,
		httpClient: &http.Client{
			Timeout: 500 * time.Millisecond,
		},
	}
}

func (nc *NodeController) containerName(nodeID string) string {
	return nc.containerPrefix + strings.TrimPrefix(nodeID, "node-")
}

func (nc *NodeController) httpPort(nodeID string) int {
	return nc.httpPortBase + parseNodeNum(nodeID) - 1
}

func parseNodeNum(nodeID string) int {
	var n int
	fmt.Sscanf(nodeID, "node-%d", &n)
	return n
}

func (nc *NodeController) KillNode(nodeID string) error {
	cmd := exec.Command("docker", "kill", "--signal=9", nc.containerName(nodeID))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker kill %s: %v: %s", nodeID, err, string(output))
	}
	return nil
}

func (nc *NodeController) RestartNode(nodeID string) error {
	cmd := exec.Command("docker", "start", nc.containerName(nodeID))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker start %s: %v: %s", nodeID, err, string(output))
	}
	return nil
}

func (nc *NodeController) NetworkDisconnect(nodeID string, networkName string) error {
	cmd := exec.Command("docker", "network", "disconnect", networkName, nc.containerName(nodeID))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker network disconnect %s: %v: %s", nodeID, err, string(output))
	}
	return nil
}

func (nc *NodeController) NetworkConnect(nodeID string, networkName string) error {
	cmd := exec.Command("docker", "network", "connect", networkName, nc.containerName(nodeID))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker network connect %s: %v: %s", nodeID, err, string(output))
	}
	return nil
}

func (nc *NodeController) WaitNodeHealthy(nodeID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	port := nc.httpPort(nodeID)
	url := fmt.Sprintf("http://localhost:%d/health/live", port)

	for time.Now().Before(deadline) {
		resp, err := nc.httpClient.Get(url)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("node %s not healthy after %v", nodeID, timeout)
}

func (nc *NodeController) QueryLeader() (string, error) {
	for i := 1; i <= 5; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		port := nc.httpPort(nodeID)
		url := fmt.Sprintf("http://localhost:%d/raft/status", port)

		resp, err := nc.httpClient.Get(url)
		if err != nil {
			continue
		}

		var stats map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		state, _ := stats["state"].(string)
		if state == "StateLeader" || state == "Leader" {
			return nodeID, nil
		}
	}

	leader, err := nc.queryLeaderFromStats()
	if err != nil {
		return "", fmt.Errorf("no leader found: %v", err)
	}
	return leader, nil
}

func (nc *NodeController) queryLeaderFromStats() (string, error) {
	for i := 1; i <= 5; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		port := nc.httpPort(nodeID)
		url := fmt.Sprintf("http://localhost:%d/raft/stats", port)

		resp, err := nc.httpClient.Get(url)
		if err != nil {
			continue
		}

		var body strings.Builder
		buf := make([]byte, 4096)
		n, _ := resp.Body.Read(buf)
		body.Write(buf[:n])
		resp.Body.Close()

		text := body.String()
		if strings.Contains(text, "state=StateLeader") || strings.Contains(text, "state=Leader") {
			return nodeID, nil
		}
	}
	return "", fmt.Errorf("no leader in /raft/stats")
}

func (nc *NodeController) GetFollowers() ([]string, error) {
	var followers []string
	for i := 1; i <= 5; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		port := nc.httpPort(nodeID)
		url := fmt.Sprintf("http://localhost:%d/raft/status", port)

		resp, err := nc.httpClient.Get(url)
		if err != nil {
			continue
		}

		var stats map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		state, _ := stats["state"].(string)
		if state == "StateFollower" || state == "Follower" {
			followers = append(followers, nodeID)
		}
	}
	return followers, nil
}

func (nc *NodeController) GetNodeStats(nodeID string) (*RaftStats, error) {
	port := nc.httpPort(nodeID)
	url := fmt.Sprintf("http://localhost:%d/raft/status", port)

	resp, err := nc.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GetNodeStats %s: %v", nodeID, err)
	}
	defer resp.Body.Close()

	var stats RaftStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, fmt.Errorf("decode %s: %v", nodeID, err)
	}
	return &stats, nil
}

func (nc *NodeController) parseRaftStatsText(text string) map[string]string {
	result := make(map[string]string)
	for _, field := range strings.Fields(text) {
		if parts := strings.SplitN(field, "=", 2); len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}
	return result
}

func (nc *NodeController) SnapshotConfirmedEntries(leaderID string, sampleCount int) ([]EntrySnapshot, error) {
	stats, err := nc.GetNodeStats(leaderID)
	if err != nil {
		return nil, err
	}

	var snapshots []EntrySnapshot
	for i := 0; i < sampleCount; i++ {
		idx := stats.Commit - int64(sampleCount) + int64(i)
		if idx < 0 {
			continue
		}
		port := nc.httpPort(leaderID)
		url := fmt.Sprintf("http://localhost:%d/raft/entry?index=%d", port, idx)
		resp, err := nc.httpClient.Get(url)
		if err != nil {
			continue
		}
		var entry struct {
			Index int64  `json:"index"`
			Term  int64  `json:"term"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()
		snapshots = append(snapshots, EntrySnapshot{
			EntryIndex: entry.Index,
			EntryTerm:  entry.Term,
			EntryValue: entry.Value,
		})
	}
	return snapshots, nil
}

func (nc *NodeController) VerifyEntrySurvival(newLeaderID string, snapshots []EntrySnapshot) (*SurvivalResult, error) {
	stats, err := nc.GetNodeStats(newLeaderID)
	if err != nil {
		return nil, err
	}

	result := &SurvivalResult{TotalCount: len(snapshots)}
	for _, snap := range snapshots {
		if snap.EntryIndex > stats.Commit {
			result.MismatchedEntries = append(result.MismatchedEntries, EntryMismatch{
				EntryIndex: snap.EntryIndex,
			})
			continue
		}

		port := nc.httpPort(newLeaderID)
		url := fmt.Sprintf("http://localhost:%d/raft/entry?index=%d", port, snap.EntryIndex)
		resp, err := nc.httpClient.Get(url)
		if err != nil {
			result.MismatchedEntries = append(result.MismatchedEntries, EntryMismatch{
				EntryIndex: snap.EntryIndex,
			})
			continue
		}

		var entry struct {
			Index int64  `json:"index"`
			Term  int64  `json:"term"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
			resp.Body.Close()
			result.MismatchedEntries = append(result.MismatchedEntries, EntryMismatch{
				EntryIndex: snap.EntryIndex,
			})
			continue
		}
		resp.Body.Close()

		if entry.Term == snap.EntryTerm && entry.Value == snap.EntryValue {
			result.SurvivedCount++
		} else {
			result.MismatchedEntries = append(result.MismatchedEntries, EntryMismatch{
				EntryIndex:    snap.EntryIndex,
				ExpectedTerm:  snap.EntryTerm,
				ActualTerm:    entry.Term,
				ExpectedValue: snap.EntryValue,
				ActualValue:   entry.Value,
			})
		}
	}

	if result.TotalCount > 0 {
		result.SurvivalRate = float64(result.SurvivedCount) / float64(result.TotalCount) * 100
	}
	return result, nil
}

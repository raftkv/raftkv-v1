package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	leaderURL := "http://localhost:9001"
	if len(os.Args) > 1 {
		leaderURL = os.Args[1]
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	fmt.Println("=== RaftKV Example Client ===")
	fmt.Printf("Leader: %s\n\n", leaderURL)

	healthResp, err := client.Get(leaderURL + "/health/live")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot connect to %s: %v\n", leaderURL, err)
		os.Exit(1)
	}
	healthResp.Body.Close()
	fmt.Println("[1] Health check: OK")

	statusResp, err := client.Get(leaderURL + "/raft/status")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Status check failed: %v\n", err)
		os.Exit(1)
	}
	statusBody, _ := io.ReadAll(statusResp.Body)
	statusResp.Body.Close()
	fmt.Printf("[2] Raft status: %s\n", string(statusBody))

	entries := []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}{
		{"user:1", "Alice"},
		{"user:2", "Bob"},
		{"user:3", "Charlie"},
		{"counter:visits", "0"},
		{"config:timeout", "30s"},
	}

	fmt.Println("\n[3] Proposing key-value pairs via /raft/propose...")
	type proposeResp struct {
		Success bool   `json:"success"`
		Index   int64  `json:"index"`
		Error   string `json:"error"`
	}
	var indices []int64
	for _, entry := range entries {
		body, _ := json.Marshal(entry)
		resp, err := client.Post(leaderURL+"/raft/propose", "application/json", bytes.NewReader(body))
		if err != nil {
			fmt.Fprintf(os.Stderr, "  POST %s failed: %v\n", entry.Key, err)
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var pr proposeResp
		json.Unmarshal(respBody, &pr)
		if resp.StatusCode == 200 && pr.Success {
			fmt.Printf("  POST %s=%s -> index=%d\n", entry.Key, entry.Value, pr.Index)
			indices = append(indices, pr.Index)
		} else if resp.StatusCode == 503 {
			fmt.Printf("  POST %s=%s -> REJECTED (degraded mode, writes require license)\n", entry.Key, entry.Value)
		} else {
			fmt.Printf("  POST %s=%s -> HTTP %d (%s)\n", entry.Key, entry.Value, resp.StatusCode, string(respBody))
		}
	}

	fmt.Println("\n[4] Reading entries via /raft/get?index=N...")
	for i, idx := range indices {
		resp, err := client.Get(fmt.Sprintf("%s/raft/get?index=%d", leaderURL, idx))
		if err != nil {
			fmt.Fprintf(os.Stderr, "  GET index=%d failed: %v\n", idx, err)
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Printf("  GET %s -> index=%d HTTP %d: %s\n", entries[i].Key, idx, resp.StatusCode, string(respBody))
	}

	fmt.Println("\n[5] Cluster members...")
	membersResp, err := client.Get(leaderURL + "/cluster/members")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cluster members failed: %v\n", err)
	} else {
		membersBody, _ := io.ReadAll(membersResp.Body)
		membersResp.Body.Close()
		fmt.Printf("  %s\n", string(membersBody))
	}

	fmt.Println("\n[6] Latency decomposition...")
	latencyResp, err := client.Get(leaderURL + "/latency/decomp")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Latency decomp failed: %v\n", err)
	} else {
		latencyBody, _ := io.ReadAll(latencyResp.Body)
		latencyResp.Body.Close()
		fmt.Printf("  %s\n", string(latencyBody))
	}

	fmt.Println("\n=== Example complete ===")
}

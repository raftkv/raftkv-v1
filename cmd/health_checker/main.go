package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type NodeHealth struct {
	NodeID    string                 `json:"node_id"`
	Address   string                 `json:"address"`
	Live      bool                   `json:"live"`
	Ready     bool                   `json:"ready"`
	RaftStats map[string]interface{} `json:"raft_stats,omitempty"`
	Error     string                 `json:"error,omitempty"`
	Latency   string                 `json:"latency,omitempty"`
}

type HealthReport struct {
	Timestamp string       `json:"timestamp"`
	Version   string       `json:"version"`
	Total     int          `json:"total"`
	Healthy   int          `json:"healthy"`
	Nodes     []NodeHealth `json:"nodes"`
}

type nodeTarget struct {
	id   string
	addr string
}

var defaultNodes = []nodeTarget{
	{"node-1", "localhost:9001"},
	{"node-2", "localhost:9002"},
	{"node-3", "localhost:9003"},
	{"node-4", "localhost:9104"},
	{"node-5", "localhost:9105"},
}

func parsePorts(portsStr string) ([]nodeTarget, error) {
	parts := strings.Split(portsStr, ",")
	var nodes []nodeTarget
	for i, p := range parts {
		p = strings.TrimSpace(p)
		port, err := strconv.Atoi(p)
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("invalid port: %s", p)
		}
		nodes = append(nodes, nodeTarget{
			id:   fmt.Sprintf("node-%d", i+1),
			addr: fmt.Sprintf("localhost:%d", port),
		})
	}
	return nodes, nil
}

func checkNode(nodeID, addr string) NodeHealth {
	result := NodeHealth{NodeID: nodeID, Address: addr}
	client := &http.Client{Timeout: 3 * time.Second}

	start := time.Now()
	resp, err := client.Get("http://" + addr + "/health/live")
	elapsed := time.Since(start)
	result.Latency = elapsed.Round(time.Millisecond).String()

	if err != nil {
		result.Live = false
		result.Error = err.Error()
		return result
	}
	resp.Body.Close()
	result.Live = resp.StatusCode == http.StatusOK

	resp, err = client.Get("http://" + addr + "/health/ready")
	if err != nil {
		result.Ready = false
		if result.Error == "" {
			result.Error = err.Error()
		}
		return result
	}
	resp.Body.Close()
	result.Ready = resp.StatusCode == http.StatusOK

	resp, err = client.Get("http://" + addr + "/raft/status")
	if err != nil {
		if result.Error == "" {
			result.Error = err.Error()
		}
		return result
	}
	defer resp.Body.Close()

	var stats map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err == nil {
		result.RaftStats = stats
	}

	return result
}

func main() {
	nodes := defaultNodes

	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		if arg == "--ports" && i+1 < len(os.Args) {
			i++
			parsed, err := parsePorts(os.Args[i])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			nodes = parsed
		} else if arg == "-h" || arg == "--help" {
			fmt.Println("Usage: health_checker [options]")
			fmt.Println()
			fmt.Println("Options:")
			fmt.Println("  --ports 9001,9002,9003,9104,9105")
			fmt.Println("           Specify HTTP ports for each node (comma-separated)")
			fmt.Println("           Default: 9001,9002,9003,9104,9105")
			fmt.Println("  -h, --help")
			fmt.Println("           Show this help message")
			os.Exit(0)
		}
	}

	report := HealthReport{
		Timestamp: time.Now().Format(time.RFC3339),
		Version:   "V1.1",
		Total:     len(nodes),
		Nodes:     make([]NodeHealth, 0, len(nodes)),
	}

	for _, n := range nodes {
		h := checkNode(n.id, n.addr)
		report.Nodes = append(report.Nodes, h)
		if h.Live && h.Ready {
			report.Healthy++
		}
	}

	out, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(out))
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	pb "raftkv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	target := flag.String("target", "", "目标节点 gRPC 地址 (host:port)")
	count := flag.Int("count", 10000, "写入条数")
	batchSize := flag.Int("batch", 100, "每批条数")
	httpPort := flag.String("http-port", "", "目标节点 HTTP 端口 (用于查询 term)")
	flag.Parse()

	if *target == "" {
		fmt.Fprintln(os.Stderr, "用法: cluster_client -target host:port -count N -http-port PORT")
		os.Exit(1)
	}

	fmt.Printf("══════════════════════════════════════════════════\n")
	fmt.Printf("  RaftKV 集群写入客户端\n")
	fmt.Printf("  目标: %s  条数: %d  批大小: %d\n", *target, *count, *batchSize)
	fmt.Printf("══════════════════════════════════════════════════\n\n")

	// 查询节点当前 term 和 leader_id
	raftTerm := int64(1)
	leaderID := "external"
	if *httpPort != "" {
		statusURL := fmt.Sprintf("http://localhost:%s/raft/status", *httpPort)
		resp, err := http.Get(statusURL)
		if err == nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			var status map[string]interface{}
			if json.Unmarshal(body, &status) == nil {
				if t, ok := status["term"].(float64); ok {
					raftTerm = int64(t)
				}
				if l, ok := status["leader_id"].(string); ok && l != "" {
					leaderID = l
				}
				if s, ok := status["state"].(string); ok {
					fmt.Printf("  节点状态: state=%s term=%d leader=%s\n\n", s, raftTerm, leaderID)
				}
			}
		}
	}

	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "gRPC 连接失败: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewRaftServiceClient(conn)

	var seq atomic.Int64
	var success atomic.Int64
	var fail atomic.Int64

	start := time.Now()
	totalBatches := (*count + *batchSize - 1) / *batchSize

	for b := 0; b < totalBatches; b++ {
		entries := make([]*pb.LogEntry, 0, *batchSize)
		startIdx := int64(b**batchSize + 1)
		for i := 0; i < *batchSize && int(startIdx)+i-1 < *count; i++ {
			idx := startIdx + int64(i)
			entries = append(entries, &pb.LogEntry{
				Term:    raftTerm,
				Index:   idx,
				Command: []byte(fmt.Sprintf(`{"src":"cluster-client","index":%d,"ts":%d}`, idx, time.Now().UnixNano())),
			})
		}

		req := &pb.AppendEntriesRequest{
			Term:         raftTerm,
			LeaderId:     leaderID,
			PrevLogIndex: startIdx - 1,
			PrevLogTerm:  raftTerm,
			Entries:      entries,
			LeaderCommit: startIdx + int64(len(entries)) - 1,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.AppendEntries(ctx, req)
		cancel()

		seq.Add(int64(len(entries)))
		if err != nil {
			fail.Add(int64(len(entries)))
			if fail.Load() < 10 {
				fmt.Fprintf(os.Stderr, "RPC 错误: %v\n", err)
			}
		} else if resp.Success {
			success.Add(int64(len(entries)))
		} else {
			fail.Add(int64(len(entries)))
		}
	}

	elapsed := time.Since(start)
	total := seq.Load()
	succ := success.Load()
	fl := fail.Load()
	tps := float64(total) / elapsed.Seconds()

	fmt.Printf("──────────────────────────────────────────────────\n")
	fmt.Printf("  总写入:   %d\n", total)
	fmt.Printf("  成功:     %d\n", succ)
	fmt.Printf("  失败:     %d\n", fl)
	fmt.Printf("  耗时:     %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  TPS:      %.0f\n", tps)
	fmt.Printf("──────────────────────────────────────────────────\n")
}

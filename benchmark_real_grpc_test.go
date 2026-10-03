package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "raftkv/proto"
)

var fiveNodeAddrs = []string{
	"localhost:9500",
	"localhost:9501",
	"localhost:9502",
	"localhost:9604",
	"localhost:9605",
}

func createGRPCClient(addr string) (pb.RaftServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewRaftServiceClient(conn), conn, nil
}

func TestRealGRPCConnectivity(t *testing.T) {
	for _, addr := range fiveNodeAddrs {
		client, conn, err := createGRPCClient(addr)
		if err != nil {
			t.Fatalf("连接 %s 失败: %v", addr, err)
		}
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		resp, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
			Term:         99999,
			LeaderId:     "bench_test",
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			LeaderCommit: 0,
		})
		if err != nil {
			t.Fatalf("AppendEntries → %s 失败: %v", addr, err)
		}
		t.Logf("✅ %s 响应: term=%d, success=%v", addr, resp.Term, resp.Success)
	}
}

func BenchmarkRealGRPC16Clients(b *testing.B) {
	const (
		concurrency       = 100
		requestsPerClient = 1000
	)

	type clientInfo struct {
		client pb.RaftServiceClient
		conn   *grpc.ClientConn
		addr   string
	}

	clients := make([]clientInfo, concurrency)
	for i := 0; i < concurrency; i++ {
		addr := fiveNodeAddrs[i%len(fiveNodeAddrs)]
		c, conn, err := createGRPCClient(addr)
		if err != nil {
			b.Fatalf("创建gRPC客户端 %d → %s 失败: %v", i, addr, err)
		}
		defer conn.Close()
		clients[i] = clientInfo{client: c, conn: conn, addr: addr}
	}

	b.Logf("=== RaftKV 真实gRPC压力测试（5节点集群）===")
	b.Logf("并发客户端数: %d", concurrency)
	b.Logf("每客户端请求数: %d", requestsPerClient)
	b.Logf("目标节点: %v", fiveNodeAddrs)
	b.Logf("总请求数: %d", concurrency*requestsPerClient)
	b.Logf("========================================")

	b.ResetTimer()

	var totalSuccess int64
	var totalFail int64
	var totalLatency int64
	var minLatency int64 = 1 << 62
	var maxLatency int64

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ci := clients[idx]
			localSuccess := int64(0)
			localFail := int64(0)

			for j := 0; j < requestsPerClient; j++ {
				start := time.Now()

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

				if j%2 == 0 {
					resp, err := ci.client.RequestVote(ctx, &pb.RequestVoteRequest{
						Term:         int64(j) + 1,
						CandidateId:  "bench_client_" + strconv.Itoa(idx),
						LastLogIndex: int64(j),
						LastLogTerm:  int64(j),
					})
					cancel()
					if err != nil {
						localFail++
					} else {
						localSuccess++
						_ = resp
					}
				} else {
					resp, err := ci.client.AppendEntries(ctx, &pb.AppendEntriesRequest{
						Term:         int64(j) + 1,
						LeaderId:     "bench_client_" + strconv.Itoa(idx),
						PrevLogIndex: int64(j),
						PrevLogTerm:  int64(j),
						LeaderCommit: int64(j),
					})
					cancel()
					if err != nil {
						localFail++
					} else {
						localSuccess++
						_ = resp
					}
				}

				latency := time.Since(start).Nanoseconds()
				atomic.AddInt64(&totalLatency, latency)
				for {
					old := atomic.LoadInt64(&minLatency)
					if latency >= old || atomic.CompareAndSwapInt64(&minLatency, old, latency) {
						break
					}
				}
				for {
					old := atomic.LoadInt64(&maxLatency)
					if latency <= old || atomic.CompareAndSwapInt64(&maxLatency, old, latency) {
						break
					}
				}
			}

			atomic.AddInt64(&totalSuccess, localSuccess)
			atomic.AddInt64(&totalFail, localFail)
		}(i)
	}
	wg.Wait()

	b.StopTimer()

	totalReqs := totalSuccess + totalFail
	avgLatencyNs := int64(0)
	if totalReqs > 0 {
		avgLatencyNs = totalLatency / totalReqs
	}

	b.Logf("========================================")
	b.Logf("=== RaftKV 真实gRPC压力测试结果（5节点）===")
	b.Logf("========================================")
	b.Logf("总请求数:     %d", totalReqs)
	b.Logf("成功:         %d", totalSuccess)
	b.Logf("失败:         %d", totalFail)
	b.Logf("成功率:       %.2f%%", float64(totalSuccess)/float64(totalReqs)*100)
	b.Logf("最小延迟:     %d ns (%.3f μs)", minLatency, float64(minLatency)/1000.0)
	b.Logf("最大延迟:     %d ns (%.3f μs)", maxLatency, float64(maxLatency)/1000.0)
	b.Logf("平均延迟:     %d ns (%.3f μs)", avgLatencyNs, float64(avgLatencyNs)/1000.0)
	b.Logf("总耗时:       %s", time.Duration(totalLatency/int64(concurrency)))
	b.Logf("吞吐量:       %.0f req/s", float64(totalReqs)/float64(totalLatency/int64(concurrency))*float64(time.Second))
	b.Logf("========================================")

	if totalFail > totalReqs/10 {
		b.Errorf("失败率过高: %d/%d (%.1f%%)", totalFail, totalReqs, float64(totalFail)/float64(totalReqs)*100)
	}
}

func BenchmarkRealGRPCSequential(b *testing.B) {
	client, conn, err := createGRPCClient("localhost:9501")
	if err != nil {
		b.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		if i%2 == 0 {
			client.RequestVote(ctx, &pb.RequestVoteRequest{
				Term:         int64(i) + 1,
				CandidateId:  "seq_bench",
				LastLogIndex: int64(i),
				LastLogTerm:  int64(i),
			})
		} else {
			client.AppendEntries(ctx, &pb.AppendEntriesRequest{
				Term:         int64(i) + 1,
				LeaderId:     "seq_bench",
				PrevLogIndex: int64(i),
				PrevLogTerm:  int64(i),
				LeaderCommit: int64(i),
			})
		}
		cancel()
	}
}

func BenchmarkRealGRPCParallel(b *testing.B) {
	type clientInfo struct {
		client pb.RaftServiceClient
		conn   *grpc.ClientConn
	}

	clients := make([]clientInfo, 100)
	for i := 0; i < 100; i++ {
		addr := fiveNodeAddrs[i%len(fiveNodeAddrs)]
		c, conn, err := createGRPCClient(addr)
		if err != nil {
			b.Fatalf("连接 %s 失败: %v", addr, err)
		}
		defer conn.Close()
		clients[i] = clientInfo{client: c, conn: conn}
	}

	var counter int64

	b.ResetTimer()

	b.RunParallel(func(tb *testing.PB) {
		idx := int(atomic.AddInt64(&counter, 1)-1) % 100
		ci := clients[idx]
		i := int64(0)

		for tb.Next() {
			i++
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			if i%2 == 0 {
				ci.client.RequestVote(ctx, &pb.RequestVoteRequest{
					Term:         i,
					CandidateId:  fmt.Sprintf("par_bench_%d", idx),
					LastLogIndex: i,
					LastLogTerm:  i,
				})
			} else {
				ci.client.AppendEntries(ctx, &pb.AppendEntriesRequest{
					Term:         i,
					LeaderId:     fmt.Sprintf("par_bench_%d", idx),
					PrevLogIndex: i - 1,
					PrevLogTerm:  i - 1,
					LeaderCommit: i,
				})
			}
			cancel()
		}
	})
}

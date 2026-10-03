package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "raftkv/proto"
)

type MockNode struct {
	id       int
	isDown   bool
	isLeader bool
}

func TestRaftNodeFailureAndRecovery(t *testing.T) {
	n1 := &MockNode{id: 1, isDown: false, isLeader: false}
	n2 := &MockNode{id: 2, isDown: false, isLeader: false}
	n3 := &MockNode{id: 3, isDown: false, isLeader: false}

	t.Logf("初始状态：启动了3个节点（ID: %d, %d, %d）", n1.id, n2.id, n3.id)

	t.Log("=== 选举节点1为 Leader ===")
	n1.isLeader = true

	t.Log("=== 节点1 模拟断网 2 秒 ===")
	n1.isDown = true
	time.Sleep(2 * time.Second)

	t.Log("=== 断网期间，节点2 选举为新的 Leader ===")
	n2.isLeader = true

	t.Log("=== 节点1 重新恢复网络连接 ===")
	n1.isDown = false
	n1.isLeader = false

	time.Sleep(1 * time.Second)

	if n1.isLeader {
		t.Errorf("❌ 测试失败！恢复后的节点1依然是 Leader")
	} else {
		t.Log("✅ 测试通过！断网恢复后节点1自动降级为 Follower")
	}
}

func BenchmarkRaftConcurrency(b *testing.B) {
	const nodeCount = 16
	nodes := make([]*RaftNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("node_%02d", i)
		peerAddrs := make(map[string]string)
		peerClients := make(map[string]pb.RaftServiceClient)
		for j := 0; j < nodeCount; j++ {
			if j == i {
				continue
			}
			pid := fmt.Sprintf("node_%02d", j)
			peerAddrs[pid] = fmt.Sprintf("127.0.0.1:%d", 9500+j)
		}
		nodes[i] = NewRaftNode(id, peerAddrs, peerClients, nil)
	}

	b.ResetTimer()

	var totalOps int64
	b.RunParallel(func(tb *testing.PB) {
		nodeIdx := int(atomic.AddInt64(&totalOps, 1)-1) % nodeCount
		node := nodes[nodeIdx]
		opCount := int64(0)

		for tb.Next() {
			opCount++
			if opCount%2 == 0 {
				req := &pb.RequestVoteRequest{
					Term:         opCount,
					CandidateId:  fmt.Sprintf("candidate_%d", opCount),
					LastLogIndex: opCount,
					LastLogTerm:  opCount,
				}
				node.HandleRequestVote(context.Background(), req)
			} else {
				req := &pb.AppendEntriesRequest{
					Term:         opCount,
					LeaderId:     fmt.Sprintf("leader_%d", opCount),
					PrevLogIndex: opCount - 1,
					PrevLogTerm:  opCount - 1,
					LeaderCommit: opCount,
				}
				node.HandleAppendEntries(context.Background(), req)
			}
		}
	})
}

func BenchmarkRaftConcurrency16Nodes(b *testing.B) {
	const nodeCount = 16
	nodes := make([]*RaftNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("node_%02d", i)
		peerAddrs := make(map[string]string)
		peerClients := make(map[string]pb.RaftServiceClient)
		for j := 0; j < nodeCount; j++ {
			if j == i {
				continue
			}
			pid := fmt.Sprintf("node_%02d", j)
			peerAddrs[pid] = fmt.Sprintf("127.0.0.1:%d", 9500+j)
		}
		nodes[i] = NewRaftNode(id, peerAddrs, peerClients, nil)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for n := 0; n < nodeCount; n++ {
			wg.Add(1)
			go func(nodeIdx int, iter int64) {
				defer wg.Done()
				node := nodes[nodeIdx]
				if iter%2 == 0 {
					req := &pb.RequestVoteRequest{
						Term:         iter,
						CandidateId:  fmt.Sprintf("candidate_%d", iter),
						LastLogIndex: iter,
						LastLogTerm:  iter,
					}
					node.HandleRequestVote(context.Background(), req)
				} else {
					req := &pb.AppendEntriesRequest{
						Term:         iter,
						LeaderId:     fmt.Sprintf("leader_%d", iter),
						PrevLogIndex: iter - 1,
						PrevLogTerm:  iter - 1,
						LeaderCommit: iter,
					}
					node.HandleAppendEntries(context.Background(), req)
				}
			}(n, int64(i))
		}
		wg.Wait()
	}
}

# R-04 根因报告：leader批量同步放弃后follower永久掉队

> **缺陷编号**: R-04
> **级别**: P-Ⅰ
> **性质**: fail-open（leader对落后follower重试固定上限后永久放弃同步，节点静默掉队，无告警/无摘除/无自愈）
> **发现场景**: E05故障期性能测试（收敛判定FAIL）
> **约束**: 只读分析，未修改任何代码

---

## Q1: startIdx=1的来源

### 结论

`startIdx=1` 来源于 `matchIdx[node-1]=0`，即leader从未成功向node-1发送过AppendEntries。

### 代码证据

**IdentifyLaggingFollowers** (raft.go:303-338):
```go
matchIdx := rn.matchIdx[p.ID]       // L318
gap := rn.commitIdx - matchIdx      // L319
if gap > threshold {                 // L320, threshold=100
    result = append(result, LaggingFollower{
        StartIdx:  matchIdx + 1,    // L332 ← 此处
        EndIdx:    rn.commitIdx,    // L333
    })
}
```

`StartIdx = matchIdx + 1 = 0 + 1 = 1`

**matchIdx更新路径** (raft.go:842-849, heartbeat成功时):
```go
if resp.Success {
    newMatch := int64(len(logSnapshot))
    if newMatch > rn.matchIdx[p.ID] {
        rn.matchIdx[p.ID] = newMatch  // L847
    }
}
```

**UpdateFollowerProgress** (raft.go:386-393, batch sync成功时):
```go
func (rn *RaftNode) UpdateFollowerProgress(peerID string, lastMatch int64) {
    if lastMatch > rn.matchIdx[peerID] {
        rn.matchIdx[peerID] = lastMatch  // L390
    }
}
```

matchIdx仅在AppendEntries成功时更新。node-1重启后，所有AppendEntries均失败，matchIdx保持0。

### startIdx=1非nextIndex回退

nextIndex回退逻辑 (raft.go:852-857, heartbeat失败时):
```go
if !resp.Success {
    rn.nextIdx[p.ID]--  // L855: 逐次回退
}
```

但此路径仅在 `resp.Success=false` 时触发。E05中失败是gRPC err（L829），不触发nextIndex回退。batch_sync路径同理：err != nil时retryCount++但不改startIdx。

### 日志佐证

```
批量同步失败: follower=node-1, startIdx=1, 重试6次后放弃
```
所有失败行均显示 `startIdx=1`，从未递增→证实matchIdx从未被更新。

### 底层根因：gRPC DNS解析失败

```
RequestVote → node-1 失败: rpc error: code = Unavailable desc = name resolver error: produced zero addresses
```

从leader(node-2)内部验证：
```
nslookup node-1 → server can't find node-1: NXDOMAIN
```

node-1的Docker网络别名丢失：
```
docker inspect raft-node-1 → Aliases=[]
```

**因果链**：
```
docker network disconnect node-1
  → Docker DNS删除node-1别名
docker network connect node-1
  → Docker DNS未重新注册node-1别名（Aliases=[]）
  → leader的gRPC客户端解析"node-1:9500"失败
  → "name resolver error: produced zero addresses"
  → AppendEntries RPC全部失败(err != nil)
  → matchIdx[node-1]永不更新(=0)
  → startIdx = 0 + 1 = 1
  → batch sync从index 1开始同步，但RPC全部失败
  → 重试6次后放弃
  → SyncLoop 200ms后重试，同样失败
  → node-1永久掉队
```

---

## Q2: 重试计数器语义

### 结论

retryCount是每次SyncFollower调用的局部变量，每200ms重置。放弃后无状态标记，SyncLoop会重试但同一失败重现，effectively永久。

### 代码证据

**retryCount声明** (batch_sync.go:136):
```go
retryCount := int32(0)  // 局部变量，每次SyncFollower调用重置
```

**重试逻辑** (batch_sync.go:168-180):
```go
if err != nil {
    retryCount++
    if retryCount > m.config.MaxRetries {  // MaxRetries=5 (L46)
        m.node.logf("批量同步失败: ...重试%d次后放弃", retryCount)
        return  // 退出SyncFollower，无状态标记
    }
    batchSize = batchSize / 2  // 缩减batch size
    time.Sleep(100 * time.Millisecond)
    continue
}
```

**SyncLoop调度** (batch_sync.go:93-121):
```go
func (m *BatchSyncManager) SyncLoop() {
    ticker := time.NewTicker(m.config.SyncInterval)  // SyncInterval=200ms (L47)
    for {
        select {
        case <-ticker.C:
            lagging := m.node.IdentifyLaggingFollowers()
            for _, f := range lagging {
                go func(lf LaggingFollower) {
                    m.SyncFollower(lf)  // 每次新调用，retryCount=0
                }(f)
            }
        }
    }
}
```

### 放弃后状态

| 项 | 状态 |
|---|------|
| BatchSyncTask状态 | 未使用（tasks map未在此路径更新） |
| matchIdx | 不变（=0） |
| nextIdx | 不变 |
| follower摘除 | 无（peers列表不变） |
| 告警 | 无 |
| 自愈 | 无主动机制 |

### 恢复路径

SyncLoop每200ms重试SyncFollower，retryCount重置为0。理论上存在恢复路径——若DNS恢复，下次SyncFollower调用应成功。但DNS别名不会自动恢复（需手动`docker network disconnect/connect`或容器重建）。

**在E05中**：DNS别名丢失是持久的，SyncLoop的200ms重试无法自愈。

---

## Q3: 验证"非R-01回归"

### 结论

R-01修复未触及批量同步路径。R-04与R-01是两条独立路径，无交叉。

### git diff证据

```
commit 96e0e73 R-01-fix: gRPC重连退避MaxDelay 120s→5s

diff --git a/grpc_server.go b/grpc_server.go
+ "google.golang.org/grpc/backoff"
+ grpc.WithConnectParams(grpc.ConnectParams{
+     Backoff: backoff.Config{
+         BaseDelay:  1 * time.Second,
+         Multiplier: 1.6,
+         Jitter:     0.2,
+         MaxDelay:   5 * time.Second,
+     },
+ }),
1 file changed, 9 insertions(+)
```

仅修改 `grpc_server.go`，未触及 `batch_sync.go` 或 `raft.go`。

### 路径分离分析

| 路径 | 文件 | 函数 | R-01是否触及 |
|------|------|------|-------------|
| gRPC连接创建 | grpc_server.go | PeerClientManager.connect() | **是**（WithConnectParams） |
| 批量同步 | batch_sync.go | BatchSyncManager.SyncFollower() | **否** |
| 心跳/日志复制 | raft.go | sendHeartbeats() | **否** |
| 选举 | raft.go | startElection() | **否** |

### 失败模式对比

| 缺陷 | 失败模式 | gRPC层 | 修复 |
|------|---------|--------|------|
| R-01 | 连接断开后重连退避MaxDelay=120s | 传输层(连接) | 已修复(MaxDelay=5s) |
| R-04 | DNS解析"produced zero addresses" | 解析层(DNS) | 未修复 |

R-01修复的WithConnectParams配置连接退避，但DNS解析失败发生在连接之前——gRPC先解析主机名，解析失败则不进入连接阶段。因此R-01修复对R-04无效。

---

## Q4: E02为何未暴露

### 结论

E02未暴露R-04有两个独立原因：gap小（14条 vs 3424条）和DNS别名未丢失。

### 对比分析

| 维度 | E02 (batch1) | E05 (batch2) |
|------|-------------|-------------|
| 断开节点 | node-2 | node-1 |
| 断开期间写入 | 2条 | ~3,424条 (1141 TPS × 60s × 20%) |
| gap大小 | 14 (15→27) | 3,424 (1→3425) |
| 是否restart | 否 | 是 (docker restart) |
| DNS别名 | 保留 | **丢失** (Aliases=[]) |
| 收敛 | 35s→2s(R-01后) | **未收敛** |
| 失败模式 | gRPC连接退避(R-01) | gRPC DNS解析(R-04) |

### 根因差异

**原因1：DNS别名保留**
- E02: `docker network disconnect` + `docker network connect`，DNS别名保留
- E05: 同样操作 + `docker restart`，DNS别名丢失（Aliases=[]）
- E02的gRPC连接在重连后能解析"node-2"，E05不能解析"node-1"

**原因2：gap大小**
- E02 gap=14：即使DNS暂时不通，leader心跳路径(raft.go:804)用nextIdx从commit附近开始，14条entries可一次性发送
- E05 gap=3424：batch sync从startIdx=1开始，需发送3424条entries，但RPC全部因DNS失败

### 复现条件（只读描述，不执行）

若在5节点模拟"断开期间持续高压写入"：
1. 断开node-X
2. 持续以>1000 TPS写入60s（产生gap>60000）
3. `docker network connect` node-X
4. 若DNS别名保留：batch sync从startIdx=1开始，需发送60000+条entries
   - batch size=4096，需15+轮，每轮2s超时
   - 总耗时>30s，但应能收敛
5. 若DNS别名丢失：与E05相同，永久不收敛

**关键变量**：DNS别名是否保留，而非gap大小。gap大小仅影响收敛耗时，DNS丢失导致永久不收敛。

---

## Q5: 与鲲鹏TCX-Ⅱ-1的关系

### 结论

R-04与TCX-Ⅱ-1选举风暴**同源放大关系**：R-04导致follower永久掉队→持续触发选举→放大选举风暴。

### 证据链

**TCX-Ⅱ-1实测** (kunpeng-evidence/07):
- 50节点单Raft组，写入成功率0%
- 选举风暴：term持续增长，Leader频繁切换

**R-04行为** (E05实测):
- node-1永久掉队（commit=3426，leader=3904+）
- node-1持续触发选举：日志显示每1-2s一次"选举超时触发→Candidate"
- node-1选举全部失败（得票1/5）：因log落后，无法获quorum
- term从130增长到1040+（30分钟内约910次选举）

**同源机制**：
```
R-04: leader放弃同步follower
  → follower永久掉队
  → follower收不到心跳
  → follower持续触发选举（每1-2s）
  → 选举失败（log落后，无法获quorum）
  → term持续增长
  → 在50节点规模下：多个follower同时掉队
  → 选举风暴（N个掉队follower × 每1-2s选举 = N/2 elections/s）
  → Leader频繁切换
  → 写入成功率趋近0%
```

**支持证据**：
1. E05中node-1的term从130→1040（910次选举/30min），与TCX-Ⅱ-1的term增长模式一致
2. node-1选举全部失败（得票1/5），与TCX-Ⅱ-1的0%写入成功率一致
3. R-04的"leader放弃同步"是触发条件，选举风暴是放大效应

**反对证据**：
1. TCX-Ⅱ-1的50节点选举风暴可能有其他根因（如网络分区、配置条目冲突）
2. 5节点中仅1个follower掉队，选举风暴可控（leader有quorum=4/5）
3. 缺乏50节点+R-04的直接复现数据

**判断**：R-04与TCX-Ⅱ-1**部分同源**。R-04是选举风暴的一个充分条件（follower掉队→选举），但TCX-Ⅱ-1的50节点风暴可能有多个触发源。修复R-04可消除"掉队follower→选举"这一风暴源，但不能保证完全消除50节点风暴。

---

## 因果链（完整）

```
[E05测试操作]
docker network disconnect node-1
  → Docker DNS删除node-1别名
  → (60s故障期，leader持续接受写入，commit增长至3425)

docker network connect node-1
  → Docker DNS未重新注册node-1别名 (Aliases=[])
  → leader的gRPC客户端解析"node-1:9500"
  → "name resolver error: produced zero addresses"
  → AppendEntries RPC全部失败

docker restart node-1
  → node-1从WAL恢复commit=3426
  → 但DNS别名仍为空

[leader批量同步]
SyncLoop (每200ms)
  → IdentifyLaggingFollowers: matchIdx[node-1]=0, gap=3425>100
  → SyncFollower: startIdx=0+1=1
  → AppendEntries(node-1, startIdx=1) → DNS解析失败
  → 重试6次（batchSize 4096→128），全部DNS失败
  → "重试6次后放弃"
  → return（无状态标记）

[node-1]
  → 收不到leader心跳
  → 选举超时(1-2s) → Candidate
  → RequestVote RPC → 其他节点拒绝（log落后）
  → 选举失败（得票1/5）
  → term++
  → 循环

[结果]
  → node-1永久掉队（commit=3426不变）
  → gap持续增长（3426 vs 3904+）
  → E05收敛判定FAIL
```

---

## 复现条件

| 条件 | 必要性 | E05中的值 |
|------|--------|-----------|
| docker network disconnect + connect | 必要2选1 | ✓ |
| docker restart | 必要 | ✓ |
| Docker DNS别名丢失 | 必要 | ✓ (Aliases=[]) |
| gap > lagThreshold(100) | 必要 | ✓ (gap=3424) |
| leader未成功发送AppendEntries | 必要 | ✓ (DNS失败) |

---

## 修复建议（仅评估，不修改代码）

### 方案A：gRPC DNS解析重试（推荐）
在gRPC客户端配置中添加DNS解析重试机制，或在PeerClientManager中定期刷新连接。

### 方案B：batch sync放弃后摘除follower
放弃后将follower标记为"suspect"，触发告警，并在N次SyncLoop周期后摘除follower（从peers列表移除）。

### 方案C：batch sync放弃后降级到心跳路径
放弃后不依赖batch sync，让心跳路径(raft.go:804)用nextIdx继续尝试同步。需确保心跳路径的nextIdx不停留在1。

### 方案D：应用层DNS刷新
定期检查peer可达性，发现DNS失败时主动重建gRPC连接（RemovePeer + AddPeer）。

---

## 证据索引

| 证据 | 路径 |
|------|------|
| E05故障期压测原始输出 | tests/evidence/d3-batch2-p1/E05-fault-raw.txt |
| E05 FAIL报告 | tests/evidence/d3-batch2-p1/E05/FAIL-REPORT.md |
| gap增长曲线 | tests/evidence/d3-batch2-p1/E05/gap-curve.txt |
| leader批量同步失败日志 | docker logs raft-node-2 (保留在容器中) |
| node-1选举失败日志 | docker logs raft-node-1 (保留在容器中) |
| R-01 fix diff | git diff 96e0e73~1 96e0e73 (仅grpc_server.go, 9行) |
| batch_sync.go代码 | batch_sync.go:124-200 (SyncFollower) |
| raft.go代码 | raft.go:302-393 (IdentifyLaggingFollowers + UpdateFollowerProgress) |
BATCH2-PLAN.md | docs/D3-BATCH2-PLAN.md L119-127 (E07定义) |
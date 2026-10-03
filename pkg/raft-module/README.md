# RaftKV Module01 — Raft 强一致性共识引擎（独立闭环模块）

> 纯标准库零外部依赖 | 3 节点集群 | Leader 选举 + 日志复制 + WAL 持久化 + AES-GCM 加密

---

## 1. 模块概述

本模块从 `raftkv_go_engine` 主工程中剥离 Raft 强一致性共识引擎，形成**独立闭环、零外部依赖**的纯 Go 标准库模块。

### 核心能力

| 能力 | 说明 |
|------|------|
| Leader 选举 | 随机选举超时（150-300ms），Follower → Candidate → Leader 状态机 |
| 日志复制 | Leader 通过 AppendEntries 向 Follower 复制日志，多数派确认后提交 |
| WAL 持久化 | 1GB 预分配 + 批量 fsync group commit，崩溃恢复 |
| AES-GCM 加密 | 透明叠加在 WAL 之上，每条记录独立 Nonce，机密性 + 完整性认证 |
| 批量提案 | sync.Pool 零拷贝序列化 + 200μs 攒批窗口 |

### 改造要点（零外部依赖）

| 原依赖 | 改造为 | 说明 |
|--------|--------|------|
| `google.golang.org/grpc` + `raftkv/proto` | `net/http` + `encoding/json` | 纯标准库 HTTP RPC 传输层 |
| `github.com/tjfoc/gmsm/sm4` (SM4-CTR) | `crypto/aes` + `crypto/cipher` (AES-128-GCM) | 标准库认证加密 |
| `github.com/go-sql-driver/mysql` | 移除 | TiDB 落盘属下游模块，不在共识引擎范围 |

---

## 2. 文件清单

```
pkg/raft-module/
├── go.mod                        # 模块定义（零 require，纯标准库）
├── types.go                      # 共享类型（NodeState, RaftLog, Transport 接口, RPC 结构体）
├── raft.go                       # Raft 核心状态机（选举 + 日志复制 + Propose 提案）
├── raft_transport.go             # 纯标准库 HTTP RPC 传输层（替代 gRPC）
├── raft_wal.go                   # WAL 预写式日志（1GB 预分配 + 批量 fsync）
├── raft_pool.go                  # 零拷贝内存池 + 批量提案器
├── raft_storage.go               # AES-128-GCM 加密存储层（替代 SM4）
├── raft-test-linux-arm64         # 交叉编译产物（GOOS=linux GOARCH=arm64）
├── cmd/
│   └── raft-test/
│       └── main.go               # 独立沙箱验证测试程序（3 节点 + 10 条日志）
└── README.md                     # 本说明文档
```

---

## 3. 架构设计

### 3.1 分层架构

```
┌─────────────────────────────────────────────┐
│           cmd/raft-test/main.go             │  沙箱测试驱动
├─────────────────────────────────────────────┤
│              raft.go (核心)                  │  状态机 + 选举 + 复制 + Propose
│  ┌─────────────────────────────────────┐    │
│  │  Transport 接口 (解耦通信)           │    │
│  └─────────────────────────────────────┘    │
├─────────────────────────────────────────────┤
│         raft_transport.go (HTTP RPC)        │  net/http + JSON
├─────────────────────────────────────────────┤
│  raft_wal.go  │ raft_storage.go │ raft_pool │  持久化 + 加密 + 批量
└─────────────────────────────────────────────┘
```

### 3.2 Raft 状态机

- **Follower** → 选举超时 → **Candidate** → 获多数派投票 → **Leader**
- **Leader** 每 50ms 发送 AppendEntries（心跳 + 日志复制）
- 收到更高 term → stepDown → **Follower**
- 随机选举超时 150-300ms 避免脑裂

### 3.3 日志复制流程

```
Client → Leader.ProposeSync(cmd)
  → Leader 追加日志 (index, term, cmd)
  → replicateAll: 向所有 peer 发送 AppendEntries(entries)
  → peer 校验 prevLogIndex/prevLogTerm，追加日志，返回 Success
  → Leader 更新 nextIdx/matchIdx
  → advanceCommit: 找多数派 matchIdx ≥ N 且 logs[N].term == 当前 term
  → commitIdx 推进，触发 onCommit 回调
```

---

## 4. 纯标准库零依赖声明

`go.mod` 内容：

```
module raftkv/raft-module

go 1.21
```

**零 `require`**，仅使用 Go 标准库：

| 标准库包 | 用途 |
|----------|------|
| `net/http` | RPC 传输层 |
| `encoding/json` | 序列化 |
| `crypto/aes` + `crypto/cipher` | AES-128-GCM 加密 |
| `crypto/rand` | Nonce 生成 |
| `os` + `encoding/binary` | WAL 文件 IO |
| `sync` + `sync/atomic` | 并发控制 |
| `time` + `math/rand` | 选举超时 |

---

## 5. 运行方式

### 5.1 本地沙箱跑测

```bash
cd pkg/raft-module
go run cmd/raft-test/main.go
```

### 5.2 交叉编译（linux/arm64）

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o raft-test-linux-arm64 ./cmd/raft-test/
```

### 5.3 静态检查

```bash
go vet ./...
go build ./...
```

---

## 6. 沙箱验证结果

### 结论：✅ PASS

| 验证项 | 结果 |
|--------|------|
| 3 节点集群启动 | ✅ 通过 |
| Leader 选举（有且仅有 1 个 Leader） | ✅ 通过（node2 当选，得票 3/3） |
| 10 条日志通过 Leader 提交 | ✅ 通过（index 1-10） |
| 所有节点 commitIdx ≥ 10 | ✅ 通过（3 节点均 commitIdx=10） |
| 日志内容跨节点一致性 | ✅ 通过（3 节点日志完全一致） |
| 日志条目内容正确性 | ✅ 通过（10 条内容匹配） |
| onCommit 回调触发 | ✅ 通过（每节点 10 次） |

### 关键指标

- Leader 选举耗时：**1.6ms**
- 10 条日志提交：**全部成功**
- 脑裂检测：**无脑裂**（任意时刻仅 1 个 Leader）

---

## 7. MD5 校验清单

| 文件 | MD5 |
|------|-----|
| go.mod | `4DC0688CB0C8B90238FD7177F07B79DA` |
| types.go | `3ADA4F093F7621FFC33EABC3E2389420` |
| raft.go | `B380B1A76B4DDC1EBD391F1C9312F04D` |
| raft_transport.go | `772D01D76C5002988814ECD759B5F822` |
| raft_wal.go | `1FDE37838CECDF81E152522399AE429C` |
| raft_pool.go | `A689A591D2D304370836C9B6CB38262C` |
| raft_storage.go | `4C3A6DEA7BE14E2109BE6B126F164084` |
| cmd/raft-test/main.go | `B3639650586B72B04F15907FB5ADEA8F` |
| raft-test-linux-arm64 | `E4CEFBD927CE14ECDC5B573DC159A1C5` |

---

## 8. 交叉编译产物

| 属性 | 值 |
|------|-----|
| 文件名 | raft-test-linux-arm64 |
| 目标平台 | linux/arm64 |
| 编译选项 | CGO_ENABLED=0 |
| 文件大小 | 8,700,421 字节（约 8.3 MB） |
| MD5 | E4CEFBD927CE14ECDC5B573DC159A1C5 |

---

## 9. 最终结论

| 项目 | 结论 |
|------|------|
| 模块独立性 | ✅ PASS — 零外部依赖，纯标准库 |
| Leader 选举 | ✅ PASS — 成功选出唯一 Leader |
| 日志同步 | ✅ PASS — 10 条日志全节点一致同步 |
| 沙箱跑测 | ✅ PASS — 全部验证通过 |
| 交叉编译 | ✅ PASS — linux/arm64 产物生成 |
| **总体结论** | **✅ PASS — Raft 共识引擎独立闭环模块交付合格** |
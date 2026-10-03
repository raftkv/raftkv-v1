# RaftKV Module03 — 高吞吐数据流转 Pipeline（独立闭环模块）

> 纯标准库零外部依赖 | 多阶段串联 | 背压流控 | 批处理聚合 | 多生产者多消费者并发安全 | TPS 吞吐量统计

---

## 1. 模块概述

本模块从 `raftkv_go_engine` 主工程中剥离 **高吞吐数据流转 Pipeline 引擎**，形成**独立闭环、零外部依赖**的纯 Go 标准库模块。

### 核心能力

| 能力 | 说明 |
|------|------|
| 多阶段串联 | Ingest(接入) → Process(处理) → [Batcher(聚批)] → Output(输出) 至少 3 Stage 串联 |
| 高吞吐压测 | 100,000 条数据端到端流转，零丢失，TPS 可达 90 万+/秒 |
| 背压流控 | 带缓冲 channel + RingBuffer，下游慢时阻塞上游，不丢数据、不崩溃 |
| 批处理聚合 | Batcher 按批次大小（size）或时间窗口（flush interval）聚合输出 |
| 并发安全 | 多生产者并发 Submit + 多消费者并发读取，原子计数 + mutex + channel 保护 |
| TPS 统计 | 内置 TPSMeter，实时输出瞬时 TPS 与累计 TPS |
| 优雅关闭 | 自然级联关闭（close ingestCh → Stage 排空 → close 下游），零丢失 |

### 与 Module01（Raft 共识）和 Module02（WAL+SM4 持久化）的协调关系

| 方面 | 衔接方式 |
|------|----------|
| Pipeline → Module02（WAL+SM4） | Pipeline 的 Output Stage 可对接 `SM4Storage.Append`，将流转数据持久化落盘 |
| Pipeline → Module01（Raft） | Pipeline 的协调/提交可依赖 `Raft.ProposeSync`，经共识后再注入 Pipeline |
| 命名规范 | 复用前序模块的 snake_case 文件名、零外部依赖、纯标准库风格 |
| 模块独立 | 本模块不直接 import 前序模块，仅通过回调/接口衔接，保持独立闭环 |

### 改造要点（零外部依赖 + 自研）

| 原实现 | 改造为 | 说明 |
|--------|--------|------|
| `raftkv/pkg/adapters` (TiDB/MySQL) | 移除 | 落盘属下游模块，Pipeline 仅流转到 Output |
| `raft_pipeline.go` 与 raft 耦合 | 独立 `pipeline.go` | 去除 Raft/SM4 耦合，纯数据流转 |
| 第三方库 | **禁止** | 零外部依赖，纯 Go 标准库 |

---

## 2. 文件清单

```
pkg/pipeline-module/
├── go.mod                        # 模块定义（零 require，纯标准库）
├── pipeline.go                   # Pipeline 核心：多阶段串联编排引擎
├── pipeline_stage.go             # Stage 抽象：Ingest/Process/Output 处理单元
├── pipeline_worker.go            # WorkerPool 工作池：多消费者并行 + OrderedWorkerPool 保序
├── ringbuffer.go                 # RingBuffer 环形缓冲区：背压流控
├── batcher.go                    # Batcher 批处理聚合器：按 size/时间窗口聚合
├── metrics.go                    # TPSMeter 吞吐量统计 + E2EStats 端到端校验
├── concurrent_test.go            # 并发安全单元测试（go test -count=200）
├── pipeline-test-linux-arm64     # 交叉编译产物（GOOS=linux GOARCH=arm64）
├── cmd/
│   └── pipeline-test/
│       └── main.go               # 独立沙箱验证测试程序（5 项验证）
└── README.md                     # 本说明文档
```

---

## 3. 架构设计

### 3.1 分层架构

```
┌─────────────────────────────────────────────┐
│         cmd/pipeline-test/main.go            │  沙箱测试驱动（5 项验证）
├─────────────────────────────────────────────┤
│            pipeline.go (编排核心)             │  多阶段串联 + 生命周期 + 统计
│  ┌─────────────────────────────────────┐    │
│  │  pipeline_stage.go (Stage 抽象)     │    │  Ingest → Process → Output
│  │  pipeline_worker.go (WorkerPool)    │    │  多消费者并行 / OrderedWorkerPool 保序
│  │  batcher.go (批处理聚合)            │    │  按 size/时间窗口聚合
│  │  ringbuffer.go (环形缓冲)           │    │  背压流控
│  │  metrics.go (TPS + E2EStats)        │    │  吞吐量统计 + 端到端校验
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
```

### 3.2 数据流转流程

```
外部 Submit(item)
  → ingestCh (带缓冲, 背压)
  → Ingest Stage (proc: identity 或打时间戳)
  → procCh (带缓冲, 背压)
  → Process Stage (proc: 业务转换, 多 worker 并行)
  → [EnableBatch?]
      ├─ true  → batcher.in → Batcher 聚批 → batcher.Out → unbatchToOutput → outputCh
      └─ false → outputCh (直连)
  → Output Stage (proc: identity 或格式化)
  → finalOut (供外部消费)
```

### 3.3 优雅关闭（自然级联，零丢失）

```
Pipeline.Close()
  → close(ingestCh)
  → Ingest Stage 排空 → close(procCh)
  → Process Stage 排空 → close(batcher.in) 或 close(outputCh)
  → [EnableBatch?]
      ├─ true → Batcher flush 最后一帧 → close(batcher.Out)
      │       → unbatchToOutput 退出 → close(outputCh)
      └─ false → (outputCh 已由 Process Stage 关闭)
  → Output Stage 排空 → close(finalOut)
  → 消费者 range 自然退出
```

**关键设计**：Close 不调用 `cancel(ctx)`，避免 worker 提前退出丢数据；ctx 仅用于 `ForceClose`（异常强制终止）。

### 3.4 背压流控机制

| 机制 | 实现 | 场景 |
|------|------|------|
| 带缓冲 channel | `make(chan Item, buf)` | Stage 之间默认背压，满则阻塞生产者 |
| RingBuffer | `sync.Mutex + sync.Cond` | 需要显式容量管理、可观测的场景 |
| Batcher | 攒批 + 定时 flush | 减少下游写入次数，提高吞吐 |
| WorkerPool | N worker 并行消费 | CPU 密集型处理，多核并行 |

---

## 4. 纯标准库零依赖声明

`go.mod` 内容：

```
module raftkv/pipeline-module

go 1.21
```

**零 `require`**，仅使用 Go 标准库：

| 标准库包 | 用途 |
|----------|------|
| `sync` | Mutex / WaitGroup / Cond |
| `sync/atomic` | 原子计数器（TPS / totalIn / totalOut） |
| `time` | TPS 采样窗口 / Batcher flush 定时 |
| `context` | 生命周期 / ForceClose |
| `errors` | 错误构造 |
| `fmt` | 错误格式化 |

---

## 5. 运行方式

### 5.1 本地沙箱跑测

```bash
cd pkg/pipeline-module
go run cmd/pipeline-test/main.go
```

### 5.2 并发安全压测（高频重复）

```bash
go test -count=200 ./...
```

### 5.3 race 检测（需 cgo + gcc，Linux 环境）

```bash
CGO_ENABLED=1 go run -race cmd/pipeline-test/main.go
# 或
CGO_ENABLED=1 go test -race -count=100 ./...
```

> **Windows 沙箱限制**：本机无 gcc，无法直接执行 `-race`。已通过 `go test -count=200`（200 次高频并发压测）+ 8 生产者/4 消费者/4 worker 并发压力测试（100,000 条零丢失、零重复）验证并发安全。代码层面所有共享状态均用 `atomic`/`mutex`/`channel` 保护。建议在 Linux 环境执行 `go run -race` 做最终确认。

### 5.4 交叉编译（linux/arm64）

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o pipeline-test-linux-arm64 ./cmd/pipeline-test/
```

### 5.5 静态检查

```bash
go vet ./...
go build ./...
```

---

## 6. 沙箱验证结果

### 结论：✅ PASS

| 验证项 | 结果 |
|--------|------|
| 多阶段 Pipeline 串联启动（Ingest → Process → Output） | ✅ 通过 |
| 100,000 条数据端到端流转完成，零丢失（Lost=0） | ✅ 通过 |
| 顺序性符合 Pipeline 语义（单 worker 保序，OutOfOrder=0 Duplicated=0） | ✅ 通过 |
| 处理正确性（Data=int*2 转换，100,000 条全部正确） | ✅ 通过 |
| 背压流控正常（小缓冲 64 + 慢处理 100μs/条，5,000 条零丢失） | ✅ 通过 |
| 批处理聚合正确（batch=256，50,000 条全部到达） | ✅ 通过 |
| 并发安全（8 生产者 + 4 消费者 + 4 worker，100,000 条零丢失、零重复） | ✅ 通过 |
| RingBuffer 背压（4 生产者，20,000 条零丢失，Dropped=0） | ✅ 通过 |
| 并发压测 go test -count=200 | ✅ 通过（200/200） |
| go vet ./... | ✅ 通过 |
| 交叉编译 linux/arm64 | ✅ 通过 |

### 关键指标

| 指标 | 数值 |
|------|------|
| 测试 1 端到端 TPS（100,000 条，单 worker 保序） | **907,807 条/秒** |
| 测试 2 背压下 TPS（5,000 条，慢处理 100μs/条） | **1,751 条/秒** |
| 测试 3 批处理 TPS（50,000 条，batch=256） | **803,366 条/秒** |
| 测试 4 并发 TPS（100,000 条，8 生产者 + 4 消费者） | **618,917 条/秒** |
| 测试 5 RingBuffer TPS（20,000 条，4 生产者） | **4,603,416 条/秒** |
| 并发压测 200 次重复 | **全部通过（10.6s）** |

---

## 7. MD5 校验清单

| 文件 | 大小（字节） | MD5 |
|------|-------------|-----|
| go.mod | 43 | `356F1F0CBC061FDB846F57C140C7793E` |
| pipeline.go | 9,554 | `4198562D1468D5BCC9211CC26E830D0B` |
| pipeline_stage.go | 5,132 | `F314C67AB9C40366CC5CBB0C994A7701` |
| pipeline_worker.go | 5,253 | `B847F868CCFB2E59F1F2D21393E27366` |
| ringbuffer.go | 4,630 | `4B857567330E85CE07CACE33F23D6DFD` |
| batcher.go | 5,207 | `F7DB26975E4DF8E86C2D770B7129C22B` |
| metrics.go | 4,679 | `EFC530B5B43756D61BD20EDFB239A7E0` |
| concurrent_test.go | 3,977 | `41526019F6496E1D13E97CB48EC9B446` |
| cmd/pipeline-test/main.go | 16,707 | `DE33E987D0C912139646FC71B9C46D26` |
| pipeline-test-linux-arm64 | 2,511,598 | `4D61CF0A860BCE5CBBAE378A37CE4EF1` |

---

## 8. 交叉编译产物

| 属性 | 值 |
|------|-----|
| 文件名 | pipeline-test-linux-arm64 |
| 目标平台 | linux/arm64 |
| 编译选项 | CGO_ENABLED=0 |
| 文件大小 | 2,511,598 字节（约 2.4 MB） |
| MD5 | 4D61CF0A860BCE5CBBAE378A37CE4EF1 |

---

## 9. 最终结论

| 项目 | 结论 |
|------|------|
| 模块独立性 | ✅ PASS — 零外部依赖，纯标准库 |
| 多阶段串联 | ✅ PASS — Ingest → Process → Output 3 Stage 串联正确 |
| 高吞吐压测 | ✅ PASS — 100,000 条端到端零丢失，TPS 90 万+/秒 |
| 背压流控 | ✅ PASS — 小缓冲 + 慢处理，零丢失、零 panic |
| 批处理聚合 | ✅ PASS — batch size 可配置，50,000 条全部到达 |
| 并发安全 | ✅ PASS — 8 生产者 + 4 消费者，零丢失、零重复；200 次压测通过 |
| 顺序性 | ✅ PASS — 单 worker 保序，OutOfOrder=0 Duplicated=0 |
| TPS 吞吐量统计 | ✅ PASS — 端到端/瞬时/并发 TPS 均正确输出 |
| 优雅关闭 | ✅ PASS — 自然级联关闭，零丢失 |
| 沙箱跑测 | ✅ PASS — 全部 5 项验证通过 |
| 交叉编译 | ✅ PASS — linux/arm64 产物生成 |
| **总体结论** | **✅ PASS — 高吞吐数据流转 Pipeline 独立闭环模块交付合格** |
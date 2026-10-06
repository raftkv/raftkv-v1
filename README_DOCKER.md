# RaftKV 确定性管控中枢 — Docker 部署与验证手册

---

## 前置条件

- Docker Desktop 已安装并运行
- 终端 (PowerShell / CMD) 已打开

---

## 第一步：进入项目目录

```powershell
cd "<HOME>\Desktop\RaftKV\raftkv_go_engine"
```

确认文件齐全：

```powershell
# 应显示 9+ 个 .go 文件、Dockerfile、docker-compose.yml、go.mod、go.sum
dir *.go Dockerfile docker-compose.yml
```

---

## 第二步：构建并启动集群

```powershell
docker-compose up -d --build
```

首次构建约 2-3 分钟（下载 Go 依赖 + 编译）。后续启动仅需 5 秒。

预期输出：
```
[+] Building 120.0s (15/15) FINISHED
[+] Running 5/5
 ✔ Container raft-node-1     Started
 ✔ Container raftkv-frontend   Started
 ✔ Container raft-node-2     Started
 ✔ Container raft-node-3     Started
```

---

## 第三步：验证三节点集群

### 3.1 健康检查

```powershell
curl http://localhost:9001/health/live
curl http://localhost:9002/health/live
curl http://localhost:9003/health/live
```

预期：三个节点均返回 `OK`

### 3.2 Raft 集群状态

```powershell
curl http://localhost:9001/raft/stats
```

预期输出：

```
id=node-1 state=Leader term=1 leader=node-1 commit=X applied=X logs=X peers=2 voted=node-1
```

关键字段：
- `state=Leader` → node-1 已当选 Leader
- `peers=2` → 两个 Follower 已连接
- `term≥1` → 已完成至少一轮选举

### 3.3 查看 Follower 状态

```powershell
curl http://localhost:9002/raft/stats
curl http://localhost:9003/raft/stats
```

预期：`state=Follower`，`leader=node-1`

---

## 第四步：验证前端大屏

浏览器打开：

```
http://localhost:8096
```

你应该看到RaftKV 确定性管控中枢大屏，包含：
- 左上：系统总览 KPI 指标
- 左中：确定性引擎参数
- 中上：7 步 Agent Pipeline
- 右上：实时事件审计
- 右下：系统性能与安全

DeepSeek 统一管控大屏：

```
http://localhost:8096/deepseek.html
```

---

## 第五步：验证降级联动（关键）

### 5.1 模拟节点故障

停止 node-2 和 node-3：

```powershell
docker stop raft-node-2 raft-node-3
```

### 5.2 验证仲裁丢失

```powershell
curl http://localhost:9001/raft/stats
```

预期：`peers=0`（两 Follower 离线）

### 5.3 前端降级横幅

刷新前端大屏 `http://localhost:8096`，应看到：

```
⚠ 系统异常 — 降级只读模式
存活节点: 1/3 (法定票数需 ≥ 2) · 写操作已全部阻断 · 等待节点恢复
```

红色闪烁横幅，所有写操作按钮灰化不可点击。

### 5.4 恢复验证

```powershell
docker start raft-node-2 raft-node-3
```

等待约 10 秒，刷新前端大屏——降级横幅自动消失，按钮恢复可用。

---

## 第六步：测试 API 写操作

### 6.1 正常模式（3 节点全部存活）

```powershell
# 注册一个模拟 GPU 节点（curl POST 示例）
curl -X POST http://localhost:9001/raft/status

# 查询资源池
curl http://localhost:9001/raft/status
```

### 6.2 降级模式（2 节点离线）

```powershell
docker stop raft-node-2 raft-node-3
curl http://localhost:9001/raft/stats
# 预期: peers=0，写操作被拒绝
```

---

## 常用命令速查

```powershell
# 查看所有容器状态
docker-compose ps

# 查看节点 1 日志
docker logs -f raft-node-1

# 查看所有节点日志
docker-compose logs -f

# 停止集群
docker-compose down

# 停止并删除数据卷
docker-compose down -v

# 重新构建（代码修改后）
docker-compose up -d --build

# 扩容到 5 节点（修改 docker-compose.yml 后）
docker-compose up -d --scale node-1=1 --scale node-2=1 --scale node-3=1
```

---

## 端口映射速查

| 服务 | 容器内端口 | 宿主机端口 | 用途 |
|------|-----------|-----------|------|
| node-1 gRPC | 9500 | 9500 | Raft 共识通信 |
| node-1 HTTP | 9000 | 9001 | API / 健康检查 |
| node-2 gRPC | 9501 | 9501 | Raft 共识通信 |
| node-2 HTTP | 9000 | 9002 | API / 健康检查 |
| node-3 gRPC | 9502 | 9502 | Raft 共识通信 |
| node-3 HTTP | 9000 | 9003 | API / 健康检查 |
| frontend | 80 | 8096 | 前端大屏 |

---

## 故障排查

**容器启动失败：**
```powershell
docker-compose logs node-1    # 查看具体错误
docker-compose down            # 清理
docker-compose up -d --build   # 重建
```

**端口被占用：**
```powershell
netstat -ano | findstr :9500   # 查找占用进程
taskkill /PID <PID> /F         # 终止占用
```

**前端大屏不显示：**
```powershell
docker logs raftkv-frontend  # 检查 nginx 日志
```

---

## 3 节点 → 5 节点扩容

修改 `docker-compose.yml`，新增 `node-4` 和 `node-5`：

```yaml
  node-4:
    build: .
    image: raftkit-gateway:v1
    container_name: raft-node-4
    restart: always
    environment:
      - NODE_ID=node-4
      - GRPC_PORT=9503
      - HTTP_PORT=9000
      - PEERS=node-1=raft-node-1:9500,node-2=raft-node-2:9501,node-3=raft-node-3:9502,node-5=raft-node-5:9504
    ports:
      - "9503:9503"
      - "9004:9000"
    networks:
      - raftkv-net

  node-5:
    build: .
    image: raftkit-gateway:v1
    container_name: raft-node-5
    restart: always
    environment:
      - NODE_ID=node-5
      - GRPC_PORT=9504
      - HTTP_PORT=9000
      - PEERS=node-1=raft-node-1:9500,node-2=raft-node-2:9501,node-3=raft-node-3:9502,node-4=raft-node-4:9503
    ports:
      - "9504:9504"
      - "9005:9000"
    networks:
      - raftkv-net
```

同时更新 node-1/node-2/node-3 的 PEERS 环境变量，加入 node-4 和 node-5。

5 节点法定票数 = 3，可容忍 2 节点故障。

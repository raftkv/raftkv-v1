# D3-batch1 Part2 测试计划 (E01-E03)

> **声明**: E01-E03 无原始书面定义，属口头计划。本文档基于 D3 生产就绪目标边界重建，标注 **[重建]**。
> 重建依据: F01-F16 已覆盖基础功能/幂等/fail-closed/健康/WAL持久化/授权，
> Part2 补充分布式系统生产就绪必需的边界场景。

## 1. 目标

验证RaftKV集群在生产边界条件下的容错与确定性，覆盖 F01-F16 未触及的三个关键场景：
节点崩溃恢复、网络分区自愈、并发写入确定性。

## 2. 前置条件

- 5节点集群正常运行（docker-compose-5node.yml）
- Leader 已当选，F01-F06 基线 PASS
- SM4_KEY 合法，IDEM_TOKEN_CAPACITY=5

## 3. 用例定义

### E01 [重建] 节点崩溃恢复

| 项 | 内容 |
|---|---|
| **目的** | 验证非Leader节点崩溃后重启，能自动重新加入集群并同步至最新commit |
| **步骤** | 1. 5节点集群正常运行，记录leader和commit值C1<br>2. 选一个follower节点，`docker kill` 强制杀死<br>3. 等待5秒（Raft心跳超时）<br>4. 通过leader写入3条新数据，commit升至C2<br>5. 重启被杀节点 `docker start`<br>6. 等待10秒，查询重启节点commit值C3 |
| **预期** | C3 == C2（重启节点自动追平leader日志）<br>集群始终维持5节点（重启后peers=4） |
| **证据格式** | `E01/result.txt`: 崩溃前commit、崩溃节点、写入后commit、重启后commit、结论<br>`E01/node-log.txt`: 重启节点Raft日志最后20行 |

### E01b [追加-已批复] Leader崩溃接管

| 项 | 内容 |
|---|---|
| **目的** | 验证Leader节点崩溃后，集群自动选举新Leader继续服务，原Leader重启后以Follower身份重新加入并同步至最新commit |
| **步骤** | 1. 5节点集群正常运行，记录Leader=L1和commit=C1<br>2. `docker kill` 强制杀死L1<br>3. 等待5秒（Raft选举超时）<br>4. 查询剩余4节点，确认新Leader=L2已当选<br>5. 通过L2写入3条新数据，commit升至C2<br>6. 重启L1 `docker start`<br>7. 等待10秒，查询L1的state（应为Follower）和commit值C3 |
| **预期** | L1崩溃后剩余4节点自动选举新Leader L2（4>=quorum=3）<br>L1重启后state=Follower（不重新抢占Leader）<br>C3 == C2（重启的旧Leader追平新Leader日志） |
| **证据格式** | `E01b/result.txt`: 原Leader、新Leader、崩溃后commit、重启后state和commit、结论<br>`E01b/node-log.txt`: 重启节点Raft日志最后20行 |

### E02 [重建] 网络分区自愈

| 项 | 内容 |
|---|---|
| **目的** | 验证网络分区恢复后集群自动重新同步，无脑裂、无数据丢失 |
| **步骤** | 1. 5节点集群正常运行<br>2. 通过`docker network disconnect`将一个follower从集群网络断开<br>3. 等待10秒（模拟网络分区）<br>4. 通过leader写入2条数据<br>5. `docker network connect`恢复断开节点网络<br>6. [修订-已批复-原值15秒]轮询等待，上限60秒：每5秒查一次断开节点commit，追平leader即收敛 |
| **预期** | 分区期间集群仍可用（剩余4节点>=quorum=3）<br>恢复后所有5节点commit一致（轮询收敛，上限60秒）<br>无脑裂（始终仅1个Leader） |
| **证据格式** | `E02/result.txt`: 分区前commit、分区期间写入结果、恢复后各节点commit、结论<br>`E02/timeline.log`: 操作时间线 |

### E03 [重建] 并发写入确定性

| 项 | 内容 |
|---|---|
| **目的** | 验证高并发写入下，所有节点最终日志序列完全一致（确定性引擎核心要求） |
| **步骤** | 1. 5节点集群正常运行<br>2. 通过leader并发提交20条数据（10个带idem_token去重，10个不带）<br>3. 等待3秒（确保同步完成）<br>4. 逐一查询5节点`/raft/stats`的commit和logs值<br>5. 逐一查询各节点index=1..N的日志内容 |
| **预期** | 所有5节点 commit值相同<br>所有5节点 logs值相同<br>带idem_token的10条仅产生1条日志（去重）<br>不带idem_token的10条产生10条日志<br>总增量 = 1 + 10 = 11 |
| **证据格式** | `E03/result.txt`: 各节点commit/logs、增量计算、确定性结论<br>`E03/per-node-stats.txt`: 5节点stats原始输出 |

## 4. 执行顺序

E01 → E01b → E02 → E03（每个用例前确保集群健康状态）

## 5. 通过标准

- E01: 重启节点commit追平leader
- E01b: 旧Leader重启后state=Follower且commit追平新Leader
- E02: 分区恢复后5节点commit一致
- E03: 5节点commit/logs完全一致，增量=11

## 6. 失败处理

任一用例FAIL → 停机写失败报告 → 等用户批复，禁止跳过继续

## 7. 批准流程

本计划提交tag `d3-part2-plan-pass` 等用户批复后再执行 Part2 测试。
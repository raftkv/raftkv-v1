# KNOWN_DEBTS — 已知债登记

> 生成时间: 2026-10-08
> 生成单: 流水线闭卷·归档执行 (LEDGER #76)
> 仓库: github.com/raftkv/raftkv-v1 @ 0cb7443
> 用途: 下一版本pending任务书素材底稿

## P2 — 中优先级已知债 (3项)

### P2-1: P99延迟超目标
- **描述**: 压测P99=89.9ms > 50ms目标值
- **来源**: v18压测重跑 (LEDGER #72), c=128 write-ratio=1 10min
- **归因**: v18隔离集群与deploy5同机运行, 资源竞争部分归因, 非代码回归
- **证据**: audit_workspace/v18_rework/ (压测重跑输出)
- **处置建议**: 独立资源环境复测, 确认是否为环境竞争

### P2-2: TestRealGRPCConnectivity需live gRPC集群
- **描述**: go test ./... exit=1, TestRealGRPCConnectivity需本地live gRPC集群(端口9500/9604/9605), 与deploy5端口(9001-9005)不匹配
- **来源**: v21终局深测 (LEDGER #74/#75), go test 104/105 PASS 1 FAIL
- **现状**: 加-skip后exit=0, README已声明须-skip
- **证据**: audit_workspace/stranger_v21/p1_gotest_detail.txt
- **处置建议**: 补充端口适配或标注为集成测试需独立环境

### P2-3: batch内容引用在测试函数名(29文件)
- **描述**: ~125处batch内容引用分布在29个文件中(函数名TestBatch15_*等), 文件名已清(v20 PART-4)但内容保留
- **来源**: v20清偿 (LEDGER #73), scan_internal.sh已知债豁免*_test.go
- **证据**: audit_workspace/v20_rework/final_scan_snapshot.txt (假阳性排除列表)
- **处置建议**: 下一版本批量重命名测试函数, 或保留为测试编号约定

## P3 — 低优先级已知债 (6项)

### P3-1: Snapshot() io.ReadAll O(N)技术债
- **描述**: raft_storage.go Snapshot()使用io.ReadAll, 内存O(N)随快照大小线性增长
- **来源**: 代码审查发现
- **证据**: raft_storage.go Snapshot()方法
- **处置建议**: 改用io.Copy流式写入, 或限制快照大小

### P3-2: gateway二进制无pprof端点
- **描述**: gateway HTTP服务未注册net/http/pprof, 无法运行时性能分析
- **来源**: v16深测 (LEDGER #70), P2项
- **证据**: audit_workspace/deep_v16/ (问题清单)
- **处置建议**: 加pprof路由(可配置开关, 默认关闭)

### P3-3: soak测试未达2h目标
- **描述**: v16 soak仅12min(目标2h), 受会话时间限制
- **来源**: v16深测 (LEDGER #70), P2项
- **证据**: audit_workspace/deep_v16/soak_test.txt
- **处置建议**: 独立环境跑2h+ soak, 确认无内存/FD泄漏

### P3-4: WAL截断未触发损坏路径测试
- **描述**: v16测试WAL截断50字节(尾部未使用区), 未触发损坏路径(回放完整), 但未测试截断已使用区的损坏场景
- **来源**: v16深测 (LEDGER #70), P3项
- **证据**: audit_workspace/deep_v16/ (WAL截断测试)
- **处置建议**: 补充WAL头部/中间截断测试, 验证损坏检测与恢复

### P3-5: D3口径差异(豁免为测试基础设施)
- **描述**: D3-前缀grep -i=31行 vs 大小写敏感=3行, 差异=大小写敏感度; 大写3行全在scan_internal.sh自引用, 小写28行全在tests/contracts+tests/deploy+.gitignore已声明豁免范围
- **来源**: v21终局深测 (LEDGER #74/#75), D3口径差异结论
- **现状**: 真残留=0, 差异为豁免目录口径, 非实际清偿遗漏
- **证据**: audit_workspace/stranger_v21/p1_crosscan.txt
- **处置建议**: 无需处置, 已定性为豁免口径差异

### P3-6: batch内容引用在.gitignore/yaml/py文件(31文件)
- **描述**: batch内容引用分布在.gitignore/yaml/py等配置与脚本文件中, 属scan_internal.sh已声明豁免范围
- **来源**: v20清偿 (LEDGER #73), scan_internal.sh假阳性排除
- **证据**: audit_workspace/v20_rework/final_scan_snapshot.txt (假阳性排除列表)
- **处置建议**: 下一版本评估是否中性化, 或保留为内部测试编号约定

---

> 本文件由流水线闭卷·归档执行(LEDGER #76)生成, 作为下一版本pending任务书的素材底稿.
> 仓库状态: HEAD=0cb7443, 28面全绿, 5/5 deploy5 healthy, 常驻协议已解除.
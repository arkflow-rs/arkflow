## Why

Hub 平台阶段 2 的现存唯一剩余项(PLANNING.md 5.2-9):长稳与规模上限验证从未做过——`crates/arkflow-server/tests/` 只有功能性测试(`two_node_job_smoke` 2 节点、`kernel_job_lifecycle`),没有任何耐力或规模证据。此前它立不了项是因为存在已知无界表与 tick 上的重清扫;`bound-control-plane-storage-history` 已归档(2026-09-13)后,「表行数收敛」「零无界增长」类断言全部可以钉死,验证窗口已开。不关闭它,阶段 2 的「生产基础」永远差一块可判定证据。

## What Changes

- **舰队 harness**(新测试设施,零生产行为变更):以 `two_node_job_smoke` 同型放大——真实 Hub(`serve_hub` over loopback TCP)+ N 个真实 `agent::run` 循环(真实 HTTP、真实重连/退避/会话 TTL 路径),共享 `ControlPlaneStore`;间隔与 TTL 可注入(加速时钟);进程 RSS 采样器(Linux `/proc/self/statm`,macOS `ps -o rss=` 回退,零新依赖)。
- **规模阶梯测试**:N 阶梯爬升,断言每个梯级功能完备(全部命令终态、节点注册表可查询、清扫后历史表收敛)并记录派发延迟百分位;**CI 门禁版** N=32 快速运行,**`#[ignore]` 完整版** N 至 `MAX_NODES=256`。
- **长稳 soak 测试**:小 TTL + 高频间隔的加速时钟下持续 Job churn,注入 Hub 重启风暴(同 store 重建 Hub,token 全灭 → 真实 jitter 化 re-register 洪峰)与 agent 全灭重生;断言命令 p99 无漂移、RSS 进入平台期、全部有界表收敛。**CI 短版**(~2 分钟,N=16)+ **`#[ignore]` 一小时版**(N=64)。
- **容量结论文档化**:真实运行后把规模上限数字记入 PLANNING.md(沿用 `kernel_perf_baseline` 的先例)。

## Capabilities

### New Capabilities

(无)

### Modified Capabilities

- `control-plane-fleet`:新增「Hub sustains the maximum fleet」需求——`MAX_NODES`(256)满员舰队下 Hub SHALL 保持功能完备:每个派发命令到达终态、注册表可查询、持久历史存储在保留策略下收敛;中断(Hub 重启/agent 全灭)后 SHALL 自动恢复且不产生同步注册风暴。这是对既有 `MAX_NODES` 准入上限背后**从未规格化的舰队级保证**的显式化,也是本验证 harness 的 spec 锚点。

## Impact

- 新增 `crates/arkflow-server/tests/` 测试文件(fleet fixture + 4 个测试,2 个 CI 门禁、2 个 `#[ignore]`);可能新增少量测试专用辅助模块。
- **零生产行为变更**:不改 `hub.rs`/`agent.rs`/`lib.rs`/`storage.rs` 的任何生产路径;若实现中发现缺陷,以独立修复提交处理并在本 change 记录。
- 无新依赖(RSS 走 `/proc` 与 `ps` 回退;HTTP 走既有 `reqwest`)。
- CI 时长预算:门禁版合计新增 < 3 分钟。
- 明确不在本 change:绝对性能门禁(环境相关,只记录不门禁)、embedded_kv 之外的状态后端验证、v1 合入 main、多节点 network shuffle 类分布式验证。

## Non-goals

- 绝对性能门禁:延迟/RSS 数字随环境波动,本 change 只记录不设硬门禁;硬门禁仅限正确性与有界性。
- 生产代码变更:零生产行为改动;实现中若暴露缺陷,独立修复提交处理。
- embedded_kv 之外的状态后端验证、WAL/S3 长稳、多节点 network shuffle 类分布式验证。
- v1 合入 main(独立动作,不与本验证耦合)。
- 新依赖与观测面扩展:RSS 走 `/proc` + `ps` 回退,指标走既有 `/api/v1/metrics` 与 `operational_aggregates`。

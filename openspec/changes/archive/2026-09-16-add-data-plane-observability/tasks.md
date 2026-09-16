## 1. 内核指标注册表（arkflow-core）

- [x] 1.1 新增 `JobMetricsRegistry`（`Arc<Mutex<BTreeMap<String, Arc<KernelMetrics>>>>`）并挂到 `RuntimeManager`，提供 register/unregister/snapshot 访问器
- [x] 1.2 `run_job_with_checkpoints_started` 增加可选 registry 参数：spawn 成功后注册 `handle.metrics()`，watcher 返回后注销；现有调用方传 `None` 保持行为
- [x] 1.3 `Engine::run_with_cancellation` 为每个 YAML Job 传入共享 registry（编译为 JobSpec 的 Streams 不注册），`ControlPlane` 经 `runtime_manager()` 可达
- [x] 1.4 单测：Job 运行期间 registry 含该 job_id 的 `KernelMetrics`，Job 结束后注销（tokio test，用现有 job_runner_adapter 测试基建）

## 2. Prometheus 渲染层（arkflow-server/src/metrics.rs）

- [x] 2.1 新建 `metrics.rs`：实现快照 → `prometheus::proto::MetricFamily` 渲染，`TextEncoder` 输出 0.0.4 文本；按 design D2 词表定义 kernel Job family（counter/gauge 类型正确）
- [x] 2.2 legacy `arkflow_stream_*` 序列并入同一渲染函数，名字/标签/含义不变，补齐 HELP/TYPE
- [x] 2.3 单测：渲染输出含 TYPE counter（`_total` 后缀）与 gauge 的 family；错误消息/关联 ID 不出现在任何 label（构造带错误计数的快照断言序列数不变）
- [x] 2.4 本地 `/metrics` handler（`lib.rs:3123`）切换到渲染函数，接入 registry 快照；现有 metrics 集成断言更新并通过

## 3. 独立 observability 监听与 /ready /live

- [x] 3.1 `ServerConfig` 增加 `observability` 配置节（enabled 默认 true、默认 `127.0.0.1:8081`、路径可配），含反序列化默认值单测
- [x] 3.2 实现 `serve_observability(control_plane, config, cancellation)`：仅挂 `/metrics`、`/ready`、`/live` 三个 handler，绑定失败记告警不 panic；API server 启用时跳过独立监听
- [x] 3.3 现有 router 增加 `/ready`、`/live` 路由（`/readiness`、`/liveness` 原样保留），`/ready` 复用 `control_plane.health()` 语义
- [x] 3.4 `crates/arkflow/src/main.rs` 接线：始终 spawn observability 监听（enabled 时），与 server/agent 任务并行受同一 cancellation 管理
- [x] 3.5 集成测试：`server.enabled=false` 时 `/metrics` 返回合法 exposition、`/ready` 在 runtime 启动完成前 503、完成后 200（tower::ServiceExt oneshot 或绑定临时端口）

## 4. Agent per-Job 上报与 Hub 导出

- [x] 4.1 `NodeReport` 新增 `jobs: BTreeMap<String, KernelMetricsSnapshot>`（serde default）；Agent 填充 per-Job 快照，保留现有扁平 `kernel_*` 聚合键
- [x] 4.2 Hub 按 (node, job) 保存最近一次快照；Agent 租约过期/注销后清除该 node 的数据面快照
- [x] 4.3 `hub_metrics` 追加数据面 family：复用渲染函数、附加 `node` 标签，仍走 operator Bearer 认证
- [x] 4.4 单测：旧格式心跳（无 jobs 字段）→ Hub 正常无数据面序列；两个 Agent 上报 → 序列按 node+job 区分；Agent 离线 → 其序列消失、其他节点保留

## 5. 端到端验证与回归

- [x] 5.1 集成测试：本地 YAML Job（generate→sql→stdout 类）运行中抓取 `/metrics`，断言 `arkflow_job_chain_*` 与 checkpoint/watermark/late events 序列存在且 counter 随数据递增
- [x] 5.2 回归：`cargo test --workspace` 全绿；`--validate` 与 examples 黄金测试不受配置新字段影响
- [x] 5.3 双节点 smoke（`two_node_job_smoke.rs`）补一条断言：Hub `/metrics` 出现两个节点的数据面序列

## 6. 文档

- [x] 6.1 配置参考补 `server.observability` 字段与默认值；说明生产部署应显式配置绑定地址
- [x] 6.2 可观测性文档：指标词表（名称/类型/标签）、`/ready` 与 `/readiness` 的关系、Hub 抓取与认证说明
- [x] 6.3 更新 `openspec/PLANNING.md`：数据面可观测性方向落地状态记录

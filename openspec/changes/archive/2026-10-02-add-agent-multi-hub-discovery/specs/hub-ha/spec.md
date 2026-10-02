## ADDED Requirements

### Requirement: Agent SHALL 通过候选列表与 leader 提示发现并跟随当前 leader

Agent 模式的 `hub_urls` SHALL 为 Hub 候选地址列表（YAML 序列；空列表或缺省维持 standalone 模式语义），候选列表去除尾部 `/`、保序去重。配置解析遇到旧字段名 `hub_url`（无论何种形态）SHALL 返回带迁移指引（改写为 `hub_urls: ["…"]` 列表）的配置错误，MUST NOT 静默忽略该字段。注册尝试与会话结束时 SHALL 按候选顺序轮换活跃地址；收到 standby 的 503 `hub_standby` 响应 SHALL 视为「可达但非 leader」，立即轮换到下一候选而不消耗完整指数退避；全部候选失败一整圈后才应用既有带抖动的指数退避。注册成功的地址 SHALL 提升为首选（移至候选首位），后续扫描从它开始。Hub 会话切换 SHALL 不影响本地数据面（Stream/Job 继续运行、shuffle 监听保持）。单条目列表的运行行为 SHALL 与本变更前的单地址部署一致（仅配置字段名与语法为破坏性变更）。

#### Scenario: 旧字段名被响亮拒绝

- **WHEN** 配置以旧字段名声明 `hub_url: "http://hub-a:8080"`（或任何旧键形态）并启动或 `--validate`
- **THEN** 配置解析失败，错误信息包含改名与列表化的迁移指引，进程不以 standalone 模式静默启动

#### Scenario: standby 503 立即轮换

- **WHEN** Agent 的活跃 Hub 地址对注册返回 503 与 `hub_standby` 错误码，且候选列表中存在其他地址
- **THEN** Agent 不等待完整退避即尝试下一候选，standby 侧不创建节点会话与凭据

#### Scenario: 全候选失败保留指数退避

- **WHEN** 候选列表所有地址在一整圈内均注册失败（不可达、超时或非 standby 错误）
- **THEN** Agent 应用既有 equal-jitter 指数退避后重新扫描，不进入无退避忙转

#### Scenario: 成功注册提升首选

- **WHEN** 某候选接受注册且会话建立
- **THEN** 该地址被移至候选首位，下一次重连扫描从它开始，退避计数重置

#### Scenario: leader 提示直达

- **WHEN** standby 的 503 响应体携带指向另一可达 Hub 的 `leader_url` 提示
- **THEN** Agent 将提示目标插队至候选最前并在下一次尝试注册它；提示目标失败时回落到配置列表继续轮换

#### Scenario: 切换不影响数据面

- **WHEN** Agent 因会话失败或 standby 轮换切换活跃 Hub 地址
- **THEN** 本地 Stream/Job 与 shuffle 数据面监听继续运行，切换后以相同 `boot_id` 重新注册并重放缓存的终态结果

### Requirement: 租约 SHALL 携带 leader 广播地址并由 standby 响应提示

`HubHaConfig` SHALL 支持 `advertise_url`（leader 对 Agent 广播的 API 基址）；`try_acquire` 与 `renew` SHALL 把调用方当前的广播值写入租约行（未配置即清空该列，行始终镜像当前持有者的广播值），`HubLeaseSnapshot` SHALL 暴露该值。SQLite 与 PostgreSQL SHALL 以幂等 DDL 提供 `cp_hub_lease.advertise_url` 列，空库与既有库启动时自动收敛，无需停机迁移。standby 的 503 响应 SHALL 在租约快照未过期且携带广播地址时，于 problem body 中输出 `leader_url` 扩展字段；HA 未启用或租约行无广播地址时，503 响应体 SHALL 与现状一致（无该字段）。

#### Scenario: 晋升时广播地址写入租约行

- **WHEN** 配置了 `advertise_url` 的 standby 取得租约
- **THEN** 租约行的 `advertise_url` 等于该值，租约快照将其暴露给读取方

#### Scenario: 续期同步更新广播地址

- **WHEN** leader 以不同于上次获取时的 `advertise_url` 续约
- **THEN** 租约行的广播列更新为新值，epoch 不变

#### Scenario: standby 503 携带 leader 提示

- **WHEN** 非白名单请求打到 standby，且共享存储中的租约行未过期并携带 `advertise_url`
- **THEN** 响应为 503 `hub_standby`，problem body 含 `leader_url` 字段指向该地址

#### Scenario: 无广播地址时 503 与现状一致

- **WHEN** 租约行未携带 `advertise_url`（未配置或 HA 未启用）
- **THEN** 503 响应体不含 `leader_url` 字段，Agent 按候选扫描语义工作

## MODIFIED Requirements

### Requirement: leader SHALL 周期续约并在丢失时让位

leader SHALL 以不超过 TTL/3 的周期续约；续约返回 Lost 或存储不可达时 SHALL 立即让位为 standby（周期任务在下一 tick 停摆、readiness 转未就绪、路由回到 503 门控）。整个故障接管窗口 SHALL 有界于租约 TTL 加一个探测周期。leader 优雅关停时 SHALL 尽力主动释放租约，使 standby 无需等待 TTL 过期即可接管。续约的 epoch 读取与条件写入 SHALL 原子完成：一次条件 UPDATE SHALL 同时返回生效后的 epoch，任何后端实现 SHALL NOT 在条件写入成功后另行读取 epoch 而留下可观察的不一致窗口。两个存储后端 SHALL 对同一续约契约测试（含并发续约）表现一致。

#### Scenario: 续约丢失立即让位

- **WHEN** leader 的续约返回 Lost（例如另一实例在 TTL 过期后接管）
- **THEN** 该实例转为 standby，停止周期调和，readiness 报告未就绪

#### Scenario: leader 进程死亡后 standby 有界接管

- **WHEN** leader 进程异常退出且未释放租约
- **THEN** standby 在租约 TTL 过期后的首个尝试周期内取得租约并完成晋升

#### Scenario: 优雅关停主动释放

- **WHEN** leader 收到关停信号正常退出
- **THEN** 租约被立即释放，standby 的下一次 `try_acquire` 即可成功

#### Scenario: 续约原子返回 epoch

- **WHEN** 任一后端执行一次成功的续约（条件 UPDATE 命中）
- **THEN** 返回的 Renewed epoch 来自同一条条件写入，且并发续约场景下每个成功续约方读到的 epoch 与其写入生效时的租约状态一致（契约测试在两后端上钉扎）

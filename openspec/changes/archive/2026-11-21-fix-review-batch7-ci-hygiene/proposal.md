## Why

2026-11 全量代码审查（仓库根 `CODE-REVIEW-2026-11-15.md`）的修复路线图中，批次 1-2（#1367/#1372）、A+B 批（#1375）、P0+C 批（#1376）已全部落地；2026-11-21 逐条核实确认批次 3-6 登记的全部 P1（#8-#11、#12-#20、#17-#18/#28/#29、#19/#25/#26/#27、#21-#24）均已在 main 修复。剩余工作收敛为批次 7（CI 卫生）中未被任何 change 覆盖的三项：

- **全仓库无 `[workspace.lints]`**：clippy 纪律（`-D warnings`）只活在 CI 命令行，本地运行与清单均不携带。
- **rand 版本声明违反集中化约定**：`arkflow-plugin` 直接声明 `rand = "0.10"`，与 workspace 的 `rand = "0.9"` 并存——这是锁文件里第三代 rand 的"自家贡献"；审查报告同时指出锁卫生测试只看 arrow/datafusion/zstd，对此无守卫。
- **CI 无一 `--locked`**：`rust.yml`/`coverage.yml`/`benchmark.yml` 的 cargo 命令都允许 Cargo 在 CI 静默改锁文件，架空锁卫生测试。

## What Changes

- **`[workspace.lints]` 落地**：根 `Cargo.toml` 增加 `[workspace.lints]`（`rust::unsafe_op_in_unsafe_fn`、`clippy::all`、`clippy::dbg_macro`、`clippy::todo`，全部当前零违规），四个成员 crate 增加 `[lints] workspace = true`。CI 侧 `-D warnings` 保持不变，纪律从此随清单走。
- **rand 声明集中化**：`arkflow-plugin` 改用 `rand = { workspace = true }`（用法仅 qdrant 的 `rand::random::<u64>()`，0.9 语义相同）。**诚实声明**：锁文件中 rand 0.8/0.9/0.10 三代共存是传递依赖结构性钉死的（`jsonwebtoken`/`nkeys`/`rsa` 需 ^0.8，`arrow-avro` 经 datafusion 需 ^0.9，`apache-avro`/`ballista`/`async-nats` 需 ^0.10），本侧无法收敛，只能消除自家直接声明。
- **锁卫生测试扩展**：`dependency_lock_hygiene.rs` 新增约定测试——工作区成员必须经 workspace 表声明 `rand`（单版本断言对 rand 会永久红，约定断言才是可守卫的形状）；文档注释如实记录三代共存的传递根因。
- **CI `--locked`**：`rust.yml`（clippy/build/nextest/test）、`coverage.yml`（llvm-cov）、`benchmark.yml`（cargo run）全部加 `--locked`，锁文件与清单失同步即刻红。

## Capabilities

### New Capabilities

- `ci-hygiene-gates`：把 CI 卫生门禁固化为可验证契约——cargo CI 命令 SHALL `--locked`；clippy 纪律 SHALL 由 `[workspace.lints]` 携带并经 `[lints] workspace = true` 继承、CI deny warnings；成员 crate 依赖 `rand` SHALL 经 `[workspace.dependencies]` 声明（锁卫生测试强制），rand 三代传递共存如实记录为不可收敛豁免。

### Modified Capabilities

（无。）

## Impact

- **清单**：根 + 4 个成员 `Cargo.toml`；`Cargo.lock` 仅 plugin 的 rand 依赖边 0.10.3 → 0.9.5（解析集不变）。
- **CI**：三个 workflow 的 cargo 命令加 `--locked`；行为无其他变化。
- **测试**：`crates/arkflow/tests/dependency_lock_hygiene.rs` +1 约定测试。
- **规格**：新 capability `ci-hygiene-gates`（CI 门禁契约）。
- **无用户可见面**：无文档任务（docs/docs/ 无涉及页）。

## Non-goals

- **rand 三代收敛到一代**：传递依赖钉死，需上游（jsonwebtoken/nkeys/rsa、apache-avro/ballista）发布迁移后才能收敛；已在测试注释记录根因。
- **依赖集中化约定的全面强制**：`tower`/`tower-http`/`base64`/`bytes`/`object_store`/`once_cell`/`num_cpus` 在 plugin 的重声明是审查报告 P2 层独立条目，不在本批次。
- **第三方 action SHA 固定 / permissions 块**：P2 层独立条目。
- **`unwrap_used`/`unimplemented` 等更严 lint**：生产面存量不支持（unwrap 8600 处、unimplemented 测试内 1 处合法使用），启用即大面积红。

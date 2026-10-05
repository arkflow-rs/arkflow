# Proposal: release-engineering-batch-e

## Why

v1.0 评审批次 E（发布工程）判定「技术内核已达 v1.0 水准，发布工程未就绪」，且维护者已于 2026-10-04 决策跳过 0.6 中间版、积压随 v1.0 一次性释放（PLANNING §9.4）——发布工程成为打 v1.0 的唯一硬前置。当前证据：

- CI 装了 clippy/rustfmt 组件但从不执行（`.github/workflows/rust.yml:64` 仅 `components: clippy, rustfmt`，整个文件只有 `cargo build`（`:84`）与测试步骤（`:107-108`）），lint 门禁为零。
- 无 release 自动化：`.github/workflows/` 下无 tag 触发的产物构建/发布流程，唯一发布通道是 `docker.yml` 的镜像 tag push；无二进制产物、无 GitHub Release、无 crate publish 通道。
- 仓库根无 `CHANGELOG.md`、无 `SECURITY.md`、无产品版本策略与升级指南（`docs/reference/compatibility.md` 只讲文档站点快照）；版本号自 v0.5.0（2025-10-19）后一年未动，期间约 160 个已归档变更无面向用户的发行说明。
- `arkflow-server` 的 CLI 面（`migrate` 子命令 + `ARKFLOW_HUB_*`/`ARKFLOW_HUB_HA_*` 启动环境变量，`crates/arkflow-server/src/bin/arkflow-server.rs`）完全未进 `docs/reference/cli.md`；MAX_NODES=256 与长稳结论只存在于内部 `openspec/PLANNING.md`，用户不可见。

## What Changes

- **CI lint 门禁**：`rust.yml` 新增 `cargo fmt --check` 与 `cargo clippy --workspace --all-targets -- -D warnings` 步骤（复用已装组件与既有缓存策略）；如存量代码存在 fmt 漂移，随本变更一次性格式化对齐。
- **Release 自动化**：新增 `.github/workflows/release.yml`，tag `v*` 触发：四平台二进制构建矩阵（linux amd64/arm64 gnu、macOS amd64/arm64）→ 压缩产物（含 LICENSE/README）→ GitHub Release 附产物；附带 `workflow_dispatch` 手动通道与 `cargo publish --dry-run` 打包校验任务。
- **CHANGELOG.md**（仓库根，Keep a Changelog 格式）：以「Unreleased（面向 v1.0）」为唯一节，按主题（内核/控制面/插件/console/文档/依赖）归纳 v0.5.0 以来的演进，链接到 PR/change 归档。
- **SECURITY.md**（仓库根）：漏洞报告渠道、支持版本策略、范围声明。
- **版本策略与升级指南**：新增文档页（en + zh-Hans），定义语义化版本承诺、v1.0 冻结面、升级步骤与兼容边界；与 `compatibility.md`（文档快照机制）互补不重复。
- **server CLI 与规模上限进用户文档**：`docs/reference/cli.md`（en/zh）补 `arkflow-server` 一节（env-var 启动面、`migrate` 子命令）；分布式文档页（en/zh）补运营上限一节（MAX_NODES=256、长稳测试结论）。
- **评审清单收口**：`CODE_REVIEW_2026-09-29.md` 与 `PLANNING.md` 将批次 E 剩余项划掉（0.6 一项已提前划掉）。

## Capabilities

### New Capabilities

- `release-engineering`: CI lint 门禁（fmt/clippy 必须实际执行并阻断）、tag 驱动的多平台二进制发布流程、crate 打包校验、CHANGELOG 与 SECURITY 维护义务。

### Modified Capabilities

- `documentation-release-lifecycle`: 新增需求——产品级 CHANGELOG 与版本策略/升级指南页（en/zh）是发布面向用户的必备产物。
- `control-plane-deployment`: 新增需求——Hub server CLI 启动面（环境变量与 `migrate` 子命令）SHALL 进入 CLI 参考文档（en/zh）。
- `control-plane-fleet`: 新增需求——fleet 运营上限（MAX_NODES 与长稳结论）SHALL 进入用户文档（en/zh）。

## Impact

- `.github/workflows/rust.yml`（新增 lint 步骤）、新增 `.github/workflows/release.yml`。
- 仓库根新增 `CHANGELOG.md`、`SECURITY.md`。
- `docs/docs/`（versioning/升级指南新页）、`docs/docs/reference/cli.md`、分布式文档页，及 `docs/i18n/zh-Hans/` 对应镜像、`docs/sidebars.ts`（新页注册）；全部过 `pnpm docs:check`。
- 如存量 fmt 漂移：全仓一次性 `cargo fmt`（纯格式化，零语义变更）。
- `openspec/CODE_REVIEW_2026-09-29.md`、`openspec/PLANNING.md` 划掉批次 E 剩余项。
- 不动版本号、不产生实际发布动作（tag 由维护者另行决定）。

## Non-goals

- **不发 0.6**（2026-10-04 维护者决策，已记录于 PLANNING §9.4）；本变更不 bump 版本号、不打 tag。
- 不做 Windows 平台二进制产物（核心引擎刚获得 Windows 编译门控，产物化另行评估）。
- 不做 crate 实际自动 publish 上线（crates.io token 与发布时机是维护者动作；本变更只提供 `--dry-run` 打包校验与手动触发通道）。
- 不做覆盖率/多平台测试矩阵扩展（覆盖率已有 `coverage.yml`；测试矩阵扩展另行评估）。
- 插件层契约合规测试框架已随 P1-5（`fix-input-cancellation-safety`）落地，不重复。
- 不重写 `compatibility.md` 的文档快照机制。

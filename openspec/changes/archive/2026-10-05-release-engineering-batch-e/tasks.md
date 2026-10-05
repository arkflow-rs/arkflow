# Tasks: release-engineering-batch-e

## 1. CI lint 门禁（rust.yml）

- [x] 1.1 探测存量 fmt 漂移规模（`cargo fmt --check`）；如有漂移执行一次 `cargo fmt` 全量格式化（独立于功能改动的纯格式变更），确认 `cargo build --workspace` 与 `cargo test -p arkflow-core` 不受影响
- [x] 1.2 `rust.yml` 新增 fmt job：`cargo fmt --all -- --check`（复用既有 protoc 安装与缓存模式）
- [x] 1.3 `rust.yml` 新增 clippy job：`cargo clippy --workspace --all-targets -- -D warnings`，本地预跑确认零告警
- [x] 1.4 验证两个新 job 的步骤顺序/缓存与既有 build/test job 一致，不引入新第三方 action

## 2. Release 自动化（release.yml）

- [x] 2.1 新增 `.github/workflows/release.yml`：`on: push: tags: ['v*']` + `workflow_dispatch`；首个 job 校验 tag 与 workspace version 一致（不一致 fail 并给出先 bump 版本的提示）
- [x] 2.2 构建矩阵 job：ubuntu-latest（x86_64-unknown-linux-gnu）、ubuntu-24.04-arm（aarch64-unknown-linux-gnu）、macos-13（x86_64-apple-darwin）、macos-14（aarch64-apple-darwin），release profile 构建全部 bin target
- [x] 2.3 打包步骤：每 target 产 `arkflow-<tag>-<triple>.tar.gz`（含 arkflow、arkflow-server 二进制 + LICENSE + README.md）并 upload-artifact
- [x] 2.4 release job：tag 触发时下载全部产物、`gh release create` 附产物（`permissions: contents: write`；dispatch 触发时跳过创建 Release）
- [x] 2.5 crate 打包校验 job：按依赖序对 arkflow-core → arkflow-plugin → arkflow → arkflow-server 跑 `cargo publish --dry-run`（不配 crates.io token，不做真实发布）
- [x] 2.6 workflow YAML 静态校验（actionlint 或等价检查）+ 复查与 rust.yml/coverage.yml 的步骤模式一致性

## 3. CHANGELOG.md 与 SECURITY.md

- [x] 3.1 以 `git log v0.5.0..HEAD --oneline` 与 `openspec/changes/archive/` 目录清单交叉核对，按用户可感知主题（新组件与处理器/内核与执行语义/控制面与 HA/可靠性修复/可观测性/console/安全/依赖）归纳 v0.5.0 以来演进，写入根目录 CHANGELOG.md（Keep a Changelog，单节 `Unreleased`，注明面向 v1.0）
- [x] 3.2 每个主题条目附 PR 链接（git log 可得者）；不逐条罗列内部重构
- [x] 3.3 根目录 SECURITY.md：私密漏洞报告主渠道（GitHub private vulnerability reporting）+ 维护者邮箱 fallback、支持版本表（当前仅 latest）、处理范围与 SLA 描述
- [x] 3.4 CHANGELOG 事实抽查：随机抽 5 条陈述回溯到对应 PR/代码，确认无虚标

## 4. 版本策略 / 升级指南（en/zh）

- [x] 4.1 新页 `docs/docs/reference/versioning.md`：语义化版本承诺、v1.0 公共面冻结范围（config schema/CLI/pub API）、升级步骤与兼容边界、指向 CHANGELOG；与 compatibility.md 互补不重复
- [x] 4.2 zh-Hans 镜像页同步；`docs/sidebars.ts` 注册新页；如页内含 yaml 示例块则带 validate 分类标记
- [x] 4.3 `pnpm docs:check` 通过（含 sidebar 可达性与 en/zh 一致性）

## 5. server CLI 与 fleet 上限文档（en/zh）

- [x] 5.1 `docs/docs/reference/cli.md` 新增 `arkflow-server` 章节：全部启动环境变量（含默认值与效果、`ARKFLOW_HUB_HA_*` 租约族）+ `migrate` 子命令契约；以 `crates/arkflow-server/src/bin/arkflow-server.rs` 实际解析行为为准逐项核对
- [x] 5.2 `docs/docs/build/distributed-jobs.md` 新增运营上限小节：MAX_NODES=256、长稳测试结论摘要、超限指引
- [x] 5.3 两页 zh-Hans 镜像同步；yaml/shell 代码块分类标记补齐；`pnpm docs:check` 通过

## 6. 评审清单收口与整体验证

- [x] 6.1 `openspec/CODE_REVIEW_2026-09-29.md` 发布工程节 2-4 项与「批次 E」行划掉并注明落地 change；`openspec/PLANNING.md` §9.3 批次 E 行同步划掉
- [x] 6.2 `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets -- -D warnings` 本地全绿
- [x] 6.3 `cargo build --workspace --all-targets` 通过（确认 fmt 格式化与 workflow 改动不破坏构建）
- [x] 6.4 `pnpm docs:check` 全绿（en/zh）；docs 相关 workspace 测试（`docs_inventory_snapshot`、`examples_validate`、`docs_snippets_validate`）无回归

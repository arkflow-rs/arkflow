# Design: release-engineering-batch-e

## Context

批次 A/B/D 已全部闭环，维护者决策跳过 0.6（PLANNING §9.4），批次 E 剩余子项成为打 v1.0 前的最后一批工程。现状：`rust.yml` 已装 clippy/rustfmt 组件但只跑 build+test；`.github/workflows/` 无 release 流程（唯一发布通道 `docker.yml` 镜像 tag push）；仓库根无 CHANGELOG/SECURITY/版本策略；`arkflow-server` CLI 与 MAX_NODES 长稳结论未进用户文档。约束：docs 站有多重 CI 门禁（`pnpm docs:check`、yaml 代码块分类标记、en/zh 镜像、sidebar 注册）；Rust 1.97+；CI 需 protoc；仓库已有 `coverage.yml`（cargo-llvm-cov）与 `docker.yml` 可作 workflow 风格参照。

## Goals / Non-Goals

**Goals:**

- CI 上 fmt/clippy 从「装了不用」变为实际阻断门禁，且当前代码库零整改即可通过（fmt 漂移随本变更一次性格式化）。
- tag `v*` 一键产出四平台二进制并附到 GitHub Release；crate 打包正确性有 CI 级校验（`publish --dry-run`）。
- 用户可感知的发布产物齐备：CHANGELOG（v0.5.0 以全部演进）、SECURITY.md、版本策略/升级指南、server CLI 参考、fleet 运营上限，全部 en/zh 双语。

**Non-Goals:** 见 proposal「Non-goals」（不发 0.6/不 bump 版本/无 Windows 产物/无 crate 实际上线/无测试矩阵扩展）。

## Decisions

1. **lint 门禁放进现有 `rust.yml`，作为独立 job**，而非新文件。理由：复用既有的 protoc 安装、Swatinem/rust-cache 依赖缓存与磁盘清理模式，避免第二份缓存逻辑漂移。`fmt` 与 `clippy` 拆两个 job：fmt 失败最快暴露、clippy 吃缓存独立重跑。clippy 用 `cargo clippy --workspace --all-targets -- -D warnings`——`fix-review-p2-remainder` 收尾验证已确认全仓零 clippy 告警，`-D` 只是把告警升级为失败，不需要先整改代码。
2. **存量 fmt 漂移一次性格式化**（`cargo fmt`），随本变更合入。备选「逐步豁免」被否：仓库从未强制过 fmt，漂移面只会更大。格式化是纯空白/换行变更，与功能改动分开成独立提交，便于 review 与回溯。
3. **release workflow 用 native runner 矩阵，不用 cross/QEMU**：`ubuntu-latest`（linux amd64）、`ubuntu-24.04-arm`（linux arm64，GitHub 官方免费 runner）、`macos-13`（amd64）、`macos-14`（arm64）。理由：PyO3/protoc/native 依赖在交叉编译下工具链复杂度高，native runner 零额外工具链。备选（cross + musl 静态链接）记录为回退路径：若 arm64 runner 不可用再切换。
4. **产物形态**：每 target 一个压缩包 `arkflow-<tag>-<target-triple>.tar.gz`（macOS 同 tar.gz，保持统一），内含二进制 + LICENSE + README.md。Release 创建用预装 `gh release create`（`permissions: contents: write`），不引入第三方 release action——少一个供应链依赖。
5. **tag 与版本一致性校验步骤**：release job 先比对 tag 名与 workspace `version`，不一致即 fail 并提示先 bump 版本再打 tag。本变更自身不 bump 版本（打 tag 时机由维护者决定），但把流程写死，避免 tag 与 Cargo.toml 脱节的产物。
6. **crate 校验为独立 job**：`cargo publish --dry-run` 按依赖序逐 crate（arkflow-core → arkflow-plugin → arkflow → arkflow-server），挂在 release workflow 内（tag 触发）+ `workflow_dispatch` 可单独跑。不配置 crates.io token、不做真实 publish——发布时机与凭据是维护者动作。
7. **CHANGELOG 采用 Keep a Changelog 单节 `Unreleased`**（标题注明面向 v1.0）。来源两路交叉核对：`git log v0.5.0..HEAD --oneline` 与 `openspec/changes/archive/` 目录清单。不逐条罗列 ~160 个 change，按用户可感知主题归纳（新增组件与处理器/内核与执行语义/控制面与 HA/可靠性修复/可观测性/console/安全/依赖），每条带 PR 链接（有则附）。发布时由维护者把 `Unreleased` 改名为版本号——此义务写入 `release-engineering` spec。
8. **SECURITY.md 用 GitHub 私密漏洞报告为主渠道**（repo 在 github.com/arkflow-rs/arkflow，原生支持），维护者邮箱为 fallback；支持版本表只列当前行（v1.0 发布前仅 latest），不做多版本承诺。
9. **文档落点**：版本策略/升级指南新页 `docs/docs/reference/versioning.md`（与 `compatibility.md` 同级、互补：那边讲文档站点快照，这边讲产品语义化版本与升级）；server CLI 并入既有 `docs/docs/reference/cli.md` 新增 `arkflow-server` 章节（env-var 启动面 + `migrate`）；MAX_NODES=256 与长稳结论进 `docs/docs/build/distributed-jobs.md`（该页 #1302 刚更新过，主题匹配）。zh 镜像按 `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/` 对应路径同步；新页注册进 `docs/sidebars.ts`；所有新增 yaml 代码块带 `validate=foreign reason` 或 fragment 分类标记（env 片段非引擎配置）。
10. **评审清单收口**：`CODE_REVIEW` 发布工程节 2-4 项与批次 E 行划掉；PLANNING §9.3 批次 E 行划掉并注明落地 change 名。

## Risks / Trade-offs

- [一次性 `cargo fmt` diff 大、污染 blame/冲突面] → 格式化独立成单独提交；冲突面靠「尽早合入、合入前 rebase」缓解；不引入 `.git-blame-ignore-revs`（另行评估，避免本变更膨胀）。
- [release workflow 首次运行前无法端到端验证] → YAML 用 actionlint（若本机可用）静态校验 + 复用既有 workflow 的成熟步骤模式 + `workflow_dispatch` 通道允许不打 tag 空跑构建矩阵；真实 Release 效果由维护者打第一个 tag 时验证。
- [`ubuntu-24.04-arm` runner 配额/可用性变化] → 回退方案已记录（cross 工具链或 QEMU，决策 3）；矩阵改一行即可切换。
- [`cargo publish --dry-run` 需访问 crates.io index，离线环境会挂] → 视为预期失败信号（打包/依赖序问题才是它要抓的），不在 CI 里静默吞掉。
- [clippy 新版本引入新 lint 导致未来误红] → `-D warnings` 是仓库既有验收标准（AGENTS.md「lint before finishing」），接受未来 lint 升级推动的零星整改，属预期维护成本。
- [CHANGELOG 编写有主观取舍] → 以「用户可感知」为唯一收录标准（新组件/新配置/行为变化/重要修复），纯内部重构不列；spec 化为发布义务而非穷举义务。

## Migration Plan

合入即生效：CI lint 门禁立即阻断后续 PR；release workflow 处于待命状态（无 tag 不触发）。回滚 = revert 本变更（fmt 格式化可随整体 revert，无数据迁移）。首个真实 tag（v1.0 系）前，维护者需：bump 版本 → 把 CHANGELOG `Unreleased` 改名为版本号 → 打 tag push。

## Open Questions

无阻塞项。实现时需实测确认两点：存量 fmt 漂移的实际规模（决定格式化提交的拆分粒度）；`docs/i18n/zh-Hans` 下 reference/cli 与 versioning 的确切路径结构（以 `pnpm docs:check` 通过为准）。

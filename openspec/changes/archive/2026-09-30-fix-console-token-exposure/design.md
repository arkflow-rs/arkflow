## Context

`VITE_API_TOKEN` 经 Vite 在构建期静态替换进 JS bundle（`console/src/api.ts:305`），机制本身向后兼容且运行文档已警示。风险在于凭据的**意外扩散通道**：`COPY . .` 把 `.env` 拷进镜像层、无 git 忽略规则可致误提交、示例文件无警示。本变更为纯卫生学修复（打包/版本库/文档），四个文件、无代码逻辑改动。

## Goals / Non-Goals

**Goals:** 关闭三个意外扩散通道（镜像上下文、版本库、无警示示例），并为受控构建提供显式 token 注入通道。

**Non-Goals:** token 换发代理、移除静态机制、镜像其他加固（见 proposal）。

## Decisions

1. **`.dockerignore` 用 `.env*` 全排除而非仅 `.env`。** Vite 会读取 `.env.local`、`.env.[mode]` 等变体，逐个枚举必有遗漏；构建无需任何 env 文件（token 走 ARG，见决策 3）。`node_modules`/`dist` 一并排除（`npm ci` 重新安装、dist 在多阶段构建中重新生成，拷入只污染上下文与层缓存）。
2. **git 忽略放根 `.gitignore` 而非新建 `console/.gitignore`。** 仓库现状是集中式根忽略（`console/dist/` 也在根）；同规则也顺带覆盖其他子目录可能出现的 `.env`。模式 `.env` + `.env.*` + `!.env.example`。
3. **Dockerfile 加 `ARG VITE_API_TOKEN` + `ENV`。** 备选"不加，让 docker 构建一律无 token"会砍掉文档化的受控构建用法；build-arg 是 Docker 的显式注入通道，构建命令可审计，且与 `.env*` 排除后不留隐性替代路径。token 仍会进 bundle（机制固有），警示交给 .env.example 与运行文档。
4. **`.env.example` 警示放在文件头注释。** 用户第一时间看到的位置。

## Risks / Trade-offs

- [排除 `.env*` 后，依赖"目录里放 .env 再 docker build"的既有工作流断掉] → 这是本变更的意图（隐性凭据通道关闭）；迁移路径一行：`docker build --build-arg VITE_API_TOKEN=...`，README/示例文件均有指引。
- [`ARG` 值会出现在 `docker history` 的构建层元数据] → 与"token 进公开 bundle"同级的既有限制（机制固有），运行文档已要求受控环境构建；换发代理是中期解。
- [纯配置改动无单测可写] → 以 `git check-ignore` 行为验证与文件内容审查代替；CI 的 docs:check 不涉及。

## Migration Plan

无配置面变化。回滚即 revert。

## Open Questions

无。

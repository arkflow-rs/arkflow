## Why

v1.0 就绪度审查 P1-12（`openspec/CODE_REVIEW_2026-09-29.md`）：console 的静态 operator token 会随构建上下文与版本库意外扩散。证据：

- `console/` 无 `.dockerignore`，Dockerfile 的 `COPY . .`（`console/Dockerfile:5`）把本地 `.env`（若存在）整体拷入构建上下文，凭据进入镜像层。
- 仓库无任何 `.env` 忽略规则（根 `.gitignore` 仅 `node_modules/`、`console/dist/` 等；`git check-ignore console/.env` 不命中，console/ 自身无 `.gitignore`）——含 token 的真实 `.env` 可能被直接提交进版本库。
- `console/.env.example:2` 的 `VITE_API_TOKEN=operator-secret` 只说明"须与 Hub 一致"，未警示该 token 经 Vite 静态替换后**内联进公开 JS bundle**，任何访客可从 bundle 提取写权限凭据（机制本身见 `console/src/api.ts:305`）。

运行文档（`docs/docs/operate/control-plane/deploy.md` en/zh、`console/README.md:19`）已有"仅在受控构建中设置"的警示——本变更补齐打包/版本库卫生与示例文件警示，不改变 token 机制本身（换发代理属中期方案，不在本变更）。

## What Changes

- 新增 `console/.dockerignore`：排除 `.env*`、`node_modules`、`dist` 等非构建必需内容，凭据文件不再进入镜像构建上下文。
- Dockerfile 增加显式 `ARG VITE_API_TOKEN` 通道（需要静态 token 的受控构建经 build-arg 注入，取代"把 .env 放进构建目录"的隐性做法）。
- 根 `.gitignore` 增加 `.env` / `.env.*` / `!.env.example` 规则，防误提交。
- `console/.env.example` 补警示：静态 token 会进入公开 bundle，仅限受信网络，生产环境走 OIDC。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `control-console`: 新增"凭据材料不得进入镜像构建上下文与版本库"要求——`.env` 排除、token 注入显式化、示例文件警示。

## Impact

- 新文件 `console/.dockerignore`；`console/Dockerfile`（+2 行 ARG/ENV）；根 `.gitignore`（+3 行）；`console/.env.example`（警示注释）。
- 行为变化：依赖"构建目录里放 .env"的隐性 docker 构建改用 `--build-arg VITE_API_TOKEN=...`（显式且可审计）。

## Non-goals

- 不实现 token 换发代理 / BFF（中期方案，另行立项）。
- 不移除 `VITE_API_TOKEN` 静态机制本身（向后兼容，已有运行文档警示）。
- 不改 `console/src/api.ts` 的 token 消费逻辑与 OIDC 流程。
- 不处理镜像的其他加固（非 root 用户、nginx 配置加固等）。

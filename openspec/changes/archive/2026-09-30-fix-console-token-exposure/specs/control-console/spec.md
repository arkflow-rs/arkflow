## ADDED Requirements

### Requirement: Console 凭据材料 SHALL NOT 进入镜像构建上下文与版本库

console 的构建产物链 SHALL 防止凭据意外扩散：`console/` 构建上下文 SHALL 通过 `.dockerignore` 排除 `.env*` 与 `node_modules`、`dist` 等非必需内容；版本库忽略规则 SHALL 覆盖 `.env` 与 `.env.*`（显式保留 `.env.example`）。静态 token 的受控构建注入 SHALL 走显式 build-arg（`ARG VITE_API_TOKEN`），不得依赖把 `.env` 文件放进构建目录。示例配置 SHALL 警示静态 token 会被内联进公开 JS bundle、仅限受信网络使用，生产环境应使用 OIDC。

#### Scenario: 本地 .env 不进入镜像

- **WHEN** `console/` 目录下存在含 `VITE_API_TOKEN` 的 `.env` 文件且执行 docker 镜像构建
- **THEN** `.env` 被排除在构建上下文之外，不出现在任何镜像层

#### Scenario: 静态 token 经显式 build-arg 注入

- **WHEN** 受控构建需要静态 token
- **THEN** 通过 `--build-arg VITE_API_TOKEN=...` 显式传入（Dockerfile 声明 `ARG`），构建命令可审计，不依赖构建目录中的文件

#### Scenario: 真实 .env 不会被提交

- **WHEN** 开发者在 `console/` 下创建 `.env`（或任意 `.env.*`，除 `.env.example`）并执行 `git add`
- **THEN** 该文件被忽略规则拦截，凭据不进入版本库

#### Scenario: 示例文件携带暴露警示

- **WHEN** 查阅 `console/.env.example`
- **THEN** 其中明示静态 token 会进入公开 bundle、仅限受信网络、生产环境走 OIDC

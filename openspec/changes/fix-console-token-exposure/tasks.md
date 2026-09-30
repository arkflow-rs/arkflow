## 1. 打包与版本库卫生

- [x] 1.1 新增 `console/.dockerignore`：排除 `.env*`、`node_modules`、`dist`、`src/**/*.test.ts` 等非构建必需内容
- [x] 1.2 `console/Dockerfile` 构建阶段声明 `ARG VITE_API_TOKEN` 并导出为 ENV（受控构建显式注入通道）
- [x] 1.3 根 `.gitignore` 增加 `.env`、`.env.*`、`!.env.example` 规则

## 2. 文档警示

- [x] 2.1 `console/.env.example` 头部补警示：静态 token 会内联进公开 JS bundle、仅限受信网络、生产走 OIDC；docker 受控构建用 `--build-arg VITE_API_TOKEN=...`

## 3. 验证

- [x] 3.1 `git check-ignore` 验证：`console/.env`、`console/.env.local` 命中忽略，`console/.env.example` 不命中
- [x] 3.2 `.dockerignore` 内容覆盖 spec 场景（.env* / node_modules / dist）
- [x] 3.3 `openspec validate fix-console-token-exposure` 通过
- [x] 3.4 受影响文档检查（`pnpm docs:check`，确认未破坏既有引用）

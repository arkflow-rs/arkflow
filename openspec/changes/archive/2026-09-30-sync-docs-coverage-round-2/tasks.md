## 1. 控制面文档(en+zh 成对编辑)

- [x] 1.1 deploy.md 围栏断言修正:从 crates/arkflow-server 实现确认 stale-leader 拒写的实际错误/事件形态(错误码或事件名),改写 EN `docs/docs/operate/control-plane/deploy.md` HA 小节——删除「full storage-level write fencing is a later HA stage」,改述 lease fencing epoch 强制与过期 leader 写入被拒、拒绝可观察;同步 zh-Hans 镜像同段落
- [x] 1.2 deploy.md token 构建小节:读 console/Dockerfile、.dockerignore、.env.example 确认实际行为,EN HA 小节附近补「console 生产镜像 token 注入」——`--build-arg VITE_API_TOKEN=...`、`.env*` 排除出构建上下文、token 内联进 bundle 的提示与生产 OIDC 建议;同步 zh-Hans 镜像
- [x] 1.3 api.md Hub 端点补齐:读 crates/arkflow-server hub 路由确认 `GET /api/v1/status`、`GET /api/v1/metrics`(内容协商:`format=json` → `{items, aggregate}`,默认 Prometheus text)、`POST /nodes/{id}/configuration/validate`、`GET /nodes/{id}/configuration/diff` 的方法/路径/响应形态,更新 EN `docs/docs/reference/api.md` Hub 小节(修正 :230 一带「仅 Prometheus text」的表述);同步 zh-Hans 镜像
- [x] 1.4 console.md 节点操作与 Audit 视图:读 console 源码确认 Drain/Maintain/Resume 三操作各自调用什么 API、触发什么后果,以及 Audit 页数据来源(GET /audit + 本地过滤);EN `docs/docs/operate/control-plane/console.md` Views 表加 Audit 行、概览页补节点维护操作小节与维护徽标说明;同步 zh-Hans 镜像

## 2. 组件与语义文档(en+zh 成对编辑)

- [x] 2.1 取消安全语义小节(delivery-semantics)
- [x] 2.2 六个 channel 型 input 页取消语义短句 + 链接
- [x] 2.3 sql.md 计划缓存与连接池
- [x] 2.4 memory.md 边界行为

## 3. 验证与收尾

- [x] 3.1 `pnpm docs:check` 全绿(预期 140 页,组件清单不变仍 49)
- [x] 3.2 `cargo test -p arkflow --test docs_snippets_validate --test examples_validate` 全绿
- [x] 3.3 `node scripts/i18n-freshness-report.mjs` 保持 0 过时页(en/zh 逐对同步确认)
- [x] 3.4 `openspec validate --change sync-docs-coverage-round-2` 通过;按 docs/DOCUMENTATION.md Change coverage 约定复核本变更任务清单已覆盖 en+zh

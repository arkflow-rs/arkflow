## ADDED Requirements

### Requirement: deploy 页 SHALL 如实描述存储级写围栏的当前状态
`docs/docs/operate/control-plane/deploy.md`(及 zh-Hans 镜像)SHALL NOT 声称
存储级写围栏(storage-level write fencing)属于尚未实现的后续 HA 阶段——
存储写路径已强制 lease fencing epoch(shipped,见
`openspec/specs/hub-ha/spec.md`)。页面 SHALL 记录:leader 续租推进 epoch,
过期 leader 的持久化写入被显式拒绝,拒绝以可观察的错误/事件形式暴露。

#### Scenario: 无「后续阶段」矛盾断言
- **WHEN** 阅读者查看 deploy 页的 HA 小节(en 或 zh)
- **THEN** 页面不包含「full storage-level write fencing is a later HA
  stage」或同义的未来时描述

#### Scenario: stale-leader 拒写行为可发现
- **WHEN** 阅读者查看 deploy 页的 HA 小节
- **THEN** 页面说明:失去租约的 leader 的存储写入会因 epoch 过期被拒绝,
  且该拒绝有明确的可观察表现(错误响应或事件流条目)

### Requirement: API 参考 SHALL 覆盖 Hub 机群状态与配置校验端点
`docs/docs/reference/api.md`(及 zh-Hans 镜像)的 Hub API 小节 SHALL 记录:
`GET /api/v1/status`(机群聚合状态)、`GET /api/v1/metrics` 的内容协商
(默认 Prometheus text,`format=json` 返回 `{items, aggregate}`),以及
`POST /nodes/{id}/configuration/validate` 与
`GET /nodes/{id}/configuration/diff`。方法/路径/响应形态 SHALL 与
crates/arkflow-server 的 hub 路由实现一致。

#### Scenario: Hub 端点表完整
- **WHEN** 阅读者查看 api.md 的 Hub API 小节
- **THEN** 上述四个端点均已列出且参数/响应描述与实现路由一致,页面不再把
  Hub metrics 描述为仅有 Prometheus text exposition

### Requirement: console 文档 SHALL 覆盖节点维护操作与 Audit 视图
`docs/docs/operate/control-plane/console.md`(及 zh-Hans 镜像)SHALL 记录:
概览页的节点 Drain/Maintain/Resume 操作(含各操作触发什么、何时使用、
Drain/Maintain 属于影响调度的操作)、节点维护态徽标的含义,以及 Views 表
中的 Audit 视图行(审计页展示 fleet 级操作记录)。

#### Scenario: 破坏性节点操作有文档说明
- **WHEN** 阅读者查看 console 页的概览/节点操作小节
- **THEN** 页面解释 Drain、Maintain、Resume 三个操作的作用与后果,维护
  徽标的判定来源,以及 Audit 视图在 Views 表中有一行描述

### Requirement: 取消安全契约 SHALL 有用户文档
输入的取消安全契约 SHALL 出现在用户文档(en 与 zh-Hans 均须覆盖)。语义:read()
被取消时不丢已解码消息,解码在 producer 侧完成后才可被取消边界截断(权威
语义见 `openspec/specs/input-cancellation-safety/spec.md`)。载体:concepts
(或等价的语义说明页)有取消安全小节,取消敏感的 channel 型 input 页
(mqtt、nats、redis、websocket、pulsar、http)说明或链接到该契约。

#### Scenario: 取消敏感 input 页可发现取消语义
- **WHEN** 阅读者查看上述任一 channel 型 input 的组件页
- **THEN** 页面本身或其链接目标说明:取消 read 时,已解码/已接收的消息
  不会丢失,取消在解码边界生效

### Requirement: sql processor 页 SHALL 说明计划缓存与连接池语义
`docs/docs/components/2-processors/sql.md`(及 zh-Hans 镜像)SHALL 记录:
SQL 计划被缓存(同一 query 文本复用编译计划)、`now()`/`current_date`
等时间函数在每批上新鲜求值(缓存不冻结时间函数结果)、以及连接池/
context_pool 的并发语义(多 worker 并发查询共享池)。

#### Scenario: 性能与时间函数语义可发现
- **WHEN** 阅读者查看 sql processor 页
- **THEN** 页面说明计划缓存行为与时间函数每批求值的保证,且不与实现矛盾

### Requirement: deploy 页 SHALL 说明 console token 的构建注入方式
`docs/docs/operate/control-plane/deploy.md`(及 zh-Hans 镜像)SHALL 说明
console 生产镜像的 token 注入方式:`--build-arg VITE_API_TOKEN=...` 构建参数,
`.env*` 文件被排除在构建上下文之外,以及生产环境建议改用 OIDC 认证。

#### Scenario: token 构建流程可跟随
- **WHEN** 运维者按 deploy 页构建 console 生产镜像
- **THEN** 页面给出的注入方式与 console/Dockerfile、.dockerignore 的实际
  行为一致,且提示 token 会内联进前端 bundle、生产建议 OIDC

### Requirement: memory buffer 页 SHALL 说明容量与合并失败边界
`docs/docs/components/1-buffers/memory.md`(及 zh-Hans 镜像)SHALL 记录:
capacity 为 0 的构建会被拒绝,以及 merge 失败时队列保留原批、下次重试。

#### Scenario: 边界行为可发现
- **WHEN** 阅读者查看 memory buffer 页
- **THEN** 页面说明零容量拒绝与 merge 失败保留重试两种边界行为

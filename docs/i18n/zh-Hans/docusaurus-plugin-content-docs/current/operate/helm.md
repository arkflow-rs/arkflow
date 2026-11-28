---
description: 使用官方 Helm Chart 安装 ArkFlow。
---

# Helm Chart

官方 Chart 从 CI 发布到 ghcr.io 的镜像安装 ArkFlow 引擎。它渲染的是
[Kubernetes](./kubernetes.md) 一章描述的原始清单,并把安全默认值直接内置:
单副本、`Recreate` 更新策略、接好线的探针,以及透传式配置。

## 安装

```bash
helm install my-arkflow oci://ghcr.io/arkflow-rs/charts/arkflow \
  --values my-values.yaml
```

Chart 的 `version` 独立演进;`appVersion` 跟随引擎发布标签,因此
`helm upgrade` 通过镜像标签决定引擎版本。

## 模式

| `mode` | 渲染内容 | 适用场景 |
|---|---|---|
| `standalone`(默认) | 单副本 Deployment(`Recreate`)、ClusterIP Service(HTTP) | 自包含流水线 |
| `agent` | 相同工作负载 + 数据面端口、headless Service | 接入控制面 Hub |
| `control-plane` | Hub + Console + 共享凭据(+ 可选的 chart 内 agent) | 一个 release 装下分布式运行时 |

## 透传式配置

完整的引擎配置文档通过 `config` 值提供,逐字节渲染进挂载于
`/app/etc/config.yaml` 的 ConfigMap。引擎 schema 演进不需要改动 chart;
`arkflow --validate` 始终是配置合法性的判定权威。

```yaml validate=foreign reason="Helm values file"
mode: standalone

config: |
  health_check:
    address: "0.0.0.0:8080"
  streams:
    # ... full engine configuration document
```

:::warning 绑定可达地址

引擎的 `health_check.address` 默认为 `127.0.0.1:8080`,kubelet 探针无法
访问。在 chart 提供的配置中务必绑定 Pod 内可达的地址,例如
`0.0.0.0:8080`。

:::

## 密钥

Chart 永远不接收密钥值。在 `config` 中用引擎的 `${env:VAR}` 展开引用,
变量值由 Kubernetes Secret 注入:

```yaml validate=foreign reason="Helm values file"
config: |
  health_check:
    address: "0.0.0.0:8080"
  outputs:
    - type: kafka
      brokers: ["${env:KAFKA_BROKERS}"]

env:
  - name: KAFKA_BROKERS
    valueFrom:
      secretKeyRef:
        name: my-release-secrets
        key: brokers
```

## 单副本语义

standalone 模式严格运行一个副本,更新采用 `Recreate` 策略:引擎是有状态的,
两个实例会重复消费同一批数据源。因此 `helm upgrade` 不会让新旧 Pod 同时
运行。横向扩展是控制面的职责,不是 standalone 发布的职责。存在
`unsafe.allowMultipleReplicas` 显式开关,供确认数据源已做分区分配的运维者
使用;对分区不感知的数据源不受支持。

## Agent 模式

`mode: agent` 让引擎接入控制面 Hub:

```yaml validate=foreign reason="Helm values file"
mode: agent

agent:
  dataPort: 9090
  tls:
    existingSecret: arkflow-agent-tls

persistence:
  enabled: true

config: |
  health_check:
    address: "0.0.0.0:8080"
    hub_urls: ["https://arkflow-hub:8080"]
    # node_id omitted: defaults to the pod name (ARKFLOW_NODE_ID)

env:
  - name: ARKFLOW_NODE_TOKEN
    valueFrom:
      secretKeyRef:
        name: arkflow-fleet
        key: node_token
```

- 未设置配置中的 `node_id` 且 `agent.nodeId` 为空时,节点身份默认通过 downward
  API 取 Pod 名称;也可通过 `agent.nodeId` 固定 `ARKFLOW_NODE_ID`。
- 通过 Secret 注入 bootstrap 注册凭据 `ARKFLOW_NODE_TOKEN`;开启
  `persistence.enabled: true` 后,Chart 自动挂载 `ARKFLOW_NODE_CREDENTIAL_FILE`
  (`/app/data/node_credential`),使 Hub 签发的 `arkn_` 逐节点凭据跨重启保留。
  由于 `arkn_` 凭据严格绑定首次注册时的 `node_id`,若在 Deployment Pod 替换后
  `metadata.name` 发生变化并继续复用旧 `node_credential`,Hub 会拒绝
  (`node_identity_mismatch`)——请固定 `agent.nodeId`(单节点)、改用
  `StatefulSet` 保持稳定 Pod 名称,或在允许 bootstrap 时清理旧的 `node_credential`
  文件再以新 `node_id` 启动。
- 设置 `agent.tls.existingSecret`(`ca.crt`、`tls.crt`、`tls.key`)挂载用于
  Hub mTLS(`ARKFLOW_AGENT_TLS_*`)与跨节点 shuffle(`ARKFLOW_DATA_PLANE_TLS_*`)
  的证书材料。未配置 TLS 或数据面秘密时引擎会拒绝 split shuffle——参见
  [TLS 矩阵](./tls-matrix.md)。

## 控制面模式

`mode: control-plane` 在一个 release 中渲染完整的分布式运行时:Hub
(`arkflow-server` 镜像)、Console(同源 nginx 反代后的静态 UI)、bootstrap
节点凭据 Secret、Fleet CA + Hub/Agent mTLS 证书,以及可选的接入 chart 内 Hub
的 agent。

```yaml validate=foreign reason="Helm values file"
mode: control-plane

controlPlane:
  # nodeToken unset => the chart generates and KEEPS a Secret across
  # upgrades and uninstalls (delete it manually when certain).
  # tls.enabled defaults to true (mTLS for Hub, Agent, and data plane).
  hub:
    storage:
      # Default: SQLite on the persistence PVC. A postgres:// URL switches
      # the Hub to Postgres (HA prerequisite) and skips the PVC.
      sqlitePath: /var/lib/arkflow/hub.sqlite
  console:
    service:
      type: ClusterIP
  agent:
    enabled: true
    replicas: 1

config: |
  health_check:
    address: "0.0.0.0:8080"
    hub_urls: ["https://<release-name>-hub:8080"]
```

存储、mTLS 与可用性契约:

- **默认存储**:SQLite 位于 `controlPlane.hub.storage.sqlitePath`,落在持久化
  PVC 上;Hub Deployment 单副本、`Recreate` 更新(绝不让两个 Hub 同时面对
  一个存储)。
- **Postgres**:设置 `controlPlane.hub.storage.postgresURL`(允许 env
  `${env:...}` 引用)即跳过 PVC;Hub HA 租约选举还需要
  `controlPlane.hub.ha.enabled` 与 Postgres 存储。
- **后端之间的迁移**保持为手动的 `arkflow-server migrate` 操作(先停 Hub)
  ——见[控制面部署](./control-plane/deploy.md)。
- **默认开启 mTLS(`controlPlane.tls.enabled: true`)**:首次安装时 Chart 在
  `<release>-tls` 中生成并保留(`helm.sh/resource-policy: keep`)Fleet CA(10 年)、
  Hub 服务端证书及 Agent 客户端/数据面证书(叶证书有效期 365 天,只要 Secret 存在
  就会在升级时复用;生产环境建议通过 `controlPlane.tls.existingSecret` 接入
  `cert-manager` 等自动轮换证书,或在 365 天到期前先删除 `<release>-tls` Secret、执行
  `helm upgrade` 重新渲染生成 Secret(证书在 Helm 渲染阶段生成而非 Pod 启动时生成),
  再重启 Hub、Console 与 Agent Pod),为 Hub
  配置 `ARKFLOW_HUB_TLS_{CA,CERT,KEY}`、`ARKFLOW_HUB_TLS_CLIENT_AUTH=agent_required`
  与 `HTTPS` 探针(注意:Chart 会在渲染期拒绝 `clientAuth=required`,因为 kubelet
  探针与 Console 反代不携带客户端证书),将 Console 的 `ARKFLOW_HUB_UPSTREAM` 设为
  `https://` 并通过挂载的 Fleet CA 开启 `proxy_ssl_verify on`,同时仅为 chart 内
  Agent 挂载 CA 与 Agent 证书私钥(不挂载 `hub.key`)。设置
  `controlPlane.tls.enabled=false`(并把 `hub_urls` 改回 `http://`)可显式退回明文 HTTP。

共享 bootstrap 凭据同时供给 Hub 与 chart 内 agent 的 `ARKFLOW_NODE_TOKEN`;默认由
chart 在首次安装时生成进 Secret(`helm.sh/resource-policy: keep`),除非
`controlPlane.nodeToken.existingSecret` 指向你自己的 Secret。

Console 在容器启动时从 `ARKFLOW_HUB_UPSTREAM` 解析反代目标——启用 mTLS 时
chart 将其设为 `https://<release>-hub:8080`(并通过 `ARKFLOW_HUB_PROXY_SSL` 开启
CA 校验),关闭时为 `http://`;镜像默认值(`http://arkflow-hub:8080` 与空的
`ARKFLOW_HUB_PROXY_SSL`)保持 chart 之外独立运行 Console 的行为不变。

## 路线图

本 Chart 是 Kubernetes 的交付面:standalone/agent 引擎安装,以及上文的控制面
伞形安装。CRD/operator 被明确推迟,直到出现真实的 GitOps 需求信号——并且即使
构建,它也只做 CR 到 Hub API 的翻译,永不做调度决策。

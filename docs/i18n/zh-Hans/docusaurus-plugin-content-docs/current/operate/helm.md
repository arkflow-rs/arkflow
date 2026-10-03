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

config: |
  health_check:
    address: "0.0.0.0:8080"
    hub_urls: ["http://arkflow-hub:8080"]
    # node_id omitted: defaults to the pod name (ARKFLOW_NODE_ID)

env:
  - name: ARKFLOW_NODE_TOKEN
    valueFrom:
      secretKeyRef:
        name: arkflow-fleet
        key: node_token
```

- 节点身份默认通过 downward API 取 Pod 名称;配置未设置 `node_id` 时,
  引擎原生读取 `ARKFLOW_NODE_ID`。
- 注册凭据以 `ARKFLOW_NODE_TOKEN` 从 Secret 注入。
- 数据面 shuffle 端口通过 headless Service 暴露。引擎在没有舰队凭据时
  拒绝服务数据面——依赖 split placement 前先阅读
  [TLS 矩阵](./tls-matrix.md)。

## 控制面模式

`mode: control-plane` 在一个 release 中渲染完整的分布式运行时:Hub
(`arkflow-server` 镜像)、Console(同源 nginx 反代后的静态 UI)、共享节点凭据
Secret,以及可选的接入 chart 内 Hub 的 agent。

```yaml validate=foreign reason="Helm values file"
mode: control-plane

controlPlane:
  # nodeToken unset => the chart generates and KEEPS a Secret across
  # upgrades and uninstalls (delete it manually when certain).
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
    hub_urls: ["http://<release-name>-hub:8080"]
```

存储与可用性契约:

- **默认**:SQLite 位于 `controlPlane.hub.storage.sqlitePath`,落在持久化
  PVC 上;Hub Deployment 单副本、`Recreate` 更新(绝不让两个 Hub 同时面对
  一个存储)。
- **Postgres**:设置 `controlPlane.hub.storage.postgresURL`(允许 env
  `${env:...}` 引用)即跳过 PVC;Hub HA 租约选举还需要
  `controlPlane.hub.ha.enabled` 与 Postgres 存储。
- **后端之间的迁移**保持为手动的 `arkflow-server migrate` 操作(先停 Hub)
  ——见[控制面部署](./control-plane/deploy.md)。

共享节点凭据同时供给 Hub 与 chart 内 agent 的 `ARKFLOW_NODE_TOKEN`;默认由
chart 在首次安装时生成进 Secret(`helm.sh/resource-policy: keep`),除非
`controlPlane.nodeToken.existingSecret` 指向你自己的 Secret。

Console 在容器启动时从 `ARKFLOW_HUB_UPSTREAM` 解析反代目标——chart 将其设为
chart 内 Hub Service;镜像默认值(`http://arkflow-hub:8080`)保持 chart 之外
独立运行 Console 的行为不变。

## 路线图

本 Chart 是 Kubernetes 的交付面:standalone/agent 引擎安装,以及上文的控制面
伞形安装。CRD/operator 被明确推迟,直到出现真实的 GitOps 需求信号——并且即使
构建,它也只做 CR 到 Hub API 的翻译,永不做调度决策。

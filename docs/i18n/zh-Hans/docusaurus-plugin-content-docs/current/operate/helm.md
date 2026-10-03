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

## 路线图

本 Chart 是 Kubernetes 故事的第一层交付物:控制面(Hub + Console)的伞形
Chart 随后到来;CRD/operator 被明确推迟,直到出现真实的 GitOps 需求信号
——并且即使构建,它也只做 CR 到 Hub API 的翻译,永不做调度决策。

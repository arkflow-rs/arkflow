---
description: 以 Docker 容器方式运行 ArkFlow。
---

# Docker

仓库自带多阶段 [`docker/Dockerfile`](https://github.com/arkflow-rs/arkflow/blob/main/docker/Dockerfile):
用 `cargo-chef` 分层缓存构建 release 二进制,运行于 `debian:bookworm-slim` 之上。带 release 标签的构建由 CI 发布为容器镜像;下文介绍引擎的运行、状态持久化与自行构建镜像。

## 运行引擎

镜像入口为 `arkflow --config /app/etc/config.yaml`,因此把配置挂载到该路径即可满足单节点部署的全部需要:

```bash
docker run -d --name arkflow \
  -p 8080:8080 \
  -v $PWD/config.yaml:/app/etc/config.yaml:ro \
  -v arkflow-data:/app/data \
  ghcr.io/arkflow-rs/arkflow:0.5.0   # 固定使用已发布的 tag(或 digest);生产环境不要浮动 :latest
```

- **`-p 8080:8080`** 暴露 HTTP 健康服务(`/health`、`/readiness`、`/liveness`——见[运维概览](/zh-Hans/docs/operate/overview)),供容器健康检查使用。
- **`-v arkflow-data:/app/data`** 持久化默认相对路径在工作目录(`/app`)下写入的所有内容:WAL 目录、内嵌状态根目录与 `file://` 检查点存储。把 `durability.path`、`state.root` 与 `checkpoint.object_store_uri` 指向该卷之下的路径,重建容器后即走恢复而不是冷启动。

### 分布式作业

参与 `split` 放置的节点必须发布可路由的数据面:设置 `health_check.data_port` 与 `health_check.data_host`(对端拨号的局域网地址,而不是 `0.0.0.0`),并用 `-p <data_port>:<data_port>` 发布端口。未设置 `data_port` 的节点保持共置放置行为,也不宣告 `network_shuffle` 能力——参见[分布式 Job](/zh-Hans/docs/build/distributed-jobs)。

## 容器化之前先校验

镜像携带完整二进制,错误配置可以在进入编排之前被发现:

```bash
docker run --rm \
  -v $PWD/config.yaml:/app/etc/config.yaml:ro \
  ghcr.io/arkflow-rs/arkflow:0.5.0 \
  /app/arkflow --config /app/etc/config.yaml --validate
```

## 自行构建镜像

Dockerfile 需要完整的工具链头(clang、protobuf-compiler、OpenSSL、libcurl,以及 Python UDF 处理器所需的 python3-dev);`cargo-chef` 的 planner/builder 拆分把依赖重编译排除在每次变更的路径之外:

```bash
git clone https://github.com/arkflow-rs/arkflow.git
cd arkflow
docker build -f docker/Dockerfile -t arkflow:local .
```

:::note
release profile 使用 fat LTO;构建需要充足的磁盘与内存(BuildKit 缓存加上链接期优化)。CI 在构建前清理本地镜像缓存并添加 swap,也是出于同样的原因。
:::

## 控制面与控制台

Hub 及其控制台与引擎容器分别发布——它们的部署模型(二进制启动参数、SQLite/PostgreSQL 存储、静态控制台资产)见[控制面部署](/zh-Hans/docs/operate/control-plane/deploy)。

---
description: ArkFlow 文档页面。
---

# Kubernetes 部署指南

本文介绍如何在 Kubernetes 集群中部署 ArkFlow。

## 前提条件

- Kubernetes 集群(版本 >= 1.27)
- kubectl 命令行工具
- 已构建的 ArkFlow Docker 镜像

## 部署配置

### ConfigMap

首先,创建一个 ConfigMap 来存放 ArkFlow 配置文件:

```yaml validate=foreign reason="Kubernetes manifest"
apiVersion: v1
kind: ConfigMap
metadata:
  name: arkflow-config
data:
  config.yaml: |
    # Place your ArkFlow configuration here
    # The HTTP server (health + control API) must bind to an address
    # reachable by kubelet probes, e.g. 0.0.0.0:8080:
    # health_check:
    #   address: "0.0.0.0:8080"
```

### Deployment

```yaml validate=foreign reason="Kubernetes manifest"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: arkflow
  labels:
    app: arkflow
spec:
  replicas: 1
  selector:
    matchLabels:
      app: arkflow
  template:
    metadata:
      labels:
        app: arkflow
    spec:
      containers:
      - name: arkflow
        image: arkflow:latest  # Replace with your image address
        ports:
        - containerPort: 8080
          name: http
        env:
        - name: RUST_LOG
          value: "info"
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
          limits:
            cpu: "500m"
            memory: "512Mi"
        livenessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /readiness
            port: http
          initialDelaySeconds: 5
          periodSeconds: 5
        volumeMounts:
        - name: config
          mountPath: /app/etc
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: arkflow-config
```

### Service

```yaml validate=foreign reason="Kubernetes manifest"
apiVersion: v1
kind: Service
metadata:
  name: arkflow
spec:
  selector:
    app: arkflow
  ports:
  - port: 8080
    targetPort: 8080
  type: ClusterIP  # Can be changed to NodePort or LoadBalancer as needed
```

## 部署步骤

1. 创建配置文件

```bash
# 创建命名空间(可选)
kubectl create namespace arkflow

# 应用 ConfigMap
kubectl apply -f configmap.yaml
```

2. 部署应用

```bash
# 部署 Deployment
kubectl apply -f deployment.yaml

# 部署 Service
kubectl apply -f service.yaml
```

3. 验证部署

```bash
# 查看 Pod 状态
kubectl get pods -l app=arkflow

# 查看 Service 状态
kubectl get svc arkflow
```

## 配置说明

- **镜像配置**:在 Deployment 配置中,将 `image: arkflow:latest` 替换为你的实际镜像地址
- **环境变量**:可通过 env 字段配置环境变量,当前配置了 RUST_LOG=info
- **端口配置**:Service 默认暴露 8080 端口(ArkFlow HTTP 服务器的默认端口)。确保 `health_check.address` 绑定到 `0.0.0.0:8080`,以便 kubelet 探针可以访问。
- **配置文件**:通过 ConfigMap 挂载到容器的 /app/etc 目录
- **资源限制**:设置了默认的资源请求与上限,以防止资源争用
- **健康检查**:配置了存活与就绪探针,确保对应用健康状态的正常监控

## 注意事项

1. 确保 ConfigMap 中的配置文件格式正确
2. 根据实际需要调整副本数
3. 根据你的环境选择合适的 Service 类型
4. 根据应用的实际资源消耗调整资源限制
5. 修改健康检查端点,使其与应用的实际健康检查端点相匹配

## 持久化存储(可选)

如果 ArkFlow 部署需要持久化存储,可以添加一个 PersistentVolumeClaim:

```yaml validate=foreign reason="Kubernetes manifest"
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: arkflow-data
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```

然后更新 Deployment 以使用这个 PVC:

```yaml validate=foreign reason="Kubernetes manifest fragment"
# Add to the volumes section
volumes:
- name: data
  persistentVolumeClaim:
    claimName: arkflow-data

# Add to the volumeMounts section of your container
volumeMounts:
- name: data
  mountPath: /app/data
```

## 故障排查

如果部署后服务无法正常运行,可以用以下命令排查问题:

```bash
# 查看 Pod 日志
kubectl logs -l app=arkflow

# 查看 Pod 详细信息
kubectl describe pod -l app=arkflow
```

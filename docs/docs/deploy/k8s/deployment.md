# Kubernetes Deployment Guide

This document describes how to deploy ArkFlow in a Kubernetes cluster.

## Prerequisites

- Kubernetes cluster (version >= 1.16)
- kubectl command-line tool
- Built ArkFlow Docker image

## Deployment Configuration

### ConfigMap

First, create a ConfigMap to store the ArkFlow configuration file:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: arkflow-config
data:
  config.toml: |
    # Place your ArkFlow configuration here
    # You can copy the configuration content from examples/config.toml
```

### Deployment

```yaml
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
        - containerPort: 8000
        env:
        - name: RUST_LOG
          value: "info"
        volumeMounts:
        - name: config
          mountPath: /app/examples
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: arkflow-config
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: arkflow
spec:
  selector:
    app: arkflow
  ports:
  - port: 8000
    targetPort: 8000
  type: ClusterIP  # Can be changed to NodePort or LoadBalancer as needed
```

## Deployment Steps

1. Create Configuration Files

```bash
# Create namespace (optional)
kubectl create namespace arkflow

# Apply ConfigMap
kubectl apply -f configmap.yaml
```

2. Deploy Application

```bash
# Deploy Deployment
kubectl apply -f deployment.yaml

# Deploy Service
kubectl apply -f service.yaml
```

3. Verify Deployment

```bash
# Check Pod status
kubectl get pods -l app=arkflow

# Check service status
kubectl get svc arkflow
```

## Configuration Details

- **Image Configuration**: In the Deployment configuration, replace `image: arkflow:latest` with your actual image address
- **Environment Variables**: Environment variables can be configured through the env field, currently configured with RUST_LOG=info
- **Port Configuration**: Service exposes port 8000 by default
- **Configuration File**: Mounted to the container's /app/examples directory via ConfigMap

## Important Notes

1. Ensure the configuration file format in ConfigMap is correct
2. Adjust the number of replicas according to actual needs
3. Choose appropriate Service type based on your environment
4. Recommended to use resource limits to manage container resource usage

## Troubleshooting

If the service fails to run properly after deployment, you can check the issues using these commands:

```bash
# View Pod logs
kubectl logs -l app=arkflow

# View detailed Pod information
kubectl describe pod -l app=arkflow
```
# 远端配置管理功能

## 概述

远端配置管理功能允许ArkFlow流处理引擎自动从远端API接口拉取配置信息，并动态地创建、修改、删除和启停流处理管道。这个功能特别适用于需要集中管理多个流处理实例的场景。

## 功能特性

- **自动配置拉取**: 定期从远端API获取最新的管道配置
- **动态管道管理**: 支持运行时创建、更新、停止和删除管道
- **版本控制**: 基于配置版本进行变更检测，避免不必要的重启
- **认证支持**: 支持Bearer Token认证
- **健康检查**: 保持原有的健康检查功能
- **错误处理**: 优雅处理网络错误和配置错误

## 使用方法

### 命令行参数

```bash
# 使用远端配置启动
arkflow --remote-config-url "http://api.example.com/pipelines" \
        --remote-config-interval 30 \
        --remote-config-token "your-auth-token"

# 参数说明:
# --remote-config-url: 远端配置API的URL地址
# --remote-config-interval: 轮询间隔（秒），默认30秒
# --remote-config-token: 认证令牌（可选）
```

### 远端API接口规范

#### 请求格式

```
GET /pipelines
Authorization: Bearer <token>  # 如果提供了token
Content-Type: application/json
```

#### 响应格式

```json
{
  "version": "v1.0.0",
  "pipelines": [
    {
      "id": "pipeline-001",
      "name": "数据处理管道",
      "status": "active",
      "version": "1.0.0",
      "config": {
        "input": {
          "type": "generate",
          "config": {
            "interval": "1s",
            "count": 100,
            "mapping": "root.message = 'Hello World'"
          }
        },
        "pipeline": {
          "thread_num": 1,
          "processors": [
            {
              "type": "json",
              "config": {
                "operator": "select",
                "mapping": "root.processed_at = now()"
              }
            }
          ]
        },
        "output": {
          "type": "stdout",
          "config": {}
        }
      }
    }
  ]
}
```

#### 字段说明

- `version`: 配置版本号，用于变更检测
- `pipelines`: 管道配置数组
  - `id`: 管道唯一标识符
  - `name`: 管道名称
  - `status`: 管道状态
    - `active`: 激活状态，管道将运行
    - `inactive`: 非激活状态，管道将停止
    - `deleted`: 删除状态，管道将被移除
  - `version`: 管道配置版本
  - `config`: 标准的ArkFlow流配置

## 管道生命周期管理

### 创建管道

当远端API返回新的管道配置时，引擎会自动创建并启动该管道。

### 更新管道

当管道的`version`字段发生变化时，引擎会：
1. 停止当前运行的管道
2. 使用新配置重新创建管道
3. 启动新管道

### 停止管道

当管道状态变为`inactive`时，引擎会停止该管道但保留其配置。

### 删除管道

当管道状态变为`deleted`或从配置中移除时，引擎会停止并删除该管道。

## 配置示例

### 本地配置文件

当使用远端配置时，本地配置文件主要用于设置日志和健康检查：

```yaml
logging:
  level: info
  format: plain

health_check:
  enabled: true
  address: "0.0.0.0:8080"

# 远端配置模式下，streams数组可以为空
streams: []
```

### 远端API实现示例

```python
# Flask示例
from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/pipelines')
def get_pipelines():
    # 验证认证令牌
    auth_header = request.headers.get('Authorization')
    if auth_header:
        token = auth_header.replace('Bearer ', '')
        # 验证token逻辑
    
    # 返回管道配置
    return jsonify({
        "version": "v1.0.0",
        "pipelines": [
            # 管道配置...
        ]
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
```

## 监控和日志

引擎会记录以下关键事件：

- 配置拉取成功/失败
- 管道创建、更新、停止、删除
- 网络错误和重试
- 配置解析错误

示例日志输出：

```
2024-01-15T10:30:00Z INFO Starting remote configuration manager
2024-01-15T10:30:00Z INFO Polling interval: 30 seconds
2024-01-15T10:30:00Z INFO API endpoint: http://api.example.com/pipelines
2024-01-15T10:30:30Z INFO Configuration changed, updating pipelines (version: v1.0.1)
2024-01-15T10:30:30Z INFO Starting new pipeline 'Data Processing Pipeline'
2024-01-15T10:30:30Z INFO Pipeline 'Data Processing Pipeline' started successfully
```

## 错误处理

### 网络错误

当无法连接到远端API时，引擎会：
- 记录错误日志
- 继续运行现有管道
- 在下一个轮询周期重试

### 配置错误

当远端配置格式错误时，引擎会：
- 记录详细错误信息
- 保持现有管道运行
- 跳过本次更新

### 管道启动失败

当管道配置无效导致启动失败时，引擎会：
- 记录错误详情
- 继续处理其他管道
- 不影响已运行的管道

## 最佳实践

1. **版本管理**: 确保每次配置变更都更新版本号
2. **渐进式部署**: 先在测试环境验证配置，再推送到生产环境
3. **监控告警**: 监控API可用性和管道健康状态
4. **备份策略**: 保留配置历史版本以便回滚
5. **认证安全**: 使用强认证令牌并定期轮换

## 故障排除

### 常见问题

1. **管道无法启动**
   - 检查配置格式是否正确
   - 验证输入/输出源的连接性
   - 查看详细错误日志

2. **配置未更新**
   - 确认API返回了新的版本号
   - 检查网络连接
   - 验证认证令牌

3. **性能问题**
   - 调整轮询间隔
   - 优化管道配置
   - 监控资源使用情况

### 调试技巧

- 设置日志级别为`debug`获取详细信息
- 使用健康检查端点监控状态
- 检查管道指标和性能数据
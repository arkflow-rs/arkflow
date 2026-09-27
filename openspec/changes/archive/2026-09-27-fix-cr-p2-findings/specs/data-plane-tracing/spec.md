## ADDED Requirements

### Requirement: OTel 导出层级别过滤与关停冲刷

追踪启用时，OTel span 导出层 SHALL 应用与日志层相同的级别过滤配置：低于配置级别的 span SHALL 不进入导出管线（依赖库产生的 DEBUG/TRACE span 不因启用追踪而被全量导出）。引擎优雅关停时 SHALL 关闭 OTel `TracerProvider`，批量导出器中缓冲的 span SHALL 在进程退出前冲刷到 `endpoint`；关停时的导出失败 SHALL 记录警告且不影响退出码。

#### Scenario: 级别过滤作用于导出层

- **WHEN** 配置日志级别为 `info` 且启用追踪，依赖库产生 DEBUG 级 span
- **THEN** DEBUG span 不被导出到 OTLP endpoint，`info` 及以上 span 正常导出

#### Scenario: 优雅关停冲刷缓冲 span

- **WHEN** 引擎优雅关停时批量导出器中仍有未发送的 span
- **THEN** provider 关闭流程将这些 span 冲刷到 endpoint 后进程才退出

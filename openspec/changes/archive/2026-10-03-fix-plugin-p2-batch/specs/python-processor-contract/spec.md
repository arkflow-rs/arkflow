# python-processor-contract 增量

## ADDED Requirements

### Requirement: sys.path 装配 SHALL 无 panic、按配置顺序且跨实例去重
python processor 构建 `sys.path` 时 SHALL NOT 使用可 panic 的调用：`PyList::insert` 的结果 SHALL 被检查，失败时返回配置错误而非 panic。配置的 `python_path` 列表 SHALL 按配置顺序生效（列表靠前优先级高），不再倒置；工作目录 `"."` SHALL 在用户路径之后。多次构建 processor 实例 SHALL NOT 向进程级 `sys.path` 重复追加同一路径（插入前查重）。

#### Scenario: 路径按配置顺序生效
- **WHEN** 配置 `python_path: ["/a", "/b"]`
- **THEN** 解析顺序为 `/a` 优先于 `/b`，`/a`/`/b` 均优先于 `"."`

#### Scenario: 多实例不重复污染 sys.path
- **WHEN** 同一进程构建两个含相同 `python_path` 的 python processor
- **THEN** `sys.path` 中每个配置路径至多出现一次

#### Scenario: 装配失败返回错误而非 panic
- **WHEN** `sys.path` 插入操作返回 `Err`
- **THEN** processor 构建返回配置错误，进程不 panic

### Requirement: UDF 调用 SHALL 有超时上界
python processor SHALL 对 UDF 调用施加超时：配置 `timeout_ms`（缺省 60000）到期时 `process()` SHALL 返回错误（含已耗时信息），不得无限阻塞流。超时放弃等待后，底层阻塞线程在 UDF 自然返回前持续占用的事实 SHALL 在文档中明示。

#### Scenario: 死循环 UDF 不挂死流
- **WHEN** 一个 UDF 含无限循环且未显式配置 `timeout_ms`
- **THEN** 该批次在缺省超时到期后返回错误，流不永久阻塞

#### Scenario: 正常 UDF 不受影响
- **WHEN** UDF 执行耗时远小于配置的 `timeout_ms`
- **THEN** 处理正常返回，无额外延迟或错误

## ADDED Requirements

### Requirement: embedding 输出列构建 SHALL 对向量维度不一致显式失败
`embedding` processor 在把向量集合构建为 `FixedSizeList` 列前 SHALL 校验全部向量维度一致（以首向量为准），不一致时 SHALL 返回指名两个维度值的显式错误，MUST NOT panic、MUST NOT 静默错位写列。该校验 SHALL 覆盖跨 API 响应拼接的向量来源，而不限于单响应内部。

#### Scenario: 跨响应维度不一致报错不 panic
- **WHEN** 向量集合中存在维度与首向量不同的向量（如 128 与 64）
- **THEN** 处理器返回显式错误且错误信息同时包含两个维度值，进程不 panic

#### Scenario: 一致向量集行为不变
- **WHEN** 向量集合全部维度一致
- **THEN** 列构建行为与既有版本一致

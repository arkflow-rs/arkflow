## ADDED Requirements

### Requirement: Avro 解码的批内 reader 复用 SHALL 保持逐消息语义

`schema_registry` codec 对 Avro payload 的解码 SHALL 在批内按 schema id 复用 reader（每批每 id 构建一次 `GenericDatumReader`，`read_value` 跨消息复用），且复用 SHALL NOT 改变解码输出、分组行为或错误顺序：任何一条消息的处理（wire-format 解析 → schema 解析 → payload 解码 → 形状检查）失败 SHALL 在与其后任何消息被触碰之前中止整个 decode。

#### Scenario: 同 id 多消息只构建一次 reader 且输出等价

- **WHEN** 一个 batch 含同一 schema id 的连续多条消息
- **THEN** reader 按 id 构建一次并跨消息复用（名字表每批每 id 一次），产出的多行批与"逐消息构建 reader"的结果逐字节等价（列序、nullable、值）

#### Scenario: 交织 id 不破坏复用与分组

- **WHEN** 一个 batch 的消息按 id 1、2、1、2 交织
- **THEN** 两个 id 各自复用各自的 reader，行按 schema id 首现顺序分组、组内保持消息序，与既有交织分组语义一致

#### Scenario: 逐消息错误顺序保持——fetch 先于后续 wire 错误

- **WHEN** 消息 0 的 schema id 无法解析且消息 1 的 wire header 损坏
- **THEN** 返回消息 0 的 schema 解析错误（消息 1 的 wire-format 解析先不被触碰）

#### Scenario: 逐消息错误顺序保持——payload 解码先于形状检查

- **WHEN** 某 schema 含不支持的嵌套字段且其消息 payload 本身解码失败
- **THEN** 返回 payload 解码错误（accumulator 的 schema 形状检查在首次成功 read 之后才发生）

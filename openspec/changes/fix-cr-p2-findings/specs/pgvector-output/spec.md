## MODIFIED Requirements

### Requirement: SQL 生成与 upsert 语义

INSERT 语句 SHALL 按参数化绑定生成（id/vector/payload 各占一个绑定），并 SHALL 按行分块使单条语句的绑定参数总数不超过 Postgres 的 65,535 上限（按当前列组合计算每行绑定数，块内行数取不超过上限的最大值）；各块顺序执行，任一块失败 SHALL 停止并返回错误。SQL 标识符（表名与列名）SHALL 以双引号引用且将标识符内的 `"` 转义为 `""`，含引号的标识符 SHALL NOT 破坏语句结构或改变目标对象。

#### Scenario: 大批量按绑定上限分块

- **WHEN** 配置 id_field 与 payload_field（每行 3 个绑定）且输入 batch 含 30,000 行
- **THEN** 生成的每条 INSERT 语句绑定数不超过 65,535，语句按行序顺序执行，全部成功时输出成功返回

#### Scenario: 单小批仍为单语句

- **WHEN** 输入 batch 含 2 行且配置 id_field 与 payload_field
- **THEN** 单条 INSERT 语句含 6 个绑定参数

#### Scenario: 标识符内嵌引号被转义

- **WHEN** 配置 `table: odd"table`、`vector_field: em"bedding`
- **THEN** 生成 SQL 中标识符以 `"odd""table"`、`"em""bedding"` 形式引用，语句可被 Postgres 解析为对应对象

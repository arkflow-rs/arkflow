## MODIFIED Requirements

### Requirement: 多版本 schema 解码
codec SHALL 支持同一流中不同 schema id（不同 schema 版本）的消息，各自用对应版本的 descriptor 解码。同 id 的消息 SHALL 列式累积为一个多行批次：行内容、列序、列类型均与逐消息解码后合并的结果一致；列 nullable 一致地按 writer schema（此前多消息批经并集合并会丢失非空标记，而单消息批保留——本变更消除该不一致）。同一 batch 内不同版本解码出的批次 schema 不一致（真实 schema 演进，如新增/缺失字段）时，codec SHALL 先把各分组批次归一到并集 schema（缺失列以 null 填充、列序取首个 schema id 分组内首条消息的字段顺序并追加后续分组的新列；行按 schema id 分组排列——组内保持消息顺序、组间按首次出现）再合并；同名同列类型冲突（无法 null 填充消解）时 SHALL 返回指明列名与两个类型的错误。

#### Scenario: 同 batch 多版本
- **WHEN** 一个 batch 含 schema id=1 与 schema id=2 的消息（两版 schema）
- **THEN** 各自用对应 descriptor 解码，不互相干扰

#### Scenario: 单版本批列式累积等价
- **WHEN** 一个 batch 的全部消息共享同一 schema id
- **THEN** 产出一个多行批次，字段顺序为该 schema 的声明顺序，行内容、列类型与"逐消息解码再合并"的既有结果一致；列 nullable 按 writer schema（非空列不再因合并被放宽为 nullable）

#### Scenario: 真实 schema 演进合并
- **WHEN** 一个 batch 混有 id=1（少一列）与 id=2（多一列）的消息，两版 schema 文本不同
- **THEN** 产出一个并集 schema 的批次，id=1 消息的新增列为 null，合并不报错

#### Scenario: 同名不同类型报错
- **WHEN** 两版 schema 中同名列类型不同（如 Utf8 与 Int64）
- **THEN** 解码返回错误，错误信息含列名与两个类型

# Proposal: fix-migrate-self-reference-ordering

## Why

CR 发现迁移工具的自引用外键行序缺陷：PG DDL 中 cp_config_versions.parent_version_id 与 cp_intents.superseded_by_intent_id 是表内自引用；迁移按自然行序插入，子行先于父行即 FK 违规、迁移中途失败。设计文档声称「自引用先于引用者」，实现只排了表间序；门控迁移测试只灌无父链数据。

## What Changes

两张自引用表拷贝前按父链排序（order_parents_first）：父行（含 NULL 根）先行，每轮发射所有不再被阻塞的行；悬空引用不阻塞、保持原序，由 PG 拷贝如实报错。

## Capabilities

### New Capabilities
（无）

### Modified Capabilities

- `hub-ha`: 迁移需求补充自引用表的父母先行行序。

## Impact

- `crates/arkflow-server/src/storage/migrate_tool.rs` + 单测。

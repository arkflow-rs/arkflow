## Context

四个独立小修复，合一个 change 交付。hub_problem 的映射不一致已在审查中核实；分页溢出与 ${file:} 沙箱是防御性修复；Pipeline 删除是 API 面清理。

## Decisions

**D1 — 映射用枚举臂而非字符串匹配。** hub_problem 的 match 加 `StorageUnavailable | Storage(_)` 和 `GenerationConflict` 臂——与既有 StaleLeader 臂同级。

**D2 — 饱和算术对齐 helper。** `page_items` helper 已用 `saturating_sub/saturating_mul`——5 处副本机械对齐。

**D3 — ${file:} 沙箱取最小面。** 拒绝 `..` 组件 + 要求绝对路径。不实现白名单（配置面复杂度）；错误信息固定文案（不回显文件路径或内容）。

**D4 — Pipeline 整模块删除。** 死代码 + 语义矛盾，删除优于 deprecated（用户量=0）。

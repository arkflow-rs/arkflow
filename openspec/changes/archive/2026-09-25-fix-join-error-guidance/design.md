# Design: fix-join-error-guidance

## Context

双流 join 未实现，但两处拒绝报错互相矛盾：Stream 编译器说「去用 Job DAG 的 join 算子」，Job 校验说「Join 算子不支持」。根因是 `stream-config-compilation` 规格要求「migration message pointing to Job DAG configuration」，实现照做，而 Job 侧的拒绝早于任何 join 运行时存在。

## Goals / Non-Goals

- Goals：统一两处报错为同一诚实边界（stream-stream join not yet supported）；给出可行替代；文档显式声明；规格措辞与实现一致。
- Non-Goals：实现 join 本身（`add-stream-join-operator` 承载）；改变任何拒绝**行为**（仍然拒绝，只是文案）；i18n。

## Decisions

1. **单一事实短语**：两处报错都包含 `stream-stream join is not yet supported`，后续 join 落地时以该短语全局搜索即可找到全部触点（含测试与文档）。
2. **替代方案清单收敛为两条**：SQL processor 对临时表的批内 join（引擎内、立即可用）；外部共置（如 Kafka 把两个流重分区到同 key 再单流处理）。不提「Job DAG join operator」。
3. **规格先行修正**：`stream-config-compilation` 的 MODIFIED delta 把「migration message」措辞改为「honest rejection message」，防止文案回退；`streaming-job-api` 新增 ADDED 需求固定「两条拒绝路径不得互相矛盾」。
4. **文档边界**：`docs/docs/concepts/7-distributed-jobs.md` 已有边界章节处补充 join 边界段落（en；该文件无 zh 副本则仅 en）。

## Risks / Trade-offs

- 文案变长：可接受，错误路径非热路径。
- 下游若有测试断言旧文案：本变更同步更新仓库内全部断言（`stream_compiler.rs` 与 `job.rs` 的内联测试）。

## Migration Plan

纯文案与文档变更，无配置/数据迁移。用户可见差异仅为报错文本更准确。

# Design: fix-migrate-self-reference-ordering

迭代发射而非全量拓扑：每轮扫描 pending，父引用为 NULL、已发射或悬空（目标不在本表键集）即发射；无进展即剩余保持原序落尾——环或深链由 PG 拷贝如实报错。键列按表特判（config_version_id / intent_id）。

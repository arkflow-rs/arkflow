## 1. 记录 ack 高水位（D1）

- [x] 1.1 在 `S3Store`（`crates/arkflow-plugin/src/wal/s3.rs:229`）新增 `acked_hwm: AtomicU64` 字段，并在 `build_with_client` 的构造体里初始化为 0。验证：`cargo build -p arkflow-plugin`。
- [x] 1.2 改写 `advance_cursor(seq)`（`s3.rs:643`）：用 `self.acked_hwm.fetch_max(seq, Ordering::AcqRel)` 替换 `let _ = seq;`，保留既有的 `cursor_pending` 阈值触发 `flush_manifest` 逻辑；去掉因此变成 unused 的 `let mut active`（消除 `s3.rs:647` 的 `unused_mut` 警告）。验证：`cargo build -p arkflow-plugin`。

## 2. flush_manifest 按 ack 高水位推进 cursor（D2）

- [x] 2.1 改 `flush_manifest`（`s3.rs:881`）的 manifest mutator：读取 `acked_hwm`，把 cursor 设为 `max(m.cursor, acked_hwm.min(m.max_sealed_seq))`，替换原先基于 `active.last_seq` 的推导。验证：`cargo build -p arkflow-plugin`。
- [x] 2.2 若 mutator 不再使用 `active.last_seq`，移除函数开头对 `active` 的读取与 `active_last` 局部变量。验证：`cargo clippy -p arkflow-plugin --lib`（无 unused 警告）。

## 3. next_seq_hint 基于已写最高序列号（D3）

- [x] 3.1 让 `recover()`（`s3.rs:481`）把已算出的 `max_seq_seen`（`s3.rs:534`）写入 store 的一个缓存字段（如新增 `max_written_seq: AtomicU64`），并在 `append_batch` 写入新条目时 `fetch_max` 更新该缓存（所有新 seq 的唯一入口，覆盖 active 与 sealed；seal 只搬运已见 seq，故无需在 seal 路径更新），使运行期与重启后都准确。验证：`cargo build -p arkflow-plugin`。
- [x] 3.2 改写 `next_seq_hint()`（`s3.rs:747`）：返回 `max_written_seq.max(active.last_seq) + 1`（对齐 redb 的 `max_seq()+1`），替换 `cursor().saturating_add(1)`。验证：`cargo build -p arkflow-plugin`。

## 4. 测试

- [x] 4.1 现有复现测试 `s3_cursor_does_not_track_acked_seq_after_restart` 转绿（`next_seq_hint()==6`、`read_after_cursor()` 为空）。验证：`cargo test -p arkflow-plugin --lib wal::s3::tests::s3_cursor_does_not_track_acked_seq_after_restart` 通过。
- [x] 4.2 新增回归测试「部分 ack 不复用序列号」：sealed 到 M、仅 ack 到 K<M，重启后断言 `next_seq_hint()==M+1`，且再 append 一条得到的 seq 不与 sealed 的 K+1..=M 冲突。验证：新测试通过。
- [x] 4.3 新增回归测试「cursor 不越过未 seal 数据」：ack 一条仍在 active（未 seal）的条目后 close+reopen，断言 cursor 未越过 `max_sealed_seq`、该条仍被 `read_after_cursor()` 返回（不丢）。验证：新测试通过。

## 5. 校验

- [x] 5.1 全量 WAL 测试绿：`cargo test -p arkflow-plugin --lib wal::`。验证：全部通过。
- [x] 5.2 `cargo clippy -p arkflow-plugin --lib` 无本次引入的新警告。验证：clippy 输出干净。
- [x] 5.3 `openspec validate fix-s3-wal-cursor-tracking --strict` 通过（proposal/design/specs/tasks 一致、spec scenario 格式正确）。验证：命令退出码 0。

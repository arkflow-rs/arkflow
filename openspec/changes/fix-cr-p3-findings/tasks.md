## 1. 向量后端请求正确性

- [x] 1.1 `vector_util::encode_path_segment`（RFC 3986 pchar 保留集，其余 `%XX`）；`vector_search` 与 `qdrant` 的集合名经编码拼 URL + 回归测试（常规名逐字节不变；`a/b?c d` → 编码输出）
- [x] 1.2 `qdrant::random_uuid_v4` 缩进修整

## 2. 处理器健壮性

- [x] 2.1 `embedding::embed_all` 分块以 `buffered(concurrency)` 流水化（`try_collect` 短路、按块序展平）+ 回归测试（多块请求并发上限与行序、默认 concurrency=1 退化为顺序）
- [x] 2.2 `pgvector_search` 行解码改 `Option<String>` id，NULL id 报指明列名的 `Error::Process` + 回归测试

## 3. 基准套件

- [x] 3.1 `setup_plugins` 传播初始化错误；`run_suite` 以 `Error::Config` 校验 `count`/`runs`/`warmup`（删除 `assert!`/`expect`）+ 回归测试（`runs = 0` 返回 Err 而非 panic）
- [x] 3.2 `state_backend` put/get 循环移入 `spawn_blocking`（测量口径不变）
- [x] 3.3 示例入口参数缺失/未知时输出用法并以退出码 2 结束（删除 `expect` panic）

## 4. 验证与收尾

- [x] 4.1 `cargo test --workspace --all-targets` 通过
- [x] 4.2 `cargo clippy --workspace --all-targets` 无新增告警（与 95 基线对比）
- [x] 4.3 `openspec validate fix-cr-p3-findings` 通过

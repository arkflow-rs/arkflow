# Tasks: pin-pump-cancel-semantics

## 1. 语义固定测试

- [x] 1.1 Ack 间谍 + wire 写失败路径：abort 非 ack、pump Err
- [x] 1.2 shutdown 取消路径：未决分支 abort、flush 先于 abort
- [x] 1.3 回执 Acked 正常路径：ack + pending 移除

## 2. 文档与收尾

- [x] 2.1 remote.rs 模块文档固化取消不变量
- [x] 2.2 全量验证（test×2 + clippy + docs:check）、归档、同步 spec、更新 PLANNING、推送

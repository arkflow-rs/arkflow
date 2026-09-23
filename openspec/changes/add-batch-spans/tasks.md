# Tasks: add-batch-spans

## 1. 实现

- [ ] 1.1 source chain read 路径添加 batch 级 span（rows/task 属性）
- [ ] 1.2 interior chain 处理路径添加 batch span
- [ ] 1.3 验证关闭追踪时零开销

## 2. 测试与文档

- [ ] 2.1 InMemorySpanExporter 测试：batch span 存在、属性正确、父子关系正确
- [ ] 2.2 文档更新 + 全量验证 + 归档

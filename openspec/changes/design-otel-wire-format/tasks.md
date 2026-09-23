# Tasks: design-otel-wire-format

## 1. 内核精读

- [ ] 1.1 精读 task.rs 的 source chain / interior chain / worker pool / dispatch_data 路径
- [ ] 1.2 识别 batch 进入/退出 pipeline 的关键节点和上下文传播挑战
- [ ] 1.3 评估 worker pool 队列项扩展的可行性和影响面

## 2. 设计产出

- [ ] 2.1 产出评审级设计文档：batch span 切面、pool 传播方案、跨节点传播方案、性能评估、分阶段实施计划
- [ ] 2.2 openspec validate 通过

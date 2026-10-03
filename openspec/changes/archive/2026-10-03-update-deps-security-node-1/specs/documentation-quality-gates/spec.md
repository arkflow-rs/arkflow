## ADDED Requirements

### Requirement: 文档站依赖刷新保持构建与门禁契约

docs 站的依赖刷新（范围内更新与 pnpm overrides 强推同 major 修复版）SHALL 保持既有构建与质量门禁契约：Docusaurus 双语言（en + zh-Hans）生产构建成功产出静态站点，`docs:check`（页面-清单双向归属、README 组件对照、内部锚点、sidebar 可达性、yaml 代码块分类）全部通过，示例校验工作区测试不受影响。overrides SHALL 仅覆盖与被钉版本同 major 的修复版，SHALL NOT 跨 major 覆盖传递依赖。

#### Scenario: 刷新后双语言构建成功

- **WHEN** 锁文件刷新（31 个安全告警包范围内更新 + lodash-es/qs overrides）后执行 Docusaurus 生产构建
- **THEN** en 与 zh-Hans 两个 locale 均产出完整静态站点，无构建错误

#### Scenario: 质量门禁不受依赖刷新影响

- **WHEN** 刷新后运行 `docs:check` 与示例校验相关测试
- **THEN** 全部通过，且无需对文档内容做任何适配性修改

#### Scenario: 同 major 滞留经 overrides 收敛

- **WHEN** 某告警包的修复版在同 major 内但被依赖方钉在旧 minor（如 lodash-es 4.17.x、qs 6.14.x）
- **THEN** 经 `pnpm.overrides` 强推后锁内只存在修复线版本（4.18.x / 6.16.x），且构建与门禁保持通过

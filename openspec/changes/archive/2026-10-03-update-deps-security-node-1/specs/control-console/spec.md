## ADDED Requirements

### Requirement: console 依赖升级保持构建与测试契约

console 的开发依赖升级与 lockfile 再生成 SHALL 保持既有构建与测试契约：`tsc -b && vite build` 成功、vitest 全部用例通过；lockfile 再生成 SHALL 仅改变 devDependencies 邻域的解析结果，SHALL NOT 引入新的运行时依赖（dependencies 面不变）。

#### Scenario: 升级后构建与测试全绿

- **WHEN** vitest 升至修复版（^4.1.11）并重新生成 lockfile 后执行 `npm run build` 与 `npm test`
- **THEN** TypeScript 编译与 Vite 构建成功，全部 vitest 用例通过

#### Scenario: 运行时依赖面不变

- **WHEN** 对比升级前后的 console lockfile
- **THEN** `dependencies`（运行时）声明的解析保持不变，差异仅出现在 devDependencies 及其传递闭包

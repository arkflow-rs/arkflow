# control-plane-identity Delta

## ADDED Requirements

### Requirement: Operator credential parsing SHALL fail closed
结构化 operator 凭据（`id|role|secret[|scopes]`）解析失败时（段数不足、未知角色、空 id 或空 secret）MUST 拒绝该凭据：启动期校验 SHALL 发现含分隔符但畸形的凭据并拒绝启动（错误信息脱敏，不回显完整密钥），请求期该凭据 MUST 视为不可匹配。不含分隔符的纯静态 token 模式 SHALL 保持既有行为（整串作为 Admin 密钥）。系统 MUST NOT 在解析失败时把整串配置静默当作 Admin 凭据。

#### Scenario: 畸形结构化凭据拒绝启动
- **WHEN** operator 凭据配置含 `|` 但角色名未知（如 `alice|typo|secret`）并启动 Hub
- **THEN** 启动失败并列出该畸形凭据（脱敏），该字符串不再构成任何有效凭据

#### Scenario: 纯静态 token 模式保持
- **WHEN** operator token 配置为不含 `|` 的普通字符串
- **THEN** 该字符串按既有语义作为 Admin 静态凭据参与鉴权

#### Scenario: 启动期校验覆盖凭据配置的唯一位置
- **WHEN** Hub 的 operator 凭据（单条静态字段 `operator_token`）为「含分隔符但解析失败」
- **THEN** 启动校验拒绝启动并列出该畸形凭据（脱敏），且不存在可绕过该校验的其他 operator 凭据配置位置

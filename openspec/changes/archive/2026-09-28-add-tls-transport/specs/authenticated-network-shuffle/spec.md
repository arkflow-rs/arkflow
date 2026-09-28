# authenticated-network-shuffle Delta

## ADDED Requirements

### Requirement: 数据面 SHALL 支持以舰队 CA 锚定的 mTLS

配置了 TLS 材料（节点证书、私钥、舰队 CA 三者齐备）时，数据面 SHALL 以 TLS 承载全部跨节点连接：入站连接先完成 TLS 握手——要求对端出示证书且链锚定舰队 CA——再进入既有 HMAC 会话握手；出站连接以同一 CA 校验服务端证书（ServerName 固定为 `arkflow-data-plane`，节点证书须含该 SAN）并出示自身证书。TLS 握手失败与 TCP 失败同预算（重连退避/fail-closed 不变）。未配置 TLS 时全部行为与现状逐字节一致；三份材料缺一 SHALL 以显式配置错误拒绝启动数据面。HMAC 会话握手与 TLS 并存：TLS 提供传输加密与"舰队 CA 签发证书"门禁，节点身份与作业/代数绑定仍由 HMAC 层认证。

#### Scenario: TLS 两端完成握手并传输帧

- **WHEN** 两个节点都以同一舰队 CA 签发的证书启用 TLS，一条远程边建立
- **THEN** TLS 与 HMAC 双层握手成功，数据帧照常往返，语义与明文路径一致

#### Scenario: 明文客户端连接 TLS 服务端被拒

- **WHEN** 一个未启用 TLS 的节点连接启用 TLS 的节点
- **THEN** 入站 TLS 握手失败，连接关闭，计入既有失败路径（不降级为明文）

#### Scenario: 非舰队 CA 签发的证书被拒

- **WHEN** 对端出示的证书链不锚定配置的舰队 CA
- **THEN** 握手失败，连接拒绝，无数据帧被接受

#### Scenario: 材料不齐拒绝启动

- **WHEN** 证书/私钥/CA 三份材料只配置了部分
- **THEN** Agent 启动以显式配置错误失败，数据面不绑定端口

#### Scenario: 未配置 TLS 行为不变

- **WHEN** 未配置任何 TLS 材料的部署升级
- **THEN** 数据面全部行为与升级前逐字节一致

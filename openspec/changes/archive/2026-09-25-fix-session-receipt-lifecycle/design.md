# Design: fix-session-receipt-lifecycle

same_channel 是本文件既有先例（inbound 注册表清理同款）。安装记录用 Vec<(key, sender)>——同一连接可为多会话装槽，重复帧路径不重复安装（registered_keys 去重）。干净 EOS 结束的会话路由保留到 job 移除/确认丢失才撤销，有界（随会话数），结构注释说明。

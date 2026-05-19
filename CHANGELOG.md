# 改动日志

## 2026-05-19

### 设备连接接管

- 当新连接上报的心跳设备标识已存在时，服务会删除旧连接、移除旧映射并关闭旧 socket。
- 设备重启导致 `device_id` 变化后，通过心跳设备标识发送命令会自动命中新连接。
- 统一抽出设备标识映射清理逻辑，断开连接和手动删除设备时都会避免留下旧映射。

### 命令回显处理

- 发送命令后不再由发送线程读取 socket 响应，避免和心跳接收线程抢读同一个连接。
- 心跳接收线程会识别设备直接返回的完整命令帧，并记录为命令回显而不是心跳。
- `/stats` 增加 `command_echo_received` 和 `last_command_echo`，方便查看设备是否回显了命令。
- README 增加命令回显说明，并更新设备标识接管规则。

### 安全加固

- 调整心跳处理顺序：只有 CRC 校验通过的心跳才会更新 `active_clients` 和 `stats`，CRC 失败的帧会被忽略。
- 增加设备连接数限制，默认最多允许 20 个活动设备连接，可通过 `MODBUS_MAX_ACTIVE_CLIENTS` 配置。
- 增加 FastAPI 请求频率限制，默认每个客户端 IP 在 60 秒内最多请求 60 次，可通过 `MODBUS_API_RATE_LIMIT_REQUESTS` 和 `MODBUS_API_RATE_LIMIT_WINDOW_SECONDS` 配置。
- 限制心跳数据和命令响应的日志输出长度，默认最多记录 128 个十六进制字符，可通过 `MODBUS_MAX_LOG_HEX_CHARS` 配置。

### 验证

- 已通过 `python -m py_compile modbus_api.py` 语法检查。

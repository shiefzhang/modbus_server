# 改动日志

## 2026-05-19

### 安全加固

- 调整心跳处理顺序：只有 CRC 校验通过的心跳才会更新 `active_clients` 和 `stats`，CRC 失败的帧会被忽略。
- 增加设备连接数限制，默认最多允许 20 个活动设备连接，可通过 `MODBUS_MAX_ACTIVE_CLIENTS` 配置。
- 增加 FastAPI 请求频率限制，默认每个客户端 IP 在 60 秒内最多请求 60 次，可通过 `MODBUS_API_RATE_LIMIT_REQUESTS` 和 `MODBUS_API_RATE_LIMIT_WINDOW_SECONDS` 配置。
- 限制心跳数据和命令响应的日志输出长度，默认最多记录 128 个十六进制字符，可通过 `MODBUS_MAX_LOG_HEX_CHARS` 配置。

### 验证

- 已通过 `python -m py_compile modbus_api.py` 语法检查。

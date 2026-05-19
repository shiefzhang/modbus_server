#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modbus 命令控制系统
- 心跳服务（10015端口）：维护设备连接，接收心跳不回复
- FastAPI 服务（8288端口）：接收命令，通过心跳连接发送 Modbus 命令
"""

import socket
import threading
import struct
import logging
import os
from collections import deque
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime
import uvicorn
import time
from typing import Dict, Optional

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============ 配置 ============
MODBUS_HOST = '0.0.0.0'
MODBUS_PORT = 10015

FASTAPI_HOST = '0.0.0.0'
FASTAPI_PORT = 8288

MAX_ACTIVE_CLIENTS = int(os.getenv("MODBUS_MAX_ACTIVE_CLIENTS", "20"))
MAX_LOG_HEX_CHARS = int(os.getenv("MODBUS_MAX_LOG_HEX_CHARS", "128"))
API_RATE_LIMIT_REQUESTS = int(os.getenv("MODBUS_API_RATE_LIMIT_REQUESTS", "60"))
API_RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("MODBUS_API_RATE_LIMIT_WINDOW_SECONDS", "60"))

# Modbus 命令帧定义
MODBUS_COMMANDS = {
    1: bytes([0x01, 0x06, 0x04, 0x0E, 0x00, 0x03]),  # 打开声音和灯光
    2: bytes([0x01, 0x06, 0x04, 0x0E, 0x00, 0x00]),  # 关闭声光
    3: bytes([0x01, 0x06, 0x04, 0x11, 0x00, 0x01]),  # 单曲循环模式
    4: bytes([0x01, 0x06, 0x04, 0x11, 0x00, 0x02]),  # 单曲模式
    5: bytes([0x01, 0x06, 0x04, 0x0E, 0x00, 0x01]),  # 单独打开声音
    6: bytes([0x01, 0x06, 0x04, 0x0E, 0x00, 0x02]),  # 单独打开灯光
    
    11: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x01]), # 音调切换到第一个文件夹第一条语音
    12: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x02]), # 音调切换到第一个文件夹第二条语音
    13: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x03]), # 音调切换到第一个文件夹第三条语音
    14: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x04]), # 音调切换到第一个文件夹第四条语音
    15: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x05]), # 音调切换到第一个文件夹第五条语音
    16: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x06]), # 音调切换到第一个文件夹第六条语音
    17: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x07]), # 音调切换到第一个文件夹第七条语音
    18: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x08]), # 音调切换到第一个文件夹第八条语音
    19: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x09]), # 音调切换到第一个文件夹第九条语音
    20: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x0A]), # 音调切换到第一个文件夹第十条语音
    21: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x0B]), # 音调切换到第一个文件夹第十一条语音
    22: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x0C]), # 音调切换到第一个文件夹第十二条语音
    23: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x0D]), # 音调切换到第一个文件夹第十三条语音
    24: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x0E]), # 音调切换到第一个文件夹第十四条语音
    25: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x0F]), # 音调切换到第一个文件夹第十五条语音
    26: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x10]), # 音调切换到第一个文件夹第十六条语音
    27: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x11]), # 音调切换到第一个文件夹第十七条语音
    28: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x12]), # 音调切换到第一个文件夹第十八条语音
    29: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x13]), # 音调切换到第一个文件夹第十九条语音
    30: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x14]), # 音调切换到第一个文件夹第二十条语音
    31: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x15]), # 音调切换到第一个文件夹第二十一条语音
    32: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x16]), # 音调切换到第一个文件夹第二十二条语音
    33: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x17]), # 音调切换到第一个文件夹第二十三条语音
    34: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x18]), # 音调切换到第一个文件夹第二十四条语音
    35: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x19]), # 音调切换到第一个文件夹第二十五条语音
    36: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x1A]), # 音调切换到第一个文件夹第二十六条语音
    37: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x1B]), # 音调切换到第一个文件夹第二十七条语音
    38: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x1C]), # 音调切换到第一个文件夹第二十八条语音
    39: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x1D]), # 音调切换到第一个文件夹第二十九条语音
    40: bytes([0x01, 0x06, 0x04, 0x10, 0x01, 0x1E]), # 音调切换到第一个文件夹第三十条语音

    51: bytes([0x01, 0x06, 0x04, 0x0F, 0x00, 0x01]), # 音量调为1级
    52: bytes([0x01, 0x06, 0x04, 0x0F, 0x00, 0x05]), # 音量调为5级
    53: bytes([0x01, 0x06, 0x04, 0x0F, 0x00, 0x0A]), # 音量调为10级
    54: bytes([0x01, 0x06, 0x04, 0x0F, 0x00, 0x0F]), # 音量调为15级
    55: bytes([0x01, 0x06, 0x04, 0x0F, 0x00, 0x14]), # 音量调为20级
    56: bytes([0x01, 0x06, 0x04, 0x0F, 0x00, 0x19]), # 音量调为25级
    57: bytes([0x01, 0x06, 0x04, 0x0F, 0x00, 0x1E])  # 音量调为30级
}

# 存储所有活动的客户端连接
active_clients: Dict[str, dict] = {}
clients_lock = threading.Lock()
rate_limit_lock = threading.Lock()
rate_limit_hits: Dict[str, deque] = {}

# 统计信息
stats = {
    "heartbeat_received": 0,
    "command_echo_received": 0,
    "commands_sent": 0,
    "last_command_echo": None,
    "last_heartbeat": None,
    "last_command": None,
    "active_connections": 0,
    "device_identifiers": {}
}

# ============ Modbus 工具函数 ============
def truncate_hex(data: bytes, max_chars: int = MAX_LOG_HEX_CHARS) -> str:
    """返回适合日志输出的十六进制字符串，避免大包刷爆日志。"""
    data_hex = data.hex().upper()
    if len(data_hex) <= max_chars:
        return data_hex
    return f"{data_hex[:max_chars]}...({len(data)} bytes)"

def calculate_crc16(data: bytes) -> int:
    """计算 Modbus CRC16"""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc

def validate_modbus_frame(data: bytes) -> bool:
    """验证 Modbus 帧的 CRC"""
    if len(data) < 4:
        return False
    
    received_crc = struct.unpack('<H', data[-2:])[0]
    calculated_crc = calculate_crc16(data[:-2])
    
    return received_crc == calculated_crc

def extract_device_identifier(data: bytes) -> Optional[str]:
    """提取心跳数据第 4~11 个字节作为设备标识。"""
    if len(data) < 11:
        return None

    return data[3:11].hex().upper()

def build_command_frame(command: int) -> bytes:
    """构建带 CRC 的完整 Modbus 命令帧。"""
    command_frame = MODBUS_COMMANDS[command]
    crc = calculate_crc16(command_frame)
    return command_frame + struct.pack('<H', crc)

def match_command_echo(data: bytes) -> Optional[int]:
    """识别设备直接返回的命令回显帧。"""
    for command in MODBUS_COMMANDS:
        if data == build_command_frame(command):
            return command

    return None

def send_command_to_client(client_socket: socket.socket, command: int, client_id: str) -> bool:
    """
    通过指定的客户端连接发送 Modbus 命令
    
    Args:
        client_socket: 客户端 socket
        command: 命令号 (1 或 2)
        client_id: 客户端标识
        
    Returns:
        是否发送成功
    """
    if command not in MODBUS_COMMANDS:
        logger.error(f"不支持的命令: {command}")
        return False
    
    command_frame = build_command_frame(command)
    
    try:
        # 通过现有的连接发送命令
        client_socket.send(command_frame)
        logger.info(f"✅ 通过连接 {client_id} 发送命令 {command}: {command_frame.hex().upper()}")

        # 更新统计
        stats["commands_sent"] += 1
        stats["last_command"] = {
            "command": command,
            "frame": command_frame.hex().upper(),
            "client_id": client_id,
            "time": datetime.now().isoformat()
        }
        
        return True
        
    except Exception as e:
        logger.error(f"发送命令到 {client_id} 失败: {e}")
        return False

def get_latest_heartbeat_client():
    """获取最近收到心跳的在线设备。调用方需要持有 clients_lock。"""
    latest_client_id = None
    latest_client_info = None

    for client_id, client_info in active_clients.items():
        last_heartbeat = client_info.get("last_heartbeat")
        if not last_heartbeat:
            continue

        if latest_client_info is None or last_heartbeat > latest_client_info["last_heartbeat"]:
            latest_client_id = client_id
            latest_client_info = client_info

    if latest_client_id is None:
        return None, None, None

    return latest_client_id, latest_client_info["socket"], latest_client_info["last_heartbeat"]

def get_client_by_device_identifier(device_identifier: str):
    """通过客户端ID或心跳设备标识获取在线设备。调用方需要持有 clients_lock。"""
    if device_identifier in active_clients:
        client_info = active_clients[device_identifier]
        return device_identifier, client_info, "client_id"

    normalized_identifier = device_identifier.upper()
    matched_client_id = None
    matched_client_info = None

    for client_id, client_info in active_clients.items():
        if client_info.get("device_identifier") != normalized_identifier:
            continue

        if (
            matched_client_info is None or
            (
                client_info.get("last_heartbeat") and
                client_info["last_heartbeat"] > matched_client_info.get("last_heartbeat", "")
            )
        ):
            matched_client_id = client_id
            matched_client_info = client_info

    if matched_client_id is None:
        return None, None, None

    return matched_client_id, matched_client_info, "device_identifier"

def remove_device_identifier_mapping(device_identifier: Optional[str], client_id: str) -> None:
    """移除仍指向指定连接的设备标识映射。调用方需要持有 clients_lock。"""
    if (
        device_identifier and
        stats["device_identifiers"].get(device_identifier, {}).get("client_id") == client_id
    ):
        del stats["device_identifiers"][device_identifier]

# ============ Modbus 心跳服务器 ============
class ModbusHeartbeatServer:
    """Modbus 心跳服务器 - 维护客户端连接，接收心跳不回复"""
    
    def __init__(self, host=MODBUS_HOST, port=MODBUS_PORT):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        
    def handle_client(self, client_socket, client_address, client_id):
        """
        处理客户端连接 - 接收心跳不回复，等待命令
        
        Args:
            client_socket: 客户端 socket
            client_address: 客户端地址
            client_id: 客户端唯一标识
        """
        logger.info(f"新设备连接 [{client_id}]: {client_address}")
        
        # 存储客户端信息
        with clients_lock:
            active_clients[client_id] = {
                "socket": client_socket,
                "address": client_address,
                "connected_at": datetime.now().isoformat(),
                "last_heartbeat": None,
                "heartbeat_count": 0,
                "device_identifier": None
            }
            stats["active_connections"] = len(active_clients)
        
        try:
            while self.running:
                try:
                    # 接收心跳数据
                    data = client_socket.recv(1024)
                    if not data:
                        break
                    
                    data_hex = truncate_hex(data)
                    echoed_command = match_command_echo(data)
                    if echoed_command is not None:
                        logger.info(f"↩️ 收到命令回显 [{client_id}] 命令 {echoed_command}: {data_hex}")
                        with clients_lock:
                            stats["command_echo_received"] += 1
                            stats["last_command_echo"] = {
                                "command": echoed_command,
                                "frame": data_hex,
                                "client_id": client_id,
                                "time": datetime.now().isoformat()
                            }
                        continue

                    logger.info(f"💓 收到心跳 [{client_id}]: {data_hex}")

                    if not validate_modbus_frame(data):
                        logger.warning(f"   CRC 校验: ❌ 失败，已忽略本次心跳")
                        continue

                    logger.info(f"   CRC 校验: ✅ 通过")

                    heartbeat_time = datetime.now().isoformat()
                    device_identifier = extract_device_identifier(data)

                    # 只统计通过 CRC 校验的心跳，避免伪造帧更新在线设备状态。
                    stale_socket = None
                    stale_client_id = None
                    with clients_lock:
                        if client_id in active_clients:
                            previous_identifier = active_clients[client_id].get("device_identifier")
                            if previous_identifier != device_identifier:
                                remove_device_identifier_mapping(previous_identifier, client_id)

                            if device_identifier:
                                existing_client_id = stats["device_identifiers"].get(
                                    device_identifier, {}
                                ).get("client_id")
                                if existing_client_id and existing_client_id != client_id:
                                    stale_client = active_clients.pop(existing_client_id, None)
                                    if stale_client:
                                        stale_socket = stale_client.get("socket")
                                        stale_client_id = existing_client_id
                                        stats["active_connections"] = len(active_clients)

                            active_clients[client_id]["last_heartbeat"] = heartbeat_time
                            active_clients[client_id]["heartbeat_count"] += 1
                            active_clients[client_id]["device_identifier"] = device_identifier
                    
                        stats["heartbeat_received"] += 1
                        stats["last_heartbeat"] = {
                            "time": heartbeat_time,
                            "data": data_hex,
                            "client_id": client_id,
                            "device_identifier": device_identifier,
                            "from": f"{client_address[0]}:{client_address[1]}"
                        }

                        if device_identifier:
                            stats["device_identifiers"][device_identifier] = {
                                "client_id": client_id,
                                "last_heartbeat": heartbeat_time,
                                "from": f"{client_address[0]}:{client_address[1]}"
                            }

                    if stale_socket:
                        logger.info(
                            f"设备标识 {device_identifier} 已由 {client_id} 接管，关闭旧连接 {stale_client_id}"
                        )
                        try:
                            stale_socket.close()
                        except Exception as e:
                            logger.warning(f"关闭旧连接 {stale_client_id} 时出错: {e}")

                    # ⚠️ 关键：不发送任何回复
                    # 保持连接打开，等待后续命令
                    
                except socket.timeout:
                    continue
                except ConnectionResetError:
                    break
                except Exception as e:
                    logger.error(f"处理心跳时出错 [{client_id}]: {e}")
                    break
                    
        finally:
            # 清理断开连接的客户端
            with clients_lock:
                if client_id in active_clients:
                    device_identifier = active_clients[client_id].get("device_identifier")
                    del active_clients[client_id]
                    remove_device_identifier_mapping(device_identifier, client_id)
                    stats["active_connections"] = len(active_clients)
            client_socket.close()
            logger.info(f"设备断开连接 [{client_id}]: {client_address}")
    
    def start(self):
        """启动心跳服务器"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(10)
            self.running = True
            
            logger.info(f"✅ Modbus 心跳服务器启动在 {self.host}:{self.port}")
            logger.info(f"   模式: 维护连接，接收心跳不回复，支持命令发送")
            
            client_id_counter = 0
            
            while self.running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    client_socket.settimeout(60)

                    with clients_lock:
                        active_count = len(active_clients)
                    if active_count >= MAX_ACTIVE_CLIENTS:
                        logger.warning(
                            f"拒绝设备连接 {client_address}: 当前连接数 {active_count} 已达到上限 {MAX_ACTIVE_CLIENTS}"
                        )
                        client_socket.close()
                        continue
                    
                    # 生成唯一客户端 ID
                    client_id_counter += 1
                    client_id = f"device_{client_id_counter:04d}"
                    
                    # 为每个客户端创建处理线程
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, client_address, client_id),
                        daemon=True
                    )
                    client_thread.start()
                    
                except Exception as e:
                    if self.running:
                        logger.error(f"接受连接时出错: {e}")
                        
        except Exception as e:
            logger.error(f"启动心跳服务器失败: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """停止心跳服务器"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        
        # 关闭所有客户端连接
        with clients_lock:
            for client_id, client_info in active_clients.items():
                try:
                    client_info["socket"].close()
                except:
                    pass
            active_clients.clear()
        
        logger.info("心跳服务器已停止")

# ============ FastAPI 应用 ============
app = FastAPI(
    title="Modbus Command API",
    description="接收命令1或2，通过已建立的心跳连接发送 Modbus 命令",
    version="1.0.0"
)

class CommandRequest(BaseModel):
    """命令请求模型"""
    command: int  # 1 或 2
    client_id: Optional[str] = None  # 可选的客户端ID，不指定则发送给最近收到心跳的设备

@app.middleware("http")
async def rate_limit_api_requests(request: Request, call_next):
    client_host = request.client.host if request.client else "unknown"
    now = time.monotonic()
    window_start = now - API_RATE_LIMIT_WINDOW_SECONDS

    with rate_limit_lock:
        hits = rate_limit_hits.setdefault(client_host, deque())
        while hits and hits[0] < window_start:
            hits.popleft()

        if len(hits) >= API_RATE_LIMIT_REQUESTS:
            return JSONResponse(
                status_code=429,
                content={"detail": "请求过于频繁，请稍后再试"}
            )

        hits.append(now)

        for host in list(rate_limit_hits.keys()):
            host_hits = rate_limit_hits[host]
            while host_hits and host_hits[0] < window_start:
                host_hits.popleft()
            if not host_hits:
                del rate_limit_hits[host]

    return await call_next(request)

@app.get("/")
async def root():
    """API 信息"""
    return {
        "service": "Modbus Command API",
        "version": "1.0.0",
        "ports": {
            "api_port": FASTAPI_PORT,
            "modbus_heartbeat_port": MODBUS_PORT
        },
        "endpoints": {
            "/": "API 信息",
            "/docs": "API 文档",
            "/send/{command}": "发送命令到最近收到心跳的设备 (GET)",
            "/send/{command}/{client_id}": "发送命令到指定设备，支持连接ID或心跳设备标识 (GET)",
            "/command": "发送命令到最近收到心跳的设备 (POST)",
            "/command/{client_id}": "发送命令到指定设备，支持连接ID或心跳设备标识 (POST)",
            "/clients": "查看所有连接的设备",
            "/stats": "统计信息",
            "/heartbeat/status": "心跳状态"
        },
        "commands": {
            "1": MODBUS_COMMANDS[1].hex().upper(),
            "2": MODBUS_COMMANDS[2].hex().upper()
        }
    }

@app.get("/clients")
async def get_clients():
    """获取所有连接的客户端"""
    with clients_lock:
        clients_info = []
        for client_id, client_info in active_clients.items():
            clients_info.append({
                "client_id": client_id,
                "device_identifier": client_info["device_identifier"],
                "address": f"{client_info['address'][0]}:{client_info['address'][1]}",
                "connected_at": client_info["connected_at"],
                "last_heartbeat": client_info["last_heartbeat"],
                "heartbeat_count": client_info["heartbeat_count"]
            })
    
    return {
        "total_connections": len(clients_info),
        "clients": clients_info,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/send/{command}")
async def send_command_to_default(command: int):
    """
    发送命令到最近收到心跳的设备
    
    Args:
        command: 1 或 2
    """
    with clients_lock:
        if not active_clients:
            raise HTTPException(status_code=404, detail="没有活动的设备连接")
        
        target_client_id, client_socket, last_heartbeat = get_latest_heartbeat_client()
        if not target_client_id:
            raise HTTPException(status_code=404, detail="没有收到过心跳数据的活动设备")
        device_identifier = active_clients[target_client_id].get("device_identifier")
    
    success = send_command_to_client(client_socket, command, target_client_id)
    
    if success:
        return {
            "status": "success",
            "command": command,
            "client_id": target_client_id,
            "device_identifier": device_identifier,
            "last_heartbeat": last_heartbeat,
            "modbus_frame": MODBUS_COMMANDS[command].hex().upper(),
            "timestamp": datetime.now().isoformat()
        }
    else:
        raise HTTPException(status_code=500, detail=f"发送命令 {command} 失败")

@app.get("/send/{command}/{client_id}")
async def send_command_to_specific(command: int, client_id: str):
    """
    发送命令到指定设备。client_id 参数可使用连接ID或心跳里的设备标识。
    
    Args:
        command: 1 或 2
        client_id: 连接ID或心跳设备标识
    """
    with clients_lock:
        target_client_id, client_info, matched_by = get_client_by_device_identifier(client_id)
        if not target_client_id:
            raise HTTPException(status_code=404, detail=f"设备 {client_id} 不存在或未连接")
        
        client_socket = client_info["socket"]
        device_identifier = client_info.get("device_identifier")
    
    success = send_command_to_client(client_socket, command, target_client_id)
    
    if success:
        return {
            "status": "success",
            "command": command,
            "client_id": target_client_id,
            "device_identifier": device_identifier,
            "matched_by": matched_by,
            "modbus_frame": MODBUS_COMMANDS[command].hex().upper(),
            "timestamp": datetime.now().isoformat()
        }
    else:
        raise HTTPException(status_code=500, detail=f"发送命令 {command} 到设备 {target_client_id} 失败")

@app.post("/command")
async def send_command_post_default(request: CommandRequest):
    """
    发送命令到最近收到心跳的设备（POST方式）
    """
    if request.client_id:
        return await send_command_to_specific(request.command, request.client_id)

    return await send_command_to_default(request.command)

@app.post("/command/{client_id}")
async def send_command_post_specific(client_id: str, request: CommandRequest):
    """
    发送命令到指定设备（POST方式）
    """
    return await send_command_to_specific(request.command, client_id)

@app.get("/stats")
async def get_stats():
    """获取统计信息"""
    with clients_lock:
        active_count = len(active_clients)
        device_identifiers = dict(stats["device_identifiers"])
        last_heartbeat = stats["last_heartbeat"]
        last_command = stats["last_command"]
        last_command_echo = stats["last_command_echo"]
    
    return {
        "statistics": {
            "heartbeat_received": stats["heartbeat_received"],
            "command_echo_received": stats["command_echo_received"],
            "commands_sent": stats["commands_sent"],
            "active_connections": active_count,
            "device_identifiers": device_identifiers,
            "last_heartbeat": last_heartbeat,
            "last_command": last_command,
            "last_command_echo": last_command_echo
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/heartbeat/status")
async def get_heartbeat_status():
    """获取心跳状态"""
    last_hb = stats["last_heartbeat"]
    if last_hb:
        last_time = datetime.fromisoformat(last_hb["time"])
        seconds_ago = (datetime.now() - last_time).seconds
    else:
        seconds_ago = None
    
    with clients_lock:
        active_count = len(active_clients)
    
    return {
        "heartbeat_received": stats["heartbeat_received"],
        "last_heartbeat_time": last_hb["time"] if last_hb else None,
        "last_device_identifier": last_hb.get("device_identifier") if last_hb else None,
        "seconds_since_last_hb": seconds_ago,
        "active_connections": active_count,
        "status": "active" if active_count > 0 else "no_connections",
        "timestamp": datetime.now().isoformat()
    }

@app.delete("/client/{client_id}")
async def disconnect_client(client_id: str):
    """
    强制断开指定客户端连接
    """
    with clients_lock:
        if client_id not in active_clients:
            raise HTTPException(status_code=404, detail=f"设备 {client_id} 不存在")
        
        try:
            active_clients[client_id]["socket"].close()
        except:
            pass
        
        device_identifier = active_clients[client_id].get("device_identifier")
        del active_clients[client_id]
        remove_device_identifier_mapping(device_identifier, client_id)
        stats["active_connections"] = len(active_clients)
    
    logger.info(f"强制断开设备: {client_id}")
    
    return {
        "status": "success",
        "message": f"设备 {client_id} 已断开",
        "timestamp": datetime.now().isoformat()
    }

# ============ 启动服务 ============
heartbeat_server = None

def run_heartbeat_server():
    """在单独线程中运行心跳服务器"""
    global heartbeat_server
    heartbeat_server = ModbusHeartbeatServer()
    heartbeat_server.start()

def run_api_server():
    """运行 FastAPI 服务器"""
    logger.info(f"🚀 FastAPI 服务启动在 {FASTAPI_HOST}:{FASTAPI_PORT}")
    logger.info(f"📖 API 文档: http://{FASTAPI_HOST}:{FASTAPI_PORT}/docs")
    
    uvicorn.run(
        app,
        host=FASTAPI_HOST,
        port=FASTAPI_PORT,
        log_level="info",
        access_log=True
    )

if __name__ == "__main__":
    print("=" * 70)
    print("Modbus 命令控制系统 - 复用心跳连接")
    print("=" * 70)
    print(f"📡 心跳服务器端口: {MODBUS_PORT}")
    print(f"   - 接收设备心跳")
    print(f"   - 维护设备连接")
    print(f"   - 不回复心跳")
    print(f"   - 复用连接发送命令")
    print("=" * 70)
    print(f"🌐 API 服务器端口: {FASTAPI_PORT}")
    print(f"   - 接收命令1/2")
    print(f"   - 通过现有心跳连接发送 Modbus 命令")
    print("=" * 70)
    print("\nModbus 命令:")
    print(f"  命令 1 -> {MODBUS_COMMANDS[1].hex().upper()} (开启)")
    print(f"  命令 2 -> {MODBUS_COMMANDS[2].hex().upper()} (关闭)")
    print("=" * 70)
    print("\nAPI 测试方法:")
    print(f"  GET:  http://127.0.0.1:{FASTAPI_PORT}/send/1")
    print(f"  POST: curl -X POST http://127.0.0.1:{FASTAPI_PORT}/command -H 'Content-Type: application/json' -d '{{\"command\":1}}'")
    print(f"  查看设备: http://127.0.0.1:{FASTAPI_PORT}/clients")
    print(f"  API 文档: http://127.0.0.1:{FASTAPI_PORT}/docs")
    print("=" * 70)
    print("\n💡 工作流程:")
    print("  1. 设备连接心跳服务器 (10015)")
    print("  2. 设备定期发送心跳，服务器不回复")
    print("  3. API 收到命令后，复用设备连接发送 Modbus 命令")
    print("  4. 设备执行命令，可选择性回复")
    print("=" * 70)
    print("\n按 Ctrl+C 停止所有服务\n")
    
    # 启动心跳服务器（后台线程）
    heartbeat_thread = threading.Thread(target=run_heartbeat_server, daemon=True)
    heartbeat_thread.start()
    
    # 等待心跳服务器启动
    time.sleep(1)
    
    # 启动 FastAPI 服务器（主线程）
    try:
        run_api_server()
    except KeyboardInterrupt:
        print("\n\n停止所有服务...")
        if heartbeat_server:
            heartbeat_server.stop()
        print("服务已停止")

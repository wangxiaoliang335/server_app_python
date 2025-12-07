# app.py
import os
#from flask import Flask, request, jsonify, session
import mysql.connector
from mysql.connector import Error
import hashlib
import secrets
import datetime
import random
import string
import logging
import traceback
import time
import base64
import os
import redis
import json
import uuid
import struct
import hmac
import zlib
import urllib.error
import urllib.parse
import urllib.request
try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False
    print("[警告] httpx 未安装，SRS 信令转发功能将使用 urllib（同步方式）")
from fastapi import FastAPI, Query
from typing import Any, List, Dict, Optional, Union
#import session
from logging.handlers import TimedRotatingFileHandler
from typing import Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, File, UploadFile, Form
from fastapi.responses import JSONResponse
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import ClientDisconnect
from fastapi.encoders import jsonable_encoder
from dotenv import load_dotenv
try:
    import oss2
except ImportError:
    oss2 = None
#from datetime import datetime
import jwt
import asyncio
import shutil

import time
import secrets
import hashlib
from typing import Dict
#from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
#from fastapi.responses import JSONResponse
import mysql.connector
from mysql.connector import Error

# 加载 .env 文件
load_dotenv()

# 验证关键环境变量是否加载
print(f"[启动检查] TENCENT_API_IDENTIFIER = {os.getenv('TENCENT_API_IDENTIFIER')}")
print(f"[启动检查] TENCENT_API_SDK_APP_ID = {os.getenv('TENCENT_API_SDK_APP_ID')}")

IMAGE_DIR = "/var/www/images"  # 存头像的目录

# SRS 服务器配置（支持 WHIP/WHEP）
SRS_SERVER = os.getenv('SRS_SERVER', '47.100.126.194')  # SRS 服务器地址
SRS_PORT = os.getenv('SRS_PORT', '1985')  # SRS WebRTC API 端口（传统 API 使用 1985）
SRS_HTTPS_PORT = os.getenv('SRS_HTTPS_PORT', '443')  # HTTPS 端口（nginx 反向代理）
SRS_APP = os.getenv('SRS_APP', 'live')  # SRS 应用名称，默认 'live'
SRS_USE_HTTPS = os.getenv('SRS_USE_HTTPS', 'true').lower() == 'true'  # 是否使用 HTTPS（默认启用）
# SRS_BASE_URL 用于 WHIP/WHEP（通过 nginx HTTPS 代理）
SRS_BASE_URL = f"{'https' if SRS_USE_HTTPS else 'http'}://{SRS_SERVER}"
if SRS_USE_HTTPS:
    # HTTPS 模式：通过 nginx 443 端口访问
    SRS_BASE_URL = f"https://{SRS_SERVER}"
    SRS_WEBRTC_API_URL = f"https://{SRS_SERVER}:{SRS_HTTPS_PORT}"
else:
    # HTTP 模式：直接访问 SRS 1985 端口
    SRS_BASE_URL = f"http://{SRS_SERVER}"
    SRS_WEBRTC_API_URL = f"http://{SRS_SERVER}:{SRS_PORT}"
print(f"[启动检查] SRS 服务器配置: 协议={'HTTPS' if SRS_USE_HTTPS else 'HTTP'}, BASE_URL={SRS_BASE_URL}, WebRTC API: {SRS_WEBRTC_API_URL}, APP={SRS_APP}")

# ===== 停止事件，用于控制心跳协程退出 =====
stop_event = asyncio.Event()

from contextlib import asynccontextmanager
# ===== 生命周期管理 =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    global stop_event
    stop_event.clear()

    # 启动时从数据库加载仍然活跃的临时语音房间
    load_active_temp_rooms_from_db()

    # 启动心跳检测任务
    hb_task = asyncio.create_task(heartbeat_checker())
    print("🚀 应用启动，心跳检测已启动")

    yield  # 应用运行中

    # 应用关闭逻辑
    print("🛑 应用关闭，准备停止心跳检测")
    stop_event.set()  # 通知心跳退出
    hb_task.cancel()  # 强制取消
    try:
        await hb_task
    except asyncio.CancelledError:
        print("heartbeat_checker 已安全停掉")

app = FastAPI(lifespan=lifespan)

# 本机维护的客户端连接表
connections: Dict[str, Dict] = {}  # {user_id: {"ws": WebSocket, "last_heartbeat": timestamp}}
active_temp_rooms: Dict[str, Dict[str, Any]] = {}  # {group_id: {...room info...}}


def load_active_temp_rooms_from_db() -> None:
    """
    应用启动时，从数据库加载仍然处于活跃状态的临时语音房间到内存 active_temp_rooms。
    防止程序重启后丢失房间信息。
    """
    try:
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[temp_room][startup] 数据库连接失败，无法从数据库加载临时语音房间")
            app_logger.error("[temp_room][startup] 数据库连接失败，无法从数据库加载临时语音房间")
            return

        cursor = connection.cursor(dictionary=True)

        # 查询所有状态为活跃的临时语音房间
        query_rooms = """
            SELECT room_id, group_id, owner_id, owner_name, owner_icon,
                   whip_url, whep_url, stream_name, status, create_time
            FROM temp_voice_rooms
            WHERE status = 1
        """
        cursor.execute(query_rooms)
        rooms = cursor.fetchall() or []

        if not rooms:
            print("[temp_room][startup] 数据库中没有状态为活跃的临时语音房间")
            app_logger.info("[temp_room][startup] 数据库中没有状态为活跃的临时语音房间")
            return

        loaded_count = 0
        for room in rooms:
            group_id = room.get("group_id")
            room_id = room.get("room_id")
            stream_name = room.get("stream_name")
            if not group_id or not room_id or not stream_name:
                continue

            # 根据 stream_name 重新生成传统 WebRTC 推流/拉流地址
            publish_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/publish/?app={SRS_APP}&stream={stream_name}"
            play_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/play/?app={SRS_APP}&stream={stream_name}"

            # 查询房间成员
            members_query = """
                SELECT user_id, user_name, status
                FROM temp_voice_room_members
                WHERE room_id = %s AND status = 1
            """
            cursor.execute(members_query, (room_id,))
            member_rows = cursor.fetchall() or []
            members = [m.get("user_id") for m in member_rows if m.get("user_id")]

            active_temp_rooms[group_id] = {
                "room_id": room_id,
                "publish_url": publish_url,
                "play_url": play_url,
                "whip_url": room.get("whip_url"),
                "whep_url": room.get("whep_url"),
                "stream_name": stream_name,
                "owner_id": room.get("owner_id"),
                "owner_name": room.get("owner_name"),
                "owner_icon": room.get("owner_icon"),
                "group_id": group_id,
                "timestamp": time.time(),
                "members": members,
            }
            loaded_count += 1

        print(f"[temp_room][startup] 已从数据库加载 {loaded_count} 个临时语音房间到内存")
        app_logger.info(f"[temp_room][startup] 已从数据库加载 {loaded_count} 个临时语音房间到内存")

    except Exception as e:
        print(f"[temp_room][startup] 从数据库加载临时语音房间失败: {e}")
        app_logger.error(f"[temp_room][startup] 从数据库加载临时语音房间失败: {e}", exc_info=True)
    finally:
        try:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'connection' in locals() and connection and connection.is_connected():
                connection.close()
        except Exception:
            pass


async def notify_temp_room_closed(group_id: str, room_info: Dict[str, Any], reason: str, initiator: str):
    """通知房间成员房间已解散，提醒客户端停止推拉流。"""
    if not room_info:
        return

    members_snapshot = list(room_info.get("members", []))
    if not members_snapshot:
        return

    notification = {
        "type": "temp_room_closed",
        "status": "closed",
        "action": "stop_stream",
        "group_id": group_id,
        "room_id": room_info.get("room_id"),
        "stream_name": room_info.get("stream_name"),
        "owner_id": room_info.get("owner_id"),
        "reason": reason,
        "initiator": initiator,
        "message": "临时房间已解散，请立即停止推流/拉流"
    }
    notification_json = json.dumps(notification, ensure_ascii=False)

    for member_id in members_snapshot:
        target_conn = connections.get(member_id)
        if not target_conn:
            continue
        try:
            await target_conn["ws"].send_text(notification_json)
            app_logger.info(f"[temp_room] 已通知成员停止推拉流 - group_id={group_id}, member_id={member_id}, reason={reason}")
        except Exception as notify_error:
            app_logger.warning(f"[temp_room] 通知成员停止推拉流失败 - group_id={group_id}, member_id={member_id}, error={notify_error}")

if not os.path.exists('logs'):
    os.makedirs('logs')

#app = Flask(__name__)
# 设置 Flask Session 密钥
#app.secret_key = 'a1b2c3d4e5f67890123456789012345678901234567890123456789012345678'
app.secret_key = os.getenv("FLASK_SECRET_KEY", "default_key")

# 创建一个 TimedRotatingFileHandler，每天 (midnight) 轮转，保留 30 天的日志
file_handler = TimedRotatingFileHandler(
    filename='logs/app.log',
    when='midnight',
    interval=1,
    backupCount=30,
    encoding='utf-8'
)

formatter = logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
file_handler.setFormatter(formatter)

app_logger = logging.getLogger('teacher-assistant')
app_logger.setLevel(logging.INFO)
app_logger.addHandler(file_handler)
app_logger.propagate = False

DB_CONFIG = {
    'host': 'rm-uf65y451aa995i174io.mysql.rds.aliyuncs.com',
    'database': 'teacher_assistant',
    'user': 'ta_user',
    'password': 'Ta_0909DB&'
}

# 短信服务配置 (模拟)
# SMS_CONFIG = {
#     'access_key_id': 'LTAI5tHt3ejFCgp5Qi4gjg2w',
#     'access_key_secret': 'itqsnPgUti737u0JdQ7WJTHHFeJyHv',
#     'sign_name': '临沂师悦数字科技有限公司',
#     'template_code': 'SMS_325560474'
# }

SMS_CONFIG = {
    'access_key_id': os.getenv("ALIYUN_AK_ID"),
    'access_key_secret': os.getenv("ALIYUN_AK_SECRET"),
    'sign_name': os.getenv("ALIYUN_SMS_SIGN"),
    'template_code': os.getenv("ALIYUN_SMS_TEMPLATE")
}

# ===== 阿里云 OSS 配置 =====
ALIYUN_OSS_ENDPOINT = os.getenv("ALIYUN_OSS_ENDPOINT")
ALIYUN_OSS_BUCKET = os.getenv("ALIYUN_OSS_BUCKET")
ALIYUN_OSS_ACCESS_KEY_ID = os.getenv("ALIYUN_OSS_ACCESS_KEY_ID")
ALIYUN_OSS_ACCESS_KEY_SECRET = os.getenv("ALIYUN_OSS_ACCESS_KEY_SECRET")
ALIYUN_OSS_BASE_URL = os.getenv("ALIYUN_OSS_BASE_URL")  # 可选，自定义 CDN 或访问域名

# ===== 本地头像访问配置（用于OSS失败时兜底）=====
LOCAL_AVATAR_BASE_URL = os.getenv("LOCAL_AVATAR_BASE_URL")  # 例如 https://cdn.xxx.com/images

# ===== 腾讯 REST API 配置 =====
TENCENT_API_URL = os.getenv("TENCENT_API_URL")
TENCENT_API_BASE_URL = os.getenv("TENCENT_API_BASE_URL")
TENCENT_API_PATH = os.getenv("TENCENT_API_PATH")
TENCENT_API_SDK_APP_ID = os.getenv("TENCENT_API_SDK_APP_ID")
TENCENT_API_IDENTIFIER = os.getenv("TENCENT_API_IDENTIFIER")
TENCENT_API_USER_SIG = os.getenv("TENCENT_API_USER_SIG")
TENCENT_API_TOKEN = os.getenv("TENCENT_API_TOKEN")
TENCENT_API_TIMEOUT = float(os.getenv("TENCENT_API_TIMEOUT", "10"))
TENCENT_API_SECRET_KEY = os.getenv("TENCENT_API_SECRET_KEY")
TENCENT_PROFILE_API_URL = os.getenv("TENCENT_PROFILE_API_URL")
TENCENT_PROFILE_API_PATH = os.getenv("TENCENT_PROFILE_API_PATH", "v4/profile/portrait_set")

# 打印关键配置（用于调试）
print(f"[配置加载] TENCENT_API_IDENTIFIER = {TENCENT_API_IDENTIFIER}")
print(f"[配置加载] TENCENT_API_SDK_APP_ID = {TENCENT_API_SDK_APP_ID}")

# 验证码有效期 (秒)
VERIFICATION_CODE_EXPIRY = 300 # 5分钟

from werkzeug.utils import secure_filename

# IMAGE_DIR = "./group_images"  # 群组头像目录
os.makedirs(IMAGE_DIR, exist_ok=True)

# 根上传目录
UPLOAD_FOLDER = './uploads/audio'
ALLOWED_EXTENSIONS = {'mp3', 'wav', 'aac', 'ogg', 'm4a'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_daily_upload_folder():
    """
    获取当天的上传子目录，如 ./uploads/audio/2025-09-13
    """
    today = datetime.now().strftime('%Y-%m-%d')
    daily_folder = os.path.join(UPLOAD_FOLDER, today)
    os.makedirs(daily_folder, exist_ok=True)
    return daily_folder


def upload_avatar_to_oss(avatar_bytes: bytes, object_name: str) -> Optional[str]:
    """
    上传头像文件到阿里云 OSS，返回可访问的 URL。
    """
    print(f"[upload_avatar_to_oss] 开始上传头像到OSS")
    print(f"[upload_avatar_to_oss] object_name: {object_name}")
    print(f"[upload_avatar_to_oss] avatar_bytes大小: {len(avatar_bytes) if avatar_bytes else 0} bytes")
    
    if not avatar_bytes:
        error_msg = "upload_avatar_to_oss: avatar_bytes 为空"
        app_logger.error(error_msg)
        print(f"[upload_avatar_to_oss] 错误: {error_msg}")
        return None

    print(f"[upload_avatar_to_oss] 检查oss2模块... oss2={oss2}")
    if oss2 is None:
        error_msg = "upload_avatar_to_oss: oss2 模块未安装，无法上传到 OSS"
        app_logger.error(error_msg)
        print(f"[upload_avatar_to_oss] 错误: {error_msg}")
        return None

    print(f"[upload_avatar_to_oss] 检查OSS配置...")
    print(f"[upload_avatar_to_oss]   ALIYUN_OSS_ENDPOINT: {ALIYUN_OSS_ENDPOINT}")
    print(f"[upload_avatar_to_oss]   ALIYUN_OSS_BUCKET: {ALIYUN_OSS_BUCKET}")
    print(f"[upload_avatar_to_oss]   ALIYUN_OSS_ACCESS_KEY_ID: {'已设置' if ALIYUN_OSS_ACCESS_KEY_ID else '未设置'}")
    print(f"[upload_avatar_to_oss]   ALIYUN_OSS_ACCESS_KEY_SECRET: {'已设置' if ALIYUN_OSS_ACCESS_KEY_SECRET else '未设置'}")
    print(f"[upload_avatar_to_oss]   ALIYUN_OSS_BASE_URL: {ALIYUN_OSS_BASE_URL}")
    
    if not all([ALIYUN_OSS_ENDPOINT, ALIYUN_OSS_BUCKET, ALIYUN_OSS_ACCESS_KEY_ID, ALIYUN_OSS_ACCESS_KEY_SECRET]):
        error_msg = "upload_avatar_to_oss: OSS 配置缺失，请检查环境变量"
        app_logger.error(error_msg)
        print(f"[upload_avatar_to_oss] 错误: {error_msg}")
        print(f"[upload_avatar_to_oss] 配置检查结果:")
        print(f"[upload_avatar_to_oss]   - ALIYUN_OSS_ENDPOINT存在: {bool(ALIYUN_OSS_ENDPOINT)}")
        print(f"[upload_avatar_to_oss]   - ALIYUN_OSS_BUCKET存在: {bool(ALIYUN_OSS_BUCKET)}")
        print(f"[upload_avatar_to_oss]   - ALIYUN_OSS_ACCESS_KEY_ID存在: {bool(ALIYUN_OSS_ACCESS_KEY_ID)}")
        print(f"[upload_avatar_to_oss]   - ALIYUN_OSS_ACCESS_KEY_SECRET存在: {bool(ALIYUN_OSS_ACCESS_KEY_SECRET)}")
        return None

    normalized_object_name = object_name.lstrip("/")
    print(f"[upload_avatar_to_oss] 标准化对象名称: {normalized_object_name}")

    try:
        print(f"[upload_avatar_to_oss] 创建OSS认证对象...")
        auth = oss2.Auth(ALIYUN_OSS_ACCESS_KEY_ID, ALIYUN_OSS_ACCESS_KEY_SECRET)
        print(f"[upload_avatar_to_oss] 创建OSS Bucket对象...")
        bucket = oss2.Bucket(auth, ALIYUN_OSS_ENDPOINT, ALIYUN_OSS_BUCKET)
        
        # 设置过期时间为100年后
        expire_time = datetime.datetime.utcnow() + datetime.timedelta(days=36500)  # 100年 = 36500天
        expires_header = expire_time.strftime('%a, %d %b %Y %H:%M:%S GMT')
        
        # 设置HTTP头，包括Expires和Cache-Control
        headers = {
            'Expires': expires_header,
            'Cache-Control': 'max-age=3153600000'  # 100年的秒数（约31.5亿秒）
        }
        
        print(f"[upload_avatar_to_oss] 设置过期时间: {expires_header} (100年后)")
        print(f"[upload_avatar_to_oss] 开始上传文件到OSS...")
        bucket.put_object(normalized_object_name, avatar_bytes, headers=headers)
        print(f"[upload_avatar_to_oss] 文件上传成功！")

        if ALIYUN_OSS_BASE_URL:
            base = ALIYUN_OSS_BASE_URL.rstrip("/")
            url = f"{base}/{normalized_object_name}"
            print(f"[upload_avatar_to_oss] 使用自定义BASE_URL生成URL: {url}")
            return url

        endpoint_host = ALIYUN_OSS_ENDPOINT.replace("https://", "").replace("http://", "").strip("/")
        url = f"https://{ALIYUN_OSS_BUCKET}.{endpoint_host}/{normalized_object_name}"
        print(f"[upload_avatar_to_oss] 使用默认格式生成URL: {url}")
        return url
    except Exception as exc:
        error_msg = f"upload_avatar_to_oss: 上传失败 object={normalized_object_name}, error={exc}"
        app_logger.error(error_msg)
        print(f"[upload_avatar_to_oss] 异常: {error_msg}")
        print(f"[upload_avatar_to_oss] 异常类型: {type(exc).__name__}")
        print(f"[upload_avatar_to_oss] 异常堆栈:\n{traceback.format_exc()}")
        return None


def upload_excel_to_oss(excel_bytes: bytes, object_name: str) -> Optional[str]:
    """
    上传Excel文件到阿里云 OSS，返回可访问的 URL。
    """
    print(f"[upload_excel_to_oss] ========== 开始上传Excel文件到OSS ==========")
    app_logger.info(f"[upload_excel_to_oss] ========== 开始上传Excel文件到OSS ==========")
    print(f"[upload_excel_to_oss] 📋 输入参数:")
    print(f"[upload_excel_to_oss]   - object_name: {object_name}")
    print(f"[upload_excel_to_oss]   - excel_bytes大小: {len(excel_bytes) if excel_bytes else 0} bytes")
    app_logger.info(f"[upload_excel_to_oss] 📋 输入参数: object_name={object_name}, excel_bytes大小={len(excel_bytes) if excel_bytes else 0} bytes")
    
    if not excel_bytes:
        error_msg = "upload_excel_to_oss: excel_bytes 为空"
        app_logger.error(error_msg)
        print(f"[upload_excel_to_oss] 错误: {error_msg}")
        return None

    print(f"[upload_excel_to_oss] 检查oss2模块... oss2={oss2}")
    if oss2 is None:
        error_msg = "upload_excel_to_oss: oss2 模块未安装，无法上传到 OSS"
        app_logger.error(error_msg)
        print(f"[upload_excel_to_oss] 错误: {error_msg}")
        return None

    print(f"[upload_excel_to_oss] 检查OSS配置...")
    print(f"[upload_excel_to_oss]   ALIYUN_OSS_ENDPOINT: {ALIYUN_OSS_ENDPOINT}")
    print(f"[upload_excel_to_oss]   ALIYUN_OSS_BUCKET: {ALIYUN_OSS_BUCKET}")
    print(f"[upload_excel_to_oss]   ALIYUN_OSS_ACCESS_KEY_ID: {'已设置' if ALIYUN_OSS_ACCESS_KEY_ID else '未设置'}")
    print(f"[upload_excel_to_oss]   ALIYUN_OSS_ACCESS_KEY_SECRET: {'已设置' if ALIYUN_OSS_ACCESS_KEY_SECRET else '未设置'}")
    print(f"[upload_excel_to_oss]   ALIYUN_OSS_BASE_URL: {ALIYUN_OSS_BASE_URL}")
    
    if not all([ALIYUN_OSS_ENDPOINT, ALIYUN_OSS_BUCKET, ALIYUN_OSS_ACCESS_KEY_ID, ALIYUN_OSS_ACCESS_KEY_SECRET]):
        error_msg = "upload_excel_to_oss: OSS 配置缺失，请检查环境变量"
        app_logger.error(error_msg)
        print(f"[upload_excel_to_oss] 错误: {error_msg}")
        print(f"[upload_excel_to_oss] 配置检查结果:")
        print(f"[upload_excel_to_oss]   - ALIYUN_OSS_ENDPOINT存在: {bool(ALIYUN_OSS_ENDPOINT)}")
        print(f"[upload_excel_to_oss]   - ALIYUN_OSS_BUCKET存在: {bool(ALIYUN_OSS_BUCKET)}")
        print(f"[upload_excel_to_oss]   - ALIYUN_OSS_ACCESS_KEY_ID存在: {bool(ALIYUN_OSS_ACCESS_KEY_ID)}")
        print(f"[upload_excel_to_oss]   - ALIYUN_OSS_ACCESS_KEY_SECRET存在: {bool(ALIYUN_OSS_ACCESS_KEY_SECRET)}")
        return None

    normalized_object_name = object_name.lstrip("/")
    print(f"[upload_excel_to_oss] 标准化对象名称: {normalized_object_name}")

    try:
        print(f"[upload_excel_to_oss] 创建OSS认证对象...")
        auth = oss2.Auth(ALIYUN_OSS_ACCESS_KEY_ID, ALIYUN_OSS_ACCESS_KEY_SECRET)
        print(f"[upload_excel_to_oss] 创建OSS Bucket对象...")
        bucket = oss2.Bucket(auth, ALIYUN_OSS_ENDPOINT, ALIYUN_OSS_BUCKET)
        
        # 设置过期时间为100年后
        expire_time = datetime.datetime.utcnow() + datetime.timedelta(days=36500)  # 100年 = 36500天
        expires_header = expire_time.strftime('%a, %d %b %Y %H:%M:%S GMT')
        
        # 设置HTTP头，包括Expires和Cache-Control
        headers = {
            'Expires': expires_header,
            'Cache-Control': 'max-age=3153600000'  # 100年的秒数（约31.5亿秒）
        }
        
        print(f"[upload_excel_to_oss] 设置过期时间: {expires_header} (100年后)")
        print(f"[upload_excel_to_oss] ☁️ 开始上传文件到OSS...")
        app_logger.info(f"[upload_excel_to_oss] ☁️ 开始上传文件到OSS: {normalized_object_name}")
        bucket.put_object(normalized_object_name, excel_bytes, headers=headers)
        print(f"[upload_excel_to_oss] ✅ 文件上传成功！")
        app_logger.info(f"[upload_excel_to_oss] ✅ 文件上传成功: {normalized_object_name}")

        print(f"[upload_excel_to_oss] 🔗 开始生成访问URL...")
        app_logger.info(f"[upload_excel_to_oss] 🔗 开始生成访问URL...")
        if ALIYUN_OSS_BASE_URL:
            base = ALIYUN_OSS_BASE_URL.rstrip("/")
            url = f"{base}/{normalized_object_name}"
            print(f"[upload_excel_to_oss] ✅ 使用自定义BASE_URL生成URL: {url}")
            app_logger.info(f"[upload_excel_to_oss] ✅ 使用自定义BASE_URL生成URL: {url}")
            print(f"[upload_excel_to_oss] ========== 上传完成，返回URL ==========")
            app_logger.info(f"[upload_excel_to_oss] ========== 上传完成，返回URL: {url} ==========")
            return url

        endpoint_host = ALIYUN_OSS_ENDPOINT.replace("https://", "").replace("http://", "").strip("/")
        url = f"https://{ALIYUN_OSS_BUCKET}.{endpoint_host}/{normalized_object_name}"
        print(f"[upload_excel_to_oss] ✅ 使用默认格式生成URL: {url}")
        app_logger.info(f"[upload_excel_to_oss] ✅ 使用默认格式生成URL: {url}")
        print(f"[upload_excel_to_oss] ========== 上传完成，返回URL ==========")
        app_logger.info(f"[upload_excel_to_oss] ========== 上传完成，返回URL: {url} ==========")
        return url
    except Exception as exc:
        error_msg = f"upload_excel_to_oss: 上传失败 object={normalized_object_name}, error={exc}"
        app_logger.error(error_msg)
        print(f"[upload_excel_to_oss] 异常: {error_msg}")
        print(f"[upload_excel_to_oss] 异常类型: {type(exc).__name__}")
        print(f"[upload_excel_to_oss] 异常堆栈:\n{traceback.format_exc()}")
        return None


def save_avatar_locally(avatar_bytes: bytes, object_name: str) -> Optional[str]:
    """
    OSS 上传失败时，将头像保存到本地 IMAGE_DIR/avatars 下，返回相对路径。
    """
    print("[save_avatar_locally] 开始执行本地保存逻辑")
    if not avatar_bytes:
        print("[save_avatar_locally] avatar_bytes 为空，无法保存")
        return None

    filename = os.path.basename(object_name) or f"{int(time.time())}.png"
    local_dir = os.path.join(IMAGE_DIR, "avatars")
    os.makedirs(local_dir, exist_ok=True)
    file_path = os.path.join(local_dir, filename)

    try:
        with open(file_path, "wb") as f:
            f.write(avatar_bytes)
        relative_path = os.path.join("avatars", filename).replace("\\", "/")
        print(f"[save_avatar_locally] 保存成功 -> {file_path}, relative_path={relative_path}")
        return relative_path
    except Exception as exc:
        error_msg = f"save_avatar_locally: 保存失败 path={file_path}, error={exc}"
        app_logger.error(error_msg)
        print(f"[save_avatar_locally] 异常: {error_msg}")
        print(f"[save_avatar_locally] 异常堆栈:\n{traceback.format_exc()}")
        return None


def build_public_url_from_local_path(relative_path: Optional[str]) -> Optional[str]:
    """
    如果配置了 LOCAL_AVATAR_BASE_URL，则根据本地相对路径拼接可访问的 HTTP 地址。
    """
    if not relative_path:
        return None
    if not LOCAL_AVATAR_BASE_URL:
        return None
    base = LOCAL_AVATAR_BASE_URL.rstrip("/")
    cleaned = relative_path.lstrip("/")
    public_url = f"{base}/{cleaned}"
    print(f"[build_public_url_from_local_path] 生成URL: {public_url}")
    return public_url


def resolve_local_avatar_file_path(avatar_path: Optional[str]) -> Optional[str]:
    """
    根据数据库中存储的 avatar 字段推断本地文件路径。
    当 avatar 已经是 URL 时返回 None。
    """
    if not avatar_path:
        return None

    path_str = str(avatar_path).strip()
    if not path_str:
        return None

    lowered = path_str.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return None

    if os.path.isabs(path_str):
        return path_str

    return os.path.join(IMAGE_DIR, path_str)

def safe_json_response(data: dict, status_code: int = 200):
    return JSONResponse(jsonable_encoder(data), status_code=status_code)

def get_db_connection():
    """获取数据库连接"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        app_logger.info("Database connection established.")
        return connection
    except Error as e:
        app_logger.error(f"Error connecting to MySQL: {e}")
        return None


def build_tencent_request_url(
    identifier: Optional[str] = None,
    usersig: Optional[str] = None,
    *,
    url_override: Optional[str] = None,
    path_override: Optional[str] = None,
    base_override: Optional[str] = None
) -> Optional[str]:
    """
    生成腾讯 REST API 的完整请求 URL。
    优先使用 TENCENT_API_URL，其次使用 base + path + query 参数。
    """
    if url_override is not None:
        selected_url = url_override
    elif path_override is not None:
        selected_url = None
    else:
        selected_url = TENCENT_API_URL
    selected_base = base_override or TENCENT_API_BASE_URL
    selected_path = path_override if path_override is not None else TENCENT_API_PATH

    effective_identifier = identifier or TENCENT_API_IDENTIFIER
    effective_usersig = usersig or TENCENT_API_USER_SIG

    extra_query: Dict[str, str] = {}

    if selected_url:
        parsed = urllib.parse.urlparse(selected_url)
        if parsed.scheme and parsed.netloc:
            existing_query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

            def pick_single(values):
                if isinstance(values, list):
                    return values[0] if values else ""
                return values

            normalized_query: Dict[str, str] = {k: pick_single(v) for k, v in existing_query.items()}

            def ensure_query_param(key: str, value: Optional[str], force: bool = False):
                if value is not None:
                    if force or (key not in normalized_query or not normalized_query[key]):
                        normalized_query[key] = value

            ensure_query_param("sdkappid", TENCENT_API_SDK_APP_ID)
            # 强制覆盖 identifier 和 usersig，确保使用传入的值
            ensure_query_param("identifier", effective_identifier, force=True)
            ensure_query_param("usersig", effective_usersig, force=True)
            ensure_query_param("contenttype", "json")
            if "random" not in normalized_query or not normalized_query["random"]:
                normalized_query["random"] = str(random.randint(1, 2**31 - 1))

            if "sdkappid" not in normalized_query or not normalized_query["sdkappid"]:
                app_logger.error("腾讯 REST API URL 缺少 sdkappid 且未配置 TENCENT_API_SDK_APP_ID，无法构建完整 URL。")
                return None

            rebuilt_query = urllib.parse.urlencode(normalized_query)
            rebuilt_url = urllib.parse.urlunparse(parsed._replace(query=rebuilt_query))
            return rebuilt_url
        else:
            if parsed.path:
                computed_path = parsed.path.lstrip("/")
                if parsed.scheme and not parsed.netloc:
                    combined = parsed.scheme
                    if computed_path:
                        combined = f"{parsed.scheme}/{computed_path}"
                    selected_path = combined
                else:
                    selected_path = parsed.path
            if parsed.netloc and not selected_base and parsed.scheme:
                selected_base = f"{parsed.scheme}://{parsed.netloc}"
            if parsed.query:
                for key, values in urllib.parse.parse_qs(parsed.query, keep_blank_values=True).items():
                    if values:
                        extra_query[key] = values[0]

    if not selected_base and TENCENT_API_URL:
        parsed_base_source = urllib.parse.urlparse(TENCENT_API_URL)
        if parsed_base_source.scheme and parsed_base_source.netloc:
            selected_base = f"{parsed_base_source.scheme}://{parsed_base_source.netloc}"

    if not selected_base:
        app_logger.error(
            "构建腾讯 REST API URL 失败：缺少 base URL。"
            f" selected_url={selected_url}, selected_base={selected_base}, selected_path={selected_path}"
        )
        return None

    path = (selected_path or "").strip("/")
    base = selected_base.rstrip("/")
    url = f"{base}/{path}" if path else base

    if not (TENCENT_API_SDK_APP_ID and effective_identifier and effective_usersig):
        # 缺少拼装 query 所需的参数，则直接返回 base/path
        return url

    query_params = {
        "sdkappid": TENCENT_API_SDK_APP_ID,
        "identifier": effective_identifier,
        "usersig": effective_usersig,
        "random": random.randint(1, 2**31 - 1),
        "contenttype": "json"
    }
    for key, value in extra_query.items():
        query_params.setdefault(key, value)

    final_url = f"{url}?{urllib.parse.urlencode(query_params)}"
    if not final_url.lower().startswith(("http://", "https://")):
        app_logger.error(f"构建腾讯 REST API URL 失败，结果缺少协议: {final_url}")
        return None
    app_logger.debug(f"构建腾讯 REST API URL: base={base}, path={path}, final={final_url}")
    return final_url


def build_tencent_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {
        "Content-Type": "application/json; charset=utf-8"
    }
    if TENCENT_API_TOKEN:
        headers["Authorization"] = f"Bearer {TENCENT_API_TOKEN}"

    sanitized_headers: Dict[str, str] = {}
    for key, value in headers.items():
        try:
            value.encode("latin-1")
            sanitized_headers[key] = value
        except UnicodeEncodeError:
            app_logger.warning(f"Tencent REST API header {key} 包含非 Latin-1 字符，已跳过该字段。")
    return sanitized_headers


def resolve_tencent_identifier(connection, *, id_number: Optional[str] = None, phone: Optional[str] = None) -> Optional[str]:
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        if id_number:
            cursor.execute("SELECT teacher_unique_id FROM ta_teacher WHERE id_card = %s", (id_number,))
            row = cursor.fetchone()
            if row:
                identifier = row.get("teacher_unique_id")
                if identifier:
                    return identifier
        if phone:
            cursor.execute("SELECT teacher_unique_id FROM ta_teacher WHERE phone = %s", (phone,))
            row = cursor.fetchone()
            if row:
                identifier = row.get("teacher_unique_id")
                if identifier:
                    return identifier
    except Exception as e:
        app_logger.error(f"解析腾讯 Identifier 时发生错误: {e}")
    finally:
        if cursor:
            cursor.close()
    return id_number or phone


def convert_group_type_to_int(group_type: Union[str, int, None]) -> int:
    """
    将群类型字符串转换为整数
    腾讯IM群类型：Public=0, Private=1, ChatRoom=2, AVChatRoom=3, BChatRoom=4, Community=5, Work=6, Meeting=7
    注意：会议群(Meeting)在腾讯IM中通常映射到 ChatRoom(2) 或 AVChatRoom(3)，但有些版本可能有独立的 Meeting 类型
    """
    if group_type is None:
        return 0  # 默认 Public
    
    if isinstance(group_type, int):
        return group_type
    
    if isinstance(group_type, str):
        type_mapping = {
            "public": 0,
            "Public": 0,
            "PUBLIC": 0,
            "private": 1,
            "Private": 1,
            "PRIVATE": 1,
            "chatroom": 2,
            "ChatRoom": 2,
            "CHATROOM": 2,
            "avchatroom": 3,
            "AVChatRoom": 3,
            "AVCHATROOM": 3,
            "bchatroom": 4,
            "BChatRoom": 4,
            "BCHATROOM": 4,
            "community": 5,
            "Community": 5,
            "COMMUNITY": 5,
            "work": 6,
            "Work": 6,
            "WORK": 6,
            "class": 6,  # 班级群使用 Work 类型
            "Class": 6,
            # 会议群相关映射（通常映射到 ChatRoom 或 AVChatRoom）
            "meeting": 2,  # 会议群映射到 ChatRoom
            "Meeting": 2,
            "MEETING": 2,
            "meetinggroup": 2,  # 会议群组映射到 ChatRoom
            "MeetingGroup": 2,
            "MEETINGGROUP": 2,
            "会议": 2,  # 中文"会议"映射到 ChatRoom
            "会议群": 2,  # 中文"会议群"映射到 ChatRoom
            # 如果需要音视频会议功能，可以映射到 AVChatRoom(3)
            "avmeeting": 3,  # 音视频会议映射到 AVChatRoom
            "AVMeeting": 3,
            "AVMEETING": 3
        }
        return type_mapping.get(group_type, 0)  # 默认返回 0 (Public)
    
    return 0  # 默认返回 0 (Public)

def normalize_tencent_group_type(raw_type: Optional[str]) -> str:
    default_type = "ChatRoom"
    if not raw_type:
        return default_type

    mapping = {
        "private": "Private",
        "public": "Public",
        "chatroom": "ChatRoom",
        "meeting": "ChatRoom",
        "meetinggroup": "ChatRoom",
        "会议": "ChatRoom",
        "会议群": "ChatRoom",
        "avchatroom": "AVChatRoom",
        "bchatroom": "BChatRoom",
        "community": "Community",
        "work": "Work",
        "class": "Work",
        "group": "Work"
    }

    normalized_key = str(raw_type).strip().lower()
    return mapping.get(normalized_key, default_type)


def normalize_tencent_group_id(group_id: Optional[str]) -> Optional[str]:
    """
    清理群组ID，移除腾讯IM不允许的 @TGS# 前缀。
    腾讯 REST API 不允许群组ID包含 @TGS# 前缀，需要移除。
    """
    if not group_id:
        return group_id
    
    group_id_str = str(group_id).strip()
    # 移除 @TGS# 前缀（如果存在）
    if group_id_str.startswith("@TGS#"):
        group_id_str = group_id_str[5:]  # 移除 "@TGS#" 这5个字符
    
    return group_id_str if group_id_str else None


def generate_tencent_user_sig(identifier: str, expire: int = 86400) -> str:
    if not (TENCENT_API_SDK_APP_ID and TENCENT_API_SECRET_KEY):
        raise ValueError("缺少 TENCENT_API_SDK_APP_ID 或 TENCENT_API_SECRET_KEY 配置，无法生成 UserSig。")

    sdk_app_id = int(TENCENT_API_SDK_APP_ID)
    current_time = int(time.time())

    data_to_sign = [
        f"TLS.identifier:{identifier}",
        f"TLS.sdkappid:{sdk_app_id}",
        f"TLS.time:{current_time}",
        f"TLS.expire:{expire}",
        ""
    ]
    content = "\n".join(data_to_sign)

    digest = hmac.new(
        TENCENT_API_SECRET_KEY.encode("utf-8"),
        content.encode("utf-8"),
        hashlib.sha256
    ).digest()

    signature = base64.b64encode(digest).decode("utf-8")

    sig_doc = {
        "TLS.ver": "2.0",
        "TLS.identifier": identifier,
        "TLS.sdkappid": sdk_app_id,
        "TLS.expire": expire,
        "TLS.time": current_time,
        "TLS.sig": signature
    }

    json_data = json.dumps(sig_doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    compressed = zlib.compress(json_data)
    return base64.b64encode(compressed).decode("utf-8")


async def notify_tencent_user_profile(identifier: str, *, name: Optional[str] = None, avatar_url: Optional[str] = None) -> Dict[str, Any]:
    if not identifier:
        return {"status": "error", "error": "缺少腾讯用户 Identifier"}

    profile_items: List[Dict[str, Any]] = []
    if name:
        profile_items.append({"Tag": "Tag_Profile_IM_Nick", "Value": name})
    if avatar_url:
        profile_items.append({"Tag": "Tag_Profile_IM_Image", "Value": avatar_url})

    if not profile_items:
        return {"status": "skipped", "reason": "empty_profile_items"}

    usersig_to_use: Optional[str] = None
    sig_error: Optional[str] = None
    if TENCENT_API_SECRET_KEY:
        try:
            usersig_to_use = generate_tencent_user_sig(identifier)
        except Exception as e:
            sig_error = f"自动生成用户 UserSig 失败: {e}"
            app_logger.error(sig_error)

    if not usersig_to_use:
        usersig_to_use = TENCENT_API_USER_SIG

    if not usersig_to_use:
        error_message = "缺少可用的 UserSig，已跳过腾讯用户资料同步。"
        app_logger.error(error_message)
        return {"status": "error", "error": error_message}

    url = build_tencent_request_url(
        identifier=identifier,
        usersig=usersig_to_use,
        url_override=TENCENT_PROFILE_API_URL,
        path_override=TENCENT_PROFILE_API_PATH
    )
    if not url:
        msg = "腾讯用户资料接口未配置，跳过同步"
        app_logger.warning(msg)
        return {"status": "skipped", "reason": "missing_configuration", "message": msg}

    headers = build_tencent_headers()
    payload = {
        "From_Account": identifier,
        "ProfileItem": profile_items
    }

    if sig_error:
        masked_error = sig_error.replace(usersig_to_use or "", "***")
        app_logger.warning(masked_error)

    def _send_request() -> Dict[str, Any]:
        encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url=url,
            data=encoded_payload,
            headers=headers,
            method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=TENCENT_API_TIMEOUT) as response:
                raw_body = response.read()
                text_body = raw_body.decode("utf-8", errors="replace")
                try:
                    parsed_body = json.loads(text_body)
                except json.JSONDecodeError:
                    parsed_body = None

                result = {
                    "status": "success",
                    "http_status": response.status,
                    "response": parsed_body or text_body
                }
                app_logger.info(f"Tencent 用户资料同步成功: {result}")
                return result
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            app_logger.error(f"Tencent 用户资料同步失败 (HTTP {e.code}): {body}")
            return {"status": "error", "http_status": e.code, "error": body}
        except urllib.error.URLError as e:
            app_logger.error(f"Tencent 用户资料接口调用异常: {e}")
            return {"status": "error", "http_status": None, "error": str(e)}
        except Exception as exc:
            app_logger.exception(f"Tencent 用户资料接口未知异常: {exc}")
            return {"status": "error", "http_status": None, "error": str(exc)}

    return await asyncio.to_thread(_send_request)


async def notify_tencent_group_sync(user_id: str, groups: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    将同步到本地数据库的群组数据推送到腾讯 REST API。
    注意：腾讯 REST API 要求使用管理员账号作为 identifier，而不是普通用户账号。
    """
    print(f"[notify_tencent_group_sync] 函数被调用: user_id={user_id}, groups数量={len(groups) if groups else 0}")
    app_logger.info(f"notify_tencent_group_sync 被调用: user_id={user_id}, groups数量={len(groups) if groups else 0}")
    
    if not groups:
        return {"status": "skipped", "reason": "empty_groups"}

    # 使用管理员账号作为 identifier（腾讯 REST API 要求）
    admin_identifier = TENCENT_API_IDENTIFIER
    print(f"[notify_tencent_group_sync] TENCENT_API_IDENTIFIER 值: {admin_identifier}")
    app_logger.info(f"TENCENT_API_IDENTIFIER 环境变量值: {admin_identifier}")
    
    if not admin_identifier:
        error_message = "缺少腾讯 REST API 管理员账号配置 (TENCENT_API_IDENTIFIER)，已跳过同步。"
        app_logger.error(error_message)
        return {
            "status": "error",
            "http_status": None,
            "error": error_message
        }

    # 确保 identifier 是字符串类型
    identifier_to_use = str(admin_identifier) if admin_identifier else None
    print(f"[notify_tencent_group_sync] 最终使用的 identifier: {identifier_to_use}, 类型: {type(identifier_to_use)}")
    app_logger.info(f"群组同步使用管理员账号作为 identifier: {identifier_to_use} (原始 user_id: {user_id})")

    usersig_to_use: Optional[str] = None
    sig_error: Optional[str] = None
    if TENCENT_API_SECRET_KEY:
        try:
            # 为管理员账号生成 UserSig
            print(f"[notify_tencent_group_sync] 准备为管理员账号生成 UserSig: identifier={identifier_to_use}, type={type(identifier_to_use)}")
            usersig_to_use = generate_tencent_user_sig(identifier_to_use)
            print(f"[notify_tencent_group_sync] UserSig 生成成功，长度: {len(usersig_to_use) if usersig_to_use else 0}")
            app_logger.info(f"为管理员账号 {identifier_to_use} 生成 UserSig 成功")
        except Exception as e:
            sig_error = f"自动生成管理员 UserSig 失败: {e}"
            print(f"[notify_tencent_group_sync] UserSig 生成失败: {sig_error}")
            app_logger.error(sig_error)

    if not usersig_to_use:
        print(f"[notify_tencent_group_sync] 使用配置的 TENCENT_API_USER_SIG")
        usersig_to_use = TENCENT_API_USER_SIG

    if not usersig_to_use:
        error_message = "缺少可用的管理员 UserSig，已跳过腾讯 REST API 同步。"
        app_logger.error(error_message)
        return {
            "status": "error",
            "http_status": None,
            "error": error_message
        }

    url = build_tencent_request_url(identifier=identifier_to_use, usersig=usersig_to_use)
    if not url:
        msg = "腾讯 REST API 未配置，跳过同步"
        app_logger.warning(msg)
        return {"status": "skipped", "reason": "missing_configuration", "message": msg}

    if sig_error:
        masked_error = sig_error.replace(usersig_to_use or "", "***")
        app_logger.warning(masked_error)

    def _prepare_group_for_tencent(group: Dict[str, Any]) -> Dict[str, Any]:
        prepared = dict(group)
        group_type = prepared.get("group_type") or prepared.get("Type")
        normalized_type = normalize_tencent_group_type(group_type)
        prepared["group_type"] = normalized_type
        prepared["Type"] = normalized_type
        prepared["Name"] = prepared.get("Name") or prepared.get("group_name") or prepared.get("name")
        if not prepared["Name"]:
            prepared["Name"] = f"group_{prepared.get('group_id') or random.randint(1, 2**31 - 1)}"

        # Owner_Account 应该使用实际的群主账号，而不是管理员账号
        # identifier_to_use 现在是管理员账号，用于 REST API 认证
        # 但群主应该是从 group 数据中获取，或者使用传入的 user_id
        owner = prepared.get("Owner_Account") or prepared.get("owner_identifier") or user_id
        if owner:
            prepared["Owner_Account"] = owner

        return prepared

    payload_groups = [_prepare_group_for_tencent(group) for group in groups]

    app_logger.info(f"Tencent REST API payload preview: {payload_groups}")

    def validate_and_log_url(current_url: str) -> Dict[str, Any]:
        parsed_url = urllib.parse.urlparse(current_url)
        query_dict = urllib.parse.parse_qs(parsed_url.query, keep_blank_values=True)
        sdkappid_values = query_dict.get("sdkappid", [])
        if not sdkappid_values or not sdkappid_values[0]:
            error_message = "腾讯 REST API 请求 URL 缺少 sdkappid，已跳过同步。"
            app_logger.error(error_message + f" URL: {current_url}")
            return {"error": error_message}

        def mask_value(value: str, keep: int = 4) -> str:
            if not value:
                return value
            if len(value) <= keep:
                return "*" * len(value)
            return value[:keep] + "*" * (len(value) - keep)

        masked_query = {
            key: [mask_value(val[0]) if key in {"usersig", "identifier", "Authorization"} else val[0]]
            for key, val in query_dict.items()
        }
        app_logger.info(f"Tencent REST API 请求 URL: {parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}")
        app_logger.info(f"Tencent REST API 请求 Query 参数: {masked_query}")
        return {"query": query_dict}

    def build_group_payload(group: Dict[str, Any]) -> Dict[str, Any]:
        # 获取原始群组ID并清理 @TGS# 前缀
        raw_group_id = group.get("GroupId") or group.get("group_id")
        cleaned_group_id = normalize_tencent_group_id(raw_group_id)
        
        # 记录群组ID清理过程（如果发生了清理）
        if raw_group_id and raw_group_id != cleaned_group_id:
            app_logger.info(f"群组ID已清理: 原始ID='{raw_group_id}' -> 清理后ID='{cleaned_group_id}'")
        
        payload: Dict[str, Any] = {
            "Owner_Account": group.get("Owner_Account"),
            "Type": normalize_tencent_group_type(group.get("Type") or group.get("group_type")),
            "GroupId": cleaned_group_id,
            "Name": group.get("Name")
        }

        optional_fields = {
            "Introduction": ["introduction", "Introduction"],
            "Notification": ["notification", "Notification"],
            "FaceUrl": ["face_url", "FaceUrl"],
            "ApplyJoinOption": ["add_option", "ApplyJoinOption"],
            "MaxMemberCount": ["max_member_num", "MaxMemberCount"],
            "AppDefinedData": ["AppDefinedData", "app_defined_data"]
        }

        for target_key, source_keys in optional_fields.items():
            for source_key in source_keys:
                value = group.get(source_key)
                if value not in (None, "", []):
                    payload[target_key] = value
                    break

        member_info = group.get("member_info") or group.get("MemberList")
        member_list = []
        if isinstance(member_info, dict):
            member_account = member_info.get("user_id") or member_info.get("Member_Account")
            if member_account:
                member_entry = {"Member_Account": member_account}
                role = member_info.get("self_role") or member_info.get("Role")
                if role:
                    member_entry["Role"] = str(role)
                member_list.append(member_entry)
        elif isinstance(member_info, list):
            for member in member_info:
                if not isinstance(member, dict):
                    continue
                member_account = member.get("user_id") or member.get("Member_Account")
                if member_account:
                    entry = {"Member_Account": member_account}
                    role = member.get("self_role") or member.get("Role")
                    if role:
                        entry["Role"] = str(role)
                    member_list.append(entry)

        if member_list:
            payload["MemberList"] = member_list
        owner_account = payload.get("Owner_Account")
        if owner_account:
            owner_present = any(m.get("Member_Account") == owner_account for m in member_list)
            if not owner_present:
                payload.setdefault("MemberList", []).append({"Member_Account": owner_account, "Role": "Owner"})

        return payload

    headers = build_tencent_headers()

    def send_http_request(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """发送 HTTP 请求到腾讯 REST API"""
        encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url=url,
            data=encoded_payload,
            headers=headers,
            method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=TENCENT_API_TIMEOUT) as response:
                raw_body = response.read()
                text_body = raw_body.decode("utf-8", errors="replace")
                try:
                    parsed_body = json.loads(text_body)
                except json.JSONDecodeError:
                    parsed_body = None

                result: Dict[str, Any] = {
                    "status": "success",
                    "http_status": response.status,
                    "response": parsed_body or text_body
                }
                return result
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            app_logger.error(f"Tencent REST API 同步失败 (HTTP {e.code}): {body}")
            return {
                "status": "error",
                "http_status": e.code,
                "error": body
            }
        except urllib.error.URLError as e:
            app_logger.error(f"Tencent REST API 调用异常: {e}")
            return {
                "status": "error",
                "http_status": None,
                "error": str(e)
            }
        except Exception as exc:
            app_logger.exception(f"Tencent REST API 未知异常: {exc}")
            return {
                "status": "error",
                "http_status": None,
                "error": str(exc)
            }

    def build_update_group_payload(group_payload: Dict[str, Any]) -> Dict[str, Any]:
        """构建更新群组信息的 payload（只包含可更新的字段）"""
        update_payload = {
            "GroupId": group_payload.get("GroupId"),
            "Name": group_payload.get("Name"),
        }
        
        # 添加可选字段
        optional_fields = {
            "Introduction": group_payload.get("Introduction"),
            "Notification": group_payload.get("Notification"),
            "FaceUrl": group_payload.get("FaceUrl"),
            "MaxMemberCount": group_payload.get("MaxMemberCount"),
            "ApplyJoinOption": group_payload.get("ApplyJoinOption"),
        }
        
        for key, value in optional_fields.items():
            if value is not None:
                update_payload[key] = value
        
        return update_payload

    def send_group_welcome_message(group_payload: Dict[str, Any]) -> None:
        """调用腾讯 REST API 发送欢迎群消息"""
        group_id = group_payload.get("GroupId")
        if not group_id:
            app_logger.warning("send_group_welcome_message: 缺少 GroupId，跳过发送欢迎消息")
            return

        group_name = (
            group_payload.get("Name")
            or group_payload.get("group_name")
            or f"{group_id}"
        )
        welcome_text = f"欢迎大家来到{group_name}里面"

        message_url = build_tencent_request_url(
            identifier=identifier_to_use,
            usersig=usersig_to_use,
            path_override="v4/group_open_http_svc/send_group_msg"
        )
        if not message_url:
            app_logger.error(f"[send_group_welcome_message] 构建 send_group_msg URL 失败，group_id={group_id}")
            print(f"[send_group_welcome_message] FAILED -> url missing, group_id={group_id}")
            return

        random_value = random.randint(1, 2**31 - 1)
        message_payload: Dict[str, Any] = {
            "GroupId": group_id,
            "Random": random_value,
            "From_Account": identifier_to_use,
            "MsgBody": [
                {
                    "MsgType": "TIMTextElem",
                    "MsgContent": {"Text": welcome_text}
                }
            ]
        }

        print(f"[send_group_welcome_message] READY -> group_id={group_id}, random={random_value}, text={welcome_text}")
        app_logger.info(
            f"[send_group_welcome_message] 准备发送欢迎消息 group_id={group_id}, random={random_value}, text={welcome_text}"
        )
        app_logger.debug(f"[send_group_welcome_message] payload={message_payload}")

        welcome_result = send_http_request(message_url, message_payload)
        app_logger.info(f"[send_group_welcome_message] 响应: {welcome_result}")

        if welcome_result.get("status") == "success" and isinstance(welcome_result.get("response"), dict):
            resp = welcome_result.get("response")
            action_status = resp.get("ActionStatus")
            if action_status == "OK":
                print(f"[send_group_welcome_message] SUCCESS -> group_id={group_id}")
                app_logger.info(f"[send_group_welcome_message] 群 {group_id} 欢迎消息发送成功 resp={resp}")
            else:
                error_info = resp.get("ErrorInfo")
                error_code = resp.get("ErrorCode")
                print(f"[send_group_welcome_message] FAIL -> group_id={group_id}, error={error_info}, code={error_code}")
                app_logger.warning(
                    f"[send_group_welcome_message] 群 {group_id} 欢迎消息失败 code={error_code}, info={error_info}, resp={resp}"
                )
        else:
            error_detail = welcome_result.get("error")
            print(f"[send_group_welcome_message] REQUEST FAIL -> group_id={group_id}, error={error_detail}")
            app_logger.error(f"[send_group_welcome_message] 群 {group_id} 欢迎消息请求失败: {welcome_result}")

    def send_single_group(group_payload: Dict[str, Any]) -> Dict[str, Any]:
        group_id = group_payload.get("GroupId", "unknown")
        print(f"[send_single_group] 准备同步群组: group_id={group_id}, 使用 identifier={identifier_to_use}")
        app_logger.info(f"准备同步群组到腾讯 REST API: group_id={group_id}, 使用 identifier={identifier_to_use}")
        
        # 构建导入群组的 URL（默认 API）
        current_url = build_tencent_request_url(identifier=identifier_to_use, usersig=usersig_to_use)
        print(f"[send_single_group] 构建的 URL (前100字符): {current_url[:100] if current_url else 'None'}...")
        if not current_url:
            return {
                "status": "error",
                "http_status": None,
                "error": "腾讯 REST API 未配置有效 URL"
            }

        validation = validate_and_log_url(current_url)
        if "error" in validation:
            return {
                "status": "error",
                "http_status": None,
                "error": validation["error"]
            }
        
        # 从 URL 中提取实际使用的 identifier，用于验证
        parsed_url = urllib.parse.urlparse(current_url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        actual_identifier = query_params.get("identifier", [None])[0]
        print(f"[send_single_group] 实际使用的 identifier (从 URL 提取): {actual_identifier}, 期望的管理员账号: {identifier_to_use}")
        app_logger.info(f"实际使用的 identifier (从 URL 提取): {actual_identifier}, 期望的管理员账号: {identifier_to_use}")

        # 先尝试导入群组
        app_logger.info(f"发送群组导入请求: group_id={group_id}, payload_keys={list(group_payload.keys())}")
        result = send_http_request(current_url, group_payload)
        
        # 检查响应中的错误信息
        if result.get("status") == "success" and isinstance(result.get("response"), dict):
            parsed_body = result.get("response")
            action_status = parsed_body.get("ActionStatus")
            error_code = parsed_body.get("ErrorCode")
            error_info = parsed_body.get("ErrorInfo")
            print(f"[send_single_group] import_group 响应: group_id={group_id}, ActionStatus={action_status}, ErrorCode={error_code}, ErrorInfo={error_info}")
            app_logger.info(f"[send_single_group] import_group 响应 group_id={group_id}: {parsed_body}")
            if action_status == "OK":
                print(f"[send_single_group] import_group 成功，准备发送欢迎消息 group_id={group_id}")
                app_logger.info(f"[send_single_group] import_group 成功，准备发送欢迎消息 group_id={group_id}")
                # 创建群成功，发送欢迎消息
                send_group_welcome_message(group_payload)
            elif action_status == "FAIL":
                print(f"[send_single_group] 腾讯 API 返回错误: ErrorCode={error_code}, ErrorInfo={error_info}")
                print(f"[send_single_group] 请求使用的 identifier: {actual_identifier}, group_id: {group_id}")
                
                # 如果是群组已存在的错误（10021），尝试使用更新 API
                if error_code == 10021:
                    print(f"[send_single_group] 群组 {group_id} 已存在，尝试使用更新 API")
                    app_logger.info(f"群组 {group_id} 已存在，切换到更新群组信息 API")
                    
                    # 构建更新群组的 URL
                    # 将 import_group 替换为 modify_group_base_info
                    if "/import_group" in current_url:
                        update_path = current_url.replace("/import_group", "/modify_group_base_info")
                    elif "/group_open_http_svc/import_group" in current_url:
                        # 如果 URL 中包含 group_open_http_svc/import_group，替换路径
                        update_path = current_url.replace("/group_open_http_svc/import_group", "/group_open_http_svc/modify_group_base_info")
                    else:
                        # 如果 URL 中没有找到 import_group，尝试从路径构建
                        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                        query_str = parsed_url.query
                        update_path = f"{base_url}/v4/group_open_http_svc/modify_group_base_info" + (f"?{query_str}" if query_str else "")
                    
                    # 构建更新群组的 payload
                    update_payload = build_update_group_payload(group_payload)
                    print(f"[send_single_group] 使用更新 API，URL: {update_path[:100]}..., payload: {list(update_payload.keys())}")
                    app_logger.info(f"使用更新群组信息 API: group_id={group_id}")
                    
                    # 发送更新请求
                    update_result = send_http_request(update_path, update_payload)
                    
                    # 检查更新结果
                    if update_result.get("status") == "success" and isinstance(update_result.get("response"), dict):
                        update_body = update_result.get("response")
                        update_action_status = update_body.get("ActionStatus")
                        if update_action_status == "OK":
                            print(f"[send_single_group] 群组 {group_id} 更新成功")
                            app_logger.info(f"群组 {group_id} 更新成功")
                            return update_result
                        else:
                            print(f"[send_single_group] 群组 {group_id} 更新失败: {update_body.get('ErrorInfo')}")
                            app_logger.warning(f"群组 {group_id} 更新失败: {update_body.get('ErrorInfo')}")
                            # 返回更新结果，即使失败也记录
                            return update_result
                    else:
                        print(f"[send_single_group] 群组 {group_id} 更新请求失败")
                        app_logger.error(f"群组 {group_id} 更新请求失败: {update_result.get('error')}")
                        # 返回原始导入结果
                        return result
            else:
                print(f"[send_single_group] import_group 返回未知状态: {parsed_body}")
                app_logger.warning(f"[send_single_group] import_group 返回未知状态 group_id={group_id}: {parsed_body}")

        app_logger.info(f"Tencent REST API 同步完成: group_id={group_id}")
        return result

    loop = asyncio.get_running_loop()
    tasks = []
    for group in payload_groups:
        group_payload = build_group_payload(group)
        app_logger.info(f"Tencent REST API 单群组请求 payload: {group_payload}")
        task = loop.run_in_executor(None, send_single_group, group_payload)
        tasks.append(task)

    group_results = await asyncio.gather(*tasks)

    success_count = sum(1 for result in group_results if result.get("status") == "success")
    error_count = len(group_results) - success_count
    overall_status = "success" if error_count == 0 else ("partial" if success_count > 0 else "error")

    return {
        "status": overall_status,
        "success_count": success_count,
        "error_count": error_count,
        "results": group_results
    }


@app.post("/tencent/user_sig")
async def create_tencent_user_sig(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            {'data': {'message': '请求体必须为 JSON', 'code': 400}},
            status_code=400
        )

    identifier = body.get("identifier") or body.get("user_id")
    expire = body.get("expire", 86400)

    if not identifier:
        return JSONResponse(
            {'data': {'message': '缺少 identifier 参数', 'code': 400}},
            status_code=400
        )

    try:
        expire_int = int(expire)
        if expire_int <= 0:
            raise ValueError("expire must be positive")
    except (ValueError, TypeError):
        return JSONResponse(
            {'data': {'message': 'expire 参数必须为正整数', 'code': 400}},
            status_code=400
        )

    try:
        user_sig = generate_tencent_user_sig(identifier, expire_int)
    except ValueError as config_error:
        app_logger.error(f"生成 UserSig 配置错误: {config_error}")
        return JSONResponse(
            {'data': {'message': str(config_error), 'code': 500}},
            status_code=500
        )
    except Exception as e:
        app_logger.exception(f"生成 UserSig 时发生异常: {e}")
        return JSONResponse(
            {'data': {'message': f'生成 UserSig 失败: {e}', 'code': 500}},
            status_code=500
        )

    response_data = {
        'identifier': identifier,
        'sdk_app_id': TENCENT_API_SDK_APP_ID,
        'expire': expire_int,
        'user_sig': user_sig
    }
    return JSONResponse({'data': response_data, 'code': 200})


@app.post("/getUserSig")
async def get_user_sig(request: Request):
    """
    获取腾讯 IM UserSig 接口
    客户端调用：POST /getUserSig
    支持 JSON 格式：{"user_id": "xxx"} 或表单格式：user_id=xxx
    返回格式：{"data": {"user_sig": "...", "usersig": "...", "sig": "..."}, "code": 200}
    """
    user_id = None
    expire = 86400
    
    # 尝试解析 JSON
    try:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            body = await request.json()
            user_id = body.get("user_id") or body.get("identifier")
            expire = body.get("expire", 86400)
        else:
            # 尝试解析表单数据
            form_data = await request.form()
            user_id_val = form_data.get("user_id") or form_data.get("identifier")
            if user_id_val:
                user_id = str(user_id_val) if not isinstance(user_id_val, str) else user_id_val
            if form_data.get("expire"):
                expire_val = form_data.get("expire")
                expire = str(expire_val) if not isinstance(expire_val, str) else expire_val
    except Exception as e:
        print(f"[getUserSig] 解析请求失败: {e}")
        app_logger.error(f"解析请求失败: {e}")
        return JSONResponse(
            {'data': {'message': '请求格式错误', 'code': 400}},
            status_code=400
        )

    if not user_id:
        return JSONResponse(
            {'data': {'message': '缺少 user_id 参数', 'code': 400}},
            status_code=400
        )

    try:
        expire_int = int(expire)
        if expire_int <= 0:
            raise ValueError("expire must be positive")
    except (ValueError, TypeError):
        return JSONResponse(
            {'data': {'message': 'expire 参数必须为正整数', 'code': 400}},
            status_code=400
        )

    try:
        user_sig = generate_tencent_user_sig(user_id, expire_int)
        print(f"[getUserSig] 为 user_id={user_id} 生成 UserSig 成功，长度: {len(user_sig)}")
        app_logger.info(f"为 user_id={user_id} 生成 UserSig 成功")
    except ValueError as config_error:
        app_logger.error(f"生成 UserSig 配置错误: {config_error}")
        return JSONResponse(
            {'data': {'message': str(config_error), 'code': 500}},
            status_code=500
        )
    except Exception as e:
        app_logger.exception(f"生成 UserSig 时发生异常: {e}")
        return JSONResponse(
            {'data': {'message': f'生成 UserSig 失败: {e}', 'code': 500}},
            status_code=500
        )

    # 返回客户端期望的格式，支持多种字段名
    response_data = {
        'user_sig': user_sig,  # 主要字段
        'usersig': user_sig,   # 备用字段
        'sig': user_sig        # 备用字段
    }
    return JSONResponse({'data': response_data, 'code': 200})


def insert_class_schedule(schedule_items: List[Dict], table_name: str = 'ta_class_schedule') -> Dict[str, object]:
    """
    批量插入课程表数据到指定表。

    要求每个字典拥有相同的键集合，键名即为表字段名；会在一个事务内批量写入。

    参数:
    - schedule_items: 课程表条目列表，每个元素为 {列名: 值} 的字典
    - table_name: 目标表名，默认 'ta_class_schedule'

    返回:
    - { 'success': bool, 'inserted': int, 'message': str }
    """
    if not schedule_items:
        return { 'success': True, 'inserted': 0, 'message': '无数据可插入' }

    # 校验列一致性
    first_keys = list(schedule_items[0].keys())
    for idx, item in enumerate(schedule_items):
        if list(item.keys()) != first_keys:
            return {
                'success': False,
                'inserted': 0,
                'message': f'第 {idx} 条与首条的列不一致，请保证所有字典的键顺序和集合一致'
            }

    columns = first_keys
    placeholders = ", ".join(["%s"] * len(columns))
    column_list_sql = ", ".join([f"`{col}`" for col in columns])
    insert_sql = f"INSERT INTO `{table_name}` ({column_list_sql}) VALUES ({placeholders})"

    values: List[tuple] = []
    for item in schedule_items:
        values.append(tuple(item.get(col) for col in columns))

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Insert class schedule failed: Database connection error.")
        return { 'success': False, 'inserted': 0, 'message': '数据库连接失败' }

    try:
        connection.start_transaction()
        cursor = connection.cursor()
        cursor.executemany(insert_sql, values)
        connection.commit()
        inserted_count = cursor.rowcount if cursor.rowcount is not None else len(values)
        return { 'success': True, 'inserted': inserted_count, 'message': '插入成功' }
    except mysql.connector.Error as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Database error during insert_class_schedule: {e}")
        return { 'success': False, 'inserted': 0, 'message': f'数据库错误: {e}' }
    except Exception as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Unexpected error during insert_class_schedule: {e}")
        return { 'success': False, 'inserted': 0, 'message': f'未知错误: {e}' }
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after inserting class schedule.")

def save_course_schedule(
    class_id: str,
    term: str,
    days,
    times,
    remark: Optional[str],
    cells: List[Dict]
) -> Dict[str, object]:
    """
    写入/更新课程表：
    1) 依据 (class_id, term) 在 course_schedule 中插入或更新 days_json/times_json/remark；
    2) 批量写入/更新 course_schedule_cell（依据唯一键 schedule_id + row_index + col_index）。

    参数说明：
    - class_id: 班级ID
    - term: 学期，如 '2025-2026-1'
    - days: 可传 list[str] 或 JSON 字符串（示例: ["周一",...,"周日"]）
    - times: 可传 list[str] 或 JSON 字符串（示例: ["6:00","8:10",...]）
    - remark: 备注，可为空
    - cells: 单元格列表，每个元素包含: { row_index:int, col_index:int, course_name:str, is_highlight:int(0/1) }

    返回：
    - { success, schedule_id, upserted_cells, message }
    """
    # 规范化 days_json/times_json
    try:
        if isinstance(days, str):
            days_json = days.strip()
        else:
            days_json = json.dumps(days, ensure_ascii=False)
        if isinstance(times, str):
            times_json = times.strip()
        else:
            times_json = json.dumps(times, ensure_ascii=False)
    except Exception as e:
        return { 'success': False, 'schedule_id': None, 'upserted_cells': 0, 'message': f'行列标签序列化失败: {e}' }

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Save course schedule failed: Database connection error.")
        return { 'success': False, 'schedule_id': None, 'upserted_cells': 0, 'message': '数据库连接失败' }

    try:
        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # 先尝试获取是否已存在该 (class_id, term)
        cursor.execute(
            "SELECT id FROM course_schedule WHERE class_id = %s AND term = %s LIMIT 1",
            (class_id, term)
        )
        row = cursor.fetchone()

        if row is None:
            # 插入头
            insert_header_sql = (
                "INSERT INTO course_schedule (class_id, term, days_json, times_json, remark) "
                "VALUES (%s, %s, %s, %s, %s)"
            )
            cursor.execute(insert_header_sql, (class_id, term, days_json, times_json, remark))
            schedule_id = cursor.lastrowid
        else:
            schedule_id = row['id']
            # 更新头（若存在）
            update_header_sql = (
                "UPDATE course_schedule SET days_json = %s, times_json = %s, remark = %s, updated_at = NOW() "
                "WHERE id = %s"
            )
            cursor.execute(update_header_sql, (days_json, times_json, remark, schedule_id))

        upsert_count = 0
        if cells:
            # 批量写入/更新单元格
            # 依赖唯一键 (schedule_id, row_index, col_index)
            # 对于 MySQL，我们用 ON DUPLICATE KEY UPDATE；如果唯一键未建，将退化为仅插入。
            insert_cell_sql = (
                "INSERT INTO course_schedule_cell (schedule_id, row_index, col_index, course_name, is_highlight) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE course_name = VALUES(course_name), is_highlight = VALUES(is_highlight)"
            )
            values = []
            for cell in cells:
                values.append((
                    schedule_id,
                    int(cell.get('row_index', 0)),
                    int(cell.get('col_index', 0)),
                    str(cell.get('course_name', '')),
                    int(cell.get('is_highlight', 0)),
                ))
            cursor.executemany(insert_cell_sql, values)
            # 在 DUPLICATE 的情况下，rowcount 可能为 2x 更新行数或实现相关，这里统一返回输入数量
            upsert_count = len(values)

        connection.commit()
        return { 'success': True, 'schedule_id': schedule_id, 'upserted_cells': upsert_count, 'message': '保存成功' }
    except mysql.connector.Error as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Database error during save_course_schedule: {e}")
        return { 'success': False, 'schedule_id': None, 'upserted_cells': 0, 'message': f'数据库错误: {e}' }
    except Exception as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Unexpected error during save_course_schedule: {e}")
        return { 'success': False, 'schedule_id': None, 'upserted_cells': 0, 'message': f'未知错误: {e}' }
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after saving course schedule.")

def save_seat_arrangement(
    class_id: str,
    seats: List[Dict]
) -> Dict[str, object]:
    """
    写入/更新座位安排：
    1) 依据 class_id 在 seat_arrangement 中插入或更新；
    2) 批量写入/更新 seat_arrangement_item（依据唯一键 arrangement_id + row + col）。

    参数说明：
    - class_id: 班级ID
    - seats: 座位列表，每个元素包含: { row:int, col:int, student_name:str, name:str, student_id:str }

    返回：
    - { success, arrangement_id, upserted_seats, message }
    """
    connection = get_db_connection()
    if connection is None:
        app_logger.error("Save seat arrangement failed: Database connection error.")
        return { 'success': False, 'arrangement_id': None, 'upserted_seats': 0, 'message': '数据库连接失败' }

    try:
        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # 先尝试获取是否已存在该 class_id
        cursor.execute(
            "SELECT id FROM seat_arrangement WHERE class_id = %s LIMIT 1",
            (class_id,)
        )
        row = cursor.fetchone()

        if row is None:
            # 插入主表
            insert_header_sql = (
                "INSERT INTO seat_arrangement (class_id) "
                "VALUES (%s)"
            )
            cursor.execute(insert_header_sql, (class_id,))
            arrangement_id = cursor.lastrowid
        else:
            arrangement_id = row['id']
            # 更新主表（更新时间戳）
            update_header_sql = (
                "UPDATE seat_arrangement SET updated_at = NOW() "
                "WHERE id = %s"
            )
            cursor.execute(update_header_sql, (arrangement_id,))

        # 始终删除旧座位
        delete_old_sql = "DELETE FROM seat_arrangement_item WHERE arrangement_id = %s"
        cursor.execute(delete_old_sql, (arrangement_id,))
        deleted_count = cursor.rowcount
        app_logger.info(f"[seat_arrangement] 删除 class_id={class_id} 的旧座位数据，arrangement_id={arrangement_id}，删除行数={deleted_count}")
        print(f"[seat_arrangement] 删除 class_id={class_id} 的旧座位数据，arrangement_id={arrangement_id}，删除行数={deleted_count}")

        upsert_count = 0
        if seats:
            # 批量插入新座位数据
            insert_seat_sql = (
                "INSERT INTO seat_arrangement_item (arrangement_id, `row`, `col`, student_name, name, student_id) "
                "VALUES (%s, %s, %s, %s, %s, %s)"
            )
            values = []
            for seat in seats:
                seat_student_id = str(seat.get('student_id', '') or '')
                seat_name = str(seat.get('name', '') or '')
                seat_full_name = seat.get('student_name')
                if not seat_full_name:
                    if seat_name and seat_student_id:
                        seat_full_name = f"{seat_name}{seat_student_id}"
                    else:
                        seat_full_name = seat_name or seat_student_id
                seat_full_name = str(seat_full_name or '')

                values.append((
                    arrangement_id,
                    int(seat.get('row', 0)),
                    int(seat.get('col', 0)),
                    seat_full_name,
                    seat_name,
                    seat_student_id,
                ))
            if values:
                cursor.executemany(insert_seat_sql, values)
                upsert_count = len(values)
                app_logger.info(f"[seat_arrangement] 插入 class_id={class_id} 的新座位数据，arrangement_id={arrangement_id}，插入行数={upsert_count}")
                print(f"[seat_arrangement] 插入 class_id={class_id} 的新座位数据，arrangement_id={arrangement_id}，插入行数={upsert_count}")

        connection.commit()
        return { 'success': True, 'arrangement_id': arrangement_id, 'upserted_seats': upsert_count, 'message': '保存成功' }
    except mysql.connector.Error as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Database error during save_seat_arrangement: {e}")
        return { 'success': False, 'arrangement_id': None, 'upserted_seats': 0, 'message': f'数据库错误: {e}' }
    except Exception as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Unexpected error during save_seat_arrangement: {e}")
        return { 'success': False, 'arrangement_id': None, 'upserted_seats': 0, 'message': f'未知错误: {e}' }
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after saving seat arrangement.")

# ===== 课程表 API =====
@app.post("/course-schedule/save")
async def api_save_course_schedule(request: Request):
    """
    保存/更新课程表
    请求体 JSON:
    {
      "class_id": "class_1001",
      "term": "2025-2026-1",
      "days": ["周一", "周二", ...],      // 或 JSON 字符串
      "times": ["08:00", "08:55", ...], // 或 JSON 字符串
      "remark": "可选",
      "cells": [
        {"row_index":0, "col_index":0, "course_name":"语文", "is_highlight":0},
        ...
      ]
    }
    """
    try:
        data = await request.json()
    except Exception:
        return safe_json_response({'message': '无效的 JSON 请求体', 'code': 400}, status_code=400)

    # 支持新字段 class_id，兼容旧字段 group_id（若两者同时提供，以 class_id 为准）
    class_id = data.get('class_id') or data.get('group_id')
    term = data.get('term')
    days = data.get('days')
    times = data.get('times')
    remark = data.get('remark')
    cells = data.get('cells', [])

    if not class_id or not term or days is None or times is None:
        return safe_json_response({'message': '缺少必要参数 class_id/term/days/times', 'code': 400}, status_code=400)

    result = save_course_schedule(
        class_id=class_id,
        term=term,
        days=days,
        times=times,
        remark=remark,
        cells=cells if isinstance(cells, list) else []
    )

    if result.get('success'):
        return safe_json_response({'message': '保存成功', 'code': 200, 'data': result})
    else:
        return safe_json_response({'message': result.get('message', '保存失败'), 'code': 500}, status_code=500)

# ===== 座位安排 API =====
async def _handle_save_seat_arrangement_payload(data: Dict[str, Any]):
    class_id = data.get('class_id')
    seats = data.get('seats', [])

    if not class_id:
        return safe_json_response({'message': '缺少必要参数 class_id', 'code': 400}, status_code=400)

    if not isinstance(seats, list):
        return safe_json_response({'message': 'seats 必须是数组', 'code': 400}, status_code=400)

    result = save_seat_arrangement(
        class_id=class_id,
        seats=seats if isinstance(seats, list) else []
    )

    if result.get('success'):
        return safe_json_response({'message': '保存成功', 'code': 200, 'data': result})
    else:
        return safe_json_response({'message': result.get('message', '保存失败'), 'code': 500}, status_code=500)


@app.post("/seat-arrangement/save")
async def api_save_seat_arrangement(request: Request):
    """
    保存学生座位信息到数据库
    请求体 JSON:
    {
      "class_id": "班级ID",
      "seats": [
        {
          "row": 1,
          "col": 1,
          "student_name": "刘峻源8-4",
          "name": "刘峻源",
          "student_id": "8-4"
        },
        ...
      ]
    }
    """
    try:
        data = await request.json()
    except Exception:
        return safe_json_response({'message': '无效的 JSON 请求体', 'code': 400}, status_code=400)

    return await _handle_save_seat_arrangement_payload(data)


@app.post("/seat-arrange")
async def api_save_seat_arrangement_alias(request: Request):
    """
    兼容旧客户端的保存座位接口，与 /seat-arrangement/save 功能相同。
    """
    try:
        data = await request.json()
    except Exception:
        return safe_json_response({'message': '无效的 JSON 请求体', 'code': 400}, status_code=400)

    return await _handle_save_seat_arrangement_payload(data)

@app.get("/seat-arrangement")
async def api_get_seat_arrangement(
    request: Request,
    class_id: str = Query(..., description="班级ID")
):
    """
    获取学生座位信息
    查询参数:
    - class_id: 班级ID
    
    返回 JSON:
    {
      "message": "查询成功",
      "code": 200,
      "data": {
        "class_id": "班级ID",
        "seats": [
          {
            "row": 1,
            "col": 1,
            "student_name": "刘峻源8-4",
            "name": "刘峻源",
            "student_id": "8-4"
          },
          ...
        ]
      }
    }
    """
    connection = get_db_connection()
    if connection is None:
        return safe_json_response({'message': '数据库连接失败', 'code': 500}, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)
        # 查询座位安排主表
        cursor.execute(
            "SELECT id, class_id, created_at, updated_at "
            "FROM seat_arrangement WHERE class_id = %s LIMIT 1",
            (class_id,)
        )
        arrangement = cursor.fetchone()
        
        if not arrangement:
            return safe_json_response({'message': '未找到座位信息', 'code': 404}, status_code=404)

        arrangement_id = arrangement['id']
        
        # 查询座位详细数据
        cursor.execute(
            "SELECT `row`, `col`, student_name, name, student_id "
            "FROM seat_arrangement_item WHERE arrangement_id = %s "
            "ORDER BY `row`, `col`",
            (arrangement_id,)
        )
        seat_items = cursor.fetchall()
        
        # 转换为前端需要的格式
        seats = []
        for item in seat_items:
            seats.append({
                "row": item['row'],
                "col": item['col'],
                "student_name": item['student_name'] or '',
                "name": item['name'] or '',
                "student_id": item['student_id'] or ''
            })
        
        return safe_json_response({
            'message': '查询成功',
            'code': 200,
            'data': {
                'class_id': class_id,
                'seats': seats
            }
        })
    except mysql.connector.Error as e:
        app_logger.error(f"Database error during api_get_seat_arrangement: {e}")
        return safe_json_response({'message': f'数据库错误: {e}', 'code': 500}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during api_get_seat_arrangement: {e}")
        return safe_json_response({'message': f'查询失败: {e}', 'code': 500}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after getting seat arrangement.")

@app.get("/course-schedule")
async def api_get_course_schedule(
    request: Request,
    class_id: str = Query(..., description="班级ID"),
    term: Optional[str] = Query(None, description="学期，如 2025-2026-1。如果不传或为空，则返回该班级所有学期的课表")
):
    """
    查询课程表：根据 (class_id, term) 返回课表头与单元格列表。
    
    如果 term 参数不传或为空，返回该班级所有学期的课表数据。
    
    返回 JSON（指定 term 时）:
    {
      "message": "查询成功",
      "code": 200,
      "data": {
        "schedule": {
          "id": 1,
          "class_id": "class_1001",
          "term": "2025-2026-1",
          "days": ["周一", ...],
          "times": ["08:00", ...],
          "remark": "...",
          "updated_at": "..."
        },
        "cells": [ {"row_index":0, "col_index":0, "course_name":"语文", "is_highlight":0}, ... ]
      }
    }
    
    返回 JSON（term 为空时，返回所有学期）:
    {
      "message": "查询成功",
      "code": 200,
      "data": [
        {
          "schedule": {
            "id": 1,
            "class_id": "class_1001",
            "term": "2025-2026-1",
            "days": ["周一", ...],
            "times": ["08:00", ...],
            "remark": "...",
            "updated_at": "..."
          },
          "cells": [ {"row_index":0, "col_index":0, "course_name":"语文", "is_highlight":0}, ... ]
        },
        {
          "schedule": {...},
          "cells": [...]
        }
      ]
    }
    """
    connection = get_db_connection()
    if connection is None:
        return safe_json_response({'message': '数据库连接失败', 'code': 500}, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)
        
        # 判断 term 是否为空或 None
        term_empty = not term or (isinstance(term, str) and term.strip() == '')
        
        if term_empty:
            # term 为空，查询该班级所有学期的课表
            cursor.execute(
                "SELECT id, class_id, term, days_json, times_json, remark, updated_at "
                "FROM course_schedule WHERE class_id = %s ORDER BY term DESC",
                (class_id,)
            )
            headers = cursor.fetchall()
            
            if not headers:
                return safe_json_response({'message': '未找到课表', 'code': 404}, status_code=404)
            
            # 获取所有学期的数据
            all_schedules = []
            for header in headers:
                schedule_id = header['id']
                
                # 解析 JSON 字段
                try:
                    days = json.loads(header['days_json']) if header.get('days_json') else []
                except Exception:
                    days = header.get('days_json') or []
                try:
                    times = json.loads(header['times_json']) if header.get('times_json') else []
                except Exception:
                    times = header.get('times_json') or []
                
                schedule = {
                    'id': schedule_id,
                    'class_id': header.get('class_id'),
                    'term': header.get('term'),
                    'days': days,
                    'times': times,
                    'remark': header.get('remark'),
                    'updated_at': header.get('updated_at')
                }
                
                # 获取该学期的单元格数据
                cursor.execute(
                    "SELECT row_index, col_index, course_name, is_highlight "
                    "FROM course_schedule_cell WHERE schedule_id = %s",
                    (schedule_id,)
                )
                rows = cursor.fetchall() or []
                cells = []
                for r in rows:
                    cells.append({
                        'row_index': r.get('row_index'),
                        'col_index': r.get('col_index'),
                        'course_name': r.get('course_name'),
                        'is_highlight': r.get('is_highlight')
                    })
                
                all_schedules.append({
                    'schedule': schedule,
                    'cells': cells
                })
            
            return safe_json_response({
                'message': '查询成功', 
                'code': 200, 
                'data': all_schedules
            })
        else:
            # term 有值，查询指定学期的课表（原逻辑）
            cursor.execute(
                "SELECT id, class_id, term, days_json, times_json, remark, updated_at "
                "FROM course_schedule WHERE class_id = %s AND term = %s LIMIT 1",
                (class_id, term)
            )
            header = cursor.fetchone()
            if not header:
                return safe_json_response({'message': '未找到课表', 'code': 404}, status_code=404)

            schedule_id = header['id']
            # 解析 JSON 字段
            try:
                days = json.loads(header['days_json']) if header.get('days_json') else []
            except Exception:
                days = header.get('days_json') or []
            try:
                times = json.loads(header['times_json']) if header.get('times_json') else []
            except Exception:
                times = header.get('times_json') or []

            schedule = {
                'id': schedule_id,
                'class_id': header.get('class_id'),
                'term': header.get('term'),
                'days': days,
                'times': times,
                'remark': header.get('remark'),
                'updated_at': header.get('updated_at')
            }

            cursor.execute(
                "SELECT row_index, col_index, course_name, is_highlight "
                "FROM course_schedule_cell WHERE schedule_id = %s",
                (schedule_id,)
            )
            rows = cursor.fetchall() or []
            cells = []
            for r in rows:
                cells.append({
                    'row_index': r.get('row_index'),
                    'col_index': r.get('col_index'),
                    'course_name': r.get('course_name'),
                    'is_highlight': r.get('is_highlight')
                })

            return safe_json_response({'message': '查询成功', 'code': 200, 'data': {'schedule': schedule, 'cells': cells}})
    except mysql.connector.Error as e:
        app_logger.error(f"Database error during api_get_course_schedule: {e}")
        return safe_json_response({'message': '数据库错误', 'code': 500}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during api_get_course_schedule: {e}")
        return safe_json_response({'message': '未知错误', 'code': 500}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching course schedule.")

# ===== 学生成绩表 API =====
def save_student_scores(
    class_id: str,
    exam_name: str,
    term: Optional[str] = None,
    remark: Optional[str] = None,
    scores: List[Dict] = None,
    excel_file_url: Optional[str] = None,
    excel_file_name: Optional[str] = None
) -> Dict[str, object]:
    """
    保存学生成绩表
    参数说明：
    - class_id: 班级ID（必需）
    - exam_name: 考试名称（必需，如"期中考试"、"期末考试"）
    - term: 学期（可选，如 '2025-2026-1'）
    - remark: 备注（可选）
    - excel_file_url: Excel文件在OSS的URL（可选）
    - excel_file_name: Excel文件名（可选，用于管理多个文件）
    - scores: 成绩明细列表，每个元素包含:
      {
        'student_id': str,      # 学号（可选）
        'student_name': str,    # 姓名（必需）
        'chinese': int,         # 语文成绩（可选）
        'math': int,            # 数学成绩（可选）
        'english': int,         # 英语成绩（可选）
        'total_score': float    # 总分（可选，可自动计算）
      }
    
    返回：
    - { success, score_header_id, inserted_count, message }
    """
    if not class_id or not exam_name:
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '缺少必要参数 class_id 或 exam_name' }
    
    if not scores or not isinstance(scores, list):
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '成绩明细列表不能为空' }

    print(f"[save_student_scores] 开始保存成绩 - class_id={class_id}, exam_name={exam_name}, term={term}, scores数量={len(scores) if scores else 0}")
    app_logger.info(f"[save_student_scores] 开始保存成绩 - class_id={class_id}, exam_name={exam_name}, term={term}, scores数量={len(scores) if scores else 0}")
    
    connection = get_db_connection()
    if connection is None:
        error_msg = "Save student scores failed: Database connection error."
        print(f"[save_student_scores] 错误: {error_msg}")
        app_logger.error(error_msg)
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '数据库连接失败' }

    print(f"[save_student_scores] 数据库连接成功，开始事务")
    app_logger.info(f"[save_student_scores] 数据库连接成功，开始事务")
    try:
        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # 1. 插入或获取成绩表头
        print(f"[save_student_scores] 查询成绩表头 - class_id={class_id}, exam_name={exam_name}, term={term}")
        app_logger.info(f"[save_student_scores] 查询成绩表头 - class_id={class_id}, exam_name={exam_name}, term={term}")
        cursor.execute(
            "SELECT id, excel_file_url FROM ta_student_score_header WHERE class_id = %s AND exam_name = %s AND (%s IS NULL OR term = %s) LIMIT 1",
            (class_id, exam_name, term, term)
        )
        header_row = cursor.fetchone()
        print(f"[save_student_scores] 查询成绩表头结果: {header_row}")
        app_logger.info(f"[save_student_scores] 查询成绩表头结果: {header_row}")

        if header_row is None:
            # 插入新表头
            print(f"[save_student_scores] ========== 插入新成绩表头 ==========")
            app_logger.info(f"[save_student_scores] ========== 插入新成绩表头 ==========")
            print(f"[save_student_scores] 📝 准备插入新表头:")
            print(f"[save_student_scores]   - class_id: {class_id}")
            print(f"[save_student_scores]   - exam_name: {exam_name}")
            print(f"[save_student_scores]   - term: {term}")
            print(f"[save_student_scores]   - remark: {remark}")
            print(f"[save_student_scores]   - excel_file_url: {excel_file_url}")
            print(f"[save_student_scores]   - excel_file_name: {excel_file_name}")
            print(f"[save_student_scores]   - excel_file_url类型: {type(excel_file_url)}")
            app_logger.info(f"[save_student_scores] 📝 准备插入新表头 - class_id={class_id}, exam_name={exam_name}, term={term}, remark={remark}, excel_file_url={excel_file_url}, excel_file_name={excel_file_name}, excel_file_url类型={type(excel_file_url)}")
            
            # 如果有excel_file_url，使用JSON格式存储（支持多个文件）
            final_excel_file_url = None
            if excel_file_url:
                if excel_file_name:
                    # 使用文件名作为key
                    url_dict = {excel_file_name: excel_file_url}
                else:
                    # 如果没有文件名，使用默认key
                    timestamp = int(time.time())
                    url_dict = {f"excel_file_{timestamp}": excel_file_url}
                final_excel_file_url = json.dumps(url_dict, ensure_ascii=False)
                print(f"[save_student_scores] 📝 新表头的excel_file_url（JSON格式）: {final_excel_file_url}")
                app_logger.info(f"[save_student_scores] 📝 新表头的excel_file_url（JSON格式）: {final_excel_file_url}")
            else:
                final_excel_file_url = excel_file_url
            
            insert_header_sql = (
                "INSERT INTO ta_student_score_header (class_id, exam_name, term, remark, excel_file_url, created_at) "
                "VALUES (%s, %s, %s, %s, %s, NOW())"
            )
            print(f"[save_student_scores] 📝 SQL语句: {insert_header_sql}")
            print(f"[save_student_scores] 📝 SQL参数: ({class_id}, {exam_name}, {term}, {remark}, {final_excel_file_url})")
            app_logger.info(f"[save_student_scores] 📝 SQL语句: {insert_header_sql}, SQL参数: ({class_id}, {exam_name}, {term}, {remark}, {final_excel_file_url})")
            cursor.execute(insert_header_sql, (class_id, exam_name, term, remark, final_excel_file_url))
            score_header_id = cursor.lastrowid
            print(f"[save_student_scores] ✅ 插入成绩表头成功 - score_header_id={score_header_id}")
            print(f"[save_student_scores] ✅ excel_file_url已写入数据库: {excel_file_url}")
            app_logger.info(f"[save_student_scores] ✅ 插入成绩表头成功 - score_header_id={score_header_id}, excel_file_url={excel_file_url}")
        else:
            score_header_id = header_row['id']
            print(f"[save_student_scores] ========== 成绩表头已存在，准备更新 ==========")
            app_logger.info(f"[save_student_scores] ========== 成绩表头已存在，准备更新 ==========")
            print(f"[save_student_scores] 📋 现有表头ID: {score_header_id}")
            app_logger.info(f"[save_student_scores] 📋 现有表头ID: {score_header_id}")
            # 更新表头信息（若存在）
            update_fields = []
            update_values = []
            if remark is not None:
                update_fields.append("remark = %s")
                update_values.append(remark)
                print(f"[save_student_scores] 📝 将更新remark字段: {remark}")
                app_logger.info(f"[save_student_scores] 📝 将更新remark字段: {remark}")
            # 更新 excel_file_url（如果提供了有效的 URL）
            # 支持多个Excel文件的URL管理：如果文件名相同则更新，否则追加
            print(f"[save_student_scores] 🔍 检查excel_file_url是否需要更新:")
            print(f"[save_student_scores]   - excel_file_url值: {excel_file_url}")
            print(f"[save_student_scores]   - excel_file_name值: {excel_file_name}")
            print(f"[save_student_scores]   - excel_file_url类型: {type(excel_file_url)}")
            app_logger.info(f"[save_student_scores] 🔍 检查excel_file_url是否需要更新: excel_file_url={excel_file_url}, excel_file_name={excel_file_name}, 类型={type(excel_file_url)}")
            
            if excel_file_url:
                # 获取现有的excel_file_url值
                existing_excel_file_url = header_row.get('excel_file_url') if header_row else None
                print(f"[save_student_scores] 📋 现有的excel_file_url值: {existing_excel_file_url}")
                app_logger.info(f"[save_student_scores] 📋 现有的excel_file_url值: {existing_excel_file_url}")
                
                # 解析现有的URL列表（JSON格式：{"文件名1": "URL1", "文件名2": "URL2"}）
                url_dict = {}
                if existing_excel_file_url:
                    try:
                        # 尝试解析为JSON对象
                        url_dict = json.loads(existing_excel_file_url)
                        if not isinstance(url_dict, dict):
                            # 如果不是字典，可能是旧的单个URL格式，转换为字典
                            url_dict = {}
                            # 尝试从旧格式中提取文件名（如果有的话）
                            if excel_file_name:
                                url_dict[excel_file_name] = existing_excel_file_url
                            else:
                                url_dict['excel_file'] = existing_excel_file_url
                        print(f"[save_student_scores] ✅ 成功解析现有的URL字典: {url_dict}")
                        app_logger.info(f"[save_student_scores] ✅ 成功解析现有的URL字典: {url_dict}")
                    except (json.JSONDecodeError, TypeError):
                        # 如果解析失败，说明是旧的单个URL格式
                        print(f"[save_student_scores] ⚠️ 现有值不是JSON格式，转换为字典格式")
                        app_logger.warning(f"[save_student_scores] ⚠️ 现有值不是JSON格式，转换为字典格式")
                        if excel_file_name:
                            url_dict[excel_file_name] = existing_excel_file_url
                        else:
                            url_dict['excel_file'] = existing_excel_file_url
                
                # 更新或添加新的URL
                if excel_file_name:
                    # 如果提供了文件名，使用文件名作为key
                    url_dict[excel_file_name] = excel_file_url
                    print(f"[save_student_scores] 📝 更新/添加URL: {excel_file_name} -> {excel_file_url}")
                    app_logger.info(f"[save_student_scores] 📝 更新/添加URL: {excel_file_name} -> {excel_file_url}")
                else:
                    # 如果没有提供文件名，使用默认key
                    timestamp = int(time.time())
                    default_key = f"excel_file_{timestamp}"
                    url_dict[default_key] = excel_file_url
                    print(f"[save_student_scores] 📝 添加URL（无文件名）: {default_key} -> {excel_file_url}")
                    app_logger.info(f"[save_student_scores] 📝 添加URL（无文件名）: {default_key} -> {excel_file_url}")
                
                # 将字典转换为JSON字符串保存
                updated_excel_file_url = json.dumps(url_dict, ensure_ascii=False)
                print(f"[save_student_scores] ✅ 更新后的excel_file_url（JSON格式）: {updated_excel_file_url}")
                app_logger.info(f"[save_student_scores] ✅ 更新后的excel_file_url（JSON格式）: {updated_excel_file_url}")
                
                update_fields.append("excel_file_url = %s")
                update_values.append(updated_excel_file_url)
            else:
                print(f"[save_student_scores] ⚠️ excel_file_url为空或None，不更新该字段，保留原有值")
                app_logger.info(f"[save_student_scores] ⚠️ excel_file_url为空或None，不更新该字段，保留原有值")
            if update_fields:
                update_values.append(score_header_id)
                update_sql = f"UPDATE ta_student_score_header SET {', '.join(update_fields)}, updated_at = NOW() WHERE id = %s"
                print(f"[save_student_scores] 📝 准备执行UPDATE SQL:")
                print(f"[save_student_scores]   - SQL语句: {update_sql}")
                print(f"[save_student_scores]   - 更新字段: {', '.join(update_fields)}")
                print(f"[save_student_scores]   - SQL参数: {tuple(update_values)}")
                app_logger.info(f"[save_student_scores] 📝 准备执行UPDATE SQL: {update_sql}, 更新字段: {', '.join(update_fields)}, SQL参数: {tuple(update_values)}")
                cursor.execute(update_sql, tuple(update_values))
                print(f"[save_student_scores] ✅ UPDATE执行成功，影响行数: {cursor.rowcount}")
                app_logger.info(f"[save_student_scores] ✅ UPDATE执行成功，影响行数: {cursor.rowcount}")
                if excel_file_url:
                    print(f"[save_student_scores] ✅ excel_file_url已更新到数据库: {excel_file_url}")
                    app_logger.info(f"[save_student_scores] ✅ excel_file_url已更新到数据库: {excel_file_url}")
            else:
                print(f"[save_student_scores] ℹ️ 没有需要更新的字段")
                app_logger.info(f"[save_student_scores] ℹ️ 没有需要更新的字段")
            # 不删除旧的成绩明细和字段定义，而是追加新的数据
            print(f"[save_student_scores] 表头已存在，将追加新的字段定义和成绩明细 - score_header_id={score_header_id}")
            app_logger.info(f"[save_student_scores] 表头已存在，将追加新的字段定义和成绩明细 - score_header_id={score_header_id}")

        # 2. 打印scores数据用于调试
        print(f"[save_student_scores] ========== 收到scores数据 ==========")
        print(f"[save_student_scores] scores数量: {len(scores)}")
        for idx, score_item in enumerate(scores):
            print(f"[save_student_scores] 第{idx+1}条: {json.dumps(score_item, ensure_ascii=False)}")
        print(f"[save_student_scores] =====================================")
        app_logger.info(f"[save_student_scores] 收到scores数据: {json.dumps(scores, ensure_ascii=False, indent=2)}")
        
        # 3. 从scores数据中提取所有字段名（除了student_id和student_name）
        print(f"[save_student_scores] 开始提取字段定义 - score_header_id={score_header_id}, 待处理数量={len(scores)}")
        app_logger.info(f"[save_student_scores] 开始提取字段定义 - score_header_id={score_header_id}, 待处理数量={len(scores)}")
        
        # 收集所有出现的字段名
        field_set = set()
        for score_item in scores:
            for key in score_item.keys():
                if key not in ['student_id', 'student_name']:
                    field_set.add(key)
        
        field_list = sorted(list(field_set))  # 排序以保证一致性
        print(f"[save_student_scores] 提取到的字段: {field_list}")
        app_logger.info(f"[save_student_scores] 提取到的字段: {field_list}")
        
        # 4. 查询现有字段定义，获取最大field_order
        cursor.execute(
            "SELECT MAX(field_order) as max_order FROM ta_student_score_field WHERE score_header_id = %s",
            (score_header_id,)
        )
        max_order_result = cursor.fetchone()
        max_order = max_order_result['max_order'] if max_order_result and max_order_result['max_order'] is not None else 0
        print(f"[save_student_scores] 现有字段最大顺序: {max_order}")
        app_logger.info(f"[save_student_scores] 现有字段最大顺序: {max_order}")
        
        # 5. 保存字段定义到ta_student_score_field表（追加，不删除旧的）
        if field_list:
            insert_field_sql = (
                "INSERT INTO ta_student_score_field "
                "(score_header_id, field_name, field_type, field_order, is_total) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE field_name = field_name"  # 如果字段已存在，不更新
            )
            new_field_count = 0
            for idx, field_name in enumerate(field_list):
                # 检查字段是否已存在
                cursor.execute(
                    "SELECT id FROM ta_student_score_field WHERE score_header_id = %s AND field_name = %s",
                    (score_header_id, field_name)
                )
                existing_field = cursor.fetchone()
                
                if not existing_field:
                    # 字段不存在，插入新字段
                    is_total = 1 if '总分' in field_name or 'total' in field_name.lower() else 0
                    cursor.execute(insert_field_sql, (
                        score_header_id,
                        field_name,
                        'number',  # 默认为数字类型
                        max_order + idx + 1,   # 字段顺序（追加到现有字段后面）
                        is_total
                    ))
                    new_field_count += 1
                    print(f"[save_student_scores] 新增字段: {field_name} (顺序: {max_order + idx + 1})")
                    app_logger.info(f"[save_student_scores] 新增字段: {field_name} (顺序: {max_order + idx + 1})")
                else:
                    print(f"[save_student_scores] 字段已存在，跳过: {field_name}")
                    app_logger.info(f"[save_student_scores] 字段已存在，跳过: {field_name}")
            
            print(f"[save_student_scores] 字段定义保存完成 - 新增{new_field_count}个字段，跳过{len(field_list) - new_field_count}个已存在字段")
            app_logger.info(f"[save_student_scores] 字段定义保存完成 - 新增{new_field_count}个字段，跳过{len(field_list) - new_field_count}个已存在字段")

        # 6. 批量插入或更新成绩明细（使用JSON格式存储动态字段）
        print(f"[save_student_scores] 开始插入/更新成绩明细 - score_header_id={score_header_id}, 待处理数量={len(scores)}")
        app_logger.info(f"[save_student_scores] 开始插入/更新成绩明细 - score_header_id={score_header_id}, 待处理数量={len(scores)}")
        
        # 使用 INSERT ... ON DUPLICATE KEY UPDATE 来支持插入或更新
        # 注意：需要根据student_id和student_name来判断是否已存在
        insert_detail_sql = (
            "INSERT INTO ta_student_score_detail "
            "(score_header_id, student_id, student_name, scores_json, total_score) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "scores_json = VALUES(scores_json), "
            "total_score = VALUES(total_score), "
            "updated_at = NOW()"
        )
        
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        
        for idx, score_item in enumerate(scores):
            student_id = score_item.get('student_id')
            student_name = score_item.get('student_name', '').strip()
            if not student_name:
                skipped_count += 1
                print(f"[save_student_scores] 跳过第{idx+1}条记录：缺少学生姓名 - score_item={score_item}")
                app_logger.warning(f"[save_student_scores] 跳过第{idx+1}条记录：缺少学生姓名 - score_item={score_item}")
                continue  # 跳过没有姓名的记录
            
            # 检查该学生是否已有成绩记录
            check_sql = (
                "SELECT id, scores_json FROM ta_student_score_detail "
                "WHERE score_header_id = %s AND student_name = %s "
                "AND (%s IS NULL OR student_id = %s) "
                "LIMIT 1"
            )
            cursor.execute(check_sql, (score_header_id, student_name, student_id, student_id))
            existing_record = cursor.fetchone()
            
            # 构建JSON对象（包含除student_id和student_name外的所有字段）
            scores_json = {}
            total_score = None
            for key, value in score_item.items():
                if key not in ['student_id', 'student_name']:
                    if value is not None:
                        # 尝试转换为数字
                        try:
                            if isinstance(value, (int, float)):
                                scores_json[key] = float(value)
                            elif isinstance(value, str) and value.strip():
                                # 尝试解析为数字
                                scores_json[key] = float(value.strip())
                            else:
                                scores_json[key] = value
                        except (ValueError, TypeError):
                            scores_json[key] = value
                    
                    # 检查是否为总分字段
                    if ('总分' in key or 'total' in key.lower()) and value is not None:
                        try:
                            total_score = float(value)
                        except (ValueError, TypeError):
                            pass
            
            # 如果记录已存在，合并JSON数据（保留旧字段，添加新字段）
            if existing_record and existing_record.get('scores_json'):
                try:
                    existing_json = json.loads(existing_record['scores_json']) if isinstance(existing_record['scores_json'], str) else existing_record['scores_json']
                    # 合并JSON：新字段覆盖旧字段，保留旧字段中没有的字段
                    merged_json = {**existing_json, **scores_json}
                    scores_json = merged_json
                    print(f"[save_student_scores] 合并已有成绩数据 - student_name={student_name}, 旧字段数={len(existing_json)}, 新字段数={len(scores_json)}")
                    app_logger.info(f"[save_student_scores] 合并已有成绩数据 - student_name={student_name}")
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"[save_student_scores] 解析已有JSON失败，使用新数据 - student_name={student_name}, error={e}")
                    app_logger.warning(f"[save_student_scores] 解析已有JSON失败，使用新数据 - student_name={student_name}, error={e}")
            
            # 如果没有找到总分字段，自动计算总分（所有数字字段的和）
            if total_score is None:
                total_score = 0.0
                for key, value in scores_json.items():
                    if isinstance(value, (int, float)):
                        total_score += float(value)
                if total_score == 0.0:
                    total_score = None  # 如果所有值都是0或没有值，设为None
            
            # 将scores_json转换为JSON字符串
            scores_json_str = json.dumps(scores_json, ensure_ascii=False)
            
            is_update = existing_record is not None
            action = "更新" if is_update else "插入"
            print(f"[save_student_scores] {action}第{idx+1}条成绩 - student_name={student_name}, student_id={student_id}, scores_json={scores_json_str}, total_score={total_score}")
            app_logger.info(f"[save_student_scores] {action}第{idx+1}条成绩 - student_name={student_name}, student_id={student_id}, scores_json={scores_json_str}, total_score={total_score}")
            
            try:
                # 如果记录已存在，使用UPDATE语句
                if existing_record:
                    update_detail_sql = (
                        "UPDATE ta_student_score_detail "
                        "SET scores_json = %s, total_score = %s, updated_at = NOW() "
                        "WHERE id = %s"
                    )
                    cursor.execute(update_detail_sql, (
                        scores_json_str,
                        total_score,
                        existing_record['id']
                    ))
                    updated_count += 1
                    print(f"[save_student_scores] 第{idx+1}条成绩更新成功 - rowcount={cursor.rowcount}")
                else:
                    # 新记录，使用INSERT
                    cursor.execute(insert_detail_sql, (
                        score_header_id,
                        student_id,
                        student_name,
                        scores_json_str,
                        total_score
                    ))
                    inserted_count += 1
                    print(f"[save_student_scores] 第{idx+1}条成绩插入成功 - rowcount={cursor.rowcount}")
            except Exception as insert_error:
                print(f"[save_student_scores] 第{idx+1}条成绩{action}失败 - student_name={student_name}, error={insert_error}")
                app_logger.error(f"[save_student_scores] 第{idx+1}条成绩{action}失败 - student_name={student_name}, error={insert_error}", exc_info=True)
                raise  # 重新抛出异常，让外层捕获

        print(f"[save_student_scores] 成绩明细处理完成 - 插入={inserted_count}, 更新={updated_count}, 跳过={skipped_count}, 总计={len(scores)}")
        app_logger.info(f"[save_student_scores] 成绩明细处理完成 - 插入={inserted_count}, 更新={updated_count}, 跳过={skipped_count}, 总计={len(scores)}")
        
        print(f"[save_student_scores] 开始提交事务")
        app_logger.info(f"[save_student_scores] 开始提交事务")
        connection.commit()
        total_processed = inserted_count + updated_count
        print(f"[save_student_scores] 事务提交成功 - score_header_id={score_header_id}, 插入={inserted_count}, 更新={updated_count}, 总计={total_processed}")
        app_logger.info(f"[save_student_scores] 事务提交成功 - score_header_id={score_header_id}, 插入={inserted_count}, 更新={updated_count}, 总计={total_processed}")
        return { 'success': True, 'score_header_id': score_header_id, 'inserted_count': inserted_count, 'updated_count': updated_count, 'message': '保存成功' }
    except mysql.connector.Error as e:
        if connection and connection.is_connected():
            print(f"[save_student_scores] 数据库错误，回滚事务 - error={e}")
            app_logger.error(f"[save_student_scores] 数据库错误，回滚事务 - error={e}")
            connection.rollback()
        else:
            print(f"[save_student_scores] 数据库错误，连接已断开 - error={e}")
            app_logger.error(f"[save_student_scores] 数据库错误，连接已断开 - error={e}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[save_student_scores] 数据库错误堆栈:\n{traceback_str}")
        app_logger.error(f"Database error during save_student_scores: {e}\n{traceback_str}")
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': f'数据库错误: {e}' }
    except Exception as e:
        if connection and connection.is_connected():
            print(f"[save_student_scores] 未知错误，回滚事务 - error={e}")
            app_logger.error(f"[save_student_scores] 未知错误，回滚事务 - error={e}")
            connection.rollback()
        else:
            print(f"[save_student_scores] 未知错误，连接已断开 - error={e}")
            app_logger.error(f"[save_student_scores] 未知错误，连接已断开 - error={e}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[save_student_scores] 未知错误堆栈:\n{traceback_str}")
        app_logger.error(f"Unexpected error during save_student_scores: {e}\n{traceback_str}")
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': f'未知错误: {e}' }
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after saving student scores.")

@app.post("/student-scores/save")
async def api_save_student_scores(request: Request):
    """
    保存学生成绩表
    支持两种请求格式：
    1. application/json: 直接发送JSON数据
    2. multipart/form-data: 包含data字段（JSON字符串）和excel_file字段（Excel文件）
    
    请求体 JSON (或multipart中的data字段):
    {
      "class_id": "class_1001",
      "exam_name": "期中考试",
      "term": "2025-2026-1",  // 可选
      "remark": "备注信息",    // 可选
      "excel_file_name": "成绩表.xlsx",  // 可选，Excel文件名
      "scores": [
        {
          "student_id": "2024001",    // 可选
          "student_name": "张三",
          "chinese": 100,
          "math": 89,
          "english": 95,
          "total_score": 284           // 可选，会自动计算
        },
        {
          "student_name": "李四",
          "chinese": 90,
          "math": 78
          // total_score 会自动计算为 168
        }
      ]
    }
    """
    data = None
    excel_file = None
    excel_file_name = None
    excel_file_url = None
    
    # 检查Content-Type
    content_type = request.headers.get("content-type", "").lower()
    
    if "multipart/form-data" in content_type:
        # 处理multipart/form-data格式
        try:
            form_data = await request.form()
            
            # 获取JSON数据（从data字段）
            data_str = form_data.get("data")
            if not data_str:
                return safe_json_response({'message': 'multipart请求中缺少data字段', 'code': 400}, status_code=400)
            
            # 解析JSON字符串（form_data.get返回的可能是字符串）
            if isinstance(data_str, str):
                data = json.loads(data_str)
            else:
                # 如果不是字符串，尝试转换为字符串再解析
                data = json.loads(str(data_str))
            
            # 获取Excel文件（可选）
            excel_file = form_data.get("excel_file")
            excel_file_url = None
            print(f"[student-scores/save] ========== 开始处理Excel文件 ==========")
            app_logger.info(f"[student-scores/save] ========== 开始处理Excel文件 ==========")
            print(f"[student-scores/save] excel_file是否存在: {excel_file is not None}")
            app_logger.info(f"[student-scores/save] excel_file是否存在: {excel_file is not None}")
            if excel_file:
                print(f"[student-scores/save] excel_file类型: {type(excel_file)}")
                print(f"[student-scores/save] excel_file类型名称: {type(excel_file).__name__}")
                print(f"[student-scores/save] excel_file模块: {type(excel_file).__module__}")
                app_logger.info(f"[student-scores/save] excel_file类型: {type(excel_file)}, 类型名称: {type(excel_file).__name__}, 模块: {type(excel_file).__module__}")
                
                # 检查是否是UploadFile类型（支持fastapi.UploadFile和starlette.datastructures.UploadFile）
                is_upload_file = isinstance(excel_file, UploadFile) or type(excel_file).__name__ == 'UploadFile'
                print(f"[student-scores/save] isinstance(excel_file, UploadFile): {isinstance(excel_file, UploadFile)}")
                print(f"[student-scores/save] type(excel_file).__name__ == 'UploadFile': {type(excel_file).__name__ == 'UploadFile'}")
                print(f"[student-scores/save] is_upload_file: {is_upload_file}")
                app_logger.info(f"[student-scores/save] is_upload_file检查结果: {is_upload_file}")
                
                if is_upload_file:
                    filename_value = getattr(excel_file, 'filename', None)
                    print(f"[student-scores/save] excel_file.filename值: {filename_value}")
                    print(f"[student-scores/save] excel_file.filename类型: {type(filename_value)}")
                    app_logger.info(f"[student-scores/save] excel_file.filename值: {filename_value}, 类型: {type(filename_value)}")
                    
                    # 优先使用客户端JSON中的excel_file_name字段
                    # 如果JSON中没有，再使用excel_file.filename
                    # 如果都没有，使用默认名称
                    excel_file_name = None
                    if data:
                        excel_file_name = data.get('excel_file_name')
                        if excel_file_name:
                            print(f"[student-scores/save] ✅ 从JSON数据中获取excel_file_name: {excel_file_name}")
                            app_logger.info(f"[student-scores/save] ✅ 从JSON数据中获取excel_file_name: {excel_file_name}")
                    
                    # 如果JSON中没有，尝试使用excel_file.filename
                    if not excel_file_name and filename_value:
                        excel_file_name = filename_value
                        print(f"[student-scores/save] ✅ 使用excel_file.filename: {excel_file_name}")
                        app_logger.info(f"[student-scores/save] ✅ 使用excel_file.filename: {excel_file_name}")
                    
                    # 如果都没有，使用默认名称
                    if not excel_file_name:
                        timestamp = int(time.time())
                        excel_file_name = f"excel_{timestamp}.xlsx"
                        print(f"[student-scores/save] ⚠️ 使用默认文件名: {excel_file_name}")
                        app_logger.warning(f"[student-scores/save] ⚠️ 使用默认文件名: {excel_file_name}")
                    
                    print(f"[student-scores/save] 📋 最终使用的文件名: {excel_file_name}")
                    app_logger.info(f"[student-scores/save] 📋 最终使用的文件名: {excel_file_name}")
                    
                    # 读取Excel文件内容
                    try:
                        print(f"[student-scores/save] 📖 开始读取Excel文件内容...")
                        app_logger.info(f"[student-scores/save] 📖 开始读取Excel文件内容...")
                        excel_content = await excel_file.read()
                        print(f"[student-scores/save] ✅ Excel文件读取成功，文件大小: {len(excel_content)} bytes")
                        app_logger.info(f"[student-scores/save] ✅ Excel文件读取成功，文件大小: {len(excel_content)} bytes")
                        
                        # 生成OSS对象名称（使用时间戳和文件名避免冲突）
                        timestamp = int(time.time())
                        file_ext = os.path.splitext(excel_file_name)[1] or '.xlsx'
                        oss_object_name = f"excel/student-scores/{timestamp}_{excel_file_name}"
                        print(f"[student-scores/save] 📝 生成OSS对象名称: {oss_object_name}")
                        app_logger.info(f"[student-scores/save] 📝 生成OSS对象名称: {oss_object_name}")
                        
                        # 上传到阿里云OSS
                        print(f"[student-scores/save] ☁️ 开始上传Excel文件到阿里云OSS...")
                        print(f"[student-scores/save] ☁️ OSS对象名称: {oss_object_name}")
                        app_logger.info(f"[student-scores/save] ☁️ 开始上传Excel文件到阿里云OSS: {oss_object_name}")
                        excel_file_url = upload_excel_to_oss(excel_content, oss_object_name)
                        
                        print(f"[student-scores/save] ========== Excel文件上传结果 ==========")
                        app_logger.info(f"[student-scores/save] ========== Excel文件上传结果 ==========")
                        print(f"[student-scores/save] upload_excel_to_oss返回值类型: {type(excel_file_url)}")
                        app_logger.info(f"[student-scores/save] upload_excel_to_oss返回值类型: {type(excel_file_url)}")
                        print(f"[student-scores/save] upload_excel_to_oss返回值: {excel_file_url}")
                        app_logger.info(f"[student-scores/save] upload_excel_to_oss返回值: {excel_file_url}")
                        
                        if excel_file_url:
                            print(f"[student-scores/save] ✅ Excel文件上传成功！")
                            print(f"[student-scores/save] ✅ 阿里云OSS URL: {excel_file_url}")
                            app_logger.info(f"[student-scores/save] ✅ Excel文件上传成功，OSS URL: {excel_file_url}")
                        else:
                            print(f"[student-scores/save] ❌ Excel文件上传失败，返回值为None或空")
                            app_logger.warning(f"[student-scores/save] ❌ Excel文件上传失败，返回值为None或空")
                    except Exception as e:
                        error_msg = f'读取或上传Excel文件时出错: {str(e)}'
                        print(f"[student-scores/save] ❌ 错误: {error_msg}")
                        app_logger.error(f"[student-scores/save] ❌ {error_msg}", exc_info=True)
                        import traceback
                        traceback_str = traceback.format_exc()
                        print(f"[student-scores/save] ❌ 错误堆栈:\n{traceback_str}")
                        app_logger.error(f"[student-scores/save] ❌ 错误堆栈:\n{traceback_str}")
                        # 继续处理，不阻止成绩数据保存
                else:
                    # 即使不是标准的UploadFile类型，也尝试处理（可能是其他类型的文件对象）
                    print(f"[student-scores/save] ⚠️ Excel文件类型检查未通过，但尝试继续处理")
                    print(f"[student-scores/save] ⚠️ 文件对象类型: {type(excel_file)}, 类型名称: {type(excel_file).__name__}")
                    app_logger.warning(f"[student-scores/save] ⚠️ Excel文件类型检查未通过，但尝试继续处理，类型: {type(excel_file)}")
                    
                    # 尝试从JSON数据中获取文件名
                    excel_file_name = None
                    if data:
                        excel_file_name = data.get('excel_file_name')
                        if excel_file_name:
                            print(f"[student-scores/save] ✅ 从JSON数据中获取excel_file_name: {excel_file_name}")
                            app_logger.info(f"[student-scores/save] ✅ 从JSON数据中获取excel_file_name: {excel_file_name}")
                    
                    # 如果JSON中没有，使用默认名称
                    if not excel_file_name:
                        timestamp = int(time.time())
                        excel_file_name = f"excel_{timestamp}.xlsx"
                        print(f"[student-scores/save] ⚠️ 使用默认文件名: {excel_file_name}")
                        app_logger.warning(f"[student-scores/save] ⚠️ 使用默认文件名: {excel_file_name}")
                    
                    # 尝试读取文件内容（如果对象有read方法）
                    try:
                        if hasattr(excel_file, 'read'):
                            print(f"[student-scores/save] 📖 尝试读取文件内容（使用read方法）...")
                            app_logger.info(f"[student-scores/save] 📖 尝试读取文件内容（使用read方法）...")
                            if asyncio.iscoroutinefunction(excel_file.read):
                                excel_content = await excel_file.read()
                            else:
                                excel_content = excel_file.read()
                            
                            print(f"[student-scores/save] ✅ 文件读取成功，文件大小: {len(excel_content)} bytes")
                            app_logger.info(f"[student-scores/save] ✅ 文件读取成功，文件大小: {len(excel_content)} bytes")
                            
                            # 生成OSS对象名称
                            timestamp = int(time.time())
                            oss_object_name = f"excel/student-scores/{timestamp}_{excel_file_name}"
                            print(f"[student-scores/save] 📝 生成OSS对象名称: {oss_object_name}")
                            app_logger.info(f"[student-scores/save] 📝 生成OSS对象名称: {oss_object_name}")
                            
                            # 上传到阿里云OSS
                            print(f"[student-scores/save] ☁️ 开始上传Excel文件到阿里云OSS...")
                            app_logger.info(f"[student-scores/save] ☁️ 开始上传Excel文件到阿里云OSS: {oss_object_name}")
                            excel_file_url = upload_excel_to_oss(excel_content, oss_object_name)
                            
                            if excel_file_url:
                                print(f"[student-scores/save] ✅ Excel文件上传成功，OSS URL: {excel_file_url}")
                                app_logger.info(f"[student-scores/save] ✅ Excel文件上传成功，OSS URL: {excel_file_url}")
                            else:
                                print(f"[student-scores/save] ❌ Excel文件上传失败")
                                app_logger.warning(f"[student-scores/save] ❌ Excel文件上传失败")
                        else:
                            print(f"[student-scores/save] ❌ 文件对象没有read方法，无法读取")
                            app_logger.error(f"[student-scores/save] ❌ 文件对象没有read方法，无法读取")
                    except Exception as e:
                        error_msg = f'读取或上传Excel文件时出错: {str(e)}'
                        print(f"[student-scores/save] ❌ 错误: {error_msg}")
                        app_logger.error(f"[student-scores/save] ❌ {error_msg}", exc_info=True)
                        import traceback
                        traceback_str = traceback.format_exc()
                        print(f"[student-scores/save] ❌ 错误堆栈:\n{traceback_str}")
                        app_logger.error(f"[student-scores/save] ❌ 错误堆栈:\n{traceback_str}")
            else:
                print(f"[student-scores/save] ℹ️ 未提供Excel文件")
                app_logger.info(f"[student-scores/save] ℹ️ 未提供Excel文件")
            print(f"[student-scores/save] ========== Excel文件处理完成 ==========")
            print(f"[student-scores/save] 最终excel_file_url值: {excel_file_url}")
            app_logger.info(f"[student-scores/save] ========== Excel文件处理完成，最终excel_file_url值: {excel_file_url} ==========")
            
        except json.JSONDecodeError as e:
            error_msg = f'无法解析multipart中的JSON数据: {str(e)}'
            print(f"[student-scores/save] 错误: {error_msg}")
            app_logger.warning(f"[student-scores/save] {error_msg}")
            return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
        except Exception as e:
            error_msg = f'处理multipart请求时出错: {str(e)}'
            print(f"[student-scores/save] 错误: {error_msg}")
            app_logger.warning(f"[student-scores/save] {error_msg}")
            return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    else:
        # 处理application/json格式
        try:
            data = await request.json()
        except Exception as e:
            error_msg = f'无效的 JSON 请求体: {str(e)}'
            print(f"[student-scores/save] 错误: {error_msg}")
            app_logger.warning(f"[student-scores/save] {error_msg}")
            return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    
    if not data:
        return safe_json_response({'message': '无法解析请求数据', 'code': 400}, status_code=400)
    
    # 打印接收到的数据
    print(f"[student-scores/save] 收到请求数据:")
    print(json.dumps(data, ensure_ascii=False, indent=2))
    if excel_file_name:
        print(f"[student-scores/save] Excel文件名: {excel_file_name}")
    
    # 从JSON数据中提取excel_file_name（如果multipart中没有提供）
    if not excel_file_name:
        excel_file_name = data.get('excel_file_name')
    
    # 从JSON数据中提取excel_file_url（如果multipart中没有提供）
    print(f"[student-scores/save] 📋 检查是否需要从JSON数据中提取excel_file_url...")
    app_logger.info(f"[student-scores/save] 📋 检查是否需要从JSON数据中提取excel_file_url...")
    print(f"[student-scores/save] 当前excel_file_url值: {excel_file_url}")
    app_logger.info(f"[student-scores/save] 当前excel_file_url值: {excel_file_url}")
    if not excel_file_url:
        json_excel_file_url = data.get('excel_file_url')
        print(f"[student-scores/save] 从JSON数据中获取excel_file_url: {json_excel_file_url}")
        app_logger.info(f"[student-scores/save] 从JSON数据中获取excel_file_url: {json_excel_file_url}")
        excel_file_url = json_excel_file_url
    else:
        print(f"[student-scores/save] ✅ excel_file_url已有值，无需从JSON数据中提取")
        app_logger.info(f"[student-scores/save] ✅ excel_file_url已有值，无需从JSON数据中提取")
    
    class_id = data.get('class_id')
    exam_name = data.get('exam_name')
    term = data.get('term')
    remark = data.get('remark')
    scores = data.get('scores', [])

    print(f"[student-scores/save] ========== 解析后的参数 ==========")
    print(f"[student-scores/save] class_id: {class_id}")
    print(f"[student-scores/save] exam_name: {exam_name}")
    print(f"[student-scores/save] term: {term}")
    print(f"[student-scores/save] excel_file_name: {excel_file_name}")
    print(f"[student-scores/save] excel_file_url: {excel_file_url}")
    print(f"[student-scores/save] excel_file_url类型: {type(excel_file_url)}")
    print(f"[student-scores/save] excel_file_url是否为空: {not excel_file_url}")
    print(f"[student-scores/save] scores数量: {len(scores) if scores else 0}")
    app_logger.info(f"[student-scores/save] 解析后的参数: class_id={class_id}, exam_name={exam_name}, term={term}, excel_file_name={excel_file_name}, excel_file_url={excel_file_url}, excel_file_url类型={type(excel_file_url)}, scores数量={len(scores) if scores else 0}")

    if not class_id or not exam_name:
        error_msg = '缺少必要参数 class_id 或 exam_name'
        print(f"[student-scores/save] 错误: {error_msg}")
        app_logger.warning(f"[student-scores/save] {error_msg}")
        return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)

    print(f"[student-scores/save] ========== 准备调用 save_student_scores 函数 ==========")
    app_logger.info(f"[student-scores/save] ========== 准备调用 save_student_scores 函数 ==========")
    print(f"[student-scores/save] 📤 传递给save_student_scores的参数:")
    print(f"[student-scores/save]   - class_id: {class_id}")
    print(f"[student-scores/save]   - exam_name: {exam_name}")
    print(f"[student-scores/save]   - term: {term}")
    print(f"[student-scores/save]   - remark: {remark}")
    print(f"[student-scores/save]   - excel_file_url: {excel_file_url}")
    print(f"[student-scores/save]   - scores数量: {len(scores) if scores else 0}")
    app_logger.info(f"[student-scores/save] 📤 传递给save_student_scores的参数: class_id={class_id}, exam_name={exam_name}, term={term}, remark={remark}, excel_file_url={excel_file_url}, scores数量={len(scores) if scores else 0}")
    result = save_student_scores(
        class_id=class_id,
        exam_name=exam_name,
        term=term,
        remark=remark,
        scores=scores,
        excel_file_url=excel_file_url,
        excel_file_name=excel_file_name
    )

    print(f"[student-scores/save] save_student_scores 返回结果: {result}")
    app_logger.info(f"[student-scores/save] save_student_scores 返回结果: {result}")

    if result.get('success'):
        return safe_json_response({'message': '保存成功', 'code': 200, 'data': result})
    else:
        return safe_json_response({'message': result.get('message', '保存失败'), 'code': 500}, status_code=500)

@app.get("/student-scores")
async def api_get_student_scores(
    request: Request,
    class_id: str = Query(..., description="班级ID"),
    exam_name: Optional[str] = Query(None, description="考试名称，如不提供则返回该班级所有成绩表"),
    term: Optional[str] = Query(None, description="学期，可选")
):
    """
    查询学生成绩表
    返回 JSON:
    {
      "message": "查询成功",
      "code": 200,
      "data": {
        "headers": [
          {
            "id": 1,
            "class_id": "class_1001",
            "exam_name": "期中考试",
            "term": "2025-2026-1",
            "remark": "...",
            "created_at": "...",
            "scores": [
              {
                "id": 1,
                "student_id": "2024001",
                "student_name": "张三",
                "chinese": 100,
                "math": 89,
                "english": 95,
                "total_score": 284
              },
              ...
            ]
          },
          ...
        ]
      }
    }
    """
    connection = get_db_connection()
    if connection is None:
        return safe_json_response({'message': '数据库连接失败', 'code': 500}, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)
        
        # 查询成绩表头
        if exam_name:
            cursor.execute(
                "SELECT id, class_id, exam_name, term, remark, excel_file_url, created_at, updated_at "
                "FROM ta_student_score_header "
                "WHERE class_id = %s AND exam_name = %s AND (%s IS NULL OR term = %s)",
                (class_id, exam_name, term, term)
            )
        else:
            cursor.execute(
                "SELECT id, class_id, exam_name, term, remark, excel_file_url, created_at, updated_at "
                "FROM ta_student_score_header "
                "WHERE class_id = %s AND (%s IS NULL OR term = %s) "
                "ORDER BY created_at DESC",
                (class_id, term, term)
            )
        
        headers = cursor.fetchall() or []
        
        # 查询每个表头的成绩明细和字段定义
        result_headers = []
        for header in headers:
            score_header_id = header['id']
            
            # 查询字段定义
            cursor.execute(
                "SELECT field_name, field_type, field_order, is_total "
                "FROM ta_student_score_field "
                "WHERE score_header_id = %s "
                "ORDER BY field_order ASC",
                (score_header_id,)
            )
            fields = cursor.fetchall() or []
            field_names = [f['field_name'] for f in fields]
            
            # 查询成绩明细
            cursor.execute(
                "SELECT id, student_id, student_name, scores_json, total_score "
                "FROM ta_student_score_detail "
                "WHERE score_header_id = %s "
                "ORDER BY total_score DESC, student_name ASC",
                (score_header_id,)
            )
            score_rows = cursor.fetchall() or []
            
            # 解析JSON字段并构建成绩列表
            scores = []
            for row in score_rows:
                score_dict = {
                    'id': row['id'],
                    'student_id': row.get('student_id'),
                    'student_name': row.get('student_name'),
                    'total_score': float(row['total_score']) if row['total_score'] is not None else None
                }
                
                # 解析JSON字段
                if row.get('scores_json'):
                    try:
                        if isinstance(row['scores_json'], str):
                            scores_data = json.loads(row['scores_json'])
                        else:
                            scores_data = row['scores_json']
                        
                        # 将JSON中的字段添加到score_dict中
                        for field_name in field_names:
                            if field_name in scores_data:
                                score_dict[field_name] = scores_data[field_name]
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"[api_get_student_scores] 解析JSON失败: {e}, scores_json={row.get('scores_json')}")
                        app_logger.warning(f"[api_get_student_scores] 解析JSON失败: {e}")
                
                scores.append(score_dict)
            
            header_dict = {
                'id': header['id'],
                'class_id': header['class_id'],
                'exam_name': header['exam_name'],
                'term': header.get('term'),
                'remark': header.get('remark'),
                'excel_file_url': header.get('excel_file_url'),
                'created_at': header.get('created_at'),
                'updated_at': header.get('updated_at'),
                'fields': fields,  # 字段定义列表
                'scores': scores
            }
            result_headers.append(header_dict)

        return safe_json_response({
            'message': '查询成功',
            'code': 200,
            'data': {'headers': result_headers}
        })
    except mysql.connector.Error as e:
        app_logger.error(f"Database error during api_get_student_scores: {e}")
        return safe_json_response({'message': '数据库错误', 'code': 500}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during api_get_student_scores: {e}")
        return safe_json_response({'message': '未知错误', 'code': 500}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching student scores.")

@app.get("/student-scores/get")
async def api_get_student_score(
    class_id: str = Query(..., description="班级ID"),
    exam_name: str = Query(..., description="考试名称，如'期中考试'"),
    term: str = Query(..., description="学期，如'2025-2026-1'")
):
    """
    查询学生成绩表（单个，如果查询到多个则返回最新的）
    返回 JSON:
    {
      "message": "查询成功",
      "code": 200,
      "data": {
        "id": 1,
        "class_id": "class_1001",
        "exam_name": "期中考试",
        "term": "2025-2026-1",
        "remark": "...",
        "created_at": "...",
        "updated_at": "...",
        "scores": [
          {
            "id": 1,
            "student_id": "2024001",
            "student_name": "张三",
            "chinese": 100,
            "math": 89,
            "english": 95,
            "total_score": 284
          },
          ...
        ]
      }
    }
    """
    print("=" * 80)
    print(f"[student-scores/get] 收到查询请求 - class_id: {class_id}, exam_name: {exam_name}, term: {term}")
    app_logger.info(f"[student-scores/get] 收到查询请求 - class_id: {class_id}, exam_name: {exam_name}, term: {term}")
    
    connection = get_db_connection()
    if connection is None:
        print("[student-scores/get] 错误: 数据库连接失败")
        app_logger.error(f"[student-scores/get] 数据库连接失败 - class_id: {class_id}, exam_name: {exam_name}, term: {term}")
        return safe_json_response({'message': '数据库连接失败', 'code': 500}, status_code=500)
    
    print("[student-scores/get] 数据库连接成功")
    app_logger.info(f"[student-scores/get] 数据库连接成功 - class_id: {class_id}")

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 查询成绩表头，如果有多个则按创建时间降序排列，取最新的
        print(f"[student-scores/get] 查询成绩表头...")
        app_logger.info(f"[student-scores/get] 开始查询成绩表头 - class_id: {class_id}, exam_name: {exam_name}, term: {term}")
        cursor.execute(
            "SELECT id, class_id, exam_name, term, remark, created_at, updated_at "
            "FROM ta_student_score_header "
            "WHERE class_id = %s AND exam_name = %s AND term = %s "
            "ORDER BY created_at DESC, updated_at DESC "
            "LIMIT 1",
            (class_id, exam_name, term)
        )
        
        header = cursor.fetchone()
        
        if not header:
            print(f"[student-scores/get] 未找到成绩表 - class_id: {class_id}, exam_name: {exam_name}, term: {term}")
            app_logger.warning(f"[student-scores/get] 未找到成绩表 - class_id: {class_id}, exam_name: {exam_name}, term: {term}")
            return safe_json_response({
                'message': '未找到成绩表',
                'code': 404,
                'data': None
            }, status_code=404)
        
        print(f"[student-scores/get] 找到成绩表头 - id: {header['id']}, created_at: {header.get('created_at')}")
        app_logger.info(f"[student-scores/get] 找到成绩表头 - id: {header['id']}, class_id: {class_id}, exam_name: {exam_name}, term: {term}, created_at: {header.get('created_at')}")
        
        # 查询字段定义
        score_header_id = header['id']
        print(f"[student-scores/get] 查询字段定义 - score_header_id: {score_header_id}")
        app_logger.info(f"[student-scores/get] 开始查询字段定义 - score_header_id: {score_header_id}")
        cursor.execute(
            "SELECT field_name, field_type, field_order, is_total "
            "FROM ta_student_score_field "
            "WHERE score_header_id = %s "
            "ORDER BY field_order ASC",
            (score_header_id,)
        )
        fields = cursor.fetchall() or []
        field_names = [f['field_name'] for f in fields]
        
        # 查询成绩明细
        print(f"[student-scores/get] 查询成绩明细 - score_header_id: {score_header_id}")
        app_logger.info(f"[student-scores/get] 开始查询成绩明细 - score_header_id: {score_header_id}")
        cursor.execute(
            "SELECT id, student_id, student_name, scores_json, total_score "
            "FROM ta_student_score_detail "
            "WHERE score_header_id = %s "
            "ORDER BY total_score DESC, student_name ASC",
            (score_header_id,)
        )
        score_rows = cursor.fetchall() or []
        
        print(f"[student-scores/get] 查询到 {len(score_rows)} 条成绩明细")
        app_logger.info(f"[student-scores/get] 查询到 {len(score_rows)} 条成绩明细 - score_header_id: {score_header_id}")
        
        # 解析JSON字段并构建成绩列表
        scores = []
        for row in score_rows:
            score_dict = {
                'id': row['id'],
                'student_id': row.get('student_id'),
                'student_name': row.get('student_name'),
                'total_score': float(row['total_score']) if row['total_score'] is not None else None
            }
            
            # 解析JSON字段
            if row.get('scores_json'):
                try:
                    if isinstance(row['scores_json'], str):
                        scores_data = json.loads(row['scores_json'])
                    else:
                        scores_data = row['scores_json']
                    
                    # 将JSON中的字段添加到score_dict中
                    for field_name in field_names:
                        if field_name in scores_data:
                            score_dict[field_name] = scores_data[field_name]
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"[api_get_student_score] 解析JSON失败: {e}, scores_json={row.get('scores_json')}")
                    app_logger.warning(f"[api_get_student_score] 解析JSON失败: {e}")
            
            scores.append(score_dict)
        
        # 转换 Decimal 类型为 float（用于 JSON 序列化）
        from decimal import Decimal
        def convert_decimal(obj):
            """递归转换 Decimal 类型为 float"""
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, dict):
                return {k: convert_decimal(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_decimal(item) for item in obj]
            return obj
        
        # 转换成绩明细中的 Decimal 类型
        scores = convert_decimal(scores)
        
        # 转换 datetime 为字符串
        if header.get('created_at') and isinstance(header['created_at'], datetime.datetime):
            header['created_at'] = header['created_at'].strftime("%Y-%m-%d %H:%M:%S")
        if header.get('updated_at') and isinstance(header['updated_at'], datetime.datetime):
            header['updated_at'] = header['updated_at'].strftime("%Y-%m-%d %H:%M:%S")
        
        # 转换 header 中的 Decimal 类型（如果有）
        header = convert_decimal(header)
        
        result = {
            'id': header['id'],
            'class_id': header['class_id'],
            'exam_name': header['exam_name'],
            'term': header.get('term'),
            'remark': header.get('remark'),
            'excel_file_url': header.get('excel_file_url'),
            'created_at': header.get('created_at'),
            'updated_at': header.get('updated_at'),
            'fields': fields,  # 字段定义列表
            'scores': scores
        }
        
        print(f"[student-scores/get] 返回结果 - id: {result['id']}, scores_count: {len(scores)}")
        app_logger.info(f"[student-scores/get] 查询成功 - score_header_id: {result['id']}, class_id: {class_id}, exam_name: {exam_name}, term: {term}, scores_count: {len(scores)}")
        
        response_data = {
            'message': '查询成功',
            'code': 200,
            'data': result
        }
        
        # 打印返回的 JSON 结果
        try:
            response_json = json.dumps(response_data, ensure_ascii=False, indent=2)
            print(f"[student-scores/get] 返回的 JSON 结果:\n{response_json}")
            app_logger.info(f"[student-scores/get] 返回的 JSON 结果: {json.dumps(response_data, ensure_ascii=False)}")
        except Exception as json_error:
            print(f"[student-scores/get] 打印 JSON 时出错: {json_error}")
            app_logger.warning(f"[student-scores/get] 打印 JSON 时出错: {json_error}")
        
        print("=" * 80)
        
        return safe_json_response(response_data)
        
    except mysql.connector.Error as e:
        print(f"[student-scores/get] 数据库错误: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        app_logger.error(f"[student-scores/get] 数据库错误 - class_id: {class_id}, exam_name: {exam_name}, term: {term}, error: {e}\n{traceback_str}")
        return safe_json_response({'message': '数据库错误', 'code': 500}, status_code=500)
    except Exception as e:
        print(f"[student-scores/get] 未知错误: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[student-scores/get] 错误堆栈: {traceback_str}")
        app_logger.error(f"[student-scores/get] 未知错误 - class_id: {class_id}, exam_name: {exam_name}, term: {term}, error: {e}\n{traceback_str}")
        return safe_json_response({'message': '未知错误', 'code': 500}, status_code=500)
    finally:
        if cursor:
            cursor.close()
            print("[student-scores/get] 游标已关闭")
        if connection and connection.is_connected():
            connection.close()
            print("[student-scores/get] 数据库连接已关闭")
            app_logger.info(f"[student-scores/get] 数据库连接已关闭 - class_id: {class_id}")

# ===== 小组管理表 API =====
def save_group_scores(
    class_id: str,
    term: Optional[str] = None,
    remark: Optional[str] = None,
    group_scores: List[Dict] = None
) -> Dict[str, object]:
    """
    保存小组管理表
    参数说明：
    - class_id: 班级ID（必需）
    - term: 学期（可选，如 '2025-2026-1'）
    - remark: 备注（可选）
    - group_scores: 小组评分明细列表，每个元素包含:
      {
        'group_number': int,           # 小组编号（必需）
        'student_id': str,             # 学号（可选）
        'student_name': str,           # 姓名（必需）
        'hygiene': int,                # 卫生评分（可选）
        'participation': int,          # 课堂发言评分（可选）
        'discipline': int,             # 纪律评分（可选）
        'homework': int,               # 作业评分（可选）
        'recitation': int,             # 背诵评分（可选）
        'total_score': int             # 个人总分（可选，可自动计算）
      }
    
    返回：
    - { success, score_header_id, inserted_count, message }
    """
    if not class_id:
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '缺少必要参数 class_id' }
    
    if not group_scores or not isinstance(group_scores, list):
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '小组评分明细列表不能为空' }

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Save group scores failed: Database connection error.")
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '数据库连接失败' }

    try:
        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # 1. 插入或获取小组管理表头（每个班级每个学期一个表头）
        cursor.execute(
            "SELECT id FROM ta_group_score_header WHERE class_id = %s AND (%s IS NULL OR term = %s) LIMIT 1",
            (class_id, term, term)
        )
        header_row = cursor.fetchone()

        if header_row is None:
            # 插入新表头
            insert_header_sql = (
                "INSERT INTO ta_group_score_header (class_id, term, remark, created_at) "
                "VALUES (%s, %s, %s, NOW())"
            )
            cursor.execute(insert_header_sql, (class_id, term, remark))
            score_header_id = cursor.lastrowid
        else:
            score_header_id = header_row['id']
            # 更新表头信息（若存在）
            if remark is not None:
                cursor.execute(
                    "UPDATE ta_group_score_header SET remark = %s, updated_at = NOW() WHERE id = %s",
                    (remark, score_header_id)
                )
            # 删除旧的评分明细（重新上传时覆盖）
            cursor.execute("DELETE FROM ta_group_score_detail WHERE score_header_id = %s", (score_header_id,))

        # 2. 批量插入评分明细
        insert_detail_sql = (
            "INSERT INTO ta_group_score_detail "
            "(score_header_id, group_number, student_id, student_name, hygiene, participation, discipline, homework, recitation, total_score) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        
        inserted_count = 0
        for score_item in group_scores:
            group_number = score_item.get('group_number')
            student_id = score_item.get('student_id')
            student_name = score_item.get('student_name', '').strip()
            
            if not student_name or group_number is None:
                continue  # 跳过没有姓名或小组编号的记录
            
            hygiene = score_item.get('hygiene')
            participation = score_item.get('participation')
            discipline = score_item.get('discipline')
            homework = score_item.get('homework')
            recitation = score_item.get('recitation')
            
            # 计算个人总分（如果未提供或需要重新计算）
            total_score = score_item.get('total_score')
            if total_score is None:
                # 自动计算总分（只计算提供的科目）
                total_score = 0
                if hygiene is not None:
                    total_score += int(hygiene)
                if participation is not None:
                    total_score += int(participation)
                if discipline is not None:
                    total_score += int(discipline)
                if homework is not None:
                    total_score += int(homework)
                if recitation is not None:
                    total_score += int(recitation)
            
            cursor.execute(insert_detail_sql, (
                score_header_id,
                int(group_number),
                student_id,
                student_name,
                hygiene,
                participation,
                discipline,
                homework,
                recitation,
                total_score
            ))
            inserted_count += 1

        connection.commit()
        return { 'success': True, 'score_header_id': score_header_id, 'inserted_count': inserted_count, 'message': '保存成功' }
    except mysql.connector.Error as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Database error during save_group_scores: {e}")
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': f'数据库错误: {e}' }
    except Exception as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Unexpected error during save_group_scores: {e}")
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': f'未知错误: {e}' }
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after saving group scores.")

@app.post("/group-scores/save")
async def api_save_group_scores(request: Request):
    """
    保存小组管理表
    请求体 JSON:
    {
      "class_id": "class_1001",
      "term": "2025-2026-1",  // 可选
      "remark": "备注信息",    // 可选
      "group_scores": [
        {
          "group_number": 1,              // 小组编号（必需）
          "student_id": "2024001",        // 可选
          "student_name": "张三",
          "hygiene": 100,                 // 卫生评分（可选）
          "participation": 89,            // 课堂发言评分（可选）
          "discipline": 84,               // 纪律评分（可选）
          "homework": 90,                 // 作业评分（可选）
          "recitation": 85,               // 背诵评分（可选）
          "total_score": 448              // 个人总分（可选，会自动计算）
        },
        {
          "group_number": 1,
          "student_name": "李四",
          "hygiene": 90,
          "participation": 78,
          "discipline": 53
          // total_score 会自动计算为 221
        },
        {
          "group_number": 2,
          "student_name": "王五",
          "hygiene": 67,
          "participation": 97,
          "discipline": 23
        }
      ]
    }
    """
    try:
        data = await request.json()
    except Exception:
        return safe_json_response({'message': '无效的 JSON 请求体', 'code': 400}, status_code=400)

    class_id = data.get('class_id')
    term = data.get('term')
    remark = data.get('remark')
    group_scores = data.get('group_scores', [])

    if not class_id:
        return safe_json_response({'message': '缺少必要参数 class_id', 'code': 400}, status_code=400)

    result = save_group_scores(
        class_id=class_id,
        term=term,
        remark=remark,
        group_scores=group_scores
    )

    if result.get('success'):
        return safe_json_response({'message': '保存成功', 'code': 200, 'data': result})
    else:
        return safe_json_response({'message': result.get('message', '保存失败'), 'code': 500}, status_code=500)

@app.get("/group-scores")
async def api_get_group_scores(
    request: Request,
    class_id: str = Query(..., description="班级ID"),
    term: Optional[str] = Query(None, description="学期，可选")
):
    """
    查询小组管理表
    返回 JSON:
    {
      "message": "查询成功",
      "code": 200,
      "data": {
        "header": {
          "id": 1,
          "class_id": "class_1001",
          "term": "2025-2026-1",
          "remark": "...",
          "created_at": "...",
          "updated_at": "..."
        },
        "group_scores": [
          {
            "group_number": 1,
            "group_total_score": 765,  // 小组总分（自动计算）
            "students": [
              {
                "id": 1,
                "student_id": "2024001",
                "student_name": "张三",
                "hygiene": 100,
                "participation": 89,
                "discipline": 84,
                "homework": 90,
                "recitation": 85,
                "total_score": 448
              },
              ...
            ]
          },
          {
            "group_number": 2,
            "group_total_score": 544,
            "students": [...]
          },
          ...
        ]
      }
    }
    """
    connection = get_db_connection()
    if connection is None:
        return safe_json_response({'message': '数据库连接失败', 'code': 500}, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)
        
        # 查询小组管理表头
        cursor.execute(
            "SELECT id, class_id, term, remark, created_at, updated_at "
            "FROM ta_group_score_header "
            "WHERE class_id = %s AND (%s IS NULL OR term = %s) "
            "ORDER BY created_at DESC LIMIT 1",
            (class_id, term, term)
        )
        
        header = cursor.fetchone()
        if not header:
            return safe_json_response({'message': '未找到小组管理表', 'code': 404}, status_code=404)

        score_header_id = header['id']
        
        # 查询所有评分明细，按小组编号和学生姓名排序
        cursor.execute(
            "SELECT id, group_number, student_id, student_name, hygiene, participation, discipline, homework, recitation, total_score "
            "FROM ta_group_score_detail "
            "WHERE score_header_id = %s "
            "ORDER BY group_number ASC, student_name ASC",
            (score_header_id,)
        )
        all_scores = cursor.fetchall() or []
        
        # 按小组分组，并计算每个小组的总分
        group_dict = {}
        for score in all_scores:
            group_num = score['group_number']
            if group_num not in group_dict:
                group_dict[group_num] = {
                    'group_number': group_num,
                    'group_total_score': 0,
                    'students': []
                }
            group_dict[group_num]['students'].append({
                'id': score['id'],
                'student_id': score.get('student_id'),
                'student_name': score['student_name'],
                'hygiene': score.get('hygiene'),
                'participation': score.get('participation'),
                'discipline': score.get('discipline'),
                'homework': score.get('homework'),
                'recitation': score.get('recitation'),
                'total_score': score.get('total_score')
            })
            # 累加小组总分
            if score.get('total_score'):
                group_dict[group_num]['group_total_score'] += int(score['total_score'])
        
        # 转换为列表，按小组编号排序
        group_scores_list = sorted(group_dict.values(), key=lambda x: x['group_number'])

        return safe_json_response({
            'message': '查询成功',
            'code': 200,
            'data': {
                'header': {
                    'id': header['id'],
                    'class_id': header['class_id'],
                    'term': header.get('term'),
                    'remark': header.get('remark'),
                    'created_at': header.get('created_at'),
                    'updated_at': header.get('updated_at')
                },
                'group_scores': group_scores_list
            }
        })
    except mysql.connector.Error as e:
        app_logger.error(f"Database error during api_get_group_scores: {e}")
        return safe_json_response({'message': '数据库错误', 'code': 500}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during api_get_group_scores: {e}")
        return safe_json_response({'message': '未知错误', 'code': 500}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching group scores.")

def hash_password(password, salt):
    return hashlib.sha256((password + salt).encode('utf-8')).hexdigest()

def generate_verification_code(length=6):
    return ''.join(random.choices(string.digits, k=length))

def send_sms_verification_code(phone, code):
    client = AcsClient(SMS_CONFIG['access_key_id'], SMS_CONFIG['access_key_secret'], 'cn-hangzhou')
    request = CommonRequest()
    request.set_accept_format('json')
    request.set_domain('dysmsapi.aliyuncs.com')
    request.set_method('POST')
    request.set_protocol_type('https')
    request.set_version('2017-05-25')
    request.set_action_name('SendSms')
    request.add_query_param('RegionId', "cn-hangzhou")
    request.add_query_param('PhoneNumbers', phone)
    request.add_query_param('SignName', SMS_CONFIG['sign_name'])
    request.add_query_param('TemplateCode', SMS_CONFIG['template_code'])
    request.add_query_param('TemplateParam', f"{{\"code\":\"{code}\"}}")
    response = client.do_action_with_exception(request)
    print(str(response, encoding='utf-8'))
    return True

    # 模拟发送成功
    app_logger.info(f"手机号: {phone}, 验证码: {code}")
    return True

verification_memory = {}

# @app.before_request
# def log_request_info():
#     app_logger.info(f"Incoming request: {request.method} {request.url} from {request.remote_addr}")

async def log_request_info(request: Request, call_next):
    client_host = request.client.host  # 等于 Flask 的 request.remote_addr
    app_logger.info(
        f"Incoming request: {request.method} {request.url} from {client_host}"
    )
    response = await call_next(request)
    return response

# 添加中间件
app.add_middleware(BaseHTTPMiddleware, dispatch=log_request_info)

def verify_code_from_session(input_phone, input_code):
    stored_data = session.get('verification_code')
    if not stored_data:
        app_logger.warning(f"Verification failed for {input_phone}: No code sent or expired.")
        return False, "未发送验证码或验证码已过期"

    if stored_data['phone'] != input_phone:
        app_logger.warning(f"Verification failed for {input_phone}: Phone number mismatch.")
        return False, "手机号不匹配"

    #if datetime.datetime.now() > stored_data['expires_at']:
    if time.time() > stored_data['expires_at']:
        session.pop('verification_code', None)
        app_logger.info(f"Verification code expired for {input_phone}.")
        return False, "验证码已过期"

    if stored_data['code'] != input_code:
        app_logger.warning(f"Verification failed for {input_phone}: Incorrect code entered.")
        return False, "验证码错误"

    session.pop('verification_code', None)
    app_logger.info(f"Verification successful for {input_phone}.")
    return True, "验证成功"

def verify_code_from_memory(input_phone, input_code):
    # 验证验证码
    valid_info = verification_memory.get(input_phone)
    if not valid_info:
        app_logger.warning(f"Verification failed for {input_phone}: No code sent or expired.")
        return False, "未发送验证码或验证码已过期"
    elif time.time() > valid_info['expires_at']:
        verification_memory.pop(input_phone, None)
        app_logger.info(f"Verification code expired for {input_phone}.")
        return False, "验证码已过期"
    elif str(input_code) != str(valid_info['code']):
        app_logger.warning(f"Verification failed for {input_phone}: Incorrect code entered.")
        return False, "验证码错误"
    else:
        verification_memory.pop(input_phone, None)
        app_logger.info(f"Verification successful for {input_phone}.")
        return True, "验证成功"

    # stored_data = session.get('verification_code')
    # if not stored_data:
    #     app_logger.warning(f"Verification failed for {input_phone}: No code sent or expired.")
    #     return False, "未发送验证码或验证码已过期"

    # if stored_data['phone'] != input_phone:
    #     app_logger.warning(f"Verification failed for {input_phone}: Phone number mismatch.")
    #     return False, "手机号不匹配"

    # #if datetime.datetime.now() > stored_data['expires_at']:
    # if time.time() > stored_data['expires_at']:
    #     session.pop('verification_code', None)
    #     app_logger.info(f"Verification code expired for {input_phone}.")
    #     return False, "验证码已过期"

    # if stored_data['code'] != input_code:
    #     app_logger.warning(f"Verification failed for {input_phone}: Incorrect code entered.")
    #     return False, "验证码错误"

    # session.pop('verification_code', None)
    # app_logger.info(f"Verification successful for {input_phone}.")
    # return True, "验证成功"


# Redis 连接
r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

def get_max_code_from_mysql(connection):
    #"""从 MySQL 找最大号码"""
    print(" get_max_code_from_mysql 111\n");
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("SELECT MAX(CAST(id AS UNSIGNED)) AS max_id FROM ta_school")
        print(" get_max_code_from_mysql 222\n");
        row = cursor.fetchone()
        #row = cursor.fetchone()[0]
        print(" get_max_code_from_mysql 333\n", row);
        if row and row['max_id'] is not None:
            return int(row['max_id'])
        return 0

def generate_unique_code():
    #"""生成唯一 6 位数字"""
    connection = get_db_connection()
    if connection is None:
        app_logger.error("List schools failed: Database connection error.")
        print(" 数据库连接失败\n");
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'schools': []
            }
        }), 500

    print(" generate_unique_code 111\n");

    # 先从 Redis 缓存取
    max_code = r.get("unique_max_code")
    if max_code:
        new_code = int(max_code) + 1
    else:
        # Redis 没缓存，从 MySQL 查
        new_code = get_max_code_from_mysql(connection) + 1

    print(" get_max_code_from_mysql leave");
    if new_code >= 1000000:
        raise ValueError("6位数字已用完")

    code_str = f"{new_code:06d}"

    print(" INSERT INTO ta_school\n");

    cursor = None
    # 写入 MySQL
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("INSERT INTO ta_school (id) VALUES (%s)", (new_code,))
        connection.commit()
        cursor.close()
    except mysql.connector.errors.IntegrityError:
        # 如果主键冲突，递归重试
            return generate_unique_code()
    finally:
        if connection and connection.is_connected():
            connection.close()

        # 更新 Redis 缓存
    r.set("unique_max_code", new_code)
    print(" INSERT INTO code_str:", code_str, "\n");
    return code_str

#from fastapi import Request
#from fastapi.responses import JSONResponse
#import base64, os, datetime

@app.get("/unique6digit")
async def unique_code_api():
    try:
        code = generate_unique_code()
        return JSONResponse({"code": code, "status": "ok"})
    except Exception as e:
        return JSONResponse({"error": str(e), "status": "fail"}, status_code=500)


@app.get("/schools")
async def list_schools(request: Request):
    connection = get_db_connection()
    if connection is None:
        app_logger.error("List schools failed: Database connection error.")
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500, 'schools': []}}, status_code=500)

    cursor = None
    try:
        school_id = request.query_params.get('id')
        name_filter = request.query_params.get('name')

        base_columns = "id, name, address"
        base_query = f"SELECT {base_columns} FROM ta_school WHERE 1=1"
        filters, params = [], []

        if school_id is not None:
            filters.append("AND id = %s")
            params.append(school_id)
        elif name_filter:
            filters.append("AND name LIKE %s")
            params.append(f"%{name_filter}%")

        final_query = base_query + " " + " ".join(filters)
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        schools = cursor.fetchall()

        app_logger.info(f"Fetched {len(schools)} schools.")
        return safe_json_response({'data': {'message': '获取学校列表成功', 'code': 200, 'schools': schools}})
    except Error as e:
        app_logger.error(f"Database error during fetching schools: {e}")
        return JSONResponse({'data': {'message': '获取学校列表失败', 'code': 500, 'schools': []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching schools: {e}")
        return JSONResponse({'data': {'message': '内部服务器错误', 'code': 500, 'schools': []}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching schools.")


@app.post("/updateUserInfo")
async def updateUserInfo(request: Request):
    print("=" * 80)
    print("[updateUserInfo] 收到更新用户信息请求")
    print(f"[updateUserInfo] 请求方法: {request.method}")
    print(f"[updateUserInfo] 请求URL: {request.url}")
    print(f"[updateUserInfo] 请求头: {dict(request.headers)}")
    connection = None
    cursor = None
    user_details: Optional[Dict[str, Any]] = None
    tencent_identifier: Optional[str] = None
    avatar_url = None  # 存入数据库的值（可能是URL或相对路径）
    avatar_sync_url = None  # 发给腾讯或前端的可访问URL
    
    try:
        # 步骤1: 解析请求数据
        print("[updateUserInfo] 步骤1: 开始解析请求JSON数据...")
        print(f"[updateUserInfo] 步骤1: 请求内容类型: {request.headers.get('content-type', '未指定')}")
        try:
            body = await request.body()
            print(f"[updateUserInfo] 步骤1: 原始请求体大小: {len(body)} bytes")
            if body:
                print(f"[updateUserInfo] 步骤1: 原始请求体前200字符: {body[:200]}")
            
            data = await request.json()
            print(f"[updateUserInfo] 步骤1完成: 成功解析JSON, payload keys: {list(data.keys()) if data else 'None'}")
            print(f"[updateUserInfo] 步骤1: 完整payload: {data}")
        except Exception as e:
            print(f"[updateUserInfo] 步骤1失败: JSON解析错误 - {type(e).__name__}: {str(e)}")
            app_logger.error(f"UpdateUserInfo failed: JSON parse error - {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] 异常堆栈:\n{traceback.format_exc()}")
            return JSONResponse({'data': {'message': f'请求数据解析失败: {str(e)}', 'code': 400}}, status_code=400)
        
        print(f"[updateUserInfo] Received payload: {data}")
        try:
            phone = data.get('phone')
            id_number = data.get('id_number')
            avatar = data.get('avatar')
            print(f"[updateUserInfo] 提取的字段 - phone: {phone}, id_number: {id_number}, avatar_length: {len(avatar) if avatar else 0}, avatar_type: {type(avatar)}")
            print(f"[updateUserInfo] 所有字段列表: {list(data.keys())}")
            for key, value in data.items():
                if key != 'avatar':  # 头像数据太长，不完整打印
                    print(f"[updateUserInfo]   - {key}: {value} (type: {type(value).__name__})")
        except Exception as e:
            print(f"[updateUserInfo] 提取字段时出错: {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] 异常堆栈:\n{traceback.format_exc()}")
            raise

        # 步骤2: 验证必需字段
        print("[updateUserInfo] 步骤2: 验证必需字段...")
        print(f"[updateUserInfo] 步骤2: id_number检查 - 值: {id_number}, 类型: {type(id_number).__name__}, 是否为空: {not id_number}")
        print(f"[updateUserInfo] 步骤2: avatar检查 - 值长度: {len(avatar) if avatar else 0}, 类型: {type(avatar).__name__}, 是否为空: {not avatar}")
        if not id_number or not avatar:
            app_logger.warning("UpdateUserInfo failed: Missing id_number or avatar.")
            print(f"[updateUserInfo] 步骤2失败: Missing id_number or avatar -> id_number={id_number}, avatar_present={avatar is not None}")
            return JSONResponse({'data': {'message': '身份证号码和头像必须提供', 'code': 400}}, status_code=400)
        print("[updateUserInfo] 步骤2完成: 必需字段验证通过")

        # 步骤3: 连接数据库
        print("[updateUserInfo] 步骤3: 连接数据库...")
        try:
            connection = get_db_connection()
            print(f"[updateUserInfo] 步骤3: get_db_connection返回: {connection}, 类型: {type(connection).__name__}")
            if connection:
                print(f"[updateUserInfo] 步骤3: connection.is_connected() = {connection.is_connected()}")
            if connection is None or not connection.is_connected():
                app_logger.error("UpdateUserInfo failed: Database connection error.")
                print("[updateUserInfo] 步骤3失败: 数据库连接失败或未连接")
                return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)
            print("[updateUserInfo] 步骤3完成: 数据库连接成功")
        except Exception as e:
            print(f"[updateUserInfo] 步骤3异常: 连接数据库时出错 - {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] 异常堆栈:\n{traceback.format_exc()}")
            app_logger.error(f"UpdateUserInfo failed: Database connection exception - {type(e).__name__}: {str(e)}")
            raise

        # 步骤4: 解码头像数据
        print("[updateUserInfo] 步骤4: 解码Base64头像数据...")
        print(f"[updateUserInfo] 步骤4: avatar前100字符: {avatar[:100] if avatar else 'None'}...")
        try:
            # 确保avatar是字符串
            if not isinstance(avatar, str):
                print(f"[updateUserInfo] 步骤4: avatar不是字符串类型，当前类型: {type(avatar).__name__}, 值: {avatar}")
                avatar = str(avatar)
            # 移除可能的前缀
            if avatar.startswith('data:image'):
                print("[updateUserInfo] 步骤4: 检测到data URL前缀，移除前缀...")
                avatar = avatar.split(',', 1)[1]
            avatar_bytes = base64.b64decode(avatar)
            print(f"[updateUserInfo] 步骤4完成: 头像解码成功, 大小: {len(avatar_bytes)} bytes")
        except Exception as e:
            app_logger.error(f"UpdateUserInfo failed: Avatar decode error for {id_number}: {e}")
            print(f"[updateUserInfo] 步骤4失败: Avatar decode error for id_number={id_number}: {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] avatar字符串长度: {len(avatar) if avatar else 0}")
            print(f"[updateUserInfo] avatar字符串类型: {type(avatar).__name__}")
            print(f"[updateUserInfo] 异常堆栈:\n{traceback.format_exc()}")
            return JSONResponse({'data': {'message': f'头像数据解析失败: {str(e)}', 'code': 400}}, status_code=400)

        # 步骤5: 上传头像到OSS
        print("[updateUserInfo] 步骤5: 上传头像到OSS...")
        print(f"[updateUserInfo] 步骤5: avatar_bytes类型: {type(avatar_bytes).__name__}, 大小: {len(avatar_bytes) if avatar_bytes else 0} bytes")
        object_name = f"avatars/{id_number}_{int(time.time())}.png"
        print(f"[updateUserInfo] 步骤5: OSS对象名称: {object_name}")
        print(f"[updateUserInfo] 步骤5: 检查upload_avatar_to_oss函数是否可用...")
        try:
            print(f"[updateUserInfo] 步骤5: 调用upload_avatar_to_oss(avatar_bytes长度={len(avatar_bytes)}, object_name={object_name})...")
            avatar_url = upload_avatar_to_oss(avatar_bytes, object_name)
            avatar_sync_url = avatar_url
            print(f"[updateUserInfo] 步骤5: upload_avatar_to_oss返回: {avatar_url}, 类型: {type(avatar_url).__name__}")
            if not avatar_url:
                print("[updateUserInfo] 步骤5: OSS 上传失败，尝试本地兜底存储...")
                local_path = save_avatar_locally(avatar_bytes, object_name)
                if not local_path:
                    app_logger.error("UpdateUserInfo failed: OSS 和本地保存均失败")
                    print("[updateUserInfo] 步骤5失败: save_avatar_locally返回None")
                    return JSONResponse({'data': {'message': '头像上传失败，请稍后再试', 'code': 500}}, status_code=500)
                avatar_url = local_path
                avatar_sync_url = build_public_url_from_local_path(local_path) or local_path
                print(f"[updateUserInfo] 步骤5: 本地兜底成功, relative_path={local_path}, sync_url={avatar_sync_url}")
            else:
                print(f"[updateUserInfo] 步骤5完成: 头像上传成功, URL: {avatar_url}")
        except Exception as e:
            app_logger.error(f"UpdateUserInfo failed: OSS upload error for {id_number}: {e}")
            print(f"[updateUserInfo] 步骤5失败: OSS上传异常 - {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] 步骤5: 异常参数: {e.args}")
            print(f"[updateUserInfo] 步骤5异常堆栈:\n{traceback.format_exc()}")
            return JSONResponse({'data': {'message': f'头像上传失败: {str(e)}', 'code': 500}}, status_code=500)

        # 步骤6: 更新数据库
        print("[updateUserInfo] 步骤6: 更新数据库中的头像URL...")
        print(f"[updateUserInfo] 步骤6: 准备更新，avatar_url={avatar_url}, id_number={id_number}")
        try:
            if not cursor:
                print("[updateUserInfo] 步骤6: 创建数据库游标...")
                cursor = connection.cursor(dictionary=True)
                print(f"[updateUserInfo] 步骤6: 游标创建成功: {cursor}")
            else:
                print("[updateUserInfo] 步骤6: 使用现有游标")
            update_query = "UPDATE ta_user_details SET avatar = %s WHERE id_number = %s"
            print(f"[updateUserInfo] 步骤6: 执行SQL: {update_query}")
            print(f"[updateUserInfo] 步骤6: SQL参数 - avatar_url类型: {type(avatar_url).__name__}, 值: {avatar_url}")
            print(f"[updateUserInfo] 步骤6: SQL参数 - id_number类型: {type(id_number).__name__}, 值: {id_number}")
            cursor.execute(update_query, (avatar_url, id_number))
            affected_rows = cursor.rowcount
            print(f"[updateUserInfo] 步骤6: SQL执行完成, 受影响行数: {affected_rows}")
            
            if affected_rows == 0:
                print("[updateUserInfo] 未更新任何行, 尝试通过id_number查询用户...")
                cursor.execute(
                    "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE id_number = %s",
                    (id_number,)
                )
                user_details = cursor.fetchone()
                print(f"[updateUserInfo] 通过id_number查询结果: {user_details}")
                
                if not user_details and phone:
                    print(f"[updateUserInfo] 通过id_number未找到, 尝试通过phone查询: {phone}")
                    cursor.execute(
                        "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE phone = %s",
                        (phone,)
                    )
                    user_details = cursor.fetchone()
                    print(f"[updateUserInfo] 通过phone查询结果: {user_details}")

                if not user_details:
                    cursor.execute(
                        "SELECT avatar FROM ta_user_details WHERE id_number = %s",
                        (id_number,)
                    )
                    existing_avatar_row = cursor.fetchone()
                    existing_avatar = existing_avatar_row["avatar"] if existing_avatar_row else None
                    print(f"[updateUserInfo] 最终未找到用户记录, id_number={id_number}, existing_avatar={existing_avatar}")
                    connection.commit()
                    app_logger.warning(f"UpdateUserInfo: No user_details record found for id_number={id_number}")
                    return JSONResponse({'data': {'message': '未找到对应的用户信息', 'code': 404}}, status_code=404)
                else:
                    print("[updateUserInfo] 找到用户记录但UPDATE未影响行, 继续处理...")
            else:
                print("[updateUserInfo] UPDATE成功, 提交事务并查询用户详情...")
                connection.commit()
                cursor.execute(
                    "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE id_number = %s",
                    (id_number,)
                )
                user_details = cursor.fetchone()
                print(f"[updateUserInfo] 更新后查询结果: {user_details}")
                
                if not user_details and phone:
                    print(f"[updateUserInfo] 更新后通过id_number未找到, 尝试通过phone查询: {phone}")
                    cursor.execute(
                        "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE phone = %s",
                        (phone,)
                    )
                    user_details = cursor.fetchone()
                    print(f"[updateUserInfo] 通过phone查询结果: {user_details}")

            print("[updateUserInfo] 步骤6完成: 数据库更新成功")
        except Error as e:
            app_logger.error(f"Database error during updateUserInfo for {phone}: {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] 步骤6失败: 数据库错误 - {type(e).__name__}: {str(e)}")
            if connection:
                try:
                    connection.rollback()
                    print("[updateUserInfo] 已回滚事务")
                except Exception as rollback_e:
                    print(f"[updateUserInfo] 回滚失败: {str(rollback_e)}")
            print(f"[updateUserInfo] 异常堆栈:\n{traceback.format_exc()}")
            return JSONResponse({'data': {'message': f'数据库更新失败: {str(e)}', 'code': 500}}, status_code=500)
        except Exception as e:
            app_logger.error(f"Unexpected error during database update for {phone}: {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] 步骤6失败: 意外错误 - {type(e).__name__}: {str(e)}")
            if connection:
                try:
                    connection.rollback()
                    print("[updateUserInfo] 已回滚事务")
                except Exception as rollback_e:
                    print(f"[updateUserInfo] 回滚失败: {str(rollback_e)}")
            print(f"[updateUserInfo] 异常堆栈:\n{traceback.format_exc()}")
            return JSONResponse({'data': {'message': f'数据库操作失败: {str(e)}', 'code': 500}}, status_code=500)

        # 步骤7: 解析腾讯标识符
        print("[updateUserInfo] 步骤7: 解析腾讯用户标识符...")
        print(f"[updateUserInfo] 步骤7: 调用参数 - connection={connection}, id_number={id_number}, phone={phone}")
        try:
            tencent_identifier = resolve_tencent_identifier(connection, id_number=id_number, phone=phone)
            print(f"[updateUserInfo] 步骤7完成: Tencent identifier={tencent_identifier}, 类型: {type(tencent_identifier).__name__}")
        except Exception as e:
            app_logger.error(f"UpdateUserInfo failed: resolve_tencent_identifier error for {id_number}: {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] 步骤7失败: resolve_tencent_identifier异常 - {type(e).__name__}: {str(e)}")
            import traceback
            print(f"[updateUserInfo] 步骤7异常堆栈:\n{traceback.format_exc()}")
            tencent_identifier = None  # 确保变量被设置
            print(f"[updateUserInfo] 步骤7: 使用None作为fallback，将继续使用id_number")
            # 继续执行，使用id_number作为fallback

        # 步骤8: 准备同步数据
        print("[updateUserInfo] 步骤8: 准备腾讯同步数据...")
        print(f"[updateUserInfo] 步骤8: user_details状态: {user_details}")
        print(f"[updateUserInfo] 步骤8: avatar_url状态: {avatar_url}")
        name_for_sync = None
        avatar_for_sync = None
        try:
            if user_details:
                name_for_sync = user_details.get("name")
                avatar_from_db = user_details.get("avatar")
                avatar_for_sync = avatar_sync_url or avatar_from_db or avatar_url
                print(f"[updateUserInfo] 步骤8: 从user_details获取 - name={name_for_sync}, avatar_db={avatar_from_db}, avatar_for_sync={avatar_for_sync}")
            else:
                avatar_for_sync = avatar_sync_url or avatar_url
                print(f"[updateUserInfo] 步骤8: user_details为空，使用上传的头像URL: {avatar_for_sync}")
            print(f"[updateUserInfo] 步骤8: 最终同步数据 - name_for_sync={name_for_sync}, avatar_for_sync={avatar_for_sync}")
            print("[updateUserInfo] 步骤8完成")
        except Exception as e:
            print(f"[updateUserInfo] 步骤8异常: 准备同步数据时出错 - {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] 步骤8异常堆栈:\n{traceback.format_exc()}")
            raise

        # 步骤9: 同步到腾讯
        print("[updateUserInfo] 步骤9: 同步用户信息到腾讯...")
        final_identifier = tencent_identifier or id_number
        print(f"[updateUserInfo] 步骤9: 最终使用的identifier={final_identifier} (tencent_identifier={tencent_identifier}, id_number={id_number})")
        print(f"[updateUserInfo] 步骤9: 同步参数 - identifier={final_identifier}, name={name_for_sync}, avatar_url={avatar_for_sync}")
        print(f"[updateUserInfo] Tencent sync request -> identifier={final_identifier}, "
              f"name={name_for_sync}, avatar={avatar_for_sync}")
        app_logger.info(
            f"updateUserInfo: 准备同步腾讯用户资料 identifier={final_identifier}, "
            f"name={name_for_sync}, avatar={avatar_for_sync}"
        )
        tencent_sync_summary = None
        try:
            print(f"[updateUserInfo] 步骤9: 调用notify_tencent_user_profile...")
            tencent_sync_summary = await notify_tencent_user_profile(
                final_identifier,
                name=name_for_sync,
                avatar_url=avatar_for_sync
            )
            print(f"[updateUserInfo] 步骤9完成: 腾讯同步成功")
            print(f"[updateUserInfo] Tencent sync response <- {tencent_sync_summary}, 类型: {type(tencent_sync_summary).__name__}")
            app_logger.info(f"updateUserInfo: 腾讯接口返回 {tencent_sync_summary}")
        except Exception as e:
            app_logger.error(f"UpdateUserInfo failed: notify_tencent_user_profile error: {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] 步骤9失败: 腾讯同步异常 - {type(e).__name__}: {str(e)}")
            import traceback
            print(f"[updateUserInfo] 步骤9异常堆栈:\n{traceback.format_exc()}")
            tencent_sync_summary = {'success': False, 'error': str(e)}
            print(f"[updateUserInfo] 步骤9: 设置tencent_sync_summary为: {tencent_sync_summary}")
            # 继续执行，不阻止返回成功响应

        print("[updateUserInfo] 所有步骤完成, 准备返回成功响应")
        response_data = {'data': {'message': '更新成功', 'code': 200, 'tencent_sync': tencent_sync_summary}}
        print(f"[updateUserInfo] 响应数据: {response_data}")
        try:
            response = JSONResponse(response_data)
            print(f"[updateUserInfo] JSONResponse创建成功: {response}")
            return response
        except Exception as e:
            print(f"[updateUserInfo] 创建响应时出错: {type(e).__name__}: {str(e)}")
            print(f"[updateUserInfo] 异常堆栈:\n{traceback.format_exc()}")
            raise
    
    except Exception as e:
        app_logger.error(f"UpdateUserInfo failed: Unexpected error - {type(e).__name__}: {str(e)}")
        print(f"[updateUserInfo] ========== 未预期的异常 ==========")
        print(f"[updateUserInfo] 异常类型: {type(e).__name__}")
        print(f"[updateUserInfo] 异常消息: {str(e)}")
        print(f"[updateUserInfo] 异常参数: {e.args}")
        import traceback
        exc_tb = traceback.format_exc()
        print(f"[updateUserInfo] 完整异常堆栈:\n{exc_tb}")
        print(f"[updateUserInfo] 当前变量状态:")
        print(f"[updateUserInfo]   - connection: {connection}")
        print(f"[updateUserInfo]   - cursor: {cursor}")
        print(f"[updateUserInfo]   - avatar_url: {avatar_url}")
        print(f"[updateUserInfo]   - user_details: {user_details}")
        print(f"[updateUserInfo]   - tencent_identifier: {tencent_identifier}")
        print(f"[updateUserInfo] ==================================")
        return JSONResponse({'data': {'message': f'更新失败: {str(e)}', 'code': 500}}, status_code=500)
    
    finally:
        print("[updateUserInfo] 清理资源...")
        if cursor:
            try:
                cursor.close()
                print("[updateUserInfo] 游标已关闭")
            except Exception as e:
                print(f"[updateUserInfo] 关闭游标时出错: {str(e)}")
        if connection and connection.is_connected():
            try:
                connection.close()
                print("[updateUserInfo] 数据库连接已关闭")
                app_logger.info(f"Database connection closed after updating user info.")
            except Exception as e:
                print(f"[updateUserInfo] 关闭数据库连接时出错: {str(e)}")
        print("[updateUserInfo] 资源清理完成")
        print("=" * 80)


@app.post("/updateUserName")
async def update_user_name(request: Request):
    data = await request.json()
    print(f"[updateUserName] Received payload: {data}")
    name = data.get('name')
    id_number = data.get('id_number')
    phone = data.get('phone')

    if not name or (not id_number and not phone):
        app_logger.warning("update_user_name failed: Missing name or identifier.")
        return JSONResponse(
            {'data': {'message': '姓名和身份证号码或手机号必须提供', 'code': 400}},
            status_code=400
        )

    connection = get_db_connection()
    if connection is None:
        app_logger.error("update_user_name failed: Database connection error.")
        return JSONResponse(
            {'data': {'message': '数据库连接失败', 'code': 500}},
            status_code=500
        )

    cursor = None
    user_details: Optional[Dict[str, Any]] = None
    effective_id_number: Optional[str] = id_number
    tencent_identifier: Optional[str] = None
    try:
        cursor = connection.cursor(dictionary=True)

        if id_number:
            cursor.execute(
                "UPDATE ta_user_details SET name = %s WHERE id_number = %s",
                (name, id_number)
            )
        else:
            cursor.execute(
                "UPDATE ta_user_details SET name = %s WHERE phone = %s",
                (name, phone)
            )
            cursor.execute(
                "SELECT id_number FROM ta_user_details WHERE phone = %s",
                (phone,)
            )
            row = cursor.fetchone()
            if row:
                effective_id_number = row.get("id_number")

        if cursor.rowcount == 0:
            app_logger.warning("update_user_name: No matching user_details record found.")
            return JSONResponse(
                {'data': {'message': '未找到对应的用户信息', 'code': 404}},
                status_code=404
            )

        # 选填: 同步更新 ta_teacher 的姓名（如果存在）
        if effective_id_number:
            cursor.execute(
                "UPDATE ta_teacher SET name = %s WHERE id_card = %s",
                (name, effective_id_number)
            )

        connection.commit()

        cursor.execute(
            "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE id_number = %s",
            (effective_id_number,)
        )
        user_details = cursor.fetchone()
        if not user_details and phone:
            cursor.execute(
                "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE phone = %s",
                (phone,)
            )
            user_details = cursor.fetchone()

        tencent_identifier = resolve_tencent_identifier(
            connection,
            id_number=effective_id_number,
            phone=phone
        )

    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error during update_user_name for {id_number or phone}: {e}")
        return JSONResponse(
            {'data': {'message': '用户名更新失败', 'code': 500}},
            status_code=500
        )
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after update_user_name for {id_number or phone}.")

    avatar_for_sync = None
    if user_details:
        avatar_for_sync = user_details.get("avatar")

    print(f"[updateUserName] Tencent sync request -> identifier={tencent_identifier or effective_id_number or phone}, "
          f"name={name}, avatar={avatar_for_sync}")
    app_logger.info(
        f"updateUserName: 准备同步腾讯用户资料 identifier={tencent_identifier or effective_id_number or phone}, "
        f"name={name}, avatar={avatar_for_sync}"
    )
    tencent_sync_summary = await notify_tencent_user_profile(
        tencent_identifier or effective_id_number or phone,
        name=name,
        avatar_url=avatar_for_sync
    )
    print(f"[updateUserName] Tencent sync response <- {tencent_sync_summary}")
    app_logger.info(f"updateUserName: 腾讯接口返回 {tencent_sync_summary}")

    return JSONResponse({'data': {'message': '用户名更新成功', 'code': 200, 'tencent_sync': tencent_sync_summary}})


async def _update_user_field(phone: Optional[str], field: str, value, field_label: str, id_number: Optional[str] = None):
    if (not phone and not id_number) or value is None:
        return JSONResponse(
            {'data': {'message': f'手机号或身份证号以及{field_label}必须提供', 'code': 400}},
            status_code=400
        )

    connection = get_db_connection()
    if connection is None:
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        if id_number:
            update_query = f"UPDATE ta_user_details SET {field} = %s WHERE id_number = %s"
            params = (value, id_number)
            print(f"[{field_label}] SQL -> {update_query}, params={params}")
            cursor.execute(update_query, params)
        else:
            cursor.execute("SELECT id_number FROM ta_user_details WHERE phone = %s", (phone,))
            row = cursor.fetchone()
            if row:
                id_number = row.get("id_number")
                print(f"[{field_label}] Resolved id_number={id_number} from phone={phone}")
            update_query = f"UPDATE ta_user_details SET {field} = %s WHERE phone = %s"
            params = (value, phone)
            print(f"[{field_label}] SQL -> {update_query}, params={params}")
            cursor.execute(update_query, params)
        if cursor.rowcount == 0:
            connection.commit()
            print(f"[{field_label}] No ta_user_details record found for phone={phone}, id_number={id_number}")
            return JSONResponse({'data': {'message': '未找到对应的用户信息', 'code': 404}}, status_code=404)

        connection.commit()
        print(f"[{field_label}] Update success for phone={phone}")
        return JSONResponse({'data': {'message': f'{field_label}更新成功', 'code': 200}})
    except Error as e:
        connection.rollback()
        app_logger.error(f"数据库错误: 更新{field_label}失败 phone={phone}: {e}")
        return JSONResponse({'data': {'message': f'{field_label}更新失败', 'code': 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def _normalize_is_administrator(value: Optional[Union[str, int, bool]]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, int):
        return "是" if value else "否"
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        lower_value = normalized.lower()
        truthy = {"1", "true", "yes", "y", "管理员", "是"}
        falsy = {"0", "false", "no", "n", "普通", "否"}
        if lower_value in truthy:
            return "是"
        if lower_value in falsy:
            return "否"
        return normalized
    return str(value)


@app.post("/updateUserAdministrator")
async def update_user_administrator(request: Request):
    data = await request.json()
    print(f"[updateUserAdministrator] Received payload: {data}")
    phone = data.get("phone")
    id_number = data.get("id_number")
    is_administrator_raw = data.get("is_administrator")
    normalized_value = _normalize_is_administrator(is_administrator_raw)

    if normalized_value is None:
        return JSONResponse(
            {'data': {'message': '管理员状态不能为空', 'code': 400}},
            status_code=400
        )

    return await _update_user_field(
        phone,
        "is_administrator",
        normalized_value,
        "管理员状态",
        id_number=id_number
    )


@app.post("/updateUserSex")
async def update_user_sex(request: Request):
    data = await request.json()
    print(f"[updateUserSex] Received payload: {data}")
    phone = data.get('phone')
    id_number = data.get('id_number')
    sex = data.get('sex')
    return await _update_user_field(phone, "sex", sex, "性别", id_number=id_number)


@app.post("/updateUserAddress")
async def update_user_address(request: Request):
    data = await request.json()
    print(f"[updateUserAddress] Received payload: {data}")
    phone = data.get('phone')
    id_number = data.get('id_number')
    address = data.get('address')
    return await _update_user_field(phone, "address", address, "地址", id_number=id_number)


@app.post("/updateUserSchoolName")
async def update_user_school_name(request: Request):
    data = await request.json()
    print(f"[updateUserSchoolName] Received payload: {data}")
    phone = data.get('phone')
    id_number = data.get('id_number')
    school_name = data.get('school_name')
    return await _update_user_field(phone, "school_name", school_name, "学校名称", id_number=id_number)


@app.post("/updateUserGradeLevel")
async def update_user_grade_level(request: Request):
    data = await request.json()
    print(f"[updateUserGradeLevel] Received payload: {data}")
    phone = data.get('phone')
    id_number = data.get('id_number')
    grade_level = data.get('grade_level')
    return await _update_user_field(phone, "grade_level", grade_level, "学段", id_number=id_number)


@app.get("/userInfo")
async def list_userInfo(request: Request):
    connection = get_db_connection()
    if connection is None:
        app_logger.error("Get User Info failed: Database connection error.")
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500, 'userinfo': []}}, status_code=500)

    cursor = None
    try:
        phone_filter = request.query_params.get('phone')
        user_id_filter = request.query_params.get('userid')  # 新增: userid 参数
        print(" xxx user_id_filter:", user_id_filter)
        # 如果传的是 userid 而不是 phone
        if not phone_filter and user_id_filter:
            app_logger.info(f"Received userid={user_id_filter}, will fetch phone from ta_user table.")
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT phone FROM ta_user WHERE id = %s", (user_id_filter,))
            user_row = cursor.fetchone()
            if not user_row:
                app_logger.warning(f"No user found with id={user_id_filter}")
                return JSONResponse({'data': {'message': '未找到该用户', 'code': 404, 'userinfo': []}}, status_code=404)
            phone_filter = user_row["phone"]  # 从 ta_user 获取 phone
            cursor.close()

        print(" xxx phone_filter:", phone_filter)
        if not phone_filter:
            return JSONResponse({'data': {'message': '缺少必要参数 phone 或 userid', 'code': 400, 'userinfo': []}}, status_code=400)

        # 继续走原来的逻辑：关联 ta_user_details 和 ta_teacher
        base_query = """
            SELECT u.*, t.teacher_unique_id, t.schoolId AS schoolId
            FROM ta_user_details AS u
            LEFT JOIN ta_teacher AS t ON u.id_number = t.id_card
            WHERE u.phone = %s
        """

        cursor = connection.cursor(dictionary=True)
        cursor.execute(base_query, (phone_filter,))
        userinfo = cursor.fetchall()

        # 附加头像Base64字段
        for user in userinfo:
            avatar_path = user.get("avatar")
            local_avatar_file = resolve_local_avatar_file_path(avatar_path)
            if local_avatar_file and os.path.exists(local_avatar_file):
                try:
                    with open(local_avatar_file, "rb") as img:
                        user["avatar_base64"] = base64.b64encode(img.read()).decode("utf-8")
                except Exception as e:
                    app_logger.error(f"读取图片失败 {local_avatar_file}: {e}")
                    user["avatar_base64"] = None
            else:
                user["avatar_base64"] = None

        app_logger.info(f"Fetched {len(userinfo)} userinfo.")
        return safe_json_response({'data': {'message': '获取用户信息成功', 'code': 200, 'userinfo': userinfo}})

    except Error as e:
        print("Database error during fetching userinfo:", e)
        app_logger.error(f"Database error during fetching userinfo: {e}")
        return JSONResponse({'data': {'message': '获取用户信息失败', 'code': 500, 'userinfo': []}}, status_code=500)
    except Exception as e:
        print("Unexpected error during fetching userinfo:", e)
        app_logger.error(f"Unexpected error during fetching userinfo: {e}")
        return JSONResponse({'data': {'message': '内部服务器错误', 'code': 500, 'userinfo': []}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching userinfo.")

def generate_class_code(connection, schoolid):
    """
    生成唯一的班级编号 class_code
    格式：前6位是 schoolid（左补零），后3位是流水号（左补零），总长度9位
    例如：如果 schoolid=123456，流水号=1，则 class_code=123456001
    
    优先重用被删除的编号（从1开始查找最小的未使用流水号）
    如果1-999都被使用，则使用最大流水号+1
    """
    if not schoolid:
        app_logger.error("generate_class_code: schoolid 不能为空")
        return None
    
    cursor = None
    try:
        cursor = connection.cursor()
        
        # 将 schoolid 转换为字符串并左补零到6位
        schoolid_str = str(schoolid).zfill(6)
        if len(schoolid_str) > 6:
            # 如果超过6位，取前6位
            schoolid_str = schoolid_str[:6]
        
        # 查询该 schoolid 下所有已使用的流水号（1-999范围内）
        cursor.execute("""
            SELECT CAST(SUBSTRING(class_code, 7) AS UNSIGNED) AS sequence_num
            FROM ta_classes
            WHERE class_code LIKE %s AND LENGTH(class_code) = 9
            AND CAST(SUBSTRING(class_code, 7) AS UNSIGNED) BETWEEN 1 AND 999
            ORDER BY sequence_num ASC
        """, (f"{schoolid_str}%",))
        
        used_sequences = set()
        for row in cursor.fetchall():
            if row and row[0]:
                try:
                    used_sequences.add(int(row[0]))
                except (ValueError, TypeError):
                    pass
        
        # 查找最小的未使用流水号（从1开始）
        new_sequence = None
        for seq in range(1, 1000):  # 1-999
            if seq not in used_sequences:
                new_sequence = seq
                break
        
        # 如果1-999都被使用，使用最大流水号+1
        if new_sequence is None:
            cursor.execute("""
                SELECT CAST(SUBSTRING(class_code, 7) AS UNSIGNED) AS sequence_num
                FROM ta_classes
                WHERE class_code LIKE %s AND LENGTH(class_code) = 9
                ORDER BY sequence_num DESC
                LIMIT 1
            """, (f"{schoolid_str}%",))
            result = cursor.fetchone()
            if result and result[0]:
                try:
                    max_sequence = int(result[0])
                    new_sequence = max_sequence + 1
                except (ValueError, TypeError):
                    new_sequence = 1
            else:
                new_sequence = 1
        
        # 检查流水号是否超过999
        if new_sequence > 999:
            app_logger.error(f"generate_class_code: schoolid {schoolid_str} 的流水号已超过999")
            return None
        
        # 将流水号左补零到3位
        sequence_str = str(new_sequence).zfill(3)
        
        # 组合成 class_code（9位：6位schoolid + 3位流水号）
        class_code = f"{schoolid_str}{sequence_str}"
        
        # 再次检查是否已存在（防止并发问题）
        cursor.execute("SELECT class_code FROM ta_classes WHERE class_code = %s", (class_code,))
        if cursor.fetchone() is not None:
            # 如果已存在，尝试下一个未使用的流水号
            for seq in range(new_sequence + 1, 1000):
                if seq not in used_sequences:
                    new_sequence = seq
                    sequence_str = str(new_sequence).zfill(3)
                    class_code = f"{schoolid_str}{sequence_str}"
                    # 再次检查
                    cursor.execute("SELECT class_code FROM ta_classes WHERE class_code = %s", (class_code,))
                    if cursor.fetchone() is None:
                        break
            else:
                # 如果都冲突，使用最大+1
                app_logger.warning(f"generate_class_code: 所有流水号都被使用，使用最大+1")
                new_sequence = max(used_sequences) + 1 if used_sequences else 1
                if new_sequence > 999:
                    app_logger.error(f"generate_class_code: schoolid {schoolid_str} 的流水号已超过999（并发冲突）")
                    return None
                sequence_str = str(new_sequence).zfill(3)
                class_code = f"{schoolid_str}{sequence_str}"
        
        return class_code
    except Error as e:
        app_logger.error(f"Error generating class_code: {e}")
        return None
    finally:
        if cursor:
            cursor.close()

@app.post("/updateClasses")
async def updateClasses(request: Request):
    data_list = await request.json()
    if not isinstance(data_list, list) or not data_list:
        return JSONResponse({'data': {'message': '必须提供班级数组数据', 'code': 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    try:
        cursor = connection.cursor()
        sql = """
        INSERT INTO ta_classes (
            class_code, school_stage, grade, class_name, remark, schoolid, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            school_stage = VALUES(school_stage),
            grade        = VALUES(grade),
            class_name   = VALUES(class_name),
            remark       = VALUES(remark),
            schoolid     = VALUES(schoolid),
            created_at   = VALUES(created_at);
        """
        values = []
        result_list = []  # 用于返回完整的列表
        
        # 用于跟踪每个 schoolid 的流水号（批量处理时避免重复）
        schoolid_sequence_map = {}  # {schoolid: current_sequence}
        
        for item in data_list:
            class_code = item.get('class_code')
            schoolid = item.get('schoolid')  # 从上传的数据中获取 schoolid
            
            # 如果 class_code 为空，则生成新的唯一编号
            if not class_code or class_code.strip() == '':
                # 如果 schoolid 也为空，无法生成 class_code
                if not schoolid or str(schoolid).strip() == '':
                    app_logger.error(f"生成 class_code 失败：缺少 schoolid，跳过该班级: {item}")
                    continue
                
                # 确保 schoolid 格式正确
                schoolid_str = str(schoolid).zfill(6)[:6]
                
                # 如果是第一次遇到这个 schoolid，查询数据库中所有已使用的流水号
                if schoolid_str not in schoolid_sequence_map:
                    try:
                        check_cursor = connection.cursor()
                        # 查询所有已使用的流水号（1-999范围内）
                        check_cursor.execute("""
                            SELECT CAST(SUBSTRING(class_code, 7) AS UNSIGNED) AS sequence_num
                            FROM ta_classes
                            WHERE class_code LIKE %s AND LENGTH(class_code) = 9
                            AND CAST(SUBSTRING(class_code, 7) AS UNSIGNED) BETWEEN 1 AND 999
                            ORDER BY sequence_num ASC
                        """, (f"{schoolid_str}%",))
                        
                        used_sequences = set()
                        for row in check_cursor.fetchall():
                            if row and row[0]:
                                try:
                                    used_sequences.add(int(row[0]))
                                except (ValueError, TypeError):
                                    pass
                        
                        # 存储已使用的流水号集合，用于查找最小的未使用流水号
                        schoolid_sequence_map[schoolid_str] = {
                            'used': used_sequences,
                            'next': 1  # 下一个要尝试的流水号
                        }
                        check_cursor.close()
                    except Exception as e:
                        app_logger.error(f"查询 schoolid {schoolid_str} 的已使用流水号失败: {e}")
                        schoolid_sequence_map[schoolid_str] = {
                            'used': set(),
                            'next': 1
                        }
                
                # 查找最小的未使用流水号
                seq_info = schoolid_sequence_map[schoolid_str]
                used_sequences = seq_info['used']
                next_seq = seq_info['next']
                
                # 从 next_seq 开始查找未使用的流水号
                new_sequence = None
                for seq in range(next_seq, 1000):  # 从 next_seq 到 999
                    if seq not in used_sequences:
                        new_sequence = seq
                        # 更新下一个要尝试的流水号
                        seq_info['next'] = seq + 1
                        # 将该流水号标记为已使用（在当前批量处理中）
                        used_sequences.add(seq)
                        break
                
                # 如果从 next_seq 到 999 都被使用，从1开始查找
                if new_sequence is None:
                    for seq in range(1, next_seq):
                        if seq not in used_sequences:
                            new_sequence = seq
                            seq_info['next'] = seq + 1
                            used_sequences.add(seq)
                            break
                
                # 如果1-999都被使用，使用最大流水号+1
                if new_sequence is None:
                    try:
                        check_cursor = connection.cursor()
                        check_cursor.execute("""
                            SELECT CAST(SUBSTRING(class_code, 7) AS UNSIGNED) AS sequence_num
                            FROM ta_classes
                            WHERE class_code LIKE %s AND LENGTH(class_code) = 9
                            ORDER BY sequence_num DESC
                            LIMIT 1
                        """, (f"{schoolid_str}%",))
                        result = check_cursor.fetchone()
                        if result and result[0]:
                            try:
                                max_sequence = int(result[0])
                                new_sequence = max_sequence + 1
                            except (ValueError, TypeError):
                                new_sequence = 1
                        else:
                            new_sequence = 1
                        check_cursor.close()
                    except Exception as e:
                        app_logger.error(f"查询 schoolid {schoolid_str} 的最大流水号失败: {e}")
                        new_sequence = 1
                    
                    # 更新下一个要尝试的流水号
                    seq_info['next'] = new_sequence + 1
                    used_sequences.add(new_sequence)
                
                # 检查流水号是否超过999
                if new_sequence > 999:
                    app_logger.error(f"生成 class_code 失败：schoolid {schoolid_str} 的流水号已超过999")
                    continue
                
                # 生成 class_code
                sequence_str = str(new_sequence).zfill(3)
                class_code = f"{schoolid_str}{sequence_str}"
                
                # 再次检查是否已存在（防止并发问题）
                try:
                    check_cursor = connection.cursor()
                    check_cursor.execute("SELECT class_code FROM ta_classes WHERE class_code = %s", (class_code,))
                    if check_cursor.fetchone() is not None:
                        # 如果已存在，标记为已使用，并查找下一个未使用的流水号
                        used_sequences.add(new_sequence)
                        # 从下一个流水号开始查找
                        for seq in range(new_sequence + 1, 1000):
                            if seq not in used_sequences:
                                new_sequence = seq
                                seq_info['next'] = seq + 1
                                used_sequences.add(seq)
                                sequence_str = str(new_sequence).zfill(3)
                                class_code = f"{schoolid_str}{sequence_str}"
                                # 再次检查
                                check_cursor.execute("SELECT class_code FROM ta_classes WHERE class_code = %s", (class_code,))
                                if check_cursor.fetchone() is None:
                                    break
                        else:
                            # 如果都冲突，使用最大+1
                            app_logger.warning(f"生成 class_code 时所有流水号都冲突，使用最大+1")
                            max_used = max(used_sequences) if used_sequences else 0
                            new_sequence = max_used + 1
                            if new_sequence > 999:
                                app_logger.error(f"生成 class_code 失败：schoolid {schoolid_str} 的流水号已超过999（并发冲突）")
                                check_cursor.close()
                                continue
                            seq_info['next'] = new_sequence + 1
                            used_sequences.add(new_sequence)
                            sequence_str = str(new_sequence).zfill(3)
                            class_code = f"{schoolid_str}{sequence_str}"
                    check_cursor.close()
                except Exception as e:
                    app_logger.warning(f"检查 class_code 是否存在时出错: {e}")
                
                # 更新 item 中的 class_code，以便返回给客户端
                item['class_code'] = class_code
                print(f"[updateClasses] 为班级生成新的 class_code: {class_code}, schoolid: {schoolid_str}, sequence: {new_sequence}")
            
            # 如果 class_code 已存在，从 class_code 的前六位提取作为 schoolid（如果 schoolid 为空）
            if not schoolid or str(schoolid).strip() == '':
                schoolid = class_code[:6] if len(class_code) >= 6 else class_code
            else:
                # 确保 schoolid 是字符串格式
                schoolid = str(schoolid).zfill(6)[:6]
            
            values.append((
                class_code,
                item.get('school_stage'),
                item.get('grade'),
                item.get('class_name'),
                item.get('remark'),
                schoolid
            ))
            
            # 添加到结果列表（包含生成的 class_code）
            result_list.append({
                'class_code': class_code,
                'school_stage': item.get('school_stage'),
                'grade': item.get('grade'),
                'class_name': item.get('class_name'),
                'remark': item.get('remark'),
                'schoolid': schoolid
            })
        
        if values:
            cursor.executemany(sql, values)
            connection.commit()
            print(f"[updateClasses] 批量插入/更新完成，共处理 {len(values)} 条记录")
        
        cursor.close()
        connection.close()
        
        response_data = {
            'data': {
                'message': '批量插入/更新完成', 
                'code': 200, 
                'count': len(result_list),
                'classes': result_list  # 返回完整的列表，包括生成的 class_code
            }
        }
        
        # 打印返回的 JSON 结果
        try:
            response_json = json.dumps(response_data, ensure_ascii=False, indent=2)
            print(f"[updateClasses] 返回的 JSON 结果:\n{response_json}")
            app_logger.info(f"[updateClasses] 返回的 JSON 结果: {json.dumps(response_data, ensure_ascii=False)}")
        except Exception as json_error:
            print(f"[updateClasses] 打印 JSON 时出错: {json_error}")
            app_logger.warning(f"[updateClasses] 打印 JSON 时出错: {json_error}")
        
        return safe_json_response(response_data)
    except Error as e:
        if connection:
            connection.rollback()
        app_logger.error(f"Database error during updateClasses: {e}")
        return JSONResponse({'data': {'message': f'数据库操作失败: {e}', 'code': 500}}, status_code=500)
    except Exception as e:
        if connection:
            connection.rollback()
        app_logger.error(f"Unexpected error during updateClasses: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        app_logger.error(f"Error stack: {traceback_str}")
        return JSONResponse({'data': {'message': f'操作失败: {str(e)}', 'code': 500}}, status_code=500)


@app.post("/deleteClasses")
async def delete_classes(request: Request):
    """
    删除班级接口
    接收班级编号列表，从 ta_classes 表中删除对应的班级
    删除后，系统唯一班级编号会被收回（可以重新使用）
    """
    print("=" * 80)
    print("[deleteClasses] 收到删除班级请求")
    
    try:
        data = await request.json()
        print(f"[deleteClasses] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        # 支持多种格式：
        # 1. 数组格式：[{"class_code": "123456001", ...}, {"class_code": "123456002", ...}]
        # 2. {"class_codes": ["123456001", "123456002"]} - 批量删除（字符串数组）
        # 3. {"class_code": "123456001"} - 单个删除
        class_codes = []
        
        if isinstance(data, list):
            # 如果是数组格式，提取每个对象的 class_code
            for item in data:
                if isinstance(item, dict) and "class_code" in item:
                    class_code = item.get("class_code")
                    if class_code:
                        class_codes.append(class_code)
            print(f"[deleteClasses] 从数组格式中提取到 {len(class_codes)} 个 class_code")
        elif isinstance(data, dict):
            # 如果是对象格式
            if "class_codes" in data and isinstance(data["class_codes"], list):
                class_codes = data["class_codes"]
            elif "class_code" in data:
                class_codes = [data["class_code"]]
            else:
                print("[deleteClasses] 错误: 对象格式中缺少 class_code 或 class_codes 参数")
                return JSONResponse({
                    'data': {
                        'message': '缺少必需参数 class_code 或 class_codes',
                        'code': 400
                    }
                }, status_code=400)
        else:
            print("[deleteClasses] 错误: 请求数据格式不正确，应为数组或对象")
            return JSONResponse({
                'data': {
                    'message': '请求数据格式不正确，应为数组或对象',
                    'code': 400
                }
            }, status_code=400)
        
        if not class_codes:
            print("[deleteClasses] 错误: class_codes 列表为空")
            return JSONResponse({
                'data': {
                    'message': 'class_codes 列表不能为空',
                    'code': 400
                }
            }, status_code=400)
        
        print(f"[deleteClasses] 准备删除 {len(class_codes)} 个班级: {class_codes}")
        app_logger.info(f"[deleteClasses] 收到删除请求 - class_codes: {class_codes}")
        
        connection = get_db_connection()
        if connection is None:
            print("[deleteClasses] 错误: 数据库连接失败")
            app_logger.error("[deleteClasses] 数据库连接失败")
            return JSONResponse({
                'data': {
                    'message': '数据库连接失败',
                    'code': 500
                }
            }, status_code=500)
        
        print("[deleteClasses] 数据库连接成功")
        app_logger.info("[deleteClasses] 数据库连接成功")
        
        cursor = None
        try:
            cursor = connection.cursor()
            
            # 先查询要删除的班级是否存在
            placeholders = ','.join(['%s'] * len(class_codes))
            check_sql = f"SELECT class_code, class_name FROM ta_classes WHERE class_code IN ({placeholders})"
            cursor.execute(check_sql, tuple(class_codes))
            existing_classes = cursor.fetchall()
            
            # 处理查询结果（可能是元组或列表）
            existing_codes = []
            if existing_classes:
                for row in existing_classes:
                    if isinstance(row, (tuple, list)):
                        existing_codes.append(row[0])
                    elif isinstance(row, dict):
                        existing_codes.append(row.get('class_code'))
                    else:
                        existing_codes.append(str(row))
            
            not_found_codes = [code for code in class_codes if code not in existing_codes]
            
            print(f"[deleteClasses] 找到 {len(existing_codes)} 个班级，未找到 {len(not_found_codes)} 个")
            app_logger.info(f"[deleteClasses] 查询结果 - 找到: {existing_codes}, 未找到: {not_found_codes}")
            
            if not existing_codes:
                print("[deleteClasses] 未找到任何要删除的班级")
                return JSONResponse({
                    'data': {
                        'message': '未找到要删除的班级',
                        'code': 404,
                        'deleted_count': 0,
                        'not_found_codes': not_found_codes
                    }
                }, status_code=404)
            
            # 执行删除操作
            delete_sql = f"DELETE FROM ta_classes WHERE class_code IN ({placeholders})"
            cursor.execute(delete_sql, tuple(existing_codes))
            deleted_count = cursor.rowcount
            connection.commit()
            
            print(f"[deleteClasses] 删除完成，成功删除 {deleted_count} 个班级")
            app_logger.info(f"[deleteClasses] 删除完成 - 成功删除 {deleted_count} 个班级，class_codes: {existing_codes}")
            
            result = {
                'message': '删除班级成功',
                'code': 200,
                'deleted_count': deleted_count,
                'deleted_codes': existing_codes
            }
            
            if not_found_codes:
                result['not_found_codes'] = not_found_codes
                result['message'] = f'部分删除成功，{len(not_found_codes)} 个班级未找到'
            
            print(f"[deleteClasses] 返回结果: {result}")
            print("=" * 80)
            
            return safe_json_response({'data': result})
            
        except mysql.connector.Error as e:
            if connection:
                connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[deleteClasses] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[deleteClasses] 错误堆栈: {traceback_str}")
            app_logger.error(f"[deleteClasses] {error_msg}\n{traceback_str}")
            return JSONResponse({
                'data': {
                    'message': f'数据库操作失败: {str(e)}',
                    'code': 500
                }
            }, status_code=500)
        except Exception as e:
            if connection:
                connection.rollback()
            error_msg = f"删除班级时发生异常: {e}"
            print(f"[deleteClasses] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[deleteClasses] 错误堆栈: {traceback_str}")
            app_logger.error(f"[deleteClasses] {error_msg}\n{traceback_str}")
            return JSONResponse({
                'data': {
                    'message': f'操作失败: {str(e)}',
                    'code': 500
                }
            }, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[deleteClasses] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[deleteClasses] 数据库连接已关闭")
                app_logger.info("[deleteClasses] 数据库连接已关闭")
    
    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[deleteClasses] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[deleteClasses] 错误堆栈: {traceback_str}")
        app_logger.error(f"[deleteClasses] {error_msg}\n{traceback_str}")
        return JSONResponse({
            'data': {
                'message': '请求数据格式错误',
                'code': 400
            }
        }, status_code=400)
    finally:
        print("=" * 80)


@app.post("/getClassesByPrefix")
async def get_classes_by_prefix(request: Request):
    data = await request.json()
    prefix = data.get("prefix")
    if not prefix or len(prefix) != 6 or not prefix.isdigit():
        return JSONResponse({'data': {'message': '必须提供6位数字前缀', 'code': 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)
        sql = """
        SELECT class_code, school_stage, grade, class_name, remark, schoolid, created_at
        FROM ta_classes
        WHERE LEFT(class_code, 6) = %s
        """
        cursor.execute(sql, (prefix,))
        results = cursor.fetchall()
        #results = jsonable_encoder(results)
        cursor.close()
        connection.close()
        return safe_json_response({'data': {'message': '查询成功', 'code': 200, 'count': len(results), 'classes': results}})
    except Error as e:
        app_logger.error(f"查询失败: {e}")
        return JSONResponse({'data': {'message': '查询失败', 'code': 500}}, status_code=500)


@app.post("/updateSchoolInfo")
async def updateSchoolInfo(request: Request):
    data = await request.json()
    id = data.get('id')
    name = data.get('name')
    address = data.get('address')

    if not id:
        app_logger.warning("UpdateSchoolInfo failed: Missing id.")
        return JSONResponse({'data': {'message': 'id值必须提供', 'code': 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("UpdateSchoolInfo failed: Database connection error.")
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    cursor = None
    try:
        update_query = "UPDATE ta_school SET name = %s, address = %s WHERE id = %s"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(update_query, (name, address, id))
        connection.commit()
        cursor.close()
        return JSONResponse({'data': {'message': '更新成功', 'code': 200}})
    except Error as e:
        app_logger.error(f"Database error during updateSchoolInfo for {name}: {e}")
        return JSONResponse({'data': {'message': '更新失败', 'code': 500}}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after updating school info for {name}.")


# # 生成教师唯一编号
# def generate_teacher_unique_id(school_id):
#     connection = get_db_connection()
#     if connection is None:
#         return None
#     cursor = None
#     try:
#         print(" generate_teacher_unique_id 00\n");
#         cursor = connection.cursor()

#         print(" generate_teacher_unique_id 01:", school_id, "\n");
#         cursor.execute("""
#             SELECT MAX(teacher_unique_id) 
#             FROM ta_teacher 
#             WHERE schoolId = %s
#         """, (school_id,))
#         print(" generate_teacher_unique_id 10\n");
#         result = cursor.fetchone()
#         print(" generate_teacher_unique_id 11", result, "\n");
#         if result and result[0]:
#             last_num = int(str(result[0])[6:])
#             new_num = last_num + 1
#         else:
#             new_num = 1

#         return int(f"{school_id}{str(new_num).zfill(4)}")
#     except Error as e:
#         app_logger.error(f"Error generating teacher_unique_id: {e}")
#         return None
#     finally:
#         if cursor:
#             cursor.close()
#         if connection and connection.is_connected():
#             connection.close()

from fastapi import Request
from fastapi.responses import JSONResponse
import datetime

def generate_teacher_unique_id(school_id):
    """
    并发安全生成 teacher_unique_id
    格式：前6位为schoolId（左补零），后4位为流水号（左补零），总长度10位
    返回字符串类型
    """
    connection = get_db_connection()
    if connection is None:
        return None
    cursor = None
    try:
        cursor = connection.cursor()
        connection.start_transaction()
        cursor.execute("""
            SELECT teacher_unique_id
            FROM ta_teacher
            WHERE schoolId = %s
            ORDER BY CAST(teacher_unique_id AS UNSIGNED) DESC
            LIMIT 1
            FOR UPDATE
        """, (school_id,))
        result = cursor.fetchone()
        if result and result[0]:
            # teacher_unique_id 现在是字符串类型，格式为10位数字字符串
            max_id_str = str(result[0]).zfill(10)
            last_num = int(max_id_str[6:])
            new_num = last_num + 1
        else:
            new_num = 1
        teacher_unique_id_str = f"{str(school_id).zfill(6)}{str(new_num).zfill(4)}"
        return teacher_unique_id_str
    except Error as e:
        app_logger.error(f"Error generating teacher_unique_id: {e}")
        return None
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close()


@app.post("/add_teacher")
async def add_teacher(request: Request):
    data = await request.json()
    if not data or 'schoolId' not in data:
        return JSONResponse({'data': {'message': '缺少 schoolId', 'code': 400}}, status_code=400)

    print(data)

    school_id = data['schoolId']
    teacher_unique_id = generate_teacher_unique_id(school_id)
    if teacher_unique_id is None:
        return JSONResponse({'data': {'message': '生成教师唯一编号失败', 'code': 500}}, status_code=500)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Add teacher failed: Database connection error.")
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    is_admin_flag = data.get('is_Administarator')
    try:
        if isinstance(is_admin_flag, bool):
            is_admin_flag = int(is_admin_flag)
        else:
            is_admin_flag = int(is_admin_flag) if is_admin_flag is not None else 0
    except ValueError:
        is_admin_flag = 0

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        # 生成字符串主键（与 ta_teacher.id=VARCHAR(255) 兼容）
        generated_teacher_id = str(uuid.uuid4())
        sql_insert = """
        INSERT INTO ta_teacher 
        (id, name, icon, subject, gradeId, schoolId, is_Administarator, phone, id_card, sex, 
         teaching_tenure, education, graduation_institution, major, 
         teacher_certification_level, subjects_of_teacher_qualification_examination, 
         educational_stage, teacher_unique_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s)
        """
        cursor.execute(sql_insert, (
            generated_teacher_id,
            data.get('name'), data.get('icon'), data.get('subject'), data.get('gradeId'),
            school_id, is_admin_flag, data.get('phone'), data.get('id_card'),
            data.get('sex'), data.get('teaching_tenure'), data.get('education'),
            data.get('graduation_institution'), data.get('major'),
            data.get('teacher_certification_level'),
            data.get('subjects_of_teacher_qualification_examination'),
            data.get('educational_stage'), teacher_unique_id
        ))

        teacher_id = generated_teacher_id
        
        # 2️⃣ 检查 ta_user_details 是否已经存在该手机号
        cursor.execute("SELECT phone FROM ta_user_details WHERE phone = %s", (data.get('phone'),))
        user_exists = cursor.fetchone()

        if user_exists:
            # 已存在 -> 更新信息
            sql_update_user_details = """
            UPDATE ta_user_details
            SET name=%s, sex=%s, address=%s, school_name=%s, grade_level=%s, grade=%s,
                subject=%s, class_taught=%s, is_administrator=%s, id_number=%s
            WHERE phone=%s
            """
            cursor.execute(sql_update_user_details, (
                data.get('name'),
                data.get('sex'),
                data.get('address'),
                data.get('school_name'),
                data.get('grade_level'),
                data.get('grade'),
                data.get('subject'),
                data.get('class_taught'),
                str(is_admin_flag),
                data.get('id_card'),  # 教师表的 id_card 对应用户表的 id_number
                data.get('phone')
            ))
        else:
            # 不存在 -> 插入新用户详情
            sql_insert_user_details = """
            INSERT INTO ta_user_details 
            (phone, name, sex, address, school_name, grade_level, grade,
             subject, class_taught, is_administrator, avatar, id_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s)
            """
            cursor.execute(sql_insert_user_details, (
                data.get('phone'),
                data.get('name'),
                data.get('sex'),
                data.get('address'),
                data.get('school_name'),
                data.get('grade_level'),
                data.get('grade'),
                data.get('subject'),
                data.get('class_taught'),
                str(is_admin_flag),
                '',  # avatar 默认空字符串
                data.get('id_card')
            ))
        
        connection.commit()
        
        cursor.execute("SELECT * FROM ta_teacher WHERE id = %s", (teacher_id,))
        teacher_info = cursor.fetchone()
        return safe_json_response({'data': {'message': '新增教师成功', 'code': 200, 'teacher': teacher_info}})
    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error during adding teacher: {e}")
        return JSONResponse({'data': {'message': '新增教师失败', 'code': 500}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during adding teacher: {e}")
        return JSONResponse({'data': {'message': '内部服务器错误', 'code': 500}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after adding teacher.")


@app.post("/delete_teacher")
async def delete_teacher(request: Request):
    data = await request.json()
    if not data or "teacher_unique_id" not in data:
        return JSONResponse({'data': {'message': '缺少 teacher_unique_id', 'code': 400}}, status_code=400)

    teacher_unique_id = str(data["teacher_unique_id"])
    connection = get_db_connection()
    if connection is None:
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM ta_teacher WHERE teacher_unique_id = %s", (teacher_unique_id,))
        connection.commit()
        if cursor.rowcount > 0:
            return safe_json_response({'data': {'message': '删除教师成功', 'code': 200}})
        else:
            return safe_json_response({'data': {'message': '未找到对应教师', 'code': 404}}, status_code=404)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"删除教师时数据库异常: {e}")
        return JSONResponse({'data': {'message': '删除教师失败', 'code': 500}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close()


@app.get("/get_list_teachers")
async def get_list_teachers(request: Request):
    school_id = request.query_params.get("schoolId")
    final_query = "SELECT * FROM ta_teacher WHERE (%s IS NULL OR schoolId = %s)"
    params = (school_id, school_id)

    connection = get_db_connection()
    if connection is None:
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500, 'teachers': []}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, params)
        teachers = cursor.fetchall()
        app_logger.info(f"Fetched {len(teachers)} teachers.")
        return safe_json_response({'data': {'message': '获取老师列表成功', 'code': 200, 'teachers': teachers}})
    except Error as e:
        app_logger.error(f"Database error during fetching teachers: {e}")
        return JSONResponse({'data': {'message': '获取老师列表失败', 'code': 500, 'teachers': []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching teachers: {e}")
        return JSONResponse({'data': {'message': '内部服务器错误', 'code': 500, 'teachers': []}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching teachers.")


@app.get("/teachers")
async def list_teachers(request: Request):
    connection = get_db_connection()
    if connection is None:
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500, 'teachers': []}}, status_code=500)

    cursor = None
    try:
        school_id_filter = request.query_params.get('school_id')
        grade_id_filter = request.query_params.get('grade_id')
        name_filter = request.query_params.get('name')

        base_columns = "id, name, icon, subject, gradeId, schoolId"
        base_query = f"SELECT {base_columns} FROM ta_teacher WHERE 1=1"
        filters, params = [], []

        if school_id_filter:
            filters.append("AND schoolId = %s")
            params.append(school_id_filter)
        if grade_id_filter:
            filters.append("AND gradeId = %s")
            params.append(int(grade_id_filter))
        if name_filter:
            filters.append("AND name LIKE %s")
            params.append(f"%{name_filter}%")

        final_query = base_query + " " + " ".join(filters)
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        teachers = cursor.fetchall()
        app_logger.info(f"Fetched {len(teachers)} teachers.")
        return safe_json_response({'data': {'message': '获取老师列表成功', 'code': 200, 'teachers': teachers}})
    except Error as e:
        app_logger.error(f"Database error during fetching teachers: {e}")
        return JSONResponse({'data': {'message': '获取老师列表失败', 'code': 500, 'teachers': []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching teachers: {e}")
        return JSONResponse({'data': {'message': '内部服务器错误', 'code': 500, 'teachers': []}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching teachers.")


@app.get("/messages/recent")
async def get_recent_messages(request: Request):
    connection = get_db_connection()
    if connection is None:
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500, 'messages': []}}, status_code=500)

    cursor = None
    try:
        school_id = request.query_params.get('school_id')
        class_id = request.query_params.get('class_id')
        sender_id_filter = request.query_params.get('sender_id')

        three_days_ago = datetime.datetime.now() - datetime.timedelta(days=3)
        base_columns = "id, sender_id, content_type, text_content, school_id, class_id, sent_at, created_at, updated_at"
        base_query = f"SELECT {base_columns} FROM ta_message WHERE sent_at >= %s and content_type='text'"
        filters, params = [], [three_days_ago]

        if school_id: filters.append("AND school_id = %s"); params.append(school_id)
        if class_id: filters.append("AND class_id = %s"); params.append(int(class_id))
        if sender_id_filter: filters.append("AND sender_id = %s"); params.append(sender_id_filter)

        order_clause = "ORDER BY sent_at DESC"
        final_query = f"{base_query} {' '.join(filters)} {order_clause}"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        messages = cursor.fetchall()

        sender_ids = list(set(msg['sender_id'] for msg in messages))
        sender_info_map = {}
        if sender_ids:
            placeholders = ','.join(['%s'] * len(sender_ids))
            info_query = f"SELECT id, name, icon FROM ta_teacher WHERE id IN ({placeholders})"
            cursor.execute(info_query, tuple(sender_ids))
            teacher_infos = cursor.fetchall()
            sender_info_map = {t['id']: {'sender_name': t['name'], 'sender_icon': t['icon']} for t in teacher_infos}

        for msg in messages:
            info = sender_info_map.get(msg['sender_id'], {})
            msg['sender_name'] = info.get('sender_name', '未知老师')
            msg['sender_icon'] = info.get('sender_icon')
            for f in ['sent_at', 'created_at', 'updated_at']:
                if isinstance(msg.get(f), datetime.datetime):
                    msg[f] = msg[f].strftime('%Y-%m-%d %H:%M:%S')

        app_logger.info(f"Fetched {len(messages)} recent messages with sender info.")
        return safe_json_response({'data': {'message': '获取最近消息列表成功', 'code': 200, 'messages': messages}})
    except Error as e:
        app_logger.error(f"Database error during fetching recent messages: {e}")
        return JSONResponse({'data': {'message': '获取最近消息列表失败', 'code': 500, 'messages': []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching recent messages: {e}")
        return JSONResponse({'data': {'message': '内部服务器错误', 'code': 500, 'messages': []}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching recent messages.")

from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi import Path

@app.post("/messages")
async def add_message(request: Request):
    connection = get_db_connection()
    if not connection:
        return JSONResponse({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'message': None
            }
        }, status_code=500)

    cursor = None
    try:
        content_type_header = request.headers.get("content-type", "")

        # 先从 query 或 form 中获取 sender_id
        sender_id = request.query_params.get('sender_id')
        if sender_id:
            try:
                sender_id = str(sender_id).strip()
                if not sender_id:
                    sender_id = None
            except Exception:
                sender_id = None

        # === 情况1: JSON 格式 - 发送文本消息 ===
        if content_type_header.startswith('application/json'):
            data = await request.json()
            if not data:
                return JSONResponse({'data': {'message': '无效的 JSON 数据', 'code': 400, 'message': None}}, status_code=400)

            sender_id = data.get('sender_id') or sender_id
            text_content = data.get('text_content')
            content_type = data.get('content_type', 'text').lower()
            school_id = data.get('school_id')
            class_id = data.get('class_id')
            sent_at_str = data.get('sent_at')

            if not sender_id:
                return JSONResponse({'data': {'message': '缺少 sender_id', 'code': 400, 'message': None}}, status_code=400)
            if content_type != 'text':
                return JSONResponse({'data': {'message': 'content_type 必须为 text', 'code': 400, 'message': None}}, status_code=400)
            if not text_content or not text_content.strip():
                return JSONResponse({'data': {'message': 'text_content 不能为空', 'code': 400, 'message': None}}, status_code=400)

            text_content = text_content.strip()
            sent_at = datetime.datetime.now()
            if sent_at_str:
                try:
                    sent_at = datetime.datetime.strptime(sent_at_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return JSONResponse({'data': {'message': 'sent_at 格式错误，应为 YYYY-MM-DD HH:MM:SS', 'code': 400}}, status_code=400)

            # 插入数据库
            insert_query = """
                INSERT INTO ta_message 
                (sender_id, content_type, text_content, audio_data, school_id, class_id, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor = connection.cursor()
            cursor.execute(insert_query, (sender_id, 'text', text_content, None, school_id, class_id, sent_at))
            connection.commit()

            new_message_id = cursor.lastrowid
            message_dict = {
                'id': new_message_id,
                'sender_id': sender_id,
                'content_type': 'text',
                'text_content': text_content,
                'audio_url': None,
                'school_id': school_id,
                'class_id': class_id,
                'sent_at': sent_at.strftime('%Y-%m-%d %H:%M:%S'),
                'created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            return safe_json_response({'data': {'message': '文本消息发送成功', 'code': 201, 'message': message_dict}}, status_code=201)

        # === 情况2: 二进制流 - 发送音频消息 ===
        elif content_type_header.startswith('application/octet-stream'):
            if not sender_id:
                return JSONResponse({'data': {'message': '缺少 sender_id', 'code': 400, 'message': None}}, status_code=400)

            msg_content_type = request.query_params.get('content_type') or request.headers.get('X-Content-Type')
            if msg_content_type != 'audio':
                return JSONResponse({'data': {'message': 'content_type 必须为 audio', 'code': 400, 'message': None}}, status_code=400)

            audio_data = await request.body()
            if not audio_data:
                return JSONResponse({'data': {'message': '音频数据为空', 'code': 400, 'message': None}}, status_code=400)

            client_audio_type = request.headers.get('X-Audio-Content-Type') or content_type_header
            valid_types = ['audio/mpeg', 'audio/wav', 'audio/aac', 'audio/ogg', 'audio/mp4']
            if client_audio_type not in valid_types:
                return JSONResponse({'data': {'message': f'不支持的音频类型: {client_audio_type}', 'code': 400, 'message': None}}, status_code=400)

            school_id = request.query_params.get('school_id')
            class_id = request.query_params.get('class_id')
            sent_at_str = request.query_params.get('sent_at')
            sent_at = datetime.datetime.now()
            if sent_at_str:
                try:
                    sent_at = datetime.datetime.strptime(sent_at_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return JSONResponse({'data': {'message': 'sent_at 格式错误', 'code': 400}}, status_code=400)

            insert_query = """
                INSERT INTO ta_message 
                (sender_id, content_type, text_content, audio_data, school_id, class_id, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor = connection.cursor()
            cursor.execute(insert_query, (sender_id, 'audio', None, audio_data, school_id, class_id, sent_at))
            connection.commit()

            new_message_id = cursor.lastrowid
            audio_url = f"/api/audio/{new_message_id}"
            message_dict = {
                'id': new_message_id,
                'sender_id': sender_id,
                'content_type': 'audio',
                'text_content': None,
                'audio_url': audio_url,
                'school_id': school_id,
                'class_id': class_id,
                'sent_at': sent_at.strftime('%Y-%m-%d %H:%M:%S'),
                'created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            return safe_json_response({'data': {'message': '音频消息发送成功', 'code': 201, 'message': message_dict}}, status_code=201)

        else:
            return JSONResponse({'data': {'message': '仅支持 application/json 或 application/octet-stream', 'code': 400, 'message': None}}, status_code=400)

    except Exception as e:
        app_logger.error(f"Error in add_message: {e}")
        if connection and connection.is_connected():
            connection.rollback()
        return JSONResponse({'data': {'message': '服务器内部错误', 'code': 500, 'message': None}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close()


@app.get("/api/audio/{message_id}")
async def get_audio(message_id: int = Path(..., description="音频消息ID")):
    connection = get_db_connection()
    if not connection:
        return JSONResponse({'message': 'Database error'}, status_code=500)

    cursor = None
    try:
        query = "SELECT audio_data FROM ta_message WHERE id = %s AND content_type = 'audio'"
        cursor = connection.cursor()
        cursor.execute(query, (message_id,))
        result = cursor.fetchone()

        if not result or not result[0]:
            return JSONResponse({'message': 'Audio not found'}, status_code=404)

        audio_data = result[0]
        return safe_json_response(content=audio_data, media_type="audio/mpeg")  # 替代 Flask response_class
    except Exception as e:
        app_logger.error(f"Error serving audio: {e}")
        return JSONResponse({'message': 'Internal error'}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close()


@app.post("/notifications")
async def send_notification_to_class(request: Request):
    connection = get_db_connection()
    if connection is None:
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    cursor = None
    try:
        data = await request.json()
        sender_id = data.get('sender_id')
        class_id = data.get('class_id')
        content = data.get('content')

        if not all([sender_id, class_id, content]):
            return JSONResponse({'data': {'message': '缺少必需参数', 'code': 400}}, status_code=400)

        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)
        insert_query = "INSERT INTO ta_notification (sender_id, receiver_id, content) VALUES (%s, %s, %s)"
        cursor.execute(insert_query, (sender_id, class_id, content))
        notification_id = cursor.lastrowid

        select_query = """
            SELECT n.*, t.name AS sender_name, t.icon AS sender_icon
            FROM ta_notification n
            JOIN ta_teacher t ON n.sender_id = t.id
            WHERE n.id = %s
        """
        cursor.execute(select_query, (notification_id,))
        new_notification = cursor.fetchone()

        if not new_notification:
            connection.rollback()
            app_logger.error(f"Failed to retrieve notification {notification_id}")
            return JSONResponse({'data': {'message': '创建通知后查询失败', 'code': 500}}, status_code=500)

        new_notification = format_notification_time(new_notification)
        connection.commit()
        return safe_json_response({'data': {'message': '通知发送成功', 'code': 201, 'notification': new_notification}}, status_code=201)
    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error: {e}")
        return JSONResponse({'data': {'message': '发送通知失败', 'code': 500}}, status_code=500)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"Unexpected error: {e}")
        return JSONResponse({'data': {'message': '内部服务器错误', 'code': 500}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close()

from fastapi import Path

@app.get("/notifications/class/{class_id}")
async def get_notifications_for_class(
    class_id: int = Path(..., description="班级ID"),
    request: Request = None
):
    """
    获取指定班级的最新通知，并将这些通知标记为已读 (is_read=1)。
    - class_id (path参数): 班级ID
    - limit (query参数, 可选): 默认 20，最大 100
    """
    connection = get_db_connection()
    if connection is None:
        return JSONResponse({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'notifications': []
            }
        }, status_code=500)

    cursor = None
    try:
        # 获取 limit 参数并限制范围
        limit_param = request.query_params.get('limit')
        try:
            limit = int(limit_param) if limit_param else 20
        except ValueError:
            limit = 20
        limit = max(1, min(limit, 100))

        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # 1. 查询该班级未读通知，并关联老师表
        select_query = """
            SELECT n.*, t.name AS sender_name, t.icon AS sender_icon
            FROM ta_notification n
            JOIN ta_teacher t ON n.sender_id = t.id
            WHERE n.receiver_id = %s AND n.is_read = 0
            ORDER BY n.created_at DESC
            LIMIT %s
        """
        cursor.execute(select_query, (class_id, limit))
        notifications = cursor.fetchall()

        # 2. 批量标记为已读
        notification_ids = [notif['id'] for notif in notifications]
        if notification_ids:
            ids_placeholder = ','.join(['%s'] * len(notification_ids))
            update_query = f"""
                UPDATE ta_notification 
                SET is_read = 1, updated_at = CURRENT_TIMESTAMP 
                WHERE id IN ({ids_placeholder})
            """
            cursor.execute(update_query, tuple(notification_ids))
            app_logger.info(f"Marked {len(notification_ids)} notifications as read for class {class_id}.")
        else:
            app_logger.info(f"No unread notifications found for class {class_id}.")

        # 3. 格式化时间
        for i, notif in enumerate(notifications):
            notifications[i] = format_notification_time(notif)

        connection.commit()
        return safe_json_response({
            'data': {
                'message': '获取班级通知成功',
                'code': 200,
                'notifications': notifications
            }
        })
    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error fetching/reading notifications for class {class_id}: {e}")
        return JSONResponse({
            'data': {
                'message': '获取/标记通知失败',
                'code': 500,
                'notifications': []
            }
        }, status_code=500)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"Unexpected error fetching/reading notifications for class {class_id}: {e}")
        return JSONResponse({
            'data': {
                'message': '内部服务器错误',
                'code': 500,
                'notifications': []
            }
        }, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after fetching/reading notifications for class {class_id}.")


# --- 修改后的壁纸列表接口 ---
from fastapi import Request
from fastapi.responses import JSONResponse
import time, secrets

@app.get("/wallpapers")
async def list_wallpapers(request: Request):
    """
    获取所有壁纸列表 (支持筛选、排序)
    Query Parameters:
        - is_enabled (int, optional)
        - resolution (str, optional)
        - sort_by (str, optional)
        - order (str, optional)
    """
    connection = get_db_connection()
    if connection is None:
        app_logger.error("List wallpapers failed: Database connection error.")
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500, 'wallpapers': []}}, status_code=500)

    cursor = None
    try:
        # 1. 获取查询参数
        is_enabled_filter = request.query_params.get('is_enabled')
        resolution_filter = request.query_params.get('resolution')
        sort_by = request.query_params.get('sort_by', 'created_at')
        order = request.query_params.get('order', 'desc')

        # 转类型
        try:
            is_enabled_filter = int(is_enabled_filter) if is_enabled_filter is not None else None
        except ValueError:
            is_enabled_filter = None

        # 2. 验证排序参数
        valid_sort_fields = ['created_at', 'updated_at', 'id']
        valid_orders = ['asc', 'desc']
        if sort_by not in valid_sort_fields:
            sort_by = 'created_at'
        if order not in valid_orders:
            order = 'desc'

        # 3. 构建 SQL
        base_columns = "id, title, image_url, resolution, file_size, file_type, uploader_id, is_enabled, created_at, updated_at"
        base_query = f"SELECT {base_columns} FROM ta_wallpaper WHERE 1=1"
        filters, params = [], []

        if is_enabled_filter is not None:
            filters.append("AND is_enabled = %s")
            params.append(is_enabled_filter)
        if resolution_filter:
            filters.append("AND resolution = %s")
            params.append(resolution_filter)

        order_clause = f"ORDER BY {sort_by} {order}"
        final_query = base_query + " " + " ".join(filters) + " " + order_clause

        # 4. 执行
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        wallpapers = cursor.fetchall()

        app_logger.info(f"Fetched {len(wallpapers)} wallpapers.")
        return safe_json_response({'data': {'message': '获取壁纸列表成功', 'code': 200, 'wallpapers': wallpapers}})
    except Error as e:
        app_logger.error(f"Database error during fetching wallpapers: {e}")
        return JSONResponse({'data': {'message': '获取壁纸列表失败', 'code': 500, 'wallpapers': []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching wallpapers: {e}")
        return JSONResponse({'data': {'message': '内部服务器错误', 'code': 500, 'wallpapers': []}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): 
            connection.close()
            app_logger.info("Database connection closed after fetching wallpapers.")

@app.post("/send_verification_code")
async def send_verification_code(request: Request):
    """发送短信验证码接口"""
    # 存储验证码和发送时间
    data = await request.json()
    phone = data.get('phone')

    if not phone:
        app_logger.warning("Send verification code failed: Phone number is missing.")
        return JSONResponse({'data': {'message': '手机号不能为空', 'code': 400}}, status_code=400)

    code = generate_verification_code()

    # 用一个全局内存缓存（可以替代 Flask session）
    verification_memory[phone] = {  # 你可以在程序顶部定义： verification_memory = {}
        'code': code,
        'expires_at': time.time() + VERIFICATION_CODE_EXPIRY
    }

    if send_sms_verification_code(phone, code):
        app_logger.info(f"Verification code sent successfully to {phone}.")
        return JSONResponse({'data': {'message': '验证码已发送', 'code': 200}})
    else:
        verification_memory.pop(phone, None)
        app_logger.error(f"Failed to send verification code to {phone}.")
        return JSONResponse({'data': {'message': '验证码发送失败', 'code': 500}}, status_code=500)


@app.post("/register")
async def register(request: Request):
    data = await request.json()
    phone = data.get('phone')
    password = data.get('password')
    verification_code = data.get('verification_code')
    
    print(data);

    if not phone or not password or not verification_code:
        app_logger.warning("Registration failed: Missing phone, password, or verification code.")
        return JSONResponse({'data': {'message': '手机号、密码和验证码不能为空', 'code': 400}}, status_code=400)

    # 验证验证码
    valid_info = verification_memory.get(phone)
    if not valid_info:
        return JSONResponse({'data': {'message': '验证码已失效，请重新获取', 'code': 400}}, status_code=400)
    elif time.time() > valid_info['expires_at']:
        verification_memory.pop(phone, None)
        return JSONResponse({'data': {'message': '验证码已过期，请重新获取', 'code': 400}}, status_code=400)
    elif str(verification_code) != str(valid_info['code']):
        return JSONResponse({'data': {'message': '验证码错误', 'code': 400}}, status_code=400)
    else:
        verification_memory.pop(phone, None)

    salt = secrets.token_hex(16)
    password_hash = hash_password(password, salt)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Registration failed: Database connection error.")
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM ta_user WHERE phone = %s", (phone,))
        if cursor.fetchone():
            app_logger.info(f"Registration failed for {phone}: Phone number already registered.")
            cursor.close()
            return JSONResponse({'data': {'message': '手机号已注册', 'code': 400}}, status_code=400)

        insert_query = """
            INSERT INTO ta_user (phone, password_hash, salt, is_verified, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (phone, password_hash, salt, 1, None))
        connection.commit()
        user_id = cursor.lastrowid
        cursor.close()
        app_logger.info(f"User registered successfully: Phone {phone}, User ID {user_id}.")
        return safe_json_response({'data': {'message': '注册成功', 'code': 201, 'user_id': user_id}}, status_code=201)
    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error during registration for {phone}: {e}")
        return JSONResponse({'data': {'message': '注册失败', 'code': 500}}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after registration attempt.")

# 用于签名的密钥（实际项目中放到环境变量里）
#SECRET_KEY = "my_secret_key"
ALGORITHM = "HS256"

# 生成 JWT token
def create_access_token(data: dict, expires_delta: int = 30):
    to_encode = data.copy()
    expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=expires_delta)
    to_encode.update({"exp": expire})
    token = jwt.encode(to_encode, app.secret_key, algorithm=ALGORITHM)
    return token

# ======= 登录接口 =======
@app.post("/login")
async def login(request: Request):
    data = await request.json()
    login_type = data.get('login_type')
    
    print(f"[login] 收到登录请求，login_type={login_type}, data={data}")
    app_logger.info(f"[login] 收到登录请求，login_type={login_type}")
    
    # 班级端登录
    if login_type == "class":
        class_number = data.get('class_number')
        
        if not class_number:
            app_logger.warning("[login] 班级端登录失败：缺少班级唯一编号")
            return JSONResponse({'data': {'message': '班级唯一编号不能为空', 'code': 400}}, status_code=400)
        
        connection = get_db_connection()
        if connection is None:
            app_logger.error("[login] 数据库连接失败")
            return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)
        
        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)
            # 根据班级唯一编号查询班级信息（使用 ta_classes 表）
            cursor.execute("""
                SELECT class_code, class_name, school_stage, grade, schoolid, remark, created_at
                FROM ta_classes
                WHERE class_code = %s
            """, (class_number,))
            class_info = cursor.fetchone()
            
            if not class_info:
                app_logger.warning(f"[login] 班级端登录失败：班级 {class_number} 不存在")
                return JSONResponse({'data': {'message': '班级不存在', 'code': 404}}, status_code=404)
            
            # 使用班级编号作为 user_id（班级端登录）
            user_id = class_number
            
            app_logger.info(f"[login] 班级端登录成功 - class_number={class_number}, class_name={class_info.get('class_name')}, user_id={user_id}")
            
            # 生成 token（使用班级编号作为标识）
            token_data = {"sub": class_number, "type": "class"}
            access_token = create_access_token(token_data, expires_delta=60)  # 60分钟有效期
            
            return safe_json_response({
                'data': {
                    'message': '登录成功',
                    'code': 200,
                    'access_token': access_token,
                    'token_type': 'bearer',
                    'user_id': user_id,
                    'class_code': class_info.get('class_code'),
                    'class_name': class_info.get('class_name'),
                    'school_stage': class_info.get('school_stage'),
                    'grade': class_info.get('grade'),
                    'schoolid': class_info.get('schoolid')
                }
            }, status_code=200)
        except Exception as e:
            app_logger.error(f"[login] 班级端登录异常: {e}")
            return JSONResponse({'data': {'message': '登录失败', 'code': 500}}, status_code=500)
        finally:
            if cursor: cursor.close()
            if connection and connection.is_connected(): connection.close()
    
    # 普通用户登录（手机号+密码/验证码）
    phone = data.get('phone')
    password = data.get('password')
    verification_code = data.get('verification_code')
    
    print(data);

    if not phone or (not password and not verification_code):
        return JSONResponse({'data': {'message': '手机号和密码或验证码必须提供', 'code': 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        print(" 数据库连接失败\n")
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id, password_hash, salt, is_verified FROM ta_user WHERE phone = %s", (phone,))
        user = cursor.fetchone()

        if not user:
            return JSONResponse({'data': {'message': '用户不存在', 'code': 404}}, status_code=404)
        if not user['is_verified']:
            return JSONResponse({'data': {'message': '账户未验证', 'code': 403}}, status_code=403)

        print(" 111111 phone:", phone, "\n")
        auth_success = False
        if password:
            if hash_password(password, user['salt']) == user['password_hash']:
                auth_success = True
            else:
                print(hash_password(password, user['salt']));
                print(user['password_hash']);
                return JSONResponse({'data': {'message': '密码错误', 'code': 401}}, status_code=401)
        elif verification_code:
            is_valid, message = verify_code_from_memory(phone, verification_code)
            if is_valid:
                auth_success = True
            else:
                return JSONResponse({'data': {'message': message, 'code': 400}}, status_code=400)

        print(" 111111 auth_success:", auth_success, "\n")
        if auth_success:
            # 登录成功 -> 生成 token
            token_data = {"sub": phone}  # sub: subject，表示用户标识
            access_token = create_access_token(token_data, expires_delta=60)  # 60分钟有效期
            cursor.execute("UPDATE ta_user SET last_login_at = %s WHERE id = %s", (datetime.datetime.now(), user['id']))
            connection.commit()
            return safe_json_response({'data': {'message': '登录成功', 'code': 200, "access_token": access_token, "token_type": "bearer", 'user_id': user['id']}}, status_code=200)
    except Exception as e:
        app_logger.error(f"Database error during login: {e}")
        return JSONResponse({'data': {'message': '登录失败', 'code': 500}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close()


@app.get("/api/class/info")
async def get_class_info(request: Request):
    """获取班级信息接口（包含学校信息）"""
    class_code = request.query_params.get('class_code')
    
    # 提取 Authorization header（可选，用于日志记录）
    auth_header = request.headers.get('Authorization', '')
    token = auth_header.replace('Bearer ', '') if auth_header.startswith('Bearer ') else ''
    
    app_logger.info(f"[class/info] 收到请求 - class_code={class_code}, has_token={bool(token)}")
    
    if not class_code:
        app_logger.warning("[class/info] 缺少 class_code 参数")
        return JSONResponse({'data': {'message': '班级编号不能为空', 'code': 400}}, status_code=400)
    
    connection = get_db_connection()
    if connection is None:
        app_logger.error("[class/info] 数据库连接失败")
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)
    
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 1. 从 ta_classes 表查询班级信息
        cursor.execute("""
            SELECT class_code, class_name, school_stage, grade, schoolid, remark, created_at
            FROM ta_classes
            WHERE class_code = %s
        """, (class_code,))
        class_info = cursor.fetchone()
        
        if not class_info:
            app_logger.warning(f"[class/info] 班级 {class_code} 不存在")
            return JSONResponse({'data': {'message': '班级不存在', 'code': 404}}, status_code=404)
        
        schoolid = class_info.get('schoolid')
        
        # 2. 根据 schoolid 从 ta_school 表查询学校信息
        school_info = None
        if schoolid:
            cursor.execute("""
                SELECT id, name, address
                FROM ta_school
                WHERE id = %s
            """, (schoolid,))
            school_info = cursor.fetchone()
        
        # 3. 合并返回数据
        result = {
            'class_code': class_info.get('class_code'),
            'class_name': class_info.get('class_name'),
            'school_stage': class_info.get('school_stage'),
            'grade': class_info.get('grade'),
            'schoolid': schoolid,
            'remark': class_info.get('remark')
        }
        
        # 添加学校信息（如果存在）
        if school_info:
            result['school_name'] = school_info.get('name')
            result['address'] = school_info.get('address')
        else:
            result['school_name'] = None
            result['address'] = None
            if schoolid:
                app_logger.warning(f"[class/info] 学校 {schoolid} 不存在")
        
        app_logger.info(f"[class/info] 查询成功 - class_code={class_code}, schoolid={schoolid}, school_name={result.get('school_name')}")
        
        return safe_json_response({
            'data': {
                'message': '获取班级信息成功',
                'code': 200,
                **result
            }
        }, status_code=200)
        
    except Exception as e:
        app_logger.error(f"[class/info] 查询异常: {e}")
        return JSONResponse({'data': {'message': '获取班级信息失败', 'code': 500}}, status_code=500)
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close()


from fastapi import Request
from fastapi.responses import JSONResponse
import secrets

@app.post("/verify_and_set_password")
async def verify_and_set_password(request: Request):
    """忘记密码 - 验证并重置密码"""
    data = await request.json()
    phone = data.get('phone')
    verification_code = data.get('verification_code')
    new_password = data.get('new_password')

    if not phone or not verification_code or not new_password:
        app_logger.warning("Password reset failed: Missing phone, verification code, or new password.")
        return JSONResponse({
            'data': {
                'message': '手机号、验证码和新密码不能为空',
                'code': 400
            }
        }, status_code=400)

    # 统一验证码校验方式
    is_valid, message = verify_code_from_memory(phone, verification_code)
    if not is_valid:
        app_logger.warning(f"Password reset failed for {phone}: {message}")
        return JSONResponse({
            'data': {
                'message': message,
                'code': 400
            }
        }, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Password reset failed: Database connection error.")
        return JSONResponse({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM ta_user WHERE phone = %s AND is_verified = 1", (phone,))
        user = cursor.fetchone()

        if not user:
            app_logger.info(f"Password reset failed for {phone}: User not found or not verified.")
            return JSONResponse({
                'data': {
                    'message': '用户不存在或账户未验证',
                    'code': 400
                }
            }, status_code=400)

        new_salt = secrets.token_hex(16)
        new_password_hash = hash_password(new_password, new_salt)

        update_query = """
            UPDATE ta_user
            SET password_hash = %s, salt = %s
            WHERE id = %s
        """
        cursor.execute(update_query, (new_password_hash, new_salt, user[0]))
        connection.commit()

        if cursor.rowcount == 0:
            app_logger.error(f"Password reset failed for user ID {user[0]}: Update query affected 0 rows.")
            return JSONResponse({
                'data': {
                    'message': '更新失败',
                    'code': 500
                }
            }, status_code=500)

        app_logger.info(f"Password reset successful for user ID {user[0]}.")
        return safe_json_response({
            'data': {
                'message': '密码重置成功',
                'code': 200
            }
        }, status_code=200)

    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error during password reset for {phone}: {e}")
        return JSONResponse({
            'data': {
                'message': '密码重置失败',
                'code': 500
            }
        }, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after password reset attempt for {phone}.")

BASE_PATH = '/data/nginx/html/icons'
os.makedirs(BASE_PATH, exist_ok=True)

@app.post("/upload_icon")
async def upload_icon(
    teacher_id: str = Form(...),     # 唯一教师编号
    file: UploadFile = File(...)     # 图标文件
):
    # 1. 创建教师目录
    teacher_dir = os.path.join(BASE_PATH, teacher_id)
    os.makedirs(teacher_dir, exist_ok=True)

    # 2. 保存文件
    save_path = os.path.join(teacher_dir, file.filename)
    with open(save_path, "wb") as f:
        f.write(await file.read())

    # 3. 返回结果
    url_path = f"/icons/{teacher_id}/{file.filename}"
    return JSONResponse({
        "status": "ok",
        "message": "Upload success",
        "url": url_path
    })

@app.get("/groups")
def get_groups_by_admin(group_admin_id: str = Query(..., description="群管理员的唯一ID"),nickname_keyword: str = Query(None, description="群名关键词（支持模糊查询）")):
    """
    根据群管理员ID查询ta_group表，可选群名关键词模糊匹配
    """
    # 参数校验
    if not group_admin_id:
        return JSONResponse({
            "data": {
                "message": "缺少群管理员ID",
                "code": 400
            }
        }, status_code=400)

    # 数据库连接
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({
            "data": {
                "message": "数据库连接失败",
                "code": 500
            }
        }, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)

        # 判断是否要加模糊查询
        if nickname_keyword:
            sql = """
                SELECT * FROM ta_group
                WHERE group_admin_id=%s AND nickname LIKE %s
            """
            cursor.execute(sql, (group_admin_id, f"%{nickname_keyword}%"))
        else:
            sql = "SELECT * FROM ta_group WHERE group_admin_id=%s"
            cursor.execute(sql, (group_admin_id,))

        groups = cursor.fetchall()
        for group in groups:
            avatar_path = group.get("headImage_path")
            local_avatar_file = resolve_local_avatar_file_path(avatar_path)
            if local_avatar_file and os.path.exists(local_avatar_file):
                try:
                    with open(local_avatar_file, "rb") as img:
                        group["avatar_base64"] = base64.b64encode(img.read()).decode("utf-8")
                except Exception as e:
                    app_logger.error(f"读取图片失败 {local_avatar_file}: {e}")
                    group["avatar_base64"] = None
            else:
                group["avatar_base64"] = None

         # 转换所有的 datetime 成字符串
        for row in groups:
            for key in row:
                if isinstance(row[key], datetime.datetime):
                    row[key] = row[key].strftime("%Y-%m-%d %H:%M:%S")

        return JSONResponse({
            "data": {
                "message": "查询成功",
                "code": 200,
                "groups": groups
            }
        }, status_code=200)

    except mysql.connector.Error as e:
        print(f"查询错误: {e}")
        return JSONResponse({
            "data": {
                "message": "查询失败",
                "code": 500
            }
        }, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after get_groups_by_admin attempt for {group_admin_id}.")

@app.get("/member/groups")
def get_member_groups(
    unique_member_id: str = Query(..., description="成员唯一ID")
):
    """
    根据 unique_member_id 查询该成员所在的群列表 (JOIN ta_group)
    """
    if not unique_member_id:
        return JSONResponse({
            "data": {
                "message": "缺少成员唯一ID",
                "code": 400
            }
        }, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({
            "data": {
                "message": "数据库连接失败",
                "code": 500
            }
        }, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)
        sql = """
            SELECT g.*
            FROM ta_group_member_relation m
            INNER JOIN ta_group g ON m.unique_group_id = g.unique_group_id
            WHERE m.unique_member_id = %s
        """
        cursor.execute(sql, (unique_member_id,))
        groups = cursor.fetchall()

        for group in groups:
            avatar_path = group.get("headImage_path")
            local_avatar_file = resolve_local_avatar_file_path(avatar_path)
            if local_avatar_file and os.path.exists(local_avatar_file):
                try:
                    with open(local_avatar_file, "rb") as img:
                        group["avatar_base64"] = base64.b64encode(img.read()).decode("utf-8")
                except Exception as e:
                    app_logger.error(f"读取图片失败 {local_avatar_file}: {e}")
                    group["avatar_base64"] = None
            else:
                group["avatar_base64"] = None

        # 转换 datetime 防止 JSON 报错
        for row in groups:
            for key, value in row.items():
                if isinstance(value, datetime.datetime):
                    row[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        return JSONResponse({
            "data": {
                "message": "查询成功",
                "code": 200,
                "joingroups": groups
            }
        }, status_code=200)

    except mysql.connector.Error as e:
        print(f"查询错误: {e}")
        return JSONResponse({
            "data": {
                "message": "查询失败",
                "code": 500
            }
        }, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after get_member_groups attempt for {unique_member_id}.")

@app.get("/groups/by-teacher")
def get_groups_by_teacher(
    teacher_unique_id: str = Query(..., description="教师唯一ID，对应group_members表的user_id")
):
    """
    根据 teacher_unique_id 查询该教师所在的群组，按角色分组返回
    - 是群主的群组（self_role = 400）
    - 不是群主的群组（self_role != 400）
    """
    if not teacher_unique_id:
        return JSONResponse({
            "data": {
                "message": "缺少教师唯一ID",
                "code": 400
            }
        }, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({
            "data": {
                "message": "数据库连接失败",
                "code": 500
            }
        }, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 查询该教师所在的群组及成员信息
        sql = """
            SELECT 
                g.*,
                gm.user_id,
                gm.user_name,
                gm.self_role,
                gm.join_time as member_join_time,
                gm.msg_flag,
                gm.self_msg_flag,
                gm.readed_seq,
                gm.unread_num
            FROM `group_members` gm
            INNER JOIN `groups` g ON gm.group_id = g.group_id
            WHERE gm.user_id = %s
            ORDER BY g.create_time DESC
        """
        cursor.execute(sql, (teacher_unique_id,))
        results = cursor.fetchall()
        
        # 转换 datetime 为字符串
        for row in results:
            for key, value in row.items():
                if isinstance(value, datetime.datetime):
                    row[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        
        # 按角色分组：self_role = 400 表示群主
        owner_groups = []  # 是群主的群组
        member_groups = []  # 不是群主的群组
        
        for row in results:
            # 构建群组信息（包含成员信息）
            group_id = row.get("group_id")
            group_info = {
                "group_id": group_id,
                "group_name": row.get("group_name"),
                "group_type": row.get("group_type"),
                "face_url": row.get("face_url"),
                "detail_face_url": row.get("detail_face_url"),
                "owner_identifier": row.get("owner_identifier"),
                "create_time": row.get("create_time"),
                "max_member_num": row.get("max_member_num"),
                "member_num": row.get("member_num"),
                "introduction": row.get("introduction"),
                "notification": row.get("notification"),
                "searchable": row.get("searchable"),
                "visible": row.get("visible"),
                "add_option": row.get("add_option"),
                "is_shutup_all": row.get("is_shutup_all"),
                "next_msg_seq": row.get("next_msg_seq"),
                "latest_seq": row.get("latest_seq"),
                "last_msg_time": row.get("last_msg_time"),
                "last_info_time": row.get("last_info_time"),
                "info_seq": row.get("info_seq"),
                "detail_info_seq": row.get("detail_info_seq"),
                "detail_group_id": row.get("detail_group_id"),
                "detail_group_name": row.get("detail_group_name"),
                "detail_group_type": row.get("detail_group_type"),
                "detail_is_shutup_all": row.get("detail_is_shutup_all"),
                "online_member_num": row.get("online_member_num"),
                "classid": row.get("classid"),
                "schoolid": row.get("schoolid"),
                "is_class_group": row.get("is_class_group"),
                # 成员信息
                "member_info": {
                    "user_id": row.get("user_id"),
                    "user_name": row.get("user_name"),
                    "self_role": row.get("self_role"),
                    "join_time": row.get("member_join_time"),
                    "msg_flag": row.get("msg_flag"),
                    "self_msg_flag": row.get("self_msg_flag"),
                    "readed_seq": row.get("readed_seq"),
                    "unread_num": row.get("unread_num")
                }
            }
            
            # 检查该群组是否有临时语音房间（先从内存查找，如果没有则从数据库恢复）
            temp_room_info = None
            if group_id:
                # 优先从内存中查找
                if group_id in active_temp_rooms:
                    room_info = active_temp_rooms[group_id]
                    temp_room_info = {
                        "room_id": room_info.get("room_id"),
                        "publish_url": room_info.get("publish_url"),  # 推流地址（传统 WebRTC API）
                        "play_url": room_info.get("play_url"),  # 拉流地址（传统 WebRTC API）
                        "stream_name": room_info.get("stream_name"),
                        "owner_id": room_info.get("owner_id"),
                        "owner_name": room_info.get("owner_name"),
                        "owner_icon": room_info.get("owner_icon"),
                        "members": room_info.get("members", [])
                    }
                    app_logger.info(f"[groups/by-teacher] 群组 {group_id} 有临时语音房间（内存），已添加到返回信息")
                else:
                    # 内存中没有，从数据库查询
                    try:
                        room_query = """
                            SELECT room_id, group_id, owner_id, owner_name, owner_icon,
                                   whip_url, whep_url, stream_name, status, create_time
                            FROM temp_voice_rooms
                            WHERE group_id = %s AND status = 1
                            ORDER BY create_time DESC
                            LIMIT 1
                        """
                        cursor.execute(room_query, (group_id,))
                        room_row = cursor.fetchone()
                        
                        if room_row:
                            stream_name = room_row.get("stream_name")
                            # 从 stream_name 重新生成传统 WebRTC API 地址
                            publish_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/publish/?app={SRS_APP}&stream={stream_name}"
                            play_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/play/?app={SRS_APP}&stream={stream_name}"
                            
                            # 查询房间成员
                            members_query = """
                                SELECT user_id, user_name, status
                                FROM temp_voice_room_members
                                WHERE room_id = %s AND status = 1
                            """
                            cursor.execute(members_query, (room_row.get("room_id"),))
                            member_rows = cursor.fetchall()
                            members = [m.get("user_id") for m in member_rows if m.get("user_id")]
                            
                            temp_room_info = {
                                "room_id": room_row.get("room_id"),
                                "publish_url": publish_url,  # 推流地址（传统 WebRTC API）
                                "play_url": play_url,  # 拉流地址（传统 WebRTC API）
                                "stream_name": stream_name,
                                "owner_id": room_row.get("owner_id"),
                                "owner_name": room_row.get("owner_name"),
                                "owner_icon": room_row.get("owner_icon"),
                                "members": members
                            }
                            
                            # 将房间信息恢复到内存中（可选，用于后续快速访问）
                            active_temp_rooms[group_id] = {
                                "room_id": room_row.get("room_id"),
                                "publish_url": publish_url,
                                "play_url": play_url,
                                "whip_url": room_row.get("whip_url"),
                                "whep_url": room_row.get("whep_url"),
                                "stream_name": stream_name,
                                "owner_id": room_row.get("owner_id"),
                                "owner_name": room_row.get("owner_name"),
                                "owner_icon": room_row.get("owner_icon"),
                                "group_id": group_id,
                                "timestamp": time.time(),
                                "members": members
                            }
                            
                            app_logger.info(f"[groups/by-teacher] 群组 {group_id} 有临时语音房间（数据库恢复），已添加到返回信息并恢复到内存")
                    except Exception as db_error:
                        app_logger.error(f"[groups/by-teacher] 从数据库查询临时语音房间失败 - group_id={group_id}, error={db_error}")
                        # 数据库查询失败不影响主流程，继续处理
                
                if temp_room_info:
                    group_info["temp_room"] = temp_room_info
            
            # 判断是否是群主：self_role = 400 表示群主
            if row.get("self_role") == 400:
                owner_groups.append(group_info)
            else:
                member_groups.append(group_info)
        
        return JSONResponse({
            "data": {
                "message": "查询成功",
                "code": 200,
                "owner_groups": owner_groups,  # 是群主的群组
                "member_groups": member_groups,  # 不是群主的群组
                "total_count": len(results),
                "owner_count": len(owner_groups),
                "member_count": len(member_groups)
            }
        }, status_code=200)

    except mysql.connector.Error as e:
        app_logger.error(f"查询群组错误: {e}")
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500
            }
        }, status_code=500)
    except Exception as e:
        app_logger.error(f"查询群组时发生异常: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        app_logger.error(traceback_str)
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500
            }
        }, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after get_groups_by_teacher attempt for {teacher_unique_id}.")

@app.get("/groups/search")
def search_groups(
    schoolid: str = Query(..., description="学校ID，必需参数"),
    group_id: str = Query(None, description="群组ID，与group_name二选一"),
    group_name: str = Query(None, description="群组名称，与group_id二选一，支持模糊查询")
):
    """
    搜索群组
    根据 schoolid 和 group_id 或 group_name 搜索 groups 表
    - schoolid: 必需参数
    - group_id 或 group_name: 二选一，不会同时上传
    """
    print("=" * 80)
    print("[groups/search] 收到搜索群组请求")
    print(f"[groups/search] 请求参数 - schoolid: {schoolid}, group_id: {group_id}, group_name: {group_name}")
    
    # 参数验证
    if not schoolid:
        print("[groups/search] 错误: 缺少必需参数 schoolid")
        return JSONResponse({
            "data": {
                "message": "缺少必需参数 schoolid",
                "code": 400
            }
        }, status_code=400)
    
    # group_id 和 group_name 必须至少提供一个
    if not group_id and not group_name:
        print("[groups/search] 错误: group_id 和 group_name 必须至少提供一个")
        return JSONResponse({
            "data": {
                "message": "group_id 和 group_name 必须至少提供一个",
                "code": 400
            }
        }, status_code=400)
    
    # group_id 和 group_name 不能同时提供
    if group_id and group_name:
        print("[groups/search] 错误: group_id 和 group_name 不能同时提供")
        return JSONResponse({
            "data": {
                "message": "group_id 和 group_name 不能同时提供",
                "code": 400
            }
        }, status_code=400)
    
    print("[groups/search] 开始连接数据库...")
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        print("[groups/search] 错误: 数据库连接失败")
        app_logger.error(f"[groups/search] 数据库连接失败 for schoolid={schoolid}")
        return JSONResponse({
            "data": {
                "message": "数据库连接失败",
                "code": 500
            }
        }, status_code=500)
    print("[groups/search] 数据库连接成功")

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 构建查询条件
        if group_id:
            # 根据 group_id 精确查询
            print(f"[groups/search] 根据 group_id 精确查询: {group_id}")
            sql = """
                SELECT *
                FROM `groups`
                WHERE schoolid = %s AND group_id = %s
            """
            params = (schoolid, group_id)
        else:
            # 根据 group_name 模糊查询
            print(f"[groups/search] 根据 group_name 模糊查询: {group_name}")
            sql = """
                SELECT *
                FROM `groups`
                WHERE schoolid = %s AND group_name LIKE %s
            """
            params = (schoolid, f"%{group_name}%")
        
        print(f"[groups/search] 执行SQL查询: {sql}")
        print(f"[groups/search] 查询参数: {params}")
        
        cursor.execute(sql, params)
        groups = cursor.fetchall()
        
        print(f"[groups/search] 查询结果: 找到 {len(groups)} 个群组")
        
        # 转换 datetime 为字符串
        for idx, group in enumerate(groups):
            print(f"[groups/search] 处理第 {idx+1} 个群组: group_id={group.get('group_id')}, group_name={group.get('group_name')}")
            for key, value in group.items():
                if isinstance(value, datetime.datetime):
                    group[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        
        result = {
            "data": {
                "message": "查询成功",
                "code": 200,
                "schoolid": schoolid,
                "search_key": group_id if group_id else group_name,
                "search_type": "group_id" if group_id else "group_name",
                "groups": groups,
                "count": len(groups)
            }
        }
        
        print(result)
        print(f"[groups/search] 返回结果: 找到 {len(groups)} 个群组")
        print("=" * 80)
        
        return JSONResponse(result, status_code=200)

    except mysql.connector.Error as e:
        error_msg = f"搜索群组错误: {e}"
        print(f"[groups/search] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/search] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/search] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500
            }
        }, status_code=500)
    except Exception as e:
        error_msg = f"搜索群组时发生异常: {e}"
        print(f"[groups/search] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/search] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/search] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500
            }
        }, status_code=500)
    finally:
        if cursor:
            cursor.close()
            print("[groups/search] 游标已关闭")
        if connection and connection.is_connected():
            connection.close()
            print("[groups/search] 数据库连接已关闭")
            app_logger.info(f"[groups/search] Database connection closed after search groups attempt for schoolid={schoolid}.")

@app.get("/teachers/search")
def search_teachers(
    schoolid: str = Query(..., description="学校ID，必需参数"),
    teacher_id: str = Query(None, description="老师ID，与teacher_unique_id和name三选一"),
    teacher_unique_id: str = Query(None, description="老师唯一ID，与teacher_id和name三选一"),
    name: str = Query(None, description="老师姓名，与teacher_id和teacher_unique_id三选一，支持模糊查询")
):
    """
    搜索同一学校的老师
    根据 schoolid 和 teacher_id 或 teacher_unique_id 或 name 搜索 ta_teacher 表
    - schoolid: 必需参数
    - teacher_id、teacher_unique_id、name: 三选一，不会同时上传
    """
    print("=" * 80)
    print("[teachers/search] 收到搜索老师请求")
    print(f"[teachers/search] 请求参数 - schoolid: {schoolid}, teacher_id: {teacher_id}, teacher_unique_id: {teacher_unique_id}, name: {name}")
    
    # 参数验证
    if not schoolid:
        print("[teachers/search] 错误: 缺少必需参数 schoolid")
        return JSONResponse({
            "data": {
                "message": "缺少必需参数 schoolid",
                "code": 400
            }
        }, status_code=400)
    
    # teacher_id、teacher_unique_id 和 name 必须至少提供一个
    search_params_count = sum([bool(teacher_id), bool(teacher_unique_id), bool(name)])
    if search_params_count == 0:
        print("[teachers/search] 错误: teacher_id、teacher_unique_id 和 name 必须至少提供一个")
        return JSONResponse({
            "data": {
                "message": "teacher_id、teacher_unique_id 和 name 必须至少提供一个",
                "code": 400
            }
        }, status_code=400)
    
    # 不能同时提供多个搜索参数
    if search_params_count > 1:
        print("[teachers/search] 错误: teacher_id、teacher_unique_id 和 name 不能同时提供")
        return JSONResponse({
            "data": {
                "message": "teacher_id、teacher_unique_id 和 name 不能同时提供",
                "code": 400
            }
        }, status_code=400)
    
    print("[teachers/search] 开始连接数据库...")
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        print("[teachers/search] 错误: 数据库连接失败")
        app_logger.error(f"[teachers/search] 数据库连接失败 for schoolid={schoolid}")
        return JSONResponse({
            "data": {
                "message": "数据库连接失败",
                "code": 500
            }
        }, status_code=500)
    print("[teachers/search] 数据库连接成功")

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 构建查询条件
        if teacher_id:
            # 根据 teacher_id 精确查询
            print(f"[teachers/search] 根据 teacher_id 精确查询: {teacher_id}")
            sql = """
                SELECT *
                FROM `ta_teacher`
                WHERE schoolId = %s AND id = %s
            """
            params = (schoolid, teacher_id)
            search_key = teacher_id
            search_type = "teacher_id"
        elif teacher_unique_id:
            # 根据 teacher_unique_id 精确查询
            print(f"[teachers/search] 根据 teacher_unique_id 精确查询: {teacher_unique_id}")
            sql = """
                SELECT *
                FROM `ta_teacher`
                WHERE schoolId = %s AND teacher_unique_id = %s
            """
            params = (schoolid, teacher_unique_id)
            search_key = teacher_unique_id
            search_type = "teacher_unique_id"
        else:
            # 根据 name 模糊查询
            print(f"[teachers/search] 根据 name 模糊查询: {name}")
            sql = """
                SELECT *
                FROM `ta_teacher`
                WHERE schoolId = %s AND name LIKE %s
            """
            params = (schoolid, f"%{name}%")
            search_key = name
            search_type = "name"
        
        print(f"[teachers/search] 执行SQL查询: {sql}")
        print(f"[teachers/search] 查询参数: {params}")
        
        cursor.execute(sql, params)
        teachers = cursor.fetchall()
        
        print(f"[teachers/search] 查询结果: 找到 {len(teachers)} 个老师")
        
        # 转换 datetime 为字符串
        for idx, teacher in enumerate(teachers):
            print(f"[teachers/search] 处理第 {idx+1} 个老师: id={teacher.get('id')}, name={teacher.get('name')}, teacher_unique_id={teacher.get('teacher_unique_id')}")
            for key, value in teacher.items():
                if isinstance(value, datetime.datetime):
                    teacher[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[teachers/search]   转换时间字段 {key}: {teacher[key]}")
        
        result = {
            "data": {
                "message": "查询成功",
                "code": 200,
                "schoolid": schoolid,
                "search_key": search_key,
                "search_type": search_type,
                "teachers": teachers,
                "count": len(teachers)
            }
        }
        
        print(f"[teachers/search] 返回结果: 找到 {len(teachers)} 个老师")
        print("=" * 80)
        
        return JSONResponse(result, status_code=200)

    except mysql.connector.Error as e:
        error_msg = f"搜索老师错误: {e}"
        print(f"[teachers/search] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[teachers/search] 错误堆栈: {traceback_str}")
        app_logger.error(f"[teachers/search] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500
            }
        }, status_code=500)
    except Exception as e:
        error_msg = f"搜索老师时发生异常: {e}"
        print(f"[teachers/search] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[teachers/search] 错误堆栈: {traceback_str}")
        app_logger.error(f"[teachers/search] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500
            }
        }, status_code=500)
    finally:
        if cursor:
            cursor.close()
            print("[teachers/search] 游标已关闭")
        if connection and connection.is_connected():
            connection.close()
            print("[teachers/search] 数据库连接已关闭")
            app_logger.info(f"[teachers/search] Database connection closed after search teachers attempt for schoolid={schoolid}.")

@app.post("/groups/join")
async def join_group(request: Request):
    """
    用户申请加入群组
    接收客户端发送的 group_id, user_id, user_name, reason
    将用户添加到 group_members 表中
    """
    print("=" * 80)
    print("[groups/join] 收到加入群组请求")
    
    try:
        data = await request.json()
        print(f"[groups/join] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        group_id = data.get('group_id')
        user_id = data.get('user_id')
        user_name = data.get('user_name')
        reason = data.get('reason')
        
        print(f"[groups/join] 解析结果 - group_id: {group_id}, user_id: {user_id}, user_name: {user_name}, reason: {reason}")
        
        # 参数验证
        if not group_id:
            print("[groups/join] 错误: 缺少 group_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 group_id"
            }, status_code=400)
        
        if not user_id:
            print("[groups/join] 错误: 缺少 user_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 user_id"
            }, status_code=400)
        
        print("[groups/join] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/join] 错误: 数据库连接失败")
            app_logger.error("[groups/join] 数据库连接失败")
            return JSONResponse({
                "code": 500,
                "message": "数据库连接失败"
            }, status_code=500)
        print("[groups/join] 数据库连接成功")
        
        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 1. 检查群组是否存在
            print(f"[groups/join] 检查群组 {group_id} 是否存在...")
            cursor.execute("SELECT group_id, group_name, max_member_num, member_num FROM `groups` WHERE group_id = %s", (group_id,))
            group_info = cursor.fetchone()
            
            if not group_info:
                print(f"[groups/join] 错误: 群组 {group_id} 不存在")
                return JSONResponse({
                    "code": 404,
                    "message": "群组不存在"
                }, status_code=404)
            
            print(f"[groups/join] 群组信息: {group_info}")
            max_member_num = group_info.get('max_member_num') if group_info.get('max_member_num') else 0
            member_num = group_info.get('member_num') if group_info.get('member_num') else 0
            
            # 检查群组是否已满
            if max_member_num > 0 and member_num >= max_member_num:
                print(f"[groups/join] 错误: 群组已满 (当前: {member_num}/{max_member_num})")
                return JSONResponse({
                    "code": 400,
                    "message": "群组已满，无法加入"
                }, status_code=400)
            
            # 2. 检查用户是否已经在群组中
            print(f"[groups/join] 检查用户 {user_id} 是否已在群组 {group_id} 中...")
            cursor.execute(
                "SELECT group_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id)
            )
            member_exists = cursor.fetchone()
            
            if member_exists:
                print(f"[groups/join] 用户 {user_id} 已在群组 {group_id} 中")
                return JSONResponse({
                    "code": 400,
                    "message": "您已经在该群组中"
                }, status_code=400)
            
            # 3. 插入新成员（默认角色为普通成员，不是群主）
            print(f"[groups/join] 插入新成员到群组 {group_id}...")
            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            insert_member_sql = """
                INSERT INTO `group_members` (
                    group_id, user_id, user_name, self_role, join_time, msg_flag,
                    self_msg_flag, readed_seq, unread_num
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            # self_role: 200 表示普通成员，400 表示群主
            insert_params = (
                group_id,
                user_id,
                user_name if user_name else None,  # 如果为空则插入 NULL
                200,  # 默认角色为普通成员
                current_time,
                0,  # msg_flag
                0,  # self_msg_flag
                0,  # readed_seq
                0   # unread_num
            )
            
            print(f"[groups/join] 插入参数: {insert_params}")
            cursor.execute(insert_member_sql, insert_params)
            affected_rows = cursor.rowcount
            lastrowid = cursor.lastrowid
            print(f"[groups/join] 插入成员完成, 影响行数: {affected_rows}, lastrowid: {lastrowid}")
            
            # 4. 更新群组的成员数量
            print(f"[groups/join] 更新群组 {group_id} 的成员数量...")
            cursor.execute(
                "UPDATE `groups` SET member_num = member_num + 1 WHERE group_id = %s",
                (group_id,)
            )
            print(f"[groups/join] 群组成员数量已更新")
            
            # 提交事务
            connection.commit()
            print(f"[groups/join] 事务提交成功")
            
            result = {
                "code": 200,
                "message": "成功加入群组",
                "data": {
                    "group_id": group_id,
                    "user_id": user_id,
                    "user_name": user_name,
                    "join_time": current_time
                }
            }
            
            print(f"[groups/join] 返回结果: {result}")
            print("=" * 80)
            
            return JSONResponse(result, status_code=200)
            
        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/join] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/join] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/join] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"数据库操作失败: {str(e)}"
            }, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"加入群组时发生异常: {e}"
            print(f"[groups/join] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/join] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/join] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"操作失败: {str(e)}"
            }, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/join] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/join] 数据库连接已关闭")
                app_logger.info("[groups/join] Database connection closed after join group attempt.")
    
    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/join] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/join] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/join] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "code": 400,
            "message": "请求数据格式错误"
        }, status_code=400)
    finally:
        print("=" * 80)

@app.post("/groups/invite")
async def invite_group_members(request: Request):
    """
    群主邀请成员加入群组
    接收客户端发送的 group_id 和 members 列表
    1. 调用腾讯接口邀请成员
    2. 邀请成功后，将相关信息插入数据库
    请求体 JSON:
    {
      "group_id": "群组ID",
      "members": [
        {
          "unique_member_id": "成员ID",
          "member_name": "成员名称",
          "group_role": 300
        }
      ]
    }
    """
    print("=" * 80)
    print("[groups/invite] 收到邀请成员请求")
    
    try:
        data = await request.json()
        print(f"[groups/invite] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        group_id = data.get('group_id')
        members = data.get('members', [])
        
        # 参数验证
        if not group_id:
            print("[groups/invite] 错误: 缺少 group_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 group_id"
            }, status_code=400)
        
        if not members or not isinstance(members, list):
            print("[groups/invite] 错误: 缺少或无效的 members")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 members 或 members 必须是数组"
            }, status_code=400)
        
        # 验证每个成员的必要字段
        for idx, member in enumerate(members):
            if not member.get('unique_member_id'):
                print(f"[groups/invite] 错误: 成员 {idx} 缺少 unique_member_id")
                return JSONResponse({
                    "code": 400,
                    "message": f"成员 {idx} 缺少必需参数 unique_member_id"
                }, status_code=400)
        
        print("[groups/invite] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/invite] 错误: 数据库连接失败")
            app_logger.error("[groups/invite] 数据库连接失败")
            return JSONResponse({
                "code": 500,
                "message": "数据库连接失败"
            }, status_code=500)
        print("[groups/invite] 数据库连接成功")
        
        cursor = None
        try:
            # 开始事务（在开始时就启动，确保所有操作在一个事务中）
            connection.start_transaction()
            cursor = connection.cursor(dictionary=True)
            
            # 1. 检查群组是否存在
            print(f"[groups/invite] 检查群组 {group_id} 是否存在...")
            cursor.execute(
                "SELECT group_id, group_name, max_member_num, member_num FROM `groups` WHERE group_id = %s",
                (group_id,)
            )
            group_info = cursor.fetchone()
            
            if not group_info:
                print(f"[groups/invite] 错误: 群组 {group_id} 不存在")
                return JSONResponse({
                    "code": 404,
                    "message": "群组不存在"
                }, status_code=404)
            
            print(f"[groups/invite] 群组信息: {group_info}")
            max_member_num = group_info.get('max_member_num') if group_info.get('max_member_num') else 0
            member_num = group_info.get('member_num') if group_info.get('member_num') else 0
            
            # 检查群组是否已满
            if max_member_num > 0 and member_num + len(members) > max_member_num:
                print(f"[groups/invite] 错误: 群组已满 (当前: {member_num}, 最大: {max_member_num}, 邀请: {len(members)})")
                return JSONResponse({
                    "code": 400,
                    "message": f"群组已满，无法邀请 {len(members)} 个成员（当前: {member_num}/{max_member_num}）"
                }, status_code=400)
            
            # 2. 检查哪些成员已经在群组中
            existing_members = []
            for member in members:
                unique_member_id = member.get('unique_member_id')
                cursor.execute(
                    "SELECT user_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                    (group_id, unique_member_id)
                )
                if cursor.fetchone():
                    existing_members.append(unique_member_id)
            
            if existing_members:
                print(f"[groups/invite] 警告: 以下成员已在群组中: {existing_members}")
                # 可以选择跳过已存在的成员，或者返回错误
                # 这里选择跳过已存在的成员，只邀请新成员
                members = [m for m in members if m.get('unique_member_id') not in existing_members]
                if not members:
                    return JSONResponse({
                        "code": 400,
                        "message": "所有成员已在群组中"
                    }, status_code=400)
            
            # 3. 调用腾讯接口邀请成员
            print(f"[groups/invite] 准备调用腾讯接口邀请 {len(members)} 个成员...")
            
            # 使用管理员账号作为 identifier（与群组同步保持一致）
            identifier_to_use = TENCENT_API_IDENTIFIER
            
            # 检查必需的配置
            if not TENCENT_API_SDK_APP_ID:
                print("[groups/invite] 错误: TENCENT_API_SDK_APP_ID 未配置")
                app_logger.error("[groups/invite] TENCENT_API_SDK_APP_ID 未配置")
                return JSONResponse({
                    "code": 500,
                    "message": "腾讯接口配置错误: 缺少 SDKAppID"
                }, status_code=500)
            
            if not identifier_to_use:
                print("[groups/invite] 错误: TENCENT_API_IDENTIFIER 未配置")
                app_logger.error("[groups/invite] TENCENT_API_IDENTIFIER 未配置")
                return JSONResponse({
                    "code": 500,
                    "message": "腾讯接口配置错误: 缺少 Identifier"
                }, status_code=500)
            
            # 尝试生成或使用配置的 UserSig（与群组同步逻辑一致）
            usersig_to_use: Optional[str] = None
            sig_error: Optional[str] = None
            if TENCENT_API_SECRET_KEY:
                try:
                    # 为管理员账号生成 UserSig
                    print(f"[groups/invite] 准备为管理员账号生成 UserSig: identifier={identifier_to_use}")
                    usersig_to_use = generate_tencent_user_sig(identifier_to_use)
                    print(f"[groups/invite] UserSig 生成成功，长度: {len(usersig_to_use) if usersig_to_use else 0}")
                    app_logger.info(f"为管理员账号 {identifier_to_use} 生成 UserSig 成功")
                except Exception as e:
                    sig_error = f"自动生成管理员 UserSig 失败: {e}"
                    print(f"[groups/invite] UserSig 生成失败: {sig_error}")
                    app_logger.error(sig_error)
            
            if not usersig_to_use:
                print(f"[groups/invite] 使用配置的 TENCENT_API_USER_SIG")
                usersig_to_use = TENCENT_API_USER_SIG
            
            if not usersig_to_use:
                error_message = "缺少可用的管理员 UserSig，无法调用腾讯接口。"
                print(f"[groups/invite] 错误: {error_message}")
                app_logger.error(f"[groups/invite] {error_message}")
                return JSONResponse({
                    "code": 500,
                    "message": error_message
                }, status_code=500)
            
            print(f"[groups/invite] 使用 identifier: {identifier_to_use}, SDKAppID: {TENCENT_API_SDK_APP_ID}")
            
            # 构建腾讯接口 URL
            invite_url = build_tencent_request_url(
                identifier=identifier_to_use,
                usersig=usersig_to_use,
                path_override="v4/group_open_http_svc/add_group_member"
            )
            
            if not invite_url:
                print("[groups/invite] 错误: 无法构建腾讯接口 URL")
                app_logger.error("[groups/invite] 无法构建腾讯接口 URL")
                return JSONResponse({
                    "code": 500,
                    "message": "腾讯接口配置错误"
                }, status_code=500)
            
            # 验证 URL 中是否包含 sdkappid
            if "sdkappid" not in invite_url:
                print(f"[groups/invite] 警告: URL 中缺少 sdkappid，完整 URL: {invite_url}")
                app_logger.warning(f"[groups/invite] URL 中缺少 sdkappid: {invite_url}")
                # 手动添加 sdkappid（如果 URL 构建失败）
                parsed_url = urllib.parse.urlparse(invite_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                query_params['sdkappid'] = [TENCENT_API_SDK_APP_ID]
                query_params['identifier'] = [identifier_to_use]
                query_params['usersig'] = [usersig_to_use]
                query_params['contenttype'] = ['json']
                if 'random' not in query_params:
                    query_params['random'] = [str(random.randint(1, 2**31 - 1))]
                new_query = urllib.parse.urlencode(query_params, doseq=True)
                invite_url = urllib.parse.urlunparse(parsed_url._replace(query=new_query))
                print(f"[groups/invite] 已手动添加参数，新 URL: {invite_url[:200]}...")
            
            # 构建邀请成员的 payload
            member_list = []
            for member in members:
                member_entry = {
                    "Member_Account": member.get('unique_member_id')
                }
                # 如果有角色信息，添加到 payload（腾讯接口支持 Role 字段）
                group_role = member.get('group_role')
                if group_role:
                    # 腾讯接口角色：Admin=300, Member=200, Owner=400
                    role_map = {
                        300: "Admin",
                        200: "Member",
                        400: "Owner"
                    }
                    if group_role in role_map:
                        member_entry["Role"] = role_map[group_role]
                member_list.append(member_entry)
            
            invite_payload = {
                "GroupId": group_id,
                "MemberList": member_list,
                "Silence": 0  # 0表示邀请时发送系统消息
            }
            
            print(f"[groups/invite] 腾讯接口 URL: {invite_url[:100]}...")
            print(f"[groups/invite] 邀请 payload: {json.dumps(invite_payload, ensure_ascii=False, indent=2)}")
            
            # 调用腾讯接口
            def _invite_tencent_members() -> Dict[str, Any]:
                """调用腾讯接口邀请成员"""
                headers = {
                    "Content-Type": "application/json; charset=utf-8"
                }
                encoded_payload = json.dumps(invite_payload, ensure_ascii=False).encode("utf-8")
                request_obj = urllib.request.Request(
                    url=invite_url,
                    data=encoded_payload,
                    headers=headers,
                    method="POST"
                )
                try:
                    with urllib.request.urlopen(request_obj, timeout=TENCENT_API_TIMEOUT) as response:
                        raw_body = response.read()
                        text_body = raw_body.decode("utf-8", errors="replace")
                        try:
                            parsed_body = json.loads(text_body)
                        except json.JSONDecodeError:
                            parsed_body = None
                        
                        result = {
                            "status": "success",
                            "http_status": response.status,
                            "response": parsed_body or text_body
                        }
                        return result
                except urllib.error.HTTPError as e:
                    body = e.read().decode("utf-8", errors="replace")
                    app_logger.error(f"[groups/invite] 腾讯接口调用失败 (HTTP {e.code}): {body}")
                    return {"status": "error", "http_status": e.code, "error": body}
                except urllib.error.URLError as e:
                    app_logger.error(f"[groups/invite] 腾讯接口调用异常: {e}")
                    return {"status": "error", "http_status": None, "error": str(e)}
                except Exception as exc:
                    app_logger.exception(f"[groups/invite] 腾讯接口未知异常: {exc}")
                    return {"status": "error", "http_status": None, "error": str(exc)}
            
            tencent_result = await asyncio.to_thread(_invite_tencent_members)
            
            # 检查腾讯接口调用结果
            if tencent_result.get('status') != 'success':
                error_msg = tencent_result.get('error', '腾讯接口调用失败')
                print(f"[groups/invite] 腾讯接口调用失败: {error_msg}")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": f"邀请成员失败: {error_msg}"
                }, status_code=500)
            
            tencent_response = tencent_result.get('response', {})
            if isinstance(tencent_response, dict):
                action_status = tencent_response.get('ActionStatus')
                error_code = tencent_response.get('ErrorCode')
                error_info = tencent_response.get('ErrorInfo')
                
                if action_status != 'OK' or error_code != 0:
                    print(f"[groups/invite] 腾讯接口返回错误: ErrorCode={error_code}, ErrorInfo={error_info}")
                    if connection and connection.is_connected():
                        connection.rollback()
                    return JSONResponse({
                        "code": 500,
                        "message": f"邀请成员失败: {error_info or '未知错误'}"
                    }, status_code=500)
            
            print(f"[groups/invite] 腾讯接口调用成功")
            
            # 4. 邀请成功后，插入数据库（事务已在开始时启动）
            print(f"[groups/invite] 开始插入数据库...")
            
            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            inserted_count = 0
            failed_members = []
            
            for member in members:
                unique_member_id = member.get('unique_member_id')
                member_name = member.get('member_name', '')
                group_role = member.get('group_role', 200)  # 默认200（普通成员），300是管理员，400是群主
                
                # 再次检查是否已存在（防止并发）
                cursor.execute(
                    "SELECT user_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                    (group_id, unique_member_id)
                )
                if cursor.fetchone():
                    print(f"[groups/invite] 成员 {unique_member_id} 已在群组中，跳过")
                    failed_members.append({
                        "unique_member_id": unique_member_id,
                        "reason": "已在群组中"
                    })
                    continue
                
                try:
                    insert_member_sql = """
                        INSERT INTO `group_members` (
                            group_id, user_id, user_name, self_role, join_time, msg_flag,
                            self_msg_flag, readed_seq, unread_num
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    insert_params = (
                        group_id,
                        unique_member_id,  # user_id 使用 unique_member_id
                        member_name if member_name else None,
                        group_role,  # self_role: 200=普通成员, 300=管理员, 400=群主
                        current_time,
                        0,  # msg_flag
                        0,  # self_msg_flag
                        0,  # readed_seq
                        0   # unread_num
                    )
                    
                    cursor.execute(insert_member_sql, insert_params)
                    inserted_count += 1
                    print(f"[groups/invite] 成功插入成员: {unique_member_id}")
                    
                except mysql.connector.Error as e:
                    print(f"[groups/invite] 插入成员 {unique_member_id} 失败: {e}")
                    failed_members.append({
                        "unique_member_id": unique_member_id,
                        "reason": f"数据库错误: {str(e)}"
                    })
                    # 继续处理其他成员
            
            # 更新群组的成员数量
            if inserted_count > 0:
                cursor.execute(
                    "UPDATE `groups` SET member_num = member_num + %s WHERE group_id = %s",
                    (inserted_count, group_id)
                )
                print(f"[groups/invite] 群组成员数量已更新，新增 {inserted_count} 人")
            
            # 提交事务
            connection.commit()
            print(f"[groups/invite] 事务提交成功")
            
            result = {
                "code": 200,
                "message": "邀请成功",
                "data": {
                    "group_id": group_id,
                    "invited_count": inserted_count,
                    "total_requested": len(members),
                    "failed_members": failed_members if failed_members else None
                }
            }
            
            print(f"[groups/invite] 返回结果: {result}")
            print("=" * 80)
            
            return JSONResponse(result, status_code=200)
            
        except mysql.connector.Error as e:
            if connection and connection.is_connected():
                connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/invite] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/invite] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/invite] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"数据库操作失败: {str(e)}"
            }, status_code=500)
        except Exception as e:
            if connection and connection.is_connected():
                connection.rollback()
            error_msg = f"邀请成员时发生异常: {e}"
            print(f"[groups/invite] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/invite] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/invite] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"操作失败: {str(e)}"
            }, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/invite] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/invite] 数据库连接已关闭")
                app_logger.info("[groups/invite] Database connection closed after invite members attempt.")
    
    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/invite] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/invite] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/invite] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "code": 400,
            "message": "请求数据格式错误"
        }, status_code=400)
    finally:
        print("=" * 80)

@app.post("/groups/leave")
async def leave_group(request: Request):
    """
    用户退出群组
    接收客户端发送的 group_id, user_id
    从 group_members 表中删除该用户，并更新群组的成员数量
    """
    print("=" * 80)
    print("[groups/leave] 收到退出群组请求")
    
    # 打印请求头信息用于调试
    content_type = request.headers.get("content-type", "")
    content_length = request.headers.get("content-length", "")
    print(f"[groups/leave] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")
    
    try:
        # 解析请求体JSON数据
        try:
            # 先尝试读取原始body
            body_bytes = await request.body()
            print(f"[groups/leave] 读取到请求体长度: {len(body_bytes)} 字节")
            
            if not body_bytes:
                print("[groups/leave] 错误: 请求体为空")
                return JSONResponse({
                    "code": 400,
                    "message": "请求体不能为空"
                }, status_code=400)
            
            # 解析JSON
            try:
                data = json.loads(body_bytes.decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"[groups/leave] 错误: JSON解析失败 - {e}")
                print(f"[groups/leave] 请求体内容: {body_bytes.decode('utf-8', errors='ignore')}")
                return JSONResponse({
                    "code": 400,
                    "message": "请求数据格式错误，无法解析JSON"
                }, status_code=400)
                
        except ClientDisconnect:
            print("[groups/leave] 错误: 客户端断开连接")
            print(f"[groups/leave] 调试信息 - Content-Type: {content_type}, Content-Length: {content_length}")
            app_logger.warning("[groups/leave] 客户端在请求完成前断开连接")
            return JSONResponse({
                "code": 400,
                "message": "客户端断开连接，请检查请求数据是否正确发送"
            }, status_code=400)
        except Exception as e:
            print(f"[groups/leave] 读取请求体时发生异常: {type(e).__name__} - {e}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/leave] 错误堆栈: {traceback_str}")
            return JSONResponse({
                "code": 400,
                "message": f"读取请求数据失败: {str(e)}"
            }, status_code=400)
        
        print(f"[groups/leave] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        group_id = data.get('group_id')
        user_id = data.get('user_id')
        
        print(f"[groups/leave] 解析结果 - group_id: {group_id}, user_id: {user_id}")
        
        # 参数验证
        if not group_id:
            print("[groups/leave] 错误: 缺少 group_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 group_id"
            }, status_code=400)
        
        if not user_id:
            print("[groups/leave] 错误: 缺少 user_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 user_id"
            }, status_code=400)
        
        print("[groups/leave] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/leave] 错误: 数据库连接失败")
            app_logger.error("[groups/leave] 数据库连接失败")
            return JSONResponse({
                "code": 500,
                "message": "数据库连接失败"
            }, status_code=500)
        print("[groups/leave] 数据库连接成功")
        
        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 1. 检查群组是否存在
            print(f"[groups/leave] 检查群组 {group_id} 是否存在...")
            cursor.execute("SELECT group_id, group_name, member_num FROM `groups` WHERE group_id = %s", (group_id,))
            group_info = cursor.fetchone()
            
            if not group_info:
                print(f"[groups/leave] 错误: 群组 {group_id} 不存在")
                return JSONResponse({
                    "code": 404,
                    "message": "群组不存在"
                }, status_code=404)
            
            print(f"[groups/leave] 群组信息: {group_info}")
            
            # 2. 检查用户是否在群组中
            print(f"[groups/leave] 检查用户 {user_id} 是否在群组 {group_id} 中...")
            cursor.execute(
                "SELECT group_id, user_id, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id)
            )
            member_info = cursor.fetchone()
            
            if not member_info:
                print(f"[groups/leave] 错误: 用户 {user_id} 不在群组 {group_id} 中")
                return JSONResponse({
                    "code": 400,
                    "message": "您不在该群组中"
                }, status_code=400)
            
            print(f"[groups/leave] 成员信息: {member_info}")
            self_role = member_info.get('self_role', 200)
            
            # 3. 检查是否是群主（self_role = 400 表示群主）
            if self_role == 400:
                print(f"[groups/leave] 警告: 用户 {user_id} 是群主，不允许直接退出")
                # 可以选择不允许群主退出，或者允许退出（这里选择允许退出）
                # 如果需要不允许群主退出，可以取消下面的注释并返回错误
                # return JSONResponse({
                #     "code": 400,
                #     "message": "群主不能直接退出群组，请先转移群主权限"
                # }, status_code=400)
            
            # 4. 从群组中删除该成员
            print(f"[groups/leave] 从群组 {group_id} 中删除用户 {user_id}...")
            cursor.execute(
                "DELETE FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id)
            )
            affected_rows = cursor.rowcount
            print(f"[groups/leave] 删除成员完成, 影响行数: {affected_rows}")
            
            if affected_rows == 0:
                print(f"[groups/leave] 警告: 删除操作未影响任何行")
                return JSONResponse({
                    "code": 500,
                    "message": "退出群组失败"
                }, status_code=500)
            
            # 5. 更新群组的成员数量（确保不会小于0）
            print(f"[groups/leave] 更新群组 {group_id} 的成员数量...")
            # 使用 CASE 语句避免 UNSIGNED 类型溢出问题
            # 当 member_num 为 0 时，member_num - 1 会导致 UNSIGNED 溢出错误
            cursor.execute(
                "UPDATE `groups` SET member_num = CASE WHEN member_num > 0 THEN member_num - 1 ELSE 0 END WHERE group_id = %s",
                (group_id,)
            )
            print(f"[groups/leave] 群组成员数量已更新")
            
            # 提交事务
            connection.commit()
            print(f"[groups/leave] 事务提交成功")
            
            result = {
                "code": 200,
                "message": "成功退出群组",
                "data": {
                    "group_id": group_id,
                    "user_id": user_id
                }
            }
            
            print(f"[groups/leave] 返回结果: {result}")
            print("=" * 80)
            
            return JSONResponse(result, status_code=200)
            
        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/leave] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/leave] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/leave] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"数据库操作失败: {str(e)}"
            }, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"退出群组时发生异常: {e}"
            print(f"[groups/leave] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/leave] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/leave] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"操作失败: {str(e)}"
            }, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/leave] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/leave] 数据库连接已关闭")
                app_logger.info("[groups/leave] Database connection closed after leave group attempt.")
    
    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/leave] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/leave] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/leave] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "code": 400,
            "message": "请求数据格式错误"
        }, status_code=400)
    finally:
        print("=" * 80)

@app.post("/groups/remove-member")
async def remove_member(request: Request):
    """
    群主踢出群成员
    接收客户端发送的 group_id 和 members 数组
    1. 调用腾讯接口踢出成员
    2. 成功后，从数据库删除成员并更新群组成员数量
    请求体 JSON:
    {
      "group_id": "群组ID",
      "members": ["成员ID1", "成员ID2", ...]
    }
    """
    print("=" * 80)
    print("[groups/remove-member] 收到踢出成员请求")
    
    try:
        data = await request.json()
        print(f"[groups/remove-member] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        group_id = data.get('group_id')
        members = data.get('members', [])
        
        # 参数验证
        if not group_id:
            print("[groups/remove-member] 错误: 缺少 group_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 group_id"
            }, status_code=400)
        
        if not members or not isinstance(members, list):
            print("[groups/remove-member] 错误: 缺少或无效的 members")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 members 或 members 必须是数组"
            }, status_code=400)
        
        if len(members) == 0:
            print("[groups/remove-member] 错误: members 数组为空")
            return JSONResponse({
                "code": 400,
                "message": "members 数组不能为空"
            }, status_code=400)
        
        print("[groups/remove-member] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/remove-member] 错误: 数据库连接失败")
            app_logger.error("[groups/remove-member] 数据库连接失败")
            return JSONResponse({
                "code": 500,
                "message": "数据库连接失败"
            }, status_code=500)
        print("[groups/remove-member] 数据库连接成功")
        
        cursor = None
        try:
            # 开始事务（在开始时就启动，确保所有操作在一个事务中）
            connection.start_transaction()
            cursor = connection.cursor(dictionary=True)
            
            # 1. 检查群组是否存在
            print(f"[groups/remove-member] 检查群组 {group_id} 是否存在...")
            cursor.execute(
                "SELECT group_id, group_name, member_num FROM `groups` WHERE group_id = %s",
                (group_id,)
            )
            group_info = cursor.fetchone()
            
            if not group_info:
                print(f"[groups/remove-member] 错误: 群组 {group_id} 不存在")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({
                    "code": 404,
                    "message": "群组不存在"
                }, status_code=404)
            
            print(f"[groups/remove-member] 群组信息: {group_info}")
            
            # 2. 检查要删除的成员是否在群组中，并过滤掉群主
            print(f"[groups/remove-member] 检查成员是否在群组中...")
            valid_members = []
            owner_members = []
            
            for member_id in members:
                cursor.execute(
                    "SELECT group_id, user_id, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                    (group_id, member_id)
                )
                member_info = cursor.fetchone()
                
                if not member_info:
                    print(f"[groups/remove-member] 警告: 成员 {member_id} 不在群组中，跳过")
                    continue
                
                self_role = member_info.get('self_role', 200)
                if self_role == 400:  # 群主不能被踢出
                    print(f"[groups/remove-member] 警告: 成员 {member_id} 是群主，不允许被踢出")
                    owner_members.append(member_id)
                    continue
                
                valid_members.append(member_id)
            
            if owner_members:
                print(f"[groups/remove-member] 警告: 以下成员是群主，无法踢出: {owner_members}")
            
            if not valid_members:
                print(f"[groups/remove-member] 错误: 没有可踢出的成员")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({
                    "code": 400,
                    "message": "没有可踢出的成员（可能是群主或不在群组中）"
                }, status_code=400)
            
            print(f"[groups/remove-member] 准备踢出 {len(valid_members)} 个成员: {valid_members}")
            
            # 3. 调用腾讯接口踢出成员
            print(f"[groups/remove-member] 准备调用腾讯接口踢出 {len(valid_members)} 个成员...")
            
            # 使用管理员账号作为 identifier（与群组同步保持一致）
            identifier_to_use = TENCENT_API_IDENTIFIER
            
            # 检查必需的配置
            if not TENCENT_API_SDK_APP_ID:
                print("[groups/remove-member] 错误: TENCENT_API_SDK_APP_ID 未配置")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": "腾讯接口配置错误: 缺少 SDKAppID"
                }, status_code=500)
            
            if not identifier_to_use:
                print("[groups/remove-member] 错误: TENCENT_API_IDENTIFIER 未配置")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": "腾讯接口配置错误: 缺少 Identifier"
                }, status_code=500)
            
            # 尝试生成或使用配置的 UserSig（与群组同步逻辑一致）
            usersig_to_use: Optional[str] = None
            sig_error: Optional[str] = None
            if TENCENT_API_SECRET_KEY:
                try:
                    # 为管理员账号生成 UserSig
                    print(f"[groups/remove-member] 准备为管理员账号生成 UserSig: identifier={identifier_to_use}")
                    usersig_to_use = generate_tencent_user_sig(identifier_to_use)
                    print(f"[groups/remove-member] UserSig 生成成功，长度: {len(usersig_to_use) if usersig_to_use else 0}")
                    app_logger.info(f"为管理员账号 {identifier_to_use} 生成 UserSig 成功")
                except Exception as e:
                    sig_error = f"自动生成管理员 UserSig 失败: {e}"
                    print(f"[groups/remove-member] UserSig 生成失败: {sig_error}")
                    app_logger.error(sig_error)
            
            if not usersig_to_use:
                print(f"[groups/remove-member] 使用配置的 TENCENT_API_USER_SIG")
                usersig_to_use = TENCENT_API_USER_SIG
            
            if not usersig_to_use:
                error_message = "缺少可用的管理员 UserSig，无法调用腾讯接口。"
                print(f"[groups/remove-member] 错误: {error_message}")
                app_logger.error(f"[groups/remove-member] {error_message}")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": error_message
                }, status_code=500)
            
            print(f"[groups/remove-member] 使用 identifier: {identifier_to_use}, SDKAppID: {TENCENT_API_SDK_APP_ID}")
            
            # 构建腾讯接口 URL
            delete_url = build_tencent_request_url(
                identifier=identifier_to_use,
                usersig=usersig_to_use,
                path_override="v4/group_open_http_svc/delete_group_member"
            )
            
            if not delete_url:
                print("[groups/remove-member] 错误: 无法构建腾讯接口 URL")
                app_logger.error("[groups/remove-member] 无法构建腾讯接口 URL")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": "腾讯接口配置错误"
                }, status_code=500)
            
            # 验证 URL 中是否包含 sdkappid
            if "sdkappid" not in delete_url:
                print(f"[groups/remove-member] 警告: URL 中缺少 sdkappid，完整 URL: {delete_url}")
                app_logger.warning(f"[groups/remove-member] URL 中缺少 sdkappid: {delete_url}")
                # 手动添加 sdkappid（如果 URL 构建失败）
                parsed_url = urllib.parse.urlparse(delete_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                query_params['sdkappid'] = [TENCENT_API_SDK_APP_ID]
                query_params['identifier'] = [identifier_to_use]
                query_params['usersig'] = [usersig_to_use]
                query_params['contenttype'] = ['json']
                if 'random' not in query_params:
                    query_params['random'] = [str(random.randint(1, 2**31 - 1))]
                new_query = urllib.parse.urlencode(query_params, doseq=True)
                delete_url = urllib.parse.urlunparse(parsed_url._replace(query=new_query))
                print(f"[groups/remove-member] 已手动添加参数，新 URL: {delete_url[:200]}...")
            
            # 构建踢出成员的 payload
            delete_payload = {
                "GroupId": group_id,
                "MemberToDel_Account": valid_members,
                "Reason": "群主踢出"  # 可选：踢出原因
            }
            
            print(f"[groups/remove-member] 腾讯接口 URL: {delete_url[:100]}...")
            print(f"[groups/remove-member] 踢出 payload: {json.dumps(delete_payload, ensure_ascii=False, indent=2)}")
            
            # 调用腾讯接口
            def _delete_tencent_members() -> Dict[str, Any]:
                """调用腾讯接口踢出成员"""
                headers = {
                    "Content-Type": "application/json; charset=utf-8"
                }
                encoded_payload = json.dumps(delete_payload, ensure_ascii=False).encode("utf-8")
                request_obj = urllib.request.Request(
                    url=delete_url,
                    data=encoded_payload,
                    headers=headers,
                    method="POST"
                )
                try:
                    with urllib.request.urlopen(request_obj, timeout=TENCENT_API_TIMEOUT) as response:
                        raw_body = response.read()
                        text_body = raw_body.decode("utf-8", errors="replace")
                        try:
                            parsed_body = json.loads(text_body)
                        except json.JSONDecodeError:
                            parsed_body = None
                        
                        result = {
                            "status": "success",
                            "http_status": response.status,
                            "response": parsed_body or text_body
                        }
                        return result
                except urllib.error.HTTPError as e:
                    body = e.read().decode("utf-8", errors="replace")
                    app_logger.error(f"[groups/remove-member] 腾讯接口调用失败 (HTTP {e.code}): {body}")
                    return {"status": "error", "http_status": e.code, "error": body}
                except urllib.error.URLError as e:
                    app_logger.error(f"[groups/remove-member] 腾讯接口调用异常: {e}")
                    return {"status": "error", "http_status": None, "error": str(e)}
                except Exception as exc:
                    app_logger.exception(f"[groups/remove-member] 腾讯接口未知异常: {exc}")
                    return {"status": "error", "http_status": None, "error": str(exc)}
            
            tencent_result = await asyncio.to_thread(_delete_tencent_members)
            
            # 打印腾讯接口响应详情
            print(f"[groups/remove-member] 腾讯接口响应状态: {tencent_result.get('status')}")
            print(f"[groups/remove-member] 腾讯接口HTTP状态码: {tencent_result.get('http_status')}")
            tencent_response = tencent_result.get('response', {})
            print(f"[groups/remove-member] 腾讯接口响应内容: {json.dumps(tencent_response, ensure_ascii=False, indent=2) if isinstance(tencent_response, dict) else tencent_response}")
            
            # 检查腾讯接口调用结果
            if tencent_result.get('status') != 'success':
                error_msg = tencent_result.get('error', '腾讯接口调用失败')
                print(f"[groups/remove-member] 腾讯接口调用失败: {error_msg}")
                app_logger.error(f"[groups/remove-member] 腾讯接口调用失败: group_id={group_id}, error={error_msg}")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": f"踢出成员失败: {error_msg}"
                }, status_code=500)
            
            if isinstance(tencent_response, dict):
                action_status = tencent_response.get('ActionStatus')
                error_code = tencent_response.get('ErrorCode')
                error_info = tencent_response.get('ErrorInfo')
                
                print(f"[groups/remove-member] 腾讯接口响应解析: ActionStatus={action_status}, ErrorCode={error_code}, ErrorInfo={error_info}")
                
                if action_status != 'OK' or error_code != 0:
                    print(f"[groups/remove-member] 腾讯接口返回错误: ErrorCode={error_code}, ErrorInfo={error_info}")
                    app_logger.error(f"[groups/remove-member] 腾讯接口返回错误: group_id={group_id}, ErrorCode={error_code}, ErrorInfo={error_info}")
                    if connection and connection.is_connected():
                        connection.rollback()
                    return JSONResponse({
                        "code": 500,
                        "message": f"踢出成员失败: {error_info or '未知错误'}"
                    }, status_code=500)
            else:
                print(f"[groups/remove-member] 警告: 腾讯接口响应不是JSON格式: {type(tencent_response)}")
                app_logger.warning(f"[groups/remove-member] 腾讯接口响应格式异常: group_id={group_id}, response_type={type(tencent_response)}")
            
            print(f"[groups/remove-member] 腾讯接口调用成功，准备更新数据库")
            app_logger.info(f"[groups/remove-member] 腾讯接口调用成功: group_id={group_id}, members={valid_members}")
            
            # 4. 踢出成功后，从数据库删除成员
            print(f"[groups/remove-member] 开始从数据库删除成员...")
            
            deleted_count = 0
            for member_id in valid_members:
                cursor.execute(
                    "DELETE FROM `group_members` WHERE group_id = %s AND user_id = %s",
                    (group_id, member_id)
                )
                if cursor.rowcount > 0:
                    deleted_count += 1
                    print(f"[groups/remove-member] 成功删除成员: {member_id}")
            
            if deleted_count == 0:
                print(f"[groups/remove-member] 警告: 数据库删除操作未影响任何行")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": "删除成员失败"
                }, status_code=500)
            
            # 5. 更新群组的成员数量（确保不会小于0）
            print(f"[groups/remove-member] 更新群组 {group_id} 的成员数量...")
            cursor.execute(
                "UPDATE `groups` SET member_num = CASE WHEN member_num >= %s THEN member_num - %s ELSE 0 END WHERE group_id = %s",
                (deleted_count, deleted_count, group_id)
            )
            print(f"[groups/remove-member] 群组成员数量已更新，减少 {deleted_count} 人")
            
            # 提交事务
            connection.commit()
            print(f"[groups/remove-member] 事务提交成功")
            
            result = {
                "code": 200,
                "message": "成功踢出成员",
                "data": {
                    "group_id": group_id,
                    "deleted_count": deleted_count,
                    "total_requested": len(members),
                    "owner_members": owner_members if owner_members else None
                }
            }
            
            print(f"[groups/remove-member] 返回结果: {result}")
            print("=" * 80)
            
            return JSONResponse(result, status_code=200)
            
        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/remove-member] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/remove-member] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/remove-member] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"数据库操作失败: {str(e)}"
            }, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"移除成员时发生异常: {e}"
            print(f"[groups/remove-member] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/remove-member] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/remove-member] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"操作失败: {str(e)}"
            }, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/remove-member] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/remove-member] 数据库连接已关闭")
                app_logger.info("[groups/remove-member] Database connection closed after remove member attempt.")
    
    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/remove-member] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/remove-member] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/remove-member] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "code": 400,
            "message": "请求数据格式错误"
        }, status_code=400)
    finally:
        print("=" * 80)

@app.post("/groups/dismiss")
async def dismiss_group(request: Request):
    """
    解散群组
    接收客户端发送的 group_id, user_id
    只有群主才能解散群组
    删除群组的所有成员和群组本身
    """
    print("=" * 80)
    print("[groups/dismiss] 收到解散群组请求")
    
    # 打印请求头信息用于调试
    content_type = request.headers.get("content-type", "")
    content_length = request.headers.get("content-length", "")
    print(f"[groups/dismiss] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")
    
    try:
        # 解析请求体JSON数据
        try:
            # 先尝试读取原始body
            body_bytes = await request.body()
            print(f"[groups/dismiss] 读取到请求体长度: {len(body_bytes)} 字节")
            
            if not body_bytes:
                print("[groups/dismiss] 错误: 请求体为空")
                return JSONResponse({
                    "code": 400,
                    "message": "请求体不能为空"
                }, status_code=400)
            
            # 解析JSON
            try:
                data = json.loads(body_bytes.decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"[groups/dismiss] 错误: JSON解析失败 - {e}")
                print(f"[groups/dismiss] 请求体内容: {body_bytes.decode('utf-8', errors='ignore')}")
                return JSONResponse({
                    "code": 400,
                    "message": "请求数据格式错误，无法解析JSON"
                }, status_code=400)
                
        except ClientDisconnect:
            print("[groups/dismiss] 错误: 客户端断开连接")
            print(f"[groups/dismiss] 调试信息 - Content-Type: {content_type}, Content-Length: {content_length}")
            app_logger.warning("[groups/dismiss] 客户端在请求完成前断开连接")
            return JSONResponse({
                "code": 400,
                "message": "客户端断开连接，请检查请求数据是否正确发送"
            }, status_code=400)
        except Exception as e:
            print(f"[groups/dismiss] 读取请求体时发生异常: {type(e).__name__} - {e}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/dismiss] 错误堆栈: {traceback_str}")
            return JSONResponse({
                "code": 400,
                "message": f"读取请求数据失败: {str(e)}"
            }, status_code=400)
        
        print(f"[groups/dismiss] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        group_id = data.get('group_id')
        user_id = data.get('user_id')
        
        print(f"[groups/dismiss] 解析结果 - group_id: {group_id}, user_id: {user_id}")
        
        # 参数验证
        if not group_id:
            print("[groups/dismiss] 错误: 缺少 group_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 group_id"
            }, status_code=400)
        
        if not user_id:
            print("[groups/dismiss] 错误: 缺少 user_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 user_id"
            }, status_code=400)
        
        print("[groups/dismiss] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/dismiss] 错误: 数据库连接失败")
            app_logger.error("[groups/dismiss] 数据库连接失败")
            return JSONResponse({
                "code": 500,
                "message": "数据库连接失败"
            }, status_code=500)
        print("[groups/dismiss] 数据库连接成功")
        
        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 1. 检查群组是否存在
            print(f"[groups/dismiss] 检查群组 {group_id} 是否存在...")
            cursor.execute("SELECT group_id, group_name, member_num FROM `groups` WHERE group_id = %s", (group_id,))
            group_info = cursor.fetchone()
            
            if not group_info:
                print(f"[groups/dismiss] 错误: 群组 {group_id} 不存在")
                return JSONResponse({
                    "code": 404,
                    "message": "群组不存在"
                }, status_code=404)
            
            print(f"[groups/dismiss] 群组信息: {group_info}")
            group_name = group_info.get('group_name', '')
            
            # 2. 检查用户是否在群组中，并且是否是群主
            print(f"[groups/dismiss] 检查用户 {user_id} 是否是群组 {group_id} 的群主...")
            cursor.execute(
                "SELECT group_id, user_id, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id)
            )
            member_info = cursor.fetchone()
            
            if not member_info:
                print(f"[groups/dismiss] 错误: 用户 {user_id} 不在群组 {group_id} 中")
                return JSONResponse({
                    "code": 403,
                    "message": "您不是该群组的成员"
                }, status_code=403)
            
            print(f"[groups/dismiss] 成员信息: {member_info}")
            self_role = member_info.get('self_role', 200)
            
            # 3. 检查是否是群主（self_role = 400 表示群主）
            if self_role != 400:
                print(f"[groups/dismiss] 错误: 用户 {user_id} 不是群主，无权解散群组")
                return JSONResponse({
                    "code": 403,
                    "message": "只有群主才能解散群组"
                }, status_code=403)
            
            print(f"[groups/dismiss] 验证通过: 用户 {user_id} 是群主，可以解散群组")
            
            # 4. 删除群组的所有成员
            print(f"[groups/dismiss] 删除群组 {group_id} 的所有成员...")
            cursor.execute(
                "DELETE FROM `group_members` WHERE group_id = %s",
                (group_id,)
            )
            deleted_members = cursor.rowcount
            print(f"[groups/dismiss] 已删除 {deleted_members} 个成员")
            
            # 5. 删除群组本身
            print(f"[groups/dismiss] 删除群组 {group_id}...")
            cursor.execute(
                "DELETE FROM `groups` WHERE group_id = %s",
                (group_id,)
            )
            deleted_groups = cursor.rowcount
            print(f"[groups/dismiss] 删除群组完成, 影响行数: {deleted_groups}")
            
            if deleted_groups == 0:
                print(f"[groups/dismiss] 警告: 删除群组操作未影响任何行")
                connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": "解散群组失败"
                }, status_code=500)
            
            # 提交事务
            connection.commit()
            print(f"[groups/dismiss] 事务提交成功")
            
            result = {
                "code": 200,
                "message": "成功解散群组",
                "data": {
                    "group_id": group_id,
                    "group_name": group_name,
                    "user_id": user_id,
                    "deleted_members": deleted_members
                }
            }
            
            print(f"[groups/dismiss] 返回结果: {result}")
            print("=" * 80)
            
            return JSONResponse(result, status_code=200)
            
        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/dismiss] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/dismiss] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/dismiss] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"数据库操作失败: {str(e)}"
            }, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"解散群组时发生异常: {e}"
            print(f"[groups/dismiss] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/dismiss] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/dismiss] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"操作失败: {str(e)}"
            }, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/dismiss] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/dismiss] 数据库连接已关闭")
                app_logger.info("[groups/dismiss] Database connection closed after dismiss group attempt.")
    
    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/dismiss] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/dismiss] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/dismiss] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "code": 400,
            "message": "请求数据格式错误"
        }, status_code=400)
    finally:
        print("=" * 80)

@app.post("/groups/set_admin_role")
async def set_admin_role(request: Request):
    """
    设置群成员角色（管理员或成员）
    接收客户端发送的 group_id, user_id, role
    更新 group_members 表中的 self_role 字段
    角色映射: 群主=400, 管理员=300, 成员=1
    role: "管理员" -> self_role = 300, "成员" -> self_role = 1
    """
    print("=" * 80)
    print("[groups/set_admin_role] 收到设置管理员角色请求")
    
    # 打印请求头信息用于调试
    content_type = request.headers.get("content-type", "")
    content_length = request.headers.get("content-length", "")
    print(f"[groups/set_admin_role] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")
    
    try:
        # 解析请求体JSON数据
        try:
            # 先尝试读取原始body
            body_bytes = await request.body()
            print(f"[groups/set_admin_role] 读取到请求体长度: {len(body_bytes)} 字节")
            
            if not body_bytes:
                print("[groups/set_admin_role] 错误: 请求体为空")
                return JSONResponse({
                    "code": 400,
                    "message": "请求体不能为空"
                }, status_code=400)
            
            # 解析JSON
            try:
                data = json.loads(body_bytes.decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"[groups/set_admin_role] 错误: JSON解析失败 - {e}")
                print(f"[groups/set_admin_role] 请求体内容: {body_bytes.decode('utf-8', errors='ignore')}")
                return JSONResponse({
                    "code": 400,
                    "message": "请求数据格式错误，无法解析JSON"
                }, status_code=400)
                
        except ClientDisconnect:
            print("[groups/set_admin_role] 错误: 客户端断开连接")
            print(f"[groups/set_admin_role] 调试信息 - Content-Type: {content_type}, Content-Length: {content_length}")
            app_logger.warning("[groups/set_admin_role] 客户端在请求完成前断开连接")
            return JSONResponse({
                "code": 400,
                "message": "客户端断开连接，请检查请求数据是否正确发送"
            }, status_code=400)
        except Exception as e:
            print(f"[groups/set_admin_role] 读取请求体时发生异常: {type(e).__name__} - {e}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/set_admin_role] 错误堆栈: {traceback_str}")
            return JSONResponse({
                "code": 400,
                "message": f"读取请求数据失败: {str(e)}"
            }, status_code=400)
        
        print(f"[groups/set_admin_role] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        group_id = data.get('group_id')
        user_id = data.get('user_id')
        role = data.get('role')
        
        print(f"[groups/set_admin_role] 解析结果 - group_id: {group_id}, user_id: {user_id}, role: {role}")
        
        # 参数验证
        if not group_id:
            print("[groups/set_admin_role] 错误: 缺少 group_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 group_id"
            }, status_code=400)
        
        if not user_id:
            print("[groups/set_admin_role] 错误: 缺少 user_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 user_id"
            }, status_code=400)
        
        if not role:
            print("[groups/set_admin_role] 错误: 缺少 role")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 role"
            }, status_code=400)
        
        # 将角色从中文映射到数据库值
        # 群主: 400, 管理员: 300, 成员: 1
        role_mapping = {
            "管理员": 300,
            "成员": 1
        }
        
        if role not in role_mapping:
            print(f"[groups/set_admin_role] 错误: 无效的角色值 {role}，只支持 '管理员' 或 '成员'")
            return JSONResponse({
                "code": 400,
                "message": f"无效的角色值，只支持 '管理员' 或 '成员'"
            }, status_code=400)
        
        self_role = role_mapping[role]
        print(f"[groups/set_admin_role] 角色映射: {role} -> {self_role}")
        
        print("[groups/set_admin_role] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/set_admin_role] 错误: 数据库连接失败")
            app_logger.error("[groups/set_admin_role] 数据库连接失败")
            return JSONResponse({
                "code": 500,
                "message": "数据库连接失败"
            }, status_code=500)
        print("[groups/set_admin_role] 数据库连接成功")
        
        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 1. 检查群组是否存在
            print(f"[groups/set_admin_role] 检查群组 {group_id} 是否存在...")
            cursor.execute("SELECT group_id, group_name FROM `groups` WHERE group_id = %s", (group_id,))
            group_info = cursor.fetchone()
            
            if not group_info:
                print(f"[groups/set_admin_role] 错误: 群组 {group_id} 不存在")
                return JSONResponse({
                    "code": 404,
                    "message": "群组不存在"
                }, status_code=404)
            
            print(f"[groups/set_admin_role] 群组信息: {group_info}")
            
            # 2. 检查成员是否在群组中
            print(f"[groups/set_admin_role] 检查用户 {user_id} 是否在群组 {group_id} 中...")
            cursor.execute(
                "SELECT group_id, user_id, user_name, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id)
            )
            member_info = cursor.fetchone()
            
            if not member_info:
                print(f"[groups/set_admin_role] 错误: 用户 {user_id} 不在群组 {group_id} 中")
                return JSONResponse({
                    "code": 404,
                    "message": "该用户不是群组成员"
                }, status_code=404)
            
            print(f"[groups/set_admin_role] 成员信息: {member_info}")
            current_role = member_info.get('self_role', 200)
            user_name = member_info.get('user_name', '')
            
            # 3. 如果角色没有变化，直接返回成功
            if current_role == self_role:
                print(f"[groups/set_admin_role] 用户 {user_id} 的角色已经是 {role}，无需更新")
                return JSONResponse({
                    "code": 200,
                    "message": f"用户角色已经是{role}",
                    "data": {
                        "group_id": group_id,
                        "user_id": user_id,
                        "user_name": user_name,
                        "role": role,
                        "self_role": self_role
                    }
                }, status_code=200)
            
            # 4. 更新成员角色
            print(f"[groups/set_admin_role] 更新用户 {user_id} 的角色从 {current_role} 到 {self_role}...")
            cursor.execute(
                "UPDATE `group_members` SET self_role = %s WHERE group_id = %s AND user_id = %s",
                (self_role, group_id, user_id)
            )
            affected_rows = cursor.rowcount
            print(f"[groups/set_admin_role] 更新角色完成, 影响行数: {affected_rows}")
            
            if affected_rows == 0:
                print(f"[groups/set_admin_role] 警告: 更新角色操作未影响任何行")
                connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": "更新角色失败"
                }, status_code=500)
            
            # 提交事务
            connection.commit()
            print(f"[groups/set_admin_role] 事务提交成功")
            
            result = {
                "code": 200,
                "message": f"成功设置用户角色为{role}",
                "data": {
                    "group_id": group_id,
                    "user_id": user_id,
                    "user_name": user_name,
                    "role": role,
                    "self_role": self_role
                }
            }
            
            print(f"[groups/set_admin_role] 返回结果: {result}")
            print("=" * 80)
            
            return JSONResponse(result, status_code=200)
            
        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/set_admin_role] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/set_admin_role] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/set_admin_role] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"数据库操作失败: {str(e)}"
            }, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"设置管理员角色时发生异常: {e}"
            print(f"[groups/set_admin_role] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/set_admin_role] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/set_admin_role] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"操作失败: {str(e)}"
            }, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/set_admin_role] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/set_admin_role] 数据库连接已关闭")
                app_logger.info("[groups/set_admin_role] Database connection closed after set admin role attempt.")
    
    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/set_admin_role] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/set_admin_role] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/set_admin_role] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "code": 400,
            "message": "请求数据格式错误"
        }, status_code=400)
    finally:
        print("=" * 80)

@app.post("/groups/transfer_owner")
async def transfer_owner(request: Request):
    """
    转让群主
    接收客户端发送的 group_id, old_owner_id, new_owner_id
    1. 将新群主设置为群主（self_role = 400）
    2. 让原群主退出群组（从 group_members 表中删除）
    3. 更新群组的成员数量
    """
    print("=" * 80)
    print("[groups/transfer_owner] 收到转让群主请求")
    
    # 打印请求头信息用于调试
    content_type = request.headers.get("content-type", "")
    content_length = request.headers.get("content-length", "")
    print(f"[groups/transfer_owner] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")
    
    try:
        # 解析请求体JSON数据
        try:
            # 先尝试读取原始body
            body_bytes = await request.body()
            print(f"[groups/transfer_owner] 读取到请求体长度: {len(body_bytes)} 字节")
            
            if not body_bytes:
                print("[groups/transfer_owner] 错误: 请求体为空")
                return JSONResponse({
                    "code": 400,
                    "message": "请求体不能为空"
                }, status_code=400)
            
            # 解析JSON
            try:
                data = json.loads(body_bytes.decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"[groups/transfer_owner] 错误: JSON解析失败 - {e}")
                print(f"[groups/transfer_owner] 请求体内容: {body_bytes.decode('utf-8', errors='ignore')}")
                return JSONResponse({
                    "code": 400,
                    "message": "请求数据格式错误，无法解析JSON"
                }, status_code=400)
                
        except ClientDisconnect:
            print("[groups/transfer_owner] 错误: 客户端断开连接")
            print(f"[groups/transfer_owner] 调试信息 - Content-Type: {content_type}, Content-Length: {content_length}")
            app_logger.warning("[groups/transfer_owner] 客户端在请求完成前断开连接")
            return JSONResponse({
                "code": 400,
                "message": "客户端断开连接，请检查请求数据是否正确发送"
            }, status_code=400)
        except Exception as e:
            print(f"[groups/transfer_owner] 读取请求体时发生异常: {type(e).__name__} - {e}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/transfer_owner] 错误堆栈: {traceback_str}")
            return JSONResponse({
                "code": 400,
                "message": f"读取请求数据失败: {str(e)}"
            }, status_code=400)
        
        print(f"[groups/transfer_owner] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        group_id = data.get('group_id')
        old_owner_id = data.get('old_owner_id')
        new_owner_id = data.get('new_owner_id')
        
        print(f"[groups/transfer_owner] 解析结果 - group_id: {group_id}, old_owner_id: {old_owner_id}, new_owner_id: {new_owner_id}")
        
        # 参数验证
        if not group_id:
            print("[groups/transfer_owner] 错误: 缺少 group_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 group_id"
            }, status_code=400)
        
        if not old_owner_id:
            print("[groups/transfer_owner] 错误: 缺少 old_owner_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 old_owner_id"
            }, status_code=400)
        
        if not new_owner_id:
            print("[groups/transfer_owner] 错误: 缺少 new_owner_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 new_owner_id"
            }, status_code=400)
        
        # 检查原群主和新群主不能是同一个人
        if old_owner_id == new_owner_id:
            print(f"[groups/transfer_owner] 错误: 原群主和新群主不能是同一个人")
            return JSONResponse({
                "code": 400,
                "message": "原群主和新群主不能是同一个人"
            }, status_code=400)
        
        print("[groups/transfer_owner] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/transfer_owner] 错误: 数据库连接失败")
            app_logger.error("[groups/transfer_owner] 数据库连接失败")
            return JSONResponse({
                "code": 500,
                "message": "数据库连接失败"
            }, status_code=500)
        print("[groups/transfer_owner] 数据库连接成功")
        
        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 1. 检查群组是否存在
            print(f"[groups/transfer_owner] 检查群组 {group_id} 是否存在...")
            cursor.execute("SELECT group_id, group_name, member_num, owner_identifier FROM `groups` WHERE group_id = %s", (group_id,))
            group_info = cursor.fetchone()
            
            if not group_info:
                print(f"[groups/transfer_owner] 错误: 群组 {group_id} 不存在")
                return JSONResponse({
                    "code": 404,
                    "message": "群组不存在"
                }, status_code=404)
            
            print(f"[groups/transfer_owner] 群组信息: {group_info}")
            group_name = group_info.get('group_name', '')
            old_owner_identifier = group_info.get('owner_identifier', '')
            print(f"[groups/transfer_owner] 当前群组的 owner_identifier: {old_owner_identifier}")
            print(f"[groups/transfer_owner] 原群主ID (old_owner_id): {old_owner_id}")
            print(f"[groups/transfer_owner] 新群主ID (new_owner_id): {new_owner_id}")
            
            # 2. 检查原群主是否是群主
            print(f"[groups/transfer_owner] 检查用户 {old_owner_id} 是否是群组 {group_id} 的群主...")
            cursor.execute(
                "SELECT group_id, user_id, user_name, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, old_owner_id)
            )
            old_owner_info = cursor.fetchone()
            
            if not old_owner_info:
                print(f"[groups/transfer_owner] 错误: 用户 {old_owner_id} 不在群组 {group_id} 中")
                return JSONResponse({
                    "code": 404,
                    "message": "原群主不是该群组的成员"
                }, status_code=404)
            
            old_owner_role = old_owner_info.get('self_role', 200)
            if old_owner_role != 400:
                print(f"[groups/transfer_owner] 错误: 用户 {old_owner_id} 不是群主（当前角色: {old_owner_role}）")
                return JSONResponse({
                    "code": 403,
                    "message": "原群主不是群主，无权转让"
                }, status_code=403)
            
            print(f"[groups/transfer_owner] 原群主信息: {old_owner_info}")
            
            # 3. 检查新群主是否是群组成员
            print(f"[groups/transfer_owner] 检查用户 {new_owner_id} 是否在群组 {group_id} 中...")
            cursor.execute(
                "SELECT group_id, user_id, user_name, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, new_owner_id)
            )
            new_owner_info = cursor.fetchone()
            
            if not new_owner_info:
                print(f"[groups/transfer_owner] 错误: 用户 {new_owner_id} 不在群组 {group_id} 中")
                return JSONResponse({
                    "code": 404,
                    "message": "新群主不是该群组的成员"
                }, status_code=404)
            
            print(f"[groups/transfer_owner] 新群主信息: {new_owner_info}")
            new_owner_name = new_owner_info.get('user_name', '')
            
            # 4. 将新群主设置为群主（self_role = 400）
            print(f"[groups/transfer_owner] ========== 步骤4: 将新群主设置为群主 ==========")
            print(f"[groups/transfer_owner] 将用户 {new_owner_id} 设置为群主 (self_role = 400)...")
            sql_update_role = "UPDATE `group_members` SET self_role = %s WHERE group_id = %s AND user_id = %s"
            params_update_role = (400, group_id, new_owner_id)
            print(f"[groups/transfer_owner] 执行SQL: {sql_update_role}")
            print(f"[groups/transfer_owner] SQL参数: {params_update_role}")
            cursor.execute(sql_update_role, params_update_role)
            update_rows = cursor.rowcount
            print(f"[groups/transfer_owner] 更新新群主角色完成, 影响行数: {update_rows}")
            if update_rows > 0:
                print(f"[groups/transfer_owner] ✓ 成功将用户 {new_owner_id} 的角色更新为群主 (self_role=400)")
            else:
                print(f"[groups/transfer_owner] ✗ 警告: 更新新群主角色操作未影响任何行")
                connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": "设置新群主失败"
                }, status_code=500)
            
            # 5. 删除原群主（从群组中移除）
            print(f"[groups/transfer_owner] ========== 步骤5: 删除原群主 ==========")
            print(f"[groups/transfer_owner] 从群组 {group_id} 中删除原群主 {old_owner_id}...")
            sql_delete_owner = "DELETE FROM `group_members` WHERE group_id = %s AND user_id = %s"
            params_delete_owner = (group_id, old_owner_id)
            print(f"[groups/transfer_owner] 执行SQL: {sql_delete_owner}")
            print(f"[groups/transfer_owner] SQL参数: {params_delete_owner}")
            cursor.execute(sql_delete_owner, params_delete_owner)
            delete_rows = cursor.rowcount
            print(f"[groups/transfer_owner] 删除原群主完成, 影响行数: {delete_rows}")
            if delete_rows > 0:
                print(f"[groups/transfer_owner] ✓ 成功从群组中删除原群主 {old_owner_id}")
            else:
                print(f"[groups/transfer_owner] ✗ 警告: 删除原群主操作未影响任何行")
                connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": "删除原群主失败"
                }, status_code=500)
            
            # 6. 更新群组的 owner_identifier 字段为新群主ID
            print(f"[groups/transfer_owner] ========== 步骤6: 更新 groups 表的 owner_identifier 字段 ==========")
            print(f"[groups/transfer_owner] 更新前 - 群组 {group_id} 的 owner_identifier: {old_owner_identifier}")
            print(f"[groups/transfer_owner] 更新后 - 群组 {group_id} 的 owner_identifier 将设置为: {new_owner_id}")
            sql_update_owner = "UPDATE `groups` SET owner_identifier = %s WHERE group_id = %s"
            params_update_owner = (new_owner_id, group_id)
            print(f"[groups/transfer_owner] 执行SQL: {sql_update_owner}")
            print(f"[groups/transfer_owner] SQL参数: {params_update_owner}")
            cursor.execute(sql_update_owner, params_update_owner)
            update_owner_rows = cursor.rowcount
            print(f"[groups/transfer_owner] 更新 owner_identifier 完成, 影响行数: {update_owner_rows}")
            
            if update_owner_rows == 0:
                print(f"[groups/transfer_owner] ✗ 警告: 更新 owner_identifier 操作未影响任何行")
                connection.rollback()
                return JSONResponse({
                    "code": 500,
                    "message": "更新群主标识失败"
                }, status_code=500)
            
            # 验证更新是否成功
            print(f"[groups/transfer_owner] 验证更新结果: 查询更新后的 owner_identifier...")
            cursor.execute("SELECT owner_identifier FROM `groups` WHERE group_id = %s", (group_id,))
            verify_result = cursor.fetchone()
            if verify_result:
                updated_owner_identifier = verify_result.get('owner_identifier', '')
                print(f"[groups/transfer_owner] 验证结果 - 当前群组 {group_id} 的 owner_identifier: {updated_owner_identifier}")
                if updated_owner_identifier == new_owner_id:
                    print(f"[groups/transfer_owner] ✓ 成功: owner_identifier 已更新为新群主ID {new_owner_id}")
                else:
                    print(f"[groups/transfer_owner] ✗ 错误: owner_identifier 更新失败，期望值: {new_owner_id}, 实际值: {updated_owner_identifier}")
            else:
                print(f"[groups/transfer_owner] ✗ 错误: 无法查询到群组信息")
            
            # 7. 更新群组的成员数量（减1，因为原群主退出了）
            print(f"[groups/transfer_owner] ========== 步骤7: 更新群组成员数量 ==========")
            current_member_num = group_info.get('member_num', 0)
            print(f"[groups/transfer_owner] 更新前 - 群组 {group_id} 的成员数量: {current_member_num}")
            # 使用 CASE 语句避免 UNSIGNED 类型溢出问题
            # 当 member_num 为 0 时，member_num - 1 会导致 UNSIGNED 溢出错误
            sql_update_member_num = "UPDATE `groups` SET member_num = CASE WHEN member_num > 0 THEN member_num - 1 ELSE 0 END WHERE group_id = %s"
            params_update_member_num = (group_id,)
            print(f"[groups/transfer_owner] 执行SQL: {sql_update_member_num}")
            print(f"[groups/transfer_owner] SQL参数: {params_update_member_num}")
            cursor.execute(sql_update_member_num, params_update_member_num)
            update_member_num_rows = cursor.rowcount
            print(f"[groups/transfer_owner] 更新成员数量完成, 影响行数: {update_member_num_rows}")
            
            # 验证成员数量更新
            cursor.execute("SELECT member_num FROM `groups` WHERE group_id = %s", (group_id,))
            verify_member_result = cursor.fetchone()
            if verify_member_result:
                updated_member_num = verify_member_result.get('member_num', 0)
                print(f"[groups/transfer_owner] 更新后 - 群组 {group_id} 的成员数量: {updated_member_num}")
                print(f"[groups/transfer_owner] ✓ 成员数量已更新 (从 {current_member_num} 减少到 {updated_member_num})")
            
            # 提交事务
            print(f"[groups/transfer_owner] ========== 步骤8: 提交事务 ==========")
            connection.commit()
            print(f"[groups/transfer_owner] ✓ 事务提交成功")
            print(f"[groups/transfer_owner] ========== 转让群主操作完成 ==========")
            print(f"[groups/transfer_owner] 总结:")
            print(f"[groups/transfer_owner]   - 群组ID: {group_id}")
            print(f"[groups/transfer_owner]   - 原群主ID: {old_owner_id}")
            print(f"[groups/transfer_owner]   - 新群主ID: {new_owner_id}")
            print(f"[groups/transfer_owner]   - owner_identifier 已从 {old_owner_identifier} 更新为 {new_owner_id}")
            
            result = {
                "code": 200,
                "message": "成功转让群主",
                "data": {
                    "group_id": group_id,
                    "group_name": group_name,
                    "old_owner_id": old_owner_id,
                    "new_owner_id": new_owner_id,
                    "new_owner_name": new_owner_name
                }
            }
            
            print(f"[groups/transfer_owner] 返回结果: {result}")
            print("=" * 80)
            
            return JSONResponse(result, status_code=200)
            
        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/transfer_owner] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/transfer_owner] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/transfer_owner] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"数据库操作失败: {str(e)}"
            }, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"转让群主时发生异常: {e}"
            print(f"[groups/transfer_owner] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/transfer_owner] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/transfer_owner] {error_msg}\n{traceback_str}")
            return JSONResponse({
                "code": 500,
                "message": f"操作失败: {str(e)}"
            }, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/transfer_owner] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/transfer_owner] 数据库连接已关闭")
                app_logger.info("[groups/transfer_owner] Database connection closed after transfer owner attempt.")
    
    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/transfer_owner] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/transfer_owner] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/transfer_owner] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "code": 400,
            "message": "请求数据格式错误"
        }, status_code=400)
    finally:
        print("=" * 80)

@app.get("/groups/members")
def get_group_members_by_group_id(
    group_id: str = Query(..., description="群组ID，对应groups表的group_id")
):
    """
    根据 group_id 从 group_members 表获取群成员信息
    """
    print("=" * 80)
    print("[groups/members] 收到查询群成员请求")
    print(f"[groups/members] 请求参数 - group_id: {group_id}")
    
    if not group_id:
        print("[groups/members] 错误: 缺少群组ID")
        return JSONResponse({
            "data": {
                "message": "缺少群组ID",
                "code": 400
            }
        }, status_code=400)

    print("[groups/members] 开始连接数据库...")
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        print("[groups/members] 错误: 数据库连接失败")
        app_logger.error(f"[groups/members] 数据库连接失败 for group_id={group_id}")
        return JSONResponse({
            "data": {
                "message": "数据库连接失败",
                "code": 500
            }
        }, status_code=500)
    print("[groups/members] 数据库连接成功")

    cursor = None
    try:
        import time
        start_time = time.time()
        
        cursor = connection.cursor(dictionary=True)
        
        # 查询该群组的所有成员信息
        sql = """
            SELECT 
                gm.group_id,
                gm.user_id,
                gm.user_name,
                gm.self_role,
                gm.join_time,
                gm.msg_flag,
                gm.self_msg_flag,
                gm.readed_seq,
                gm.unread_num,
                gm.is_voice_enabled
            FROM `group_members` gm
            WHERE gm.group_id = %s
            ORDER BY gm.join_time ASC
        """
        print(f"[groups/members] 执行SQL查询: {sql}")
        print(f"[groups/members] 查询参数: group_id={group_id}")
        app_logger.info(f"[groups/members] 开始查询群组成员: group_id={group_id}")
        
        query_start = time.time()
        cursor.execute(sql, (group_id,))
        members = cursor.fetchall()
        query_time = time.time() - query_start
        
        print(f"[groups/members] 查询完成，耗时: {query_time:.3f}秒")
        print(f"[groups/members] 查询结果: 找到 {len(members)} 个成员")
        app_logger.info(f"[groups/members] 查询完成: group_id={group_id}, member_count={len(members)}, query_time={query_time:.3f}s")
        
        # 统计成员角色分布
        role_stats = {}
        for member in members:
            role = member.get('self_role', 200)
            role_name = {200: "普通成员", 300: "管理员", 400: "群主"}.get(role, f"未知角色({role})")
            role_stats[role_name] = role_stats.get(role_name, 0) + 1
        
        print(f"[groups/members] 成员角色统计: {role_stats}")
        app_logger.info(f"[groups/members] 成员角色统计: group_id={group_id}, stats={role_stats}")
        
        # 转换 datetime 为字符串
        for idx, member in enumerate(members):
            user_id = member.get('user_id')
            user_name = member.get('user_name')
            self_role = member.get('self_role')
            role_name = {200: "普通成员", 300: "管理员", 400: "群主"}.get(self_role, f"未知({self_role})")
            
            print(f"[groups/members] 处理第 {idx+1}/{len(members)} 个成员: user_id={user_id}, user_name={user_name}, role={role_name}")
            
            for key, value in member.items():
                if isinstance(value, datetime.datetime):
                    old_value = value
                    member[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[groups/members]   转换时间字段 {key}: {old_value} -> {member[key]}")
        
        total_time = time.time() - start_time
        print(f"[groups/members] 数据处理完成，总耗时: {total_time:.3f}秒")
        
        result = {
            "data": {
                "message": "查询成功",
                "code": 200,
                "group_id": group_id,
                "members": members,
                "member_count": len(members),
                "role_stats": role_stats
            }
        }
        
        print(f"[groups/members] 返回结果: group_id={group_id}, member_count={len(members)}, role_stats={role_stats}")
        print(f"[groups/members] 总耗时: {total_time:.3f}秒")
        app_logger.info(f"[groups/members] 查询成功: group_id={group_id}, member_count={len(members)}, total_time={total_time:.3f}s")
        print("=" * 80)
        
        return JSONResponse(result, status_code=200)

    except mysql.connector.Error as e:
        error_msg = f"查询群成员数据库错误: {e}"
        error_code = e.errno if hasattr(e, 'errno') else None
        print(f"[groups/members] {error_msg}")
        print(f"[groups/members] MySQL错误代码: {error_code}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/members] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/members] 数据库错误: group_id={group_id}, error={error_msg}, errno={error_code}\n{traceback_str}")
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500
            }
        }, status_code=500)
    except Exception as e:
        error_msg = f"查询群成员时发生异常: {e}"
        error_type = type(e).__name__
        print(f"[groups/members] {error_msg}")
        print(f"[groups/members] 异常类型: {error_type}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/members] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/members] 未知异常: group_id={group_id}, error_type={error_type}, error={error_msg}\n{traceback_str}")
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500
            }
        }, status_code=500)
    finally:
        if cursor:
            cursor.close()
            print("[groups/members] 游标已关闭")
        if connection and connection.is_connected():
            connection.close()
            print("[groups/members] 数据库连接已关闭")
            app_logger.info(f"[groups/members] Database connection closed after get_group_members_by_group_id attempt for group_id={group_id}.")

@app.get("/group/members")
def get_group_members(
    unique_group_id: str = Query(..., description="群唯一ID")
):
    """
    根据 unique_group_id 查询群主和所有成员的 id + name
    """
    if not unique_group_id:
        return JSONResponse({
            "data": {
                "message": "缺少群唯一ID",
                "code": 400
            }
        }, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({
            "data": {
                "message": "数据库连接失败",
                "code": 500
            }
        }, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # 1. 查群主ID
        sql_admin = """
            SELECT group_admin_id
            FROM ta_group
            WHERE unique_group_id = %s
        """
        cursor.execute(sql_admin, (unique_group_id,))
        group_info = cursor.fetchone()

        if not group_info:
            return JSONResponse({
                "data": {
                    "message": "群不存在",
                    "code": 404
                }
            }, status_code=404)

        group_admin_id = group_info.get("group_admin_id")

        members_data = []

        # 2. 查群主姓名（从 ta_teacher）
        if group_admin_id:
            sql_teacher = """
                SELECT teacher_unique_id, name
                FROM ta_teacher
                WHERE teacher_unique_id = %s
            """
            cursor.execute(sql_teacher, (group_admin_id,))
            teacher_info = cursor.fetchone()
            if teacher_info:
                members_data.append({
                    "id": teacher_info.get("teacher_unique_id"),
                    "name": teacher_info.get("name"),
                    "role": "群主"
                })

        # 3. 查群成员（从 ta_group_member_relation）
        sql_member = """
            SELECT unique_member_id, member_name
            FROM ta_group_member_relation
            WHERE unique_group_id = %s
        """
        cursor.execute(sql_member, (unique_group_id,))
        member_infos = cursor.fetchall()

        for m in member_infos:
            members_data.append({
                "id": m.get("unique_member_id"),
                "name": m.get("member_name"),
                "role": "成员"
            })

        return JSONResponse({
            "data": {
                "message": "查询成功",
                "code": 200,
                "members": members_data
            }
        }, status_code=200)

    except mysql.connector.Error as e:
        app_logger.error(f"查询错误: {e}")
        return JSONResponse({
            "data": {
                "message": "查询失败",
                "code": 500
            }
        }, status_code=500)

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after get_group_members attempt for {unique_group_id}.")

@app.post("/updateGroupInfo")
async def updateGroupInfo(request: Request):
    data = await request.json()
    unique_group_id = data.get('unique_group_id')
    avatar = data.get('avatar')

    if not unique_group_id or not avatar:
        app_logger.warning("UpdateGroupInfo failed: Missing unique_group_id or avatar.")
        return JSONResponse(
            {'data': {'message': '群ID和头像必须提供', 'code': 400}},
            status_code=400
        )

    # 数据库连接
    connection = get_db_connection()
    if connection is None:
        app_logger.error("UpdateGroupInfo failed: Database connection error.")
        return JSONResponse(
            {'data': {'message': '数据库连接失败', 'code': 500}},
            status_code=500
        )

    # 保存头像到服务器文件系统
    try:
        avatar_bytes = base64.b64decode(avatar)
    except Exception as e:
        app_logger.error(f"Base64 decode error for unique_group_id={unique_group_id}: {e}")
        return JSONResponse(
            {'data': {'message': '头像数据解析失败', 'code': 400}},
            status_code=400
        )

    filename = f"{unique_group_id}_.png"
    file_path = os.path.join(IMAGE_DIR, filename)
    try:
        with open(file_path, "wb") as f:
            f.write(avatar_bytes)
    except Exception as e:
        app_logger.error(f"Error writing avatar file {file_path}: {e}")
        return JSONResponse(
            {'data': {'message': '头像文件写入失败', 'code': 500}},
            status_code=500
        )

    # 更新数据库记录
    cursor = None
    try:
        update_query = """
            UPDATE ta_group
            SET headImage_path = %s
            WHERE unique_group_id = %s
        """
        cursor = connection.cursor(dictionary=True)
        cursor.execute(update_query, (file_path, unique_group_id))
        connection.commit()
        cursor.close()

        app_logger.info(f"Updated group avatar for {unique_group_id} -> {file_path}")
        return JSONResponse({'data': {'message': '更新成功', 'code': 200}})
    except Error as e:
        app_logger.error(f"Database error during updateGroupInfo for {unique_group_id}: {e}")
        return JSONResponse({'data': {'message': '更新失败', 'code': 500}}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after updating group info for {unique_group_id}.")

@app.post("/groups/sync")
async def sync_groups(request: Request):
    """
    同步腾讯群组数据到本地数据库
    接收客户端发送的群组列表，插入到 groups 和 group_members 表
    """
    print("=" * 80)
    print("[groups/sync] 收到同步请求")
    try:
        data = await request.json()
        print(f"[groups/sync] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        groups = data.get('groups', [])
        user_id = data.get('user_id')
        # 客户端发送的字段名：classid 和 schoolid（不再是 class_id 和 school_id）
        classid = data.get('classid')  # 从请求中获取 classid
        schoolid = data.get('schoolid')  # 从请求中获取 schoolid
        print(f"[groups/sync] 解析结果 - user_id: {user_id}, groups数量: {len(groups)}, classid: {classid}, schoolid: {schoolid}")
        
        if not groups:
            print("[groups/sync] 错误: 没有群组数据")
            return JSONResponse({
                'data': {
                    'message': '没有群组数据需要同步',
                    'code': 400
                }
            }, status_code=400)
        
        if not user_id:
            print("[groups/sync] 错误: 缺少 user_id")
            return JSONResponse({
                'data': {
                    'message': '缺少 user_id 参数',
                    'code': 400
                }
            }, status_code=400)
        
        # 数据库连接
        print("[groups/sync] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/sync] 错误: 数据库连接失败")
            app_logger.error("Database connection error in /groups/sync API.")
            return JSONResponse({
                'data': {
                    'message': '数据库连接失败',
                    'code': 500
                }
            }, status_code=500)
        print("[groups/sync] 数据库连接成功")
        
        cursor = None
        try:
            cursor = connection.cursor()
            success_count = 0
            error_count = 0
            
            # 检查表是否存在
            print("[groups/sync] 检查表是否存在...")
            cursor.execute("SHOW TABLES LIKE 'groups'")
            groups_table_exists = cursor.fetchone()
            cursor.execute("SHOW TABLES LIKE 'group_members'")
            group_members_table_exists = cursor.fetchone()
            print(f"[groups/sync] groups表存在: {groups_table_exists is not None}, group_members表存在: {group_members_table_exists is not None}")
            
            # 检查表结构
            if groups_table_exists:
                print("[groups/sync] 检查 groups 表结构...")
                cursor.execute("DESCRIBE `groups`")
                groups_columns = cursor.fetchall()
                print(f"[groups/sync] groups 表字段信息:")
                for col in groups_columns:
                    print(f"  {col}")
            
            if group_members_table_exists:
                print("[groups/sync] 检查 group_members 表结构...")
                cursor.execute("DESCRIBE `group_members`")
                group_members_columns = cursor.fetchall()
                print(f"[groups/sync] group_members 表字段信息:")
                for col in group_members_columns:
                    print(f"  {col}")
            
            # 遍历每个群组
            for idx, group in enumerate(groups):
                try:
                    group_id = group.get('group_id')
                    print(f"[groups/sync] 处理第 {idx+1}/{len(groups)} 个群组, group_id: {group_id}")
                    
                    # 检查群组是否已存在
                    print(f"[groups/sync] 检查群组 {group_id} 是否已存在...")
                    cursor.execute("SELECT group_id FROM `groups` WHERE group_id = %s", (group_id,))
                    group_exists = cursor.fetchone()
                    print(f"[groups/sync] 群组 {group_id} 已存在: {group_exists is not None}")
                    
                    # 处理时间戳转换函数（在循环外定义，避免重复定义）
                    def timestamp_to_datetime(ts):
                        if ts is None or ts == 0:
                            return None
                        try:
                            # 如果是毫秒级时间戳，转换为秒
                            if ts > 2147483647:  # 2038-01-19 03:14:07 的秒级时间戳
                                ts = int(ts / 1000)
                            else:
                                ts = int(ts)
                            
                            # 转换为 datetime 对象
                            dt = datetime.datetime.fromtimestamp(ts)
                            # 格式化为 MySQL DATETIME 格式
                            return dt.strftime('%Y-%m-%d %H:%M:%S')
                        except (ValueError, OSError) as e:
                            print(f"[groups/sync] 警告: 时间戳 {ts} 转换失败: {e}，设置为 NULL")
                            return None
                    
                    # 插入或更新 groups 表
                    if group_exists:
                        print(f"[groups/sync] 更新群组 {group_id} 的信息...")
                        # 转换时间戳
                        create_time_dt = timestamp_to_datetime(group.get('create_time'))
                        last_msg_time_dt = timestamp_to_datetime(group.get('last_msg_time'))
                        last_info_time_dt = timestamp_to_datetime(group.get('last_info_time'))
                        
                        # 更新群组信息
                        # 优先使用群组数据中的 classid 和 schoolid，如果没有则使用请求级别的
                        # 注意：客户端发送的字段名是 classid 和 schoolid（不是 class_id 和 school_id）
                        # 如果字段为空，则不更新数据库对应的字段
                        group_classid = group.get('classid') or classid
                        group_schoolid = group.get('schoolid') or schoolid
                        
                        # 检查值是否为空（None、空字符串、空值）
                        def is_empty(value):
                            return value is None or value == '' or (isinstance(value, str) and value.strip() == '')
                        
                        # 构建 UPDATE SQL，只更新非空字段
                        update_fields = [
                            "group_name = %s", "group_type = %s", "face_url = %s", "detail_face_url = %s",
                            "create_time = %s", "max_member_num = %s",
                            "member_num = %s", "introduction = %s", "notification = %s", "searchable = %s",
                            "visible = %s", "add_option = %s", "is_shutup_all = %s", "next_msg_seq = %s",
                            "latest_seq = %s", "last_msg_time = %s", "last_info_time = %s",
                            "info_seq = %s", "detail_info_seq = %s", "detail_group_id = %s",
                            "detail_group_name = %s", "detail_group_type = %s", "detail_is_shutup_all = %s",
                            "online_member_num = %s"
                        ]
                        update_params = [
                            group.get('group_name'),
                            group.get('group_type'),
                            group.get('face_url'),
                            group.get('detail_face_url'),
                            create_time_dt,
                            group.get('max_member_num'),
                            group.get('member_num'),
                            group.get('introduction'),
                            group.get('notification'),
                            group.get('searchable'),
                            group.get('visible'),
                            group.get('add_option'),
                            group.get('is_shutup_all'),
                            group.get('next_msg_seq'),
                            group.get('latest_seq'),
                            last_msg_time_dt,
                            last_info_time_dt,
                            group.get('info_seq'),
                            group.get('detail_info_seq'),
                            group.get('detail_group_id'),
                            group.get('detail_group_name'),
                            group.get('detail_group_type'),
                            group.get('detail_is_shutup_all'),
                            group.get('online_member_num')
                        ]
                        
                        # 只有当 owner_identifier 不为空时才添加到更新语句中
                        owner_identifier = group.get('owner_identifier')
                        if not is_empty(owner_identifier):
                            update_fields.append("owner_identifier = %s")
                            update_params.append(owner_identifier)
                            print(f"[groups/sync] 将更新 owner_identifier: {owner_identifier}")
                        else:
                            print(f"[groups/sync] owner_identifier 为空，跳过更新")
                        
                        # 只有当 classid 和 schoolid 不为空时才添加到更新语句中
                        if not is_empty(group_classid):
                            update_fields.append("classid = %s")
                            update_params.append(group_classid)
                            print(f"[groups/sync] 将更新 classid: {group_classid}")
                        else:
                            print(f"[groups/sync] classid 为空，跳过更新")
                        
                        if not is_empty(group_schoolid):
                            update_fields.append("schoolid = %s")
                            update_params.append(group_schoolid)
                            print(f"[groups/sync] 将更新 schoolid: {group_schoolid}")
                        else:
                            print(f"[groups/sync] schoolid 为空，跳过更新")
                        
                        # 处理 is_class_group 字段（如果客户端传过来则更新，否则使用默认值1）
                        is_class_group = group.get('is_class_group')
                        if is_class_group is not None:
                            update_fields.append("is_class_group = %s")
                            update_params.append(is_class_group)
                            print(f"[groups/sync] 将更新 is_class_group: {is_class_group}")
                        else:
                            print(f"[groups/sync] is_class_group 未提供，使用数据库默认值")
                        
                        update_params.append(group.get('group_id'))  # WHERE 条件参数
                        
                        update_group_sql = f"""
                            UPDATE `groups` SET
                                {', '.join(update_fields)}
                            WHERE group_id = %s
                        """
                        print(f"[groups/sync] 更新参数: {update_params}")
                        cursor.execute(update_group_sql, update_params)
                        affected_rows = cursor.rowcount
                        print(f"[groups/sync] 更新群组 {group_id} 完成, 影响行数: {affected_rows}")
                    else:
                        # 插入新群组
                        print(f"[groups/sync] 插入新群组 {group_id}...")
                        # 转换时间戳
                        create_time_dt = timestamp_to_datetime(group.get('create_time'))
                        last_msg_time_dt = timestamp_to_datetime(group.get('last_msg_time'))
                        last_info_time_dt = timestamp_to_datetime(group.get('last_info_time'))
                        
                        print(f"[groups/sync] 时间戳转换: create_time={create_time_dt}, last_msg_time={last_msg_time_dt}, last_info_time={last_info_time_dt}")
                        
                        # 优先使用群组数据中的 classid 和 schoolid，如果没有则使用请求级别的
                        # 注意：客户端发送的字段名是 classid 和 schoolid（不是 class_id 和 school_id）
                        # 如果字段为空，则插入 NULL
                        group_classid = group.get('classid') or classid
                        group_schoolid = group.get('schoolid') or schoolid
                        
                        # 检查值是否为空（None、空字符串、空值）
                        def is_empty(value):
                            return value is None or value == '' or (isinstance(value, str) and value.strip() == '')
                        
                        # 如果为空，则使用 None（插入 NULL）
                        if is_empty(group_classid):
                            group_classid = None
                            print(f"[groups/sync] classid 为空，将插入 NULL")
                        else:
                            print(f"[groups/sync] 将插入 classid: {group_classid}")
                        
                        if is_empty(group_schoolid):
                            group_schoolid = None
                            print(f"[groups/sync] schoolid 为空，将插入 NULL")
                        else:
                            print(f"[groups/sync] 将插入 schoolid: {group_schoolid}")
                        
                        insert_group_sql = """
                            INSERT INTO `groups` (
                                group_id, group_name, group_type, face_url, detail_face_url,
                                owner_identifier, create_time, max_member_num, member_num,
                                introduction, notification, searchable, visible, add_option,
                                is_shutup_all, next_msg_seq, latest_seq, last_msg_time,
                                last_info_time, info_seq, detail_info_seq, detail_group_id,
                                detail_group_name, detail_group_type, detail_is_shutup_all,
                                online_member_num, classid, schoolid, is_class_group
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """
                        insert_params = (
                            group.get('group_id'),
                            group.get('group_name'),
                            group.get('group_type'),
                            group.get('face_url'),
                            group.get('detail_face_url'),
                            group.get('owner_identifier'),
                            create_time_dt,  # 直接使用转换后的日期时间字符串
                            group.get('max_member_num'),
                            group.get('member_num'),
                            group.get('introduction'),
                            group.get('notification'),
                            group.get('searchable'),
                            group.get('visible'),
                            group.get('add_option'),
                            group.get('is_shutup_all'),
                            group.get('next_msg_seq'),
                            group.get('latest_seq'),
                            last_msg_time_dt,  # 直接使用转换后的日期时间字符串
                            last_info_time_dt,  # 直接使用转换后的日期时间字符串
                            group.get('info_seq'),
                            group.get('detail_info_seq'),
                            group.get('detail_group_id'),
                            group.get('detail_group_name'),
                            group.get('detail_group_type'),
                            group.get('detail_is_shutup_all'),
                            group.get('online_member_num'),
                            group_classid,  # 如果为空则为 None，插入 NULL
                            group_schoolid,  # 如果为空则为 None，插入 NULL
                            group.get('is_class_group', 1)  # 如果未提供则使用默认值1（班级群）
                        )
                        print(f"[groups/sync] 插入参数: {insert_params}")
                        cursor.execute(insert_group_sql, insert_params)
                        affected_rows = cursor.rowcount
                        lastrowid = cursor.lastrowid
                        print(f"[groups/sync] 插入群组 {group_id} 完成, 影响行数: {affected_rows}, lastrowid: {lastrowid}")
                    
                    # 处理群成员信息
                    # 1. 优先处理 member_info（群主，必须存在）
                    # 2. 然后处理 members 数组（管理员和其他成员）
                    members_list = group.get('members', [])
                    member_info = group.get('member_info')
                    print(f"[groups/sync] 群组 {group_id} 的成员信息: member_info={member_info is not None}, members数组={len(members_list)}个成员")
                    
                    # 记录已处理的成员ID，避免重复插入
                    processed_member_ids = set()
                    
                    # 第一步：处理 member_info（群主，必须存在）
                    if member_info:
                        member_user_id = member_info.get('user_id')
                        if member_user_id:
                            print(f"[groups/sync] 处理 member_info（群主）: user_id={member_user_id}")
                            member_user_name = member_info.get('user_name', '')
                            member_self_role = member_info.get('self_role', 400)  # 默认群主
                            member_join_time = timestamp_to_datetime(member_info.get('join_time')) or timestamp_to_datetime(group.get('create_time'))
                            if not member_join_time:
                                member_join_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            
                            # 检查成员是否已存在
                            cursor.execute(
                                "SELECT group_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                                (group_id, member_user_id)
                            )
                            member_exists = cursor.fetchone()
                            
                            if member_exists:
                                # 更新群主信息
                                print(f"[groups/sync] 更新群主 group_id={group_id}, user_id={member_user_id}, self_role={member_self_role}")
                                update_member_sql = """
                                    UPDATE `group_members` SET
                                        user_name = %s, self_role = %s, join_time = %s,
                                        msg_flag = %s, self_msg_flag = %s, readed_seq = %s, unread_num = %s
                                    WHERE group_id = %s AND user_id = %s
                                """
                                update_params = (
                                    member_user_name if member_user_name else None,
                                    member_self_role,
                                    member_join_time,
                                    member_info.get('msg_flag', 0),
                                    member_info.get('self_msg_flag', 0),
                                    member_info.get('readed_seq', 0),
                                    member_info.get('unread_num', 0),
                                    group_id,
                                    member_user_id
                                )
                                cursor.execute(update_member_sql, update_params)
                            else:
                                # 插入群主
                                print(f"[groups/sync] 插入群主 group_id={group_id}, user_id={member_user_id}, self_role={member_self_role}")
                                insert_member_sql = """
                                    INSERT INTO `group_members` (
                                        group_id, user_id, user_name, self_role, join_time, msg_flag,
                                        self_msg_flag, readed_seq, unread_num
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                """
                                insert_params = (
                                    group_id,
                                    member_user_id,
                                    member_user_name if member_user_name else None,
                                    member_self_role,
                                    member_join_time,
                                    member_info.get('msg_flag', 0),
                                    member_info.get('self_msg_flag', 0),
                                    member_info.get('readed_seq', 0),
                                    member_info.get('unread_num', 0)
                                )
                                cursor.execute(insert_member_sql, insert_params)
                            
                            processed_member_ids.add(member_user_id)
                        else:
                            print(f"[groups/sync] 警告: member_info 缺少 user_id，跳过")
                    else:
                        print(f"[groups/sync] 警告: 缺少 member_info（群主信息），这是必需的")
                    
                    # 第二步：处理 members 数组（管理员和其他成员）
                    if members_list:
                        print(f"[groups/sync] 处理 members 数组，共 {len(members_list)} 个成员")
                        for member_item in members_list:
                            # 兼容新旧字段名
                            member_user_id = member_item.get('user_id') or member_item.get('unique_member_id')
                            member_user_name = member_item.get('user_name') or member_item.get('member_name', '')
                            
                            if not member_user_id:
                                print(f"[groups/sync] 警告: 成员信息缺少 user_id/unique_member_id，跳过")
                                continue
                            
                            # 如果该成员已经在 member_info 中处理过（群主），跳过避免重复
                            if member_user_id in processed_member_ids:
                                print(f"[groups/sync] 跳过已处理的成员（群主）: user_id={member_user_id}")
                                continue
                            
                            # 处理 self_role：优先使用 self_role，否则从 group_role 转换
                            if 'self_role' in member_item:
                                member_self_role = member_item.get('self_role')
                            else:
                                # 从 group_role 转换：400=群主，300=管理员，其他=普通成员(200)
                                group_role = member_item.get('group_role')
                                if group_role == 400:
                                    member_self_role = 400  # 群主（但应该已经在 member_info 中处理）
                                elif group_role == 300:
                                    member_self_role = 300  # 管理员（保持300）
                                else:
                                    member_self_role = 200  # 普通成员
                            
                            # 处理 join_time：支持时间戳或直接使用当前时间
                            member_join_time = timestamp_to_datetime(member_item.get('join_time')) or timestamp_to_datetime(group.get('create_time'))
                            if not member_join_time:
                                member_join_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            
                            # 检查成员是否已存在
                            cursor.execute(
                                "SELECT group_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                                (group_id, member_user_id)
                            )
                            member_exists = cursor.fetchone()
                            
                            if member_exists:
                                # 更新成员信息
                                print(f"[groups/sync] 更新成员 group_id={group_id}, user_id={member_user_id}, self_role={member_self_role}")
                                update_member_sql = """
                                    UPDATE `group_members` SET
                                        user_name = %s, self_role = %s, join_time = %s,
                                        msg_flag = %s, self_msg_flag = %s, readed_seq = %s, unread_num = %s
                                    WHERE group_id = %s AND user_id = %s
                                """
                                update_params = (
                                    member_user_name if member_user_name else None,
                                    member_self_role,
                                    member_join_time,
                                    member_item.get('msg_flag', 0),
                                    member_item.get('self_msg_flag', 0),
                                    member_item.get('readed_seq', 0),
                                    member_item.get('unread_num', 0),
                                    group_id,
                                    member_user_id
                                )
                                cursor.execute(update_member_sql, update_params)
                            else:
                                # 插入新成员
                                print(f"[groups/sync] 插入成员 group_id={group_id}, user_id={member_user_id}, self_role={member_self_role}")
                                insert_member_sql = """
                                    INSERT INTO `group_members` (
                                        group_id, user_id, user_name, self_role, join_time, msg_flag,
                                        self_msg_flag, readed_seq, unread_num
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                """
                                insert_params = (
                                    group_id,
                                    member_user_id,
                                    member_user_name if member_user_name else None,
                                    member_self_role,
                                    member_join_time,
                                    member_item.get('msg_flag', 0),
                                    member_item.get('self_msg_flag', 0),
                                    member_item.get('readed_seq', 0),
                                    member_item.get('unread_num', 0)
                                )
                                cursor.execute(insert_member_sql, insert_params)
                            
                            processed_member_ids.add(member_user_id)
                    elif not member_info:
                        group_id = group.get('group_id')
                        member_user_id = member_info.get('user_id')
                        
                        # 检查成员是否已存在
                        print(f"[groups/sync] 检查成员 group_id={group_id}, user_id={member_user_id} 是否已存在...")
                        cursor.execute(
                            "SELECT group_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                            (group_id, member_user_id)
                        )
                        member_exists = cursor.fetchone()
                        print(f"[groups/sync] 成员已存在: {member_exists is not None}")
                        
                        if member_exists:
                            # 更新成员信息
                            print(f"[groups/sync] 更新成员信息 group_id={group_id}, user_id={member_user_id}...")
                            join_time_dt = timestamp_to_datetime(member_info.get('join_time'))
                            member_user_name = member_info.get('user_name')  # 获取成员名称
                            
                            # 检查值是否为空（None、空字符串、空值）
                            def is_empty(value):
                                return value is None or value == '' or (isinstance(value, str) and value.strip() == '')
                            
                            # 构建 UPDATE SQL，如果字段为空则不更新
                            update_fields = [
                                "self_role = %s", "join_time = %s", "msg_flag = %s",
                                "self_msg_flag = %s", "readed_seq = %s", "unread_num = %s"
                            ]
                            update_params = [
                                member_info.get('self_role'),
                                join_time_dt,
                                member_info.get('msg_flag'),
                                member_info.get('self_msg_flag'),
                                member_info.get('readed_seq'),
                                member_info.get('unread_num')
                            ]
                            
                            # 如果 user_name 不为空，则更新该字段；为空则跳过更新
                            if not is_empty(member_user_name):
                                update_fields.append("user_name = %s")
                                update_params.append(member_user_name)
                                print(f"[groups/sync] 将更新 user_name: {member_user_name}")
                            else:
                                print(f"[groups/sync] user_name 为空，跳过更新该字段")
                            
                            update_params.extend([group_id, member_user_id])  # WHERE 条件参数
                            
                            update_member_sql = f"""
                                UPDATE `group_members` SET
                                    {', '.join(update_fields)}
                                WHERE group_id = %s AND user_id = %s
                            """
                            update_member_params = tuple(update_params)
                            print(f"[groups/sync] 更新成员参数: {update_member_params}")
                            cursor.execute(update_member_sql, update_member_params)
                            affected_rows = cursor.rowcount
                            print(f"[groups/sync] 更新成员完成, 影响行数: {affected_rows}")
                        else:
                            # 插入新成员
                            print(f"[groups/sync] 插入新成员 group_id={group_id}, user_id={member_user_id}...")
                            join_time_dt = timestamp_to_datetime(member_info.get('join_time'))
                            member_user_name = member_info.get('user_name')  # 获取成员名称
                            
                            insert_member_sql = """
                                INSERT INTO `group_members` (
                                    group_id, user_id, user_name, self_role, join_time, msg_flag,
                                    self_msg_flag, readed_seq, unread_num
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                            """
                            insert_member_params = (
                                group_id,
                                member_user_id,
                                member_user_name,  # 如果为空则插入 NULL
                                member_info.get('self_role'),
                                join_time_dt,
                                member_info.get('msg_flag'),
                                member_info.get('self_msg_flag'),
                                member_info.get('readed_seq'),
                                member_info.get('unread_num')
                            )
                            print(f"[groups/sync] 插入成员参数: user_name={member_user_name}")
                            print(f"[groups/sync] 插入成员参数: {insert_member_params}")
                            cursor.execute(insert_member_sql, insert_member_params)
                            affected_rows = cursor.rowcount
                            lastrowid = cursor.lastrowid
                            print(f"[groups/sync] 插入成员完成, 影响行数: {affected_rows}, lastrowid: {lastrowid}")
                    else:
                        # 如果没有成员信息，从 owner_identifier 获取群主信息并插入
                        print(f"[groups/sync] 群组 {group_id} 没有成员信息，尝试从 owner_identifier 获取群主信息")
                        owner_identifier = group.get('owner_identifier')
                        if owner_identifier:
                            print(f"[groups/sync] 群组 {group_id} 的 owner_identifier: {owner_identifier}")
                            # 从 ta_teacher 表查询群主姓名
                            cursor.execute(
                                "SELECT name FROM ta_teacher WHERE teacher_unique_id = %s",
                                (owner_identifier,)
                            )
                            teacher_result = cursor.fetchone()
                            if teacher_result:
                                # groups/sync 接口使用普通游标，返回元组格式
                                teacher_name = teacher_result[0]
                                print(f"[groups/sync] 从 ta_teacher 表获取到群主姓名: {teacher_name}")
                                
                                # 检查该成员是否已存在
                                cursor.execute(
                                    "SELECT group_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                                    (group_id, owner_identifier)
                                )
                                member_exists = cursor.fetchone()
                                
                                if member_exists:
                                    # 更新群主信息（兼容已有的更新方法）
                                    print(f"[groups/sync] 更新群主信息 group_id={group_id}, user_id={owner_identifier}...")
                                    
                                    # 检查值是否为空（兼容已有的 is_empty 函数逻辑）
                                    def is_empty(value):
                                        return value is None or value == '' or (isinstance(value, str) and value.strip() == '')
                                    
                                    # 构建 UPDATE SQL，如果字段为空则不更新（兼容已有的更新逻辑）
                                    update_fields = [
                                        "self_role = %s"
                                    ]
                                    update_params = [
                                        400  # self_role (群主)
                                    ]
                                    
                                    # 如果 user_name 不为空，则更新该字段；为空则跳过更新（兼容已有的更新逻辑）
                                    if not is_empty(teacher_name):
                                        update_fields.append("user_name = %s")
                                        update_params.append(teacher_name)
                                        print(f"[groups/sync] 将更新 user_name: {teacher_name}")
                                    else:
                                        print(f"[groups/sync] user_name 为空，跳过更新该字段")
                                    
                                    update_params.extend([group_id, owner_identifier])  # WHERE 条件参数
                                    
                                    update_owner_sql = f"""
                                        UPDATE `group_members` SET
                                            {', '.join(update_fields)}
                                        WHERE group_id = %s AND user_id = %s
                                    """
                                    update_owner_params = tuple(update_params)
                                    print(f"[groups/sync] 更新群主参数: {update_owner_params}")
                                    cursor.execute(update_owner_sql, update_owner_params)
                                    affected_rows = cursor.rowcount
                                    print(f"[groups/sync] 更新群主完成, 影响行数: {affected_rows}")
                                else:
                                    # 插入群主信息到 group_members 表（兼容已有的插入方法）
                                    insert_owner_sql = """
                                        INSERT INTO `group_members` (
                                            group_id, user_id, user_name, self_role, join_time, msg_flag,
                                            self_msg_flag, readed_seq, unread_num
                                        ) VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                                        )
                                    """
                                    insert_owner_params = (
                                        group_id,
                                        owner_identifier,  # user_id
                                        teacher_name,  # user_name
                                        400,  # self_role (群主)
                                        None,  # join_time
                                        None,  # msg_flag
                                        None,  # self_msg_flag
                                        None,  # readed_seq
                                        None   # unread_num
                                    )
                                    print(f"[groups/sync] 插入群主信息: group_id={group_id}, user_id={owner_identifier}, user_name={teacher_name}, self_role=400")
                                    print(f"[groups/sync] 插入群主参数: {insert_owner_params}")
                                    cursor.execute(insert_owner_sql, insert_owner_params)
                                    affected_rows = cursor.rowcount
                                    lastrowid = cursor.lastrowid
                                    print(f"[groups/sync] 插入群主完成, 影响行数: {affected_rows}, lastrowid: {lastrowid}")
                            else:
                                print(f"[groups/sync] 警告: 在 ta_teacher 表中未找到 teacher_unique_id={owner_identifier} 的记录")
                        else:
                            print(f"[groups/sync] 群组 {group_id} 没有 owner_identifier 字段")
                    
                    success_count += 1
                    print(f"[groups/sync] 群组 {group_id} 处理成功")
                except Exception as e:
                    error_msg = f"处理群组 {group.get('group_id')} 时出错: {e}"
                    print(f"[groups/sync] {error_msg}")
                    import traceback
                    traceback_str = traceback.format_exc()
                    print(f"[groups/sync] 错误堆栈: {traceback_str}")
                    app_logger.error(f"{error_msg}\n{traceback_str}")
                    error_count += 1
                    continue
            
            # 提交事务
            print(f"[groups/sync] 准备提交事务, 成功: {success_count}, 失败: {error_count}")
            connection.commit()
            print(f"[groups/sync] 事务提交成功")
            
            app_logger.info(f"群组同步完成: 成功 {success_count} 个, 失败 {error_count} 个")
            print(f"[groups/sync] 群组同步完成: 成功 {success_count} 个, 失败 {error_count} 个")
            
            tencent_sync_summary = await notify_tencent_group_sync(user_id, groups)
            print(f"[groups/sync] 腾讯 REST API 同步结果: {tencent_sync_summary}")

            result = {
                'data': {
                    'message': '群组同步完成',
                    'code': 200,
                    'success_count': success_count,
                    'error_count': error_count,
                    'tencent_sync': tencent_sync_summary
                }
            }
            print(f"[groups/sync] 返回结果: {result}")
            return JSONResponse(result, status_code=200)
            
        except mysql.connector.Error as e:
            error_msg = f"数据库错误: {e}"
            print(f"[groups/sync] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/sync] 数据库错误堆栈: {traceback_str}")
            connection.rollback()
            print(f"[groups/sync] 事务已回滚")
            app_logger.error(f"{error_msg}\n{traceback_str}")
            return JSONResponse({
                'data': {
                    'message': f'数据库操作失败: {str(e)}',
                    'code': 500
                }
            }, status_code=500)
        except Exception as e:
            error_msg = f"同步群组时发生错误: {e}"
            print(f"[groups/sync] {error_msg}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/sync] 错误堆栈: {traceback_str}")
            connection.rollback()
            print(f"[groups/sync] 事务已回滚")
            app_logger.error(f"{error_msg}\n{traceback_str}")
            return JSONResponse({
                'data': {
                    'message': f'同步失败: {str(e)}',
                    'code': 500
                }
            }, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/sync] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/sync] 数据库连接已关闭")
                app_logger.info("Database connection closed after groups sync.")
    
    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/sync] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/sync] 解析错误堆栈: {traceback_str}")
        app_logger.error(f"{error_msg}\n{traceback_str}")
        return JSONResponse({
            'data': {
                'message': '请求数据格式错误',
                'code': 400
            }
        }, status_code=400)
    finally:
        print("=" * 80)

@app.get("/friends")
def get_friends(id_card: str = Query(..., description="教师身份证号")):
    """根据教师 id_card 查询关联朋友信息"""
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        app_logger.error("Database connection error in /friends API.")
        return JSONResponse({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }, status_code=500)

    results: List[Dict] = []
    try:
        # ① 查 teacher_unique_id
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT teacher_unique_id FROM ta_teacher WHERE id_card=%s", (id_card,))
            rows = cursor.fetchall()  # 保证取完数据
            app_logger.info(f"📌 Step1: ta_teacher for id_card={id_card} -> {rows}")
        if not rows:
            return {"friends": []}

        teacher_unique_id = rows[0]["teacher_unique_id"]

        # ② 查 ta_friend 获取 friendcode
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT friendcode FROM ta_friend WHERE teacher_unique_id=%s", (teacher_unique_id,))
            friend_rows = cursor.fetchall()
            app_logger.info(f"📌 Step2: ta_friend for teacher_unique_id={teacher_unique_id} -> {friend_rows}")
        if not friend_rows:
            return {"friends": []}

        # ③ 遍历每个 friendcode
        for fr in friend_rows:
            friendcode = fr["friendcode"]

            # 查 ta_teacher
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM ta_teacher WHERE teacher_unique_id=%s", (friendcode,))
                teacher_rows = cursor.fetchall()
                # app_logger.info(f"📌 Step3: ta_teacher for friendcode={friendcode} -> {teacher_rows}")
            if not teacher_rows:
                continue
            friend_teacher = teacher_rows[0]

            # 查 ta_user_details
            id_number = friend_teacher.get("id_card")
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM ta_user_details WHERE id_number=%s", (id_number,))
                user_rows = cursor.fetchall()
                # app_logger.info(f"📌 Step4: ta_user_details for id_number={id_number} -> {user_rows}")
            user_details = user_rows[0] if user_rows else None

            if user_details:
                avatar_path = user_details.get("avatar")
                if avatar_path:
                    local_avatar_file = resolve_local_avatar_file_path(avatar_path)
                    if local_avatar_file and os.path.exists(local_avatar_file):
                        try:
                            with open(local_avatar_file, "rb") as img:
                                user_details["avatar_base64"] = base64.b64encode(img.read()).decode("utf-8")
                        except Exception as e:
                            app_logger.error(f"读取图片失败 {local_avatar_file}: {e}")
                            user_details["avatar_base64"] = None
                    else:
                        user_details["avatar_base64"] = None
                else:
                    user_details["avatar_base64"] = None

            combined = {
                "teacher_info": friend_teacher,
                "user_details": user_details
            }
            # 打印组合后的数据
            # app_logger.info(f"📌 Step5: combined record -> {combined}")
            results.append({
                "teacher_info": friend_teacher,
                "user_details": user_details
            })
        app_logger.info(f"✅ Finished. Total friends found: {len(results)}")
        return {
            "count": len(results),
            "friends": results
        }

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed for id_card={id_card}")

# if __name__ == '__main__':
#     app_logger.info("Flask application starting...")
#     app.run(host="0.0.0.0", port=5000, debug=True)

#from datetime import datetime   # 注意这里！！！
def convert_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")

# ====== WebSocket 接口：聊天室 + 心跳 ======
# 创建群
 # data: { group_name, permission_level, headImage_path, group_type, nickname, owner_id, members: [{unique_member_id, member_name, group_role}] }
 #
async def create_group(data):
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        app_logger.error("Database connection error in /friends API.")
        return JSONResponse({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }, status_code=500)

    cursor = connection.cursor()
    unique_group_id = str(uuid.uuid4())

    try:
        cursor.execute(
            "INSERT INTO ta_group (permission_level, headImage_path, group_type, nickname, unique_group_id, group_admin_id, school_id, class_id, create_time)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
            (data.get('permission_level'),
             data.get('headImage_path'),
             data.get('group_type'),
             data.get('nickname'),
             unique_group_id,
             data.get('owner_id'),
             data.get('school_id'),
             data.get('class_id'))
        )

        for m in data['members']:
            cursor.execute(
                "INSERT INTO ta_group_member_relation (unique_member_id, unique_group_id, join_time, group_role, member_name)"
                " VALUES (%s,%s,NOW(),%s,%s)",
                (m['unique_member_id'], unique_group_id, m['group_role'], m['member_name'])
            )

        connection.commit()
        cursor.close()
        connection.close()

        # 给在线成员推送
        for m in data['members']:
            if m['unique_member_id'] in clients:
                await clients[m['unique_member_id']].send_text(json.dumps({
                    "type":"notify",
                    "message":f"你已加入群: {data['nickname']}",
                    "group_id": unique_group_id
                }))

        return {"code":200, "message":"群创建成功", "group_id":unique_group_id}

    except Exception as e:
        print(f"create_group错误: {e}")
        return {"code":500, "message":"群创建失败"}

 # 邀请成员加入群
 # data: { unique_group_id, group_name, new_members: [{unique_member_id, member_name, group_role}] }
 #
async def invite_members(data):
    conn = await get_db_connection()
    if conn is None:
        return {"code":500, "message":"数据库连接失败"}

    cursor = conn.cursor()
    try:
        for m in data['new_members']:
            cursor.execute(
                "INSERT INTO ta_group_member_relation (unique_member_id, unique_group_id, join_time, group_role, member_name)"
                " VALUES (%s,%s,NOW(),%s,%s)",
                (m['unique_member_id'], data['unique_group_id'], m['group_role'], m['member_name'])
            )

            if m['unique_member_id'] in clients:
                await clients[m['unique_member_id']].send_text(json.dumps({
                    "type":"notify",
                    "message":f"你被邀请加入群: {data['group_name']}",
                    "group_id": data['unique_group_id']
                }))

        conn.commit()
        cursor.close()
        conn.close()
        return {"code":200, "message":"成员邀请成功"}

    except Exception as e:
        print(f"invite_members错误: {e}")
        return {"code":500, "message":"成员邀请失败"}
    
def safe_del(user_id: str):
    conn = connections.pop(user_id, None)
    return conn

async def safe_send_text(ws: WebSocket, text: str):
    try:
        await ws.send_text(text)
        return True
    except Exception:
        return False

async def safe_send_bytes(ws: WebSocket, data: bytes):
    try:
        await ws.send_bytes(data)
        return True
    except Exception:
        return False

async def safe_close(ws: WebSocket, code: int = 1000, reason: str = ""):
    # 只在连接仍处于 CONNECTED 时尝试关闭，避免重复 close 报错
    try:
        if getattr(ws, "client_state", None) == WebSocketState.CONNECTED:
            await ws.close(code=code, reason=reason)
        return True
    except Exception:
        return False

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    current_online = len(connections)
    app_logger.info(f"[websocket] 即将接受连接 user_id={user_id}, 当前在线={current_online}")
    print(f"[websocket] 即将接受连接 user_id={user_id}, 当前在线={current_online}")
    await websocket.accept()
    connections[user_id] = {"ws": websocket, "last_heartbeat": time.time()}
    app_logger.info(f"[websocket] 用户 {user_id} 已连接，当前在线={len(connections)}")
    print(f"用户 {user_id} 已连接，当前在线={len(connections)}")

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        app_logger.error("Database connection error in /friends API.")
        print(f"[websocket][{user_id}] 数据库连接失败，立即关闭")
        return JSONResponse({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }, status_code=500)
    else:
        app_logger.info(f"[websocket] 数据库连接成功，user_id={user_id}")

    cursor = None
    try:
        # 查询条件改为：receiver_id = user_id 或 sender_id = user_id，并且 is_read = 0
        print(" xxx SELECT ta_notification")
        update_query = """
            SELECT *
            FROM ta_notification
            WHERE (receiver_id = %s OR sender_id = %s)
            AND is_read = 0;
        """
        cursor = connection.cursor(dictionary=True)
        cursor.execute(update_query, (user_id, user_id))
        unread_notifications = cursor.fetchall()

        if unread_notifications:
            await websocket.send_text(json.dumps({
                "type": "unread_notifications",
                "data": unread_notifications
            }, default=convert_datetime, ensure_ascii=False))
        
        # 查询所有课前准备（包含已读与未读）
        cursor.execute("""
            SELECT 
                cp.prepare_id, cp.group_id, cp.class_id, cp.school_id, cp.subject, cp.content, cp.date, cp.time,
                cp.sender_id, cp.sender_name, cp.created_at, g.group_name, cpr.is_read
            FROM class_preparation cp
            INNER JOIN class_preparation_receiver cpr ON cp.prepare_id = cpr.prepare_id
            LEFT JOIN `groups` g ON cp.group_id = g.group_id
            WHERE cpr.receiver_id = %s
            ORDER BY cp.created_at DESC
        """, (user_id,))
        preparation_rows = cursor.fetchall()

        if preparation_rows:
            preparation_payload: Dict[str, Any] = {
                "type": "prepare_class_history",
                "count": len(preparation_rows),
                "data": []
            }
            unread_updates: List[int] = []

            for prep in preparation_rows:
                message = {
                    "class_id": prep.get("class_id"),
                    "school_id": prep.get("school_id"),
                    "subject": prep.get("subject"),
                    "content": prep.get("content"),
                    "date": prep.get("date"),
                    "time": prep.get("time"),
                    "sender_id": prep.get("sender_id"),
                    "sender_name": prep.get("sender_name"),
                    "group_id": prep.get("group_id"),
                    "group_name": prep.get("group_name") or "",
                    "prepare_id": prep.get("prepare_id"),
                    "is_read": int(prep.get("is_read", 0)),
                    "created_at": convert_datetime(prep.get("created_at")) if prep.get("created_at") else None
                }
                preparation_payload["data"].append(message)

                if not prep.get("is_read"):
                    unread_updates.append(prep.get("prepare_id"))

            payload_str = json.dumps(preparation_payload, ensure_ascii=False)
            app_logger.info(f"[prepare_class] 用户 {user_id} 登录，推送课前准备数据: {payload_str}")
            print(f"[prepare_class] 登录推送课前准备数据: {payload_str}")
            await websocket.send_text(payload_str)

            if unread_updates:
                app_logger.info(f"[prepare_class] 标记 {len(unread_updates)} 条课前准备为已读，user_id={user_id}")
                for prep_id in unread_updates:
                    cursor.execute("""
                        UPDATE class_preparation_receiver
                        SET is_read = 1, read_at = NOW()
                        WHERE prepare_id = %s AND receiver_id = %s
                    """, (prep_id, user_id))
                connection.commit()

        async def handle_temp_room_creation(msg_data1: Dict[str, Any]):
            print(f"[temp_room] 创建请求 payload={msg_data1}")
            app_logger.info(f"[temp_room] 创建房间请求 - user_id={user_id}, payload={msg_data1}")
            
            try:
                local_cursor = connection.cursor(dictionary=True)

                owner_id = user_id
                invited_users = msg_data1.get('invited_users', []) or []
                if not isinstance(invited_users, list):
                    invited_users = [invited_users]

                group_id = msg_data1.get('group_id')
                if not group_id:
                    error_response = {
                        "type": "6",
                        "status": "error",
                        "message": "班级群唯一编号 group_id 不能为空"
                    }
                    error_response_json = json.dumps(error_response, ensure_ascii=False)
                    app_logger.warning(f"[temp_room] 创建房间失败 - group_id 为空, user_id={user_id}, 消息内容: {error_response_json}")
                    print(f"[temp_room] 返回创建房间失败消息给用户 {user_id}: {error_response_json}")
                    await websocket.send_text(error_response_json)
                    return

                # 检查用户是否已经在其他房间中
                existing_room = None
                for existing_group_id, existing_room_info in active_temp_rooms.items():
                    members = existing_room_info.get("members", [])
                    if user_id in members:
                        existing_room = existing_room_info
                        app_logger.warning(f"[temp_room] 用户 {user_id} 已在房间 {existing_group_id} 中，无法创建新房间")
                        print(f"[temp_room] 用户 {user_id} 已在房间 {existing_group_id} 中，无法创建新房间")
                        break
                
                if existing_room:
                    error_response = {
                        "type": "6",
                        "status": "error",
                        "message": f"您已在其他临时房间中（班级: {existing_room.get('group_id', '未知')}），请先离开该房间后再创建新房间"
                    }
                    error_response_json = json.dumps(error_response, ensure_ascii=False)
                    app_logger.warning(f"[temp_room] 创建房间失败 - 用户已在其他房间, user_id={user_id}, 消息内容: {error_response_json}")
                    print(f"[temp_room] 返回创建房间失败消息给用户 {user_id}: {error_response_json}")
                    await websocket.send_text(error_response_json)
                    return

                owner_name = msg_data1.get('owner_name', '') or ''
                owner_icon = msg_data1.get('owner_icon', '') or ''

                # 尝试从数据库获取创建者信息
                try:
                    if not owner_name or not owner_icon:
                        local_cursor.execute(
                            "SELECT name, icon FROM ta_teacher WHERE teacher_unique_id = %s",
                            (owner_id,)
                        )
                        owner_info = local_cursor.fetchone()
                        if owner_info:
                            if not owner_name:
                                owner_name = owner_info.get('name', '') or owner_name
                            if not owner_icon:
                                owner_icon = owner_info.get('icon', '') or owner_icon
                except Exception as db_error:
                    app_logger.error(f"[temp_room] 查询创建者信息失败 - user_id={user_id}, error={db_error}")
                    # 数据库查询失败不影响房间创建，继续使用传入的值

                # 生成唯一的房间ID和流名称
                # 客户端使用传统 SRS WebRTC API（/rtc/v1/publish/ 和 /rtc/v1/play/）
                room_id = str(uuid.uuid4())
                stream_name = f"room_{group_id}_{int(time.time())}"
                
                # 生成传统 WebRTC API 地址（推流和拉流）
                # 推流地址：/rtc/v1/publish/
                publish_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/publish/?app={SRS_APP}&stream={stream_name}"
                # 拉流地址：/rtc/v1/play/
                play_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/play/?app={SRS_APP}&stream={stream_name}"
                
                # 保留 WHIP/WHEP 地址用于向后兼容（但客户端主要使用上面的传统地址）
                whip_url = f"{SRS_BASE_URL}/rtc/v1/whip/?app={SRS_APP}&stream={stream_name}"
                whep_url = f"{SRS_BASE_URL}/rtc/v1/whep/?app={SRS_APP}&stream={stream_name}"
                
                app_logger.info(f"[temp_room] 生成流地址 - room_id={room_id}, stream_name={stream_name}, publish_url={publish_url}, play_url={play_url}")
                print(f"[temp_room] 生成流地址 - room_id={room_id}, stream_name={stream_name}, publish_url={publish_url}, play_url={play_url}")

                online_users: List[str] = []
                offline_users: List[str] = []

                # 通知被邀请的用户
                try:
                    for invited_user_id in invited_users:
                        target_conn = connections.get(invited_user_id)
                        if target_conn:
                            print(f"用户 {invited_user_id} 在线，发送拉流地址")
                            online_users.append(invited_user_id)
                            try:
                                invite_response = {
                                    "type": "6",
                                    "room_id": room_id,
                                    "owner_id": owner_id,
                                    "owner_name": owner_name,
                                    "owner_icon": owner_icon,
                                    "publish_url": publish_url,  # 推流地址（传统 WebRTC API）
                                    "play_url": play_url,  # 拉流地址（传统 WebRTC API）
                                    "stream_name": stream_name,  # 流名称
                                    "group_id": group_id,
                                    "message": f"{owner_name or '群主'}邀请你加入临时房间"
                                }
                                invite_response_json = json.dumps(invite_response, ensure_ascii=False)
                                app_logger.info(f"[temp_room] 返回房间邀请通知给用户 {invited_user_id}, 消息内容: {invite_response_json}")
                                print(f"[temp_room] 返回房间邀请通知给用户 {invited_user_id}: {invite_response_json}")
                                await target_conn["ws"].send_text(invite_response_json)
                            except Exception as send_error:
                                app_logger.warning(f"[temp_room] 发送邀请消息失败 - invited_user_id={invited_user_id}, error={send_error}")
                                # 发送失败不影响房间创建
                        else:
                            print(f"用户 {invited_user_id} 不在线")
                            offline_users.append(invited_user_id)
                except Exception as invite_error:
                    app_logger.error(f"[temp_room] 处理邀请用户时出错 - error={invite_error}")
                    # 邀请失败不影响房间创建，继续执行

                # 初始化房间成员列表（包含创建者）
                active_temp_rooms[group_id] = {
                    "room_id": room_id,
                    "owner_id": owner_id,
                    "owner_name": owner_name,
                    "owner_icon": owner_icon,
                    "publish_url": publish_url,  # 推流地址（传统 WebRTC API）
                    "play_url": play_url,  # 拉流地址（传统 WebRTC API）
                    "whip_url": whip_url,  # WHIP 地址（向后兼容）
                    "whep_url": whep_url,  # WHEP 地址（向后兼容）
                    "stream_name": stream_name,  # 流名称
                    "group_id": group_id,
                    "timestamp": time.time(),
                    "members": [owner_id]  # 初始化成员列表，包含创建者
                }
                
                # 保存临时语音房间到数据库
                try:
                    # 插入临时语音房间信息
                    insert_room_sql = """
                        INSERT INTO `temp_voice_rooms` (
                            room_id, group_id, owner_id, owner_name, owner_icon,
                            whip_url, whep_url, stream_name, status, create_time
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                        )
                    """
                    local_cursor.execute(insert_room_sql, (
                        room_id,
                        group_id,
                        owner_id,
                        owner_name if owner_name else None,
                        owner_icon if owner_icon else None,
                        whip_url,
                        whep_url,
                        stream_name,
                        1  # status = 1 (活跃)
                    ))
                    
                    # 插入房间创建者（群主）到成员表
                    insert_member_sql = """
                        INSERT INTO `temp_voice_room_members` (
                            room_id, user_id, user_name, status, join_time
                        ) VALUES (
                            %s, %s, %s, %s, NOW()
                        )
                    """
                    local_cursor.execute(insert_member_sql, (
                        room_id,
                        owner_id,
                        owner_name if owner_name else None,
                        1  # status = 1 (在线)
                    ))
                    
                    connection.commit()
                    print(f"[temp_room] 临时语音房间已保存到数据库 - room_id={room_id}, group_id={group_id}")
                    app_logger.info(f"[temp_room] 临时语音房间已保存到数据库 - room_id={room_id}, group_id={group_id}")
                except Exception as db_save_error:
                    # 数据库保存失败不影响内存中的房间创建
                    print(f"[temp_room] 保存临时语音房间到数据库失败 - room_id={room_id}, error={db_save_error}")
                    app_logger.error(f"[temp_room] 保存临时语音房间到数据库失败 - room_id={room_id}, error={db_save_error}", exc_info=True)
                    connection.rollback()
                
                print(f"[temp_room] 记录成功 group_id={group_id}, room_id={room_id}, stream_name={stream_name}, invited={invited_users}, active_total={len(active_temp_rooms)}")
                app_logger.info(f"[temp_room] 房间创建成功 - group_id={group_id}, room_id={room_id}, stream_name={stream_name}, members={[owner_id]}")

                # 返回给创建者（包含推流和拉流地址）
                create_room_response = {
                    "type": "6",
                    "room_id": room_id,
                    "publish_url": publish_url,  # 推流地址（传统 WebRTC API）- 创建者使用
                    "play_url": play_url,  # 拉流地址（传统 WebRTC API）- 创建者也可以拉流
                    "stream_name": stream_name,  # 流名称
                    "group_id": group_id,  # 添加 group_id 字段，客户端需要使用
                    "status": "success",
                    "message": f"临时房间创建成功，已邀请 {len(online_users)} 个在线用户，{len(offline_users)} 个离线用户",
                    "online_users": online_users,
                    "offline_users": offline_users
                }
                response_json = json.dumps(create_room_response, ensure_ascii=False)
                app_logger.info(f"[temp_room] 返回创建房间成功消息 - user_id={user_id}, 消息内容: {response_json}")
                print(f"[temp_room] 返回创建房间成功消息给用户 {user_id}: {response_json}")
                await websocket.send_text(response_json)
                
            except Exception as e:
                error_msg = f"创建房间失败: {str(e)}"
                app_logger.error(f"[temp_room] {error_msg} - user_id={user_id}, payload={msg_data1}", exc_info=True)
                print(f"[temp_room] 创建房间异常: {e}")
                
                # 返回错误信息给客户端
                try:
                    await websocket.send_text(json.dumps({
                        "type": "6",
                        "status": "error",
                        "message": error_msg
                    }, ensure_ascii=False))
                except Exception as send_error:
                    app_logger.error(f"[temp_room] 发送错误消息失败 - error={send_error}")

        async def handle_join_temp_room(request_group_id: str):
            # 记录调用，用于排查重复调用问题
            import time as time_module
            call_timestamp = time_module.time()
            app_logger.info(f"[temp_room] 🔵 handle_join_temp_room 被调用 - user_id={user_id}, request_group_id={request_group_id}, timestamp={call_timestamp}")
            print(f"[temp_room] 🔵 handle_join_temp_room 被调用 - user_id={user_id}, request_group_id={request_group_id}, timestamp={call_timestamp}")

            try:
                group_key = (request_group_id or "").strip()
                app_logger.info(f"[temp_room] 🔵 处理加入房间请求 - user_id={user_id}, group_key={group_key}")
                print(f"[temp_room] 🔵 处理加入房间请求 - user_id={user_id}, group_key={group_key}")
                if not group_key:
                    error_response = {
                        "type": "6",
                        "status": "error",
                        "message": "group_id 不能为空"
                    }
                    error_response_json = json.dumps(error_response, ensure_ascii=False)
                    app_logger.warning(f"[temp_room] 加入房间失败 - group_id 为空, user_id={user_id}, 消息内容: {error_response_json}")
                    print(f"[temp_room] 返回加入房间失败消息给用户 {user_id}: {error_response_json}")
                    await websocket.send_text(error_response_json)
                    return

                room_info = active_temp_rooms.get(group_key)
                if not room_info:
                    not_found_response = {
                        "type": "6",
                        "status": "not_found",
                        "group_id": group_key,
                        "message": "未找到该班级的临时房间"
                    }
                    not_found_response_json = json.dumps(not_found_response, ensure_ascii=False)
                    app_logger.warning(f"[temp_room] 用户 {user_id} 尝试加入不存在的房间 group_id={group_key}, 消息内容: {not_found_response_json}")
                    print(f"[temp_room] 返回加入房间失败消息给用户 {user_id}: {not_found_response_json}")
                    await websocket.send_text(not_found_response_json)
                    print(f"[temp_room] group_id={group_key} 无匹配房间，active_total={len(active_temp_rooms)}")
                    return

                # 检查用户是否已经在房间中（防止重复发送加入成功消息）
                app_logger.info(f"[temp_room] 🔵 检查用户是否已在房间 - user_id={user_id}, group_key={group_key}, room_exists={room_info is not None}")
                print(f"[temp_room] 🔵 检查用户是否已在房间 - user_id={user_id}, group_key={group_key}")

                was_member = False
                if "members" not in room_info:
                    room_info["members"] = []
                    app_logger.info(f"[temp_room] 🔵 房间成员列表不存在，已初始化 - group_key={group_key}")
                    print(f"[temp_room] 🔵 房间成员列表不存在，已初始化 - group_key={group_key}")
                else:
                    was_member = user_id in room_info["members"]
                    app_logger.info(f"[temp_room] 🔵 检查成员状态 - user_id={user_id}, was_member={was_member}, current_members={room_info['members']}")
                    print(f"[temp_room] 🔵 检查成员状态 - user_id={user_id}, was_member={was_member}, current_members={room_info['members']}")

                # 将用户添加到房间成员列表（如果尚未加入）
                try:
                    if not was_member:
                        room_info["members"].append(user_id)
                        print(f"[temp_room] 用户 {user_id} 加入成员列表，当前成员数={len(room_info['members'])}")
                        app_logger.info(f"[temp_room] ✅ 用户 {user_id} 首次加入房间 - group_id={group_key}, room_id={room_info['room_id']}, 当前成员={room_info['members']}")
                    else:
                        app_logger.warning(f"[temp_room] ⚠️ 用户 {user_id} 已在房间中 - group_id={group_key}, room_id={room_info['room_id']}, 当前成员={room_info['members']}")
                        print(f"[temp_room] ⚠️ 用户 {user_id} 已在房间中 - group_id={group_key}, 当前成员={room_info['members']}")
                except Exception as member_error:
                    app_logger.error(f"[temp_room] 添加成员到房间列表失败 - user_id={user_id}, group_id={group_key}, error={member_error}")
                    # 即使添加成员失败，也继续返回房间信息
                
                # 返回房间信息，包含推流和拉流地址
                # 如果用户已经在房间中，仍然返回房间信息（可能是客户端重试）
                join_room_response = {
                    "type": "6",
                    "room_id": room_info.get("room_id", ""),
                    "owner_id": room_info.get("owner_id", ""),
                    "owner_name": room_info.get("owner_name", ""),
                    "owner_icon": room_info.get("owner_icon", ""),
                    "publish_url": room_info.get("publish_url", ""),  # 推流地址（传统 WebRTC API）
                    "play_url": room_info.get("play_url", ""),  # 拉流地址（传统 WebRTC API）
                    "stream_name": room_info.get("stream_name", ""),  # 流名称
                    "group_id": group_key,
                    "members": room_info.get("members", []),
                    "status": "success",  # 添加状态字段，表示加入成功
                    "message": f"已加入临时房间（班级: {group_key}）" + ("（重复加入）" if was_member else "")
                }
                join_room_response_json = json.dumps(join_room_response, ensure_ascii=False)
                
                # 记录日志（如果是重复加入，使用不同的日志级别，并减少日志输出）
                if was_member:
                    # 重复加入时不记录完整的消息内容，避免日志过多
                    app_logger.warning(f"[temp_room] ⚠️⚠️⚠️ 用户 {user_id} 重复加入房间 group_id={group_key}，调用时间戳={call_timestamp}，当前时间戳={time_module.time()}，时间差={time_module.time() - call_timestamp:.3f}秒")
                    print(f"[temp_room] ⚠️⚠️⚠️ 用户 {user_id} 重复加入房间 {group_key}，调用时间戳={call_timestamp}，时间差={time_module.time() - call_timestamp:.3f}秒")
                    print(f"[temp_room] ⚠️ 当前房间成员：{room_info.get('members', [])}")
                else:
                    app_logger.info(f"[temp_room] ✅ 返回加入房间成功消息 - user_id={user_id}, 消息内容: {join_room_response_json}")
                    print(f"[temp_room] ✅ 返回加入房间成功消息给用户 {user_id}: {join_room_response_json}")
                
                app_logger.info(f"[temp_room] 🔵 准备发送加入房间响应 - user_id={user_id}, was_member={was_member}, timestamp={time_module.time()}")
                print(f"[temp_room] 🔵 准备发送加入房间响应 - user_id={user_id}, was_member={was_member}")
                await websocket.send_text(join_room_response_json)
                app_logger.info(f"[temp_room] 🔵 已发送加入房间响应 - user_id={user_id}, was_member={was_member}")
                print(f"[temp_room] 🔵 已发送加入房间响应 - user_id={user_id}, was_member={was_member}")
                print(f"[temp_room] user_id={user_id} 加入 group_id={group_key}, room_id={room_info.get('room_id', '')}, stream_name={room_info.get('stream_name', '')}, 当前成员={room_info.get('members', [])}")

            except Exception as e:
                error_msg = f"加入房间失败: {str(e)}"
                app_logger.error(f"[temp_room] {error_msg} - user_id={user_id}, request_group_id={request_group_id}", exc_info=True)
                print(f"[temp_room] 加入房间异常: {error_msg}")
                # 返回错误信息给客户端
                try:
                    error_response = {
                        "type": "6",
                        "status": "error",
                        "message": error_msg
                    }
                    error_response_json = json.dumps(error_response, ensure_ascii=False)
                    app_logger.error(f"[temp_room] 返回加入房间失败消息 - user_id={user_id}, 消息内容: {error_response_json}")
                    print(f"[temp_room] 返回加入房间失败消息给用户 {user_id}: {error_response_json}")
                    await websocket.send_text(error_response_json)
                except Exception as send_error:
                    app_logger.error(f"[temp_room] 发送错误消息失败 - error={send_error}")

        async def handle_temp_room_owner_leave(request_group_id: Optional[str]):
            """房间创建者主动解散临时房间"""
            group_key = (request_group_id or "").strip()
            if not group_key:
                error_response = {
                    "type": "temp_room_owner_leave",
                    "status": "error",
                    "message": "group_id 不能为空"
                }
                await websocket.send_text(json.dumps(error_response, ensure_ascii=False))
                return

            room_info = active_temp_rooms.get(group_key)
            if not room_info:
                error_response = {
                    "type": "temp_room_owner_leave",
                    "status": "not_found",
                    "group_id": group_key,
                    "message": "未找到临时房间或已解散"
                }
                await websocket.send_text(json.dumps(error_response, ensure_ascii=False))
                return

            owner_id = room_info.get("owner_id")
            if owner_id != user_id:
                error_response = {
                    "type": "temp_room_owner_leave",
                    "status": "forbidden",
                    "group_id": group_key,
                    "message": "只有房间创建者才能解散临时房间"
                }
                await websocket.send_text(json.dumps(error_response, ensure_ascii=False))
                return

            await notify_temp_room_closed(group_key, room_info, "owner_active_leave", user_id)
            active_temp_rooms.pop(group_key, None)
            app_logger.info(f"[temp_room] 房间创建者 {user_id} 主动解散临时房间 group_id={group_key}")
            print(f"[temp_room] 房间创建者 {user_id} 主动解散临时房间 group_id={group_key}")

            success_response = {
                "type": "temp_room_owner_leave",
                "status": "success",
                "group_id": group_key,
                "message": "临时房间已解散，已通知所有成员停止推流/拉流"
            }
            await websocket.send_text(json.dumps(success_response, ensure_ascii=False))

        async def handle_srs_webrtc_offer(msg_data: Dict[str, Any], action_type: str):
            """
            处理客户端通过服务器转发到 SRS 的 WebRTC offer
            action_type: 'publish' (推流) 或 'play' (拉流)
            """
            try:
                sdp = msg_data.get('sdp')
                stream_name = msg_data.get('stream_name')
                room_id = msg_data.get('room_id')
                group_id = msg_data.get('group_id')
                
                if not sdp:
                    error_response = {
                        "type": "srs_error",
                        "action": action_type,
                        "message": "缺少 SDP offer"
                    }
                    error_response_json = json.dumps(error_response, ensure_ascii=False)
                    app_logger.warning(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}, 消息内容: {error_response_json}")
                    print(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}: {error_response_json}")
                    await websocket.send_text(error_response_json)
                    return
                
                # 确定流名称（优先使用 stream_name，否则使用 room_id）
                if not stream_name:
                    if room_id:
                        # 尝试从房间信息中获取 stream_name
                        if group_id:
                            room_info = active_temp_rooms.get(group_id)
                            if room_info:
                                stream_name = room_info.get('stream_name')
                        if not stream_name:
                            stream_name = room_id  # 回退使用 room_id
                    else:
                        error_response = {
                            "type": "srs_error",
                            "action": action_type,
                            "message": "缺少 stream_name 或 room_id"
                        }
                        error_response_json = json.dumps(error_response, ensure_ascii=False)
                        app_logger.warning(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}, 消息内容: {error_response_json}")
                        print(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}: {error_response_json}")
                        await websocket.send_text(error_response_json)
                        return
                
                # 构建 SRS API URL
                api_path = "/rtc/v1/publish/" if action_type == "publish" else "/rtc/v1/play/"
                api_url = f"{SRS_WEBRTC_API_URL}{api_path}"
                # api_control_url 用于 SRS API 的 api 参数（控制URL）
                # 如果使用 HTTPS，通过 nginx 443 端口；如果使用 HTTP，直接使用 1985 端口
                if SRS_USE_HTTPS:
                    api_control_url = f"https://{SRS_SERVER}:{SRS_HTTPS_PORT}/api/v1{api_path}"
                else:
                    api_control_url = f"http://{SRS_SERVER}:{SRS_PORT}/api/v1{api_path}"
                stream_url = f"webrtc://{SRS_SERVER}/live/{stream_name}"
                
                # 记录详细的请求信息，包括使用的协议和URL
                protocol = "HTTPS" if SRS_USE_HTTPS else "HTTP"
                app_logger.info(f"[srs_webrtc] 转发 {action_type} offer - 协议={protocol}, API_URL={api_url}, user_id={user_id}, stream_name={stream_name}, stream_url={stream_url}")
                print(f"[srs_webrtc] 转发 {action_type} offer - 协议={protocol}, API_URL={api_url}, user_id={user_id}, stream_name={stream_name}, stream_url={stream_url}")
                
                # 检查是否是拉流操作，如果是则记录可能的推流方信息
                if action_type == "play":
                    room_info_check = active_temp_rooms.get(group_id) if group_id else None
                    if room_info_check:
                        owner_id = room_info_check.get('owner_id')
                        if owner_id == user_id:
                            app_logger.warning(f"[srs_webrtc] 警告：用户 {user_id} 正在拉取自己推流的流 {stream_name}，这可能导致问题")
                            print(f"[srs_webrtc] 警告：用户 {user_id} 正在拉取自己推流的流 {stream_name}")
                
                # 准备请求数据
                request_data = {
                    "api": api_control_url,
                    "streamurl": stream_url,
                    "sdp": sdp
                }
                
                # 发送请求到 SRS（异步使用 httpx，否则使用 urllib）
                if HAS_HTTPX:
                    # 如果使用 HTTPS 自签名证书，需要禁用 SSL 验证
                    verify_ssl = not SRS_USE_HTTPS or os.getenv('SRS_VERIFY_SSL', 'false').lower() == 'true'
                    async with httpx.AsyncClient(timeout=30.0, verify=verify_ssl) as client:
                        response = await client.post(
                            api_url,
                            json=request_data,
                            headers={"Content-Type": "application/json"}
                        )
                        response.raise_for_status()
                        result = response.json()
                        # 记录 SRS 响应（用于调试）
                        app_logger.info(f"[srs_webrtc] SRS {action_type} 响应 - code={result.get('code')}, has_sdp={bool(result.get('sdp'))}, 完整响应={json.dumps(result, ensure_ascii=False)}")
                        print(f"[srs_webrtc] SRS {action_type} 响应: {result}")
                else:
                    # 同步方式（在异步环境中使用 run_in_executor 避免阻塞）
                    def sync_http_request():
                        import urllib.request
                        import urllib.error
                        import ssl
                        request_json = json.dumps(request_data).encode('utf-8')
                        req = urllib.request.Request(
                            api_url,
                            data=request_json,
                            headers={"Content-Type": "application/json"},
                            method="POST"
                        )
                        # 如果使用 HTTPS 自签名证书，创建不验证 SSL 的上下文
                        if SRS_USE_HTTPS and os.getenv('SRS_VERIFY_SSL', 'false').lower() != 'true':
                            ssl_context = ssl.create_default_context()
                            ssl_context.check_hostname = False
                            ssl_context.verify_mode = ssl.CERT_NONE
                            with urllib.request.urlopen(req, timeout=30, context=ssl_context) as response:
                                return json.loads(response.read().decode('utf-8'))
                        else:
                            with urllib.request.urlopen(req, timeout=30) as response:
                                return json.loads(response.read().decode('utf-8'))
                    
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(None, sync_http_request)
                    # 记录 SRS 响应（用于调试）
                    app_logger.info(f"[srs_webrtc] SRS {action_type} 响应 - code={result.get('code')}, has_sdp={bool(result.get('sdp'))}, 完整响应={json.dumps(result, ensure_ascii=False)}")
                    print(f"[srs_webrtc] SRS {action_type} 响应: {result}")
                
                # 检查 SRS 响应
                if result.get('code') != 0:
                    # 记录完整的 SRS 响应以便调试
                    app_logger.error(f"[srs_webrtc] SRS {action_type} 失败 - 完整响应: {json.dumps(result, ensure_ascii=False)}")
                    print(f"[srs_webrtc] SRS {action_type} 失败 - 完整响应: {result}")
                    
                    # 尝试获取更详细的错误信息
                    error_message = result.get('message') or result.get('msg') or result.get('error') or '未知错误'
                    error_msg = f"SRS {action_type} 失败: code={result.get('code')}, message={error_message}"
                    app_logger.error(f"[srs_webrtc] {error_msg}")
                    print(f"[srs_webrtc] {error_msg}")
                    
                    error_response = {
                        "type": "srs_error",
                        "action": action_type,
                        "code": result.get('code'),
                        "message": error_msg,
                        "srs_response": result  # 添加完整响应以便客户端调试
                    }
                    error_response_json = json.dumps(error_response, ensure_ascii=False)
                    app_logger.error(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}, 消息内容: {error_response_json}")
                    print(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}: {error_response_json}")
                    await websocket.send_text(error_response_json)
                    return
                
                # 返回 answer 给客户端
                answer_sdp = result.get('sdp')
                if not answer_sdp:
                    error_msg = "SRS 响应中缺少 SDP answer"
                    app_logger.error(f"[srs_webrtc] {error_msg}")
                    error_response = {
                        "type": "srs_error",
                        "action": action_type,
                        "message": error_msg
                    }
                    error_response_json = json.dumps(error_response, ensure_ascii=False)
                    app_logger.error(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}, 消息内容: {error_response_json}")
                    print(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}: {error_response_json}")
                    await websocket.send_text(error_response_json)
                    return
                
                app_logger.info(f"[srs_webrtc] {action_type} 成功 - user_id={user_id}, stream_name={stream_name}")
                print(f"[srs_webrtc] {action_type} 成功 - user_id={user_id}")
                
                answer_response = {
                    "type": "srs_answer",
                    "action": action_type,
                    "sdp": answer_sdp,
                    "code": 0,
                    "stream_name": stream_name,
                    "stream_url": stream_url
                }
                answer_response_json = json.dumps(answer_response, ensure_ascii=False)
                app_logger.info(f"[srs_webrtc] 返回 {action_type} answer 给用户 {user_id}, 消息内容（SDP已省略）: {json.dumps({**answer_response, 'sdp': '...' if answer_response.get('sdp') else None}, ensure_ascii=False)}")
                print(f"[srs_webrtc] 返回 {action_type} answer 给用户 {user_id}, stream_name={stream_name}, sdp_length={len(answer_sdp) if answer_sdp else 0}")
                await websocket.send_text(answer_response_json)
                
            except Exception as e:
                error_msg = f"处理 SRS {action_type} offer 时出错: {str(e)}"
                app_logger.error(f"[srs_webrtc] {error_msg}", exc_info=True)
                print(f"[srs_webrtc] 错误: {error_msg}")
                error_response = {
                    "type": "srs_error",
                    "action": action_type,
                    "message": error_msg
                }
                error_response_json = json.dumps(error_response, ensure_ascii=False)
                app_logger.error(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}, 消息内容: {error_response_json}")
                print(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}: {error_response_json}")
                await websocket.send_text(error_response_json)

        async def handle_webrtc_signal(msg_data: Dict[str, Any], signal_type: str):
            """处理 WebRTC 信令消息（offer/answer/ice_candidate）"""
            target_user_id = msg_data.get('target_user_id')  # 目标用户ID
            room_id = msg_data.get('room_id')  # 房间ID（可选，用于验证）
            group_id = msg_data.get('group_id')  # 班级群ID（可选，用于验证）
            
            app_logger.info(f"[webrtc] 收到 {signal_type} 信令 - from={user_id}, to={target_user_id}, room_id={room_id}, group_id={group_id}")
            print(f"[webrtc] {signal_type} from={user_id} to={target_user_id}")
            
            if not target_user_id:
                error_msg = f"缺少目标用户ID (target_user_id)"
                app_logger.warning(f"[webrtc] {error_msg}")
                await websocket.send_text(json.dumps({
                    "type": "webrtc_error",
                    "signal_type": signal_type,
                    "message": error_msg
                }, ensure_ascii=False))
                return
            
            # 验证目标用户是否在线
            target_conn = connections.get(target_user_id)
            if not target_conn:
                error_msg = f"目标用户 {target_user_id} 不在线"
                app_logger.warning(f"[webrtc] {error_msg}")
                await websocket.send_text(json.dumps({
                    "type": "webrtc_error",
                    "signal_type": signal_type,
                    "message": error_msg
                }, ensure_ascii=False))
                return
            
            # 可选：验证房间和成员关系
            if group_id:
                room_info = active_temp_rooms.get(group_id)
                if room_info:
                    members = room_info.get("members", [])
                    if user_id not in members:
                        app_logger.warning(f"[webrtc] 用户 {user_id} 不在房间 {group_id} 的成员列表中")
                    if target_user_id not in members:
                        app_logger.warning(f"[webrtc] 目标用户 {target_user_id} 不在房间 {group_id} 的成员列表中")
            
            # 构建转发消息
            forward_message = {
                "type": f"webrtc_{signal_type}",
                "from_user_id": user_id,
                "target_user_id": target_user_id,
                "room_id": room_id,
                "group_id": group_id
            }
            
            # 根据信令类型添加特定字段
            if signal_type == "offer":
                forward_message["offer"] = msg_data.get('offer')
                forward_message["sdp"] = msg_data.get('sdp')  # 兼容不同格式
            elif signal_type == "answer":
                forward_message["answer"] = msg_data.get('answer')
                forward_message["sdp"] = msg_data.get('sdp')  # 兼容不同格式
            elif signal_type == "ice_candidate":
                forward_message["candidate"] = msg_data.get('candidate')
                forward_message["sdpMLineIndex"] = msg_data.get('sdpMLineIndex')
                forward_message["sdpMid"] = msg_data.get('sdpMid')
            
            # 转发给目标用户
            try:
                await target_conn["ws"].send_text(json.dumps(forward_message, ensure_ascii=False))
                app_logger.info(f"[webrtc] {signal_type} 转发成功 - from={user_id} to={target_user_id}")
                print(f"[webrtc] {signal_type} 转发成功 to={target_user_id}")
                
                # 给发送者返回成功确认
                await websocket.send_text(json.dumps({
                    "type": f"webrtc_{signal_type}_sent",
                    "target_user_id": target_user_id,
                    "status": "success"
                }, ensure_ascii=False))
            except Exception as e:
                error_msg = f"转发 {signal_type} 失败: {str(e)}"
                app_logger.error(f"[webrtc] {error_msg}")
                await websocket.send_text(json.dumps({
                    "type": "webrtc_error",
                    "signal_type": signal_type,
                    "message": error_msg
                }, ensure_ascii=False))

        print(f"[websocket][{user_id}] 数据库连接成功，开始监听消息")

        while True:
            try:
                print(f"[websocket][{user_id}] 等待消息... 当前在线={len(connections)}")
                message = await websocket.receive()
                print(f"[websocket][{user_id}] receive() 返回: {message.get('type') if isinstance(message, dict) else type(message)}, 内容预览={str(message)[:200]}")
            except WebSocketDisconnect as exc:
                # 正常断开
                print(f"用户 {user_id} 断开（WebSocketDisconnect），详情: {exc}")
                break
            except RuntimeError as e:
                # 已收到 disconnect 后再次 receive 会到这里
                print(f"用户 {user_id} receive RuntimeError: {e}")
                break

            # starlette 会在断开时 raise WebSocketDisconnect，保险起见也判断 type
            if message.get("type") == "websocket.disconnect":
                print(f"用户 {user_id} 断开（disconnect event）")
                break
            
            if "text" in message:
                data = message["text"]
                print(f"[websocket][{user_id}] recv text -> {data}")
                if data == "ping":
                    if user_id in connections:
                        connections[user_id]["last_heartbeat"] = time.time()
                    else:
                        print(f"收到 {user_id} 的 ping，但该用户已不在连接列表")
                        continue
                    await websocket.send_text("pong")
                    continue


                # 定向发送：to:目标ID:消息
                if data.startswith("to:"):
                    parts = data.split(":", 2)
                    if len(parts) == 3:
                        target_id, msg = parts[1], parts[2]
                        msg_data1 = json.loads(msg)
                        print(msg)
                        print(msg_data1['type'])
                        if msg_data1['type'] == "1":
                            print(" 加好友消息")
                            target_conn = connections.get(target_id)
                            if target_conn:
                                print(target_id, " 在线", ", 来自:", user_id)
                                print(data)
                                await target_conn["ws"].send_text(f"[私信来自 {user_id}] {msg}")
                            else:
                                print(target_id, " 不在线", ", 来自:", user_id)
                                print(data)
                                await websocket.send_text(f"用户 {target_id} 不在线")

                                # 解析 JSON
                                msg_data = json.loads(msg)
                                #print(msg_data['type'])
                                cursor = connection.cursor(dictionary=True)

                                update_query = """
                                            INSERT INTO ta_notification (sender_id, receiver_id, content, content_text)
                                            VALUES (%s, %s, %s, %s)
                                        """
                                cursor.execute(update_query, (user_id, msg_data['teacher_unique_id'], msg_data['text'], msg_data['type']))
                                connection.commit()
                        elif msg_data1['type'] == "3": 
                            print(" 创建群")   
                            cursor = connection.cursor(dictionary=True)
                            
                            # 获取当前时间
                            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            
                            # 字段映射：统一使用与 /groups/sync 相同的字段名
                            # 兼容旧字段名（nickname, headImage_path, owner_id, school_id, class_id）
                            group_name = msg_data1.get('group_name') or msg_data1.get('nickname', '')
                            face_url = msg_data1.get('face_url') or msg_data1.get('headImage_path', '')
                            detail_face_url = msg_data1.get('detail_face_url') or face_url
                            # 转换 group_type：数据库中是整数类型，需要将字符串转换为整数
                            group_type_raw = msg_data1.get('group_type', '')
                            group_type = convert_group_type_to_int(group_type_raw)
                            owner_identifier = msg_data1.get('owner_identifier') or msg_data1.get('owner_id', '')
                            schoolid = msg_data1.get('schoolid') or msg_data1.get('school_id')
                            classid = msg_data1.get('classid') or msg_data1.get('class_id')
                            is_class_group = msg_data1.get('is_class_group')
                            if is_class_group is None:
                                is_class_group = 1 if classid else 0
                            
                            # 生成群ID：优先使用客户端传过来的，如果没有则使用班级ID+01，否则使用UUID
                            unique_group_id = msg_data1.get('group_id')
                            print(f"[创建群] 收到客户端传入的 group_id={unique_group_id}, classid={classid}")
                            app_logger.info(f"[创建群] 收到客户端传入的 group_id={unique_group_id}, classid={classid}")
                            if not unique_group_id:
                                if classid:
                                    # 班级群：使用班级ID + "01"
                                    unique_group_id = str(classid) + "01"
                                    print(f"[创建群] 使用班级ID生成群ID: {unique_group_id}")
                                else:
                                    # 非班级群：使用UUID
                                    unique_group_id = str(uuid.uuid4())
                                    print(f"[创建群] 使用UUID生成群ID: {unique_group_id}")
                            else:
                                print(f"[创建群] 使用客户端传入的群ID: {unique_group_id}")
                            
                            # 插入 groups 表
                            insert_group_sql = """
                                INSERT INTO `groups` (
                                    group_id, group_name, group_type, face_url, detail_face_url,
                                    owner_identifier, create_time, max_member_num, member_num,
                                    introduction, notification, searchable, visible, add_option,
                                    is_shutup_all, next_msg_seq, latest_seq, last_msg_time,
                                    last_info_time, info_seq, detail_info_seq, detail_group_id,
                                    detail_group_name, detail_group_type, detail_is_shutup_all,
                                    online_member_num, classid, schoolid, is_class_group
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                            """
                            insert_group_params = (
                                unique_group_id,  # group_id
                                group_name,  # group_name
                                group_type,  # group_type
                                face_url,  # face_url
                                detail_face_url,  # detail_face_url
                                owner_identifier,  # owner_identifier
                                current_time,  # create_time
                                500,  # max_member_num (默认500)
                                len(msg_data1.get('members', [])),  # member_num
                                '',  # introduction
                                '',  # notification
                                1,  # searchable (默认可搜索)
                                1,  # visible (默认可见)
                                0,  # add_option (默认0)
                                0,  # is_shutup_all (默认0)
                                0,  # next_msg_seq
                                0,  # latest_seq
                                current_time,  # last_msg_time
                                current_time,  # last_info_time
                                0,  # info_seq
                                0,  # detail_info_seq
                                None,  # detail_group_id
                                None,  # detail_group_name
                                None,  # detail_group_type
                                None,  # detail_is_shutup_all
                                0,  # online_member_num
                                classid,  # classid
                                schoolid,  # schoolid
                                is_class_group  # is_class_group
                            )
                            
                            print(f"[创建群] 插入 groups 表 - group_id={unique_group_id}, group_name={group_name}")
                            cursor.execute(insert_group_sql, insert_group_params)
                            
                            # 插入群成员到 group_members 表
                            # 1. 优先处理 member_info（群主，必须存在）
                            # 2. 然后处理 members 数组（管理员和其他成员）
                            members_list = msg_data1.get('members', [])
                            member_info = msg_data1.get('member_info')
                            
                            # 记录已处理的成员ID，避免重复插入
                            processed_member_ids = set()
                            
                            # 第一步：处理 member_info（群主，必须存在）
                            if member_info:
                                member_user_id = member_info.get('user_id')
                                if member_user_id:
                                    print(f"[创建群] 处理 member_info（群主）: user_id={member_user_id}")
                                    member_user_name = member_info.get('user_name', '')
                                    member_self_role = member_info.get('self_role', 400)  # 默认群主
                                    
                                    # 处理 join_time
                                    member_join_time = current_time
                                    if 'join_time' in member_info:
                                        join_time_value = member_info.get('join_time')
                                        if join_time_value:
                                            try:
                                                if isinstance(join_time_value, (int, float)):
                                                    if join_time_value > 2147483647:
                                                        join_time_value = int(join_time_value / 1000)
                                                    dt = datetime.datetime.fromtimestamp(int(join_time_value))
                                                    member_join_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                                                else:
                                                    member_join_time = join_time_value
                                            except (ValueError, OSError):
                                                member_join_time = current_time
                                    
                                    insert_member_sql = """
                                        INSERT INTO `group_members` (
                                            group_id, user_id, user_name, self_role, join_time, msg_flag,
                                            self_msg_flag, readed_seq, unread_num
                                        ) VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                                        )
                                    """
                                    insert_member_params = (
                                        unique_group_id,
                                        member_user_id,
                                        member_user_name if member_user_name else None,
                                        member_self_role,
                                        member_join_time,
                                        member_info.get('msg_flag', 0),
                                        member_info.get('self_msg_flag', 0),
                                        member_info.get('readed_seq', 0),
                                        member_info.get('unread_num', 0)
                                    )
                                    
                                    print(f"[创建群] 插入群主 - group_id={unique_group_id}, user_id={member_user_id}, user_name={member_user_name}, self_role={member_self_role}")
                                    app_logger.info(f"[创建群] 插入群主 - group_id={unique_group_id}, user_id={member_user_id}, user_name={member_user_name}, self_role={member_self_role}")
                                    cursor.execute(insert_member_sql, insert_member_params)
                                    processed_member_ids.add(member_user_id)
                                else:
                                    print(f"[创建群] 警告: member_info 缺少 user_id，跳过")
                            else:
                                print(f"[创建群] 警告: 缺少 member_info（群主信息），这是必需的")
                            
                            # 第二步：处理 members 数组（管理员和其他成员）
                            if members_list:
                            
                                for m in members_list:
                                    # 兼容新旧字段名
                                    member_user_id = m.get('user_id') or m.get('unique_member_id')
                                    member_user_name = m.get('user_name') or m.get('member_name', '')
                                    
                                    if not member_user_id:
                                        print(f"[创建群] 警告: 成员信息缺少 user_id/unique_member_id，跳过")
                                        continue
                                    
                                    # 如果该成员已经在 member_info 中处理过（群主），跳过避免重复
                                    if member_user_id in processed_member_ids:
                                        print(f"[创建群] 跳过已处理的成员（群主）: user_id={member_user_id}")
                                        continue
                                    
                                    # self_role 字段：优先使用 self_role，否则从 group_role 转换
                                    if 'self_role' in m:
                                        self_role = m.get('self_role')
                                    else:
                                        # 从 group_role 转换：400=群主，300=管理员，其他=普通成员(200)
                                        group_role = m.get('group_role')
                                        if isinstance(group_role, int):
                                            if group_role == 400:
                                                self_role = 400  # 群主（但应该已经在 member_info 中处理）
                                            elif group_role == 300:
                                                self_role = 300  # 管理员（保持300）
                                            else:
                                                self_role = 200  # 普通成员
                                        elif isinstance(group_role, str):
                                            # 字符串格式的角色
                                            if group_role in ['owner', '群主', '400'] or member_user_id == owner_identifier:
                                                self_role = 400  # 群主（但应该已经在 member_info 中处理）
                                            elif group_role in ['admin', '管理员', '300']:
                                                self_role = 300  # 管理员
                                            else:
                                                self_role = 200  # 普通成员
                                        else:
                                            # 默认：如果是创建者则为群主，否则为普通成员
                                            if member_user_id == owner_identifier:
                                                self_role = 400  # 群主（但应该已经在 member_info 中处理）
                                            else:
                                                self_role = 200  # 普通成员
                                    
                                    insert_member_sql = """
                                        INSERT INTO `group_members` (
                                            group_id, user_id, user_name, self_role, join_time, msg_flag,
                                            self_msg_flag, readed_seq, unread_num
                                        ) VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                                        )
                                    """
                                    # 处理 join_time：支持时间戳格式（与 /groups/sync 一致）或直接使用当前时间
                                    member_join_time = current_time
                                    if 'join_time' in m:
                                        join_time_value = m.get('join_time')
                                        if join_time_value:
                                            # 如果是时间戳，转换为 datetime 字符串
                                            try:
                                                if isinstance(join_time_value, (int, float)):
                                                    if join_time_value > 2147483647:  # 毫秒级时间戳
                                                        join_time_value = int(join_time_value / 1000)
                                                    dt = datetime.datetime.fromtimestamp(int(join_time_value))
                                                    member_join_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                                                else:
                                                    member_join_time = join_time_value
                                            except (ValueError, OSError):
                                                member_join_time = current_time
                                    
                                    # 获取其他成员字段（与 /groups/sync 一致）
                                    member_msg_flag = m.get('msg_flag', 0)
                                    member_self_msg_flag = m.get('self_msg_flag', 0)
                                    member_readed_seq = m.get('readed_seq', 0)
                                    member_unread_num = m.get('unread_num', 0)
                                    
                                    insert_member_params = (
                                        unique_group_id,  # group_id
                                        member_user_id,  # user_id
                                        member_user_name if member_user_name else None,  # user_name
                                        self_role,  # self_role
                                        member_join_time,  # join_time
                                        member_msg_flag,  # msg_flag
                                        member_self_msg_flag,  # self_msg_flag
                                        member_readed_seq,  # readed_seq
                                        member_unread_num   # unread_num
                                    )
                                    
                                    print(f"[创建群] 插入成员 - group_id={unique_group_id}, user_id={member_user_id}, user_name={member_user_name}, self_role={self_role}")
                                    app_logger.info(f"[创建群] 插入成员 - group_id={unique_group_id}, user_id={member_user_id}, user_name={member_user_name}, self_role={self_role}")
                                    cursor.execute(insert_member_sql, insert_member_params)
                                    processed_member_ids.add(member_user_id)

                            connection.commit()
                            
                            # 同步到腾讯IM（异步执行，不阻塞响应）
                            try:
                                # 构建腾讯IM需要的群组数据格式
                                tencent_group_data = {
                                    "GroupId": unique_group_id,
                                    "group_id": unique_group_id,
                                    "Name": group_name,
                                    "group_name": group_name,
                                    "Type": group_type_raw,  # 使用原始字符串类型，build_group_payload 会转换
                                    "group_type": group_type_raw,
                                    "Owner_Account": owner_identifier,
                                    "owner_identifier": owner_identifier,
                                    "FaceUrl": face_url,
                                    "face_url": face_url,
                                    "Introduction": msg_data1.get('introduction', ''),
                                    "introduction": msg_data1.get('introduction', ''),
                                    "Notification": msg_data1.get('notification', ''),
                                    "notification": msg_data1.get('notification', ''),
                                    "MaxMemberCount": msg_data1.get('max_member_num', 500),
                                    "max_member_num": msg_data1.get('max_member_num', 500),
                                    "ApplyJoinOption": msg_data1.get('add_option', 0),
                                    "add_option": msg_data1.get('add_option', 0),
                                    "member_info": member_info,  # 群主信息
                                    "MemberList": []  # 成员列表（包含群主和管理员）
                                }
                                
                                # 构建成员列表（包含群主和管理员）
                                member_list = []
                                # 添加群主（从 member_info）
                                if member_info:
                                    owner_user_id = member_info.get('user_id')
                                    if owner_user_id:
                                        member_list.append({
                                            "Member_Account": owner_user_id,
                                            "user_id": owner_user_id,
                                            "Role": "Owner",
                                            "self_role": 400
                                        })
                                # 添加管理员和其他成员（从 members 数组）
                                if members_list:
                                    for m in members_list:
                                        member_user_id = m.get('user_id') or m.get('unique_member_id')
                                        if not member_user_id:
                                            continue
                                        
                                        # 如果已经在 member_info 中处理过（群主），跳过
                                        if member_user_id in processed_member_ids:
                                            continue
                                            # 确定角色
                                            if 'self_role' in m:
                                                role_value = m.get('self_role')
                                            else:
                                                group_role = m.get('group_role')
                                                if isinstance(group_role, int):
                                                    if group_role == 400:
                                                        role_value = 400
                                                    elif group_role == 300:
                                                        role_value = 300
                                                    else:
                                                        role_value = 200
                                                else:
                                                    role_value = 200
                                            
                                            # 转换为腾讯IM的角色字符串
                                            if role_value == 400:
                                                role_str = "Owner"
                                            elif role_value == 300:
                                                role_str = "Admin"
                                            else:
                                                role_str = "Member"
                                            
                                            member_list.append({
                                                "Member_Account": member_user_id,
                                                "user_id": member_user_id,
                                                "Role": role_str,
                                                "self_role": role_value
                                            })
                                
                                tencent_group_data["MemberList"] = member_list
                                
                                # 异步调用同步函数（不阻塞当前流程）
                                print(f"[创建群] 准备同步到腾讯IM - group_id={unique_group_id}")
                                app_logger.info(f"[创建群] 准备同步到腾讯IM - group_id={unique_group_id}, group_name={group_name}")
                                
                                # 使用 asyncio.create_task 异步执行，不等待结果
                                async def sync_to_tencent():
                                    try:
                                        # 调用同步函数（需要传入列表格式）
                                        result = await notify_tencent_group_sync(owner_identifier, [tencent_group_data])
                                        if result.get("status") == "success":
                                            print(f"[创建群] 腾讯IM同步成功 - group_id={unique_group_id}")
                                            app_logger.info(f"[创建群] 腾讯IM同步成功 - group_id={unique_group_id}")
                                        else:
                                            print(f"[创建群] 腾讯IM同步失败 - group_id={unique_group_id}, error={result.get('error')}")
                                            app_logger.warning(f"[创建群] 腾讯IM同步失败 - group_id={unique_group_id}, error={result.get('error')}")
                                    except Exception as sync_error:
                                        print(f"[创建群] 腾讯IM同步异常 - group_id={unique_group_id}, error={sync_error}")
                                        app_logger.error(f"[创建群] 腾讯IM同步异常 - group_id={unique_group_id}, error={sync_error}", exc_info=True)
                                
                                # 创建异步任务，不等待完成
                                asyncio.create_task(sync_to_tencent())
                                
                            except Exception as tencent_sync_error:
                                # 同步失败不影响群组创建
                                print(f"[创建群] 准备腾讯IM同步时出错 - group_id={unique_group_id}, error={tencent_sync_error}")
                                app_logger.error(f"[创建群] 准备腾讯IM同步时出错 - group_id={unique_group_id}, error={tencent_sync_error}", exc_info=True)
                            
                            # 如果是班级群（有 classid 或 class_id），自动创建临时语音群
                            temp_room_info = None
                            class_id = classid  # 使用统一后的 classid 变量
                            if class_id:
                                # 检查是否已经有临时语音群（使用 unique_group_id 作为 group_id）
                                if unique_group_id not in active_temp_rooms:
                                    try:
                                        print(f"[创建班级群] 检测到班级群，自动创建临时语音群 - group_id={unique_group_id}, class_id={class_id}")
                                        app_logger.info(f"[创建班级群] 自动创建临时语音群 - group_id={unique_group_id}, class_id={class_id}, owner_id={user_id}")
                                        
                                        # 获取创建者信息
                                        owner_id = user_id
                                        owner_name = msg_data1.get('owner_name', '') or ''
                                        owner_icon = msg_data1.get('owner_icon', '') or ''
                                        
                                        # 尝试从数据库获取创建者信息
                                        if not owner_name or not owner_icon:
                                            try:
                                                cursor.execute(
                                                    "SELECT name, icon FROM ta_teacher WHERE teacher_unique_id = %s",
                                                    (owner_id,)
                                                )
                                                owner_info = cursor.fetchone()
                                                if owner_info:
                                                    if not owner_name:
                                                        owner_name = owner_info.get('name', '') or owner_name
                                                    if not owner_icon:
                                                        owner_icon = owner_info.get('icon', '') or owner_icon
                                            except Exception as db_error:
                                                app_logger.error(f"[创建班级群] 查询创建者信息失败 - user_id={user_id}, error={db_error}")
                                        
                                        # 生成唯一的房间ID和流名称
                                        room_id = str(uuid.uuid4())
                                        stream_name = f"room_{unique_group_id}_{int(time.time())}"
                                        
                                        # 生成传统 WebRTC API 地址（推流和拉流）
                                        publish_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/publish/?app={SRS_APP}&stream={stream_name}"
                                        play_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/play/?app={SRS_APP}&stream={stream_name}"
                                        
                                        # 保留 WHIP/WHEP 地址用于向后兼容
                                        whip_url = f"{SRS_BASE_URL}/rtc/v1/whip/?app={SRS_APP}&stream={stream_name}"
                                        whep_url = f"{SRS_BASE_URL}/rtc/v1/whep/?app={SRS_APP}&stream={stream_name}"
                                        
                                        # 创建临时语音群
                                        active_temp_rooms[unique_group_id] = {
                                            "room_id": room_id,
                                            "owner_id": owner_id,
                                            "owner_name": owner_name,
                                            "owner_icon": owner_icon,
                                            "publish_url": publish_url,  # 推流地址（传统 WebRTC API）
                                            "play_url": play_url,  # 拉流地址（传统 WebRTC API）
                                            "whip_url": whip_url,  # WHIP 地址（向后兼容）
                                            "whep_url": whep_url,  # WHEP 地址（向后兼容）
                                            "stream_name": stream_name,
                                            "group_id": unique_group_id,
                                            "timestamp": time.time(),
                                            "members": [owner_id]  # 初始化成员列表，包含创建者
                                        }
                                        
                                        # 保存临时语音房间到数据库
                                        try:
                                            # 插入临时语音房间信息
                                            insert_room_sql = """
                                                INSERT INTO `temp_voice_rooms` (
                                                    room_id, group_id, owner_id, owner_name, owner_icon,
                                                    whip_url, whep_url, stream_name, status, create_time
                                                ) VALUES (
                                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                                                )
                                            """
                                            cursor.execute(insert_room_sql, (
                                                room_id,
                                                unique_group_id,
                                                owner_id,
                                                owner_name if owner_name else None,
                                                owner_icon if owner_icon else None,
                                                whip_url,
                                                whep_url,
                                                stream_name,
                                                1  # status = 1 (活跃)
                                            ))
                                            
                                            # 插入房间创建者（群主）到成员表
                                            insert_member_sql = """
                                                INSERT INTO `temp_voice_room_members` (
                                                    room_id, user_id, user_name, status, join_time
                                                ) VALUES (
                                                    %s, %s, %s, %s, NOW()
                                                )
                                            """
                                            cursor.execute(insert_member_sql, (
                                                room_id,
                                                owner_id,
                                                owner_name if owner_name else None,
                                                1  # status = 1 (在线)
                                            ))
                                            
                                            connection.commit()
                                            print(f"[创建班级群] 临时语音房间已保存到数据库 - room_id={room_id}, group_id={unique_group_id}")
                                            app_logger.info(f"[创建班级群] 临时语音房间已保存到数据库 - room_id={room_id}, group_id={unique_group_id}")
                                        except Exception as db_save_error:
                                            # 数据库保存失败不影响内存中的房间创建
                                            print(f"[创建班级群] 保存临时语音房间到数据库失败 - room_id={room_id}, error={db_save_error}")
                                            app_logger.error(f"[创建班级群] 保存临时语音房间到数据库失败 - room_id={room_id}, error={db_save_error}", exc_info=True)
                                            connection.rollback()
                                        
                                        temp_room_info = {
                                            "room_id": room_id,
                                            "publish_url": publish_url,  # 推流地址（传统 WebRTC API）
                                            "play_url": play_url,  # 拉流地址（传统 WebRTC API）
                                            "stream_name": stream_name,
                                            "group_id": unique_group_id,
                                            "owner_id": owner_id,
                                            "owner_name": owner_name,
                                            "owner_icon": owner_icon
                                        }
                                        
                                        print(f"[创建班级群] 临时语音群创建成功 - group_id={unique_group_id}, room_id={room_id}, stream_name={stream_name}")
                                        app_logger.info(f"[创建班级群] 临时语音群创建成功 - group_id={unique_group_id}, room_id={room_id}")
                                    except Exception as temp_room_error:
                                        app_logger.error(f"[创建班级群] 创建临时语音群失败 - group_id={unique_group_id}, error={temp_room_error}")
                                        print(f"[创建班级群] 创建临时语音群失败: {temp_room_error}")
                                        # 临时语音群创建失败不影响班级群创建
                                else:
                                    # 如果已存在临时语音群，获取其信息
                                    existing_room = active_temp_rooms[unique_group_id]
                                    temp_room_info = {
                                        "room_id": existing_room.get("room_id"),
                                        "publish_url": existing_room.get("publish_url"),  # 推流地址（传统 WebRTC API）
                                        "play_url": existing_room.get("play_url"),  # 拉流地址（传统 WebRTC API）
                                        "stream_name": existing_room.get("stream_name"),
                                        "group_id": unique_group_id,
                                        "owner_id": existing_room.get("owner_id"),
                                        "owner_name": existing_room.get("owner_name"),
                                        "owner_icon": existing_room.get("owner_icon")
                                    }
                                    print(f"[创建班级群] 临时语音群已存在 - group_id={unique_group_id}, room_id={temp_room_info.get('room_id')}")
                            
                            # 给在线成员推送
                            # 兼容新旧字段名：user_id 或 unique_member_id
                            members_to_notify = msg_data1.get('members', [])
                            for m in members_to_notify:
                                # 兼容新旧字段名
                                member_id = m.get('user_id') or m.get('unique_member_id')
                                if not member_id:
                                    continue
                                
                                target_conn = connections.get(member_id)
                                if target_conn:
                                    await target_conn["ws"].send_text(json.dumps({
                                        "type":"notify",
                                        "message":f"你已加入群: {msg_data1.get('group_name') or msg_data1.get('nickname', '')}",
                                        "group_id": unique_group_id,
                                        "groupname": msg_data1.get('group_name') or msg_data1.get('nickname', '')
                                    }))
                                else:
                                    print(f"[创建群] 成员 {member_id} 不在线，插入通知")
                                    cursor = connection.cursor(dictionary=True)

                                    update_query = """
                                            INSERT INTO ta_notification (sender_id, sender_name, receiver_id, unique_group_id, group_name, content, content_text)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        """
                                    cursor.execute(update_query, (user_id, msg_data1.get('owner_name'), member_id, unique_group_id, msg_data1.get("group_name") or msg_data1.get("nickname", ""), "邀请你加入了群", msg_data1['type']))
                                    connection.commit()

                            #把创建成功的群信息发回给创建者（包含临时语音群信息）
                            # 兼容新旧字段名：group_name 或 nickname
                            group_name_for_response = msg_data1.get('group_name') or msg_data1.get('nickname', '')
                            response_data = {
                                "type":"3",
                                "message":f"你创建了群: {group_name_for_response}",
                                "group_id": unique_group_id,
                                "groupname": group_name_for_response
                            }
                            
                            # 如果有临时语音群信息，添加到响应中
                            if temp_room_info:
                                response_data["temp_room"] = temp_room_info
                            
                            # 打印返回给客户端的消息
                            response_json = json.dumps(response_data, ensure_ascii=False)
                            print(f"[创建群] 返回给客户端 - user_id={user_id}, group_id={unique_group_id}, response={response_json}")
                            app_logger.info(f"[创建群] 返回给客户端 - user_id={user_id}, group_id={unique_group_id}, response={response_json}")
                            
                            await websocket.send_text(response_json)

                                    # 群消息: 群主发消息，发给除群主外的所有群成员
                        elif msg_data1['type'] == "5":
                            print("群消息发送")
                            cursor = connection.cursor(dictionary=True)
                            print(msg_data1)
                            unique_group_id = msg_data1.get('unique_group_id')
                            sender_id = user_id  # 当前发送者（可能是群主，也可能是群成员）
                            groupowner_flag = msg_data1.get('groupowner', False)  # bool 或字符串

                            # 查询群信息
                            cursor.execute("""
                                SELECT group_admin_id, nickname 
                                FROM ta_group 
                                WHERE unique_group_id = %s
                            """, (unique_group_id,))
                            row = cursor.fetchone()
                            if not row:
                                await websocket.send_text(f"群 {unique_group_id} 不存在")
                                return

                            group_admin_id = row['group_admin_id']
                            group_name = row['nickname'] or ""  # 群名

                            if str(groupowner_flag).lower() in ("true", "1", "yes"):
                                # --------------------------- 群主发送 ---------------------------
                                if group_admin_id != sender_id:
                                    await websocket.send_text(f"不是群主，不能发送群消息")
                                    return

                                # 查成员（排除群主）
                                cursor.execute("""
                                    SELECT unique_member_id 
                                    FROM ta_group_member_relation
                                    WHERE unique_group_id = %s AND unique_member_id != %s
                                """, (unique_group_id, sender_id))
                                members = cursor.fetchall()

                                if not members:
                                    await websocket.send_text("群没有其他成员")
                                    return

                                for m in members:
                                    member_id = m['unique_member_id']
                                    target_conn = connections.get(member_id)
                                    if target_conn:
                                        print(member_id, "在线，发送群消息")
                                        await target_conn["ws"].send_text(json.dumps({
                                            "type": "5",
                                            "group_id": unique_group_id,
                                            "from": sender_id,
                                            "content": msg_data1.get("content", ""),
                                            "groupname": group_name,
                                            "sender_name": msg_data1.get("sender_name", "")
                                        }, ensure_ascii=False))
                                    else:
                                        print(member_id, "不在线，插入通知")
                                        cursor.execute("""
                                            INSERT INTO ta_notification (
                                            sender_id, sender_name, receiver_id, unique_group_id, group_name, content, content_text
                                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        """, (
                                            sender_id, msg_data1.get("sender_name", ""), member_id, unique_group_id, group_name,
                                            msg_data1.get("content", ""), msg_data1['type']
                                        ))
                                        connection.commit()
                            else:
                                # --------------------------- 群成员发送 ---------------------------
                                print("群成员发送群消息")

                                # 找到所有需要接收的人：群主 + 其他成员（去掉发送者）
                                receivers = []

                                # 添加群主
                                if group_admin_id != sender_id:
                                    receivers.append(group_admin_id)

                                # 查其他成员（排除自己）
                                cursor.execute("""
                                    SELECT unique_member_id 
                                    FROM ta_group_member_relation
                                    WHERE unique_group_id = %s AND unique_member_id != %s
                                """, (unique_group_id, sender_id))
                                member_rows = cursor.fetchall()
                                for r in member_rows:
                                    receivers.append(r['unique_member_id'])

                                # 去重（以防群主也在成员列表里）
                                receivers = list(set(receivers))

                                if not receivers:
                                    await websocket.send_text("群没有其他成员可以接收此消息")
                                    return

                                # 给这些接收者发消息 / 存通知
                                for rid in receivers:
                                    target_conn = connections.get(rid)
                                    if target_conn:
                                        print(rid, "在线，发送群成员消息")
                                        await target_conn["ws"].send_text(json.dumps({
                                            "type": "5",
                                            "group_id": unique_group_id,
                                            "from": sender_id,
                                            "content": msg_data1.get("content", ""),
                                            "groupname": group_name,
                                            "sender_name": msg_data1.get("sender_name", "")
                                        }, ensure_ascii=False))
                                    else:
                                        print(rid, "不在线，插入通知")
                                        cursor.execute("""
                                            INSERT INTO ta_notification (
                                            sender_id, sender_name, receiver_id, unique_group_id, group_name, content, content_text
                                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        """, (
                                            sender_id, msg_data1.get("sender_name", ""), rid, unique_group_id, group_name,
                                            msg_data1.get("content", ""), msg_data1['type']
                                        ))
                                        connection.commit()
                        
                        # 创建临时房间: 群主创建临时房间，下发拉流地址给被邀请的人
                        elif msg_data1['type'] == "6":
                            await handle_temp_room_creation(msg_data1)
                            continue
                        elif msg_data1['type'] == "temp_room_owner_leave":
                            await handle_temp_room_owner_leave(msg_data1.get("group_id") or target_id)
                            continue
                        # 课前准备消息: 发送给群组所有成员
                        elif msg_data1['type'] == "prepare_class":
                            app_logger.info(f"[prepare_class] 收到课前准备消息，user_id={user_id}, target_id={target_id}")
                            print(f"[prepare_class] 收到课前准备消息，user_id={user_id}, target_id={target_id}")
                            cursor = connection.cursor(dictionary=True)
                            
                            group_id = target_id  # 群组ID就是target_id
                            class_id = msg_data1.get('class_id')
                            school_id = msg_data1.get('school_id')
                            subject = msg_data1.get('subject', '')
                            content = msg_data1.get('content', '')
                            date = msg_data1.get('date', '')
                            class_time = msg_data1.get('time', '')  # 上课时间
                            sender_id = msg_data1.get('sender_id') or user_id
                            sender_name = msg_data1.get('sender_name', '')
                            
                            app_logger.info(
                                f"[prepare_class] 参数解析 - group_id={group_id}, class_id={class_id}, school_id={school_id}, "
                                f"subject={subject}, sender_id={sender_id}, sender_name={sender_name}, "
                                f"date={date}, time={class_time}, content_length={len(content)}"
                            )
                            print(f"[prepare_class] group_id={group_id}, class_id={class_id}, school_id={school_id}, subject={subject}, sender_id={sender_id}, time={class_time}")
                            
                            # 验证群组是否存在（使用 groups 表）
                            cursor.execute("""
                                SELECT group_id, group_name, owner_identifier 
                                FROM `groups` 
                                WHERE group_id = %s
                            """, (group_id,))
                            group_info = cursor.fetchone()
                            
                            if not group_info:
                                error_msg = f"群组 {group_id} 不存在"
                                app_logger.warning(f"[prepare_class] {error_msg}, user_id={user_id}")
                                await websocket.send_text(json.dumps({
                                    "type": "error",
                                    "message": error_msg
                                }, ensure_ascii=False))
                                continue
                            
                            group_name = group_info.get('group_name', '')
                            owner_identifier = group_info.get('owner_identifier', '')
                            app_logger.info(f"[prepare_class] 群组验证成功 - group_id={group_id}, group_name={group_name}, owner_identifier={owner_identifier}")
                            
                            # 获取群组所有成员（使用 group_members 表）
                            cursor.execute("""
                                SELECT user_id 
                                FROM `group_members`
                                WHERE group_id = %s
                            """, (group_id,))
                            members = cursor.fetchall()
                            total_members = len(members)
                            app_logger.info(f"[prepare_class] 获取群组成员 - group_id={group_id}, 总成员数={total_members}")
                            
                            # 构建消息内容
                            prepare_message = json.dumps({
                                "type": "prepare_class",
                                "class_id": class_id,
                                "school_id": school_id,
                                "subject": subject,
                                "content": content,
                                "date": date,
                                "time": class_time,
                                "sender_id": sender_id,
                                "sender_name": sender_name,
                                "group_id": group_id,
                                "group_name": group_name
                            }, ensure_ascii=False)
                            
                            # 先为所有成员保存到数据库（不管是否在线）
                            app_logger.info(f"[prepare_class] 开始保存课前准备数据到数据库，成员数={total_members}")
                            prepare_id: Optional[int] = None

                            # 判断是否存在相同 (group_id, class_id, school_id, subject, date, time) 的记录
                            cursor.execute("""
                                SELECT prepare_id FROM class_preparation
                                WHERE group_id = %s
                                  AND class_id = %s
                                  AND IFNULL(school_id, '') = %s
                                  AND subject = %s
                                  AND date = %s
                                  AND IFNULL(time, '') = %s
                                ORDER BY prepare_id DESC
                                LIMIT 1
                            """, (group_id, class_id, school_id or "", subject, date, class_time or ""))
                            existing_prepare = cursor.fetchone()

                            if existing_prepare:
                                prepare_id = existing_prepare['prepare_id']
                                cursor.execute("""
                                    UPDATE class_preparation
                                    SET content = %s,
                                        school_id = %s,
                                        sender_id = %s,
                                        sender_name = %s,
                                        updated_at = NOW()
                                    WHERE prepare_id = %s
                                """, (content, school_id, sender_id, sender_name, prepare_id))
                                cursor.execute(
                                    "DELETE FROM class_preparation_receiver WHERE prepare_id = %s",
                                    (prepare_id,)
                                )
                                app_logger.info(f"[prepare_class] 更新已有课前准备记录 prepare_id={prepare_id}")
                            else:
                                cursor.execute("""
                                    INSERT INTO class_preparation (
                                        group_id, class_id, school_id, subject, content, date, time, sender_id, sender_name, created_at
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                                """, (group_id, class_id, school_id, subject, content, date, class_time, sender_id, sender_name))
                                prepare_id = cursor.lastrowid
                                app_logger.info(f"[prepare_class] 插入主记录成功，prepare_id={prepare_id}")
                            
                            # 为每个成员插入接收记录（is_read=0 表示未读）
                            for member in members:
                                member_id = member['user_id']
                                cursor.execute("""
                                    INSERT INTO class_preparation_receiver (
                                        prepare_id, receiver_id, is_read, created_at
                                    ) VALUES (%s, %s, 0, NOW())
                                """, (prepare_id, member_id))
                            
                            connection.commit()
                            app_logger.info(f"[prepare_class] 已为所有 {total_members} 个成员保存课前准备数据")
                            
                            online_count = 0
                            offline_count = 0
                            online_members = []
                            offline_members = []
                            
                            # 然后推送在线的成员
                            for member in members:
                                member_id = member['user_id']
                                target_conn = connections.get(member_id)
                                
                                if target_conn:
                                    app_logger.debug(f"[prepare_class] 用户 {member_id} 在线，推送消息并标记为已读")
                                    print(f"[prepare_class] 用户 {member_id} 在线，推送消息")
                                    online_count += 1
                                    online_members.append(member_id)
                                    await target_conn["ws"].send_text(prepare_message)
                                    # 标记为已读（因为已经实时推送了）
                                    cursor.execute("""
                                        UPDATE class_preparation_receiver 
                                        SET is_read = 1, read_at = NOW() 
                                        WHERE prepare_id = %s AND receiver_id = %s
                                    """, (prepare_id, member_id))
                                else:
                                    app_logger.debug(f"[prepare_class] 用户 {member_id} 不在线，已保存到数据库，等待登录时获取")
                                    print(f"[prepare_class] 用户 {member_id} 不在线，已保存到数据库")
                                    offline_count += 1
                                    offline_members.append(member_id)
                            
                            # 提交已读标记的更新
                            connection.commit()
                            app_logger.info(f"[prepare_class] 已推送并标记 {online_count} 个在线成员为已读")
                            
                            # 给发送者返回结果
                            result_message = f"课前准备消息已发送，在线: {online_count} 人，离线: {offline_count} 人"
                            app_logger.info(f"[prepare_class] 完成 - group_id={group_id}, class_id={class_id}, subject={subject}, time={class_time}, 在线={online_count}, 离线={offline_count}, 在线成员={online_members}, 离线成员={offline_members}")
                            print(f"[prepare_class] 完成，在线={online_count}, 离线={offline_count}, time={class_time}")
                            
                            await websocket.send_text(json.dumps({
                                "type": "prepare_class",
                                "status": "success",
                                "message": result_message,
                                "online_count": online_count,
                                "offline_count": offline_count
                            }, ensure_ascii=False))
                            continue
                        # WebRTC 信令消息处理
                        elif msg_data1['type'] == "webrtc_offer":
                            await handle_webrtc_signal(msg_data1, "offer")
                            continue
                        elif msg_data1['type'] == "webrtc_answer":
                            await handle_webrtc_signal(msg_data1, "answer")
                            continue
                        elif msg_data1['type'] == "webrtc_ice_candidate":
                            await handle_webrtc_signal(msg_data1, "ice_candidate")
                            continue
                        # 处理通过服务器转发到 SRS 的 offer（推流）
                        elif msg_data1['type'] == "srs_publish_offer":
                            await handle_srs_webrtc_offer(msg_data1, "publish")
                            continue
                        # 处理通过服务器转发到 SRS 的 offer（拉流）
                        elif msg_data1['type'] == "srs_play_offer":
                            await handle_srs_webrtc_offer(msg_data1, "play")
                            continue
        
                    else:
                        print(" 格式错误")
                        await websocket.send_text("格式错误: to:<target_id>:<消息>")
                else:
                    msg_data_raw = None
                    try:
                        msg_data_raw = json.loads(data)
                    except Exception:
                        msg_data_raw = None

                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "6":
                        await handle_temp_room_creation(msg_data_raw)
                        continue
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "temp_room_owner_leave":
                        await handle_temp_room_owner_leave(msg_data_raw.get("group_id"))
                        continue
                    # WebRTC 信令消息处理（纯 JSON 格式）
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "webrtc_offer":
                        await handle_webrtc_signal(msg_data_raw, "offer")
                        continue
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "webrtc_answer":
                        await handle_webrtc_signal(msg_data_raw, "answer")
                        continue
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "webrtc_ice_candidate":
                        await handle_webrtc_signal(msg_data_raw, "ice_candidate")
                        continue
                    # 处理通过服务器转发到 SRS 的 offer（推流）
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "srs_publish_offer":
                        await handle_srs_webrtc_offer(msg_data_raw, "publish")
                        continue
                    # 处理通过服务器转发到 SRS 的 offer（拉流）
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "srs_play_offer":
                        await handle_srs_webrtc_offer(msg_data_raw, "play")
                        continue
                    # 处理加入临时房间请求
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") in ("join_temp_room", "temp_room_join"):
                        group_id_from_msg = msg_data_raw.get("group_id")
                        app_logger.info(f"[temp_room] 🔵 收到 JSON 格式的加入房间请求 - user_id={user_id}, type={msg_data_raw.get('type')}, group_id={group_id_from_msg}, 原始消息={data[:200]}")
                        print(f"[temp_room] 🔵 收到 JSON 格式的加入房间请求 - user_id={user_id}, type={msg_data_raw.get('type')}, group_id={group_id_from_msg}")
                        await handle_join_temp_room(group_id_from_msg)
                        continue

                    # 处理字符串格式的加入房间请求
                    stripped_data = (data or "").strip()
                    if stripped_data and stripped_data in active_temp_rooms:
                        app_logger.info(f"[temp_room] 🔵 收到字符串格式的加入房间请求 - user_id={user_id}, stripped_data={stripped_data}, 原始消息={data[:200]}, active_rooms={list(active_temp_rooms.keys())}")
                        print(f"[temp_room] 🔵 收到字符串格式的加入房间请求 - user_id={user_id}, stripped_data={stripped_data}")
                        await handle_join_temp_room(stripped_data)
                        continue
                    elif stripped_data:
                        app_logger.debug(f"[temp_room] 🔵 字符串数据不在 active_temp_rooms 中 - user_id={user_id}, stripped_data={stripped_data}, active_rooms={list(active_temp_rooms.keys())}")
                        print(f"[temp_room] 🔵 字符串数据不在 active_temp_rooms 中 - user_id={user_id}, stripped_data={stripped_data}")
                        continue

                    # 如果都不匹配，打印原始数据用于调试
                    print(f"[websocket][{user_id}] 未处理的消息: {data[:200]}")
                # 广播
                for uid, conn in connections.items():
                    if uid != user_id:
                        await conn["ws"].send_text(f"[{user_id} 广播] {data}")
                        
            # 二进制音频消息处理 (flag协议)
            elif "bytes" in message:
                audio_bytes = message["bytes"]
                try:
                    frameType = audio_bytes[0]
                    flag = audio_bytes[1]
                    offset = 2
                    if frameType != 6:
                        continue
                    group_len = struct.unpack("<I", audio_bytes[offset:offset+4])[0]
                    offset += 4
                    group_id = audio_bytes[offset:offset+group_len].decode("utf-8")
                    offset += group_len
                    sender_len = struct.unpack("<I", audio_bytes[offset:offset+4])[0]
                    offset += 4
                    sender_id = audio_bytes[offset:offset+sender_len].decode("utf-8")
                    offset += sender_len
                    name_len = struct.unpack("<I", audio_bytes[offset:offset+4])[0]
                    offset += 4
                    sender_name = audio_bytes[offset:offset+name_len].decode("utf-8")
                    offset += name_len
                    ts = struct.unpack("<Q", audio_bytes[offset:offset+8])[0]
                    offset += 8
                    aac_len = struct.unpack("<I", audio_bytes[offset:offset+4])[0]
                    offset += 4
                    aac_data = audio_bytes[offset:offset+aac_len]

                    if flag == 0:
                        temp_filename = f"/tmp/{group_id}_{sender_id}_{ts}.aac"
                        with open(temp_filename, "wb") as f:
                            if aac_len > 0:
                                f.write(aac_data)
                        connections[sender_id]["voice_file"] = temp_filename
                        print(" init acc flag:", temp_filename)

                    elif flag == 1:
                        if "voice_file" in connections[sender_id]:
                            with open(connections[sender_id]["voice_file"], "ab") as f:
                                f.write(aac_data)
                        cursor.execute("""
                            SELECT unique_member_id FROM ta_group_member_relation
                            WHERE unique_group_id=%s AND unique_member_id!=%s
                        """, (group_id, sender_id))
                        for m in cursor.fetchall():
                            tc = connections.get(m['unique_member_id'])
                            if tc:
                                await tc["ws"].send_bytes(audio_bytes)

                    elif flag == 2:
                        voice_file_path = connections[sender_id].pop("voice_file", None)
                        cursor.execute("""
                            SELECT unique_member_id FROM ta_group_member_relation
                            WHERE unique_group_id=%s AND unique_member_id!=%s
                        """, (group_id, sender_id))
                        for m in cursor.fetchall():
                            rid = m["unique_member_id"]
                            tc = connections.get(rid)
                            
                            if voice_file_path and os.path.exists(voice_file_path):
                                offline_path = f"/var/offline_voice/{os.path.basename(voice_file_path)}"
                                os.makedirs(os.path.dirname(offline_path), exist_ok=True)

                                try:
                                    shutil.move(voice_file_path, offline_path)
                                except Exception as e:
                                    print(f"拷贝离线语音失败: {e}")
                                    offline_path = voice_file_path  # 保底使用原路径

                                # 写数据库通知
                                cursor.execute("""
                                    INSERT INTO ta_notification (
                                        sender_id, sender_name, receiver_id, unique_group_id, group_name, content, content_text
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """, (
                                    sender_id,
                                    sender_name,
                                    rid,
                                    group_id,
                                    "语音群聊",
                                    f"离线语音文件: {os.path.basename(offline_path)}",
                                    "6"  # type=6 表示音频消息
                                ))
                                connection.commit()
                            
                            if tc:
                                await tc["ws"].send_bytes(audio_bytes)
                            

                        # 清理临时文件
                        if voice_file_path and os.path.exists(voice_file_path):
                            try:
                                os.remove(voice_file_path)
                            except Exception as e:
                                print(f"删除临时语音文件失败: {e}")

                except Exception as e:
                    print(f"解析音频包失败: {e}")

    except WebSocketDisconnect as exc:
        if user_id in connections:
            connections.pop(user_id, None)
            print(f"用户 {user_id} 离线（外层捕获），当前在线={len(connections)}，详情: {exc}")
        
        # 清理用户从所有临时房间的成员列表中移除
        # 注意：不再因为 WebSocket 断开而自动解散房间，只移除成员，房间是否解散由业务消息控制（如 temp_room_owner_leave）
        for group_id, room_info in list(active_temp_rooms.items()):
            members = room_info.get("members", [])
            if user_id in members:
                members.remove(user_id)
                app_logger.info(f"[webrtc] 用户 {user_id} 离开房间 {group_id}，当前成员数={len(members)}")
                print(f"[webrtc] 用户 {user_id} 离开房间 {group_id}，当前成员数={len(members)}")
        
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
        closed = await safe_close(websocket)
        print(f"[websocket][{user_id}] safe_close called, closed={closed}，当前在线={len(connections)}")
        app_logger.info(f"WebSocket关闭，数据库连接已释放，user_id={user_id}。")

# ====== 心跳检测任务 ======
# @app.on_event("startup")
# async def startup_event():
#     import asyncio
#     asyncio.create_task(heartbeat_checker())

# ===== 心跳检测线程 =====
async def heartbeat_checker():
    try:
        while not stop_event.is_set():
            now = time.time()
            to_remove = []
            for uid, conn in list(connections.items()):
                if now - conn["last_heartbeat"] > 30:
                    print(f"用户 {uid} 心跳超时，断开连接")
                    await safe_close(conn["ws"], 1001, "Heartbeat timeout")
                    to_remove.append(uid)
            for uid in to_remove:
                connections.pop(uid, None)  # 安全移除
                # 清理用户从所有临时房间的成员列表中移除
                # 注意：不再因为心跳超时自动解散房间，只移除成员，房间是否解散由业务消息控制（如 temp_room_owner_leave）
                for group_id, room_info in list(active_temp_rooms.items()):
                    members = room_info.get("members", [])
                    if uid in members:
                        members.remove(uid)
                        print(f"[webrtc] 心跳超时：用户 {uid} 离开房间 {group_id}，当前成员数={len(members)}")
            await asyncio.sleep(10)
    except asyncio.CancelledError:
        print("heartbeat_checker 已安全退出")


# ====== 像 Flask 那样可直接运行 ======
if __name__ == "__main__":
    import uvicorn
    print("服务已启动: http://0.0.0.0:5000")
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)

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

# ===== 停止事件，用于控制心跳协程退出 =====
stop_event = asyncio.Event()

from contextlib import asynccontextmanager
# ===== 生命周期管理 =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    global stop_event
    stop_event.clear()

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

@app.get("/course-schedule")
async def api_get_course_schedule(
    request: Request,
    class_id: str = Query(..., description="班级ID"),
    term: str = Query(..., description="学期，如 2025-2026-1")
):
    """
    查询课程表：根据 (class_id, term) 返回课表头与单元格列表。
    返回 JSON:
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
    """
    connection = get_db_connection()
    if connection is None:
        return safe_json_response({'message': '数据库连接失败', 'code': 500}, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)
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
            days = header.get('days_json')
        try:
            times = json.loads(header['times_json']) if header.get('times_json') else []
        except Exception:
            times = header.get('times_json')

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
    scores: List[Dict] = None
) -> Dict[str, object]:
    """
    保存学生成绩表
    参数说明：
    - class_id: 班级ID（必需）
    - exam_name: 考试名称（必需，如"期中考试"、"期末考试"）
    - term: 学期（可选，如 '2025-2026-1'）
    - remark: 备注（可选）
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

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Save student scores failed: Database connection error.")
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '数据库连接失败' }

    try:
        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # 1. 插入或获取成绩表头
        cursor.execute(
            "SELECT id FROM ta_student_score_header WHERE class_id = %s AND exam_name = %s AND (%s IS NULL OR term = %s) LIMIT 1",
            (class_id, exam_name, term, term)
        )
        header_row = cursor.fetchone()

        if header_row is None:
            # 插入新表头
            insert_header_sql = (
                "INSERT INTO ta_student_score_header (class_id, exam_name, term, remark, created_at) "
                "VALUES (%s, %s, %s, %s, NOW())"
            )
            cursor.execute(insert_header_sql, (class_id, exam_name, term, remark))
            score_header_id = cursor.lastrowid
        else:
            score_header_id = header_row['id']
            # 更新表头信息（若存在）
            if remark is not None:
                cursor.execute(
                    "UPDATE ta_student_score_header SET remark = %s, updated_at = NOW() WHERE id = %s",
                    (remark, score_header_id)
                )
            # 删除旧的成绩明细（重新上传时覆盖）
            cursor.execute("DELETE FROM ta_student_score_detail WHERE score_header_id = %s", (score_header_id,))

        # 2. 批量插入成绩明细
        insert_detail_sql = (
            "INSERT INTO ta_student_score_detail "
            "(score_header_id, student_id, student_name, chinese, math, english, total_score) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        )
        
        inserted_count = 0
        for score_item in scores:
            student_id = score_item.get('student_id')
            student_name = score_item.get('student_name', '').strip()
            if not student_name:
                continue  # 跳过没有姓名的记录
            
            chinese = score_item.get('chinese')
            math = score_item.get('math')
            english = score_item.get('english')
            
            # 计算总分（如果未提供或需要重新计算）
            total_score = score_item.get('total_score')
            if total_score is None:
                # 自动计算总分（只计算提供的科目）
                total_score = 0.0
                if chinese is not None:
                    total_score += float(chinese)
                if math is not None:
                    total_score += float(math)
                if english is not None:
                    total_score += float(english)
            
            cursor.execute(insert_detail_sql, (
                score_header_id,
                student_id,
                student_name,
                chinese,
                math,
                english,
                total_score
            ))
            inserted_count += 1

        connection.commit()
        return { 'success': True, 'score_header_id': score_header_id, 'inserted_count': inserted_count, 'message': '保存成功' }
    except mysql.connector.Error as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Database error during save_student_scores: {e}")
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': f'数据库错误: {e}' }
    except Exception as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Unexpected error during save_student_scores: {e}")
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': f'未知错误: {e}' }
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after saving student scores.")

@app.post("/student-scores/save")
async def api_save_student_scores(request: Request):
    """
    保存学生成绩表
    请求体 JSON:
    {
      "class_id": "class_1001",
      "exam_name": "期中考试",
      "term": "2025-2026-1",  // 可选
      "remark": "备注信息",    // 可选
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
    try:
        data = await request.json()
        # 打印接收到的 JSON 数据
        print(f"[student-scores/save] 收到请求数据:")
        print(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception:
        return safe_json_response({'message': '无效的 JSON 请求体', 'code': 400}, status_code=400)

    class_id = data.get('class_id')
    exam_name = data.get('exam_name')
    term = data.get('term')
    remark = data.get('remark')
    scores = data.get('scores', [])

    if not class_id or not exam_name:
        return safe_json_response({'message': '缺少必要参数 class_id 或 exam_name', 'code': 400}, status_code=400)

    result = save_student_scores(
        class_id=class_id,
        exam_name=exam_name,
        term=term,
        remark=remark,
        scores=scores
    )

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
                "SELECT id, class_id, exam_name, term, remark, created_at, updated_at "
                "FROM ta_student_score_header "
                "WHERE class_id = %s AND exam_name = %s AND (%s IS NULL OR term = %s)",
                (class_id, exam_name, term, term)
            )
        else:
            cursor.execute(
                "SELECT id, class_id, exam_name, term, remark, created_at, updated_at "
                "FROM ta_student_score_header "
                "WHERE class_id = %s AND (%s IS NULL OR term = %s) "
                "ORDER BY created_at DESC",
                (class_id, term, term)
            )
        
        headers = cursor.fetchall() or []
        
        # 查询每个表头的成绩明细
        result_headers = []
        for header in headers:
            score_header_id = header['id']
            cursor.execute(
                "SELECT id, student_id, student_name, chinese, math, english, total_score "
                "FROM ta_student_score_detail "
                "WHERE score_header_id = %s "
                "ORDER BY total_score DESC, student_name ASC",
                (score_header_id,)
            )
            scores = cursor.fetchall() or []
            
            header_dict = {
                'id': header['id'],
                'class_id': header['class_id'],
                'exam_name': header['exam_name'],
                'term': header.get('term'),
                'remark': header.get('remark'),
                'created_at': header.get('created_at'),
                'updated_at': header.get('updated_at'),
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
        
        # 查询成绩明细
        score_header_id = header['id']
        print(f"[student-scores/get] 查询成绩明细 - score_header_id: {score_header_id}")
        app_logger.info(f"[student-scores/get] 开始查询成绩明细 - score_header_id: {score_header_id}")
        cursor.execute(
            "SELECT id, student_id, student_name, chinese, math, english, total_score "
            "FROM ta_student_score_detail "
            "WHERE score_header_id = %s "
            "ORDER BY total_score DESC, student_name ASC",
            (score_header_id,)
        )
        scores = cursor.fetchall() or []
        
        print(f"[student-scores/get] 查询到 {len(scores)} 条成绩明细")
        app_logger.info(f"[student-scores/get] 查询到 {len(scores)} 条成绩明细 - score_header_id: {score_header_id}")
        
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
            'created_at': header.get('created_at'),
            'updated_at': header.get('updated_at'),
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
    data = await request.json()
    print(f"[updateUserInfo] Received payload: {data}")
    phone = data.get('phone')
    id_number = data.get('id_number')
    avatar = data.get('avatar')

    if not id_number or not avatar:
        app_logger.warning("UpdateUserInfo failed: Missing id_number or avatar.")
        print(f"[updateUserInfo] Missing id_number or avatar -> id_number={id_number}, avatar_present={avatar is not None}")
        return JSONResponse({'data': {'message': '身份证号码和头像必须提供', 'code': 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("UpdateUserInfo failed: Database connection error.")
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    try:
        avatar_bytes = base64.b64decode(avatar)
    except Exception as e:
        app_logger.error(f"UpdateUserInfo failed: Avatar decode error for {id_number}: {e}")
        print(f"[updateUserInfo] Avatar decode error for id_number={id_number}: {e}")
        return JSONResponse({'data': {'message': '头像数据解析失败', 'code': 400}}, status_code=400)

    filename = f"{id_number}_.png"
    file_path = os.path.join(IMAGE_DIR, filename)
    with open(file_path, "wb") as f:
        f.write(avatar_bytes)

    cursor = None
    user_details: Optional[Dict[str, Any]] = None
    tencent_identifier: Optional[str] = None
    try:
        update_query = "UPDATE ta_user_details SET avatar = %s WHERE id_number = %s"
        cursor = connection.cursor(dictionary=True)
        print(f"[updateUserInfo] SQL -> {update_query}, params=({file_path}, {id_number})")
        cursor.execute(update_query, (file_path, id_number))
        if cursor.rowcount == 0:
            cursor.execute(
                "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE id_number = %s",
                (id_number,)
            )
            user_details = cursor.fetchone()
            if not user_details and phone:
                cursor.execute(
                    "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE phone = %s",
                    (phone,)
                )
                user_details = cursor.fetchone()
                print(f"[updateUserInfo] Fallback by phone={phone}, fetched user_details={user_details}")
            else:
                print(f"[updateUserInfo] Found user_details by id_number={id_number}: {user_details}")

            if not user_details:
                cursor.execute(
                    "SELECT avatar FROM ta_user_details WHERE id_number = %s",
                    (id_number,)
                )
                existing_avatar_row = cursor.fetchone()
                existing_avatar = existing_avatar_row["avatar"] if existing_avatar_row else None
                print(f"[updateUserInfo] No ta_user_details record affected for id_number={id_number}, "
                      f"existing avatar in DB: {existing_avatar}")
                connection.commit()
                app_logger.warning(f"UpdateUserInfo: No user_details record found for id_number={id_number}")
                return JSONResponse({'data': {'message': '未找到对应的用户信息', 'code': 404}}, status_code=404)
        else:
            connection.commit()
            cursor.execute(
                "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE id_number = %s",
                (id_number,)
            )
            user_details = cursor.fetchone()
            if not user_details and phone:
                cursor.execute(
                    "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE phone = %s",
                    (phone,)
                )
                user_details = cursor.fetchone()
                print(f"[updateUserInfo] Fallback by phone={phone}, fetched user_details={user_details}")
            else:
                print(f"[updateUserInfo] Found user_details by id_number={id_number}: {user_details}")

        tencent_identifier = resolve_tencent_identifier(connection, id_number=id_number, phone=phone)
        print(f"[updateUserInfo] Resolved Tencent identifier={tencent_identifier}")

    except Error as e:
        app_logger.error(f"Database error during updateUserInfo for {phone}: {e}")
        return JSONResponse({'data': {'message': '更新失败', 'code': 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after updating user info for {phone}.")

    name_for_sync = None
    avatar_for_sync = None
    if user_details:
        name_for_sync = user_details.get("name")
        avatar_for_sync = user_details.get("avatar") or file_path
    else:
        avatar_for_sync = file_path

    print(f"[updateUserInfo] Tencent sync request -> identifier={tencent_identifier or id_number}, "
          f"name={name_for_sync}, avatar={avatar_for_sync}")
    app_logger.info(
        f"updateUserInfo: 准备同步腾讯用户资料 identifier={tencent_identifier or id_number}, "
        f"name={name_for_sync}, avatar={avatar_for_sync}"
    )
    tencent_sync_summary = await notify_tencent_user_profile(
        tencent_identifier or id_number,
        name=name_for_sync,
        avatar_url=avatar_for_sync
    )
    print(f"[updateUserInfo] Tencent sync response <- {tencent_sync_summary}")
    app_logger.info(f"updateUserInfo: 腾讯接口返回 {tencent_sync_summary}")

    return JSONResponse({'data': {'message': '更新成功', 'code': 200, 'tencent_sync': tencent_sync_summary}})


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
            if avatar_path:
                full_path = os.path.join(IMAGE_DIR, avatar_path)
                if os.path.exists(full_path):
                    try:
                        with open(full_path, "rb") as img:
                            user["avatar_base64"] = base64.b64encode(img.read()).decode("utf-8")
                    except Exception as e:
                        app_logger.error(f"读取图片失败 {full_path}: {e}")
                        user["avatar_base64"] = None
                else:
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
            if avatar_path:
                #full_path = os.path.join(IMAGE_DIR, avatar_path)
                full_path = avatar_path
                print(full_path)
                if os.path.exists(full_path):
                    try:
                        with open(full_path, "rb") as img:
                            group["avatar_base64"] = base64.b64encode(img.read()).decode("utf-8")
                    except Exception as e:
                        app_logger.error(f"读取图片失败 {full_path}: {e}")
                        group["avatar_base64"] = None
                else:
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
            if avatar_path:
                #full_path = os.path.join(IMAGE_DIR, avatar_path)
                full_path = avatar_path
                print(full_path)
                if os.path.exists(full_path):
                    try:
                        with open(full_path, "rb") as img:
                            group["avatar_base64"] = base64.b64encode(img.read()).decode("utf-8")
                    except Exception as e:
                        app_logger.error(f"读取图片失败 {full_path}: {e}")
                        group["avatar_base64"] = None
                else:
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
            group_info = {
                "group_id": row.get("group_id"),
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
    群主移除群成员
    接收客户端发送的 group_id, user_id
    从 group_members 表中删除该用户，并更新群组的成员数量
    """
    print("=" * 80)
    print("[groups/remove-member] 收到移除成员请求")
    
    # 打印请求头信息用于调试
    content_type = request.headers.get("content-type", "")
    content_length = request.headers.get("content-length", "")
    print(f"[groups/remove-member] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")
    
    try:
        # 解析请求体JSON数据
        try:
            # 先尝试读取原始body
            body_bytes = await request.body()
            print(f"[groups/remove-member] 读取到请求体长度: {len(body_bytes)} 字节")
            
            if not body_bytes:
                print("[groups/remove-member] 错误: 请求体为空")
                return JSONResponse({
                    "code": 400,
                    "message": "请求体不能为空"
                }, status_code=400)
            
            # 解析JSON
            try:
                data = json.loads(body_bytes.decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"[groups/remove-member] 错误: JSON解析失败 - {e}")
                print(f"[groups/remove-member] 请求体内容: {body_bytes.decode('utf-8', errors='ignore')}")
                return JSONResponse({
                    "code": 400,
                    "message": "请求数据格式错误，无法解析JSON"
                }, status_code=400)
                
        except ClientDisconnect:
            print("[groups/remove-member] 错误: 客户端断开连接")
            print(f"[groups/remove-member] 调试信息 - Content-Type: {content_type}, Content-Length: {content_length}")
            app_logger.warning("[groups/remove-member] 客户端在请求完成前断开连接")
            return JSONResponse({
                "code": 400,
                "message": "客户端断开连接，请检查请求数据是否正确发送"
            }, status_code=400)
        except Exception as e:
            print(f"[groups/remove-member] 读取请求体时发生异常: {type(e).__name__} - {e}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[groups/remove-member] 错误堆栈: {traceback_str}")
            return JSONResponse({
                "code": 400,
                "message": f"读取请求数据失败: {str(e)}"
            }, status_code=400)
        
        print(f"[groups/remove-member] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        group_id = data.get('group_id')
        user_id = data.get('user_id')
        
        print(f"[groups/remove-member] 解析结果 - group_id: {group_id}, user_id: {user_id}")
        
        # 参数验证
        if not group_id:
            print("[groups/remove-member] 错误: 缺少 group_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 group_id"
            }, status_code=400)
        
        if not user_id:
            print("[groups/remove-member] 错误: 缺少 user_id")
            return JSONResponse({
                "code": 400,
                "message": "缺少必需参数 user_id"
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
            cursor = connection.cursor(dictionary=True)
            
            # 1. 检查群组是否存在
            print(f"[groups/remove-member] 检查群组 {group_id} 是否存在...")
            cursor.execute("SELECT group_id, group_name, member_num FROM `groups` WHERE group_id = %s", (group_id,))
            group_info = cursor.fetchone()
            
            if not group_info:
                print(f"[groups/remove-member] 错误: 群组 {group_id} 不存在")
                return JSONResponse({
                    "code": 404,
                    "message": "群组不存在"
                }, status_code=404)
            
            print(f"[groups/remove-member] 群组信息: {group_info}")
            
            # 2. 检查要删除的成员是否在群组中
            print(f"[groups/remove-member] 检查用户 {user_id} 是否在群组 {group_id} 中...")
            cursor.execute(
                "SELECT group_id, user_id, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id)
            )
            member_info = cursor.fetchone()
            
            if not member_info:
                print(f"[groups/remove-member] 错误: 用户 {user_id} 不在群组 {group_id} 中")
                return JSONResponse({
                    "code": 400,
                    "message": "该用户不在群组中"
                }, status_code=400)
            
            print(f"[groups/remove-member] 成员信息: {member_info}")
            self_role = member_info.get('self_role', 200)
            
            # 3. 检查要删除的成员是否是群主（self_role = 400 表示群主）
            if self_role == 400:
                print(f"[groups/remove-member] 错误: 用户 {user_id} 是群主，不允许被踢出")
                return JSONResponse({
                    "code": 400,
                    "message": "群主不能被踢出群组"
                }, status_code=400)
            
            # 4. 从群组中删除该成员
            print(f"[groups/remove-member] 从群组 {group_id} 中删除用户 {user_id}...")
            cursor.execute(
                "DELETE FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id)
            )
            affected_rows = cursor.rowcount
            print(f"[groups/remove-member] 删除成员完成, 影响行数: {affected_rows}")
            
            if affected_rows == 0:
                print(f"[groups/remove-member] 警告: 删除操作未影响任何行")
                return JSONResponse({
                    "code": 500,
                    "message": "移除成员失败"
                }, status_code=500)
            
            # 5. 更新群组的成员数量（确保不会小于0）
            print(f"[groups/remove-member] 更新群组 {group_id} 的成员数量...")
            # 使用 CASE 语句避免 UNSIGNED 类型溢出问题
            # 当 member_num 为 0 时，member_num - 1 会导致 UNSIGNED 溢出错误
            cursor.execute(
                "UPDATE `groups` SET member_num = CASE WHEN member_num > 0 THEN member_num - 1 ELSE 0 END WHERE group_id = %s",
                (group_id,)
            )
            print(f"[groups/remove-member] 群组成员数量已更新")
            
            # 提交事务
            connection.commit()
            print(f"[groups/remove-member] 事务提交成功")
            
            result = {
                "code": 200,
                "message": "成功移除成员",
                "data": {
                    "group_id": group_id,
                    "user_id": user_id
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
                gm.unread_num
            FROM `group_members` gm
            WHERE gm.group_id = %s
            ORDER BY gm.join_time ASC
        """
        print(f"[groups/members] 执行SQL查询: {sql}")
        print(f"[groups/members] 查询参数: group_id={group_id}")
        
        cursor.execute(sql, (group_id,))
        members = cursor.fetchall()
        
        print(f"[groups/members] 查询结果: 找到 {len(members)} 个成员")
        
        # 转换 datetime 为字符串
        for idx, member in enumerate(members):
            print(f"[groups/members] 处理第 {idx+1} 个成员: user_id={member.get('user_id')}, self_role={member.get('self_role')}")
            for key, value in member.items():
                if isinstance(value, datetime.datetime):
                    member[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[groups/members] 转换时间字段 {key}: {member[key]}")
        
        result = {
            "data": {
                "message": "查询成功",
                "code": 200,
                "group_id": group_id,
                "members": members,
                "member_count": len(members)
            }
        }
        
        print(f"[groups/members] 返回结果: group_id={group_id}, member_count={len(members)}")
        print("=" * 80)
        
        return JSONResponse(result, status_code=200)

    except mysql.connector.Error as e:
        error_msg = f"查询群成员错误: {e}"
        print(f"[groups/members] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/members] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/members] {error_msg}\n{traceback_str}")
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500
            }
        }, status_code=500)
    except Exception as e:
        error_msg = f"查询群成员时发生异常: {e}"
        print(f"[groups/members] {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[groups/members] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/members] {error_msg}\n{traceback_str}")
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
                    member_info = group.get('member_info')
                    print(f"[groups/sync] 群组 {group_id} 的成员信息: {member_info}")
                    if member_info:
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
                app_logger.info(f"📌 Step3: ta_teacher for friendcode={friendcode} -> {teacher_rows}")
            if not teacher_rows:
                continue
            friend_teacher = teacher_rows[0]

            # 查 ta_user_details
            id_number = friend_teacher.get("id_card")
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM ta_user_details WHERE id_number=%s", (id_number,))
                user_rows = cursor.fetchall()
                app_logger.info(f"📌 Step4: ta_user_details for id_number={id_number} -> {user_rows}")
            user_details = user_rows[0] if user_rows else None

            avatar_path = user_details.get("avatar")
            if avatar_path:
                full_path = os.path.join(IMAGE_DIR, avatar_path)
                if os.path.exists(full_path):
                    try:
                        with open(full_path, "rb") as img:
                            user_details["avatar_base64"] = base64.b64encode(img.read()).decode("utf-8")
                    except Exception as e:
                        app_logger.error(f"读取图片失败 {full_path}: {e}")
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
            app_logger.info(f"📌 Step5: combined record -> {combined}")
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
    await websocket.accept()
    connections[user_id] = {"ws": websocket, "last_heartbeat": time.time()}
    print(f"用户 {user_id} 已连接")

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        app_logger.error("Database connection error in /friends API.")
        return JSONResponse({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }, status_code=500)

    cursor = None
    try:
        # 查询条件改为：receiver_id = user_id 或 sender_id = user_id，并且 is_read = 0
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

        while True:
            try:
                message = await websocket.receive()
            except WebSocketDisconnect:
                # 正常断开
                print(f"用户 {user_id} 断开（WebSocketDisconnect）")
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
                            unique_group_id = str(uuid.uuid4())

                            cursor.execute(
                                "INSERT INTO ta_group (permission_level, headImage_path, group_type, nickname, unique_group_id, group_admin_id, school_id, class_id, create_time)"
                                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                                (msg_data1.get('permission_level'),
                                msg_data1.get('headImage_path'),
                                msg_data1.get('group_type'),
                                msg_data1.get('nickname'),
                                unique_group_id,
                                msg_data1.get('owner_id'),
                                msg_data1.get('school_id'),
                                msg_data1.get('class_id'))
                            )

                            for m in msg_data1['members']:
                                cursor.execute(
                                    "INSERT INTO ta_group_member_relation (unique_member_id, unique_group_id, join_time, group_role, member_name)"
                                    " VALUES (%s,%s,NOW(),%s,%s)",
                                    (m['unique_member_id'], unique_group_id, m['group_role'], m['member_name'])
                                )

                            connection.commit()
                            # 给在线成员推送
                            for m in msg_data1['members']:
                                target_conn = connections.get(m['unique_member_id'])
                                if target_conn:
                                    await target_conn["ws"].send_text(json.dumps({
                                        "type":"notify",
                                        "message":f"你已加入群: {msg_data1['nickname']}",
                                        "group_id": unique_group_id,
                                        "groupname": msg_data1.get('nickname')
                                    }))
                                else:
                                    print(m['unique_member_id'], " 不在线", ", 来自:", user_id)
                                    cursor = connection.cursor(dictionary=True)

                                    update_query = """
                                            INSERT INTO ta_notification (sender_id, sender_name, receiver_id, unique_group_id, group_name, content, content_text)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        """
                                    cursor.execute(update_query, (user_id, msg_data1.get('owner_name'), m['unique_member_id'], unique_group_id, msg_data1.get("nickname"), "邀请你加入了群", msg_data1['type']))
                                    connection.commit()

                            #把创建成功的群信息发回给创建者
                            await websocket.send_text(json.dumps({
                                        "type":"3",
                                        "message":f"你创建了群: {msg_data1['nickname']}",
                                        "group_id": unique_group_id,
                                        "groupname": msg_data1.get('nickname')
                                    }))

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
        
                    else:
                        print(" 格式错误")
                        await websocket.send_text("格式错误: to:<target_id>:<消息>")
                else:
                    print(data)
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

    except WebSocketDisconnect:
        if user_id in connections:
            connections.pop(user_id, None)
            print(f"用户 {user_id} 离线")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
        await safe_close(websocket)
        app_logger.info(f"WebSocket关闭，数据库连接已释放。")

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
            await asyncio.sleep(10)
    except asyncio.CancelledError:
        print("heartbeat_checker 已安全退出")


# ====== 像 Flask 那样可直接运行 ======
if __name__ == "__main__":
    import uvicorn
    print("服务已启动: http://0.0.0.0:5000")
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)

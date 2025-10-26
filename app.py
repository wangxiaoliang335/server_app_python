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
from fastapi import FastAPI, Query
from typing import List, Dict, Optional
#import session
from logging.handlers import TimedRotatingFileHandler
from typing import Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, File, UploadFile, Form
from fastapi.responses import JSONResponse
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.encoders import jsonable_encoder
from dotenv import load_dotenv
#from datetime import datetime
import jwt
import asyncio

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

# 验证码有效期 (秒)
VERIFICATION_CODE_EXPIRY = 300 # 5分钟

from werkzeug.utils import secure_filename

IMAGE_DIR = "./group_images"  # 群组头像目录
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
        cursor.execute("SELECT MAX(id) AS max_id FROM ta_school")
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
            params.append(int(school_id))
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
    phone = data.get('phone')
    id_number = data.get('id_number')
    avatar = data.get('avatar')

    if not id_number or not avatar:
        app_logger.warning("UpdateUserInfo failed: Missing id_number or avatar.")
        return JSONResponse({'data': {'message': '身份证号码和头像必须提供', 'code': 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("UpdateUserInfo failed: Database connection error.")
        return JSONResponse({'data': {'message': '数据库连接失败', 'code': 500}}, status_code=500)

    avatar_bytes = base64.b64decode(avatar)
    filename = f"{id_number}_.png"
    file_path = os.path.join(IMAGE_DIR, filename)
    with open(file_path, "wb") as f:
        f.write(avatar_bytes)

    cursor = None
    try:
        update_query = "UPDATE ta_user_details SET avatar = %s WHERE id_number = %s"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(update_query, (file_path, id_number))
        connection.commit()
        cursor.close()
        return JSONResponse({'data': {'message': '更新成功', 'code': 200}})
    except Error as e:
        app_logger.error(f"Database error during updateUserInfo for {phone}: {e}")
        return JSONResponse({'data': {'message': '更新失败', 'code': 500}}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after updating user info for {phone}.")


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
            SELECT u.*, t.teacher_unique_id
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
            class_code, school_stage, grade, class_name, remark, created_at
        ) VALUES (%s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            school_stage = VALUES(school_stage),
            grade        = VALUES(grade),
            class_name   = VALUES(class_name),
            remark       = VALUES(remark),
            created_at   = VALUES(created_at);
        """
        values = []
        for item in data_list:
            if not item.get('class_code'):
                continue
            values.append((
                item.get('class_code'),
                item.get('school_stage'),
                item.get('grade'),
                item.get('class_name'),
                item.get('remark')
            ))
        if values:
            cursor.executemany(sql, values)
            connection.commit()
        cursor.close()
        connection.close()
        return safe_json_response({'data': {'message': '批量插入/更新完成', 'code': 200, 'count': len(values)}})
    except Error as e:
        return JSONResponse({'data': {'message': f'数据库操作失败: {e}', 'code': 500}}, status_code=500)


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
        SELECT class_code, school_stage, grade, class_name, remark, created_at
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
    """
    connection = get_db_connection()
    if connection is None:
        return None
    cursor = None
    try:
        cursor = connection.cursor()
        connection.start_transaction()
        cursor.execute("""
            SELECT MAX(teacher_unique_id)
            FROM ta_teacher
            WHERE schoolId = %s
            FOR UPDATE
        """, (school_id,))
        result = cursor.fetchone()
        if result and result[0]:
            max_id_str = str(result[0]).zfill(10)
            last_num = int(max_id_str[6:])
            new_num = last_num + 1
        else:
            new_num = 1
        teacher_unique_id_str = f"{str(school_id).zfill(6)}{str(new_num).zfill(4)}"
        return int(teacher_unique_id_str)
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
        sql_insert = """
        INSERT INTO ta_teacher 
        (name, icon, subject, gradeId, schoolId, is_Administarator, phone, id_card, sex, 
         teaching_tenure, education, graduation_institution, major, 
         teacher_certification_level, subjects_of_teacher_qualification_examination, 
         educational_stage, teacher_unique_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s)
        """
        cursor.execute(sql_insert, (
            data.get('name'), data.get('icon'), data.get('subject'), data.get('gradeId'),
            school_id, is_admin_flag, data.get('phone'), data.get('id_card'),
            data.get('sex'), data.get('teaching_tenure'), data.get('education'),
            data.get('graduation_institution'), data.get('major'),
            data.get('teacher_certification_level'),
            data.get('subjects_of_teacher_qualification_examination'),
            data.get('educational_stage'), teacher_unique_id
        ))
        connection.commit()
        teacher_id = cursor.lastrowid
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
            params.append(int(school_id_filter))
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

        if school_id: filters.append("AND school_id = %s"); params.append(int(school_id))
        if class_id: filters.append("AND class_id = %s"); params.append(int(class_id))
        if sender_id_filter: filters.append("AND sender_id = %s"); params.append(int(sender_id_filter))

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
                sender_id = int(sender_id)
            except:
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
            "INSERT INTO ta_group (permission_level, headImage_path, group_type, nickname, unique_group_id, group_admin_id, create_time)"
            " VALUES (%s,%s,%s,%s,%s,%s,NOW())",
            (data.get('permission_level'),
             data.get('headImage_path'),
             data.get('group_type'),
             data.get('nickname'),
             unique_group_id,
             data.get('owner_id'))
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
                                "INSERT INTO ta_group (permission_level, headImage_path, group_type, nickname, unique_group_id, group_admin_id, create_time)"
                                " VALUES (%s,%s,%s,%s,%s,%s,NOW())",
                                (msg_data1.get('permission_level'),
                                msg_data1.get('headImage_path'),
                                msg_data1.get('group_type'),
                                msg_data1.get('nickname'),
                                unique_group_id,
                                msg_data1.get('owner_id'))
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
                        
            elif "bytes" in message:
                audio_bytes = message["bytes"]

                # 解析多字段协议
                offset = 0
                frameType = audio_bytes[offset]
                offset += 1

                if frameType != 6:
                    print("收到非音频类型二进制数据")
                    continue

                # 读取 groupId
                group_len = struct.unpack("<I", audio_bytes[offset:offset+4])[0]
                offset += 4
                group_id = audio_bytes[offset:offset+group_len].decode("utf-8")
                offset += group_len

                # 读取 senderId
                sender_len = struct.unpack("<I", audio_bytes[offset:offset+4])[0]
                offset += 4
                sender_id = audio_bytes[offset:offset+sender_len].decode("utf-8")
                offset += sender_len

                # 读取 senderName
                name_len = struct.unpack("<I", audio_bytes[offset:offset+4])[0]
                offset += 4
                sender_name = audio_bytes[offset:offset+name_len].decode("utf-8")
                offset += name_len

                # 读取 timestamp
                ts = struct.unpack("<Q", audio_bytes[offset:offset+8])[0]
                offset += 8

                # 读取 AAC 数据
                aac_len = struct.unpack("<I", audio_bytes[offset:offset+4])[0]
                offset += 4
                aac_data = audio_bytes[offset:offset+aac_len]

                print(f"[音频] 群 {group_id} 来自 {sender_name} ({sender_id}) AAC大小={aac_len}")

                # 查群成员并转发（排除自己）
                cursor.execute("""
                    SELECT unique_member_id 
                    FROM ta_group_member_relation
                    WHERE unique_group_id = %s AND unique_member_id != %s
                """, (group_id, sender_id))
                rows = cursor.fetchall()

                receivers = [r["unique_member_id"] for r in rows]

                # 转发给在线成员 / 存离线文件 + 通知
                for rid in receivers:
                    target_conn = connections.get(rid)
                    if target_conn:
                        await target_conn["ws"].send_bytes(audio_bytes)
                    else:
                        # 离线 -> 保存文件
                        filename = f"{group_id}_{sender_id}_{int(ts)}.aac"
                        with open(filename, "wb") as f:
                            f.write(aac_data)

                        cursor.execute("""
                            INSERT INTO ta_notification (
                                sender_id, sender_name, receiver_id, unique_group_id, group_name, content, content_text
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            sender_id, sender_name, rid, group_id, "语音群聊",
                            f"离线语音文件: {filename}", "6"  # type=6 表示音频
                        ))
                        connection.commit()

    except WebSocketDisconnect:
        if user_id in connections:
            if connections[user_id]:
                del connections[user_id]
                print(f"用户 {user_id} 离线")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
        await safe_close(websocket)    
        app_logger.info(f"Database connection closed.")

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

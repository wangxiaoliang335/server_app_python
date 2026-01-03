import datetime
import hashlib
import os
import random
import secrets
import string
import time

import jwt
from mysql.connector import Error

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest

from common import app_logger, safe_json_response
from db import get_db_connection


router = APIRouter()

# 短信服务配置 (从环境变量读取)
SMS_CONFIG = {
    "access_key_id": os.getenv("ALIYUN_AK_ID"),
    "access_key_secret": os.getenv("ALIYUN_AK_SECRET"),
    "sign_name": os.getenv("ALIYUN_SMS_SIGN"),
    "template_code": os.getenv("ALIYUN_SMS_TEMPLATE"),
}

# 验证码有效期 (秒)
VERIFICATION_CODE_EXPIRY = 300  # 5分钟

# 用一个全局内存缓存（可以替代 Flask session）
verification_memory = {}

# JWT
ALGORITHM = "HS256"
SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "default_key")


def hash_password(password, salt):
    return hashlib.sha256((password + salt).encode("utf-8")).hexdigest()


def generate_verification_code(length=6):
    return "".join(random.choices(string.digits, k=length))


def send_sms_verification_code(phone, code):
    client = AcsClient(SMS_CONFIG["access_key_id"], SMS_CONFIG["access_key_secret"], "cn-hangzhou")
    request = CommonRequest()
    request.set_accept_format("json")
    request.set_domain("dysmsapi.aliyuncs.com")
    request.set_method("POST")
    request.set_protocol_type("https")
    request.set_version("2017-05-25")
    request.set_action_name("SendSms")
    request.add_query_param("RegionId", "cn-hangzhou")
    request.add_query_param("PhoneNumbers", phone)
    request.add_query_param("SignName", SMS_CONFIG["sign_name"])
    request.add_query_param("TemplateCode", SMS_CONFIG["template_code"])
    request.add_query_param("TemplateParam", f'{{"code":"{code}"}}')
    response = client.do_action_with_exception(request)
    print(str(response, encoding="utf-8"))
    return True


def verify_code_from_memory(input_phone, input_code):
    valid_info = verification_memory.get(input_phone)
    if not valid_info:
        app_logger.warning(f"Verification failed for {input_phone}: No code sent or expired.")
        return False, "未发送验证码或验证码已过期"
    elif time.time() > valid_info["expires_at"]:
        verification_memory.pop(input_phone, None)
        app_logger.info(f"Verification code expired for {input_phone}.")
        return False, "验证码已过期"
    elif str(input_code) != str(valid_info["code"]):
        app_logger.warning(f"Verification failed for {input_phone}: Incorrect code entered.")
        return False, "验证码错误"
    else:
        verification_memory.pop(input_phone, None)
        app_logger.info(f"Verification successful for {input_phone}.")
        return True, "验证成功"


def create_access_token(data: dict, expires_delta: int = 30):
    to_encode = data.copy()
    expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=expires_delta)
    to_encode.update({"exp": expire})
    token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return token


@router.post("/send_verification_code")
async def send_verification_code(request: Request):
    """发送短信验证码接口"""
    data = await request.json()
    phone = data.get("phone")

    if not phone:
        app_logger.warning("Send verification code failed: Phone number is missing.")
        return JSONResponse({"data": {"message": "手机号不能为空", "code": 400}}, status_code=400)

    code = generate_verification_code()
    verification_memory[phone] = {"code": code, "expires_at": time.time() + VERIFICATION_CODE_EXPIRY}

    if send_sms_verification_code(phone, code):
        app_logger.info(f"Verification code sent successfully to {phone}.")
        return JSONResponse({"data": {"message": "验证码已发送", "code": 200}})
    else:
        verification_memory.pop(phone, None)
        app_logger.error(f"Failed to send verification code to {phone}.")
        return JSONResponse({"data": {"message": "验证码发送失败", "code": 500}}, status_code=500)


@router.post("/register")
async def register(request: Request):
    data = await request.json()
    phone = data.get("phone")
    password = data.get("password")
    verification_code = data.get("verification_code")

    print(data)

    if not phone or not password or not verification_code:
        app_logger.warning("Registration failed: Missing phone, password, or verification code.")
        return JSONResponse({"data": {"message": "手机号、密码和验证码不能为空", "code": 400}}, status_code=400)

    # 验证验证码（沿用旧逻辑）
    valid_info = verification_memory.get(phone)
    if not valid_info:
        return JSONResponse({"data": {"message": "验证码已失效，请重新获取", "code": 400}}, status_code=400)
    elif time.time() > valid_info["expires_at"]:
        verification_memory.pop(phone, None)
        return JSONResponse({"data": {"message": "验证码已过期，请重新获取", "code": 400}}, status_code=400)
    elif str(verification_code) != str(valid_info["code"]):
        return JSONResponse({"data": {"message": "验证码错误", "code": 400}}, status_code=400)
    else:
        verification_memory.pop(phone, None)

    salt = secrets.token_hex(16)
    password_hash = hash_password(password, salt)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Registration failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM ta_user WHERE phone = %s", (phone,))
        if cursor.fetchone():
            app_logger.info(f"Registration failed for {phone}: Phone number already registered.")
            cursor.close()
            return JSONResponse({"data": {"message": "手机号已注册", "code": 400}}, status_code=400)

        insert_query = """
            INSERT INTO ta_user (phone, password_hash, salt, is_verified, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (phone, password_hash, salt, 1, None))
        connection.commit()
        user_id = cursor.lastrowid
        cursor.close()
        app_logger.info(f"User registered successfully: Phone {phone}, User ID {user_id}.")
        return safe_json_response({"data": {"message": "注册成功", "code": 201, "user_id": user_id}}, status_code=201)
    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error during registration for {phone}: {e}")
        return JSONResponse({"data": {"message": "注册失败", "code": 500}}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after registration attempt.")


@router.post("/login")
async def login(request: Request):
    data = await request.json()
    login_type = data.get("login_type")

    print(f"[login] 收到登录请求，login_type={login_type}, data={data}")
    app_logger.info(f"[login] 收到登录请求，login_type={login_type}")

    # 班级端登录
    if login_type == "class":
        class_number = data.get("class_number")

        if not class_number:
            app_logger.warning("[login] 班级端登录失败：缺少班级唯一编号")
            return JSONResponse({"data": {"message": "班级唯一编号不能为空", "code": 400}}, status_code=400)

        connection = get_db_connection()
        if connection is None:
            app_logger.error("[login] 数据库连接失败")
            return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)
            # 根据班级唯一编号查询班级信息（使用 ta_classes 表）
            cursor.execute(
                """
                SELECT class_code, class_name, school_stage, grade, schoolid, remark, face_url, created_at
                FROM ta_classes
                WHERE class_code = %s
            """,
                (class_number,),
            )
            class_info = cursor.fetchone()

            if not class_info:
                app_logger.warning(f"[login] 班级端登录失败：班级 {class_number} 不存在")
                return JSONResponse({"data": {"message": "没有找到数据：班级不存在", "code": 200}}, status_code=200)

            # 使用班级编号作为 user_id（班级端登录）
            user_id = class_number

            app_logger.info(
                f"[login] 班级端登录成功 - class_number={class_number}, class_name={class_info.get('class_name')}, user_id={user_id}"
            )

            # 生成 token（使用班级编号作为标识）
            token_data = {"sub": class_number, "type": "class"}
            access_token = create_access_token(token_data, expires_delta=60)  # 60分钟有效期

            return safe_json_response(
                {
                    "data": {
                        "message": "登录成功",
                        "code": 200,
                        "access_token": access_token,
                        "token_type": "bearer",
                        "user_id": user_id,
                        "class_code": class_info.get("class_code"),
                        "class_name": class_info.get("class_name"),
                        "school_stage": class_info.get("school_stage"),
                        "grade": class_info.get("grade"),
                        "schoolid": class_info.get("schoolid"),
                        "face_url": class_info.get("face_url"),
                    }
                },
                status_code=200,
            )
        except Exception as e:
            app_logger.error(f"[login] 班级端登录异常: {e}")
            return JSONResponse({"data": {"message": "登录失败", "code": 500}}, status_code=500)
        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()

    # 普通用户登录（手机号+密码/验证码）
    phone = data.get("phone")
    password = data.get("password")
    verification_code = data.get("verification_code")

    print(data)

    if not phone or (not password and not verification_code):
        return JSONResponse({"data": {"message": "手机号和密码或验证码必须提供", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        print(" 数据库连接失败\n")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id, password_hash, salt, is_verified FROM ta_user WHERE phone = %s", (phone,))
        user = cursor.fetchone()

        if not user:
            return JSONResponse({"data": {"message": "没有找到数据：用户不存在", "code": 200}}, status_code=200)
        if not user["is_verified"]:
            return JSONResponse({"data": {"message": "账户未验证", "code": 403}}, status_code=403)

        print(" 111111 phone:", phone, "\n")
        auth_success = False
        if password:
            if hash_password(password, user["salt"]) == user["password_hash"]:
                auth_success = True
            else:
                print(hash_password(password, user["salt"]))
                print(user["password_hash"])
                return JSONResponse({"data": {"message": "密码错误", "code": 401}}, status_code=401)
        elif verification_code:
            is_valid, message = verify_code_from_memory(phone, verification_code)
            if is_valid:
                auth_success = True
            else:
                return JSONResponse({"data": {"message": message, "code": 400}}, status_code=400)

        print(" 111111 auth_success:", auth_success, "\n")
        if auth_success:
            # 登录成功 -> 生成 token
            token_data = {"sub": phone}  # sub: subject，表示用户标识
            access_token = create_access_token(token_data, expires_delta=60)  # 60分钟有效期
            cursor.execute(
                "UPDATE ta_user SET last_login_at = %s WHERE id = %s",
                (datetime.datetime.now(), user["id"]),
            )
            connection.commit()
            return safe_json_response(
                {
                    "data": {
                        "message": "登录成功",
                        "code": 200,
                        "access_token": access_token,
                        "token_type": "bearer",
                        "user_id": user["id"],
                    }
                },
                status_code=200,
            )
    except Exception as e:
        app_logger.error(f"Database error during login: {e}")
        return JSONResponse({"data": {"message": "登录失败", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.post("/verify_and_set_password")
async def verify_and_set_password(request: Request):
    """忘记密码 - 验证并重置密码"""
    data = await request.json()
    phone = data.get("phone")
    verification_code = data.get("verification_code")
    new_password = data.get("new_password")

    if not phone or not verification_code or not new_password:
        app_logger.warning("Password reset failed: Missing phone, verification code, or new password.")
        return JSONResponse(
            {"data": {"message": "手机号、验证码和新密码不能为空", "code": 400}},
            status_code=400,
        )

    # 统一验证码校验方式
    is_valid, message = verify_code_from_memory(phone, verification_code)
    if not is_valid:
        app_logger.warning(f"Password reset failed for {phone}: {message}")
        return JSONResponse({"data": {"message": message, "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Password reset failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM ta_user WHERE phone = %s AND is_verified = 1", (phone,))
        user = cursor.fetchone()

        if not user:
            app_logger.info(f"Password reset failed for {phone}: User not found or not verified.")
            return JSONResponse({"data": {"message": "用户不存在或账户未验证", "code": 400}}, status_code=400)

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
            return JSONResponse({"data": {"message": "更新失败", "code": 500}}, status_code=500)

        app_logger.info(f"Password reset successful for user ID {user[0]}.")
        return safe_json_response({"data": {"message": "密码重置成功", "code": 200}}, status_code=200)

    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error during password reset for {phone}: {e}")
        return JSONResponse({"data": {"message": "密码重置失败", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after password reset attempt for {phone}.")



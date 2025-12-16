import base64
import json
import os
import time
import traceback
from typing import Any, Dict, List, Optional, Union

from mysql.connector import Error

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from common import app_logger, safe_json_response
from db import get_db_connection
from services.avatars import (
    build_public_url_from_local_path,
    resolve_local_avatar_file_path,
    save_avatar_locally,
    upload_avatar_to_oss,
)
from services.tencent_api import resolve_tencent_identifier
from services.tencent_profile import notify_tencent_user_profile
from services.teachings import normalize_teachings_payload, replace_user_teachings


router = APIRouter()


@router.post("/updateUserInfo")
async def updateUserInfo(request: Request):
    """
    更新用户头像（Base64）并同步腾讯资料。
    请求体 JSON:
    {
      "phone": "...",           // 可选
      "id_number": "...",       // 必填（旧逻辑要求）
      "avatar": "base64..."     // 必填（支持 data:image/... 前缀）
    }
    """
    print("=" * 80)
    print("[updateUserInfo] 收到更新用户信息请求")

    connection = None
    cursor = None
    user_details: Optional[Dict[str, Any]] = None
    avatar_url: Optional[str] = None
    avatar_sync_url: Optional[str] = None

    try:
        try:
            body = await request.body()
            print(f"[updateUserInfo] 原始请求体大小: {len(body)} bytes")
            data = await request.json()
        except Exception as e:
            app_logger.error(f"UpdateUserInfo failed: JSON parse error - {type(e).__name__}: {str(e)}")
            return JSONResponse({"data": {"message": f"请求数据解析失败: {str(e)}", "code": 400}}, status_code=400)

        phone = data.get("phone")
        id_number = data.get("id_number")
        avatar = data.get("avatar")

        if not id_number or not avatar:
            app_logger.warning("UpdateUserInfo failed: Missing id_number or avatar.")
            return JSONResponse({"data": {"message": "身份证号码和头像必须提供", "code": 400}}, status_code=400)

        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            app_logger.error("UpdateUserInfo failed: Database connection error.")
            return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

        # 解码 Base64 头像
        try:
            if not isinstance(avatar, str):
                avatar = str(avatar)
            if avatar.startswith("data:image"):
                avatar = avatar.split(",", 1)[1]
            avatar_bytes = base64.b64decode(avatar)
        except Exception as e:
            app_logger.error(f"UpdateUserInfo failed: Avatar decode error for {id_number}: {e}")
            return JSONResponse({"data": {"message": f"头像数据解析失败: {str(e)}", "code": 400}}, status_code=400)

        # 上传 OSS（失败则本地兜底）
        object_name = f"avatars/{id_number}_{int(time.time())}.png"
        try:
            avatar_url = upload_avatar_to_oss(avatar_bytes, object_name)
            avatar_sync_url = avatar_url
            if not avatar_url:
                local_path = save_avatar_locally(avatar_bytes, object_name)
                if not local_path:
                    app_logger.error("UpdateUserInfo failed: OSS 和本地保存均失败")
                    return JSONResponse({"data": {"message": "头像上传失败，请稍后再试", "code": 500}}, status_code=500)
                avatar_url = local_path
                avatar_sync_url = build_public_url_from_local_path(local_path) or local_path
        except Exception as e:
            app_logger.error(f"UpdateUserInfo failed: avatar upload error for {id_number}: {e}")
            return JSONResponse({"data": {"message": f"头像上传失败: {str(e)}", "code": 500}}, status_code=500)

        # 更新数据库
        cursor = connection.cursor(dictionary=True)
        cursor.execute("UPDATE ta_user_details SET avatar = %s WHERE id_number = %s", (avatar_url, id_number))
        if cursor.rowcount == 0:
            connection.rollback()
            return JSONResponse({"data": {"message": "未找到对应的用户信息", "code": 404}}, status_code=404)
        connection.commit()

        cursor.execute("SELECT name, phone, id_number, avatar FROM ta_user_details WHERE id_number = %s", (id_number,))
        user_details = cursor.fetchone()

        tencent_identifier = resolve_tencent_identifier(connection, id_number=id_number, phone=phone)
        name_for_sync = user_details.get("name") if user_details else None
        avatar_for_sync = avatar_sync_url or (user_details.get("avatar") if user_details else None)

        tencent_sync_summary: Optional[Dict[str, Any]] = None
        try:
            tencent_sync_summary = await notify_tencent_user_profile(
                tencent_identifier or id_number or phone,
                name=name_for_sync,
                avatar_url=avatar_for_sync,
            )
        except Exception as e:
            app_logger.error(f"UpdateUserInfo failed: notify_tencent_user_profile error: {type(e).__name__}: {str(e)}")
            tencent_sync_summary = {"status": "error", "error": str(e)}

        return JSONResponse({"data": {"message": "更新成功", "code": 200, "tencent_sync": tencent_sync_summary}})

    except Exception as e:
        app_logger.error(f"UpdateUserInfo failed: Unexpected error - {type(e).__name__}: {str(e)}")
        print(traceback.format_exc())
        return JSONResponse({"data": {"message": f"更新失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if connection and connection.is_connected():
            try:
                connection.close()
            except Exception:
                pass
        print("=" * 80)


@router.post("/updateUserName")
async def update_user_name(request: Request):
    data = await request.json()
    name = data.get("name")
    id_number = data.get("id_number")
    phone = data.get("phone")

    if not name or (not id_number and not phone):
        app_logger.warning("update_user_name failed: Missing name or identifier.")
        return JSONResponse({"data": {"message": "姓名和身份证号码或手机号必须提供", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("update_user_name failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    user_details: Optional[Dict[str, Any]] = None
    effective_id_number: Optional[str] = id_number
    try:
        cursor = connection.cursor(dictionary=True)

        if id_number:
            cursor.execute("UPDATE ta_user_details SET name = %s WHERE id_number = %s", (name, id_number))
        else:
            cursor.execute("UPDATE ta_user_details SET name = %s WHERE phone = %s", (name, phone))
            cursor.execute("SELECT id_number FROM ta_user_details WHERE phone = %s", (phone,))
            row = cursor.fetchone()
            if row:
                effective_id_number = row.get("id_number")

        if cursor.rowcount == 0:
            return JSONResponse({"data": {"message": "未找到对应的用户信息", "code": 404}}, status_code=404)

        if effective_id_number:
            cursor.execute("UPDATE ta_teacher SET name = %s WHERE id_card = %s", (name, effective_id_number))

        connection.commit()

        cursor.execute(
            "SELECT name, phone, id_number, avatar FROM ta_user_details WHERE id_number = %s", (effective_id_number,)
        )
        user_details = cursor.fetchone()
        if not user_details and phone:
            cursor.execute("SELECT name, phone, id_number, avatar FROM ta_user_details WHERE phone = %s", (phone,))
            user_details = cursor.fetchone()

        tencent_identifier = resolve_tencent_identifier(connection, id_number=effective_id_number, phone=phone)

    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error during update_user_name for {id_number or phone}: {e}")
        return JSONResponse({"data": {"message": "用户名更新失败", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

    avatar_for_sync = user_details.get("avatar") if user_details else None
    tencent_sync_summary = await notify_tencent_user_profile(
        tencent_identifier or effective_id_number or phone, name=name, avatar_url=avatar_for_sync
    )
    return JSONResponse({"data": {"message": "用户名更新成功", "code": 200, "tencent_sync": tencent_sync_summary}})


async def _update_user_field(
    phone: Optional[str], field: str, value, field_label: str, id_number: Optional[str] = None
):
    if (not phone and not id_number) or value is None:
        return JSONResponse({"data": {"message": f"手机号或身份证号以及{field_label}必须提供", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        if id_number:
            update_query = f"UPDATE ta_user_details SET {field} = %s WHERE id_number = %s"
            cursor.execute(update_query, (value, id_number))
        else:
            update_query = f"UPDATE ta_user_details SET {field} = %s WHERE phone = %s"
            cursor.execute(update_query, (value, phone))

        if cursor.rowcount == 0:
            connection.commit()
            return JSONResponse({"data": {"message": "未找到对应的用户信息", "code": 404}}, status_code=404)

        connection.commit()
        return JSONResponse({"data": {"message": f"{field_label}更新成功", "code": 200}})
    except Error as e:
        connection.rollback()
        app_logger.error(f"数据库错误: 更新{field_label}失败 phone={phone}: {e}")
        return JSONResponse({"data": {"message": f"{field_label}更新失败", "code": 500}}, status_code=500)
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


@router.post("/updateUserTeachings")
async def update_user_teachings(request: Request):
    """
    更新“一个老师多条任教记录”（写入 ta_user_teachings）。
    """
    data = await request.json()
    phone = (data.get("phone") or "").strip()
    userid = data.get("userid")

    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        if not phone and userid is not None:
            cursor.execute("SELECT phone FROM ta_user WHERE id = %s", (userid,))
            row = cursor.fetchone()
            if not row or not row.get("phone"):
                return JSONResponse({"data": {"message": "未找到该用户", "code": 404}}, status_code=404)
            phone = str(row["phone"]).strip()

        if not phone:
            return JSONResponse({"data": {"message": "缺少必要参数 phone 或 userid", "code": 400}}, status_code=400)

        teachings = normalize_teachings_payload(data)
        if not teachings:
            return JSONResponse({"data": {"message": "teachings 不能为空", "code": 400}}, status_code=400)

        replace_user_teachings(cursor, phone, teachings)

        connection.commit()
        return safe_json_response({"data": {"message": "任教信息更新成功", "code": 200, "teachings": teachings}})
    except Error as e:
        if getattr(e, "errno", None) == 1062:
            connection.rollback()
            return JSONResponse(
                {"data": {"message": "任教记录重复（同一教师、学段/年级/科目/班级不能重复）", "code": 409}}, status_code=409
            )
        connection.rollback()
        app_logger.error(f"数据库错误: 更新任教信息失败 phone={phone}: {e}")
        return JSONResponse({"data": {"message": "任教信息更新失败", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.post("/updateUserAdministrator")
async def update_user_administrator(request: Request):
    data = await request.json()
    phone = data.get("phone")
    id_number = data.get("id_number")
    is_administrator_raw = data.get("is_administrator")
    normalized_value = _normalize_is_administrator(is_administrator_raw)

    if normalized_value is None:
        return JSONResponse({"data": {"message": "管理员状态不能为空", "code": 400}}, status_code=400)

    return await _update_user_field(phone, "is_administrator", normalized_value, "管理员状态", id_number=id_number)


@router.post("/updateUserSex")
async def update_user_sex(request: Request):
    data = await request.json()
    phone = data.get("phone")
    id_number = data.get("id_number")
    sex = data.get("sex")
    return await _update_user_field(phone, "sex", sex, "性别", id_number=id_number)


@router.post("/updateUserAddress")
async def update_user_address(request: Request):
    data = await request.json()
    phone = data.get("phone")
    id_number = data.get("id_number")
    address = data.get("address")
    return await _update_user_field(phone, "address", address, "地址", id_number=id_number)


@router.post("/updateUserSchoolName")
async def update_user_school_name(request: Request):
    data = await request.json()
    phone = data.get("phone")
    id_number = data.get("id_number")
    school_name = data.get("school_name")
    return await _update_user_field(phone, "school_name", school_name, "学校名称", id_number=id_number)


@router.post("/updateUserGradeLevel")
async def update_user_grade_level(request: Request):
    data = await request.json()
    phone = data.get("phone")
    id_number = data.get("id_number")
    grade_level = data.get("grade_level")
    return await _update_user_field(phone, "grade_level", grade_level, "学段", id_number=id_number)


@router.get("/userInfo")
async def list_userInfo(request: Request):
    connection = get_db_connection()
    if connection is None:
        app_logger.error("Get User Info failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500, "userinfo": []}}, status_code=500)

    cursor = None
    try:
        phone_filter = request.query_params.get("phone")
        user_id_filter = request.query_params.get("userid")

        if not phone_filter and user_id_filter:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT phone FROM ta_user WHERE id = %s", (user_id_filter,))
            user_row = cursor.fetchone()
            if not user_row:
                return JSONResponse({"data": {"message": "未找到该用户", "code": 404, "userinfo": []}}, status_code=404)
            phone_filter = user_row["phone"]
            cursor.close()
            cursor = None

        if not phone_filter:
            return JSONResponse(
                {"data": {"message": "缺少必要参数 phone 或 userid", "code": 400, "userinfo": []}}, status_code=400
            )

        base_query = """
            SELECT u.*, t.teacher_unique_id, t.schoolId AS schoolId
            FROM ta_user_details AS u
            LEFT JOIN ta_teacher AS t ON u.id_number = t.id_card
            WHERE u.phone = %s
        """
        cursor = connection.cursor(dictionary=True)
        cursor.execute(base_query, (phone_filter,))
        userinfo = cursor.fetchall()

        for user in userinfo:
            cursor.execute(
                """
                SELECT grade_level, grade, subject, class_taught, sort_order
                FROM ta_user_teachings
                WHERE phone = %s
                ORDER BY sort_order ASC, id ASC
                """,
                (phone_filter,),
            )
            user["teachings"] = cursor.fetchall() or []

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

        return safe_json_response({"data": {"message": "获取用户信息成功", "code": 200, "userinfo": userinfo}})

    except Error as e:
        app_logger.error(f"Database error during fetching userinfo: {e}")
        return JSONResponse({"data": {"message": "获取用户信息失败", "code": 500, "userinfo": []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching userinfo: {e}")
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500, "userinfo": []}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.get("/getUserInfo")
async def get_user_info_alias(request: Request):
    # 兼容路由别名：部分客户端仍调用 /getUserInfo
    return await list_userInfo(request)



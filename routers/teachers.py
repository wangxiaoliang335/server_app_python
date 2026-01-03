import base64
import datetime
import json
import os
import uuid
from typing import Any, Dict, List, Optional

import mysql.connector
from mysql.connector import Error

from fastapi import APIRouter, File, Form, Query, Request, UploadFile
from fastapi.responses import JSONResponse

from common import app_logger, safe_json_response
from db import get_db_connection
from services.avatars import resolve_local_avatar_file_path
from services.teachings import normalize_teachings_payload, replace_user_teachings


router = APIRouter()


def remove_icon_from_teacher_data(teacher_data):
    """
    从教师数据中移除 icon 字段（不再使用）
    支持单个字典或字典列表
    """
    if teacher_data is None:
        return teacher_data
    
    if isinstance(teacher_data, dict):
        if "icon" in teacher_data:
            del teacher_data["icon"]
    elif isinstance(teacher_data, list):
        for item in teacher_data:
            if isinstance(item, dict) and "icon" in item:
                del item["icon"]
    
    return teacher_data


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
        cursor.execute(
            """
            SELECT teacher_unique_id
            FROM ta_teacher
            WHERE schoolId = %s
            ORDER BY CAST(teacher_unique_id AS UNSIGNED) DESC
            LIMIT 1
            FOR UPDATE
        """,
            (school_id,),
        )
        result = cursor.fetchone()
        if result and result[0]:
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
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.post("/add_teacher")
async def add_teacher(request: Request):
    data = await request.json()
    if not data or "schoolId" not in data:
        return JSONResponse({"data": {"message": "缺少 schoolId", "code": 400}}, status_code=400)

    school_id = data["schoolId"]
    teacher_unique_id = generate_teacher_unique_id(school_id)
    if teacher_unique_id is None:
        return JSONResponse({"data": {"message": "生成教师唯一编号失败", "code": 500}}, status_code=500)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Add teacher failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    is_admin_flag = data.get("is_Administarator")
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
        cursor.execute(
            sql_insert,
            (
                generated_teacher_id,
                data.get("name"),
                data.get("icon"),
                data.get("subject"),
                data.get("gradeId"),
                school_id,
                is_admin_flag,
                data.get("phone"),
                data.get("id_card"),
                data.get("sex"),
                data.get("teaching_tenure"),
                data.get("education"),
                data.get("graduation_institution"),
                data.get("major"),
                data.get("teacher_certification_level"),
                data.get("subjects_of_teacher_qualification_examination"),
                data.get("educational_stage"),
                teacher_unique_id,
            ),
        )

        teacher_id = generated_teacher_id

        cursor.execute("SELECT phone FROM ta_user_details WHERE phone = %s", (data.get("phone"),))
        user_exists = cursor.fetchone()

        teachings = normalize_teachings_payload(data)
        grade_level_val = data.get("grade_level")
        grade_val = data.get("grade")
        subject_val = data.get("subject")
        class_taught_val = data.get("class_taught")

        if user_exists:
            sql_update_user_details = """
            UPDATE ta_user_details
            SET name=%s, sex=%s, address=%s, school_name=%s, grade_level=%s, grade=%s,
                subject=%s, class_taught=%s, is_administrator=%s, id_number=%s
            WHERE phone=%s
            """
            cursor.execute(
                sql_update_user_details,
                (
                    data.get("name"),
                    data.get("sex"),
                    data.get("address"),
                    data.get("school_name"),
                    grade_level_val,
                    grade_val,
                    subject_val,
                    class_taught_val,
                    str(is_admin_flag),
                    data.get("id_card"),
                    data.get("phone"),
                ),
            )
        else:
            sql_insert_user_details = """
            INSERT INTO ta_user_details
            (phone, name, sex, address, school_name, grade_level, grade,
             subject, class_taught, is_administrator, avatar, id_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s)
            """
            cursor.execute(
                sql_insert_user_details,
                (
                    data.get("phone"),
                    data.get("name"),
                    data.get("sex"),
                    data.get("address"),
                    data.get("school_name"),
                    grade_level_val,
                    grade_val,
                    subject_val,
                    class_taught_val,
                    str(is_admin_flag),
                    "",
                    data.get("id_card"),
                ),
            )

        if teachings:
            replace_user_teachings(cursor, str(data.get("phone") or "").strip(), teachings)

        connection.commit()

        cursor.execute("SELECT * FROM ta_teacher WHERE id = %s", (teacher_id,))
        teacher_info = cursor.fetchone()
        remove_icon_from_teacher_data(teacher_info)
        return safe_json_response({"data": {"message": "新增教师成功", "code": 200, "teacher": teacher_info}})
    except Error as e:
        if getattr(e, "errno", None) == 1062:
            connection.rollback()
            return JSONResponse(
                {"data": {"message": "任教记录重复（同一教师、学段/年级/科目/班级不能重复）", "code": 409}}, status_code=409
            )
        connection.rollback()
        app_logger.error(f"Database error during adding teacher: {e}")
        return JSONResponse({"data": {"message": "新增教师失败", "code": 500}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during adding teacher: {e}")
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.post("/delete_teacher")
async def delete_teacher(request: Request):
    data = await request.json()
    if not data or "teacher_unique_id" not in data:
        return JSONResponse({"data": {"message": "缺少 teacher_unique_id", "code": 400}}, status_code=400)

    teacher_unique_id = str(data["teacher_unique_id"])
    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM ta_teacher WHERE teacher_unique_id = %s", (teacher_unique_id,))
        connection.commit()
        if cursor.rowcount > 0:
            return safe_json_response({"data": {"message": "删除教师成功", "code": 200}})
        return safe_json_response({"data": {"message": "没有找到数据：未找到对应教师", "code": 200}}, status_code=200)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"删除教师时数据库异常: {e}")
        return JSONResponse({"data": {"message": "删除教师失败", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.get("/get_list_teachers")
async def get_list_teachers(request: Request):
    school_id = request.query_params.get("schoolId")
    final_query = "SELECT * FROM ta_teacher WHERE (%s IS NULL OR schoolId = %s)"
    params = (school_id, school_id)

    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500, "teachers": []}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, params)
        teachers = cursor.fetchall()
        remove_icon_from_teacher_data(teachers)
        return safe_json_response({"data": {"message": "获取老师列表成功", "code": 200, "teachers": teachers}})
    except Error as e:
        app_logger.error(f"Database error during fetching teachers: {e}")
        return JSONResponse({"data": {"message": "获取老师列表失败", "code": 500, "teachers": []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching teachers: {e}")
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500, "teachers": []}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.get("/teachers")
async def list_teachers(request: Request):
    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500, "teachers": []}}, status_code=500)

    cursor = None
    try:
        school_id_filter = request.query_params.get("school_id")
        grade_id_filter = request.query_params.get("grade_id")
        name_filter = request.query_params.get("name")

        base_columns = "id, name, subject, gradeId, schoolId"
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
        remove_icon_from_teacher_data(teachers)
        return safe_json_response({"data": {"message": "获取老师列表成功", "code": 200, "teachers": teachers}})
    except Error as e:
        app_logger.error(f"Database error during fetching teachers: {e}")
        return JSONResponse({"data": {"message": "获取老师列表失败", "code": 500, "teachers": []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching teachers: {e}")
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500, "teachers": []}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


BASE_PATH = "/data/nginx/html/icons"
os.makedirs(BASE_PATH, exist_ok=True)


@router.post("/upload_icon")
async def upload_icon(teacher_id: str = Form(...), file: UploadFile = File(...)):
    teacher_dir = os.path.join(BASE_PATH, teacher_id)
    os.makedirs(teacher_dir, exist_ok=True)
    save_path = os.path.join(teacher_dir, file.filename)
    with open(save_path, "wb") as f:
        f.write(await file.read())
    url_path = f"/icons/{teacher_id}/{file.filename}"
    return JSONResponse({"status": "ok", "message": "Upload success", "url": url_path})


@router.get("/teachers/search")
def search_teachers(
    schoolid: str = Query(None, description="学校ID，可选参数"),
    teacher_id: str = Query(None, description="老师ID，与teacher_unique_id和name三选一"),
    teacher_unique_id: str = Query(None, description="老师唯一ID，与teacher_id和name三选一，支持模糊查询"),
    name: str = Query(None, description="老师姓名，与teacher_id和teacher_unique_id三选一，支持模糊查询"),
):
    """
    搜索教师接口
    根据 teacher_id、teacher_unique_id 或 name 搜索教师
    """
    print("=" * 80)
    print("[teachers/search] ========== 收到搜索教师请求 ==========")
    print(f"[teachers/search] 请求时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[teachers/search] 请求参数:")
    print(f"[teachers/search]   - schoolid: {schoolid}")
    print(f"[teachers/search]   - teacher_id: {teacher_id}")
    print(f"[teachers/search]   - teacher_unique_id: {teacher_unique_id}")
    print(f"[teachers/search]   - name: {name}")
    app_logger.info("=" * 80)
    app_logger.info("[teachers/search] ========== 收到搜索教师请求 ==========")
    app_logger.info(f"[teachers/search] 请求参数 - schoolid: {schoolid}, teacher_id: {teacher_id}, teacher_unique_id: {teacher_unique_id}, name: {name}")

    # teacher_id、teacher_unique_id 和 name 至少提供一个，且不能同时提供多个
    search_params_count = sum([bool(teacher_id), bool(teacher_unique_id), bool(name)])
    if search_params_count == 0:
        error_msg = "teacher_id、teacher_unique_id 和 name 必须至少提供一个"
        print(f"[teachers/search] ❌ 错误: {error_msg}")
        app_logger.warning(f"[teachers/search] {error_msg}")
        
        # 构建错误响应数据
        error_response = {"data": {"message": error_msg, "code": 400}}
        
        # 打印返回的 JSON 消息
        try:
            error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
            print(f"[teachers/search] 📤 返回 JSON 消息:")
            print(error_json)
            app_logger.info(f"[teachers/search] 返回 JSON 消息:\n{error_json}")
        except Exception as json_err:
            print(f"[teachers/search] ⚠️  打印 JSON 消息失败: {json_err}")
            app_logger.warning(f"[teachers/search] 打印 JSON 消息失败: {json_err}")
        
        return JSONResponse(error_response, status_code=400)
    if search_params_count > 1:
        error_msg = "teacher_id、teacher_unique_id 和 name 不能同时提供"
        print(f"[teachers/search] ❌ 错误: {error_msg}")
        app_logger.warning(f"[teachers/search] {error_msg}")
        
        # 构建错误响应数据
        error_response = {"data": {"message": error_msg, "code": 400}}
        
        # 打印返回的 JSON 消息
        try:
            error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
            print(f"[teachers/search] 📤 返回 JSON 消息:")
            print(error_json)
            app_logger.info(f"[teachers/search] 返回 JSON 消息:\n{error_json}")
        except Exception as json_err:
            print(f"[teachers/search] ⚠️  打印 JSON 消息失败: {json_err}")
            app_logger.warning(f"[teachers/search] 打印 JSON 消息失败: {json_err}")
        
        return JSONResponse(error_response, status_code=400)

    if not schoolid:
        print("[teachers/search] ⚠️  警告: 未提供 schoolid 参数，将搜索所有学校")
        app_logger.warning("[teachers/search] 未提供 schoolid 参数，将搜索所有学校")

    print("[teachers/search] 📊 开始连接数据库...")
    app_logger.info("[teachers/search] 开始连接数据库...")
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        error_msg = "数据库连接失败"
        print(f"[teachers/search] ❌ 错误: {error_msg}")
        app_logger.error(f"[teachers/search] {error_msg} for schoolid={schoolid}")
        
        # 构建错误响应数据
        error_response = {"data": {"message": error_msg, "code": 500}}
        
        # 打印返回的 JSON 消息
        try:
            error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
            print(f"[teachers/search] 📤 返回 JSON 消息:")
            print(error_json)
            app_logger.info(f"[teachers/search] 返回 JSON 消息:\n{error_json}")
        except Exception as json_err:
            print(f"[teachers/search] ⚠️  打印 JSON 消息失败: {json_err}")
            app_logger.warning(f"[teachers/search] 打印 JSON 消息失败: {json_err}")
        
        return JSONResponse(error_response, status_code=500)
    print("[teachers/search] ✅ 数据库连接成功")
    app_logger.info("[teachers/search] 数据库连接成功")

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        if teacher_id:
            print(f"[teachers/search] 🔍 根据 teacher_id 精确查询: {teacher_id}")
            app_logger.info(f"[teachers/search] 根据 teacher_id 精确查询: {teacher_id}")
            if schoolid:
                sql = "SELECT * FROM `ta_teacher` WHERE schoolId = %s AND id = %s"
                params = (schoolid, teacher_id)
            else:
                sql = "SELECT * FROM `ta_teacher` WHERE id = %s"
                params = (teacher_id,)
            search_key = teacher_id
            search_type = "teacher_id"
        elif teacher_unique_id:
            print(f"[teachers/search] 🔍 根据 teacher_unique_id 模糊查询: {teacher_unique_id}")
            app_logger.info(f"[teachers/search] 根据 teacher_unique_id 模糊查询: {teacher_unique_id}")
            if schoolid:
                sql = "SELECT * FROM `ta_teacher` WHERE schoolId = %s AND teacher_unique_id LIKE %s"
                params = (schoolid, f"%{teacher_unique_id}%")
            else:
                sql = "SELECT * FROM `ta_teacher` WHERE teacher_unique_id LIKE %s"
                params = (f"%{teacher_unique_id}%",)
            search_key = teacher_unique_id
            search_type = "teacher_unique_id"
        else:
            print(f"[teachers/search] 🔍 根据 name 模糊查询: {name}")
            app_logger.info(f"[teachers/search] 根据 name 模糊查询: {name}")
            if schoolid:
                sql = "SELECT * FROM `ta_teacher` WHERE schoolId = %s AND name LIKE %s"
                params = (schoolid, f"%{name}%")
            else:
                sql = "SELECT * FROM `ta_teacher` WHERE name LIKE %s"
                params = (f"%{name}%",)
            search_key = name
            search_type = "name"

        print(f"[teachers/search] 📝 执行SQL查询 - search_type: {search_type}, search_key: {search_key}")
        app_logger.info(f"[teachers/search] 执行SQL查询 - search_type: {search_type}, search_key: {search_key}")
        cursor.execute(sql, params)
        teachers = cursor.fetchall()
        print(f"[teachers/search] ✅ 查询完成，找到 {len(teachers)} 条记录")
        app_logger.info(f"[teachers/search] 查询完成，找到 {len(teachers)} 条记录")

        remove_icon_from_teacher_data(teachers)

        for teacher in teachers:
            for key, value in list(teacher.items()):
                if isinstance(value, datetime.datetime):
                    teacher[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        print(f"[teachers/search] ✅ 返回结果 - count: {len(teachers)}")
        app_logger.info(f"[teachers/search] 返回结果 - count: {len(teachers)}")

        # 构建返回的响应数据
        response_data = {
            "data": {
                "message": "查询成功",
                "code": 200,
                "schoolid": schoolid,
                "search_key": search_key,
                "search_type": search_type,
                "teachers": teachers,
                "count": len(teachers),
            }
        }

        # 打印返回的 JSON 消息
        try:
            response_json = json.dumps(response_data, ensure_ascii=False, indent=2)
            print(f"[teachers/search] 📤 返回 JSON 消息:")
            print(response_json)
            app_logger.info(f"[teachers/search] 返回 JSON 消息:\n{response_json}")
        except Exception as e:
            print(f"[teachers/search] ⚠️  打印 JSON 消息失败: {e}")
            app_logger.warning(f"[teachers/search] 打印 JSON 消息失败: {e}")

        print("=" * 80)

        return JSONResponse(response_data, status_code=200)
    except mysql.connector.Error as e:
        error_msg = f"数据库错误: {e}"
        print(f"[teachers/search] ❌ {error_msg}")
        app_logger.error(f"[teachers/search] {error_msg}")
        
        # 构建错误响应数据
        error_response = {"data": {"message": f"查询失败: {str(e)}", "code": 500}}
        
        # 打印返回的 JSON 消息
        try:
            error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
            print(f"[teachers/search] 📤 返回 JSON 消息:")
            print(error_json)
            app_logger.info(f"[teachers/search] 返回 JSON 消息:\n{error_json}")
        except Exception as json_err:
            print(f"[teachers/search] ⚠️  打印 JSON 消息失败: {json_err}")
            app_logger.warning(f"[teachers/search] 打印 JSON 消息失败: {json_err}")
        
        return JSONResponse(error_response, status_code=500)
    except Exception as e:
        error_msg = f"未知错误: {e}"
        print(f"[teachers/search] ❌ {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[teachers/search] 错误堆栈: {traceback_str}")
        app_logger.error(f"[teachers/search] {error_msg}\n{traceback_str}")
        
        # 构建错误响应数据
        error_response = {"data": {"message": f"查询失败: {str(e)}", "code": 500}}
        
        # 打印返回的 JSON 消息
        try:
            error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
            print(f"[teachers/search] 📤 返回 JSON 消息:")
            print(error_json)
            app_logger.info(f"[teachers/search] 返回 JSON 消息:\n{error_json}")
        except Exception as json_err:
            print(f"[teachers/search] ⚠️  打印 JSON 消息失败: {json_err}")
            app_logger.warning(f"[teachers/search] 打印 JSON 消息失败: {json_err}")
        
        return JSONResponse(error_response, status_code=500)
    finally:
        if cursor:
            cursor.close()
            print("[teachers/search] 游标已关闭")
        if connection and connection.is_connected():
            connection.close()
            print("[teachers/search] 数据库连接已关闭")
            app_logger.info("[teachers/search] 数据库连接已关闭")
        print("=" * 80)


@router.get("/friends")
def get_friends(id_card: str = Query(..., description="教师身份证号")):
    """根据教师 id_card 查询关联朋友信息（双向关系：我是谁的朋友 + 谁是我的朋友）"""
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    results: List[Dict[str, Any]] = []
    friend_unique_ids = set()  # 使用 set 去重
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT teacher_unique_id FROM ta_teacher WHERE id_card=%s", (id_card,))
            rows = cursor.fetchall()
        if not rows:
            return {"friends": []}

        teacher_unique_id = rows[0]["teacher_unique_id"]

        # 查询双向朋友关系：
        # 1. 查询我添加的朋友（teacher_unique_id = 我，friendcode = 朋友）
        # 2. 查询添加我为朋友的人（teacher_unique_id = 朋友，friendcode = 我）
        with connection.cursor(dictionary=True) as cursor:
            # 查询我添加的朋友
            cursor.execute("SELECT friendcode FROM ta_friend WHERE teacher_unique_id=%s", (teacher_unique_id,))
            friend_rows_1 = cursor.fetchall()
            for fr in friend_rows_1:
                if fr["friendcode"]:
                    friend_unique_ids.add(fr["friendcode"])
            
            # 查询添加我为朋友的人
            cursor.execute("SELECT teacher_unique_id FROM ta_friend WHERE friendcode=%s", (teacher_unique_id,))
            friend_rows_2 = cursor.fetchall()
            for fr in friend_rows_2:
                if fr["teacher_unique_id"]:
                    friend_unique_ids.add(fr["teacher_unique_id"])

        if not friend_unique_ids:
            return {"friends": []}

        # 获取所有朋友的详细信息
        for friend_unique_id in friend_unique_ids:
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM ta_teacher WHERE teacher_unique_id=%s", (friend_unique_id,))
                teacher_rows = cursor.fetchall()
            if not teacher_rows:
                continue
            friend_teacher = teacher_rows[0]
            remove_icon_from_teacher_data(friend_teacher)

            id_number = friend_teacher.get("id_card")
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM ta_user_details WHERE id_number=%s", (id_number,))
                user_rows = cursor.fetchall()
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

            results.append({"teacher_info": friend_teacher, "user_details": user_details})

        return {"count": len(results), "friends": results}
    finally:
        if connection and connection.is_connected():
            connection.close()


@router.get("/teachers/classes")
def get_teacher_classes(
    teacher_unique_id: str = Query(..., description="教师唯一编号"),
):
    """
    查询某个教师加入的班级列表
    
    参数:
    - teacher_unique_id: 教师唯一编号（必填）
    
    返回:
    - classes: 班级列表，包含班级信息和教师在该班级的角色
    """
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # 查询教师加入的班级关系
        sql = """
            SELECT 
                tcr.id as relation_id,
                tcr.teacher_unique_id,
                tcr.class_code,
                tcr.role,
                tcr.subject,
                tcr.join_time,
                tc.class_name,
                tc.school_stage,
                tc.grade,
                tc.schoolid,
                tc.remark,
                tc.face_url,
                tc.created_at as class_created_at
            FROM ta_teacher_class_relation tcr
            INNER JOIN ta_classes tc ON tcr.class_code = tc.class_code
            WHERE tcr.teacher_unique_id = %s
            ORDER BY tcr.join_time DESC
        """
        cursor.execute(sql, (teacher_unique_id,))
        relations = cursor.fetchall()

        # 格式化日期字段
        classes = []
        for relation in relations:
            # 格式化日期
            if isinstance(relation.get("join_time"), datetime.datetime):
                relation["join_time"] = relation["join_time"].strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(relation.get("class_created_at"), datetime.datetime):
                relation["class_created_at"] = relation["class_created_at"].strftime("%Y-%m-%d %H:%M:%S")

            # 查询学校信息
            schoolid = relation.get("schoolid")
            school_info = None
            if schoolid:
                cursor.execute(
                    """
                    SELECT id, name, address
                    FROM ta_school
                    WHERE id = %s
                    """,
                    (schoolid,),
                )
                school_info = cursor.fetchone()

            class_data = {
                "relation_id": relation.get("relation_id"),
                "class_code": relation.get("class_code"),
                "class_name": relation.get("class_name"),
                "school_stage": relation.get("school_stage"),
                "grade": relation.get("grade"),
                "schoolid": relation.get("schoolid"),
                "school_name": school_info.get("name") if school_info else None,
                "school_address": school_info.get("address") if school_info else None,
                "remark": relation.get("remark"),
                "face_url": relation.get("face_url"),
                "class_created_at": relation.get("class_created_at"),
                "role": relation.get("role"),
                "subject": relation.get("subject"),
                "join_time": relation.get("join_time"),
            }
            classes.append(class_data)

        return JSONResponse(
            {
                "data": {
                    "message": "查询成功",
                    "code": 200,
                    "teacher_unique_id": teacher_unique_id,
                    "classes": classes,
                    "count": len(classes),
                }
            },
            status_code=200,
        )
    except mysql.connector.Error as e:
        app_logger.error(f"[get_teacher_classes] 数据库错误: {e}")
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        app_logger.error(f"[get_teacher_classes] 未知错误: {e}", exc_info=True)
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.post("/teachers/classes/add")
async def add_teacher_to_class(request: Request):
    """
    添加教师到班级
    
    请求体:
    - teacher_unique_id: 教师唯一编号（必填）
    - class_code: 班级编号（必填）
    - role: 角色，可选，默认 'teacher'（teacher: 任课教师, head_teacher: 班主任）
    - subject: 任教学科，可选
    """
    data = await request.json()
    if not data:
        return JSONResponse({"data": {"message": "请求数据不能为空", "code": 400}}, status_code=400)

    teacher_unique_id = data.get("teacher_unique_id")
    class_code = data.get("class_code")
    role = data.get("role", "teacher")
    subject = data.get("subject")

    if not teacher_unique_id or not class_code:
        return JSONResponse({"data": {"message": "teacher_unique_id 和 class_code 不能为空", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # 检查教师是否存在
        cursor.execute("SELECT teacher_unique_id FROM ta_teacher WHERE teacher_unique_id = %s", (teacher_unique_id,))
        teacher = cursor.fetchone()
        if not teacher:
            return JSONResponse({"data": {"message": "教师不存在", "code": 404}}, status_code=404)

        # 检查班级是否存在
        cursor.execute("SELECT class_code FROM ta_classes WHERE class_code = %s", (class_code,))
        class_info = cursor.fetchone()
        if not class_info:
            return JSONResponse({"data": {"message": "班级不存在", "code": 404}}, status_code=404)

        # 检查关系是否已存在
        cursor.execute(
            "SELECT id FROM ta_teacher_class_relation WHERE teacher_unique_id = %s AND class_code = %s",
            (teacher_unique_id, class_code),
        )
        existing = cursor.fetchone()
        if existing:
            return JSONResponse({"data": {"message": "该教师已加入该班级", "code": 409}}, status_code=409)

        # 插入关系
        cursor.execute(
            """
            INSERT INTO ta_teacher_class_relation (teacher_unique_id, class_code, role, subject)
            VALUES (%s, %s, %s, %s)
            """,
            (teacher_unique_id, class_code, role, subject),
        )
        connection.commit()

        # 查询插入后的完整信息
        cursor.execute(
            """
            SELECT 
                tcr.id as relation_id,
                tcr.teacher_unique_id,
                tcr.class_code,
                tcr.role,
                tcr.subject,
                tcr.join_time,
                tc.class_name,
                tc.school_stage,
                tc.grade,
                tc.schoolid
            FROM ta_teacher_class_relation tcr
            INNER JOIN ta_classes tc ON tcr.class_code = tc.class_code
            WHERE tcr.id = %s
            """,
            (cursor.lastrowid,),
        )
        relation = cursor.fetchone()

        # 格式化日期
        if relation and isinstance(relation.get("join_time"), datetime.datetime):
            relation["join_time"] = relation["join_time"].strftime("%Y-%m-%d %H:%M:%S")

        return JSONResponse(
            {
                "data": {
                    "message": "添加成功",
                    "code": 200,
                    "relation": relation,
                }
            },
            status_code=200,
        )
    except mysql.connector.Error as e:
        if connection:
            connection.rollback()
        app_logger.error(f"[add_teacher_to_class] 数据库错误: {e}")
        return JSONResponse({"data": {"message": f"添加失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        if connection:
            connection.rollback()
        app_logger.error(f"[add_teacher_to_class] 未知错误: {e}", exc_info=True)
        return JSONResponse({"data": {"message": f"添加失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.post("/teachers/classes/remove")
async def remove_teacher_from_class(request: Request):
    """
    从班级中移除教师
    
    请求体:
    - teacher_unique_id: 教师唯一编号（必填）
    - class_code: 班级编号（必填）
    """
    data = await request.json()
    if not data:
        return JSONResponse({"data": {"message": "请求数据不能为空", "code": 400}}, status_code=400)

    teacher_unique_id = data.get("teacher_unique_id")
    class_code = data.get("class_code")

    if not teacher_unique_id or not class_code:
        return JSONResponse({"data": {"message": "teacher_unique_id 和 class_code 不能为空", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor()

        # 删除关系
        cursor.execute(
            "DELETE FROM ta_teacher_class_relation WHERE teacher_unique_id = %s AND class_code = %s",
            (teacher_unique_id, class_code),
        )
        connection.commit()

        if cursor.rowcount > 0:
            return JSONResponse({"data": {"message": "移除成功", "code": 200}}, status_code=200)
        else:
            return JSONResponse({"data": {"message": "关系不存在", "code": 404}}, status_code=404)
    except mysql.connector.Error as e:
        if connection:
            connection.rollback()
        app_logger.error(f"[remove_teacher_from_class] 数据库错误: {e}")
        return JSONResponse({"data": {"message": f"移除失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        if connection:
            connection.rollback()
        app_logger.error(f"[remove_teacher_from_class] 未知错误: {e}", exc_info=True)
        return JSONResponse({"data": {"message": f"移除失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()



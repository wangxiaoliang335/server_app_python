import base64
import datetime
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
        return safe_json_response({"data": {"message": "未找到对应教师", "code": 404}}, status_code=404)
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
    teacher_unique_id: str = Query(None, description="老师唯一ID，与teacher_id和name三选一"),
    name: str = Query(None, description="老师姓名，与teacher_id和teacher_unique_id三选一，支持模糊查询"),
):
    # teacher_id、teacher_unique_id 和 name 至少提供一个，且不能同时提供多个
    search_params_count = sum([bool(teacher_id), bool(teacher_unique_id), bool(name)])
    if search_params_count == 0:
        return JSONResponse({"data": {"message": "teacher_id、teacher_unique_id 和 name 必须至少提供一个", "code": 400}}, status_code=400)
    if search_params_count > 1:
        return JSONResponse({"data": {"message": "teacher_id、teacher_unique_id 和 name 不能同时提供", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        if teacher_id:
            if schoolid:
                sql = "SELECT * FROM `ta_teacher` WHERE schoolId = %s AND id = %s"
                params = (schoolid, teacher_id)
            else:
                sql = "SELECT * FROM `ta_teacher` WHERE id = %s"
                params = (teacher_id,)
            search_key = teacher_id
            search_type = "teacher_id"
        elif teacher_unique_id:
            if schoolid:
                sql = "SELECT * FROM `ta_teacher` WHERE schoolId = %s AND teacher_unique_id = %s"
                params = (schoolid, teacher_unique_id)
            else:
                sql = "SELECT * FROM `ta_teacher` WHERE teacher_unique_id = %s"
                params = (teacher_unique_id,)
            search_key = teacher_unique_id
            search_type = "teacher_unique_id"
        else:
            if schoolid:
                sql = "SELECT * FROM `ta_teacher` WHERE schoolId = %s AND name LIKE %s"
                params = (schoolid, f"%{name}%")
            else:
                sql = "SELECT * FROM `ta_teacher` WHERE name LIKE %s"
                params = (f"%{name}%",)
            search_key = name
            search_type = "name"

        cursor.execute(sql, params)
        teachers = cursor.fetchall()

        for teacher in teachers:
            for key, value in list(teacher.items()):
                if isinstance(value, datetime.datetime):
                    teacher[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        return JSONResponse(
            {
                "data": {
                    "message": "查询成功",
                    "code": 200,
                    "schoolid": schoolid,
                    "search_key": search_key,
                    "search_type": search_type,
                    "teachers": teachers,
                    "count": len(teachers),
                }
            },
            status_code=200,
        )
    except mysql.connector.Error as e:
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.get("/friends")
def get_friends(id_card: str = Query(..., description="教师身份证号")):
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    results: List[Dict[str, Any]] = []
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT teacher_unique_id FROM ta_teacher WHERE id_card=%s", (id_card,))
            rows = cursor.fetchall()
        if not rows:
            return {"friends": []}

        teacher_unique_id = rows[0]["teacher_unique_id"]

        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT friendcode FROM ta_friend WHERE teacher_unique_id=%s", (teacher_unique_id,))
            friend_rows = cursor.fetchall()
        if not friend_rows:
            return {"friends": []}

        for fr in friend_rows:
            friendcode = fr["friendcode"]

            with connection.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM ta_teacher WHERE teacher_unique_id=%s", (friendcode,))
                teacher_rows = cursor.fetchall()
            if not teacher_rows:
                continue
            friend_teacher = teacher_rows[0]

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
                        except Exception:
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

import base64
import datetime
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

BASE_PATH = "/data/nginx/html/icons"
os.makedirs(BASE_PATH, exist_ok=True)


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
        return safe_json_response({"data": {"message": "未找到对应教师", "code": 404}}, status_code=404)
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

        base_columns = "id, name, icon, subject, gradeId, schoolId"
        base_query = f"SELECT {base_columns} FROM ta_teacher WHERE 1=1"
        filters: List[str] = []
        params: List[Any] = []

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


@router.post("/upload_icon")
async def upload_icon(
    teacher_id: str = Form(...),
    file: UploadFile = File(...),
):
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
    teacher_unique_id: str = Query(None, description="老师唯一ID，与teacher_id和name三选一"),
    name: str = Query(None, description="老师姓名，与teacher_id和teacher_unique_id三选一，支持模糊查询"),
):
    search_params_count = sum([bool(teacher_id), bool(teacher_unique_id), bool(name)])
    if search_params_count == 0:
        return JSONResponse({"data": {"message": "teacher_id、teacher_unique_id 和 name 必须至少提供一个", "code": 400}}, status_code=400)
    if search_params_count > 1:
        return JSONResponse({"data": {"message": "teacher_id、teacher_unique_id 和 name 不能同时提供", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        if teacher_id:
            if schoolid:
                sql = "SELECT * FROM `ta_teacher` WHERE schoolId = %s AND id = %s"
                params = (schoolid, teacher_id)
            else:
                sql = "SELECT * FROM `ta_teacher` WHERE id = %s"
                params = (teacher_id,)
            search_key = teacher_id
            search_type = "teacher_id"
        elif teacher_unique_id:
            if schoolid:
                sql = "SELECT * FROM `ta_teacher` WHERE schoolId = %s AND teacher_unique_id = %s"
                params = (schoolid, teacher_unique_id)
            else:
                sql = "SELECT * FROM `ta_teacher` WHERE teacher_unique_id = %s"
                params = (teacher_unique_id,)
            search_key = teacher_unique_id
            search_type = "teacher_unique_id"
        else:
            if schoolid:
                sql = "SELECT * FROM `ta_teacher` WHERE schoolId = %s AND name LIKE %s"
                params = (schoolid, f"%{name}%")
            else:
                sql = "SELECT * FROM `ta_teacher` WHERE name LIKE %s"
                params = (f"%{name}%",)
            search_key = name
            search_type = "name"

        cursor.execute(sql, params)
        teachers = cursor.fetchall()

        for teacher in teachers:
            for key, value in list(teacher.items()):
                if isinstance(value, datetime.datetime):
                    teacher[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        return JSONResponse(
            {
                "data": {
                    "message": "查询成功",
                    "code": 200,
                    "schoolid": schoolid,
                    "search_key": search_key,
                    "search_type": search_type,
                    "teachers": teachers,
                    "count": len(teachers),
                }
            },
            status_code=200,
        )
    except mysql.connector.Error as e:
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.get("/friends")
def get_friends(id_card: str = Query(..., description="教师身份证号")):
    """根据教师 id_card 查询关联朋友信息"""
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    results: List[Dict[str, Any]] = []
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT teacher_unique_id FROM ta_teacher WHERE id_card=%s", (id_card,))
            rows = cursor.fetchall()
        if not rows:
            return {"friends": []}

        teacher_unique_id = rows[0]["teacher_unique_id"]

        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT friendcode FROM ta_friend WHERE teacher_unique_id=%s", (teacher_unique_id,))
            friend_rows = cursor.fetchall()
        if not friend_rows:
            return {"friends": []}

        for fr in friend_rows:
            friendcode = fr["friendcode"]

            with connection.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM ta_teacher WHERE teacher_unique_id=%s", (friendcode,))
                teacher_rows = cursor.fetchall()
            if not teacher_rows:
                continue
            friend_teacher = teacher_rows[0]

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



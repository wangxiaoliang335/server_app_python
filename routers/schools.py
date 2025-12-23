import base64
import json
import time

import mysql.connector
from mysql.connector import Error

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from common import app_logger, safe_json_response
from db import get_db_connection
from services.avatars import upload_avatar_to_oss


router = APIRouter()


@router.get("/schools")
async def list_schools(request: Request):
    connection = get_db_connection()
    if connection is None:
        app_logger.error("List schools failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500, "schools": []}}, status_code=500)

    cursor = None
    try:
        school_id = request.query_params.get("id")
        name_filter = request.query_params.get("name")

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
        return safe_json_response({"data": {"message": "获取学校列表成功", "code": 200, "schools": schools}})
    except Error as e:
        app_logger.error(f"Database error during fetching schools: {e}")
        return JSONResponse({"data": {"message": "获取学校列表失败", "code": 500, "schools": []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching schools: {e}")
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500, "schools": []}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.post("/updateClasses")
async def updateClasses(request: Request):
    data_list = await request.json()
    if not isinstance(data_list, list) or not data_list:
        return JSONResponse({"data": {"message": "必须提供班级数组数据", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

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
        result_list = []

        schoolid_sequence_map = {}

        for item in data_list:
            class_code = item.get("class_code")
            schoolid = item.get("schoolid")

            if not class_code or str(class_code).strip() == "":
                if not schoolid or str(schoolid).strip() == "":
                    app_logger.error(f"生成 class_code 失败：缺少 schoolid，跳过该班级: {item}")
                    continue

                schoolid_str = str(schoolid).zfill(6)[:6]

                if schoolid_str not in schoolid_sequence_map:
                    try:
                        check_cursor = connection.cursor()
                        check_cursor.execute(
                            """
                            SELECT CAST(SUBSTRING(class_code, 7) AS UNSIGNED) AS sequence_num
                            FROM ta_classes
                            WHERE class_code LIKE %s AND LENGTH(class_code) = 9
                            AND CAST(SUBSTRING(class_code, 7) AS UNSIGNED) BETWEEN 1 AND 999
                            ORDER BY sequence_num ASC
                            """,
                            (f"{schoolid_str}%",),
                        )
                        used_sequences = set()
                        for row in check_cursor.fetchall():
                            if row and row[0]:
                                try:
                                    used_sequences.add(int(row[0]))
                                except (ValueError, TypeError):
                                    pass
                        schoolid_sequence_map[schoolid_str] = {"used": used_sequences, "next": 1}
                        check_cursor.close()
                    except Exception as e:
                        app_logger.error(f"查询 schoolid {schoolid_str} 的已使用流水号失败: {e}")
                        schoolid_sequence_map[schoolid_str] = {"used": set(), "next": 1}

                seq_info = schoolid_sequence_map[schoolid_str]
                used_sequences = seq_info["used"]
                next_seq = seq_info["next"]

                new_sequence = None
                for seq in range(next_seq, 1000):
                    if seq not in used_sequences:
                        new_sequence = seq
                        seq_info["next"] = seq + 1
                        used_sequences.add(seq)
                        break

                if new_sequence is None:
                    for seq in range(1, next_seq):
                        if seq not in used_sequences:
                            new_sequence = seq
                            seq_info["next"] = seq + 1
                            used_sequences.add(seq)
                            break

                if new_sequence is None:
                    try:
                        check_cursor = connection.cursor()
                        check_cursor.execute(
                            """
                            SELECT CAST(SUBSTRING(class_code, 7) AS UNSIGNED) AS sequence_num
                            FROM ta_classes
                            WHERE class_code LIKE %s AND LENGTH(class_code) = 9
                            ORDER BY sequence_num DESC
                            LIMIT 1
                            """,
                            (f"{schoolid_str}%",),
                        )
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

                    seq_info["next"] = new_sequence + 1
                    used_sequences.add(new_sequence)

                if new_sequence > 999:
                    app_logger.error(f"生成 class_code 失败：schoolid {schoolid_str} 的流水号已超过999")
                    continue

                sequence_str = str(new_sequence).zfill(3)
                class_code = f"{schoolid_str}{sequence_str}"

                try:
                    check_cursor = connection.cursor()
                    check_cursor.execute("SELECT class_code FROM ta_classes WHERE class_code = %s", (class_code,))
                    if check_cursor.fetchone() is not None:
                        used_sequences.add(new_sequence)
                        for seq in range(new_sequence + 1, 1000):
                            if seq not in used_sequences:
                                new_sequence = seq
                                seq_info["next"] = seq + 1
                                used_sequences.add(seq)
                                sequence_str = str(new_sequence).zfill(3)
                                class_code = f"{schoolid_str}{sequence_str}"
                                check_cursor.execute("SELECT class_code FROM ta_classes WHERE class_code = %s", (class_code,))
                                if check_cursor.fetchone() is None:
                                    break
                        else:
                            app_logger.warning("生成 class_code 时所有流水号都冲突，使用最大+1")
                            max_used = max(used_sequences) if used_sequences else 0
                            new_sequence = max_used + 1
                            if new_sequence > 999:
                                app_logger.error(
                                    f"生成 class_code 失败：schoolid {schoolid_str} 的流水号已超过999（并发冲突）"
                                )
                                check_cursor.close()
                                continue
                            seq_info["next"] = new_sequence + 1
                            used_sequences.add(new_sequence)
                            sequence_str = str(new_sequence).zfill(3)
                            class_code = f"{schoolid_str}{sequence_str}"
                    check_cursor.close()
                except Exception as e:
                    app_logger.warning(f"检查 class_code 是否存在时出错: {e}")

                item["class_code"] = class_code

            if not schoolid or str(schoolid).strip() == "":
                schoolid = class_code[:6] if len(class_code) >= 6 else class_code
            else:
                schoolid = str(schoolid).zfill(6)[:6]

            values.append(
                (
                    class_code,
                    item.get("school_stage"),
                    item.get("grade"),
                    item.get("class_name"),
                    item.get("remark"),
                    schoolid,
                )
            )
            result_list.append(
                {
                    "class_code": class_code,
                    "school_stage": item.get("school_stage"),
                    "grade": item.get("grade"),
                    "class_name": item.get("class_name"),
                    "remark": item.get("remark"),
                    "schoolid": schoolid,
                }
            )

        if values:
            cursor.executemany(sql, values)
            connection.commit()

        cursor.close()
        connection.close()

        response_data = {
            "data": {
                "message": "批量插入/更新完成",
                "code": 200,
                "count": len(result_list),
                "classes": result_list,
            }
        }
        return safe_json_response(response_data)
    except Error as e:
        if connection:
            connection.rollback()
        app_logger.error(f"Database error during updateClasses: {e}")
        return JSONResponse({"data": {"message": f"数据库操作失败: {e}", "code": 500}}, status_code=500)
    except Exception as e:
        if connection:
            connection.rollback()
        app_logger.error(f"Unexpected error during updateClasses: {e}")
        return JSONResponse({"data": {"message": f"操作失败: {str(e)}", "code": 500}}, status_code=500)


@router.post("/deleteClasses")
async def delete_classes(request: Request):
    try:
        data = await request.json()

        class_codes = []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "class_code" in item:
                    class_code = item.get("class_code")
                    if class_code:
                        class_codes.append(class_code)
        elif isinstance(data, dict):
            if "class_codes" in data and isinstance(data["class_codes"], list):
                class_codes = data["class_codes"]
            elif "class_code" in data:
                class_codes = [data["class_code"]]
            else:
                return JSONResponse({"data": {"message": "缺少必需参数 class_code 或 class_codes", "code": 400}}, status_code=400)
        else:
            return JSONResponse({"data": {"message": "请求数据格式不正确，应为数组或对象", "code": 400}}, status_code=400)

        if not class_codes:
            return JSONResponse({"data": {"message": "class_codes 列表不能为空", "code": 400}}, status_code=400)

        connection = get_db_connection()
        if connection is None:
            return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

        cursor = None
        try:
            cursor = connection.cursor()

            placeholders = ",".join(["%s"] * len(class_codes))
            check_sql = f"SELECT class_code, class_name FROM ta_classes WHERE class_code IN ({placeholders})"
            cursor.execute(check_sql, tuple(class_codes))
            existing_classes = cursor.fetchall()

            existing_codes = []
            if existing_classes:
                for row in existing_classes:
                    if isinstance(row, (tuple, list)):
                        existing_codes.append(row[0])
                    elif isinstance(row, dict):
                        existing_codes.append(row.get("class_code"))
                    else:
                        existing_codes.append(str(row))

            not_found_codes = [code for code in class_codes if code not in existing_codes]

            if not existing_codes:
                return JSONResponse(
                    {
                        "data": {
                            "message": "未找到要删除的班级",
                            "code": 404,
                            "deleted_count": 0,
                            "not_found_codes": not_found_codes,
                        }
                    },
                    status_code=404,
                )

            delete_sql = f"DELETE FROM ta_classes WHERE class_code IN ({placeholders})"
            cursor.execute(delete_sql, tuple(existing_codes))
            deleted_count = cursor.rowcount
            connection.commit()

            result = {
                "message": "删除班级成功",
                "code": 200,
                "deleted_count": deleted_count,
                "deleted_codes": existing_codes,
            }
            if not_found_codes:
                result["not_found_codes"] = not_found_codes
                result["message"] = f"部分删除成功，{len(not_found_codes)} 个班级未找到"

            return safe_json_response({"data": result})
        except mysql.connector.Error as e:
            if connection:
                connection.rollback()
            return JSONResponse({"data": {"message": f"数据库操作失败: {str(e)}", "code": 500}}, status_code=500)
        except Exception as e:
            if connection:
                connection.rollback()
            return JSONResponse({"data": {"message": f"操作失败: {str(e)}", "code": 500}}, status_code=500)
        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
    except Exception:
        return JSONResponse({"data": {"message": "请求数据格式错误", "code": 400}}, status_code=400)


@router.post("/getClassesByPrefix")
async def get_classes_by_prefix(request: Request):
    data = await request.json()
    prefix = data.get("prefix")
    if not prefix or len(prefix) != 6 or not str(prefix).isdigit():
        return JSONResponse({"data": {"message": "必须提供6位数字前缀", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)
        sql = """
        SELECT class_code, school_stage, grade, class_name, remark, schoolid, face_url, created_at
        FROM ta_classes
        WHERE LEFT(class_code, 6) = %s
        """
        cursor.execute(sql, (prefix,))
        results = cursor.fetchall()
        cursor.close()
        connection.close()
        return safe_json_response({"data": {"message": "查询成功", "code": 200, "count": len(results), "classes": results}})
    except Error as e:
        app_logger.error(f"查询失败: {e}")
        return JSONResponse({"data": {"message": "查询失败", "code": 500}}, status_code=500)


@router.post("/updateSchoolInfo")
async def updateSchoolInfo(request: Request):
    data = await request.json()
    school_id = data.get("id")
    name = data.get("name")
    address = data.get("address")

    if not school_id:
        app_logger.warning("UpdateSchoolInfo failed: Missing id.")
        return JSONResponse({"data": {"message": "id值必须提供", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("UpdateSchoolInfo failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        update_query = "UPDATE ta_school SET name = %s, address = %s WHERE id = %s"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(update_query, (name, address, school_id))
        connection.commit()
        cursor.close()
        return JSONResponse({"data": {"message": "更新成功", "code": 200}})
    except Error as e:
        app_logger.error(f"Database error during updateSchoolInfo for {name}: {e}")
        return JSONResponse({"data": {"message": "更新失败", "code": 500}}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()


@router.get("/api/class/info")
async def get_class_info(request: Request):
    class_code = request.query_params.get("class_code")
    if not class_code:
        return JSONResponse({"data": {"message": "班级编号不能为空", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT class_code, class_name, school_stage, grade, schoolid, remark, face_url, created_at
            FROM ta_classes
            WHERE class_code = %s
            """,
            (class_code,),
        )
        class_info = cursor.fetchone()
        if not class_info:
            return JSONResponse({"data": {"message": "班级不存在", "code": 404}}, status_code=404)

        schoolid = class_info.get("schoolid")
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

        result = {
            "class_code": class_info.get("class_code"),
            "class_name": class_info.get("class_name"),
            "school_stage": class_info.get("school_stage"),
            "grade": class_info.get("grade"),
            "schoolid": schoolid,
            "remark": class_info.get("remark"),
            "face_url": class_info.get("face_url"),
            "school_name": school_info.get("name") if school_info else None,
            "address": school_info.get("address") if school_info else None,
        }
        return safe_json_response({"data": {"message": "获取班级信息成功", "code": 200, **result}}, status_code=200)
    except Exception as e:
        app_logger.error(f"[class/info] 查询异常: {e}")
        return JSONResponse({"data": {"message": "获取班级信息失败", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.post("/classes/update-avatar")
async def update_class_avatar(request: Request):
    """
    单独更新班级的头像
    1. 上传头像到阿里云OSS
    2. 将OSS URL保存到ta_classes表的face_url字段
    
    请求体：
    {
        "class_code": "班级编号（如：000011002）",
        "avatar": "base64编码的头像数据（支持 data:image/... 前缀）"
    }
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"data": {"message": "无效的 JSON 请求体", "code": 400}}, status_code=400)

    class_code = data.get("class_code")
    avatar = data.get("avatar")

    if not class_code:
        app_logger.warning("UpdateClassAvatar failed: Missing class_code.")
        return JSONResponse({"data": {"message": "班级编号必须提供", "code": 400}}, status_code=400)

    if not avatar:
        app_logger.warning("UpdateClassAvatar failed: Missing avatar.")
        return JSONResponse({"data": {"message": "头像数据必须提供", "code": 400}}, status_code=400)

    app_logger.info(f"[UpdateClassAvatar] 收到请求 - class_code={class_code}, avatar数据长度={len(avatar) if avatar else 0}")

    connection = get_db_connection()
    if connection is None:
        app_logger.error("UpdateClassAvatar failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    try:
        # 解码 Base64 头像（支持 data:image/... 前缀）
        if isinstance(avatar, str) and avatar.startswith("data:image/"):
            # 移除 data:image/xxx;base64, 前缀
            avatar = avatar.split(",", 1)[1] if "," in avatar else avatar
        avatar_bytes = base64.b64decode(avatar)
    except Exception as e:
        app_logger.error(f"Base64 decode error for class_code={class_code}: {e}")
        return JSONResponse({"data": {"message": "头像数据解析失败", "code": 400}}, status_code=400)

    # 上传头像到OSS
    object_name = f"class-avatars/{class_code}_{int(time.time())}.png"
    oss_url = upload_avatar_to_oss(avatar_bytes, object_name)
    
    if not oss_url:
        app_logger.error(f"[UpdateClassAvatar] OSS上传失败 - class_code={class_code}")
        return JSONResponse({"data": {"message": "头像上传到OSS失败", "code": 500}}, status_code=500)

    app_logger.info(f"[UpdateClassAvatar] OSS上传成功 for {class_code} -> {oss_url}")

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 检查班级是否存在
        cursor.execute("SELECT class_code, class_name, face_url FROM ta_classes WHERE class_code = %s", (class_code,))
        class_row = cursor.fetchone()
        
        if not class_row:
            app_logger.warning(f"[UpdateClassAvatar] 未找到班级 - class_code={class_code}")
            return JSONResponse({"data": {"message": f"班级 {class_code} 不存在", "code": 404}}, status_code=404)
        
        app_logger.info(f"[UpdateClassAvatar] 找到班级记录: class_code={class_code}, class_name={class_row.get('class_name')}, 当前face_url={class_row.get('face_url')}")
        
        # 更新 face_url
        cursor.execute(
            "UPDATE ta_classes SET face_url = %s WHERE class_code = %s",
            (oss_url, class_code)
        )
        connection.commit()
        
        affected_rows = cursor.rowcount
        app_logger.info(f"[UpdateClassAvatar] 更新成功 - class_code={class_code}, face_url={oss_url}, 影响行数={affected_rows}")
        
        return JSONResponse({
            "data": {
                "message": "班级头像更新成功",
                "code": 200,
                "class_code": class_code,
                "face_url": oss_url
            }
        })
    except mysql.connector.Error as e:
        if connection:
            connection.rollback()
        app_logger.error(f"Database error during updateClassAvatar for {class_code}: {e}")
        return JSONResponse({"data": {"message": f"数据库操作失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        if connection:
            connection.rollback()
        app_logger.error(f"Unexpected error during updateClassAvatar for {class_code}: {e}")
        return JSONResponse({"data": {"message": f"操作失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after updating class avatar for {class_code}.")



import json
from typing import Any, Dict, List, Optional

import mysql.connector
from fastapi import APIRouter, Query, Request

from common import app_logger, safe_json_response
from db import get_db_connection

router = APIRouter()


def save_course_schedule(
    class_id: str,
    term: str,
    days,
    times,
    remark: Optional[str],
    cells: List[Dict],
) -> Dict[str, object]:
    """
    写入/更新课程表：
    1) 依据 (class_id, term) 在 course_schedule 中插入或更新 days_json/times_json/remark；
    2) 批量写入/更新 course_schedule_cell（依据唯一键 schedule_id + row_index + col_index）。
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
        return {
            "success": False,
            "schedule_id": None,
            "upserted_cells": 0,
            "message": f"行列标签序列化失败: {e}",
        }

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Save course schedule failed: Database connection error.")
        return {"success": False, "schedule_id": None, "upserted_cells": 0, "message": "数据库连接失败"}

    try:
        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # 先尝试获取是否已存在该 (class_id, term)
        cursor.execute(
            "SELECT id FROM course_schedule WHERE class_id = %s AND term = %s LIMIT 1",
            (class_id, term),
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
            schedule_id = row["id"]
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
            insert_cell_sql = (
                "INSERT INTO course_schedule_cell (schedule_id, row_index, col_index, course_name, is_highlight) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE course_name = VALUES(course_name), is_highlight = VALUES(is_highlight)"
            )
            values = []
            for cell in cells:
                values.append(
                    (
                        schedule_id,
                        int(cell.get("row_index", 0)),
                        int(cell.get("col_index", 0)),
                        str(cell.get("course_name", "")),
                        int(cell.get("is_highlight", 0)),
                    )
                )
            cursor.executemany(insert_cell_sql, values)
            # 在 DUPLICATE 的情况下 rowcount 语义不稳定，这里返回输入数量
            upsert_count = len(values)

        connection.commit()
        return {"success": True, "schedule_id": schedule_id, "upserted_cells": upsert_count, "message": "保存成功"}
    except mysql.connector.Error as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Database error during save_course_schedule: {e}")
        return {"success": False, "schedule_id": None, "upserted_cells": 0, "message": f"数据库错误: {e}"}
    except Exception as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Unexpected error during save_course_schedule: {e}")
        return {"success": False, "schedule_id": None, "upserted_cells": 0, "message": f"未知错误: {e}"}
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after saving course schedule.")


def save_seat_arrangement(class_id: str, seats: List[Dict]) -> Dict[str, object]:
    """
    写入/更新座位安排：
    1) 依据 class_id 在 seat_arrangement 中插入或更新；
    2) 批量写入/更新 seat_arrangement_item（依据唯一键 arrangement_id + row + col）。
    """
    connection = get_db_connection()
    if connection is None:
        app_logger.error("Save seat arrangement failed: Database connection error.")
        return {"success": False, "arrangement_id": None, "upserted_seats": 0, "message": "数据库连接失败"}

    try:
        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # 先尝试获取是否已存在该 class_id
        cursor.execute(
            "SELECT id FROM seat_arrangement WHERE class_id = %s LIMIT 1",
            (class_id,),
        )
        row = cursor.fetchone()

        if row is None:
            # 插入主表
            insert_header_sql = "INSERT INTO seat_arrangement (class_id) VALUES (%s)"
            cursor.execute(insert_header_sql, (class_id,))
            arrangement_id = cursor.lastrowid
        else:
            arrangement_id = row["id"]
            # 更新主表（更新时间戳）
            update_header_sql = "UPDATE seat_arrangement SET updated_at = NOW() WHERE id = %s"
            cursor.execute(update_header_sql, (arrangement_id,))

        # 始终删除旧座位
        delete_old_sql = "DELETE FROM seat_arrangement_item WHERE arrangement_id = %s"
        cursor.execute(delete_old_sql, (arrangement_id,))
        deleted_count = cursor.rowcount
        app_logger.info(
            f"[seat_arrangement] 删除 class_id={class_id} 的旧座位数据，arrangement_id={arrangement_id}，删除行数={deleted_count}"
        )
        print(
            f"[seat_arrangement] 删除 class_id={class_id} 的旧座位数据，arrangement_id={arrangement_id}，删除行数={deleted_count}"
        )

        upsert_count = 0
        if seats:
            insert_seat_sql = (
                "INSERT INTO seat_arrangement_item (arrangement_id, `row`, `col`, student_name, name, student_id) "
                "VALUES (%s, %s, %s, %s, %s, %s)"
            )
            values = []
            for seat in seats:
                seat_student_id = str(seat.get("student_id", "") or "")
                seat_name = str(seat.get("name", "") or "")
                seat_full_name = seat.get("student_name")
                if not seat_full_name:
                    if seat_name and seat_student_id:
                        seat_full_name = f"{seat_name}{seat_student_id}"
                    else:
                        seat_full_name = seat_name or seat_student_id
                seat_full_name = str(seat_full_name or "")

                values.append(
                    (
                        arrangement_id,
                        int(seat.get("row", 0)),
                        int(seat.get("col", 0)),
                        seat_full_name,
                        seat_name,
                        seat_student_id,
                    )
                )
            if values:
                cursor.executemany(insert_seat_sql, values)
                upsert_count = len(values)
                app_logger.info(
                    f"[seat_arrangement] 插入 class_id={class_id} 的新座位数据，arrangement_id={arrangement_id}，插入行数={upsert_count}"
                )
                print(
                    f"[seat_arrangement] 插入 class_id={class_id} 的新座位数据，arrangement_id={arrangement_id}，插入行数={upsert_count}"
                )

        connection.commit()
        return {
            "success": True,
            "arrangement_id": arrangement_id,
            "upserted_seats": upsert_count,
            "message": "保存成功",
        }
    except mysql.connector.Error as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Database error during save_seat_arrangement: {e}")
        return {
            "success": False,
            "arrangement_id": None,
            "upserted_seats": 0,
            "message": f"数据库错误: {e}",
        }
    except Exception as e:
        if connection and connection.is_connected():
            connection.rollback()
        app_logger.error(f"Unexpected error during save_seat_arrangement: {e}")
        return {
            "success": False,
            "arrangement_id": None,
            "upserted_seats": 0,
            "message": f"未知错误: {e}",
        }
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after saving seat arrangement.")


@router.post("/course-schedule/save")
async def api_save_course_schedule(request: Request):
    try:
        data = await request.json()
    except Exception:
        return safe_json_response({"message": "无效的 JSON 请求体", "code": 400}, status_code=400)

    # 支持新字段 class_id，兼容旧字段 group_id（若两者同时提供，以 class_id 为准）
    class_id = data.get("class_id") or data.get("group_id")
    term = data.get("term")
    days = data.get("days")
    times = data.get("times")
    remark = data.get("remark")
    cells = data.get("cells", [])

    if not class_id or not term or days is None or times is None:
        return safe_json_response({"message": "缺少必要参数 class_id/term/days/times", "code": 400}, status_code=400)

    result = save_course_schedule(
        class_id=class_id,
        term=term,
        days=days,
        times=times,
        remark=remark,
        cells=cells if isinstance(cells, list) else [],
    )

    if result.get("success"):
        return safe_json_response({"message": "保存成功", "code": 200, "data": result})
    return safe_json_response({"message": result.get("message", "保存失败"), "code": 500}, status_code=500)


async def _handle_save_seat_arrangement_payload(data: Dict[str, Any]):
    class_id = data.get("class_id")
    seats = data.get("seats", [])

    if not class_id:
        return safe_json_response({"message": "缺少必要参数 class_id", "code": 400}, status_code=400)

    if not isinstance(seats, list):
        return safe_json_response({"message": "seats 必须是数组", "code": 400}, status_code=400)

    result = save_seat_arrangement(class_id=class_id, seats=seats if isinstance(seats, list) else [])

    if result.get("success"):
        return safe_json_response({"message": "保存成功", "code": 200, "data": result})
    return safe_json_response({"message": result.get("message", "保存失败"), "code": 500}, status_code=500)


@router.post("/seat-arrangement/save")
async def api_save_seat_arrangement(request: Request):
    try:
        data = await request.json()
    except Exception:
        return safe_json_response({"message": "无效的 JSON 请求体", "code": 400}, status_code=400)

    return await _handle_save_seat_arrangement_payload(data)


@router.post("/seat-arrange")
async def api_save_seat_arrangement_alias(request: Request):
    """兼容旧客户端的保存座位接口，与 /seat-arrangement/save 功能相同。"""
    try:
        data = await request.json()
    except Exception:
        return safe_json_response({"message": "无效的 JSON 请求体", "code": 400}, status_code=400)

    return await _handle_save_seat_arrangement_payload(data)


@router.get("/seat-arrangement")
async def api_get_seat_arrangement(request: Request, class_id: str = Query(..., description="班级ID")):
    connection = get_db_connection()
    if connection is None:
        return safe_json_response({"message": "数据库连接失败", "code": 500}, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            "SELECT id, class_id, created_at, updated_at "
            "FROM seat_arrangement WHERE class_id = %s LIMIT 1",
            (class_id,),
        )
        arrangement = cursor.fetchone()

        if not arrangement:
            return safe_json_response({"message": "未找到座位信息", "code": 404}, status_code=404)

        arrangement_id = arrangement["id"]

        cursor.execute(
            "SELECT `row`, `col`, student_name, name, student_id "
            "FROM seat_arrangement_item WHERE arrangement_id = %s "
            "ORDER BY `row`, `col`",
            (arrangement_id,),
        )
        seat_items = cursor.fetchall()

        seats = []
        for item in seat_items:
            seats.append(
                {
                    "row": item["row"],
                    "col": item["col"],
                    "student_name": item["student_name"] or "",
                    "name": item["name"] or "",
                    "student_id": item["student_id"] or "",
                }
            )

        return safe_json_response(
            {"message": "查询成功", "code": 200, "data": {"class_id": class_id, "seats": seats}}
        )
    except mysql.connector.Error as e:
        app_logger.error(f"Database error during api_get_seat_arrangement: {e}")
        return safe_json_response({"message": f"数据库错误: {e}", "code": 500}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during api_get_seat_arrangement: {e}")
        return safe_json_response({"message": f"查询失败: {e}", "code": 500}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after getting seat arrangement.")


@router.get("/course-schedule")
async def api_get_course_schedule(
    request: Request,
    class_id: str = Query(..., description="班级ID"),
    term: Optional[str] = Query(None, description="学期，如 2025-2026-1。如果不传或为空，则返回该班级所有学期的课表"),
):
    connection = get_db_connection()
    if connection is None:
        return safe_json_response({"message": "数据库连接失败", "code": 500}, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)

        term_empty = not term or (isinstance(term, str) and term.strip() == "")

        if term_empty:
            cursor.execute(
                "SELECT id, class_id, term, days_json, times_json, remark, updated_at "
                "FROM course_schedule WHERE class_id = %s ORDER BY term DESC",
                (class_id,),
            )
            headers = cursor.fetchall()

            if not headers:
                return safe_json_response({"message": "未找到课表", "code": 404}, status_code=404)

            all_schedules = []
            for header in headers:
                schedule_id = header["id"]

                try:
                    days = json.loads(header["days_json"]) if header.get("days_json") else []
                except Exception:
                    days = header.get("days_json") or []
                try:
                    times = json.loads(header["times_json"]) if header.get("times_json") else []
                except Exception:
                    times = header.get("times_json") or []

                schedule = {
                    "id": schedule_id,
                    "class_id": header.get("class_id"),
                    "term": header.get("term"),
                    "days": days,
                    "times": times,
                    "remark": header.get("remark"),
                    "updated_at": header.get("updated_at"),
                }

                cursor.execute(
                    "SELECT row_index, col_index, course_name, is_highlight "
                    "FROM course_schedule_cell WHERE schedule_id = %s",
                    (schedule_id,),
                )
                rows = cursor.fetchall() or []
                cells = []
                for r in rows:
                    cells.append(
                        {
                            "row_index": r.get("row_index"),
                            "col_index": r.get("col_index"),
                            "course_name": r.get("course_name"),
                            "is_highlight": r.get("is_highlight"),
                        }
                    )

                all_schedules.append({"schedule": schedule, "cells": cells})

            return safe_json_response({"message": "查询成功", "code": 200, "data": all_schedules})

        cursor.execute(
            "SELECT id, class_id, term, days_json, times_json, remark, updated_at "
            "FROM course_schedule WHERE class_id = %s AND term = %s LIMIT 1",
            (class_id, term),
        )
        header = cursor.fetchone()
        if not header:
            return safe_json_response({"message": "未找到课表", "code": 404}, status_code=404)

        schedule_id = header["id"]
        try:
            days = json.loads(header["days_json"]) if header.get("days_json") else []
        except Exception:
            days = header.get("days_json") or []
        try:
            times = json.loads(header["times_json"]) if header.get("times_json") else []
        except Exception:
            times = header.get("times_json") or []

        schedule = {
            "id": schedule_id,
            "class_id": header.get("class_id"),
            "term": header.get("term"),
            "days": days,
            "times": times,
            "remark": header.get("remark"),
            "updated_at": header.get("updated_at"),
        }

        cursor.execute(
            "SELECT row_index, col_index, course_name, is_highlight "
            "FROM course_schedule_cell WHERE schedule_id = %s",
            (schedule_id,),
        )
        rows = cursor.fetchall() or []
        cells = []
        for r in rows:
            cells.append(
                {
                    "row_index": r.get("row_index"),
                    "col_index": r.get("col_index"),
                    "course_name": r.get("course_name"),
                    "is_highlight": r.get("is_highlight"),
                }
            )

        return safe_json_response({"message": "查询成功", "code": 200, "data": {"schedule": schedule, "cells": cells}})
    except mysql.connector.Error as e:
        app_logger.error(f"Database error during api_get_course_schedule: {e}")
        return safe_json_response({"message": "数据库错误", "code": 500}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during api_get_course_schedule: {e}")
        return safe_json_response({"message": "未知错误", "code": 500}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching course schedule.")



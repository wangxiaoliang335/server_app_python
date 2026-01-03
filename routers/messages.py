import datetime
import json
from typing import Any, Dict, List, Optional

import mysql.connector
from fastapi import APIRouter, Path, Request
from fastapi.responses import JSONResponse, Response

from common import app_logger, format_notification_time, safe_json_response
from db import get_db_connection


router = APIRouter()


@router.get("/messages/recent")
async def get_recent_messages(request: Request):
    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500, "messages": []}}, status_code=500)

    cursor = None
    try:
        school_id = request.query_params.get("school_id")
        class_id = request.query_params.get("class_id")
        sender_id_filter = request.query_params.get("sender_id")

        three_days_ago = datetime.datetime.now() - datetime.timedelta(days=3)
        base_columns = "id, sender_id, content_type, text_content, school_id, class_id, sent_at, created_at, updated_at"
        base_query = f"SELECT {base_columns} FROM ta_message WHERE sent_at >= %s and content_type='text'"
        filters: List[str] = []
        params: List[Any] = [three_days_ago]

        if school_id:
            filters.append("AND school_id = %s")
            params.append(school_id)
        if class_id:
            filters.append("AND class_id = %s")
            params.append(int(class_id))
        if sender_id_filter:
            filters.append("AND sender_id = %s")
            params.append(sender_id_filter)

        order_clause = "ORDER BY sent_at DESC"
        final_query = f"{base_query} {' '.join(filters)} {order_clause}"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        messages = cursor.fetchall()

        sender_ids = list(set(msg["sender_id"] for msg in messages))
        sender_info_map: Dict[str, Dict[str, Any]] = {}
        if sender_ids:
            placeholders = ",".join(["%s"] * len(sender_ids))
            info_query = f"SELECT id, name FROM ta_teacher WHERE id IN ({placeholders})"
            cursor.execute(info_query, tuple(sender_ids))
            teacher_infos = cursor.fetchall()
            sender_info_map = {t["id"]: {"sender_name": t["name"]} for t in teacher_infos}

        for msg in messages:
            info = sender_info_map.get(msg["sender_id"], {})
            msg["sender_name"] = info.get("sender_name", "未知老师")
            for f in ["sent_at", "created_at", "updated_at"]:
                if isinstance(msg.get(f), datetime.datetime):
                    msg[f] = msg[f].strftime("%Y-%m-%d %H:%M:%S")

        return safe_json_response({"data": {"message": "获取最近消息列表成功", "code": 200, "messages": messages}})
    except mysql.connector.Error as e:
        app_logger.error(f"Database error during fetching recent messages: {e}")
        return JSONResponse({"data": {"message": "获取最近消息列表失败", "code": 500, "messages": []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching recent messages: {e}")
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500, "messages": []}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.post("/messages")
async def add_message(request: Request):
    connection = get_db_connection()
    if not connection:
        return JSONResponse(
            {"data": {"message": "数据库连接失败", "code": 500, "message": None}},
            status_code=500,
        )

    cursor = None
    try:
        content_type_header = request.headers.get("content-type", "")

        # 先从 query 或 form 中获取 sender_id
        sender_id = request.query_params.get("sender_id")
        if sender_id:
            try:
                sender_id = str(sender_id).strip() or None
            except Exception:
                sender_id = None

        # === 情况1: JSON 格式 - 发送文本消息 ===
        if content_type_header.startswith("application/json"):
            data = await request.json()
            if not data:
                return JSONResponse({"data": {"message": "无效的 JSON 数据", "code": 400, "message": None}}, status_code=400)

            sender_id = data.get("sender_id") or sender_id
            text_content = data.get("text_content")
            content_type = str(data.get("content_type", "text")).lower()
            school_id = data.get("school_id")
            class_id = data.get("class_id")
            sent_at_str = data.get("sent_at")

            if not sender_id:
                return JSONResponse({"data": {"message": "缺少 sender_id", "code": 400, "message": None}}, status_code=400)
            if content_type != "text":
                return JSONResponse({"data": {"message": "content_type 必须为 text", "code": 400, "message": None}}, status_code=400)
            if not text_content or not str(text_content).strip():
                return JSONResponse({"data": {"message": "text_content 不能为空", "code": 400, "message": None}}, status_code=400)

            text_content = str(text_content).strip()
            sent_at = datetime.datetime.now()
            if sent_at_str:
                try:
                    sent_at = datetime.datetime.strptime(sent_at_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return JSONResponse({"data": {"message": "sent_at 格式错误，应为 YYYY-MM-DD HH:MM:SS", "code": 400}}, status_code=400)

            insert_query = """
                INSERT INTO ta_message
                (sender_id, content_type, text_content, audio_data, school_id, class_id, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor = connection.cursor()
            cursor.execute(insert_query, (sender_id, "text", text_content, None, school_id, class_id, sent_at))
            connection.commit()

            new_message_id = cursor.lastrowid
            message_dict = {
                "id": new_message_id,
                "sender_id": sender_id,
                "content_type": "text",
                "text_content": text_content,
                "audio_url": None,
                "school_id": school_id,
                "class_id": class_id,
                "sent_at": sent_at.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            return safe_json_response({"data": {"message": "文本消息发送成功", "code": 201, "message": message_dict}}, status_code=201)

        # === 情况2: 二进制流 - 发送音频消息 ===
        if content_type_header.startswith("application/octet-stream"):
            if not sender_id:
                return JSONResponse({"data": {"message": "缺少 sender_id", "code": 400, "message": None}}, status_code=400)

            msg_content_type = request.query_params.get("content_type") or request.headers.get("X-Content-Type")
            if msg_content_type != "audio":
                return JSONResponse({"data": {"message": "content_type 必须为 audio", "code": 400, "message": None}}, status_code=400)

            audio_data = await request.body()
            if not audio_data:
                return JSONResponse({"data": {"message": "音频数据为空", "code": 400, "message": None}}, status_code=400)

            client_audio_type = request.headers.get("X-Audio-Content-Type") or content_type_header
            valid_types = ["audio/mpeg", "audio/wav", "audio/aac", "audio/ogg", "audio/mp4"]
            if client_audio_type not in valid_types:
                return JSONResponse(
                    {"data": {"message": f"不支持的音频类型: {client_audio_type}", "code": 400, "message": None}}, status_code=400
                )

            school_id = request.query_params.get("school_id")
            class_id = request.query_params.get("class_id")
            sent_at_str = request.query_params.get("sent_at")
            sent_at = datetime.datetime.now()
            if sent_at_str:
                try:
                    sent_at = datetime.datetime.strptime(sent_at_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return JSONResponse({"data": {"message": "sent_at 格式错误", "code": 400}}, status_code=400)

            insert_query = """
                INSERT INTO ta_message
                (sender_id, content_type, text_content, audio_data, school_id, class_id, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor = connection.cursor()
            cursor.execute(insert_query, (sender_id, "audio", None, audio_data, school_id, class_id, sent_at))
            connection.commit()

            new_message_id = cursor.lastrowid
            audio_url = f"/api/audio/{new_message_id}"
            message_dict = {
                "id": new_message_id,
                "sender_id": sender_id,
                "content_type": "audio",
                "text_content": None,
                "audio_url": audio_url,
                "school_id": school_id,
                "class_id": class_id,
                "sent_at": sent_at.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            return safe_json_response({"data": {"message": "音频消息发送成功", "code": 201, "message": message_dict}}, status_code=201)

        return JSONResponse({"data": {"message": "仅支持 application/json 或 application/octet-stream", "code": 400, "message": None}}, status_code=400)
    except Exception as e:
        app_logger.error(f"Error in add_message: {e}")
        if connection and connection.is_connected():
            connection.rollback()
        return JSONResponse({"data": {"message": "服务器内部错误", "code": 500, "message": None}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.get("/api/audio/{message_id}")
async def get_audio(message_id: int = Path(..., description="音频消息ID")):
    connection = get_db_connection()
    if not connection:
        return JSONResponse({"message": "Database error"}, status_code=500)

    cursor = None
    try:
        query = "SELECT audio_data FROM ta_message WHERE id = %s AND content_type = 'audio'"
        cursor = connection.cursor()
        cursor.execute(query, (message_id,))
        result = cursor.fetchone()

        if not result or not result[0]:
            return JSONResponse({"message": "没有找到数据：Audio not found", "code": 200}, status_code=200)

        audio_data = result[0]
        return Response(content=audio_data, media_type="audio/mpeg")
    except Exception as e:
        app_logger.error(f"Error serving audio: {e}")
        return JSONResponse({"message": "Internal error"}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.post("/notifications")
async def send_notification_to_class(request: Request):
    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        data = await request.json()
        sender_id = data.get("sender_id")
        class_id = data.get("class_id")
        content = data.get("content")

        if not all([sender_id, class_id, content]):
            return JSONResponse({"data": {"message": "缺少必需参数", "code": 400}}, status_code=400)

        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)
        insert_query = "INSERT INTO ta_notification (sender_id, receiver_id, content) VALUES (%s, %s, %s)"
        cursor.execute(insert_query, (sender_id, class_id, content))
        notification_id = cursor.lastrowid

        select_query = """
            SELECT n.*, t.name AS sender_name
            FROM ta_notification n
            JOIN ta_teacher t ON n.sender_id = t.id
            WHERE n.id = %s
        """
        cursor.execute(select_query, (notification_id,))
        new_notification = cursor.fetchone()

        if not new_notification:
            connection.rollback()
            return JSONResponse({"data": {"message": "创建通知后查询失败", "code": 500}}, status_code=500)

        new_notification = format_notification_time(new_notification)
        connection.commit()
        return safe_json_response({"data": {"message": "通知发送成功", "code": 201, "notification": new_notification}}, status_code=201)
    except mysql.connector.Error as e:
        connection.rollback()
        app_logger.error(f"Database error: {e}")
        return JSONResponse({"data": {"message": "发送通知失败", "code": 500}}, status_code=500)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"Unexpected error: {e}")
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


@router.get("/notifications/class/{class_id}")
async def get_notifications_for_class(class_id: int = Path(..., description="班级ID"), request: Request = None):
    """
    获取指定班级的最新通知，并将这些通知标记为已读 (is_read=1)。
    - class_id (path参数): 班级ID
    - limit (query参数, 可选): 默认 20，最大 100
    """
    connection = get_db_connection()
    if connection is None:
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500, "notifications": []}}, status_code=500)

    cursor = None
    try:
        limit_param = request.query_params.get("limit") if request else None
        try:
            limit = int(limit_param) if limit_param else 20
        except ValueError:
            limit = 20
        limit = max(1, min(limit, 100))

        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        select_query = """
            SELECT n.*, t.name AS sender_name
            FROM ta_notification n
            JOIN ta_teacher t ON n.sender_id = t.id
            WHERE n.receiver_id = %s AND n.is_read = 0
            ORDER BY n.created_at DESC
            LIMIT %s
        """
        cursor.execute(select_query, (class_id, limit))
        notifications = cursor.fetchall()

        notification_ids = [notif["id"] for notif in notifications]
        if notification_ids:
            ids_placeholder = ",".join(["%s"] * len(notification_ids))
            update_query = f"""
                UPDATE ta_notification
                SET is_read = 1, updated_at = CURRENT_TIMESTAMP
                WHERE id IN ({ids_placeholder})
            """
            cursor.execute(update_query, tuple(notification_ids))
        for i, notif in enumerate(notifications):
            notifications[i] = format_notification_time(notif)

        connection.commit()
        return safe_json_response({"data": {"message": "获取班级通知成功", "code": 200, "notifications": notifications}})
    except mysql.connector.Error as e:
        connection.rollback()
        app_logger.error(f"Database error fetching/reading notifications for class {class_id}: {e}")
        return JSONResponse({"data": {"message": "获取/标记通知失败", "code": 500, "notifications": []}}, status_code=500)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"Unexpected error fetching/reading notifications for class {class_id}: {e}")
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500, "notifications": []}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()



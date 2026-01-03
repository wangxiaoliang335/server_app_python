import asyncio
import datetime
import json
import os
import random
import shutil
import ssl
import struct
import time
import time as time_module
import traceback
import urllib
import uuid
from typing import Any, Dict, List, Optional

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    httpx = None
    HAS_HTTPX = False

import mysql.connector
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from common import app_logger
from db import get_db_connection
from realtime.srs import (
    SRS_APP,
    SRS_BASE_URL,
    SRS_HTTPS_PORT,
    SRS_PORT,
    SRS_SERVER,
    SRS_USE_HTTPS,
    SRS_WEBRTC_API_URL,
)
from ws.helpers import convert_datetime, convert_group_type_to_int, notify_temp_room_closed
from ws.manager import (
    active_temp_rooms,
    connections,
    safe_close,
    safe_del,
    safe_send_bytes,
    safe_send_text,
)

router = APIRouter()


from services.tencent_groups import notify_tencent_group_sync


async def handle_homework_publish(
    *,
    websocket: WebSocket,
    user_id: str,
    connection: mysql.connector.MySQLConnection,
    group_id: Optional[str],
    msg_data: Dict[str, Any],
) -> None:
    """
    处理教师发布作业：
    - 校验群组
    - 保存 homework_messages + homework_receivers
    - 给在线成员推送并标记已读
    - 给发送者回执
    """
    app_logger.info(
        f"[homework] ========== handle_homework_publish 被调用 ========== "
        f"user_id={user_id}, group_id={group_id}, "
        f"msg_data={json.dumps(msg_data, ensure_ascii=False)[:500]}"
    )
    print(f"[homework] ========== handle_homework_publish 被调用 ========== "
          f"user_id={user_id}, group_id={group_id}")
    
    group_id = (str(group_id).strip() if group_id is not None else "") or None
    if not group_id:
        error_msg = "缺少 group_id（作业消息无法路由到群组）"
        app_logger.warning(f"[homework] {error_msg}, user_id={user_id}, raw={str(msg_data)[:300]}")
        print(f"[homework] {error_msg}")
        await websocket.send_text(json.dumps({"type": "error", "message": error_msg}, ensure_ascii=False))
        return

    class_id = msg_data.get("class_id")
    school_id = msg_data.get("school_id")
    subject = msg_data.get("subject", "") or ""
    content = msg_data.get("content", "") or ""
    date_str = msg_data.get("date", "") or ""
    sender_id = msg_data.get("sender_id") or user_id
    sender_name = msg_data.get("sender_name", "") or ""

    app_logger.info(
        f"[homework] 收到作业消息 - user_id={user_id}, group_id={group_id}, sender_id={sender_id}, "
        f"class_id={class_id}, school_id={school_id}, subject={subject}, date={date_str}, content_length={len(content)}"
    )
    print(f"[homework] 收到作业消息 user_id={user_id}, group_id={group_id}, sender_id={sender_id}")

    cursor = connection.cursor(dictionary=True)
    try:
        # 验证群组是否存在
        cursor.execute(
            """
            SELECT group_id, group_name, owner_identifier
            FROM `groups`
            WHERE group_id = %s
            """,
            (group_id,),
        )
        group_info = cursor.fetchone()
        if not group_info:
            error_msg = f"没有找到数据：群组 {group_id} 不存在"
            app_logger.warning(f"[homework] {error_msg}, user_id={user_id}")
            await websocket.send_text(json.dumps({"type": "error", "message": error_msg, "code": 200}, ensure_ascii=False))
            return

        group_name = group_info.get("group_name", "") or ""
        owner_identifier = group_info.get("owner_identifier", "") or ""
        app_logger.info(
            f"[homework] 群组验证成功 - group_id={group_id}, group_name={group_name}, owner_identifier={owner_identifier}"
        )

        # 获取群组所有成员（排除发送者自己）
        cursor.execute(
            """
            SELECT user_id
            FROM `group_members`
            WHERE group_id = %s AND user_id != %s
            """,
            (group_id, sender_id),
        )
        members = cursor.fetchall()
        total_members = len(members)
        app_logger.info(f"[homework] 获取群组成员 - group_id={group_id}, 总成员数={total_members}（排除发送者）")
        
        # 如果提供了 class_id，也添加班级端（class_code）作为接收者
        # 注意：作业消息中的 class_id 实际上就是 class_code
        class_code_receiver = None
        if class_id:
            # class_id 就是 class_code，直接使用
            class_code_receiver = str(class_id).strip()
            app_logger.info(f"[homework] 添加班级端到接收者列表 - class_code={class_code_receiver}")
            # 将班级端添加到成员列表（如果不在群组成员中）
            if class_code_receiver and class_code_receiver not in [m["user_id"] for m in members]:
                members.append({"user_id": class_code_receiver})
                total_members += 1
                app_logger.info(f"[homework] 班级端已添加到接收者列表 - class_code={class_code_receiver}, 总接收者数={total_members}")

        # 解析日期
        try:
            if date_str:
                date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            else:
                date_obj = datetime.date.today()
        except ValueError:
            date_obj = datetime.date.today()
            app_logger.warning(f"[homework] 日期格式错误，使用今天: {date_str}")

        # 构建推送消息（字符串，避免重复 dumps）
        # 获取当前时间作为 created_at
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        homework_message = json.dumps(
            {
                "type": "homework",
                "class_id": class_id,
                "school_id": school_id,
                "subject": subject,
                "content": content,
                "date": date_obj.strftime("%Y-%m-%d"),
                "sender_id": sender_id,
                "sender_name": sender_name,
                "group_id": group_id,
                "group_name": group_name,
                "created_at": current_time,
            },
            ensure_ascii=False,
        )

        # 先为所有成员保存到数据库（不管是否在线）
        app_logger.info(f"[homework] 开始保存作业数据到数据库，成员数={total_members}, group_id={group_id}")
        print(f"[homework] 开始入库 group_id={group_id}, members={total_members}")

        homework_id: Optional[int] = None
        try:
            cursor.execute(
                """
                INSERT INTO homework_messages (
                    group_id, class_id, school_id, subject, content, date, sender_id, sender_name, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (group_id, class_id, school_id, subject, content, date_obj, sender_id, sender_name),
            )
            homework_id = cursor.lastrowid
            app_logger.info(f"[homework] 插入主记录成功，homework_id={homework_id}")

            # 为每个成员插入接收记录（is_read=0 表示未读）
            for member in members:
                member_id = member["user_id"]
                cursor.execute(
                    """
                    INSERT INTO homework_receivers (
                        homework_id, receiver_id, is_read, created_at
                    ) VALUES (%s, %s, 0, NOW())
                    ON DUPLICATE KEY UPDATE is_read = 0, created_at = NOW()
                    """,
                    (homework_id, member_id),
                )

            connection.commit()
            app_logger.info(f"[homework] 入库成功 - homework_id={homework_id}, receivers={total_members}")
            print(f"[homework] 入库成功 homework_id={homework_id}")
        except Exception as e:
            connection.rollback()
            app_logger.error(
                f"[homework] 入库失败 - group_id={group_id}, sender_id={sender_id}, error={e}",
                exc_info=True,
            )
            print(f"[homework] 入库失败: {e}")
            await websocket.send_text(json.dumps({"type": "error", "message": "作业保存失败"}, ensure_ascii=False))
            return

        online_count = 0
        offline_count = 0
        online_members: List[str] = []
        offline_members: List[str] = []

        # 然后推送在线的成员
        for member in members:
            member_id = member["user_id"]
            target_conn = connections.get(member_id)

            if target_conn:
                app_logger.debug(f"[homework] 用户 {member_id} 在线，推送消息并标记为已读")
                print(f"[homework] push online -> {member_id}")
                online_count += 1
                online_members.append(member_id)
                try:
                    await target_conn["ws"].send_text(homework_message)
                    cursor.execute(
                        """
                        UPDATE homework_receivers
                        SET is_read = 1, read_at = NOW()
                        WHERE homework_id = %s AND receiver_id = %s
                        """,
                        (homework_id, member_id),
                    )
                except Exception as e:
                    # 单个用户推送失败不影响整体
                    app_logger.error(
                        f"[homework] 推送/标记已读失败 - homework_id={homework_id}, receiver_id={member_id}, error={e}",
                        exc_info=True,
                    )
            else:
                app_logger.debug(f"[homework] 用户 {member_id} 不在线，已保存到数据库，等待登录时获取")
                offline_count += 1
                offline_members.append(member_id)

        try:
            connection.commit()
        except Exception as e:
            connection.rollback()
            app_logger.error(f"[homework] 提交已读标记失败 - homework_id={homework_id}, error={e}", exc_info=True)

        # 给发送者返回结果
        result_message = f"作业已发布，在线: {online_count} 人，离线: {offline_count} 人"
        app_logger.info(
            f"[homework] 完成 - group_id={group_id}, class_id={class_id}, subject={subject}, "
            f"在线={online_count}, 离线={offline_count}, 在线成员={online_members}, 离线成员={offline_members}"
        )
        print(f"[homework] 完成 online={online_count}, offline={offline_count}")
        await websocket.send_text(
            json.dumps(
                {
                    "type": "homework",
                    "status": "success",
                    "message": result_message,
                    "online_count": online_count,
                    "offline_count": offline_count,
                },
                ensure_ascii=False,
            )
        )
    finally:
        try:
            cursor.close()
        except Exception:
            pass

async def handle_notification_publish(
    *,
    websocket: WebSocket,
    user_id: str,
    connection: mysql.connector.MySQLConnection,
    group_id: Optional[str],
    msg_data: Dict[str, Any],
) -> None:
    """
    处理通知消息发布（多对一：多个教师发给一个班级）：
    - 接收者必须是班级（class_id/class_code）
    - 保存到 ta_notification 表
    - 给班级端推送并标记已读
    - 给发送者回执
    """
    app_logger.info(
        f"[notification] ========== handle_notification_publish 被调用 ========== "
        f"user_id={user_id}, group_id={group_id}, "
        f"msg_data={json.dumps(msg_data, ensure_ascii=False)[:500]}"
    )
    print(f"[notification] ========== handle_notification_publish 被调用 ========== "
          f"user_id={user_id}, group_id={group_id}")
    
    # 获取消息字段
    class_id = msg_data.get("class_id")  # 班级ID（class_code），这是接收者
    group_id = (str(group_id).strip() if group_id is not None else "") or msg_data.get("group_id") or None
    sender_id = msg_data.get("sender_id") or user_id
    sender_name = msg_data.get("sender_name", "") or ""
    content = msg_data.get("content", "") or ""
    # content_text：约定通知类型固定使用 11，避免字符串写入整数型列
    content_text_raw = msg_data.get("content_text", "") or msg_data.get("type", "notification")
    push_content_text = 11  # 推送给客户端的值（与入库一致）
    content_text = 11       # 入库固定写 11，防止字符串导致 1366 错误
    unique_group_id = msg_data.get("unique_group_id") or group_id
    group_name = msg_data.get("group_name", "") or ""

    app_logger.info(
        f"[notification] 收到通知消息 - user_id={user_id}, sender_id={sender_id}, "
        f"class_id={class_id}, group_id={group_id}, content_length={len(content)}"
    )
    print(f"[notification] 收到通知消息 user_id={user_id}, sender_id={sender_id}, class_id={class_id}, group_id={group_id}")

    cursor = connection.cursor(dictionary=True)
    try:
        # 通知是多对一：教师发给班级，必须指定 class_id（因为教师可能在多个班级中）
        if not class_id:
            error_msg = "缺少必要参数 class_id（通知必须指定接收班级）"
            app_logger.warning(f"[notification] {error_msg}, user_id={user_id}")
            await websocket.send_text(json.dumps({"type": "error", "message": error_msg}, ensure_ascii=False))
            return

        class_code_receiver = str(class_id).strip()
        app_logger.info(f"[notification] 收到 class_id={class_code_receiver}, 开始验证教师是否在该班级中")

        # 验证：该教师是否在指定的班级群中
        # 查询该班级对应的班级群（classid = class_code_receiver 且 is_class_group=1）
        cursor.execute(
            """
            SELECT g.group_id, g.group_name, g.classid, g.schoolid
            FROM `groups` g
            INNER JOIN `group_members` gm ON g.group_id = gm.group_id
            WHERE g.classid = %s AND g.is_class_group = 1 AND gm.user_id = %s
            LIMIT 1
            """,
            (class_code_receiver, sender_id),
        )
        class_group_info = cursor.fetchone()
        
        if not class_group_info:
            error_msg = f"没有找到数据：教师 {sender_id} 不在班级 {class_code_receiver} 的班级群中，无法发送通知"
            app_logger.warning(f"[notification] {error_msg}, user_id={user_id}")
            await websocket.send_text(json.dumps({"type": "error", "message": error_msg, "code": 200}, ensure_ascii=False))
            return

        # 获取班级群信息
        class_group_id = class_group_info.get("group_id")
        
        # 使用查询到的班级群信息
        if not group_id:
            group_id = class_group_id
        if not unique_group_id:
            unique_group_id = class_group_id
        if not group_name:
            group_name = class_group_info.get("group_name", "") or ""
        
        app_logger.info(
            f"[notification] 验证通过 - 教师={sender_id}, 班级ID（class_code）={class_code_receiver}, "
            f"班级群={class_group_id}, group_name={group_name}"
        )
        print(f"[notification] 验证通过 - 教师={sender_id}, 班级ID={class_code_receiver}, 班级群={class_group_id}")

        # 如果提供了 group_id，验证群组并获取群组信息（用于补充信息）
        if group_id and group_id != class_group_id:
            cursor.execute(
                """
                SELECT group_id, group_name, owner_identifier
                FROM `groups`
                WHERE group_id = %s
                """,
                (group_id,),
            )
            group_info = cursor.fetchone()
            if group_info:
                if not group_name:
                    group_name = group_info.get("group_name", "") or ""
                app_logger.info(f"[notification] 补充群组信息 - group_id={group_id}, group_name={group_name}")

        # 构建推送消息
        notification_message = json.dumps(
            {
                "type": "notification",
                "sender_id": sender_id,
                "sender_name": sender_name,
                "content": content,
                "content_text": push_content_text,
                "class_id": class_code_receiver,
                "group_id": unique_group_id,
                "group_name": group_name,
            },
            ensure_ascii=False,
        )

        # 先保存到数据库（不管是否在线）
        app_logger.info(f"[notification] 开始保存通知数据到数据库 - class_code={class_code_receiver}")
        print(f"[notification] 开始入库 class_code={class_code_receiver}")

        notification_id: Optional[int] = None
        try:
            # 通知是多对一：教师发给班级端
            # receiver_id 设置为班级ID（class_code），班级端登录时会通过 receiver_id 查询未读通知
            app_logger.info(
                f"[notification] 准备插入数据库 - sender_id={sender_id}, receiver_id={class_code_receiver} (type={type(class_code_receiver).__name__}), "
                f"unique_group_id={unique_group_id}, group_name={group_name}, content_length={len(content)}, content_text={content_text}"
            )
            print(f"[notification] 准备插入数据库 - receiver_id={class_code_receiver} (type={type(class_code_receiver).__name__}), sender_id={sender_id}")
            
            # 确保 receiver_id 是字符串类型，避免被转换为整数（防止 000011002 变成 11002）
            receiver_id_str = str(class_code_receiver).strip()
            app_logger.info(f"[notification] receiver_id 字符串化后: {receiver_id_str} (len={len(receiver_id_str)})")
            
            # 注意：如果 receiver_id 字段是整数类型，需要先执行迁移脚本 migrate_notification_receiver_id_to_varchar.sql
            # 将 receiver_id 字段改为 VARCHAR 类型，否则 000011002 会被转换为 11002（丢失前导零）
            cursor.execute(
                """
                INSERT INTO ta_notification (
                    sender_id, sender_name, receiver_id, unique_group_id, group_name, content, content_text, is_read, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, NOW())
                """,
                (sender_id, sender_name, receiver_id_str, unique_group_id, group_name, content, content_text),
            )
            notification_id = cursor.lastrowid
            connection.commit()
            
            # 验证插入的数据：查询刚插入的记录，确认 receiver_id 是否正确
            cursor.execute(
                "SELECT receiver_id, sender_id, content FROM ta_notification WHERE id = %s",
                (notification_id,)
            )
            inserted_record = cursor.fetchone()
            if inserted_record:
                actual_receiver_id = inserted_record.get("receiver_id")
                app_logger.info(
                    f"[notification] 入库成功并验证 - notification_id={notification_id}, "
                    f"期望 receiver_id={class_code_receiver}, 实际 receiver_id={actual_receiver_id}, "
                    f"sender_id={sender_id}, content_text={content_text}"
                )
                print(f"[notification] 入库成功 notification_id={notification_id}, 期望 receiver_id={class_code_receiver}, 实际 receiver_id={actual_receiver_id}")
                if str(actual_receiver_id) != str(class_code_receiver):
                    app_logger.error(
                        f"[notification] ⚠️ receiver_id 不匹配！期望={class_code_receiver}, 实际={actual_receiver_id}"
                    )
                    print(f"[notification] ⚠️ receiver_id 不匹配！期望={class_code_receiver}, 实际={actual_receiver_id}")
            else:
                app_logger.warning(f"[notification] 无法验证插入的数据 - notification_id={notification_id}")
                print(f"[notification] 无法验证插入的数据 - notification_id={notification_id}")
        except Exception as e:
            connection.rollback()
            app_logger.error(
                f"[notification] 入库失败 - sender_id={sender_id}, receiver={class_code_receiver}, error={e}",
                exc_info=True,
            )
            print(f"[notification] 入库失败: {e}")
            await websocket.send_text(json.dumps({"type": "error", "message": "通知保存失败"}, ensure_ascii=False))
            return

        # 推送给班级端（如果在线）
        target_conn = connections.get(class_code_receiver)
        is_online = False

        if target_conn:
            app_logger.info(f"[notification] 班级端 {class_code_receiver} 在线，推送消息并标记为已读")
            print(f"[notification] push online -> {class_code_receiver}")
            is_online = True
            try:
                await target_conn["ws"].send_text(notification_message)
                # 标记为已读
                if notification_id:
                    cursor.execute(
                        """
                        UPDATE ta_notification
                        SET is_read = 1, read_at = NOW()
                        WHERE id = %s
                        """,
                        (notification_id,),
                    )
                else:
                    # 如果没有 notification_id，使用其他条件
                    cursor.execute(
                        """
                        UPDATE ta_notification
                        SET is_read = 1, read_at = NOW()
                        WHERE sender_id = %s AND receiver_id = %s AND content = %s AND is_read = 0
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (sender_id, class_code_receiver, content),
                    )
                connection.commit()
                app_logger.info(f"[notification] 已推送并标记为已读 - notification_id={notification_id}, receiver={class_code_receiver}")
            except Exception as e:
                connection.rollback()
                app_logger.error(
                    f"[notification] 推送/标记已读失败 - sender_id={sender_id}, receiver={class_code_receiver}, error={e}",
                    exc_info=True,
                )
        else:
            app_logger.info(f"[notification] 班级端 {class_code_receiver} 不在线，已保存到数据库，等待登录时获取")
            print(f"[notification] 班级端不在线 -> {class_code_receiver}")

        # 给发送者返回结果
        status_text = "在线" if is_online else "离线"
        result_message = f"通知已发送到班级 {class_code_receiver}（{status_text}）"
        app_logger.info(
            f"[notification] 完成 - sender_id={sender_id}, class_id={class_code_receiver}, group_id={group_id}, "
            f"is_online={is_online}, notification_id={notification_id}"
        )
        print(f"[notification] 完成 class_code={class_code_receiver}, is_online={is_online}")
        await websocket.send_text(
            json.dumps(
                {
                    "type": "notification",
                    "status": "success",
                    "message": result_message,
                    "class_id": class_code_receiver,
                    "is_online": is_online,
                    "notification_id": notification_id,
                },
                ensure_ascii=False,
            )
        )
    finally:
        try:
            cursor.close()
        except Exception:
            pass

@router.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    current_online = len(connections)
    app_logger.info(f"[websocket] 即将接受连接 user_id={user_id}, 当前在线={current_online}")
    print(f"[websocket] 即将接受连接 user_id={user_id}, 当前在线={current_online}")
    await websocket.accept()
    
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        app_logger.error("Database connection error in /friends API.")
        print(f"[websocket][{user_id}] 数据库连接失败，立即关闭")
        await websocket.send_text(json.dumps({
            "type": "error",
            "message": "数据库连接失败",
            "code": 500
        }, ensure_ascii=False))
        await safe_close(websocket, 1008, "Database connection failed")
        return
    else:
        app_logger.info(f"[websocket] 数据库连接成功，user_id={user_id}")

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 先验证是否为班级端登录（通过 class_code 查询 ta_classes 表）
        cursor.execute(
            """
            SELECT class_code, class_name, school_stage, grade, schoolid, remark, face_url, created_at
            FROM ta_classes
            WHERE class_code = %s
            """,
            (user_id,),
        )
        class_info = cursor.fetchone()
        
        if class_info:
            # 班级端登录成功，发送登录成功响应
            login_response = {
                "type": "login_success",
                "message": "登录成功",
                "code": 200,
                "data": {
                    "class_code": class_info.get("class_code"),
                    "class_name": class_info.get("class_name"),
                    "school_stage": class_info.get("school_stage"),
                    "grade": class_info.get("grade"),
                    "schoolid": class_info.get("schoolid"),
                    "face_url": class_info.get("face_url"),
                }
            }
            await websocket.send_text(json.dumps(login_response, ensure_ascii=False, default=str))
            app_logger.info(f"[websocket] 班级端登录成功 - class_code={user_id}, class_name={class_info.get('class_name')}")
            print(f"[websocket] 班级端登录成功 - class_code={user_id}, class_name={class_info.get('class_name')}")
        else:
            # 如果不是班级端，验证是否为老师登录
            # user_id 是教师唯一ID，对应 ta_teacher 表的 teacher_unique_id 字段
            cursor.execute(
                """
                SELECT t.id, t.teacher_unique_id, t.name, t.phone, t.id_card, t.icon,
                       u.name AS detail_name, u.phone AS detail_phone, u.id_number, u.avatar
                FROM ta_teacher AS t
                LEFT JOIN ta_user_details AS u ON t.id_card = u.id_number
                WHERE t.teacher_unique_id = %s
                LIMIT 1
                """,
                (user_id,),
            )
            teacher_info = cursor.fetchone()
            
            if teacher_info:
                # 老师登录成功，发送登录成功响应
                # 优先使用 ta_user_details 的信息，如果没有则使用 ta_teacher 的信息
                name = teacher_info.get("detail_name") or teacher_info.get("name") or ""
                phone = teacher_info.get("detail_phone") or teacher_info.get("phone") or ""
                id_number = teacher_info.get("id_number") or teacher_info.get("id_card") or ""
                avatar_url = teacher_info.get("avatar") or teacher_info.get("icon") or ""
                
                login_response = {
                    "type": "login_success",
                    "message": "登录成功",
                    "code": 200,
                    "data": {
                        "user_id": teacher_info.get("teacher_unique_id"),  # 使用 teacher_unique_id 作为 user_id
                        "name": name,
                        "phone": phone,
                        "id_number": id_number,
                        "avatar_url": avatar_url,
                    }
                }
                await websocket.send_text(json.dumps(login_response, ensure_ascii=False, default=str))
                app_logger.info(f"[websocket] 老师登录成功 - user_id={user_id}, name={name}")
                print(f"[websocket] 老师登录成功 - user_id={user_id}, name={name}")
            else:
                # 既不是班级端也不是老师，登录失败
                error_response = {
                    "type": "login_failed",
                    "message": "没有找到数据：未找到对应的班级或用户",
                    "code": 200
                }
                await websocket.send_text(json.dumps(error_response, ensure_ascii=False, default=str))
                app_logger.warning(f"[websocket] 登录失败 - 未找到对应的班级或用户: {user_id}")
                print(f"[websocket] 登录失败 - 未找到对应的班级或用户: {user_id}")
                await safe_close(websocket, 1008, "Invalid user_id or class_code")
                return
        
        # 将连接添加到连接列表
        connections[user_id] = {"ws": websocket, "last_heartbeat": time.time()}
        app_logger.info(f"[websocket] 用户 {user_id} 已连接，当前在线={len(connections)}")
        print(f"用户 {user_id} 已连接，当前在线={len(connections)}")
        
        # 判断是否为班级端
        is_class_client = class_info is not None
        app_logger.info(f"[websocket] 登录类型判断 - user_id={user_id}, is_class_client={is_class_client}")
        print(f"[websocket] 登录类型判断 - user_id={user_id}, is_class_client={is_class_client}")
        
        # 查询未读通知
        # 对于班级端：user_id 就是 class_code，查询 receiver_id = class_code 的通知（教师发给班级的通知）
        # 对于教师端：查询 receiver_id = user_id 或 sender_id = user_id 的通知
        # 查询未读通知
        # 对于班级端：user_id = class_code（班级ID），receiver_id = class_code，通知都是发给班级端的
        # 对于教师端：user_id = teacher_unique_id，可能作为 receiver_id 或 sender_id
        print(" xxx SELECT ta_notification")
        update_query = """
            SELECT *
            FROM ta_notification
            WHERE (receiver_id = %s OR sender_id = %s)
            AND is_read = 0;
        """
        cursor.execute(update_query, (user_id, user_id))
        unread_notifications = cursor.fetchall()
        app_logger.info(f"[websocket] 查询未读通知 - user_id={user_id}, 数量={len(unread_notifications)}")
        print(f"[websocket] 查询未读通知 - user_id={user_id}, 数量={len(unread_notifications)}")

        if unread_notifications:
            app_logger.info(f"[websocket] 推送未读通知 - user_id={user_id}, 数量={len(unread_notifications)}")
            print(f"[websocket] 推送未读通知 - user_id={user_id}, 数量={len(unread_notifications)}")
            await websocket.send_text(json.dumps({
                "type": "unread_notifications",
                "data": unread_notifications
            }, default=convert_datetime, ensure_ascii=False))
        
        # 查询所有课前准备（包含已读与未读）
        # 对于班级端，user_id 就是 class_code，需要查询该班级的课前准备
        app_logger.info(f"[prepare_class] ========== 开始查询课前准备 ========== user_id={user_id}, is_class_client={is_class_client}")
        print(f"[prepare_class] ========== 开始查询课前准备 ========== user_id={user_id}, is_class_client={is_class_client}")
        
        prepare_sql = """
            SELECT 
                cp.prepare_id, cp.group_id, cp.class_id, cp.school_id, cp.subject, cp.content, cp.date, cp.time,
                cp.sender_id, cp.sender_name, cp.created_at, g.group_name, cpr.is_read
            FROM class_preparation cp
            INNER JOIN class_preparation_receiver cpr ON cp.prepare_id = cpr.prepare_id
            LEFT JOIN `groups` g ON cp.group_id = g.group_id
            WHERE cpr.receiver_id = %s AND cp.date = CURDATE()
            ORDER BY cp.created_at DESC
        """
        prepare_params = (user_id,)
        app_logger.info(f"[prepare_class] SQL语句: {prepare_sql.strip()}")
        app_logger.info(f"[prepare_class] SQL参数: receiver_id={user_id}")
        print(f"[prepare_class] SQL语句: {prepare_sql.strip()}")
        print(f"[prepare_class] SQL参数: receiver_id={user_id}")
        
        cursor.execute(prepare_sql, prepare_params)
        preparation_rows = cursor.fetchall()
        app_logger.info(f"[prepare_class] 查询完成 - user_id={user_id}, 查询到 {len(preparation_rows)} 条当天课前准备记录（包含已读和未读）")
        print(f"[prepare_class] 查询完成 - user_id={user_id}, 查询到 {len(preparation_rows)} 条当天课前准备记录")

        if preparation_rows:
            preparation_payload: Dict[str, Any] = {
                "type": "prepare_class_history",
                "count": len(preparation_rows),
                "data": []
            }
            unread_updates: List[int] = []
            read_count = 0
            unread_count = 0

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
                    unread_count += 1
                else:
                    read_count += 1

            app_logger.info(f"[prepare_class] 课前准备统计 - user_id={user_id}, 总数={len(preparation_rows)}, 未读={unread_count}, 已读={read_count}")
            print(f"[prepare_class] 课前准备统计 - user_id={user_id}, 总数={len(preparation_rows)}, 未读={unread_count}, 已读={read_count}")
            
            payload_str = json.dumps(preparation_payload, ensure_ascii=False)
            app_logger.info(f"[prepare_class] ========== 推送课前准备数据 ========== user_id={user_id}, 总数={len(preparation_rows)}")
            print(f"[prepare_class] ========== 推送课前准备数据 ========== user_id={user_id}, 总数={len(preparation_rows)}")
            await websocket.send_text(payload_str)

            if unread_updates:
                app_logger.info(f"[prepare_class] ========== 标记课前准备为已读 ========== user_id={user_id}, 数量={len(unread_updates)}")
                print(f"[prepare_class] ========== 标记课前准备为已读 ========== user_id={user_id}, 数量={len(unread_updates)}")
                for prep_id in unread_updates:
                    cursor.execute("""
                        UPDATE class_preparation_receiver
                        SET is_read = 1, read_at = NOW()
                        WHERE prepare_id = %s AND receiver_id = %s
                    """, (prep_id, user_id))
                connection.commit()
                app_logger.info(f"[prepare_class] 已标记 {len(unread_updates)} 条课前准备为已读 - user_id={user_id}")
                print(f"[prepare_class] 已标记 {len(unread_updates)} 条课前准备为已读 - user_id={user_id}")
        else:
            app_logger.info(f"[prepare_class] 未查询到课前准备记录 - user_id={user_id}")
            print(f"[prepare_class] 未查询到课前准备记录 - user_id={user_id}")
        
        # 查询所有未读作业消息，逐个推送（格式与在线用户收到的格式一致）
        # 注意：线上库存在 utf8mb4_0900_ai_ci / utf8mb4_unicode_ci 混用，容易触发 "Illegal mix of collations"
        # 这里对字符串比较显式指定 COLLATE，避免 WS 登录阶段直接异常断开
        # 对于班级端，user_id 就是 class_code，需要查询该班级的未读作业
        homework_rows = []
        try:
            app_logger.info(f"[homework] ========== 开始查询未读作业 ========== user_id={user_id}, is_class_client={is_class_client}")
            print(f"[homework] ========== 开始查询未读作业 ========== user_id={user_id}, is_class_client={is_class_client}")
            
            # 先调试查询：查看数据库中所有相关的 receiver_id 记录
            debug_sql = """
                SELECT DISTINCT hr.receiver_id, hr.is_read, COUNT(*) as count
                FROM homework_receivers hr
                WHERE hr.receiver_id LIKE %s OR hr.receiver_id = %s
                GROUP BY hr.receiver_id, hr.is_read
            """
            debug_params = (f"%{user_id}%", user_id)
            cursor.execute(debug_sql, debug_params)
            debug_rows = cursor.fetchall()
            app_logger.info(f"[homework] 调试查询 - 查找包含 {user_id} 的 receiver_id 记录: {debug_rows}")
            print(f"[homework] 调试查询 - 查找包含 {user_id} 的 receiver_id 记录: {debug_rows}")
            
            # 查询所有 homework_receivers 表中的 receiver_id（用于调试）
            debug_all_sql = """
                SELECT DISTINCT receiver_id, is_read, COUNT(*) as count
                FROM homework_receivers
                GROUP BY receiver_id, is_read
                LIMIT 20
            """
            cursor.execute(debug_all_sql)
            debug_all_rows = cursor.fetchall()
            app_logger.info(f"[homework] 调试查询 - homework_receivers 表中的所有 receiver_id 示例（前20条）: {debug_all_rows}")
            print(f"[homework] 调试查询 - homework_receivers 表中的所有 receiver_id 示例（前20条）: {debug_all_rows}")
            
            # 对于班级端，查询所有作业（包括已读和未读），类似课前准备的逻辑
            # 对于老师端，只查询未读作业
            if is_class_client:
                # 班级端：查询所有作业（包含已读和未读）
                homework_sql = """
                    SELECT
                        hm.homework_id, hm.group_id, hm.class_id, hm.school_id, hm.subject, hm.content, hm.date,
                        hm.sender_id, hm.sender_name, hm.created_at, g.group_name, hr.is_read, hr.receiver_id
                    FROM homework_messages hm
                    INNER JOIN homework_receivers hr ON hm.homework_id = hr.homework_id
                    LEFT JOIN `groups` g
                        ON (hm.group_id COLLATE utf8mb4_unicode_ci) = (g.group_id COLLATE utf8mb4_unicode_ci)
                    WHERE (hr.receiver_id COLLATE utf8mb4_unicode_ci) = %s
                    ORDER BY hm.created_at DESC
                """
                app_logger.info(f"[homework] 班级端查询模式：查询所有作业（包含已读和未读）")
                print(f"[homework] 班级端查询模式：查询所有作业（包含已读和未读）")
            else:
                # 老师端：只查询未读作业
                homework_sql = """
                    SELECT
                        hm.homework_id, hm.group_id, hm.class_id, hm.school_id, hm.subject, hm.content, hm.date,
                        hm.sender_id, hm.sender_name, hm.created_at, g.group_name, hr.is_read, hr.receiver_id
                    FROM homework_messages hm
                    INNER JOIN homework_receivers hr ON hm.homework_id = hr.homework_id
                    LEFT JOIN `groups` g
                        ON (hm.group_id COLLATE utf8mb4_unicode_ci) = (g.group_id COLLATE utf8mb4_unicode_ci)
                    WHERE (hr.receiver_id COLLATE utf8mb4_unicode_ci) = %s AND hr.is_read = 0
                    ORDER BY hm.created_at DESC
                """
                app_logger.info(f"[homework] 老师端查询模式：只查询未读作业")
                print(f"[homework] 老师端查询模式：只查询未读作业")
            homework_params = (user_id,)
            app_logger.info(f"[homework] SQL语句: {homework_sql.strip()}")
            app_logger.info(f"[homework] SQL参数: receiver_id={user_id}, 参数类型={type(user_id)}, 参数长度={len(str(user_id))}")
            print(f"[homework] SQL语句: {homework_sql.strip()}")
            print(f"[homework] SQL参数: receiver_id={user_id}, 参数类型={type(user_id)}, 参数长度={len(str(user_id))}")
            
            cursor.execute(homework_sql, homework_params)
            homework_rows = cursor.fetchall()
            app_logger.info(f"[homework] 查询完成 - user_id={user_id}, 查询到 {len(homework_rows)} 条作业（is_class_client={is_class_client}）")
            print(f"[homework] 查询完成 - user_id={user_id}, 查询到 {len(homework_rows)} 条作业（is_class_client={is_class_client}）")
        except Exception as e:
            app_logger.error(f"[homework] 登录查询离线作业失败: user_id={user_id}, error={e}", exc_info=True)
            print(f"[homework] 登录查询离线作业失败: {e}")

        if homework_rows:
            app_logger.info(f"[homework] ========== 开始推送作业 ========== user_id={user_id}, 数量={len(homework_rows)}, is_class_client={is_class_client}")
            print(f"[homework] ========== 开始推送作业 ========== user_id={user_id}, 数量={len(homework_rows)}, is_class_client={is_class_client}")
            unread_homework_updates: List[int] = []
            read_count = 0
            unread_count = 0
            
            # 逐个推送，格式与在线用户收到的格式完全一致
            for idx, hw in enumerate(homework_rows, 1):
                is_read = hw.get("is_read", 0)
                if is_read == 0:
                    unread_count += 1
                else:
                    read_count += 1
                
                # 格式化 created_at 时间
                created_at = hw.get("created_at")
                if created_at:
                    if isinstance(created_at, datetime.datetime):
                        created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        created_at_str = str(created_at)
                else:
                    created_at_str = None
                
                homework_message = {
                    "type": "homework",
                    "class_id": hw.get("class_id"),
                    "school_id": hw.get("school_id"),
                    "subject": hw.get("subject"),
                    "content": hw.get("content"),
                    "date": hw.get("date").strftime("%Y-%m-%d") if hw.get("date") else None,
                    "sender_id": hw.get("sender_id"),
                    "sender_name": hw.get("sender_name"),
                    "group_id": hw.get("group_id"),
                    "group_name": hw.get("group_name") or "",
                    "created_at": created_at_str
                }
                
                payload_str = json.dumps(homework_message, ensure_ascii=False)
                app_logger.info(f"[homework] 推送第 {idx}/{len(homework_rows)} 条作业 - user_id={user_id}, homework_id={hw.get('homework_id')}, subject={hw.get('subject')}, is_read={is_read}")
                print(f"[homework] 推送第 {idx}/{len(homework_rows)} 条作业 - user_id={user_id}, homework_id={hw.get('homework_id')}, subject={hw.get('subject')}, is_read={is_read}")
                await websocket.send_text(payload_str)
                
                # 只标记未读的作业为已读
                hw_id = hw.get("homework_id")
                if hw_id and is_read == 0:
                    unread_homework_updates.append(hw_id)

            app_logger.info(f"[homework] 作业统计 - user_id={user_id}, 总数={len(homework_rows)}, 未读={unread_count}, 已读={read_count}")
            print(f"[homework] 作业统计 - user_id={user_id}, 总数={len(homework_rows)}, 未读={unread_count}, 已读={read_count}")

            if unread_homework_updates:
                app_logger.info(f"[homework] ========== 标记作业为已读 ========== user_id={user_id}, 数量={len(unread_homework_updates)}")
                print(f"[homework] ========== 标记作业为已读 ========== user_id={user_id}, 数量={len(unread_homework_updates)}")
                for hw_id in unread_homework_updates:
                    cursor.execute("""
                        UPDATE homework_receivers
                        SET is_read = 1, read_at = NOW()
                        WHERE homework_id = %s AND receiver_id = %s
                    """, (hw_id, user_id))
                connection.commit()
                app_logger.info(f"[homework] 已标记 {len(unread_homework_updates)} 条作业为已读 - user_id={user_id}")
                print(f"[homework] 已标记 {len(unread_homework_updates)} 条作业为已读 - user_id={user_id}")
        else:
            app_logger.info(f"[homework] 未查询到未读作业 - user_id={user_id}")
            print(f"[homework] 未查询到未读作业 - user_id={user_id}")

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
                # 使用纯数字生成房间ID（时间戳毫秒 + 4位随机数）
                room_id = str(int(time.time() * 1000)) + str(random.randint(1000, 9999))
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
                        "status": "success",
                        "group_id": group_key,
                        "message": "没有找到数据：未找到该班级的临时房间",
                        "code": 200
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
                # 为避免客户端重复弹窗，重复加入时使用 status=duplicate 且 message 为空
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
                    "status": "duplicate" if was_member else "success",
                    "message": "" if was_member else f"已加入临时房间（班级: {group_key}）"
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
                    "status": "success",
                    "group_id": group_key,
                    "message": "没有找到数据：未找到临时房间或已解散",
                    "code": 200
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
                
                # 如果是推流成功，自动通知房间内其他用户该用户开始说话
                if action_type == "publish" and group_id:
                    try:
                        await broadcast_voice_speaking(user_id, group_id, is_speaking=True)
                    except Exception as broadcast_error:
                        app_logger.error(f"[srs_webrtc] 广播推流开始消息失败 - user_id={user_id}, group_id={group_id}, error={broadcast_error}")
                        # 广播失败不影响推流流程，只记录错误
                
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
                # 区分不同类型的错误，提供更友好的错误消息
                import urllib.error
                
                error_code = None
                user_friendly_msg = None
                is_retryable = False
                
                # 处理 HTTP 错误
                if isinstance(e, urllib.error.HTTPError):
                    error_code = e.code
                    is_retryable = error_code in [502, 503, 504]
                    if error_code == 502:
                        user_friendly_msg = "SRS 服务器暂时不可用，请稍后重试。如果问题持续，请联系管理员。"
                        app_logger.error(f"[srs_webrtc] SRS 服务器返回 502 Bad Gateway - API_URL={api_url}, 可能原因：SRS 服务未运行、Nginx 代理配置错误或网络问题")
                    elif error_code == 503:
                        user_friendly_msg = "SRS 服务器过载，请稍后重试。"
                    elif error_code == 504:
                        user_friendly_msg = "SRS 服务器响应超时，请稍后重试。"
                    elif error_code == 404:
                        user_friendly_msg = "SRS API 端点不存在，请检查配置。"
                    else:
                        user_friendly_msg = f"SRS 服务器返回错误 (HTTP {error_code})，请稍后重试。"
                elif isinstance(e, urllib.error.URLError):
                    is_retryable = True
                    user_friendly_msg = "无法连接到 SRS 服务器，请检查网络连接或联系管理员。"
                    app_logger.error(f"[srs_webrtc] 网络连接错误 - API_URL={api_url}, 错误: {str(e)}")
                elif HAS_HTTPX:
                    if isinstance(e, httpx.HTTPStatusError):
                        error_code = e.response.status_code
                        is_retryable = error_code in [502, 503, 504]
                        if error_code == 502:
                            user_friendly_msg = "SRS 服务器暂时不可用，请稍后重试。如果问题持续，请联系管理员。"
                        else:
                            user_friendly_msg = f"SRS 服务器返回错误 (HTTP {error_code})，请稍后重试。"
                    elif isinstance(e, httpx.RequestError):
                        is_retryable = True
                        user_friendly_msg = "无法连接到 SRS 服务器，请检查网络连接或联系管理员。"
                elif isinstance(e, TimeoutError) or "timeout" in str(e).lower():
                    is_retryable = True
                    user_friendly_msg = "请求 SRS 服务器超时，请稍后重试。"
                else:
                    user_friendly_msg = f"处理 {action_type} 请求时发生未知错误，请稍后重试。"
                
                # 构建错误响应
                error_msg = f"处理 SRS {action_type} offer 时出错: {str(e)}"
                app_logger.error(f"[srs_webrtc] {error_msg}", exc_info=True)
                print(f"[srs_webrtc] 错误: {error_msg}")
                
                error_response = {
                    "type": "srs_error",
                    "action": action_type,
                    "message": user_friendly_msg,  # 用户友好的错误消息
                    "error_code": error_code,  # HTTP 错误代码（如果有）
                    "technical_message": error_msg,  # 技术细节（用于调试）
                    "retryable": is_retryable  # 是否可重试
                }
                error_response_json = json.dumps(error_response, ensure_ascii=False)
                app_logger.error(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}, 消息内容: {error_response_json}")
                print(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}: {error_response_json}")
                await websocket.send_text(error_response_json)

        async def broadcast_voice_speaking(speaker_user_id: str, group_id: str, is_speaking: bool, user_name: str = None):
            """
            广播语音说话状态给房间内其他用户
            speaker_user_id: 说话的用户ID
            group_id: 房间群组ID
            is_speaking: true=开始说话, false=停止说话
            user_name: 用户名称（可选，如果不提供则从连接信息中获取）
            """
            try:
                if not group_id:
                    app_logger.warning(f"[voice_speaking] 缺少 group_id - user_id={speaker_user_id}")
                    return
                
                # 获取房间信息
                room_info = active_temp_rooms.get(group_id)
                if not room_info:
                    app_logger.warning(f"[voice_speaking] 房间不存在 - group_id={group_id}, user_id={speaker_user_id}")
                    return
                
                # 获取房间成员列表
                members = room_info.get("members", [])
                if speaker_user_id not in members:
                    app_logger.warning(f"[voice_speaking] 用户不在房间成员列表中 - user_id={speaker_user_id}, group_id={group_id}")
                    return
                
                # 获取用户信息（从连接信息中）
                user_info = connections.get(speaker_user_id, {})
                if not user_name:
                    user_name = user_info.get("user_name", f"用户{speaker_user_id}")
                
                # 构建广播消息
                broadcast_message = {
                    "type": "voice_speaking",
                    "user_id": speaker_user_id,
                    "user_name": user_name,
                    "group_id": group_id,
                    "is_speaking": is_speaking,
                    "timestamp": time.time()
                }
                broadcast_json = json.dumps(broadcast_message, ensure_ascii=False)
                
                # 广播给房间内其他用户（不包括发送者）
                broadcast_count = 0
                for member_id in members:
                    if member_id != speaker_user_id:  # 不发送给自己
                        member_conn = connections.get(member_id)
                        if member_conn:
                            try:
                                await member_conn["ws"].send_text(broadcast_json)
                                broadcast_count += 1
                            except Exception as send_error:
                                app_logger.error(f"[voice_speaking] 发送消息失败 - to={member_id}, error={send_error}")
                
                app_logger.info(f"[voice_speaking] 广播说话状态 - user_id={speaker_user_id}, user_name={user_name}, group_id={group_id}, is_speaking={is_speaking}, 广播给 {broadcast_count} 个用户")
                print(f"[voice_speaking] 用户 {speaker_user_id}({user_name}) {'开始说话' if is_speaking else '停止说话'} - 房间 {group_id}, 广播给 {broadcast_count} 个用户")
                
            except Exception as e:
                app_logger.error(f"[voice_speaking] 广播说话状态失败 - user_id={speaker_user_id}, error={e}", exc_info=True)
                print(f"[voice_speaking] 错误: {e}")

        async def handle_voice_speaking(msg_data: Dict[str, Any]):
            """
            处理语音说话状态消息
            客户端检测到音频活动时发送此消息，服务器广播给房间内其他用户
            """
            try:
                group_id = msg_data.get('group_id')
                is_speaking = msg_data.get('is_speaking', False)  # true=开始说话, false=停止说话
                user_name = msg_data.get('user_name', '')  # 可选：用户名称
                
                # 调用广播函数
                await broadcast_voice_speaking(user_id, group_id, is_speaking, user_name)
                
            except Exception as e:
                app_logger.error(f"[voice_speaking] 处理说话状态消息失败 - user_id={user_id}, error={e}", exc_info=True)
                print(f"[voice_speaking] 错误: {e}")

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
                # 清理用户从所有临时房间的成员列表中移除，并通知其他用户该用户停止说话
                for group_id, room_info in list(active_temp_rooms.items()):
                    members = room_info.get("members", [])
                    if user_id in members:
                        # 通知其他用户该用户停止说话（断开连接意味着停止推流）
                        try:
                            await broadcast_voice_speaking(user_id, group_id, is_speaking=False)
                        except Exception as broadcast_error:
                            app_logger.error(f"[webrtc] 广播停止说话消息失败 - user_id={user_id}, group_id={group_id}, error={broadcast_error}")
                        members.remove(user_id)
                        app_logger.info(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（WebSocketDisconnect），当前成员数={len(members)}")
                        print(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（WebSocketDisconnect），当前成员数={len(members)}")
                break
            except RuntimeError as e:
                # 已收到 disconnect 后再次 receive 会到这里
                print(f"用户 {user_id} receive RuntimeError: {e}")
                # 清理用户从所有临时房间的成员列表中移除，并通知其他用户该用户停止说话
                for group_id, room_info in list(active_temp_rooms.items()):
                    members = room_info.get("members", [])
                    if user_id in members:
                        # 通知其他用户该用户停止说话（断开连接意味着停止推流）
                        try:
                            await broadcast_voice_speaking(user_id, group_id, is_speaking=False)
                        except Exception as broadcast_error:
                            app_logger.error(f"[webrtc] 广播停止说话消息失败 - user_id={user_id}, group_id={group_id}, error={broadcast_error}")
                        members.remove(user_id)
                        app_logger.info(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（RuntimeError），当前成员数={len(members)}")
                        print(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（RuntimeError），当前成员数={len(members)}")
                break

            # starlette 会在断开时 raise WebSocketDisconnect，保险起见也判断 type
            if message.get("type") == "websocket.disconnect":
                print(f"用户 {user_id} 断开（disconnect event）")
                # 清理用户从所有临时房间的成员列表中移除，并通知其他用户该用户停止说话
                for group_id, room_info in list(active_temp_rooms.items()):
                    members = room_info.get("members", [])
                    if user_id in members:
                        # 通知其他用户该用户停止说话（断开连接意味着停止推流）
                        try:
                            await broadcast_voice_speaking(user_id, group_id, is_speaking=False)
                        except Exception as broadcast_error:
                            app_logger.error(f"[webrtc] 广播停止说话消息失败 - user_id={user_id}, group_id={group_id}, error={broadcast_error}")
                        members.remove(user_id)
                        app_logger.info(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（disconnect event），当前成员数={len(members)}")
                        print(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（disconnect event），当前成员数={len(members)}")
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
                        try:
                            msg_data1 = json.loads(msg)
                        except Exception as e:
                            app_logger.error(
                                f"[websocket][{user_id}] 解析 to: 消息 JSON 失败: error={e}, raw={msg[:500]}",
                                exc_info=True,
                            )
                            print(f"[websocket][{user_id}] 解析 to: 消息 JSON 失败: {e}")
                            await websocket.send_text("格式错误: to:<target_id>:<JSON消息>")
                            continue

                        msg_type = msg_data1.get("type") if isinstance(msg_data1, dict) else None
                        app_logger.info(
                            f"[websocket][{user_id}] 收到 to: 消息: target_id={target_id}, type={msg_type}, raw={msg[:300]}"
                        )
                        print(msg)
                        print(msg_type)
                        if msg_data1['type'] == "1":
                            print(" 加好友消息")
                            app_logger.info(f"[websocket][加好友] 收到加好友请求 - user_id={user_id}, target_id={target_id}")
                            
                            # 解析 JSON
                            msg_data = json.loads(msg)
                            friend_teacher_unique_id = msg_data.get('teacher_unique_id')
                            
                            if not friend_teacher_unique_id:
                                error_msg = "缺少 teacher_unique_id 参数"
                                print(f"[websocket][加好友] ❌ 错误: {error_msg}")
                                app_logger.error(f"[websocket][加好友] {error_msg} - user_id={user_id}")
                                await websocket.send_text(json.dumps({"type": "error", "message": error_msg}, ensure_ascii=False))
                                continue
                            
                            # 检查是否是自己
                            if user_id == friend_teacher_unique_id:
                                error_msg = "不能添加自己为好友"
                                print(f"[websocket][加好友] ❌ 错误: {error_msg}")
                                app_logger.warning(f"[websocket][加好友] {error_msg} - user_id={user_id}")
                                await websocket.send_text(json.dumps({"type": "error", "message": error_msg}, ensure_ascii=False))
                                continue
                            
                            cursor = None
                            try:
                                cursor = connection.cursor(dictionary=True)
                                
                                # 检查是否已经是好友
                                check_query = """
                                    SELECT teacher_unique_id FROM ta_friend 
                                    WHERE teacher_unique_id = %s AND friendcode = %s
                                    LIMIT 1
                                """
                                cursor.execute(check_query, (user_id, friend_teacher_unique_id))
                                existing_friend = cursor.fetchone()
                                
                                if existing_friend:
                                    print(f"[websocket][加好友] ⚠️  已经是好友 - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                    app_logger.info(f"[websocket][加好友] 已经是好友 - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                else:
                                    # 插入好友关系
                                    insert_query = """
                                        INSERT INTO ta_friend (teacher_unique_id, friendcode)
                                        VALUES (%s, %s)
                                    """
                                    cursor.execute(insert_query, (user_id, friend_teacher_unique_id))
                                    connection.commit()
                                    print(f"[websocket][加好友] ✅ 添加好友成功 - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                    app_logger.info(f"[websocket][加好友] 添加好友成功 - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                
                                # 发送通知
                                target_conn = connections.get(target_id)
                                if target_conn:
                                    print(f"[websocket][加好友] {target_id} 在线, 来自: {user_id}")
                                    await target_conn["ws"].send_text(f"[私信来自 {user_id}] {msg}")
                                    # 给添加者返回成功消息
                                    success_response = {
                                        "type": "add_friend_success",
                                        "message": "添加好友成功",
                                        "friend_teacher_unique_id": friend_teacher_unique_id,
                                        "target_online": True
                                    }
                                    await websocket.send_text(json.dumps(success_response, ensure_ascii=False))
                                    print(f"[websocket][加好友] ✅ 已通知添加者 - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                    app_logger.info(f"[websocket][加好友] 已通知添加者 - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                else:
                                    print(f"[websocket][加好友] {target_id} 不在线, 来自: {user_id}")
                                    
                                    # 插入通知
                                    notification_query = """
                                        INSERT INTO ta_notification (sender_id, receiver_id, content, content_text)
                                        VALUES (%s, %s, %s, %s)
                                    """
                                    cursor.execute(notification_query, (user_id, friend_teacher_unique_id, msg_data.get('text', ''), msg_data.get('type', '1')))
                                    connection.commit()
                                    print(f"[websocket][加好友] ✅ 通知已保存 - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                    app_logger.info(f"[websocket][加好友] 通知已保存 - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                    
                                    # 给添加者返回成功消息
                                    success_response = {
                                        "type": "add_friend_success",
                                        "message": "添加好友成功，对方不在线，已发送通知",
                                        "friend_teacher_unique_id": friend_teacher_unique_id,
                                        "target_online": False
                                    }
                                    await websocket.send_text(json.dumps(success_response, ensure_ascii=False))
                                    print(f"[websocket][加好友] ✅ 已通知添加者 - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                    app_logger.info(f"[websocket][加好友] 已通知添加者 - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                
                            except mysql.connector.Error as e:
                                if connection:
                                    connection.rollback()
                                error_msg = f"数据库错误: {e}"
                                print(f"[websocket][加好友] ❌ {error_msg}")
                                app_logger.error(f"[websocket][加好友] {error_msg} - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                await websocket.send_text(json.dumps({"type": "error", "message": "添加好友失败，请稍后重试"}, ensure_ascii=False))
                            except Exception as e:
                                if connection:
                                    connection.rollback()
                                error_msg = f"未知错误: {e}"
                                print(f"[websocket][加好友] ❌ {error_msg}")
                                traceback_str = traceback.format_exc()
                                app_logger.error(f"[websocket][加好友] {error_msg}\n{traceback_str} - user_id={user_id}, friend_id={friend_teacher_unique_id}")
                                await websocket.send_text(json.dumps({"type": "error", "message": "添加好友失败，请稍后重试"}, ensure_ascii=False))
                            finally:
                                if cursor:
                                    cursor.close()
                        elif msg_data1['type'] == "3": 
                            print(" 创建群")   
                            app_logger.info(f"[创建群] 开始处理创建群组请求 - user_id={user_id}")
                            try:
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
                                
                                # 检查 classid 是否看起来像是一个群组ID（以"01"结尾），如果是则可能是客户端错误
                                if classid and str(classid).endswith("01"):
                                    # 检查这个 classid 是否在 groups 表中存在（说明是群组ID而不是班级ID）
                                    cursor.execute("SELECT group_id FROM `groups` WHERE group_id = %s", (str(classid),))
                                    existing_group = cursor.fetchone()
                                    if existing_group:
                                        error_msg = f"classid={classid} 是一个已存在的群组ID，而不是班级ID。请使用正确的班级ID创建群组。"
                                        print(f"[创建群] 错误: {error_msg}")
                                        app_logger.error(f"[创建群] {error_msg}")
                                        # 拒绝创建，返回错误消息给客户端
                                        error_response = {
                                            "type": "error",
                                            "message": error_msg,
                                            "code": 400
                                        }
                                        error_response_json = json.dumps(error_response, ensure_ascii=False)
                                        await websocket.send_text(error_response_json)
                                        print(f"[创建群] 已拒绝创建请求并向客户端返回错误 - user_id={user_id}, classid={classid}")
                                        continue  # 跳过后续处理
                                
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
                                
                                # 检查群组是否已存在
                                cursor.execute(
                                    "SELECT group_id FROM `groups` WHERE group_id = %s",
                                    (unique_group_id,)
                                )
                                existing_group = cursor.fetchone()
                                
                                if existing_group:
                                    print(f"[创建群] 群组 {unique_group_id} 已存在，跳过插入 groups 表")
                                    app_logger.info(f"[创建群] 群组 {unique_group_id} 已存在，跳过插入 groups 表")
                                else:
                                    print(f"[创建群] 插入 groups 表 - group_id={unique_group_id}, group_name={group_name}")
                                    app_logger.info(f"[创建群] 插入 groups 表 - group_id={unique_group_id}, group_name={group_name}, is_class_group={is_class_group}")
                                    try:
                                        cursor.execute(insert_group_sql, insert_group_params)
                                        affected_rows = cursor.rowcount
                                        print(f"[创建群] 插入 groups 表成功 - group_id={unique_group_id}, 影响行数: {affected_rows}")
                                        app_logger.info(f"[创建群] 插入 groups 表成功 - group_id={unique_group_id}, 影响行数: {affected_rows}")
                                    except Exception as insert_error:
                                        error_msg = f"插入 groups 表失败 - group_id={unique_group_id}, error={insert_error}"
                                        print(f"[创建群] {error_msg}")
                                        app_logger.error(f"[创建群] {error_msg}", exc_info=True)
                                        import traceback
                                        traceback_str = traceback.format_exc()
                                        print(f"[创建群] 错误堆栈: {traceback_str}")
                                        raise  # 重新抛出异常，让外层处理
                                
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
                                        
                                        # 检查群主是否已在群组中
                                        cursor.execute(
                                            "SELECT user_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                                            (unique_group_id, member_user_id)
                                        )
                                        existing_owner = cursor.fetchone()
                                        
                                        if existing_owner:
                                            print(f"[创建群] 群主 {member_user_id} 已在群组 {unique_group_id} 中，跳过插入")
                                            app_logger.info(f"[创建群] 群主 {member_user_id} 已在群组 {unique_group_id} 中，跳过插入")
                                        else:
                                            print(f"[创建群] 插入群主 - group_id={unique_group_id}, user_id={member_user_id}, user_name={member_user_name}, self_role={member_self_role}")
                                            app_logger.info(f"[创建群] 插入群主 - group_id={unique_group_id}, user_id={member_user_id}, user_name={member_user_name}, self_role={member_self_role}")
                                            cursor.execute(insert_member_sql, insert_member_params)
                                        processed_member_ids.add(member_user_id)
                                    else:
                                        print(f"[创建群] 警告: member_info 缺少 user_id，跳过")
                                else:
                                    print(f"[创建群] 警告: 缺少 member_info（群主信息），这是必需的")
                                
                                # 第二步：处理 members 数组（管理员和其他成员）
                                print(f"[创建群] 开始处理 members 数组 - group_id={unique_group_id}, members数量={len(members_list) if members_list else 0}")
                                if members_list:
                                    print(f"[创建群] members 数组内容: {members_list}")
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
                                        
                                        # 检查成员是否已在群组中
                                        cursor.execute(
                                            "SELECT user_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                                            (unique_group_id, member_user_id)
                                        )
                                        existing_member = cursor.fetchone()
                                        
                                        if existing_member:
                                            print(f"[创建群] 成员 {member_user_id} 已在群组 {unique_group_id} 中，跳过插入")
                                            app_logger.info(f"[创建群] 成员 {member_user_id} 已在群组 {unique_group_id} 中，跳过插入")
                                        else:
                                            print(f"[创建群] 插入成员 - group_id={unique_group_id}, user_id={member_user_id}, user_name={member_user_name}, self_role={self_role}")
                                            app_logger.info(f"[创建群] 插入成员 - group_id={unique_group_id}, user_id={member_user_id}, user_name={member_user_name}, self_role={self_role}")
                                            cursor.execute(insert_member_sql, insert_member_params)
                                        processed_member_ids.add(member_user_id)
                                
                                print(f"[创建群] 成员列表处理完成 - group_id={unique_group_id}, 已处理成员数={len(processed_member_ids)}")
                                app_logger.info(f"[创建群] 成员列表处理完成 - group_id={unique_group_id}, 已处理成员数={len(processed_member_ids)}, 成员列表={list(processed_member_ids)}")
                                
                                # 如果群组有 classid，将班级作为成员也插入到群组成员列表中
                                if classid and str(classid).strip():
                                    class_id_str = str(classid).strip()
                                    if class_id_str not in processed_member_ids:
                                        print(f"[创建群] 将班级 {class_id_str} 作为成员添加到群组 {unique_group_id}")
                                        app_logger.info(f"[创建群] 将班级 {class_id_str} 作为成员添加到群组 {unique_group_id}")
                                        cursor.execute(
                                            "SELECT group_id FROM `group_members` WHERE group_id = %s AND user_id = %s", 
                                            (unique_group_id, class_id_str)
                                        )
                                        class_member_exists = cursor.fetchone()
                                        
                                        if class_member_exists:
                                            print(f"[创建群] 班级成员已存在，跳过插入: group_id={unique_group_id}, class_id={class_id_str}")
                                            app_logger.info(f"[创建群] 班级成员已存在，跳过插入: group_id={unique_group_id}, class_id={class_id_str}")
                                        else:
                                            insert_class_member_sql = """
                                                INSERT INTO `group_members` (
                                                    group_id, user_id, user_name, self_role, join_time, msg_flag,
                                                    self_msg_flag, readed_seq, unread_num
                                                ) VALUES (
                                                    %s, %s, %s, %s, NOW(), %s, %s, %s, %s
                                                )
                                            """
                                            class_insert_params = (
                                                unique_group_id,
                                                class_id_str,
                                                "班级",  # user_name 设为"班级"
                                                200,     # self_role 设为普通成员
                                                0,       # msg_flag
                                                0,       # self_msg_flag
                                                0,       # readed_seq
                                                0,       # unread_num
                                            )
                                            cursor.execute(insert_class_member_sql, class_insert_params)
                                            print(f"[创建群] 成功将班级 {class_id_str} 添加为群组 {unique_group_id} 的成员")
                                            app_logger.info(f"[创建群] 成功将班级 {class_id_str} 添加为群组 {unique_group_id} 的成员")
                                            processed_member_ids.add(class_id_str)
                                    else:
                                        print(f"[创建群] 班级 {class_id_str} 已在成员列表中，跳过")
                                        app_logger.info(f"[创建群] 班级 {class_id_str} 已在成员列表中，跳过")
                                else:
                                    print(f"[创建群] 群组 {unique_group_id} 没有 classid，跳过添加班级成员")
                                    app_logger.info(f"[创建群] 群组 {unique_group_id} 没有 classid，跳过添加班级成员")
                                
                                print(f"[创建群] 准备提交事务 - group_id={unique_group_id}")
                                app_logger.info(f"[创建群] 准备提交事务 - group_id={unique_group_id}")
                                connection.commit()
                                print(f"[创建群] 事务提交成功 - group_id={unique_group_id}")
                                app_logger.info(f"[创建群] 事务提交成功 - group_id={unique_group_id}, group_name={group_name}")
                                
                                # 同步到腾讯IM（同步等待结果，确保成员被正确添加）
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
                                        "is_class_group": is_class_group,  # 添加 is_class_group 字段，用于区分班级群和普通群
                                        "classid": classid,  # 添加 classid 字段，用于辅助判断
                                        "member_info": member_info,  # 群主信息
                                        "MemberList": []  # 成员列表（包含群主和管理员）
                                    }
                                    
                                    # 构建成员列表（包含群主和管理员）
                                    member_list = []
                                    added_member_accounts = set()  # 用于跟踪已添加的成员，避免重复
                                    
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
                                            added_member_accounts.add(owner_user_id)
                                            print(f"[创建群] 腾讯IM数据：添加群主 - user_id={owner_user_id}")
                                    
                                    # 添加管理员和其他成员（从 members 数组）
                                    if members_list:
                                        for m in members_list:
                                            member_user_id = m.get('user_id') or m.get('unique_member_id')
                                            if not member_user_id:
                                                continue
                                            
                                            # 如果已经在 member_list 中添加过，跳过避免重复
                                            if member_user_id in added_member_accounts:
                                                print(f"[创建群] 腾讯IM数据：跳过重复成员 - user_id={member_user_id}")
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
                                            added_member_accounts.add(member_user_id)
                                            print(f"[创建群] 腾讯IM数据：添加成员 - user_id={member_user_id}, Role={role_str}")
                                    
                                    # 如果群组有 classid，将班级也添加到腾讯IM成员列表中
                                    if classid and str(classid).strip():
                                        class_id_str = str(classid).strip()
                                        if class_id_str not in added_member_accounts:
                                            member_list.append({
                                                "Member_Account": class_id_str,
                                                "user_id": class_id_str,
                                                "Role": "Member",  # 班级作为普通成员
                                                "self_role": 200
                                            })
                                            added_member_accounts.add(class_id_str)
                                            print(f"[创建群] 腾讯IM数据：添加班级成员 - class_id={class_id_str}, Role=Member")
                                            app_logger.info(f"[创建群] 腾讯IM数据：添加班级成员 - class_id={class_id_str}, Role=Member")
                                    
                                    tencent_group_data["MemberList"] = member_list
                                    print(f"[创建群] 腾讯IM数据构建完成 - group_id={unique_group_id}, 成员数={len(member_list)}")
                                    app_logger.info(f"[创建群] 腾讯IM数据构建完成 - group_id={unique_group_id}, 成员数={len(member_list)}, 成员列表={member_list}")
                                    
                                    # 同步调用腾讯IM接口（等待结果）
                                    print(f"[创建群] 准备同步到腾讯IM - group_id={unique_group_id}")
                                    app_logger.info(f"[创建群] 准备同步到腾讯IM - group_id={unique_group_id}, group_name={group_name}")
                                    
                                    # 调用同步函数（同步等待结果）
                                    result = await notify_tencent_group_sync(owner_identifier, [tencent_group_data])
                                    print(f"[创建群] 腾讯IM同步完成 - group_id={unique_group_id}, result_status={result.get('status')}")
                                    app_logger.info(f"[创建群] 腾讯IM同步结果 - group_id={unique_group_id}, result={result}")
                                    
                                    # 检查结果，无论成功与否，都需要单独添加成员
                                    # 因为 import_group API 可能不会自动添加 MemberList 中的成员
                                    error_info = result.get("error", "")
                                    error_code = result.get("error_code")
                                    need_add_members = False
                                    
                                    if result.get("status") != "success":
                                        # 如果是因为群组已存在（ErrorCode 10021），需要单独添加成员
                                        if error_code == 10021:
                                            need_add_members = True
                                            print(f"[创建群] 群组已存在（ErrorCode 10021），需要单独添加成员 - group_id={unique_group_id}")
                                            app_logger.info(f"[创建群] 群组已存在，需要单独添加成员 - group_id={unique_group_id}")
                                    else:
                                        # 即使 import_group 成功，也需要单独添加成员
                                        # 因为腾讯IM的 import_group API 可能不会自动添加 MemberList 中的成员
                                        need_add_members = True
                                        print(f"[创建群] import_group 成功，但需要单独添加成员以确保成员被正确添加 - group_id={unique_group_id}")
                                        app_logger.info(f"[创建群] import_group 成功，需要单独添加成员 - group_id={unique_group_id}")
                                    
                                    # 如果需要添加成员，调用 add_group_member API
                                    if need_add_members and member_list:
                                        print(f"[创建群] 需要单独添加成员 - group_id={unique_group_id}, 成员数={len(member_list)}")
                                        app_logger.info(f"[创建群] 需要单独添加成员 - group_id={unique_group_id}, 成员数={len(member_list)}")
                                        
                                        # 调用 add_group_member API 添加成员
                                        from services.tencent_api import build_tencent_request_url
                                        from services.tencent_sig import generate_tencent_user_sig
                                        import os
                                        
                                        TENCENT_API_IDENTIFIER = os.getenv("TENCENT_API_IDENTIFIER")
                                        TENCENT_API_USER_SIG = os.getenv("TENCENT_API_USER_SIG")
                                        TENCENT_API_SECRET_KEY = os.getenv("TENCENT_API_SECRET_KEY")
                                        TENCENT_API_TIMEOUT = float(os.getenv("TENCENT_API_TIMEOUT", "10"))
                                        
                                        identifier_to_use = TENCENT_API_IDENTIFIER
                                        usersig_to_use = None
                                        
                                        if TENCENT_API_SECRET_KEY and identifier_to_use:
                                            try:
                                                usersig_to_use = generate_tencent_user_sig(identifier_to_use)
                                            except Exception as e:
                                                print(f"[创建群] UserSig 生成失败: {e}")
                                                usersig_to_use = TENCENT_API_USER_SIG
                                        else:
                                            usersig_to_use = TENCENT_API_USER_SIG
                                        
                                        if identifier_to_use and usersig_to_use:
                                            add_member_url = build_tencent_request_url(
                                                identifier=identifier_to_use,
                                                usersig=usersig_to_use,
                                                path_override="v4/group_open_http_svc/add_group_member"
                                            )
                                            
                                            if add_member_url:
                                                # 构建添加成员的 payload（排除群主，因为群主已经在群组中）
                                                add_member_list = [
                                                    {"Member_Account": m["Member_Account"], "Role": m["Role"]}
                                                    for m in member_list
                                                    if m.get("Role") != "Owner"  # 排除群主
                                                ]
                                                
                                                # 在添加成员之前，先检查并注册班级账号（如果存在）
                                                if classid and str(classid).strip():
                                                    class_id_str = str(classid).strip()
                                                    # 检查班级是否在要添加的成员列表中
                                                    class_in_member_list = any(
                                                        m.get("Member_Account") == class_id_str 
                                                        for m in add_member_list
                                                    )
                                                    
                                                    if class_in_member_list:
                                                        print(f"[创建群] 检测到班级成员，先注册班级账号到腾讯IM - class_id={class_id_str}")
                                                        app_logger.info(f"[创建群] 检测到班级成员，先注册班级账号到腾讯IM - class_id={class_id_str}")
                                                        
                                                        # 从数据库获取班级名称和头像
                                                        class_nick = f"班级{class_id_str}"  # 默认昵称
                                                        class_face_url = ""  # 默认头像
                                                        try:
                                                            cursor.execute(
                                                                "SELECT class_name, face_url FROM ta_classes WHERE class_code = %s",
                                                                (class_id_str,)
                                                            )
                                                            class_info = cursor.fetchone()
                                                            if class_info:
                                                                class_nick = class_info.get("class_name", class_nick)
                                                                class_face_url = class_info.get("face_url", class_face_url) or ""
                                                                print(f"[创建群] 从数据库获取班级信息 - class_id={class_id_str}, class_name={class_nick}, face_url={class_face_url}")
                                                                app_logger.info(f"[创建群] 从数据库获取班级信息 - class_id={class_id_str}, class_name={class_nick}")
                                                        except Exception as db_error:
                                                            print(f"[创建群] 查询班级信息失败，使用默认值 - class_id={class_id_str}, error={db_error}")
                                                            app_logger.warning(f"[创建群] 查询班级信息失败 - class_id={class_id_str}, error={db_error}")
                                                        
                                                        # 调用 account_import API 注册班级账号
                                                        account_import_url = build_tencent_request_url(
                                                            identifier=identifier_to_use,
                                                            usersig=usersig_to_use,
                                                            path_override="v4/im_open_login_svc/account_import"
                                                        )
                                                        
                                                        if account_import_url:
                                                            account_import_payload = {
                                                                "Identifier": class_id_str,
                                                                "Nick": class_nick,  # 使用从数据库获取的班级名称
                                                                "FaceUrl": class_face_url  # 使用从数据库获取的班级头像
                                                            }
                                                            
                                                            def _import_account() -> Dict[str, Any]:
                                                                headers = {"Content-Type": "application/json; charset=utf-8"}
                                                                encoded_payload = json.dumps(account_import_payload, ensure_ascii=False).encode("utf-8")
                                                                request_obj = urllib.request.Request(
                                                                    url=account_import_url, data=encoded_payload, headers=headers, method="POST"
                                                                )
                                                                try:
                                                                    with urllib.request.urlopen(request_obj, timeout=TENCENT_API_TIMEOUT) as response:
                                                                        raw_body = response.read()
                                                                        text_body = raw_body.decode("utf-8", errors="replace")
                                                                        try:
                                                                            parsed_body = json.loads(text_body)
                                                                        except json.JSONDecodeError:
                                                                            parsed_body = None
                                                                        return {"status": "success", "http_status": response.status, "response": parsed_body or text_body}
                                                                except urllib.error.HTTPError as e:
                                                                    body = e.read().decode("utf-8", errors="replace")
                                                                    return {"status": "error", "http_status": e.code, "error": body}
                                                                except Exception as e:
                                                                    return {"status": "error", "http_status": None, "error": str(e)}
                                                            
                                                            import_result = await asyncio.to_thread(_import_account)
                                                            
                                                            if import_result.get("status") == "success":
                                                                response_data = import_result.get("response")
                                                                if isinstance(response_data, dict):
                                                                    action_status = response_data.get("ActionStatus")
                                                                    error_code = response_data.get("ErrorCode")
                                                                    error_info = response_data.get("ErrorInfo", "")
                                                                    if action_status == "OK" and error_code == 0:
                                                                        print(f"[创建群] ✅ 班级账号注册成功 - class_id={class_id_str}")
                                                                        app_logger.info(f"[创建群] 班级账号注册成功 - class_id={class_id_str}")
                                                                    else:
                                                                        # ErrorCode 70169 表示账号已存在，这是正常的
                                                                        if error_code == 70169:
                                                                            print(f"[创建群] ℹ️  班级账号已存在（ErrorCode 70169）- class_id={class_id_str}")
                                                                            app_logger.info(f"[创建群] 班级账号已存在 - class_id={class_id_str}")
                                                                        else:
                                                                            print(f"[创建群] ⚠️  班级账号注册失败 - class_id={class_id_str}, ErrorCode={error_code}, ErrorInfo={error_info}")
                                                                            app_logger.warning(f"[创建群] 班级账号注册失败 - class_id={class_id_str}, ErrorCode={error_code}, ErrorInfo={error_info}")
                                                                else:
                                                                    print(f"[创建群] ⚠️  班级账号注册响应格式异常 - class_id={class_id_str}, response={response_data}")
                                                                    app_logger.warning(f"[创建群] 班级账号注册响应格式异常 - class_id={class_id_str}, response={response_data}")
                                                            else:
                                                                print(f"[创建群] ⚠️  班级账号注册请求失败 - class_id={class_id_str}, error={import_result.get('error')}")
                                                                app_logger.warning(f"[创建群] 班级账号注册请求失败 - class_id={class_id_str}, error={import_result.get('error')}")
                                                        else:
                                                            print(f"[创建群] ⚠️  无法构建 account_import URL - class_id={class_id_str}")
                                                            app_logger.warning(f"[创建群] 无法构建 account_import URL - class_id={class_id_str}")
                                                
                                                if add_member_list:
                                                    print(f"[创建群] 准备添加成员到腾讯IM - group_id={unique_group_id}, 成员列表={add_member_list}")
                                                    app_logger.info(f"[创建群] 准备添加成员到腾讯IM - group_id={unique_group_id}, 成员列表={add_member_list}")
                                                    
                                                    add_member_payload = {
                                                        "GroupId": unique_group_id,
                                                        "MemberList": add_member_list,
                                                        "Silence": 0
                                                    }
                                                    
                                                    print(f"[创建群] 添加成员 payload: {json.dumps(add_member_payload, ensure_ascii=False, indent=2)}")
                                                    app_logger.info(f"[创建群] 添加成员 payload: {json.dumps(add_member_payload, ensure_ascii=False, indent=2)}")
                                                    
                                                    def _add_tencent_members() -> Dict[str, Any]:
                                                        headers = {"Content-Type": "application/json; charset=utf-8"}
                                                        encoded_payload = json.dumps(add_member_payload, ensure_ascii=False).encode("utf-8")
                                                        request_obj = urllib.request.Request(
                                                            url=add_member_url, data=encoded_payload, headers=headers, method="POST"
                                                        )
                                                        try:
                                                            with urllib.request.urlopen(request_obj, timeout=TENCENT_API_TIMEOUT) as response:
                                                                raw_body = response.read()
                                                                text_body = raw_body.decode("utf-8", errors="replace")
                                                                try:
                                                                    parsed_body = json.loads(text_body)
                                                                except json.JSONDecodeError:
                                                                    parsed_body = None
                                                                return {"status": "success", "http_status": response.status, "response": parsed_body or text_body}
                                                        except urllib.error.HTTPError as e:
                                                            body = e.read().decode("utf-8", errors="replace")
                                                            return {"status": "error", "http_status": e.code, "error": body}
                                                        except Exception as e:
                                                            return {"status": "error", "http_status": None, "error": str(e)}
                                                    
                                                    add_member_result = await asyncio.to_thread(_add_tencent_members)
                                                    
                                                    print(f"[创建群] 添加成员结果 - group_id={unique_group_id}, result={add_member_result}")
                                                    app_logger.info(f"[创建群] 添加成员结果 - group_id={unique_group_id}, result={add_member_result}")
                                                    
                                                    if add_member_result.get("status") == "success":
                                                        response_data = add_member_result.get("response")
                                                        if isinstance(response_data, dict):
                                                            action_status = response_data.get("ActionStatus")
                                                            error_code = response_data.get("ErrorCode")
                                                            error_info = response_data.get("ErrorInfo", "")
                                                            if action_status == "OK" and error_code == 0:
                                                                print(f"[创建群] ✅ 成员添加成功 - group_id={unique_group_id}, 成员数={len(add_member_list)}, 成员列表={[m.get('Member_Account') for m in add_member_list]}")
                                                                app_logger.info(f"[创建群] 成员添加成功 - group_id={unique_group_id}, 成员数={len(add_member_list)}, 成员列表={[m.get('Member_Account') for m in add_member_list]}")
                                                            else:
                                                                print(f"[创建群] ⚠️  成员添加失败 - group_id={unique_group_id}, ErrorCode={error_code}, ErrorInfo={error_info}, 尝试添加的成员={[m.get('Member_Account') for m in add_member_list]}")
                                                                app_logger.warning(f"[创建群] 成员添加失败 - group_id={unique_group_id}, ErrorCode={error_code}, ErrorInfo={error_info}, 尝试添加的成员={[m.get('Member_Account') for m in add_member_list]}")
                                                                
                                                                # ErrorCode 10019 表示所有标识符都无效，可能是成员未在腾讯IM中注册
                                                                if error_code == 10019:
                                                                    print(f"[创建群] ⚠️  错误码 10019: 成员标识符无效，可能需要在腾讯IM中先注册这些成员 - 成员列表={[m.get('Member_Account') for m in add_member_list]}")
                                                                    app_logger.warning(f"[创建群] 错误码 10019: 成员标识符无效，可能需要在腾讯IM中先注册这些成员 - 成员列表={[m.get('Member_Account') for m in add_member_list]}")
                                                    else:
                                                        print(f"[创建群] ⚠️  成员添加请求失败 - group_id={unique_group_id}, error={add_member_result.get('error')}")
                                                        app_logger.warning(f"[创建群] 成员添加请求失败 - group_id={unique_group_id}, error={add_member_result.get('error')}")
                                                else:
                                                    print(f"[创建群] 没有需要添加的成员（只有群主） - group_id={unique_group_id}")
                                            else:
                                                print(f"[创建群] ⚠️  无法构建 add_group_member URL - group_id={unique_group_id}")
                                        else:
                                            print(f"[创建群] ⚠️  缺少腾讯IM配置，无法添加成员 - group_id={unique_group_id}")
                                    
                                except Exception as tencent_sync_error:
                                    # 同步失败不影响群组创建，但记录错误
                                    print(f"[创建群] ⚠️  腾讯IM同步异常 - group_id={unique_group_id}, error={tencent_sync_error}")
                                    app_logger.error(f"[创建群] 腾讯IM同步异常 - group_id={unique_group_id}, error={tencent_sync_error}", exc_info=True)
                                
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
                                            # 使用纯数字生成房间ID（时间戳毫秒 + 4位随机数）
                                            room_id = str(int(time.time() * 1000)) + str(random.randint(1000, 9999))
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
                                print(f"[创建群] 准备构建返回给客户端的响应 - group_id={unique_group_id}")
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
                                
                                print(f"[创建群] 准备发送响应给客户端 - group_id={unique_group_id}")
                                await websocket.send_text(response_json)
                                print(f"[创建群] 响应已发送给客户端 - group_id={unique_group_id}")
                                print(f"[创建群] 创建群组流程完成 - group_id={unique_group_id}")
                                app_logger.info(f"[创建群] 创建群组流程完成 - group_id={unique_group_id}, user_id={user_id}")
                            except Exception as create_group_error:
                                error_msg = f"创建群组时发生异常 - user_id={user_id}, error={create_group_error}"
                                print(f"[创建群] {error_msg}")
                                app_logger.error(f"[创建群] {error_msg}", exc_info=True)
                                import traceback
                                traceback_str = traceback.format_exc()
                                print(f"[创建群] 错误堆栈: {traceback_str}")
                                # 回滚事务
                                if connection and connection.is_connected():
                                    connection.rollback()
                                    print(f"[创建群] 已回滚事务")
                                # 发送错误消息给客户端
                                try:
                                    error_response = {
                                        "type": "3",
                                        "status": "error",
                                        "message": f"创建群组失败: {str(create_group_error)}",
                                        "group_id": msg_data1.get('group_id', '')
                                    }
                                    await websocket.send_text(json.dumps(error_response, ensure_ascii=False))
                                except Exception as send_error:
                                    app_logger.error(f"[创建群] 发送错误消息失败: {send_error}")

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
                                await websocket.send_text(json.dumps({
                                    "type": "error",
                                    "message": f"没有找到数据：群 {unique_group_id} 不存在",
                                    "code": 200
                                }, ensure_ascii=False))
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
                                    await websocket.send_text(json.dumps({
                                        "type": "error",
                                        "message": "没有找到数据：群没有其他成员",
                                        "code": 200
                                    }, ensure_ascii=False))
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
                                    await websocket.send_text(json.dumps({
                                        "type": "error",
                                        "message": "没有找到数据：群没有其他成员可以接收此消息",
                                        "code": 200
                                    }, ensure_ascii=False))
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
                                error_msg = f"没有找到数据：群组 {group_id} 不存在"
                                app_logger.warning(f"[prepare_class] {error_msg}, user_id={user_id}")
                                await websocket.send_text(json.dumps({
                                    "type": "error",
                                    "message": error_msg,
                                    "code": 200
                                }, ensure_ascii=False))
                                continue
                            
                            group_name = group_info.get('group_name', '')
                            owner_identifier = group_info.get('owner_identifier', '')
                            app_logger.info(f"[prepare_class] 群组验证成功 - group_id={group_id}, group_name={group_name}, owner_identifier={owner_identifier}")
                            
                            # 获取群组所有成员（排除发送者自己）
                            cursor.execute("""
                                SELECT user_id 
                                FROM `group_members`
                                WHERE group_id = %s AND user_id != %s
                            """, (group_id, sender_id))
                            members = cursor.fetchall()
                            total_members = len(members)
                            app_logger.info(f"[prepare_class] 获取群组成员 - group_id={group_id}, 总成员数={total_members}（排除发送者）")
                            
                            # 如果提供了 class_id，也添加班级端（class_code）作为接收者
                            # 注意：课前准备消息中的 class_id 实际上就是 class_code
                            class_code_receiver = None
                            if class_id:
                                # class_id 就是 class_code，直接使用
                                class_code_receiver = str(class_id).strip()
                                app_logger.info(f"[prepare_class] 添加班级端到接收者列表 - class_code={class_code_receiver}")
                                # 将班级端添加到成员列表（如果不在群组成员中）
                                if class_code_receiver and class_code_receiver not in [m["user_id"] for m in members]:
                                    members.append({"user_id": class_code_receiver})
                                    total_members += 1
                                    app_logger.info(f"[prepare_class] 班级端已添加到接收者列表 - class_code={class_code_receiver}, 总接收者数={total_members}")
                            
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
                                    ON DUPLICATE KEY UPDATE is_read = 0, created_at = NOW()
                                """, (prepare_id, member_id))
                            
                            # 也为发送者自己插入接收记录（标记为已读，因为是发送者自己发送的）
                            cursor.execute("""
                                INSERT INTO class_preparation_receiver (
                                    prepare_id, receiver_id, is_read, read_at, created_at
                                ) VALUES (%s, %s, 1, NOW(), NOW())
                                ON DUPLICATE KEY UPDATE is_read = 1, read_at = NOW(), created_at = NOW()
                            """, (prepare_id, sender_id))
                            
                            connection.commit()
                            app_logger.info(f"[prepare_class] 已为所有 {total_members} 个成员和发送者 {sender_id} 保存课前准备数据")
                            
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
                        # 作业消息: 发送给群组所有成员
                        elif msg_data1['type'] == "homework":
                            app_logger.info(
                                f"[homework] 收到作业消息（to:格式） - user_id={user_id}, target_id={target_id}, "
                                f"class_id={msg_data1.get('class_id')}, subject={msg_data1.get('subject')}, "
                                f"raw={json.dumps(msg_data1, ensure_ascii=False)[:500]}"
                            )
                            print(f"[homework] 收到作业消息（to:格式） - user_id={user_id}, target_id={target_id}, "
                                  f"class_id={msg_data1.get('class_id')}, subject={msg_data1.get('subject')}")
                            await handle_homework_publish(
                                websocket=websocket,
                                user_id=user_id,
                                connection=connection,
                                group_id=target_id,
                                msg_data=msg_data1,
                            )
                            continue
                        # 通知消息: 发送给指定接收者或群组所有成员
                        elif msg_data1['type'] == "notification":
                            app_logger.info(
                                f"[notification] 收到通知消息（to:格式） - user_id={user_id}, target_id={target_id}, "
                                f"receiver_id={msg_data1.get('receiver_id')}, receiver_ids={msg_data1.get('receiver_ids')}, "
                                f"raw={json.dumps(msg_data1, ensure_ascii=False)[:500]}"
                            )
                            print(f"[notification] 收到通知消息（to:格式） - user_id={user_id}, target_id={target_id}, "
                                  f"receiver_id={msg_data1.get('receiver_id')}, receiver_ids={msg_data1.get('receiver_ids')}")
                            await handle_notification_publish(
                                websocket=websocket,
                                user_id=user_id,
                                connection=connection,
                                group_id=target_id,
                                msg_data=msg_data1,
                            )
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
                        # 处理语音说话状态消息
                        elif msg_data1['type'] == "voice_speaking":
                            await handle_voice_speaking(msg_data1)
                            continue
        
                    else:
                        print(" 格式错误")
                        await websocket.send_text("格式错误: to:<target_id>:<消息>")
                else:
                    msg_data_raw = None
                    try:
                        msg_data_raw = json.loads(data)
                        # 记录所有收到的消息类型
                        if isinstance(msg_data_raw, dict):
                            msg_type = msg_data_raw.get("type")
                            app_logger.info(
                                f"[websocket] 收到消息 - user_id={user_id}, type={msg_type}, "
                                f"raw={data[:300]}"
                            )
                            print(f"[websocket] 收到消息 - user_id={user_id}, type={msg_type}")
                    except Exception:
                        msg_data_raw = None
                        app_logger.warning(
                            f"[websocket] 解析消息失败 - user_id={user_id}, raw={data[:300]}"
                        )

                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "6":
                        await handle_temp_room_creation(msg_data_raw)
                        continue
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "temp_room_owner_leave":
                        await handle_temp_room_owner_leave(msg_data_raw.get("group_id"))
                        continue
                    # 兼容纯 JSON 格式的作业发布（不带 to: 前缀）
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "homework":
                        group_id_from_msg = (
                            msg_data_raw.get("group_id")
                            or msg_data_raw.get("target_id")
                            or msg_data_raw.get("to")
                        )
                        app_logger.info(
                            f"[homework] ========== 收到纯JSON作业消息 ========== "
                            f"user_id={user_id}, group_id={group_id_from_msg}, "
                            f"class_id={msg_data_raw.get('class_id')}, subject={msg_data_raw.get('subject')}, "
                            f"raw={data[:500]}"
                        )
                        print(f"[homework] ========== 收到纯JSON作业消息 ========== "
                              f"user_id={user_id}, group_id={group_id_from_msg}, "
                              f"class_id={msg_data_raw.get('class_id')}, subject={msg_data_raw.get('subject')}")
                        await handle_homework_publish(
                            websocket=websocket,
                            user_id=user_id,
                            connection=connection,
                            group_id=group_id_from_msg,
                            msg_data=msg_data_raw,
                        )
                        continue
                    # 兼容纯 JSON 格式的通知发布（不带 to: 前缀）
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "notification":
                        group_id_from_msg = (
                            msg_data_raw.get("group_id")
                            or msg_data_raw.get("target_id")
                            or msg_data_raw.get("to")
                        )
                        app_logger.info(
                            f"[notification] ========== 收到纯JSON通知消息 ========== "
                            f"user_id={user_id}, group_id={group_id_from_msg}, "
                            f"receiver_id={msg_data_raw.get('receiver_id')}, receiver_ids={msg_data_raw.get('receiver_ids')}, "
                            f"raw={data[:500]}"
                        )
                        print(f"[notification] ========== 收到纯JSON通知消息 ========== "
                              f"user_id={user_id}, group_id={group_id_from_msg}, "
                              f"receiver_id={msg_data_raw.get('receiver_id')}, receiver_ids={msg_data_raw.get('receiver_ids')}")
                        await handle_notification_publish(
                            websocket=websocket,
                            user_id=user_id,
                            connection=connection,
                            group_id=group_id_from_msg,
                            msg_data=msg_data_raw,
                        )
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
                    # 处理语音说话状态消息（纯 JSON 格式）
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "voice_speaking":
                        await handle_voice_speaking(msg_data_raw)
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
                app_logger.info(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（外层捕获），当前成员数={len(members)}")
                print(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（外层捕获），当前成员数={len(members)}")
        
        if connection:
            connection.rollback()
    except Exception as e:
        # 捕获其他未预期的异常
        app_logger.error(f"[websocket][{user_id}] 未预期的异常: {e}", exc_info=True)
        print(f"[websocket][{user_id}] 未预期的异常: {e}")
        # 确保清理用户从临时房间中移除
        for group_id, room_info in list(active_temp_rooms.items()):
            members = room_info.get("members", [])
            if user_id in members:
                members.remove(user_id)
                app_logger.info(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（异常清理），当前成员数={len(members)}")
                print(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（异常清理），当前成员数={len(members)}")
    finally:
        # 最终清理：确保用户从连接列表和临时房间中移除
        if user_id in connections:
            connections.pop(user_id, None)
            print(f"[websocket][{user_id}] 从连接列表中移除（finally块）")
        
        # 再次检查并清理临时房间成员（防止遗漏）
        for group_id, room_info in list(active_temp_rooms.items()):
            members = room_info.get("members", [])
            if user_id in members:
                members.remove(user_id)
                app_logger.info(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（finally清理），当前成员数={len(members)}")
                print(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（finally清理），当前成员数={len(members)}")
        
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
        closed = await safe_close(websocket)
        print(f"[websocket][{user_id}] safe_close called, closed={closed}，当前在线={len(connections)}")
        app_logger.info(f"WebSocket关闭，数据库连接已释放，user_id={user_id}。")

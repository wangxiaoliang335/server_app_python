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

@router.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    current_online = len(connections)
    app_logger.info(f"[websocket] 即将接受连接 user_id={user_id}, 当前在线={current_online}")
    print(f"[websocket] 即将接受连接 user_id={user_id}, 当前在线={current_online}")
    await websocket.accept()
    connections[user_id] = {"ws": websocket, "last_heartbeat": time.time()}
    app_logger.info(f"[websocket] 用户 {user_id} 已连接，当前在线={len(connections)}")
    print(f"用户 {user_id} 已连接，当前在线={len(connections)}")

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        app_logger.error("Database connection error in /friends API.")
        print(f"[websocket][{user_id}] 数据库连接失败，立即关闭")
        return JSONResponse({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }, status_code=500)
    else:
        app_logger.info(f"[websocket] 数据库连接成功，user_id={user_id}")

    cursor = None
    try:
        # 查询条件改为：receiver_id = user_id 或 sender_id = user_id，并且 is_read = 0
        print(" xxx SELECT ta_notification")
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
        
        # 查询所有课前准备（包含已读与未读）
        cursor.execute("""
            SELECT 
                cp.prepare_id, cp.group_id, cp.class_id, cp.school_id, cp.subject, cp.content, cp.date, cp.time,
                cp.sender_id, cp.sender_name, cp.created_at, g.group_name, cpr.is_read
            FROM class_preparation cp
            INNER JOIN class_preparation_receiver cpr ON cp.prepare_id = cpr.prepare_id
            LEFT JOIN `groups` g ON cp.group_id = g.group_id
            WHERE cpr.receiver_id = %s
            ORDER BY cp.created_at DESC
        """, (user_id,))
        preparation_rows = cursor.fetchall()

        if preparation_rows:
            preparation_payload: Dict[str, Any] = {
                "type": "prepare_class_history",
                "count": len(preparation_rows),
                "data": []
            }
            unread_updates: List[int] = []

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

            payload_str = json.dumps(preparation_payload, ensure_ascii=False)
            app_logger.info(f"[prepare_class] 用户 {user_id} 登录，推送课前准备数据: {payload_str}")
            print(f"[prepare_class] 登录推送课前准备数据: {payload_str}")
            await websocket.send_text(payload_str)

            if unread_updates:
                app_logger.info(f"[prepare_class] 标记 {len(unread_updates)} 条课前准备为已读，user_id={user_id}")
                for prep_id in unread_updates:
                    cursor.execute("""
                        UPDATE class_preparation_receiver
                        SET is_read = 1, read_at = NOW()
                        WHERE prepare_id = %s AND receiver_id = %s
                    """, (prep_id, user_id))
                connection.commit()

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
                        "status": "not_found",
                        "group_id": group_key,
                        "message": "未找到该班级的临时房间"
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
                    "status": "not_found",
                    "group_id": group_key,
                    "message": "未找到临时房间或已解散"
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
                error_msg = f"处理 SRS {action_type} offer 时出错: {str(e)}"
                app_logger.error(f"[srs_webrtc] {error_msg}", exc_info=True)
                print(f"[srs_webrtc] 错误: {error_msg}")
                error_response = {
                    "type": "srs_error",
                    "action": action_type,
                    "message": error_msg
                }
                error_response_json = json.dumps(error_response, ensure_ascii=False)
                app_logger.error(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}, 消息内容: {error_response_json}")
                print(f"[srs_webrtc] 返回 {action_type} 错误消息给用户 {user_id}: {error_response_json}")
                await websocket.send_text(error_response_json)

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
                # 清理用户从所有临时房间的成员列表中移除
                for group_id, room_info in list(active_temp_rooms.items()):
                    members = room_info.get("members", [])
                    if user_id in members:
                        members.remove(user_id)
                        app_logger.info(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（WebSocketDisconnect），当前成员数={len(members)}")
                        print(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（WebSocketDisconnect），当前成员数={len(members)}")
                break
            except RuntimeError as e:
                # 已收到 disconnect 后再次 receive 会到这里
                print(f"用户 {user_id} receive RuntimeError: {e}")
                # 清理用户从所有临时房间的成员列表中移除
                for group_id, room_info in list(active_temp_rooms.items()):
                    members = room_info.get("members", [])
                    if user_id in members:
                        members.remove(user_id)
                        app_logger.info(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（RuntimeError），当前成员数={len(members)}")
                        print(f"[webrtc] 用户 {user_id} 离开房间 {group_id}（RuntimeError），当前成员数={len(members)}")
                break

            # starlette 会在断开时 raise WebSocketDisconnect，保险起见也判断 type
            if message.get("type") == "websocket.disconnect":
                print(f"用户 {user_id} 断开（disconnect event）")
                # 清理用户从所有临时房间的成员列表中移除
                for group_id, room_info in list(active_temp_rooms.items()):
                    members = room_info.get("members", [])
                    if user_id in members:
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
                                
                                print(f"[创建群] 准备提交事务 - group_id={unique_group_id}")
                                app_logger.info(f"[创建群] 准备提交事务 - group_id={unique_group_id}")
                                connection.commit()
                                print(f"[创建群] 事务提交成功 - group_id={unique_group_id}")
                                app_logger.info(f"[创建群] 事务提交成功 - group_id={unique_group_id}, group_name={group_name}")
                                
                                # 同步到腾讯IM（异步执行，不阻塞响应）
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
                                    
                                    tencent_group_data["MemberList"] = member_list
                                    print(f"[创建群] 腾讯IM数据构建完成 - group_id={unique_group_id}, 成员数={len(member_list)}")
                                    app_logger.info(f"[创建群] 腾讯IM数据构建完成 - group_id={unique_group_id}, 成员数={len(member_list)}, 成员列表={member_list}")
                                    
                                    # 异步调用同步函数（不阻塞当前流程）
                                    print(f"[创建群] 准备同步到腾讯IM - group_id={unique_group_id}")
                                    app_logger.info(f"[创建群] 准备同步到腾讯IM - group_id={unique_group_id}, group_name={group_name}")
                                    
                                    # 使用 asyncio.create_task 异步执行，不等待结果
                                    print(f"[创建群] 创建异步任务同步到腾讯IM - group_id={unique_group_id}")
                                    async def sync_to_tencent():
                                        try:
                                            print(f"[创建群] 异步任务开始 - group_id={unique_group_id}")
                                            # 调用同步函数（需要传入列表格式）
                                            result = await notify_tencent_group_sync(owner_identifier, [tencent_group_data])
                                            print(f"[创建群] 异步任务完成 - group_id={unique_group_id}, result_status={result.get('status')}")
                                            if result.get("status") == "success":
                                                print(f"[创建群] 腾讯IM同步成功 - group_id={unique_group_id}")
                                                app_logger.info(f"[创建群] 腾讯IM同步成功 - group_id={unique_group_id}")
                                            else:
                                                print(f"[创建群] 腾讯IM同步失败 - group_id={unique_group_id}, error={result.get('error')}")
                                                app_logger.warning(f"[创建群] 腾讯IM同步失败 - group_id={unique_group_id}, error={result.get('error')}")
                                        except Exception as sync_error:
                                            print(f"[创建群] 腾讯IM同步异常 - group_id={unique_group_id}, error={sync_error}")
                                            app_logger.error(f"[创建群] 腾讯IM同步异常 - group_id={unique_group_id}, error={sync_error}", exc_info=True)
                                    
                                    # 创建异步任务，不等待完成
                                    asyncio.create_task(sync_to_tencent())
                                    
                                except Exception as tencent_sync_error:
                                    # 同步失败不影响群组创建
                                    print(f"[创建群] 准备腾讯IM同步时出错 - group_id={unique_group_id}, error={tencent_sync_error}")
                                    app_logger.error(f"[创建群] 准备腾讯IM同步时出错 - group_id={unique_group_id}, error={tencent_sync_error}", exc_info=True)
                                
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
                                error_msg = f"群组 {group_id} 不存在"
                                app_logger.warning(f"[prepare_class] {error_msg}, user_id={user_id}")
                                await websocket.send_text(json.dumps({
                                    "type": "error",
                                    "message": error_msg
                                }, ensure_ascii=False))
                                continue
                            
                            group_name = group_info.get('group_name', '')
                            owner_identifier = group_info.get('owner_identifier', '')
                            app_logger.info(f"[prepare_class] 群组验证成功 - group_id={group_id}, group_name={group_name}, owner_identifier={owner_identifier}")
                            
                            # 获取群组所有成员（使用 group_members 表）
                            cursor.execute("""
                                SELECT user_id 
                                FROM `group_members`
                                WHERE group_id = %s
                            """, (group_id,))
                            members = cursor.fetchall()
                            total_members = len(members)
                            app_logger.info(f"[prepare_class] 获取群组成员 - group_id={group_id}, 总成员数={total_members}")
                            
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
                                """, (prepare_id, member_id))
                            
                            connection.commit()
                            app_logger.info(f"[prepare_class] 已为所有 {total_members} 个成员保存课前准备数据")
                            
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
        
                    else:
                        print(" 格式错误")
                        await websocket.send_text("格式错误: to:<target_id>:<消息>")
                else:
                    msg_data_raw = None
                    try:
                        msg_data_raw = json.loads(data)
                    except Exception:
                        msg_data_raw = None

                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "6":
                        await handle_temp_room_creation(msg_data_raw)
                        continue
                    if isinstance(msg_data_raw, dict) and msg_data_raw.get("type") == "temp_room_owner_leave":
                        await handle_temp_room_owner_leave(msg_data_raw.get("group_id"))
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

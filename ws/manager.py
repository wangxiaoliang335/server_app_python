import asyncio
import time
from typing import Any, Dict

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from common import app_logger
from db import get_db_connection
from realtime.srs import SRS_APP, SRS_WEBRTC_API_URL

# ===== 停止事件，用于控制心跳协程退出 =====
stop_event = asyncio.Event()

# ===== 本机维护的客户端连接表 / 临时房间状态 =====
connections: Dict[str, Dict] = {}  # {user_id: {"ws": WebSocket, "last_heartbeat": timestamp}}
active_temp_rooms: Dict[str, Dict[str, Any]] = {}  # {group_id: {...room info...}}


def safe_del(user_id: str):
    return connections.pop(user_id, None)


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


async def heartbeat_checker():
    try:
        while not stop_event.is_set():
            now = time.time()
            to_remove = []
            for uid, conn in list(connections.items()):
                if now - conn.get("last_heartbeat", 0) > 30:
                    print(f"用户 {uid} 心跳超时，断开连接")
                    await safe_close(conn["ws"], 1001, "Heartbeat timeout")
                    to_remove.append(uid)

            for uid in to_remove:
                connections.pop(uid, None)  # 安全移除
                # 清理用户从所有临时房间的成员列表中移除
                # 注意：不再因为心跳超时自动解散房间，只移除成员
                for group_id, room_info in list(active_temp_rooms.items()):
                    members = room_info.get("members", [])
                    if uid in members:
                        members.remove(uid)
                        print(f"[webrtc] 心跳超时：用户 {uid} 离开房间 {group_id}，当前成员数={len(members)}")

            await asyncio.sleep(10)
    except asyncio.CancelledError:
        print("heartbeat_checker 已安全退出")


def load_active_temp_rooms_from_db() -> None:
    """
    应用启动时，从数据库加载仍然处于活跃状态的临时语音房间到内存 active_temp_rooms。
    防止程序重启后丢失房间信息。
    """
    try:
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[temp_room][startup] 数据库连接失败，无法从数据库加载临时语音房间")
            app_logger.error("[temp_room][startup] 数据库连接失败，无法从数据库加载临时语音房间")
            return

        cursor = connection.cursor(dictionary=True)

        query_rooms = """
            SELECT room_id, group_id, owner_id, owner_name, owner_icon,
                   whip_url, whep_url, stream_name, status, create_time
            FROM temp_voice_rooms
            WHERE status = 1
        """
        cursor.execute(query_rooms)
        rooms = cursor.fetchall() or []

        if not rooms:
            print("[temp_room][startup] 数据库中没有状态为活跃的临时语音房间")
            app_logger.info("[temp_room][startup] 数据库中没有状态为活跃的临时语音房间")
            return

        loaded_count = 0
        for room in rooms:
            group_id = room.get("group_id")
            room_id = room.get("room_id")
            stream_name = room.get("stream_name")
            if not group_id or not room_id or not stream_name:
                continue

            publish_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/publish/?app={SRS_APP}&stream={stream_name}"
            play_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/play/?app={SRS_APP}&stream={stream_name}"

            members_query = """
                SELECT user_id, user_name, status
                FROM temp_voice_room_members
                WHERE room_id = %s AND status = 1
            """
            cursor.execute(members_query, (room_id,))
            member_rows = cursor.fetchall() or []
            members = [m.get("user_id") for m in member_rows if m.get("user_id")]

            active_temp_rooms[group_id] = {
                "room_id": room_id,
                "publish_url": publish_url,
                "play_url": play_url,
                "whip_url": room.get("whip_url"),
                "whep_url": room.get("whep_url"),
                "stream_name": stream_name,
                "owner_id": room.get("owner_id"),
                "owner_name": room.get("owner_name"),
                "owner_icon": room.get("owner_icon"),
                "group_id": group_id,
                "timestamp": time.time(),
                "members": members,
            }
            loaded_count += 1

        print(f"[temp_room][startup] 已从数据库加载 {loaded_count} 个临时语音房间到内存")
        app_logger.info(f"[temp_room][startup] 已从数据库加载 {loaded_count} 个临时语音房间到内存")

    except Exception as e:
        print(f"[temp_room][startup] 从数据库加载临时语音房间失败: {e}")
        app_logger.error(f"[temp_room][startup] 从数据库加载临时语音房间失败: {e}", exc_info=True)
    finally:
        try:
            if "cursor" in locals() and cursor:
                cursor.close()
            if "connection" in locals() and connection and connection.is_connected():
                connection.close()
        except Exception:
            pass



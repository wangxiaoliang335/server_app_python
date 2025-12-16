import datetime
import json
from typing import Any, Dict, List, Optional, Union

from common import app_logger
from ws.manager import connections


def convert_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")


def convert_group_type_to_int(group_type: Union[str, int, None]) -> int:
    """
    将群类型字符串转换为整数
    腾讯IM群类型：Public=0, Private=1, ChatRoom=2, AVChatRoom=3, BChatRoom=4, Community=5, Work=6, Meeting=7
    """
    if group_type is None:
        return 0
    if isinstance(group_type, int):
        return group_type
    if isinstance(group_type, str):
        type_mapping = {
            "public": 0,
            "Public": 0,
            "PUBLIC": 0,
            "private": 1,
            "Private": 1,
            "PRIVATE": 1,
            "chatroom": 2,
            "ChatRoom": 2,
            "CHATROOM": 2,
            "avchatroom": 3,
            "AVChatRoom": 3,
            "AVCHATROOM": 3,
            "bchatroom": 4,
            "BChatRoom": 4,
            "BCHATROOM": 4,
            "community": 5,
            "Community": 5,
            "COMMUNITY": 5,
            "work": 6,
            "Work": 6,
            "WORK": 6,
            "class": 6,
            "Class": 6,
            "meeting": 2,
            "Meeting": 2,
            "MEETING": 2,
            "meetinggroup": 2,
            "MeetingGroup": 2,
            "MEETINGGROUP": 2,
            "会议": 2,
            "会议群": 2,
            "avmeeting": 3,
            "AVMeeting": 3,
            "AVMEETING": 3,
        }
        return type_mapping.get(group_type, 0)
    return 0


async def notify_temp_room_closed(group_id: str, room_info: Dict[str, Any], reason: str, initiator: str):
    """通知房间成员房间已解散，提醒客户端停止推拉流。"""
    if not room_info:
        return

    members_snapshot = list(room_info.get("members", []))
    if not members_snapshot:
        return

    notification = {
        "type": "temp_room_closed",
        "status": "closed",
        "action": "stop_stream",
        "group_id": group_id,
        "room_id": room_info.get("room_id"),
        "stream_name": room_info.get("stream_name"),
        "owner_id": room_info.get("owner_id"),
        "reason": reason,
        "initiator": initiator,
        "message": "临时房间已解散，请立即停止推流/拉流",
    }
    notification_json = json.dumps(notification, ensure_ascii=False)

    for member_id in members_snapshot:
        target_conn = connections.get(member_id)
        if not target_conn:
            continue
        try:
            await target_conn["ws"].send_text(notification_json)
            app_logger.info(
                f"[temp_room] 已通知成员停止推拉流 - group_id={group_id}, member_id={member_id}, reason={reason}"
            )
        except Exception as notify_error:
            app_logger.warning(
                f"[temp_room] 通知成员停止推拉流失败 - group_id={group_id}, member_id={member_id}, error={notify_error}"
            )



import json
from typing import Any, Dict

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from common import app_logger
from db import get_db_connection
from realtime.srs import SRS_APP, SRS_WEBRTC_API_URL
from ws.manager import active_temp_rooms

router = APIRouter()


@router.post("/temp_rooms/query")
async def query_temp_rooms(request: Request):
    """
    根据班级群ID列表查询对应的临时语音房间信息。
    """
    client_ip = request.client.host if request.client else "unknown"
    print(f"[temp_rooms/query] ========== 收到查询请求 ==========")
    print(f"[temp_rooms/query] 客户端IP: {client_ip}")
    app_logger.info(f"[temp_rooms/query] 收到查询请求，客户端IP: {client_ip}")

    try:
        body = await request.json()
        print(f"[temp_rooms/query] 请求体内容: {json.dumps(body, ensure_ascii=False)}")
        app_logger.info(f"[temp_rooms/query] 请求体内容: {json.dumps(body, ensure_ascii=False)}")
    except Exception as e:
        error_msg = f"请求体必须为 JSON，解析失败: {e}"
        print(f"[temp_rooms/query] ❌ {error_msg}")
        app_logger.error(f"[temp_rooms/query] ❌ {error_msg}")
        return JSONResponse({"data": {"message": "请求体必须为 JSON", "code": 400}}, status_code=400)

    group_ids = body.get("group_ids") or body.get("groupIds") or []
    print(f"[temp_rooms/query] 原始 group_ids: {group_ids} (type: {type(group_ids).__name__})")
    app_logger.info(f"[temp_rooms/query] 原始 group_ids: {group_ids}")

    if not isinstance(group_ids, list) or not group_ids:
        error_msg = f"group_ids 必须为非空数组，当前值: {group_ids} (type: {type(group_ids).__name__})"
        print(f"[temp_rooms/query] ❌ {error_msg}")
        app_logger.warning(f"[temp_rooms/query] ❌ {error_msg}")
        return JSONResponse({"data": {"message": "group_ids 必须为非空数组", "code": 400}}, status_code=400)

    group_ids = list({str(gid).strip() for gid in group_ids if str(gid).strip()})
    print(f"[temp_rooms/query] 清理后的 group_ids: {group_ids} (数量: {len(group_ids)})")
    app_logger.info(f"[temp_rooms/query] 清理后的 group_ids: {group_ids} (数量: {len(group_ids)})")

    if not group_ids:
        error_msg = "group_ids 不能为空（清理后）"
        print(f"[temp_rooms/query] ❌ {error_msg}")
        app_logger.warning(f"[temp_rooms/query] ❌ {error_msg}")
        return JSONResponse({"data": {"message": "group_ids 不能为空", "code": 400}}, status_code=400)

    results = []
    memory_results_count = 0

    print(f"[temp_rooms/query] 开始从内存 active_temp_rooms 查询，当前内存中的房间数: {len(active_temp_rooms)}")
    print(f"[temp_rooms/query] 内存中的房间 group_ids: {list(active_temp_rooms.keys())}")
    app_logger.info(
        f"[temp_rooms/query] 开始从内存查询，内存房间数: {len(active_temp_rooms)}, 查询的 group_ids: {group_ids}"
    )

    for gid in group_ids:
        room = active_temp_rooms.get(gid)
        if room:
            memory_results_count += 1
            room_data = {
                "group_id": gid,
                "room_id": room.get("room_id"),
                "publish_url": room.get("publish_url"),
                "play_url": room.get("play_url"),
                "stream_name": room.get("stream_name"),
                "owner_id": room.get("owner_id"),
                "owner_name": room.get("owner_name"),
                "owner_icon": room.get("owner_icon"),
                "members": room.get("members", []),
            }
            results.append(room_data)
            print(
                f"[temp_rooms/query] ✅ 从内存找到房间: group_id={gid}, room_id={room.get('room_id')}, members={len(room.get('members', []))}"
            )
            app_logger.info(
                f"[temp_rooms/query] ✅ 从内存找到房间: group_id={gid}, room_id={room.get('room_id')}, members={len(room.get('members', []))}"
            )
        else:
            print(f"[temp_rooms/query] ⚠️ 内存中未找到房间: group_id={gid}")
            app_logger.debug(f"[temp_rooms/query] ⚠️ 内存中未找到房间: group_id={gid}")

    print(f"[temp_rooms/query] 内存查询完成，找到 {memory_results_count} 个房间")

    missing = [gid for gid in group_ids if gid not in active_temp_rooms]
    print(f"[temp_rooms/query] 需要从数据库查询的 group_ids: {missing} (数量: {len(missing)})")
    app_logger.info(f"[temp_rooms/query] 需要从数据库查询的 group_ids: {missing} (数量: {len(missing)})")

    if missing:
        connection = get_db_connection()
        if connection and connection.is_connected():
            print(f"[temp_rooms/query] ✅ 数据库连接成功，开始查询")
            app_logger.info(f"[temp_rooms/query] ✅ 数据库连接成功，开始查询")
            try:
                cursor = connection.cursor(dictionary=True)
                query = """
                    SELECT room_id, group_id, owner_id, owner_name, owner_icon,
                           whip_url, whep_url, stream_name, status, create_time
                    FROM temp_voice_rooms
                    WHERE status = 1 AND group_id IN ({})
                """.format(", ".join(["%s"] * len(missing)))
                print(f"[temp_rooms/query] 执行SQL查询: {query[:200]}... (参数: {missing})")
                app_logger.info(f"[temp_rooms/query] 执行SQL查询，参数: {missing}")
                cursor.execute(query, missing)
                rows = cursor.fetchall() or []
                print(f"[temp_rooms/query] 数据库查询结果: 找到 {len(rows)} 条记录")
                app_logger.info(f"[temp_rooms/query] 数据库查询结果: 找到 {len(rows)} 条记录")

                room_ids = [r.get("room_id") for r in rows if r.get("room_id")]
                print(f"[temp_rooms/query] 需要查询成员的 room_ids: {room_ids} (数量: {len(room_ids)})")
                app_logger.info(f"[temp_rooms/query] 需要查询成员的 room_ids: {room_ids} (数量: {len(room_ids)})")

                members_map: Dict[str, list] = {}
                if room_ids:
                    member_query = """
                        SELECT room_id, user_id
                        FROM temp_voice_room_members
                        WHERE status = 1 AND room_id IN ({})
                    """.format(", ".join(["%s"] * len(room_ids)))
                    print(f"[temp_rooms/query] 执行成员查询SQL: {member_query[:200]}... (参数: {room_ids})")
                    app_logger.info(f"[temp_rooms/query] 执行成员查询SQL，参数: {room_ids}")
                    cursor.execute(member_query, room_ids)
                    member_rows = cursor.fetchall() or []
                    print(f"[temp_rooms/query] 成员查询结果: 找到 {len(member_rows)} 条成员记录")
                    app_logger.info(f"[temp_rooms/query] 成员查询结果: 找到 {len(member_rows)} 条成员记录")

                    for m in member_rows:
                        rid = m.get("room_id")
                        uid = m.get("user_id")
                        if rid and uid:
                            members_map.setdefault(rid, []).append(uid)
                    print(f"[temp_rooms/query] 成员映射结果: {json.dumps(members_map, ensure_ascii=False)}")
                    app_logger.info(f"[temp_rooms/query] 成员映射结果: {json.dumps(members_map, ensure_ascii=False)}")

                db_results_count = 0
                for r in rows:
                    gid = r.get("group_id")
                    stream = r.get("stream_name")
                    publish_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/publish/?app={SRS_APP}&stream={stream}"
                    play_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/play/?app={SRS_APP}&stream={stream}"
                    rid = r.get("room_id")
                    room_data = {
                        "group_id": gid,
                        "room_id": rid,
                        "publish_url": publish_url,
                        "play_url": play_url,
                        "stream_name": stream,
                        "owner_id": r.get("owner_id"),
                        "owner_name": r.get("owner_name"),
                        "owner_icon": r.get("owner_icon"),
                        "members": members_map.get(rid, []),
                    }
                    results.append(room_data)
                    db_results_count += 1
                    print(
                        f"[temp_rooms/query] ✅ 从数据库找到房间: group_id={gid}, room_id={rid}, members={len(members_map.get(rid, []))}"
                    )
                    app_logger.info(
                        f"[temp_rooms/query] ✅ 从数据库找到房间: group_id={gid}, room_id={rid}, members={len(members_map.get(rid, []))}"
                    )

                print(f"[temp_rooms/query] 数据库查询完成，找到 {db_results_count} 个房间")
            except Exception as db_error:
                error_msg = f"数据库查询失败: {db_error}"
                print(f"[temp_rooms/query] ❌ {error_msg}")
                app_logger.error(f"[temp_rooms/query] ❌ {error_msg}", exc_info=True)
            finally:
                try:
                    if "cursor" in locals() and cursor:
                        cursor.close()
                        print(f"[temp_rooms/query] ✅ 数据库游标已关闭")
                    if connection and connection.is_connected():
                        connection.close()
                        print(f"[temp_rooms/query] ✅ 数据库连接已关闭")
                except Exception as close_error:
                    print(f"[temp_rooms/query] ⚠️ 关闭数据库资源时出错: {close_error}")
                    app_logger.warning(f"[temp_rooms/query] ⚠️ 关闭数据库资源时出错: {close_error}")
        else:
            error_msg = "数据库连接失败或未连接"
            print(f"[temp_rooms/query] ❌ {error_msg}")
            app_logger.error(f"[temp_rooms/query] ❌ {error_msg}")

    response_data = {"data": {"rooms": results, "count": len(results)}, "code": 200}
    print(f"[temp_rooms/query] ========== 查询完成 ==========")
    print(f"[temp_rooms/query] 最终结果: 找到 {len(results)} 个房间")
    print(f"[temp_rooms/query] 响应数据: {json.dumps(response_data, ensure_ascii=False, indent=2)}")
    app_logger.info(f"[temp_rooms/query] 查询完成，找到 {len(results)} 个房间")
    app_logger.debug(f"[temp_rooms/query] 响应数据: {json.dumps(response_data, ensure_ascii=False)}")

    return JSONResponse(response_data)



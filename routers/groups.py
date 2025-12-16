import asyncio
import base64
import datetime
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

import mysql.connector
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
from starlette.requests import ClientDisconnect

from common import app_logger
from db import get_db_connection
from realtime.srs import SRS_APP, SRS_WEBRTC_API_URL
from services.avatars import resolve_local_avatar_file_path
from services.tencent_api import build_tencent_request_url, resolve_tencent_identifier
from services.tencent_groups import notify_tencent_group_sync
from services.tencent_sig import generate_tencent_user_sig
from ws.manager import active_temp_rooms


router = APIRouter()

# ===== group_members 扩展字段：教师任课科目（同一班级可多科） =====
GROUP_MEMBERS_TEACH_SUBJECTS_COL = "teach_subjects"


def _group_members_has_column(cursor, col_name: str) -> bool:
    """检查 group_members 是否存在指定列。"""
    cursor.execute("SHOW COLUMNS FROM `group_members` LIKE %s", (col_name,))
    return cursor.fetchone() is not None


def _ensure_group_members_teach_subjects_column(cursor) -> bool:
    """
    尝试为 group_members 增加 teach_subjects(JSON) 字段。
    - 成功/已存在：返回 True
    - 失败（权限不足等）：返回 False（上层可提示手工执行 ALTER TABLE）
    """
    try:
        if _group_members_has_column(cursor, GROUP_MEMBERS_TEACH_SUBJECTS_COL):
            return True
        cursor.execute(
            """
            ALTER TABLE `group_members`
            ADD COLUMN `teach_subjects` JSON NULL
            COMMENT '教师在该班级教授的科目列表（JSON数组，可多科）'
            AFTER `is_voice_enabled`
            """
        )
        return True
    except Exception as e:
        # 兼容：有些线上账号可能没有 ALTER 权限
        app_logger.warning(f"[schema] ensure group_members.teach_subjects failed: {e}")
        return False


def _normalize_teach_subjects(value: Any) -> List[str]:
    """
    将数据库返回的 teach_subjects 规范化为 list[str]。
    MySQL JSON 列在 mysql-connector 下通常会以 str 返回。
    """
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value if x is not None and str(x).strip() != ""]
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except Exception:
            # 兜底：如果数据库里是普通字符串，返回单元素列表
            return [raw]
        if isinstance(parsed, list):
            return [str(x) for x in parsed if x is not None and str(x).strip() != ""]
        if parsed is None:
            return []
        return [str(parsed)]
    return [str(value)]


# Tencent 配置（从环境变量读取；与 app.py 保持一致）
TENCENT_API_SDK_APP_ID = os.getenv("TENCENT_API_SDK_APP_ID")
TENCENT_API_IDENTIFIER = os.getenv("TENCENT_API_IDENTIFIER")
TENCENT_API_SECRET_KEY = os.getenv("TENCENT_API_SECRET_KEY")
TENCENT_API_USER_SIG = os.getenv("TENCENT_API_USER_SIG")
TENCENT_API_TIMEOUT = float(os.getenv("TENCENT_API_TIMEOUT", "10"))

# 头像/文件目录
IMAGE_DIR = os.getenv("IMAGE_DIR", "/var/www/images")

#
# 群组管理相关接口（实现已从 app.py 迁入本文件）
#


@router.post("/groups/remove-member")
async def remove_member(request: Request):
    """
    群主踢出群成员
    接收客户端发送的 group_id 和 members 数组
    1. 调用腾讯接口踢出成员
    2. 成功后，从数据库删除成员并更新群组成员数量
    请求体 JSON:
    {
      "group_id": "群组ID",
      "members": ["成员ID1", "成员ID2", ...]
    }
    """
    print("=" * 80)
    print("[groups/remove-member] 收到踢出成员请求")

    try:
        data = await request.json()
        print(f"[groups/remove-member] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")

        group_id = data.get("group_id")
        members = data.get("members", [])

        # 参数验证
        if not group_id:
            print("[groups/remove-member] 错误: 缺少 group_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 group_id"}, status_code=400)

        if not members or not isinstance(members, list):
            print("[groups/remove-member] 错误: 缺少或无效的 members")
            return JSONResponse({"code": 400, "message": "缺少必需参数 members 或 members 必须是数组"}, status_code=400)

        if len(members) == 0:
            print("[groups/remove-member] 错误: members 数组为空")
            return JSONResponse({"code": 400, "message": "members 数组不能为空"}, status_code=400)

        print("[groups/remove-member] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/remove-member] 错误: 数据库连接失败")
            app_logger.error("[groups/remove-member] 数据库连接失败")
            return JSONResponse({"code": 500, "message": "数据库连接失败"}, status_code=500)
        print("[groups/remove-member] 数据库连接成功")

        cursor = None
        try:
            # 开始事务（在开始时就启动，确保所有操作在一个事务中）
            connection.start_transaction()
            cursor = connection.cursor(dictionary=True)

            # 1. 检查群组是否存在
            print(f"[groups/remove-member] 检查群组 {group_id} 是否存在...")
            cursor.execute("SELECT group_id, group_name, member_num FROM `groups` WHERE group_id = %s", (group_id,))
            group_info = cursor.fetchone()

            if not group_info:
                print(f"[groups/remove-member] 错误: 群组 {group_id} 不存在")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({"code": 404, "message": "群组不存在"}, status_code=404)

            print(f"[groups/remove-member] 群组信息: {group_info}")

            # 2. 检查要删除的成员是否在群组中，并过滤掉群主
            print(f"[groups/remove-member] 检查成员是否在群组中...")
            valid_members = []
            owner_members = []

            for member_id in members:
                cursor.execute(
                    "SELECT group_id, user_id, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                    (group_id, member_id),
                )
                member_info = cursor.fetchone()

                if not member_info:
                    print(f"[groups/remove-member] 警告: 成员 {member_id} 不在群组中，跳过")
                    continue

                self_role = member_info.get("self_role", 200)
                if self_role == 400:  # 群主不能被踢出
                    print(f"[groups/remove-member] 警告: 成员 {member_id} 是群主，不允许被踢出")
                    owner_members.append(member_id)
                    continue

                valid_members.append(member_id)

            if owner_members:
                print(f"[groups/remove-member] 警告: 以下成员是群主，无法踢出: {owner_members}")

            if not valid_members:
                print(f"[groups/remove-member] 错误: 没有可踢出的成员")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({"code": 400, "message": "没有可踢出的成员（可能是群主或不在群组中）"}, status_code=400)

            print(f"[groups/remove-member] 准备踢出 {len(valid_members)} 个成员: {valid_members}")

            # 3. 调用腾讯接口踢出成员
            print(f"[groups/remove-member] 准备调用腾讯接口踢出 {len(valid_members)} 个成员...")

            # 使用管理员账号作为 identifier（与群组同步保持一致）
            identifier_to_use = TENCENT_API_IDENTIFIER

            # 检查必需的配置
            if not TENCENT_API_SDK_APP_ID:
                print("[groups/remove-member] 错误: TENCENT_API_SDK_APP_ID 未配置")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({"code": 500, "message": "腾讯接口配置错误: 缺少 SDKAppID"}, status_code=500)

            if not identifier_to_use:
                print("[groups/remove-member] 错误: TENCENT_API_IDENTIFIER 未配置")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({"code": 500, "message": "腾讯接口配置错误: 缺少 Identifier"}, status_code=500)

            # 尝试生成或使用配置的 UserSig（与群组同步逻辑一致）
            usersig_to_use: Optional[str] = None
            sig_error: Optional[str] = None
            if TENCENT_API_SECRET_KEY:
                try:
                    # 为管理员账号生成 UserSig
                    print(f"[groups/remove-member] 准备为管理员账号生成 UserSig: identifier={identifier_to_use}")
                    usersig_to_use = generate_tencent_user_sig(identifier_to_use)
                    print(f"[groups/remove-member] UserSig 生成成功，长度: {len(usersig_to_use) if usersig_to_use else 0}")
                    app_logger.info(f"为管理员账号 {identifier_to_use} 生成 UserSig 成功")
                except Exception as e:
                    sig_error = f"自动生成管理员 UserSig 失败: {e}"
                    print(f"[groups/remove-member] UserSig 生成失败: {sig_error}")
                    app_logger.error(sig_error)

            if not usersig_to_use:
                print(f"[groups/remove-member] 使用配置的 TENCENT_API_USER_SIG")
                usersig_to_use = TENCENT_API_USER_SIG

            if not usersig_to_use:
                error_message = "缺少可用的管理员 UserSig，无法调用腾讯接口。"
                print(f"[groups/remove-member] 错误: {error_message}")
                app_logger.error(f"[groups/remove-member] {error_message}")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({"code": 500, "message": error_message}, status_code=500)

            print(f"[groups/remove-member] 使用 identifier: {identifier_to_use}, SDKAppID: {TENCENT_API_SDK_APP_ID}")

            # 构建腾讯接口 URL
            delete_url = build_tencent_request_url(
                identifier=identifier_to_use, usersig=usersig_to_use, path_override="v4/group_open_http_svc/delete_group_member"
            )

            if not delete_url:
                print("[groups/remove-member] 错误: 无法构建腾讯接口 URL")
                app_logger.error("[groups/remove-member] 无法构建腾讯接口 URL")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({"code": 500, "message": "腾讯接口配置错误"}, status_code=500)

            # 验证 URL 中是否包含 sdkappid
            if "sdkappid" not in delete_url:
                print(f"[groups/remove-member] 警告: URL 中缺少 sdkappid，完整 URL: {delete_url}")
                app_logger.warning(f"[groups/remove-member] URL 中缺少 sdkappid: {delete_url}")
                # 手动添加 sdkappid（如果 URL 构建失败）
                parsed_url = urllib.parse.urlparse(delete_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                query_params["sdkappid"] = [TENCENT_API_SDK_APP_ID]
                query_params["identifier"] = [identifier_to_use]
                query_params["usersig"] = [usersig_to_use]
                query_params["contenttype"] = ["json"]
                if "random" not in query_params:
                    query_params["random"] = [str(random.randint(1, 2**31 - 1))]
                new_query = urllib.parse.urlencode(query_params, doseq=True)
                delete_url = urllib.parse.urlunparse(parsed_url._replace(query=new_query))
                print(f"[groups/remove-member] 已手动添加参数，新 URL: {delete_url[:200]}...")

            # 构建踢出成员的 payload
            delete_payload = {"GroupId": group_id, "MemberToDel_Account": valid_members, "Reason": "群主踢出"}

            print(f"[groups/remove-member] 腾讯接口 URL: {delete_url[:100]}...")
            print(f"[groups/remove-member] 踢出 payload: {json.dumps(delete_payload, ensure_ascii=False, indent=2)}")

            # 调用腾讯接口
            def _delete_tencent_members() -> Dict[str, Any]:
                """调用腾讯接口踢出成员"""
                headers = {"Content-Type": "application/json; charset=utf-8"}
                encoded_payload = json.dumps(delete_payload, ensure_ascii=False).encode("utf-8")
                request_obj = urllib.request.Request(url=delete_url, data=encoded_payload, headers=headers, method="POST")
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
                    app_logger.error(f"[groups/remove-member] 腾讯接口调用失败 (HTTP {e.code}): {body}")
                    return {"status": "error", "http_status": e.code, "error": body}
                except urllib.error.URLError as e:
                    app_logger.error(f"[groups/remove-member] 腾讯接口调用异常: {e}")
                    return {"status": "error", "http_status": None, "error": str(e)}
                except Exception as exc:
                    app_logger.exception(f"[groups/remove-member] 腾讯接口未知异常: {exc}")
                    return {"status": "error", "http_status": None, "error": str(exc)}

            tencent_result = await asyncio.to_thread(_delete_tencent_members)

            # 打印腾讯接口响应详情
            print(f"[groups/remove-member] 腾讯接口响应状态: {tencent_result.get('status')}")
            print(f"[groups/remove-member] 腾讯接口HTTP状态码: {tencent_result.get('http_status')}")
            tencent_response = tencent_result.get("response", {})
            print(
                f"[groups/remove-member] 腾讯接口响应内容: {json.dumps(tencent_response, ensure_ascii=False, indent=2) if isinstance(tencent_response, dict) else tencent_response}"
            )

            # 检查腾讯接口调用结果
            if tencent_result.get("status") != "success":
                error_msg = tencent_result.get("error", "腾讯接口调用失败")
                print(f"[groups/remove-member] 腾讯接口调用失败: {error_msg}")
                app_logger.error(f"[groups/remove-member] 腾讯接口调用失败: group_id={group_id}, error={error_msg}")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({"code": 500, "message": f"踢出成员失败: {error_msg}"}, status_code=500)

            if isinstance(tencent_response, dict):
                action_status = tencent_response.get("ActionStatus")
                error_code = tencent_response.get("ErrorCode")
                error_info = tencent_response.get("ErrorInfo")

                print(
                    f"[groups/remove-member] 腾讯接口响应解析: ActionStatus={action_status}, ErrorCode={error_code}, ErrorInfo={error_info}"
                )

                if action_status != "OK" or error_code != 0:
                    print(f"[groups/remove-member] 腾讯接口返回错误: ErrorCode={error_code}, ErrorInfo={error_info}")
                    app_logger.error(
                        f"[groups/remove-member] 腾讯接口返回错误: group_id={group_id}, ErrorCode={error_code}, ErrorInfo={error_info}"
                    )
                    if connection and connection.is_connected():
                        connection.rollback()
                    return JSONResponse({"code": 500, "message": f"踢出成员失败: {error_info or '未知错误'}"}, status_code=500)
            else:
                print(f"[groups/remove-member] 警告: 腾讯接口响应不是JSON格式: {type(tencent_response)}")
                app_logger.warning(
                    f"[groups/remove-member] 腾讯接口响应格式异常: group_id={group_id}, response_type={type(tencent_response)}"
                )

            print(f"[groups/remove-member] 腾讯接口调用成功，准备更新数据库")
            app_logger.info(f"[groups/remove-member] 腾讯接口调用成功: group_id={group_id}, members={valid_members}")

            # 4. 踢出成功后，从数据库删除成员
            print(f"[groups/remove-member] 开始从数据库删除成员...")

            deleted_count = 0
            for member_id in valid_members:
                cursor.execute("DELETE FROM `group_members` WHERE group_id = %s AND user_id = %s", (group_id, member_id))
                if cursor.rowcount > 0:
                    deleted_count += 1
                    print(f"[groups/remove-member] 成功删除成员: {member_id}")

            if deleted_count == 0:
                print(f"[groups/remove-member] 警告: 数据库删除操作未影响任何行")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({"code": 500, "message": "删除成员失败"}, status_code=500)

            # 5. 更新群组的成员数量（确保不会小于0）
            print(f"[groups/remove-member] 更新群组 {group_id} 的成员数量...")
            cursor.execute(
                "UPDATE `groups` SET member_num = CASE WHEN member_num >= %s THEN member_num - %s ELSE 0 END WHERE group_id = %s",
                (deleted_count, deleted_count, group_id),
            )
            print(f"[groups/remove-member] 群组成员数量已更新，减少 {deleted_count} 人")

            # 提交事务
            connection.commit()
            print(f"[groups/remove-member] 事务提交成功")

            result = {
                "code": 200,
                "message": "成功踢出成员",
                "data": {
                    "group_id": group_id,
                    "deleted_count": deleted_count,
                    "total_requested": len(members),
                    "owner_members": owner_members if owner_members else None,
                },
            }

            print(f"[groups/remove-member] 返回结果: {result}")
            print("=" * 80)

            return JSONResponse(result, status_code=200)

        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/remove-member] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/remove-member] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/remove-member] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"数据库操作失败: {str(e)}"}, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"移除成员时发生异常: {e}"
            print(f"[groups/remove-member] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/remove-member] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/remove-member] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"操作失败: {str(e)}"}, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/remove-member] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/remove-member] 数据库连接已关闭")
                app_logger.info("[groups/remove-member] Database connection closed after remove member attempt.")

    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/remove-member] {error_msg}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/remove-member] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/remove-member] {error_msg}\n{traceback_str}")
        return JSONResponse({"code": 400, "message": "请求数据格式错误"}, status_code=400)
    finally:
        print("=" * 80)


@router.post("/groups/dismiss")
async def dismiss_group(request: Request):
    """
    解散群组
    接收客户端发送的 group_id, user_id
    只有群主才能解散群组
    删除群组的所有成员和群组本身
    """
    print("=" * 80)
    print("[groups/dismiss] 收到解散群组请求")

    # 打印请求头信息用于调试
    content_type = request.headers.get("content-type", "")
    content_length = request.headers.get("content-length", "")
    print(f"[groups/dismiss] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")

    try:
        # 解析请求体JSON数据
        try:
            # 先尝试读取原始body
            body_bytes = await request.body()
            print(f"[groups/dismiss] 读取到请求体长度: {len(body_bytes)} 字节")

            if not body_bytes:
                print("[groups/dismiss] 错误: 请求体为空")
                return JSONResponse({"code": 400, "message": "请求体不能为空"}, status_code=400)

            # 解析JSON
            try:
                data = json.loads(body_bytes.decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"[groups/dismiss] 错误: JSON解析失败 - {e}")
                print(f"[groups/dismiss] 请求体内容: {body_bytes.decode('utf-8', errors='ignore')}")
                return JSONResponse({"code": 400, "message": "请求数据格式错误，无法解析JSON"}, status_code=400)

        except ClientDisconnect:
            print("[groups/dismiss] 错误: 客户端断开连接")
            print(f"[groups/dismiss] 调试信息 - Content-Type: {content_type}, Content-Length: {content_length}")
            app_logger.warning("[groups/dismiss] 客户端在请求完成前断开连接")
            return JSONResponse({"code": 400, "message": "客户端断开连接，请检查请求数据是否正确发送"}, status_code=400)
        except Exception as e:
            print(f"[groups/dismiss] 读取请求体时发生异常: {type(e).__name__} - {e}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/dismiss] 错误堆栈: {traceback_str}")
            return JSONResponse({"code": 400, "message": f"读取请求数据失败: {str(e)}"}, status_code=400)

        print(f"[groups/dismiss] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")

        group_id = data.get("group_id")
        user_id = data.get("user_id")

        print(f"[groups/dismiss] 解析结果 - group_id: {group_id}, user_id: {user_id}")

        # 参数验证
        if not group_id:
            print("[groups/dismiss] 错误: 缺少 group_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 group_id"}, status_code=400)

        if not user_id:
            print("[groups/dismiss] 错误: 缺少 user_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 user_id"}, status_code=400)

        print("[groups/dismiss] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/dismiss] 错误: 数据库连接失败")
            app_logger.error("[groups/dismiss] 数据库连接失败")
            return JSONResponse({"code": 500, "message": "数据库连接失败"}, status_code=500)
        print("[groups/dismiss] 数据库连接成功")

        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)

            # 1. 检查群组是否存在
            print(f"[groups/dismiss] 检查群组 {group_id} 是否存在...")
            cursor.execute("SELECT group_id, group_name, member_num FROM `groups` WHERE group_id = %s", (group_id,))
            group_info = cursor.fetchone()

            if not group_info:
                print(f"[groups/dismiss] 错误: 群组 {group_id} 不存在")
                return JSONResponse({"code": 404, "message": "群组不存在"}, status_code=404)

            print(f"[groups/dismiss] 群组信息: {group_info}")
            group_name = group_info.get("group_name", "")

            # 2. 检查用户是否在群组中，并且是否是群主
            print(f"[groups/dismiss] 检查用户 {user_id} 是否是群组 {group_id} 的群主...")
            cursor.execute(
                "SELECT group_id, user_id, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id),
            )
            member_info = cursor.fetchone()

            if not member_info:
                print(f"[groups/dismiss] 错误: 用户 {user_id} 不在群组 {group_id} 中")
                return JSONResponse({"code": 403, "message": "您不是该群组的成员"}, status_code=403)

            print(f"[groups/dismiss] 成员信息: {member_info}")
            self_role = member_info.get("self_role", 200)

            # 3. 检查是否是群主（self_role = 400 表示群主）
            if self_role != 400:
                print(f"[groups/dismiss] 错误: 用户 {user_id} 不是群主，无权解散群组")
                return JSONResponse({"code": 403, "message": "只有群主才能解散群组"}, status_code=403)

            print(f"[groups/dismiss] 验证通过: 用户 {user_id} 是群主，可以解散群组")

            # 4. 先调用腾讯IM API销毁群组
            tencent_sync_success = False
            tencent_error = None

            # 准备腾讯IM配置
            identifier_to_use = TENCENT_API_IDENTIFIER
            if not identifier_to_use:
                # 尝试从用户信息中获取identifier
                cursor.execute("SELECT id_number, phone FROM `users` WHERE user_id = %s", (user_id,))
                user_info = cursor.fetchone()
                if user_info:
                    identifier_to_use = resolve_tencent_identifier(
                        connection, id_number=user_info.get("id_number"), phone=user_info.get("phone")
                    )

            if identifier_to_use and TENCENT_API_SDK_APP_ID:
                try:
                    # 生成或使用配置的 UserSig
                    usersig_to_use: Optional[str] = None
                    if TENCENT_API_SECRET_KEY:
                        try:
                            usersig_to_use = generate_tencent_user_sig(identifier_to_use)
                            print(f"[groups/dismiss] UserSig 生成成功")
                        except Exception as e:
                            print(f"[groups/dismiss] UserSig 生成失败: {e}")
                            usersig_to_use = TENCENT_API_USER_SIG
                    else:
                        usersig_to_use = TENCENT_API_USER_SIG

                    if usersig_to_use:
                        # 构建腾讯IM销毁群组的URL
                        destroy_url = build_tencent_request_url(
                            identifier=identifier_to_use,
                            usersig=usersig_to_use,
                            path_override="v4/group_open_http_svc/destroy_group",
                        )

                        if destroy_url:
                            # 构建销毁群组的payload
                            destroy_payload = {"GroupId": group_id}

                            print(f"[groups/dismiss] 准备同步到腾讯IM - group_id={group_id}")
                            app_logger.info(f"[groups/dismiss] 准备同步到腾讯IM - group_id={group_id}")

                            # 调用腾讯IM API
                            def _destroy_tencent_group() -> Dict[str, Any]:
                                """调用腾讯IM API销毁群组"""
                                headers = {"Content-Type": "application/json; charset=utf-8"}
                                encoded_payload = json.dumps(destroy_payload, ensure_ascii=False).encode("utf-8")
                                request_obj = urllib.request.Request(
                                    url=destroy_url, data=encoded_payload, headers=headers, method="POST"
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
                                    raw_body = e.read() if e.fp else b""
                                    text_body = raw_body.decode("utf-8", errors="replace")
                                    try:
                                        parsed_body = json.loads(text_body)
                                    except json.JSONDecodeError:
                                        parsed_body = None

                                    return {
                                        "status": "error",
                                        "http_status": e.code,
                                        "response": parsed_body or text_body,
                                        "error": f"HTTP {e.code}: {e.reason}",
                                    }
                                except Exception as e:
                                    return {"status": "error", "http_status": None, "error": str(e)}

                            tencent_result = _destroy_tencent_group()

                            if tencent_result.get("status") == "success":
                                response_data = tencent_result.get("response")
                                if isinstance(response_data, dict):
                                    action_status = response_data.get("ActionStatus")
                                    error_code = response_data.get("ErrorCode")
                                    if action_status == "OK" and error_code == 0:
                                        tencent_sync_success = True
                                        print(f"[groups/dismiss] 腾讯IM同步成功 - group_id={group_id}")
                                        app_logger.info(f"[groups/dismiss] 腾讯IM同步成功 - group_id={group_id}")
                                    else:
                                        tencent_error = (
                                            f"腾讯IM返回错误: ErrorCode={error_code}, ErrorInfo={response_data.get('ErrorInfo')}"
                                        )
                                        print(f"[groups/dismiss] {tencent_error}")
                                        app_logger.warning(f"[groups/dismiss] {tencent_error}")
                                else:
                                    tencent_error = f"腾讯IM返回格式错误: {response_data}"
                                    print(f"[groups/dismiss] {tencent_error}")
                                    app_logger.warning(f"[groups/dismiss] {tencent_error}")
                            else:
                                tencent_error = tencent_result.get("error", "未知错误")
                                print(f"[groups/dismiss] 腾讯IM API调用失败: {tencent_error}")
                                app_logger.warning(f"[groups/dismiss] 腾讯IM API调用失败: {tencent_error}")
                        else:
                            tencent_error = "无法构建腾讯IM URL"
                            print(f"[groups/dismiss] {tencent_error}")
                            app_logger.warning(f"[groups/dismiss] {tencent_error}")
                    else:
                        tencent_error = "缺少可用的UserSig"
                        print(f"[groups/dismiss] {tencent_error}")
                        app_logger.warning(f"[groups/dismiss] {tencent_error}")
                except Exception as e:
                    tencent_error = f"调用腾讯IM API时发生异常: {str(e)}"
                    print(f"[groups/dismiss] {tencent_error}")
                    app_logger.error(f"[groups/dismiss] {tencent_error}")
                    import traceback

                    traceback_str = traceback.format_exc()
                    app_logger.error(f"[groups/dismiss] 异常堆栈: {traceback_str}")
            else:
                tencent_error = "缺少腾讯IM配置（identifier或SDKAppID）"
                print(f"[groups/dismiss] {tencent_error}")
                app_logger.warning(f"[groups/dismiss] {tencent_error}")

            # 5. 删除群组的所有成员
            print(f"[groups/dismiss] 删除群组 {group_id} 的所有成员...")
            cursor.execute("DELETE FROM `group_members` WHERE group_id = %s", (group_id,))
            deleted_members = cursor.rowcount
            print(f"[groups/dismiss] 已删除 {deleted_members} 个成员")

            # 6. 删除群组本身
            print(f"[groups/dismiss] 删除群组 {group_id}...")
            cursor.execute("DELETE FROM `groups` WHERE group_id = %s", (group_id,))
            deleted_groups = cursor.rowcount
            print(f"[groups/dismiss] 删除群组完成, 影响行数: {deleted_groups}")

            if deleted_groups == 0:
                print(f"[groups/dismiss] 警告: 删除群组操作未影响任何行")
                connection.rollback()
                return JSONResponse({"code": 500, "message": "解散群组失败"}, status_code=500)

            # 提交事务
            connection.commit()
            print(f"[groups/dismiss] 事务提交成功")

            # 记录腾讯IM同步结果
            if not tencent_sync_success and tencent_error:
                app_logger.warning(f"[groups/dismiss] 数据库操作成功，但腾讯IM同步失败: {tencent_error}")

            result = {
                "code": 200,
                "message": "成功解散群组",
                "data": {
                    "group_id": group_id,
                    "group_name": group_name,
                    "user_id": user_id,
                    "deleted_members": deleted_members,
                    "tencent_sync": {"success": tencent_sync_success, "error": tencent_error if not tencent_sync_success else None},
                },
            }

            print(f"[groups/dismiss] 返回结果: {result}")
            print("=" * 80)

            return JSONResponse(result, status_code=200)

        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/dismiss] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/dismiss] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/dismiss] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"数据库操作失败: {str(e)}"}, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"解散群组时发生异常: {e}"
            print(f"[groups/dismiss] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/dismiss] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/dismiss] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"操作失败: {str(e)}"}, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/dismiss] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/dismiss] 数据库连接已关闭")
                app_logger.info("[groups/dismiss] Database connection closed after dismiss group attempt.")

    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/dismiss] {error_msg}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/dismiss] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/dismiss] {error_msg}\n{traceback_str}")
        return JSONResponse({"code": 400, "message": "请求数据格式错误"}, status_code=400)
    finally:
        print("=" * 80)


@router.post("/groups/set_admin_role")
async def set_admin_role(request: Request):
    """
    设置群成员角色（管理员或成员）
    接收客户端发送的 group_id, user_id, role
    更新 group_members 表中的 self_role 字段
    角色映射: 群主=400, 管理员=300, 成员=1
    role: "管理员" -> self_role = 300, "成员" -> self_role = 1
    """
    print("=" * 80)
    print("[groups/set_admin_role] 收到设置管理员角色请求")

    # 打印请求头信息用于调试
    content_type = request.headers.get("content-type", "")
    content_length = request.headers.get("content-length", "")
    print(f"[groups/set_admin_role] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")

    try:
        # 解析请求体JSON数据
        try:
            # 先尝试读取原始body
            body_bytes = await request.body()
            print(f"[groups/set_admin_role] 读取到请求体长度: {len(body_bytes)} 字节")

            if not body_bytes:
                print("[groups/set_admin_role] 错误: 请求体为空")
                return JSONResponse({"code": 400, "message": "请求体不能为空"}, status_code=400)

            # 解析JSON
            try:
                data = json.loads(body_bytes.decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"[groups/set_admin_role] 错误: JSON解析失败 - {e}")
                print(f"[groups/set_admin_role] 请求体内容: {body_bytes.decode('utf-8', errors='ignore')}")
                return JSONResponse({"code": 400, "message": "请求数据格式错误，无法解析JSON"}, status_code=400)

        except ClientDisconnect:
            print("[groups/set_admin_role] 错误: 客户端断开连接")
            print(f"[groups/set_admin_role] 调试信息 - Content-Type: {content_type}, Content-Length: {content_length}")
            app_logger.warning("[groups/set_admin_role] 客户端在请求完成前断开连接")
            return JSONResponse({"code": 400, "message": "客户端断开连接，请检查请求数据是否正确发送"}, status_code=400)
        except Exception as e:
            print(f"[groups/set_admin_role] 读取请求体时发生异常: {type(e).__name__} - {e}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/set_admin_role] 错误堆栈: {traceback_str}")
            return JSONResponse({"code": 400, "message": f"读取请求数据失败: {str(e)}"}, status_code=400)

        print(f"[groups/set_admin_role] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")

        group_id = data.get("group_id")
        user_id = data.get("user_id")
        role = data.get("role")

        print(f"[groups/set_admin_role] 解析结果 - group_id: {group_id}, user_id: {user_id}, role: {role}")

        # 参数验证
        if not group_id:
            print("[groups/set_admin_role] 错误: 缺少 group_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 group_id"}, status_code=400)

        if not user_id:
            print("[groups/set_admin_role] 错误: 缺少 user_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 user_id"}, status_code=400)

        if not role:
            print("[groups/set_admin_role] 错误: 缺少 role")
            return JSONResponse({"code": 400, "message": "缺少必需参数 role"}, status_code=400)

        # 将角色从中文映射到数据库值
        role_mapping = {"管理员": 300, "成员": 1}

        if role not in role_mapping:
            print(f"[groups/set_admin_role] 错误: 无效的角色值 {role}，只支持 '管理员' 或 '成员'")
            return JSONResponse({"code": 400, "message": "无效的角色值，只支持 '管理员' 或 '成员'"}, status_code=400)

        self_role = role_mapping[role]
        print(f"[groups/set_admin_role] 角色映射: {role} -> {self_role}")

        print("[groups/set_admin_role] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/set_admin_role] 错误: 数据库连接失败")
            app_logger.error("[groups/set_admin_role] 数据库连接失败")
            return JSONResponse({"code": 500, "message": "数据库连接失败"}, status_code=500)
        print("[groups/set_admin_role] 数据库连接成功")

        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)

            # 1. 检查群组是否存在
            print(f"[groups/set_admin_role] 检查群组 {group_id} 是否存在...")
            cursor.execute("SELECT group_id, group_name FROM `groups` WHERE group_id = %s", (group_id,))
            group_info = cursor.fetchone()

            if not group_info:
                print(f"[groups/set_admin_role] 错误: 群组 {group_id} 不存在")
                return JSONResponse({"code": 404, "message": "群组不存在"}, status_code=404)

            print(f"[groups/set_admin_role] 群组信息: {group_info}")

            # 2. 检查成员是否在群组中
            print(f"[groups/set_admin_role] 检查用户 {user_id} 是否在群组 {group_id} 中...")
            cursor.execute(
                "SELECT group_id, user_id, user_name, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id),
            )
            member_info = cursor.fetchone()

            if not member_info:
                print(f"[groups/set_admin_role] 错误: 用户 {user_id} 不在群组 {group_id} 中")
                return JSONResponse({"code": 404, "message": "该用户不是群组成员"}, status_code=404)

            print(f"[groups/set_admin_role] 成员信息: {member_info}")
            current_role = member_info.get("self_role", 200)
            user_name = member_info.get("user_name", "")

            # 3. 如果角色没有变化，直接返回成功
            if current_role == self_role:
                print(f"[groups/set_admin_role] 用户 {user_id} 的角色已经是 {role}，无需更新")
                return JSONResponse(
                    {
                        "code": 200,
                        "message": f"用户角色已经是{role}",
                        "data": {
                            "group_id": group_id,
                            "user_id": user_id,
                            "user_name": user_name,
                            "role": role,
                            "self_role": self_role,
                        },
                    },
                    status_code=200,
                )

            # 4. 更新成员角色
            print(f"[groups/set_admin_role] 更新用户 {user_id} 的角色从 {current_role} 到 {self_role}...")
            cursor.execute(
                "UPDATE `group_members` SET self_role = %s WHERE group_id = %s AND user_id = %s",
                (self_role, group_id, user_id),
            )
            affected_rows = cursor.rowcount
            print(f"[groups/set_admin_role] 更新角色完成, 影响行数: {affected_rows}")

            if affected_rows == 0:
                print(f"[groups/set_admin_role] 警告: 更新角色操作未影响任何行")
                connection.rollback()
                return JSONResponse({"code": 500, "message": "更新角色失败"}, status_code=500)

            # 提交事务
            connection.commit()
            print(f"[groups/set_admin_role] 事务提交成功")

            result = {
                "code": 200,
                "message": f"成功设置用户角色为{role}",
                "data": {"group_id": group_id, "user_id": user_id, "user_name": user_name, "role": role, "self_role": self_role},
            }

            print(f"[groups/set_admin_role] 返回结果: {result}")
            print("=" * 80)

            return JSONResponse(result, status_code=200)

        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/set_admin_role] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/set_admin_role] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/set_admin_role] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"数据库操作失败: {str(e)}"}, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"设置管理员角色时发生异常: {e}"
            print(f"[groups/set_admin_role] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/set_admin_role] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/set_admin_role] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"操作失败: {str(e)}"}, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/set_admin_role] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/set_admin_role] 数据库连接已关闭")
                app_logger.info("[groups/set_admin_role] Database connection closed after set admin role attempt.")

    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/set_admin_role] {error_msg}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/set_admin_role] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/set_admin_role] {error_msg}\n{traceback_str}")
        return JSONResponse({"code": 400, "message": "请求数据格式错误"}, status_code=400)
    finally:
        print("=" * 80)


@router.post("/groups/transfer_owner")
async def transfer_owner(request: Request):
    """
    转让群主
    接收客户端发送的 group_id, old_owner_id, new_owner_id
    1. 将新群主设置为群主（self_role = 400）
    2. 让原群主退出群组（从 group_members 表中删除）
    3. 更新群组的成员数量
    """
    print("=" * 80)
    print("[groups/transfer_owner] 收到转让群主请求")

    # 打印请求头信息用于调试
    content_type = request.headers.get("content-type", "")
    content_length = request.headers.get("content-length", "")
    print(f"[groups/transfer_owner] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")

    try:
        # 解析请求体JSON数据
        try:
            body_bytes = await request.body()
            print(f"[groups/transfer_owner] 读取到请求体长度: {len(body_bytes)} 字节")

            if not body_bytes:
                print("[groups/transfer_owner] 错误: 请求体为空")
                return JSONResponse({"code": 400, "message": "请求体不能为空"}, status_code=400)

            try:
                data = json.loads(body_bytes.decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"[groups/transfer_owner] 错误: JSON解析失败 - {e}")
                print(f"[groups/transfer_owner] 请求体内容: {body_bytes.decode('utf-8', errors='ignore')}")
                return JSONResponse({"code": 400, "message": "请求数据格式错误，无法解析JSON"}, status_code=400)

        except ClientDisconnect:
            print("[groups/transfer_owner] 错误: 客户端断开连接")
            print(f"[groups/transfer_owner] 调试信息 - Content-Type: {content_type}, Content-Length: {content_length}")
            app_logger.warning("[groups/transfer_owner] 客户端在请求完成前断开连接")
            return JSONResponse({"code": 400, "message": "客户端断开连接，请检查请求数据是否正确发送"}, status_code=400)
        except Exception as e:
            print(f"[groups/transfer_owner] 读取请求体时发生异常: {type(e).__name__} - {e}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/transfer_owner] 错误堆栈: {traceback_str}")
            return JSONResponse({"code": 400, "message": f"读取请求数据失败: {str(e)}"}, status_code=400)

        print(f"[groups/transfer_owner] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")

        group_id = data.get("group_id")
        old_owner_id = data.get("old_owner_id")
        new_owner_id = data.get("new_owner_id")

        print(f"[groups/transfer_owner] 解析结果 - group_id: {group_id}, old_owner_id: {old_owner_id}, new_owner_id: {new_owner_id}")

        if not group_id:
            print("[groups/transfer_owner] 错误: 缺少 group_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 group_id"}, status_code=400)

        if not old_owner_id:
            print("[groups/transfer_owner] 错误: 缺少 old_owner_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 old_owner_id"}, status_code=400)

        if not new_owner_id:
            print("[groups/transfer_owner] 错误: 缺少 new_owner_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 new_owner_id"}, status_code=400)

        if old_owner_id == new_owner_id:
            print(f"[groups/transfer_owner] 错误: 原群主和新群主不能是同一个人")
            return JSONResponse({"code": 400, "message": "原群主和新群主不能是同一个人"}, status_code=400)

        print("[groups/transfer_owner] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/transfer_owner] 错误: 数据库连接失败")
            app_logger.error("[groups/transfer_owner] 数据库连接失败")
            return JSONResponse({"code": 500, "message": "数据库连接失败"}, status_code=500)
        print("[groups/transfer_owner] 数据库连接成功")

        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)

            print(f"[groups/transfer_owner] 检查群组 {group_id} 是否存在...")
            cursor.execute(
                "SELECT group_id, group_name, member_num, owner_identifier FROM `groups` WHERE group_id = %s", (group_id,)
            )
            group_info = cursor.fetchone()

            if not group_info:
                print(f"[groups/transfer_owner] 错误: 群组 {group_id} 不存在")
                return JSONResponse({"code": 404, "message": "群组不存在"}, status_code=404)

            print(f"[groups/transfer_owner] 群组信息: {group_info}")
            group_name = group_info.get("group_name", "")
            old_owner_identifier = group_info.get("owner_identifier", "")
            print(f"[groups/transfer_owner] 当前群组的 owner_identifier: {old_owner_identifier}")
            print(f"[groups/transfer_owner] 原群主ID (old_owner_id): {old_owner_id}")
            print(f"[groups/transfer_owner] 新群主ID (new_owner_id): {new_owner_id}")

            print(f"[groups/transfer_owner] 检查用户 {old_owner_id} 是否是群组 {group_id} 的群主...")
            cursor.execute(
                "SELECT group_id, user_id, user_name, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, old_owner_id),
            )
            old_owner_info = cursor.fetchone()

            if not old_owner_info:
                print(f"[groups/transfer_owner] 错误: 用户 {old_owner_id} 不在群组 {group_id} 中")
                return JSONResponse({"code": 404, "message": "原群主不是该群组的成员"}, status_code=404)

            old_owner_role = old_owner_info.get("self_role", 200)
            if old_owner_role != 400:
                print(f"[groups/transfer_owner] 错误: 用户 {old_owner_id} 不是群主（当前角色: {old_owner_role}）")
                return JSONResponse({"code": 403, "message": "原群主不是群主，无权转让"}, status_code=403)

            print(f"[groups/transfer_owner] 原群主信息: {old_owner_info}")

            print(f"[groups/transfer_owner] 检查用户 {new_owner_id} 是否在群组 {group_id} 中...")
            cursor.execute(
                "SELECT group_id, user_id, user_name, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, new_owner_id),
            )
            new_owner_info = cursor.fetchone()

            if not new_owner_info:
                print(f"[groups/transfer_owner] 错误: 用户 {new_owner_id} 不在群组 {group_id} 中")
                return JSONResponse({"code": 404, "message": "新群主不是该群组的成员"}, status_code=404)

            print(f"[groups/transfer_owner] 新群主信息: {new_owner_info}")
            new_owner_name = new_owner_info.get("user_name", "")

            print(f"[groups/transfer_owner] ========== 步骤4: 将新群主设置为群主 ==========")
            print(f"[groups/transfer_owner] 将用户 {new_owner_id} 设置为群主 (self_role = 400)...")
            sql_update_role = "UPDATE `group_members` SET self_role = %s WHERE group_id = %s AND user_id = %s"
            params_update_role = (400, group_id, new_owner_id)
            print(f"[groups/transfer_owner] 执行SQL: {sql_update_role}")
            print(f"[groups/transfer_owner] SQL参数: {params_update_role}")
            cursor.execute(sql_update_role, params_update_role)
            update_rows = cursor.rowcount
            print(f"[groups/transfer_owner] 更新新群主角色完成, 影响行数: {update_rows}")
            if update_rows <= 0:
                print(f"[groups/transfer_owner] ✗ 警告: 更新新群主角色操作未影响任何行")
                connection.rollback()
                return JSONResponse({"code": 500, "message": "设置新群主失败"}, status_code=500)

            print(f"[groups/transfer_owner] ========== 步骤5: 删除原群主 ==========")
            print(f"[groups/transfer_owner] 从群组 {group_id} 中删除原群主 {old_owner_id}...")
            sql_delete_owner = "DELETE FROM `group_members` WHERE group_id = %s AND user_id = %s"
            params_delete_owner = (group_id, old_owner_id)
            print(f"[groups/transfer_owner] 执行SQL: {sql_delete_owner}")
            print(f"[groups/transfer_owner] SQL参数: {params_delete_owner}")
            cursor.execute(sql_delete_owner, params_delete_owner)
            delete_rows = cursor.rowcount
            print(f"[groups/transfer_owner] 删除原群主完成, 影响行数: {delete_rows}")
            if delete_rows <= 0:
                print(f"[groups/transfer_owner] ✗ 警告: 删除原群主操作未影响任何行")
                connection.rollback()
                return JSONResponse({"code": 500, "message": "删除原群主失败"}, status_code=500)

            print(f"[groups/transfer_owner] ========== 步骤6: 更新 groups 表的 owner_identifier 字段 ==========")
            print(f"[groups/transfer_owner] 更新前 - 群组 {group_id} 的 owner_identifier: {old_owner_identifier}")
            print(f"[groups/transfer_owner] 更新后 - 群组 {group_id} 的 owner_identifier 将设置为: {new_owner_id}")
            sql_update_owner = "UPDATE `groups` SET owner_identifier = %s WHERE group_id = %s"
            params_update_owner = (new_owner_id, group_id)
            print(f"[groups/transfer_owner] 执行SQL: {sql_update_owner}")
            print(f"[groups/transfer_owner] SQL参数: {params_update_owner}")
            cursor.execute(sql_update_owner, params_update_owner)
            update_owner_rows = cursor.rowcount
            print(f"[groups/transfer_owner] 更新 owner_identifier 完成, 影响行数: {update_owner_rows}")

            if update_owner_rows == 0:
                print(f"[groups/transfer_owner] ✗ 警告: 更新 owner_identifier 操作未影响任何行")
                connection.rollback()
                return JSONResponse({"code": 500, "message": "更新群主标识失败"}, status_code=500)

            print(f"[groups/transfer_owner] 验证更新结果: 查询更新后的 owner_identifier...")
            cursor.execute("SELECT owner_identifier FROM `groups` WHERE group_id = %s", (group_id,))
            verify_result = cursor.fetchone()
            if verify_result:
                updated_owner_identifier = verify_result.get("owner_identifier", "")
                print(f"[groups/transfer_owner] 验证结果 - 当前群组 {group_id} 的 owner_identifier: {updated_owner_identifier}")

            print(f"[groups/transfer_owner] ========== 步骤7: 更新群组成员数量 ==========")
            current_member_num = group_info.get("member_num", 0)
            print(f"[groups/transfer_owner] 更新前 - 群组 {group_id} 的成员数量: {current_member_num}")
            sql_update_member_num = "UPDATE `groups` SET member_num = CASE WHEN member_num > 0 THEN member_num - 1 ELSE 0 END WHERE group_id = %s"
            params_update_member_num = (group_id,)
            print(f"[groups/transfer_owner] 执行SQL: {sql_update_member_num}")
            print(f"[groups/transfer_owner] SQL参数: {params_update_member_num}")
            cursor.execute(sql_update_member_num, params_update_member_num)

            cursor.execute("SELECT member_num FROM `groups` WHERE group_id = %s", (group_id,))
            verify_member_result = cursor.fetchone()
            if verify_member_result:
                updated_member_num = verify_member_result.get("member_num", 0)
                print(f"[groups/transfer_owner] 更新后 - 群组 {group_id} 的成员数量: {updated_member_num}")

            print(f"[groups/transfer_owner] ========== 步骤8: 提交事务 ==========")
            connection.commit()
            print(f"[groups/transfer_owner] ✓ 事务提交成功")

            result = {
                "code": 200,
                "message": "成功转让群主",
                "data": {
                    "group_id": group_id,
                    "group_name": group_name,
                    "old_owner_id": old_owner_id,
                    "new_owner_id": new_owner_id,
                    "new_owner_name": new_owner_name,
                },
            }

            print(f"[groups/transfer_owner] 返回结果: {result}")
            print("=" * 80)

            return JSONResponse(result, status_code=200)

        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/transfer_owner] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/transfer_owner] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/transfer_owner] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"数据库操作失败: {str(e)}"}, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"转让群主时发生异常: {e}"
            print(f"[groups/transfer_owner] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/transfer_owner] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/transfer_owner] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"操作失败: {str(e)}"}, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/transfer_owner] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/transfer_owner] 数据库连接已关闭")
                app_logger.info("[groups/transfer_owner] Database connection closed after transfer owner attempt.")

    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/transfer_owner] {error_msg}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/transfer_owner] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/transfer_owner] {error_msg}\n{traceback_str}")
        return JSONResponse({"code": 400, "message": "请求数据格式错误"}, status_code=400)
    finally:
        print("=" * 80)


@router.get("/groups/members")
def get_group_members_by_group_id(group_id: str = Query(..., description="群组ID，对应groups表的group_id")):
    """
    根据 group_id 从 group_members 表获取群成员信息
    """
    print("=" * 80)
    print("[groups/members] 收到查询群成员请求")
    print(f"[groups/members] 请求参数 - group_id: {group_id}")

    if not group_id:
        print("[groups/members] 错误: 缺少群组ID")
        return JSONResponse({"data": {"message": "缺少群组ID", "code": 400}}, status_code=400)

    print("[groups/members] 开始连接数据库...")
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        print("[groups/members] 错误: 数据库连接失败")
        app_logger.error(f"[groups/members] 数据库连接失败 for group_id={group_id}")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)
    print("[groups/members] 数据库连接成功")

    cursor = None
    try:
        import time

        start_time = time.time()

        cursor = connection.cursor(dictionary=True)

        has_teach_subjects = _group_members_has_column(cursor, GROUP_MEMBERS_TEACH_SUBJECTS_COL)
        sql = """
            SELECT 
                gm.group_id,
                gm.user_id,
                gm.user_name,
                gm.self_role,
                gm.join_time,
                gm.msg_flag,
                gm.self_msg_flag,
                gm.readed_seq,
                gm.unread_num,
                gm.is_voice_enabled
        """
        if has_teach_subjects:
            sql += """
                , gm.teach_subjects
            """
        sql += """
            FROM `group_members` gm
            WHERE gm.group_id = %s
            ORDER BY gm.join_time ASC
        """
        print(f"[groups/members] 执行SQL查询: {sql}")
        print(f"[groups/members] 查询参数: group_id={group_id}")
        app_logger.info(f"[groups/members] 开始查询群组成员: group_id={group_id}")

        query_start = time.time()
        cursor.execute(sql, (group_id,))
        members = cursor.fetchall()
        query_time = time.time() - query_start

        print(f"[groups/members] 查询完成，耗时: {query_time:.3f}秒")
        print(f"[groups/members] 查询结果: 找到 {len(members)} 个成员")
        app_logger.info(f"[groups/members] 查询完成: group_id={group_id}, member_count={len(members)}, query_time={query_time:.3f}s")

        role_stats = {}
        for member in members:
            role = member.get("self_role", 200)
            role_name = {200: "普通成员", 300: "管理员", 400: "群主"}.get(role, f"未知角色({role})")
            role_stats[role_name] = role_stats.get(role_name, 0) + 1

        print(f"[groups/members] 成员角色统计: {role_stats}")
        app_logger.info(f"[groups/members] 成员角色统计: group_id={group_id}, stats={role_stats}")

        for idx, member in enumerate(members):
            user_id = member.get("user_id")
            user_name = member.get("user_name")
            self_role = member.get("self_role")
            role_name = {200: "普通成员", 300: "管理员", 400: "群主"}.get(self_role, f"未知({self_role})")

            print(f"[groups/members] 处理第 {idx+1}/{len(members)} 个成员: user_id={user_id}, user_name={user_name}, role={role_name}")

            if has_teach_subjects:
                member["teach_subjects"] = _normalize_teach_subjects(member.get("teach_subjects"))

            for key, value in member.items():
                if isinstance(value, datetime.datetime):
                    old_value = value
                    member[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[groups/members]   转换时间字段 {key}: {old_value} -> {member[key]}")

        total_time = time.time() - start_time
        print(f"[groups/members] 数据处理完成，总耗时: {total_time:.3f}秒")

        result = {
            "data": {
                "message": "查询成功",
                "code": 200,
                "group_id": group_id,
                "members": members,
                "member_count": len(members),
                "role_stats": role_stats,
            }
        }

        print(f"[groups/members] 返回结果: group_id={group_id}, member_count={len(members)}, role_stats={role_stats}")
        print(f"[groups/members] 总耗时: {total_time:.3f}秒")
        app_logger.info(f"[groups/members] 查询成功: group_id={group_id}, member_count={len(members)}, total_time={total_time:.3f}s")
        print("=" * 80)

        return JSONResponse(result, status_code=200)

    except mysql.connector.Error as e:
        error_msg = f"查询群成员数据库错误: {e}"
        error_code = e.errno if hasattr(e, "errno") else None
        print(f"[groups/members] {error_msg}")
        print(f"[groups/members] MySQL错误代码: {error_code}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/members] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/members] 数据库错误: group_id={group_id}, error={error_msg}, errno={error_code}\n{traceback_str}")
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        error_msg = f"查询群成员时发生异常: {e}"
        error_type = type(e).__name__
        print(f"[groups/members] {error_msg}")
        print(f"[groups/members] 异常类型: {error_type}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/members] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/members] 未知异常: group_id={group_id}, error_type={error_type}, error={error_msg}\n{traceback_str}")
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
            print("[groups/members] 游标已关闭")
        if connection and connection.is_connected():
            connection.close()
            print("[groups/members] 数据库连接已关闭")
            app_logger.info(f"[groups/members] Database connection closed after get_group_members_by_group_id attempt for group_id={group_id}.")


@router.post("/groups/member/teach-subjects")
async def update_group_member_teach_subjects(request: Request):
    """
    设置/更新某个群成员（通常是老师）在该班级（group_id）里教授的科目列表。

    请求体 JSON:
    {
      "group_id": "班级群ID",
      "user_id": "老师/成员ID",
      "teach_subjects": ["语文","数学"]  // 也兼容 "subjects"
    }
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"data": {"message": "请求数据格式错误", "code": 400}}, status_code=400)

    group_id = str((data or {}).get("group_id") or "").strip()
    user_id = str((data or {}).get("user_id") or "").strip()
    raw_subjects = (data or {}).get("teach_subjects")
    if raw_subjects is None:
        raw_subjects = (data or {}).get("subjects")

    if not group_id or not user_id:
        return JSONResponse({"data": {"message": "缺少 group_id 或 user_id", "code": 400}}, status_code=400)

    # 兼容：传字符串（逗号分隔）/ JSON 字符串 / 列表
    subjects: List[str] = []
    if raw_subjects is None:
        subjects = []
    elif isinstance(raw_subjects, list):
        subjects = [str(x).strip() for x in raw_subjects if x is not None and str(x).strip() != ""]
    elif isinstance(raw_subjects, (bytes, bytearray)):
        subjects = [raw_subjects.decode("utf-8", errors="replace").strip()]
    elif isinstance(raw_subjects, str):
        s = raw_subjects.strip()
        if not s:
            subjects = []
        else:
            # 尝试把字符串当 JSON 解析（["语文","数学"]），失败则按逗号切分
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    subjects = [str(x).strip() for x in parsed if x is not None and str(x).strip() != ""]
                elif parsed is None:
                    subjects = []
                else:
                    subjects = [str(parsed).strip()] if str(parsed).strip() else []
            except Exception:
                subjects = [x.strip() for x in s.replace("，", ",").split(",") if x.strip()]
    else:
        subjects = [str(raw_subjects).strip()] if str(raw_subjects).strip() else []

    # 去重（保持顺序）
    deduped: List[str] = []
    seen = set()
    for sub in subjects:
        if sub in seen:
            continue
        seen.add(sub)
        deduped.append(sub)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor()

        # 确保字段存在（无 ALTER 权限时会返回 False）
        ensured = _ensure_group_members_teach_subjects_column(cursor)
        if not ensured:
            return JSONResponse(
                {
                    "data": {
                        "message": "数据库缺少字段 group_members.teach_subjects，请手工执行 ALTER TABLE 增加该字段后重试",
                        "code": 500,
                    }
                },
                status_code=500,
            )

        update_sql = "UPDATE `group_members` SET `teach_subjects` = %s WHERE `group_id` = %s AND `user_id` = %s"
        cursor.execute(update_sql, (json.dumps(deduped, ensure_ascii=False), group_id, user_id))
        connection.commit()

        if cursor.rowcount <= 0:
            return JSONResponse({"data": {"message": "未找到该群成员记录", "code": 404}}, status_code=404)

        return JSONResponse(
            {"data": {"message": "更新成功", "code": 200, "group_id": group_id, "user_id": user_id, "teach_subjects": deduped}},
            status_code=200,
        )
    except mysql.connector.Error as e:
        connection.rollback()
        return JSONResponse({"data": {"message": f"更新失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        connection.rollback()
        return JSONResponse({"data": {"message": f"更新失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        finally:
            if connection and connection.is_connected():
                connection.close()


@router.get("/group/members")
def get_group_members(unique_group_id: str = Query(..., description="群唯一ID")):
    """
    根据 unique_group_id 查询群主和所有成员的 id + name
    """
    if not unique_group_id:
        return JSONResponse({"data": {"message": "缺少群唯一ID", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        sql_admin = """
            SELECT group_admin_id
            FROM ta_group
            WHERE unique_group_id = %s
        """
        cursor.execute(sql_admin, (unique_group_id,))
        group_info = cursor.fetchone()

        if not group_info:
            return JSONResponse({"data": {"message": "群不存在", "code": 404}}, status_code=404)

        group_admin_id = group_info.get("group_admin_id")

        members_data = []

        if group_admin_id:
            sql_teacher = """
                SELECT teacher_unique_id, name
                FROM ta_teacher
                WHERE teacher_unique_id = %s
            """
            cursor.execute(sql_teacher, (group_admin_id,))
            teacher_info = cursor.fetchone()
            if teacher_info:
                members_data.append({"id": teacher_info.get("teacher_unique_id"), "name": teacher_info.get("name"), "role": "群主"})

        sql_member = """
            SELECT unique_member_id, member_name
            FROM ta_group_member_relation
            WHERE unique_group_id = %s
        """
        cursor.execute(sql_member, (unique_group_id,))
        member_infos = cursor.fetchall()

        for m in member_infos:
            members_data.append({"id": m.get("unique_member_id"), "name": m.get("member_name"), "role": "成员"})

        return JSONResponse({"data": {"message": "查询成功", "code": 200, "members": members_data}}, status_code=200)

    except mysql.connector.Error as e:
        app_logger.error(f"查询错误: {e}")
        return JSONResponse({"data": {"message": "查询失败", "code": 500}}, status_code=500)

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after get_group_members attempt for {unique_group_id}.")


@router.post("/updateGroupInfo")
async def updateGroupInfo(request: Request):
    data = await request.json()
    unique_group_id = data.get("unique_group_id")
    avatar = data.get("avatar")

    if not unique_group_id or not avatar:
        app_logger.warning("UpdateGroupInfo failed: Missing unique_group_id or avatar.")
        return JSONResponse({"data": {"message": "群ID和头像必须提供", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("UpdateGroupInfo failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    try:
        avatar_bytes = base64.b64decode(avatar)
    except Exception as e:
        app_logger.error(f"Base64 decode error for unique_group_id={unique_group_id}: {e}")
        return JSONResponse({"data": {"message": "头像数据解析失败", "code": 400}}, status_code=400)

    filename = f"{unique_group_id}_.png"
    file_path = os.path.join(IMAGE_DIR, filename)
    try:
        with open(file_path, "wb") as f:
            f.write(avatar_bytes)
    except Exception as e:
        app_logger.error(f"Error writing avatar file {file_path}: {e}")
        return JSONResponse({"data": {"message": "头像文件写入失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        update_query = """
            UPDATE ta_group
            SET headImage_path = %s
            WHERE unique_group_id = %s
        """
        cursor = connection.cursor(dictionary=True)
        cursor.execute(update_query, (file_path, unique_group_id))
        connection.commit()
        cursor.close()

        app_logger.info(f"Updated group avatar for {unique_group_id} -> {file_path}")
        return JSONResponse({"data": {"message": "更新成功", "code": 200}})
    except mysql.connector.Error as e:
        app_logger.error(f"Database error during updateGroupInfo for {unique_group_id}: {e}")
        return JSONResponse({"data": {"message": "更新失败", "code": 500}}, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after updating group info for {unique_group_id}.")


@router.post("/groups/sync")
async def sync_groups(request: Request):
    """
    同步腾讯群组数据到本地数据库
    接收客户端发送的群组列表，插入到 groups 和 group_members 表
    """
    print("=" * 80)
    print("[groups/sync] 收到同步请求")
    try:
        data = await request.json()
        print(f"[groups/sync] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
        groups = data.get("groups", [])
        user_id = data.get("user_id")
        classid = data.get("classid")
        schoolid = data.get("schoolid")
        print(f"[groups/sync] 解析结果 - user_id: {user_id}, groups数量: {len(groups)}, classid: {classid}, schoolid: {schoolid}")

        if not groups:
            print("[groups/sync] 错误: 没有群组数据")
            return JSONResponse({"data": {"message": "没有群组数据需要同步", "code": 400}}, status_code=400)

        if not user_id:
            print("[groups/sync] 错误: 缺少 user_id")
            return JSONResponse({"data": {"message": "缺少 user_id 参数", "code": 400}}, status_code=400)

        print("[groups/sync] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/sync] 错误: 数据库连接失败")
            app_logger.error("Database connection error in /groups/sync API.")
            return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)
        print("[groups/sync] 数据库连接成功")

        cursor = None
        try:
            cursor = connection.cursor()
            success_count = 0
            error_count = 0

            print("[groups/sync] 检查表是否存在...")
            cursor.execute("SHOW TABLES LIKE 'groups'")
            groups_table_exists = cursor.fetchone()
            cursor.execute("SHOW TABLES LIKE 'group_members'")
            group_members_table_exists = cursor.fetchone()
            print(
                f"[groups/sync] groups表存在: {groups_table_exists is not None}, group_members表存在: {group_members_table_exists is not None}"
            )

            if groups_table_exists:
                print("[groups/sync] 检查 groups 表结构...")
                cursor.execute("DESCRIBE `groups`")
                groups_columns = cursor.fetchall()
                print(f"[groups/sync] groups 表字段信息:")
                for col in groups_columns:
                    print(f"  {col}")

            if group_members_table_exists:
                print("[groups/sync] 检查 group_members 表结构...")
                cursor.execute("DESCRIBE `group_members`")
                group_members_columns = cursor.fetchall()
                print(f"[groups/sync] group_members 表字段信息:")
                for col in group_members_columns:
                    print(f"  {col}")

                # 尝试自动补齐 teach_subjects 字段（若账号无 ALTER 权限则跳过）
                try:
                    ensured = _ensure_group_members_teach_subjects_column(cursor)
                    print(f"[groups/sync] schema ensure group_members.teach_subjects: {ensured}")
                except Exception as e:
                    print(f"[groups/sync] schema ensure group_members.teach_subjects exception: {e}")

            for idx, group in enumerate(groups):
                try:
                    group_id = group.get("group_id")
                    print(f"[groups/sync] 处理第 {idx+1}/{len(groups)} 个群组, group_id: {group_id}")

                    print(f"[groups/sync] 检查群组 {group_id} 是否已存在...")
                    cursor.execute("SELECT group_id FROM `groups` WHERE group_id = %s", (group_id,))
                    group_exists = cursor.fetchone()
                    print(f"[groups/sync] 群组 {group_id} 已存在: {group_exists is not None}")

                    def timestamp_to_datetime(ts):
                        if ts is None or ts == 0:
                            return None
                        try:
                            if ts > 2147483647:
                                ts = int(ts / 1000)
                            else:
                                ts = int(ts)
                            dt = datetime.datetime.fromtimestamp(ts)
                            return dt.strftime("%Y-%m-%d %H:%M:%S")
                        except (ValueError, OSError) as e:
                            print(f"[groups/sync] 警告: 时间戳 {ts} 转换失败: {e}，设置为 NULL")
                            return None

                    if group_exists:
                        print(f"[groups/sync] 更新群组 {group_id} 的信息...")
                        create_time_dt = timestamp_to_datetime(group.get("create_time"))
                        last_msg_time_dt = timestamp_to_datetime(group.get("last_msg_time"))
                        last_info_time_dt = timestamp_to_datetime(group.get("last_info_time"))

                        group_classid = group.get("classid") or classid
                        group_schoolid = group.get("schoolid") or schoolid

                        def is_empty(value):
                            return value is None or value == "" or (isinstance(value, str) and value.strip() == "")

                        update_fields = [
                            "group_name = %s",
                            "group_type = %s",
                            "face_url = %s",
                            "detail_face_url = %s",
                            "create_time = %s",
                            "max_member_num = %s",
                            "member_num = %s",
                            "introduction = %s",
                            "notification = %s",
                            "searchable = %s",
                            "visible = %s",
                            "add_option = %s",
                            "is_shutup_all = %s",
                            "next_msg_seq = %s",
                            "latest_seq = %s",
                            "last_msg_time = %s",
                            "last_info_time = %s",
                            "info_seq = %s",
                            "detail_info_seq = %s",
                            "detail_group_id = %s",
                            "detail_group_name = %s",
                            "detail_group_type = %s",
                            "detail_is_shutup_all = %s",
                            "online_member_num = %s",
                        ]
                        update_params = [
                            group.get("group_name"),
                            group.get("group_type"),
                            group.get("face_url"),
                            group.get("detail_face_url"),
                            create_time_dt,
                            group.get("max_member_num"),
                            group.get("member_num"),
                            group.get("introduction"),
                            group.get("notification"),
                            group.get("searchable"),
                            group.get("visible"),
                            group.get("add_option"),
                            group.get("is_shutup_all"),
                            group.get("next_msg_seq"),
                            group.get("latest_seq"),
                            last_msg_time_dt,
                            last_info_time_dt,
                            group.get("info_seq"),
                            group.get("detail_info_seq"),
                            group.get("detail_group_id"),
                            group.get("detail_group_name"),
                            group.get("detail_group_type"),
                            group.get("detail_is_shutup_all"),
                            group.get("online_member_num"),
                        ]

                        owner_identifier = group.get("owner_identifier")
                        if not is_empty(owner_identifier):
                            update_fields.append("owner_identifier = %s")
                            update_params.append(owner_identifier)
                            print(f"[groups/sync] 将更新 owner_identifier: {owner_identifier}")
                        else:
                            print(f"[groups/sync] owner_identifier 为空，跳过更新")

                        if not is_empty(group_classid):
                            update_fields.append("classid = %s")
                            update_params.append(group_classid)
                            print(f"[groups/sync] 将更新 classid: {group_classid}")
                        else:
                            print(f"[groups/sync] classid 为空，跳过更新")

                        if not is_empty(group_schoolid):
                            update_fields.append("schoolid = %s")
                            update_params.append(group_schoolid)
                            print(f"[groups/sync] 将更新 schoolid: {group_schoolid}")
                        else:
                            print(f"[groups/sync] schoolid 为空，跳过更新")

                        is_class_group = group.get("is_class_group")
                        if is_class_group is not None:
                            update_fields.append("is_class_group = %s")
                            update_params.append(is_class_group)
                            print(f"[groups/sync] 将更新 is_class_group: {is_class_group}")
                        else:
                            print(f"[groups/sync] is_class_group 未提供，使用数据库默认值")

                        update_params.append(group.get("group_id"))

                        update_group_sql = f"""
                            UPDATE `groups` SET
                                {', '.join(update_fields)}
                            WHERE group_id = %s
                        """
                        print(f"[groups/sync] 更新参数: {update_params}")
                        cursor.execute(update_group_sql, update_params)
                        affected_rows = cursor.rowcount
                        print(f"[groups/sync] 更新群组 {group_id} 完成, 影响行数: {affected_rows}")
                    else:
                        print(f"[groups/sync] 插入新群组 {group_id}...")
                        create_time_dt = timestamp_to_datetime(group.get("create_time"))
                        last_msg_time_dt = timestamp_to_datetime(group.get("last_msg_time"))
                        last_info_time_dt = timestamp_to_datetime(group.get("last_info_time"))

                        print(
                            f"[groups/sync] 时间戳转换: create_time={create_time_dt}, last_msg_time={last_msg_time_dt}, last_info_time={last_info_time_dt}"
                        )

                        group_classid = group.get("classid") or classid
                        group_schoolid = group.get("schoolid") or schoolid

                        def is_empty(value):
                            return value is None or value == "" or (isinstance(value, str) and value.strip() == "")

                        if is_empty(group_classid):
                            group_classid = None
                            print(f"[groups/sync] classid 为空，将插入 NULL")
                        else:
                            print(f"[groups/sync] 将插入 classid: {group_classid}")

                        if is_empty(group_schoolid):
                            group_schoolid = None
                            print(f"[groups/sync] schoolid 为空，将插入 NULL")
                        else:
                            print(f"[groups/sync] 将插入 schoolid: {group_schoolid}")

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
                        insert_params = (
                            group.get("group_id"),
                            group.get("group_name"),
                            group.get("group_type"),
                            group.get("face_url"),
                            group.get("detail_face_url"),
                            group.get("owner_identifier"),
                            create_time_dt,
                            group.get("max_member_num"),
                            group.get("member_num"),
                            group.get("introduction"),
                            group.get("notification"),
                            group.get("searchable"),
                            group.get("visible"),
                            group.get("add_option"),
                            group.get("is_shutup_all"),
                            group.get("next_msg_seq"),
                            group.get("latest_seq"),
                            last_msg_time_dt,
                            last_info_time_dt,
                            group.get("info_seq"),
                            group.get("detail_info_seq"),
                            group.get("detail_group_id"),
                            group.get("detail_group_name"),
                            group.get("detail_group_type"),
                            group.get("detail_is_shutup_all"),
                            group.get("online_member_num"),
                            group_classid,
                            group_schoolid,
                            group.get("is_class_group", 1),
                        )
                        print(f"[groups/sync] 插入参数: {insert_params}")
                        cursor.execute(insert_group_sql, insert_params)
                        affected_rows = cursor.rowcount
                        lastrowid = cursor.lastrowid
                        print(f"[groups/sync] 插入群组 {group_id} 完成, 影响行数: {affected_rows}, lastrowid: {lastrowid}")

                    members_list = group.get("members", [])
                    member_info = group.get("member_info")
                    print(
                        f"[groups/sync] 群组 {group_id} 的成员信息: member_info={member_info is not None}, members数组={len(members_list)}个成员"
                    )

                    processed_member_ids = set()

                    if member_info:
                        member_user_id = member_info.get("user_id")
                        if member_user_id:
                            print(f"[groups/sync] 处理 member_info（群主）: user_id={member_user_id}")
                            member_user_name = member_info.get("user_name", "")
                            member_self_role = member_info.get("self_role", 400)
                            member_join_time = timestamp_to_datetime(member_info.get("join_time")) or timestamp_to_datetime(
                                group.get("create_time")
                            )
                            if not member_join_time:
                                member_join_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                            cursor.execute(
                                "SELECT group_id FROM `group_members` WHERE group_id = %s AND user_id = %s", (group_id, member_user_id)
                            )
                            member_exists = cursor.fetchone()

                            if member_exists:
                                print(f"[groups/sync] 更新群主 group_id={group_id}, user_id={member_user_id}, self_role={member_self_role}")
                                update_member_sql = """
                                    UPDATE `group_members` SET
                                        user_name = %s, self_role = %s, join_time = %s,
                                        msg_flag = %s, self_msg_flag = %s, readed_seq = %s, unread_num = %s
                                    WHERE group_id = %s AND user_id = %s
                                """
                                update_params = (
                                    member_user_name if member_user_name else None,
                                    member_self_role,
                                    member_join_time,
                                    member_info.get("msg_flag", 0),
                                    member_info.get("self_msg_flag", 0),
                                    member_info.get("readed_seq", 0),
                                    member_info.get("unread_num", 0),
                                    group_id,
                                    member_user_id,
                                )
                                cursor.execute(update_member_sql, update_params)
                            else:
                                print(f"[groups/sync] 插入群主 group_id={group_id}, user_id={member_user_id}, self_role={member_self_role}")
                                insert_member_sql = """
                                    INSERT INTO `group_members` (
                                        group_id, user_id, user_name, self_role, join_time, msg_flag,
                                        self_msg_flag, readed_seq, unread_num
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                """
                                insert_params = (
                                    group_id,
                                    member_user_id,
                                    member_user_name if member_user_name else None,
                                    member_self_role,
                                    member_join_time,
                                    member_info.get("msg_flag", 0),
                                    member_info.get("self_msg_flag", 0),
                                    member_info.get("readed_seq", 0),
                                    member_info.get("unread_num", 0),
                                )
                                cursor.execute(insert_member_sql, insert_params)

                            processed_member_ids.add(member_user_id)
                        else:
                            print(f"[groups/sync] 警告: member_info 缺少 user_id，跳过")
                    else:
                        print(f"[groups/sync] 警告: 缺少 member_info（群主信息），这是必需的")

                    if members_list:
                        print(f"[groups/sync] 处理 members 数组，共 {len(members_list)} 个成员")
                        for member_item in members_list:
                            member_user_id = member_item.get("user_id") or member_item.get("unique_member_id")
                            member_user_name = member_item.get("user_name") or member_item.get("member_name", "")

                            if not member_user_id:
                                print(f"[groups/sync] 警告: 成员信息缺少 user_id/unique_member_id，跳过")
                                continue

                            if member_user_id in processed_member_ids:
                                print(f"[groups/sync] 跳过已处理的成员（群主）: user_id={member_user_id}")
                                continue

                            if "self_role" in member_item:
                                member_self_role = member_item.get("self_role")
                            else:
                                group_role = member_item.get("group_role")
                                if group_role == 400:
                                    member_self_role = 400
                                elif group_role == 300:
                                    member_self_role = 300
                                else:
                                    member_self_role = 200

                            member_join_time = timestamp_to_datetime(member_item.get("join_time")) or timestamp_to_datetime(
                                group.get("create_time")
                            )
                            if not member_join_time:
                                member_join_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                            cursor.execute(
                                "SELECT group_id FROM `group_members` WHERE group_id = %s AND user_id = %s", (group_id, member_user_id)
                            )
                            member_exists = cursor.fetchone()

                            if member_exists:
                                print(
                                    f"[groups/sync] 更新成员 group_id={group_id}, user_id={member_user_id}, self_role={member_self_role}"
                                )
                                update_member_sql = """
                                    UPDATE `group_members` SET
                                        user_name = %s, self_role = %s, join_time = %s,
                                        msg_flag = %s, self_msg_flag = %s, readed_seq = %s, unread_num = %s
                                    WHERE group_id = %s AND user_id = %s
                                """
                                update_params = (
                                    member_user_name if member_user_name else None,
                                    member_self_role,
                                    member_join_time,
                                    member_item.get("msg_flag", 0),
                                    member_item.get("self_msg_flag", 0),
                                    member_item.get("readed_seq", 0),
                                    member_item.get("unread_num", 0),
                                    group_id,
                                    member_user_id,
                                )
                                cursor.execute(update_member_sql, update_params)
                            else:
                                print(
                                    f"[groups/sync] 插入成员 group_id={group_id}, user_id={member_user_id}, self_role={member_self_role}"
                                )
                                insert_member_sql = """
                                    INSERT INTO `group_members` (
                                        group_id, user_id, user_name, self_role, join_time, msg_flag,
                                        self_msg_flag, readed_seq, unread_num
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                """
                                insert_params = (
                                    group_id,
                                    member_user_id,
                                    member_user_name if member_user_name else None,
                                    member_self_role,
                                    member_join_time,
                                    member_item.get("msg_flag", 0),
                                    member_item.get("self_msg_flag", 0),
                                    member_item.get("readed_seq", 0),
                                    member_item.get("unread_num", 0),
                                )
                                cursor.execute(insert_member_sql, insert_params)

                            processed_member_ids.add(member_user_id)

                    success_count += 1
                    print(f"[groups/sync] 群组 {group_id} 处理成功")
                except Exception as e:
                    error_msg = f"处理群组 {group.get('group_id')} 时出错: {e}"
                    print(f"[groups/sync] {error_msg}")
                    import traceback

                    traceback_str = traceback.format_exc()
                    print(f"[groups/sync] 错误堆栈: {traceback_str}")
                    app_logger.error(f"{error_msg}\n{traceback_str}")
                    error_count += 1
                    continue

            print(f"[groups/sync] 准备提交事务, 成功: {success_count}, 失败: {error_count}")
            connection.commit()
            print(f"[groups/sync] 事务提交成功")

            app_logger.info(f"群组同步完成: 成功 {success_count} 个, 失败 {error_count} 个")
            print(f"[groups/sync] 群组同步完成: 成功 {success_count} 个, 失败 {error_count} 个")

            tencent_sync_summary = await notify_tencent_group_sync(user_id, groups)
            print(f"[groups/sync] 腾讯 REST API 同步结果: {tencent_sync_summary}")

            result = {
                "data": {
                    "message": "群组同步完成",
                    "code": 200,
                    "success_count": success_count,
                    "error_count": error_count,
                    "tencent_sync": tencent_sync_summary,
                }
            }
            print(f"[groups/sync] 返回结果: {result}")
            return JSONResponse(result, status_code=200)

        except mysql.connector.Error as e:
            error_msg = f"数据库错误: {e}"
            print(f"[groups/sync] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/sync] 数据库错误堆栈: {traceback_str}")
            connection.rollback()
            print(f"[groups/sync] 事务已回滚")
            app_logger.error(f"{error_msg}\n{traceback_str}")
            return JSONResponse({"data": {"message": f"数据库操作失败: {str(e)}", "code": 500}}, status_code=500)
        except Exception as e:
            error_msg = f"同步群组时发生错误: {e}"
            print(f"[groups/sync] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/sync] 错误堆栈: {traceback_str}")
            connection.rollback()
            print(f"[groups/sync] 事务已回滚")
            app_logger.error(f"{error_msg}\n{traceback_str}")
            return JSONResponse({"data": {"message": f"同步失败: {str(e)}", "code": 500}}, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/sync] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/sync] 数据库连接已关闭")
                app_logger.info("Database connection closed after groups sync.")

    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/sync] {error_msg}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/sync] 解析错误堆栈: {traceback_str}")
        app_logger.error(f"{error_msg}\n{traceback_str}")
        return JSONResponse({"data": {"message": "请求数据格式错误", "code": 400}}, status_code=400)
    finally:
        print("=" * 80)


@router.get("/groups")
def get_groups_by_admin(
    group_admin_id: str = Query(..., description="群管理员的唯一ID"),
    nickname_keyword: str = Query(None, description="群名关键词（支持模糊查询）"),
):
    """
    根据群管理员ID查询ta_group表，可选群名关键词模糊匹配
    """
    if not group_admin_id:
        return JSONResponse({"data": {"message": "缺少群管理员ID", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        if nickname_keyword:
            sql = """
                SELECT * FROM ta_group
                WHERE group_admin_id=%s AND nickname LIKE %s
            """
            cursor.execute(sql, (group_admin_id, f"%{nickname_keyword}%"))
        else:
            sql = "SELECT * FROM ta_group WHERE group_admin_id=%s"
            cursor.execute(sql, (group_admin_id,))

        groups = cursor.fetchall()
        for group in groups:
            avatar_path = group.get("headImage_path")
            local_avatar_file = resolve_local_avatar_file_path(avatar_path)
            if local_avatar_file and os.path.exists(local_avatar_file):
                try:
                    with open(local_avatar_file, "rb") as img:
                        group["avatar_base64"] = base64.b64encode(img.read()).decode("utf-8")
                except Exception as e:
                    app_logger.error(f"读取图片失败 {local_avatar_file}: {e}")
                    group["avatar_base64"] = None
            else:
                group["avatar_base64"] = None

        for row in groups:
            for key in row:
                if isinstance(row[key], datetime.datetime):
                    row[key] = row[key].strftime("%Y-%m-%d %H:%M:%S")

        return JSONResponse({"data": {"message": "查询成功", "code": 200, "groups": groups}}, status_code=200)

    except mysql.connector.Error as e:
        print(f"查询错误: {e}")
        return JSONResponse({"data": {"message": "查询失败", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after get_groups_by_admin attempt for {group_admin_id}.")


@router.get("/member/groups")
def get_member_groups(unique_member_id: str = Query(..., description="成员唯一ID")):
    """
    根据 unique_member_id 查询该成员所在的群列表 (JOIN ta_group)
    """
    if not unique_member_id:
        return JSONResponse({"data": {"message": "缺少成员唯一ID", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        sql = """
            SELECT g.*
            FROM ta_group_member_relation m
            INNER JOIN ta_group g ON m.unique_group_id = g.unique_group_id
            WHERE m.unique_member_id = %s
        """
        cursor.execute(sql, (unique_member_id,))
        groups = cursor.fetchall()

        for group in groups:
            avatar_path = group.get("headImage_path")
            local_avatar_file = resolve_local_avatar_file_path(avatar_path)
            if local_avatar_file and os.path.exists(local_avatar_file):
                try:
                    with open(local_avatar_file, "rb") as img:
                        group["avatar_base64"] = base64.b64encode(img.read()).decode("utf-8")
                except Exception as e:
                    app_logger.error(f"读取图片失败 {local_avatar_file}: {e}")
                    group["avatar_base64"] = None
            else:
                group["avatar_base64"] = None

        for row in groups:
            for key, value in row.items():
                if isinstance(value, datetime.datetime):
                    row[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        return JSONResponse({"data": {"message": "查询成功", "code": 200, "joingroups": groups}}, status_code=200)

    except mysql.connector.Error as e:
        print(f"查询错误: {e}")
        return JSONResponse({"data": {"message": "查询失败", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after get_member_groups attempt for {unique_member_id}.")


@router.get("/groups/by-teacher")
def get_groups_by_teacher(teacher_unique_id: str = Query(..., description="教师唯一ID，对应group_members表的user_id")):
    """
    根据 teacher_unique_id 查询该教师所在的群组，按角色分组返回
    - 是群主的群组（self_role = 400）
    - 不是群主的群组（self_role != 400）
    """
    if not teacher_unique_id:
        return JSONResponse({"data": {"message": "缺少教师唯一ID", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        sql = """
            SELECT 
                g.*,
                gm.user_id,
                gm.user_name,
                gm.self_role,
                gm.join_time as member_join_time,
                gm.msg_flag,
                gm.self_msg_flag,
                gm.readed_seq,
                gm.unread_num
            FROM `group_members` gm
            INNER JOIN `groups` g ON gm.group_id = g.group_id
            WHERE gm.user_id = %s
            ORDER BY g.create_time DESC
        """
        cursor.execute(sql, (teacher_unique_id,))
        results = cursor.fetchall()

        for row in results:
            for key, value in row.items():
                if isinstance(value, datetime.datetime):
                    row[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        owner_groups = []
        member_groups = []

        for row in results:
            group_id = row.get("group_id")
            group_info = {
                "group_id": group_id,
                "group_name": row.get("group_name"),
                "group_type": row.get("group_type"),
                "face_url": row.get("face_url"),
                "detail_face_url": row.get("detail_face_url"),
                "owner_identifier": row.get("owner_identifier"),
                "create_time": row.get("create_time"),
                "max_member_num": row.get("max_member_num"),
                "member_num": row.get("member_num"),
                "introduction": row.get("introduction"),
                "notification": row.get("notification"),
                "searchable": row.get("searchable"),
                "visible": row.get("visible"),
                "add_option": row.get("add_option"),
                "is_shutup_all": row.get("is_shutup_all"),
                "next_msg_seq": row.get("next_msg_seq"),
                "latest_seq": row.get("latest_seq"),
                "last_msg_time": row.get("last_msg_time"),
                "last_info_time": row.get("last_info_time"),
                "info_seq": row.get("info_seq"),
                "detail_info_seq": row.get("detail_info_seq"),
                "detail_group_id": row.get("detail_group_id"),
                "detail_group_name": row.get("detail_group_name"),
                "detail_group_type": row.get("detail_group_type"),
                "detail_is_shutup_all": row.get("detail_is_shutup_all"),
                "online_member_num": row.get("online_member_num"),
                "classid": row.get("classid"),
                "schoolid": row.get("schoolid"),
                "is_class_group": row.get("is_class_group"),
                "member_info": {
                    "user_id": row.get("user_id"),
                    "user_name": row.get("user_name"),
                    "self_role": row.get("self_role"),
                    "join_time": row.get("member_join_time"),
                    "msg_flag": row.get("msg_flag"),
                    "self_msg_flag": row.get("self_msg_flag"),
                    "readed_seq": row.get("readed_seq"),
                    "unread_num": row.get("unread_num"),
                },
            }

            temp_room_info = None
            if group_id:
                if group_id in active_temp_rooms:
                    room_info = active_temp_rooms[group_id]
                    temp_room_info = {
                        "room_id": room_info.get("room_id"),
                        "publish_url": room_info.get("publish_url"),
                        "play_url": room_info.get("play_url"),
                        "stream_name": room_info.get("stream_name"),
                        "owner_id": room_info.get("owner_id"),
                        "owner_name": room_info.get("owner_name"),
                        "owner_icon": room_info.get("owner_icon"),
                        "members": room_info.get("members", []),
                    }
                    app_logger.info(f"[groups/by-teacher] 群组 {group_id} 有临时语音房间（内存），已添加到返回信息")
                else:
                    try:
                        room_query = """
                            SELECT room_id, group_id, owner_id, owner_name, owner_icon,
                                   whip_url, whep_url, stream_name, status, create_time
                            FROM temp_voice_rooms
                            WHERE group_id = %s AND status = 1
                            ORDER BY create_time DESC
                            LIMIT 1
                        """
                        cursor.execute(room_query, (group_id,))
                        room_row = cursor.fetchone()

                        if room_row:
                            stream_name = room_row.get("stream_name")
                            publish_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/publish/?app={SRS_APP}&stream={stream_name}"
                            play_url = f"{SRS_WEBRTC_API_URL}/rtc/v1/play/?app={SRS_APP}&stream={stream_name}"

                            members_query = """
                                SELECT user_id, user_name, status
                                FROM temp_voice_room_members
                                WHERE room_id = %s AND status = 1
                            """
                            cursor.execute(members_query, (room_row.get("room_id"),))
                            member_rows = cursor.fetchall()
                            members = [m.get("user_id") for m in member_rows if m.get("user_id")]

                            temp_room_info = {
                                "room_id": room_row.get("room_id"),
                                "publish_url": publish_url,
                                "play_url": play_url,
                                "stream_name": stream_name,
                                "owner_id": room_row.get("owner_id"),
                                "owner_name": room_row.get("owner_name"),
                                "owner_icon": room_row.get("owner_icon"),
                                "members": members,
                            }

                            active_temp_rooms[group_id] = {
                                "room_id": room_row.get("room_id"),
                                "publish_url": publish_url,
                                "play_url": play_url,
                                "whip_url": room_row.get("whip_url"),
                                "whep_url": room_row.get("whep_url"),
                                "stream_name": stream_name,
                                "owner_id": room_row.get("owner_id"),
                                "owner_name": room_row.get("owner_name"),
                                "owner_icon": room_row.get("owner_icon"),
                                "group_id": group_id,
                                "timestamp": time.time(),
                                "members": members,
                            }

                            app_logger.info(
                                f"[groups/by-teacher] 群组 {group_id} 有临时语音房间（数据库恢复），已添加到返回信息并恢复到内存"
                            )
                    except Exception as db_error:
                        app_logger.error(f"[groups/by-teacher] 从数据库查询临时语音房间失败 - group_id={group_id}, error={db_error}")

                if temp_room_info:
                    group_info["temp_room"] = temp_room_info

            if row.get("self_role") == 400:
                owner_groups.append(group_info)
            else:
                member_groups.append(group_info)

        return JSONResponse(
            {
                "data": {
                    "message": "查询成功",
                    "code": 200,
                    "owner_groups": owner_groups,
                    "member_groups": member_groups,
                    "total_count": len(results),
                    "owner_count": len(owner_groups),
                    "member_count": len(member_groups),
                }
            },
            status_code=200,
        )

    except mysql.connector.Error as e:
        app_logger.error(f"查询群组错误: {e}")
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        app_logger.error(f"查询群组时发生异常: {e}")
        import traceback

        traceback_str = traceback.format_exc()
        app_logger.error(traceback_str)
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"Database connection closed after get_groups_by_teacher attempt for {teacher_unique_id}.")


@router.get("/groups/search")
def search_groups(
    schoolid: str = Query(None, description="学校ID，可选参数"),
    group_id: str = Query(None, description="群组ID，与group_name二选一"),
    group_name: str = Query(None, description="群组名称，与group_id二选一，支持模糊查询"),
):
    """
    搜索群组
    根据 schoolid 和 group_id 或 group_name 搜索 groups 表
    - schoolid: 可选参数（如果不提供，则搜索所有学校）
    - group_id 或 group_name: 二选一，不会同时上传
    """
    print("=" * 80)
    print("[groups/search] ========== 收到搜索群组请求 ==========")
    print(f"[groups/search] 请求时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[groups/search] 请求参数:")
    print(f"[groups/search]   - schoolid: {schoolid}")
    print(f"[groups/search]   - group_id: {group_id}")
    print(f"[groups/search]   - group_name: {group_name}")
    app_logger.info("=" * 80)
    app_logger.info("[groups/search] ========== 收到搜索群组请求 ==========")
    app_logger.info(f"[groups/search] 请求参数 - schoolid: {schoolid}, group_id: {group_id}, group_name: {group_name}")

    if not schoolid:
        print("[groups/search] ⚠️  警告: 未提供 schoolid 参数，将搜索所有学校")
        app_logger.warning("[groups/search] 未提供 schoolid 参数，将搜索所有学校")

    if not group_id and not group_name:
        print("[groups/search] ❌ 错误: group_id 和 group_name 必须至少提供一个")
        app_logger.warning("[groups/search] group_id 和 group_name 必须至少提供一个")
        return JSONResponse({"data": {"message": "group_id 和 group_name 必须至少提供一个", "code": 400}}, status_code=400)

    if group_id and group_name:
        print("[groups/search] ❌ 错误: group_id 和 group_name 不能同时提供")
        app_logger.warning("[groups/search] group_id 和 group_name 不能同时提供")
        return JSONResponse({"data": {"message": "group_id 和 group_name 不能同时提供", "code": 400}}, status_code=400)

    print("[groups/search] 📊 开始连接数据库...")
    app_logger.info("[groups/search] 开始连接数据库...")
    connection = get_db_connection()
    if connection is None or not connection.is_connected():
        print("[groups/search] ❌ 错误: 数据库连接失败")
        app_logger.error(f"[groups/search] 数据库连接失败 for schoolid={schoolid}")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)
    print("[groups/search] ✅ 数据库连接成功")
    app_logger.info("[groups/search] 数据库连接成功")

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        if group_id:
            print(f"[groups/search] 🔍 根据 group_id 精确查询: {group_id}")
            app_logger.info(f"[groups/search] 根据 group_id 精确查询: {group_id}")
            if schoolid:
                sql = """
                    SELECT *
                    FROM `groups`
                    WHERE schoolid = %s AND group_id = %s
                """
                params = (schoolid, group_id)
            else:
                sql = """
                    SELECT *
                    FROM `groups`
                    WHERE group_id = %s
                """
                params = (group_id,)
        else:
            print(f"[groups/search] 🔍 根据 group_name 模糊查询: {group_name}")
            print(f"[groups/search]   - 原始 group_name: {repr(group_name)}")
            print(f"[groups/search]   - group_name 长度: {len(group_name) if group_name else 0}")
            app_logger.info(f"[groups/search] 根据 group_name 模糊查询: {group_name}")

            if schoolid:
                sql = """
                    SELECT *
                    FROM `groups`
                    WHERE schoolid = %s AND group_name LIKE %s
                """
                params = (schoolid, f"%{group_name}%")
            else:
                sql = """
                    SELECT *
                    FROM `groups`
                    WHERE group_name LIKE %s
                """
                params = (f"%{group_name}%",)

        print(f"[groups/search] 📝 执行SQL查询:")
        print(f"[groups/search]   SQL: {sql}")
        print(f"[groups/search]   参数: {params}")
        app_logger.info(f"[groups/search] 执行SQL: {sql}, 参数: {params}")

        cursor.execute(sql, params)
        groups = cursor.fetchall()

        print(f"[groups/search] ✅ 查询完成: 找到 {len(groups)} 个群组")
        app_logger.info(f"[groups/search] 查询完成: 找到 {len(groups)} 个群组")

        if len(groups) == 0 and group_name:
            print(f"[groups/search] ⚠️  未找到匹配的群组，尝试查看数据库中的实际数据...")
            app_logger.warning(f"[groups/search] 未找到匹配的群组，查询条件: schoolid={schoolid}, group_name={group_name}")
            debug_sql = "SELECT group_id, group_name, schoolid FROM `groups` WHERE group_name LIKE %s LIMIT 10"
            debug_params = (f"%{group_name}%",)
            cursor.execute(debug_sql, debug_params)
            debug_groups = cursor.fetchall()
            print(f"[groups/search] 🔍 调试查询（不限制schoolid）: 找到 {len(debug_groups)} 个包含 '{group_name}' 的群组")
            for idx, dg in enumerate(debug_groups):
                print(
                    f"[groups/search]   群组 {idx+1}: group_id={dg.get('group_id')}, group_name={dg.get('group_name')}, schoolid={dg.get('schoolid')}"
                )
            app_logger.info(f"[groups/search] 调试查询结果: {debug_groups}")

        for idx, group in enumerate(groups):
            print(
                f"[groups/search] 📋 处理第 {idx+1} 个群组: group_id={group.get('group_id')}, group_name={group.get('group_name')}, schoolid={group.get('schoolid')}"
            )
            for key, value in group.items():
                if isinstance(value, datetime.datetime):
                    group[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        result = {
            "data": {
                "message": "查询成功",
                "code": 200,
                "schoolid": schoolid,
                "search_key": group_id if group_id else group_name,
                "search_type": "group_id" if group_id else "group_name",
                "groups": groups,
                "count": len(groups),
            }
        }

        print(f"[groups/search] 📤 返回结果:")
        print(f"[groups/search]   - 找到群组数: {len(groups)}")
        print(f"[groups/search]   - schoolid: {schoolid}")
        print(f"[groups/search]   - 搜索关键词: {group_id if group_id else group_name}")
        print(f"[groups/search]   - 搜索类型: {'group_id' if group_id else 'group_name'}")
        app_logger.info(
            f"[groups/search] 返回结果: 找到 {len(groups)} 个群组, schoolid={schoolid}, search_key={group_id if group_id else group_name}"
        )
        print("=" * 80)

        return JSONResponse(result, status_code=200)

    except mysql.connector.Error as e:
        error_msg = f"搜索群组数据库错误: {e}"
        print(f"[groups/search] ❌ {error_msg}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/search] 错误堆栈:\n{traceback_str}")
        app_logger.error(f"[groups/search] {error_msg}\n{traceback_str}")
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        error_msg = f"搜索群组时发生异常: {e}"
        print(f"[groups/search] ❌ {error_msg}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/search] 错误堆栈:\n{traceback_str}")
        app_logger.error(f"[groups/search] {error_msg}\n{traceback_str}")
        return JSONResponse({"data": {"message": f"查询失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
            print("[groups/search] 🔒 游标已关闭")
        if connection and connection.is_connected():
            connection.close()
            print("[groups/search] 🔒 数据库连接已关闭")
        print("[groups/search] ========== 搜索群组请求处理完成 ==========")
        print("=" * 80)
        app_logger.info(f"[groups/search] Database connection closed after search groups attempt for schoolid={schoolid}.")


@router.post("/groups/join")
async def join_group(request: Request):
    """
    用户申请加入群组
    接收客户端发送的 group_id, user_id, user_name, reason
    将用户添加到 group_members 表中
    """
    print("=" * 80)
    print("[groups/join] 收到加入群组请求")

    try:
        data = await request.json()
        print(f"[groups/join] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")

        group_id = data.get("group_id")
        user_id = data.get("user_id")
        user_name = data.get("user_name")
        reason = data.get("reason")

        print(f"[groups/join] 解析结果 - group_id: {group_id}, user_id: {user_id}, user_name: {user_name}, reason: {reason}")

        # 参数验证
        if not group_id:
            print("[groups/join] 错误: 缺少 group_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 group_id"}, status_code=400)

        if not user_id:
            print("[groups/join] 错误: 缺少 user_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 user_id"}, status_code=400)

        print("[groups/join] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/join] 错误: 数据库连接失败")
            app_logger.error("[groups/join] 数据库连接失败")
            return JSONResponse({"code": 500, "message": "数据库连接失败"}, status_code=500)
        print("[groups/join] 数据库连接成功")

        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)

            # 1. 检查群组是否存在
            print(f"[groups/join] 检查群组 {group_id} 是否存在...")
            cursor.execute(
                "SELECT group_id, group_name, max_member_num, member_num FROM `groups` WHERE group_id = %s",
                (group_id,),
            )
            group_info = cursor.fetchone()

            if not group_info:
                print(f"[groups/join] 错误: 群组 {group_id} 不存在")
                return JSONResponse({"code": 404, "message": "群组不存在"}, status_code=404)

            print(f"[groups/join] 群组信息: {group_info}")
            max_member_num = group_info.get("max_member_num") if group_info.get("max_member_num") else 0
            member_num = group_info.get("member_num") if group_info.get("member_num") else 0

            # 检查群组是否已满
            if max_member_num > 0 and member_num >= max_member_num:
                print(f"[groups/join] 错误: 群组已满 (当前: {member_num}/{max_member_num})")
                return JSONResponse({"code": 400, "message": "群组已满，无法加入"}, status_code=400)

            # 2. 检查用户是否已经在群组中
            print(f"[groups/join] 检查用户 {user_id} 是否已在群组 {group_id} 中...")
            cursor.execute(
                "SELECT group_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id),
            )
            member_exists = cursor.fetchone()

            if member_exists:
                print(f"[groups/join] 用户 {user_id} 已在群组 {group_id} 中")
                return JSONResponse({"code": 400, "message": "您已经在该群组中"}, status_code=400)

            # 3. 先调用腾讯IM API添加成员
            tencent_sync_success = False
            tencent_error = None

            # 准备腾讯IM配置
            identifier_to_use = TENCENT_API_IDENTIFIER
            if not identifier_to_use:
                # 尝试从用户信息中获取identifier
                cursor.execute("SELECT id_number, phone FROM `users` WHERE user_id = %s", (user_id,))
                user_info = cursor.fetchone()
                if user_info:
                    identifier_to_use = resolve_tencent_identifier(
                        connection, id_number=user_info.get("id_number"), phone=user_info.get("phone")
                    )

            if identifier_to_use and TENCENT_API_SDK_APP_ID:
                try:
                    # 生成或使用配置的 UserSig
                    usersig_to_use: Optional[str] = None
                    if TENCENT_API_SECRET_KEY:
                        try:
                            usersig_to_use = generate_tencent_user_sig(identifier_to_use)
                            print(f"[groups/join] UserSig 生成成功")
                        except Exception as e:
                            print(f"[groups/join] UserSig 生成失败: {e}")
                            usersig_to_use = TENCENT_API_USER_SIG
                    else:
                        usersig_to_use = TENCENT_API_USER_SIG

                    if usersig_to_use:
                        # 构建腾讯IM添加群成员的URL
                        add_member_url = build_tencent_request_url(
                            identifier=identifier_to_use,
                            usersig=usersig_to_use,
                            path_override="v4/group_open_http_svc/add_group_member",
                        )

                        if add_member_url:
                            # 构建添加成员的payload
                            add_member_payload = {
                                "GroupId": group_id,
                                "MemberList": [{"Member_Account": user_id, "Role": "Member"}],
                                "Silence": 0,
                            }

                            print(f"[groups/join] 准备同步到腾讯IM - group_id={group_id}, user_id={user_id}")
                            app_logger.info(f"[groups/join] 准备同步到腾讯IM - group_id={group_id}, user_id={user_id}")

                            # 调用腾讯IM API
                            def _add_tencent_member() -> Dict[str, Any]:
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
                                    raw_body = e.read() if e.fp else b""
                                    text_body = raw_body.decode("utf-8", errors="replace")
                                    try:
                                        parsed_body = json.loads(text_body)
                                    except json.JSONDecodeError:
                                        parsed_body = None

                                    return {
                                        "status": "error",
                                        "http_status": e.code,
                                        "response": parsed_body or text_body,
                                        "error": f"HTTP {e.code}: {e.reason}",
                                    }
                                except Exception as e:
                                    return {"status": "error", "http_status": None, "error": str(e)}

                            tencent_result = await asyncio.to_thread(_add_tencent_member)

                            if tencent_result.get("status") == "success":
                                response_data = tencent_result.get("response")
                                if isinstance(response_data, dict):
                                    action_status = response_data.get("ActionStatus")
                                    error_code = response_data.get("ErrorCode")
                                    if action_status == "OK" and error_code == 0:
                                        tencent_sync_success = True
                                        print(f"[groups/join] 腾讯IM同步成功 - group_id={group_id}, user_id={user_id}")
                                        app_logger.info(f"[groups/join] 腾讯IM同步成功 - group_id={group_id}, user_id={user_id}")
                                    else:
                                        tencent_error = (
                                            f"腾讯IM返回错误: ErrorCode={error_code}, ErrorInfo={response_data.get('ErrorInfo')}"
                                        )
                                        print(f"[groups/join] {tencent_error}")
                                        app_logger.warning(f"[groups/join] {tencent_error}")
                                else:
                                    tencent_error = f"腾讯IM返回格式错误: {response_data}"
                                    print(f"[groups/join] {tencent_error}")
                                    app_logger.warning(f"[groups/join] {tencent_error}")
                            else:
                                tencent_error = tencent_result.get("error", "未知错误")
                                print(f"[groups/join] 腾讯IM API调用失败: {tencent_error}")
                                app_logger.warning(f"[groups/join] 腾讯IM API调用失败: {tencent_error}")
                        else:
                            tencent_error = "无法构建腾讯IM URL"
                            print(f"[groups/join] {tencent_error}")
                            app_logger.warning(f"[groups/join] {tencent_error}")
                    else:
                        tencent_error = "缺少可用的UserSig"
                        print(f"[groups/join] {tencent_error}")
                        app_logger.warning(f"[groups/join] {tencent_error}")
                except Exception as e:
                    tencent_error = f"调用腾讯IM API时发生异常: {str(e)}"
                    print(f"[groups/join] {tencent_error}")
                    app_logger.error(f"[groups/join] {tencent_error}")
                    import traceback

                    traceback_str = traceback.format_exc()
                    app_logger.error(f"[groups/join] 异常堆栈: {traceback_str}")
            else:
                tencent_error = "缺少腾讯IM配置（identifier或SDKAppID）"
                print(f"[groups/join] {tencent_error}")
                app_logger.warning(f"[groups/join] {tencent_error}")

            # 4. 插入新成员到数据库
            print(f"[groups/join] 插入新成员到群组 {group_id}...")
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            insert_member_sql = """
                INSERT INTO `group_members` (
                    group_id, user_id, user_name, self_role, join_time, msg_flag,
                    self_msg_flag, readed_seq, unread_num
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            insert_params = (
                group_id,
                user_id,
                user_name if user_name else None,
                200,
                current_time,
                0,
                0,
                0,
                0,
            )

            print(f"[groups/join] 插入参数: {insert_params}")
            cursor.execute(insert_member_sql, insert_params)
            affected_rows = cursor.rowcount
            lastrowid = cursor.lastrowid
            print(f"[groups/join] 插入成员完成, 影响行数: {affected_rows}, lastrowid: {lastrowid}")

            # 5. 更新群组的成员数量
            print(f"[groups/join] 更新群组 {group_id} 的成员数量...")
            cursor.execute("UPDATE `groups` SET member_num = member_num + 1 WHERE group_id = %s", (group_id,))
            print(f"[groups/join] 群组成员数量已更新")

            connection.commit()
            print(f"[groups/join] 事务提交成功")

            if not tencent_sync_success and tencent_error:
                app_logger.warning(f"[groups/join] 数据库操作成功，但腾讯IM同步失败: {tencent_error}")

            result = {
                "code": 200,
                "message": "成功加入群组",
                "data": {
                    "group_id": group_id,
                    "user_id": user_id,
                    "user_name": user_name,
                    "join_time": current_time,
                    "tencent_sync": {"success": tencent_sync_success, "error": tencent_error if not tencent_sync_success else None},
                },
            }

            print(f"[groups/join] 返回结果: {result}")
            print("=" * 80)

            return JSONResponse(result, status_code=200)

        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/join] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/join] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/join] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"数据库操作失败: {str(e)}"}, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"加入群组时发生异常: {e}"
            print(f"[groups/join] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/join] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/join] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"操作失败: {str(e)}"}, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/join] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/join] 数据库连接已关闭")
                app_logger.info("[groups/join] Database connection closed after join group attempt.")

    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/join] {error_msg}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/join] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/join] {error_msg}\n{traceback_str}")
        return JSONResponse({"code": 400, "message": "请求数据格式错误"}, status_code=400)
    finally:
        print("=" * 80)


@router.post("/groups/leave")
async def leave_group(request: Request):
    """
    用户退出群组
    接收客户端发送的 group_id, user_id
    从 group_members 表中删除该用户，并更新群组的成员数量
    """
    print("=" * 80)
    print("[groups/leave] 收到退出群组请求")

    # 打印请求头信息用于调试
    content_type = request.headers.get("content-type", "")
    content_length = request.headers.get("content-length", "")
    print(f"[groups/leave] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")

    try:
        # 解析请求体JSON数据
        try:
            body_bytes = await request.body()
            print(f"[groups/leave] 读取到请求体长度: {len(body_bytes)} 字节")

            if not body_bytes:
                print("[groups/leave] 错误: 请求体为空")
                return JSONResponse({"code": 400, "message": "请求体不能为空"}, status_code=400)

            try:
                data = json.loads(body_bytes.decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"[groups/leave] 错误: JSON解析失败 - {e}")
                print(f"[groups/leave] 请求体内容: {body_bytes.decode('utf-8', errors='ignore')}")
                return JSONResponse({"code": 400, "message": "请求数据格式错误，无法解析JSON"}, status_code=400)

        except ClientDisconnect:
            print("[groups/leave] 错误: 客户端断开连接")
            print(f"[groups/leave] 调试信息 - Content-Type: {content_type}, Content-Length: {content_length}")
            app_logger.warning("[groups/leave] 客户端在请求完成前断开连接")
            return JSONResponse({"code": 400, "message": "客户端断开连接，请检查请求数据是否正确发送"}, status_code=400)
        except Exception as e:
            print(f"[groups/leave] 读取请求体时发生异常: {type(e).__name__} - {e}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/leave] 错误堆栈: {traceback_str}")
            return JSONResponse({"code": 400, "message": f"读取请求数据失败: {str(e)}"}, status_code=400)

        print(f"[groups/leave] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")

        group_id = data.get("group_id")
        user_id = data.get("user_id")

        print(f"[groups/leave] 解析结果 - group_id: {group_id}, user_id: {user_id}")

        # 参数验证
        if not group_id:
            print("[groups/leave] 错误: 缺少 group_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 group_id"}, status_code=400)

        if not user_id:
            print("[groups/leave] 错误: 缺少 user_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 user_id"}, status_code=400)

        print("[groups/leave] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/leave] 错误: 数据库连接失败")
            app_logger.error("[groups/leave] 数据库连接失败")
            return JSONResponse({"code": 500, "message": "数据库连接失败"}, status_code=500)
        print("[groups/leave] 数据库连接成功")

        cursor = None
        try:
            cursor = connection.cursor(dictionary=True)

            # 1. 检查群组是否存在
            print(f"[groups/leave] 检查群组 {group_id} 是否存在...")
            cursor.execute("SELECT group_id, group_name, member_num FROM `groups` WHERE group_id = %s", (group_id,))
            group_info = cursor.fetchone()

            if not group_info:
                print(f"[groups/leave] 错误: 群组 {group_id} 不存在")
                return JSONResponse({"code": 404, "message": "群组不存在"}, status_code=404)

            print(f"[groups/leave] 群组信息: {group_info}")

            # 2. 检查用户是否在群组中
            print(f"[groups/leave] 检查用户 {user_id} 是否在群组 {group_id} 中...")
            cursor.execute(
                "SELECT group_id, user_id, self_role FROM `group_members` WHERE group_id = %s AND user_id = %s",
                (group_id, user_id),
            )
            member_info = cursor.fetchone()

            if not member_info:
                print(f"[groups/leave] 错误: 用户 {user_id} 不在群组 {group_id} 中")
                return JSONResponse({"code": 400, "message": "您不在该群组中"}, status_code=400)

            print(f"[groups/leave] 成员信息: {member_info}")
            self_role = member_info.get("self_role", 200)

            # 3. 检查是否是群主（self_role = 400 表示群主）
            if self_role == 400:
                print(f"[groups/leave] 警告: 用户 {user_id} 是群主，不允许直接退出")

            # 4. 从群组中删除该成员
            print(f"[groups/leave] 从群组 {group_id} 中删除用户 {user_id}...")
            cursor.execute("DELETE FROM `group_members` WHERE group_id = %s AND user_id = %s", (group_id, user_id))
            affected_rows = cursor.rowcount
            print(f"[groups/leave] 删除成员完成, 影响行数: {affected_rows}")

            if affected_rows == 0:
                print(f"[groups/leave] 警告: 删除操作未影响任何行")
                return JSONResponse({"code": 500, "message": "退出群组失败"}, status_code=500)

            # 5. 更新群组的成员数量（确保不会小于0）
            print(f"[groups/leave] 更新群组 {group_id} 的成员数量...")
            cursor.execute(
                "UPDATE `groups` SET member_num = CASE WHEN member_num > 0 THEN member_num - 1 ELSE 0 END WHERE group_id = %s",
                (group_id,),
            )
            print(f"[groups/leave] 群组成员数量已更新")

            connection.commit()
            print(f"[groups/leave] 事务提交成功")

            result = {"code": 200, "message": "成功退出群组", "data": {"group_id": group_id, "user_id": user_id}}

            print(f"[groups/leave] 返回结果: {result}")
            print("=" * 80)

            return JSONResponse(result, status_code=200)

        except mysql.connector.Error as e:
            connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/leave] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/leave] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/leave] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"数据库操作失败: {str(e)}"}, status_code=500)
        except Exception as e:
            connection.rollback()
            error_msg = f"退出群组时发生异常: {e}"
            print(f"[groups/leave] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/leave] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/leave] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"操作失败: {str(e)}"}, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/leave] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/leave] 数据库连接已关闭")
                app_logger.info("[groups/leave] Database connection closed after leave group attempt.")

    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/leave] {error_msg}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/leave] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/leave] {error_msg}\n{traceback_str}")
        return JSONResponse({"code": 400, "message": "请求数据格式错误"}, status_code=400)
    finally:
        print("=" * 80)


@router.post("/groups/invite")
async def invite_group_members(request: Request):
    """
    群主邀请成员加入群组
    接收客户端发送的 group_id 和 members 列表
    1. 调用腾讯接口邀请成员
    2. 邀请成功后，将相关信息插入数据库
    请求体 JSON:
    {
      "group_id": "群组ID",
      "members": [
        {
          "unique_member_id": "成员ID",
          "member_name": "成员名称",
          "group_role": 300
        }
      ]
    }
    """
    print("=" * 80)
    print("[groups/invite] 收到邀请成员请求")

    try:
        data = await request.json()
        print(f"[groups/invite] 原始数据: {json.dumps(data, ensure_ascii=False, indent=2)}")

        group_id = data.get("group_id")
        members = data.get("members", [])

        # 参数验证
        if not group_id:
            print("[groups/invite] 错误: 缺少 group_id")
            return JSONResponse({"code": 400, "message": "缺少必需参数 group_id"}, status_code=400)

        if not members or not isinstance(members, list):
            print("[groups/invite] 错误: 缺少或无效的 members")
            return JSONResponse({"code": 400, "message": "缺少必需参数 members 或 members 必须是数组"}, status_code=400)

        # 验证每个成员的必要字段
        for idx, member in enumerate(members):
            if not member.get("unique_member_id"):
                print(f"[groups/invite] 错误: 成员 {idx} 缺少 unique_member_id")
                return JSONResponse({"code": 400, "message": f"成员 {idx} 缺少必需参数 unique_member_id"}, status_code=400)

        print("[groups/invite] 开始连接数据库...")
        connection = get_db_connection()
        if connection is None or not connection.is_connected():
            print("[groups/invite] 错误: 数据库连接失败")
            app_logger.error("[groups/invite] 数据库连接失败")
            return JSONResponse({"code": 500, "message": "数据库连接失败"}, status_code=500)
        print("[groups/invite] 数据库连接成功")

        cursor = None
        try:
            # 开始事务（在开始时就启动，确保所有操作在一个事务中）
            connection.start_transaction()
            cursor = connection.cursor(dictionary=True)

            # 1. 检查群组是否存在
            print(f"[groups/invite] 检查群组 {group_id} 是否存在...")
            cursor.execute(
                "SELECT group_id, group_name, max_member_num, member_num FROM `groups` WHERE group_id = %s", (group_id,)
            )
            group_info = cursor.fetchone()

            if not group_info:
                print(f"[groups/invite] 错误: 群组 {group_id} 不存在")
                return JSONResponse({"code": 404, "message": "群组不存在"}, status_code=404)

            print(f"[groups/invite] 群组信息: {group_info}")
            max_member_num = group_info.get("max_member_num") if group_info.get("max_member_num") else 0
            member_num = group_info.get("member_num") if group_info.get("member_num") else 0

            # 检查群组是否已满
            if max_member_num > 0 and member_num + len(members) > max_member_num:
                print(f"[groups/invite] 错误: 群组已满 (当前: {member_num}, 最大: {max_member_num}, 邀请: {len(members)})")
                return JSONResponse(
                    {"code": 400, "message": f"群组已满，无法邀请 {len(members)} 个成员（当前: {member_num}/{max_member_num}）"},
                    status_code=400,
                )

            # 2. 检查哪些成员已经在群组中
            existing_members = []
            for member in members:
                unique_member_id = member.get("unique_member_id")
                cursor.execute(
                    "SELECT user_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                    (group_id, unique_member_id),
                )
                if cursor.fetchone():
                    existing_members.append(unique_member_id)

            if existing_members:
                print(f"[groups/invite] 警告: 以下成员已在群组中: {existing_members}")
                members = [m for m in members if m.get("unique_member_id") not in existing_members]
                if not members:
                    return JSONResponse({"code": 400, "message": "所有成员已在群组中"}, status_code=400)

            # 3. 调用腾讯接口邀请成员
            print(f"[groups/invite] 准备调用腾讯接口邀请 {len(members)} 个成员...")

            # 使用管理员账号作为 identifier（与群组同步保持一致）
            identifier_to_use = TENCENT_API_IDENTIFIER

            # 检查必需的配置
            if not TENCENT_API_SDK_APP_ID:
                print("[groups/invite] 错误: TENCENT_API_SDK_APP_ID 未配置")
                app_logger.error("[groups/invite] TENCENT_API_SDK_APP_ID 未配置")
                return JSONResponse({"code": 500, "message": "腾讯接口配置错误: 缺少 SDKAppID"}, status_code=500)

            if not identifier_to_use:
                print("[groups/invite] 错误: TENCENT_API_IDENTIFIER 未配置")
                app_logger.error("[groups/invite] TENCENT_API_IDENTIFIER 未配置")
                return JSONResponse({"code": 500, "message": "腾讯接口配置错误: 缺少 Identifier"}, status_code=500)

            # 尝试生成或使用配置的 UserSig（与群组同步逻辑一致）
            usersig_to_use: Optional[str] = None
            sig_error: Optional[str] = None
            if TENCENT_API_SECRET_KEY:
                try:
                    print(f"[groups/invite] 准备为管理员账号生成 UserSig: identifier={identifier_to_use}")
                    usersig_to_use = generate_tencent_user_sig(identifier_to_use)
                    print(f"[groups/invite] UserSig 生成成功，长度: {len(usersig_to_use) if usersig_to_use else 0}")
                    app_logger.info(f"为管理员账号 {identifier_to_use} 生成 UserSig 成功")
                except Exception as e:
                    sig_error = f"自动生成管理员 UserSig 失败: {e}"
                    print(f"[groups/invite] UserSig 生成失败: {sig_error}")
                    app_logger.error(sig_error)

            if not usersig_to_use:
                print(f"[groups/invite] 使用配置的 TENCENT_API_USER_SIG")
                usersig_to_use = TENCENT_API_USER_SIG

            if not usersig_to_use:
                error_message = "缺少可用的管理员 UserSig，无法调用腾讯接口。"
                print(f"[groups/invite] 错误: {error_message}")
                app_logger.error(f"[groups/invite] {error_message}")
                return JSONResponse({"code": 500, "message": error_message}, status_code=500)

            print(f"[groups/invite] 使用 identifier: {identifier_to_use}, SDKAppID: {TENCENT_API_SDK_APP_ID}")

            invite_url = build_tencent_request_url(
                identifier=identifier_to_use, usersig=usersig_to_use, path_override="v4/group_open_http_svc/add_group_member"
            )

            if not invite_url:
                print("[groups/invite] 错误: 无法构建腾讯接口 URL")
                app_logger.error("[groups/invite] 无法构建腾讯接口 URL")
                return JSONResponse({"code": 500, "message": "腾讯接口配置错误"}, status_code=500)

            if "sdkappid" not in invite_url:
                print(f"[groups/invite] 警告: URL 中缺少 sdkappid，完整 URL: {invite_url}")
                app_logger.warning(f"[groups/invite] URL 中缺少 sdkappid: {invite_url}")
                parsed_url = urllib.parse.urlparse(invite_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                query_params["sdkappid"] = [TENCENT_API_SDK_APP_ID]
                query_params["identifier"] = [identifier_to_use]
                query_params["usersig"] = [usersig_to_use]
                query_params["contenttype"] = ["json"]
                if "random" not in query_params:
                    query_params["random"] = [str(random.randint(1, 2**31 - 1))]
                new_query = urllib.parse.urlencode(query_params, doseq=True)
                invite_url = urllib.parse.urlunparse(parsed_url._replace(query=new_query))
                print(f"[groups/invite] 已手动添加参数，新 URL: {invite_url[:200]}...")

            member_list = []
            for member in members:
                member_entry = {"Member_Account": member.get("unique_member_id")}
                group_role = member.get("group_role")
                if group_role:
                    role_map = {300: "Admin", 200: "Member", 400: "Owner"}
                    if group_role in role_map:
                        member_entry["Role"] = role_map[group_role]
                member_list.append(member_entry)

            invite_payload = {"GroupId": group_id, "MemberList": member_list, "Silence": 0}

            print(f"[groups/invite] 腾讯接口 URL: {invite_url[:100]}...")
            print(f"[groups/invite] 邀请 payload: {json.dumps(invite_payload, ensure_ascii=False, indent=2)}")

            def _invite_tencent_members() -> Dict[str, Any]:
                headers = {"Content-Type": "application/json; charset=utf-8"}
                encoded_payload = json.dumps(invite_payload, ensure_ascii=False).encode("utf-8")
                request_obj = urllib.request.Request(url=invite_url, data=encoded_payload, headers=headers, method="POST")
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
                    app_logger.error(f"[groups/invite] 腾讯接口调用失败 (HTTP {e.code}): {body}")
                    return {"status": "error", "http_status": e.code, "error": body}
                except urllib.error.URLError as e:
                    app_logger.error(f"[groups/invite] 腾讯接口调用异常: {e}")
                    return {"status": "error", "http_status": None, "error": str(e)}
                except Exception as exc:
                    app_logger.exception(f"[groups/invite] 腾讯接口未知异常: {exc}")
                    return {"status": "error", "http_status": None, "error": str(exc)}

            tencent_result = await asyncio.to_thread(_invite_tencent_members)

            if tencent_result.get("status") != "success":
                error_msg = tencent_result.get("error", "腾讯接口调用失败")
                print(f"[groups/invite] 腾讯接口调用失败: {error_msg}")
                if connection and connection.is_connected():
                    connection.rollback()
                return JSONResponse({"code": 500, "message": f"邀请成员失败: {error_msg}"}, status_code=500)

            tencent_response = tencent_result.get("response", {})
            if isinstance(tencent_response, dict):
                action_status = tencent_response.get("ActionStatus")
                error_code = tencent_response.get("ErrorCode")
                error_info = tencent_response.get("ErrorInfo")

                if action_status != "OK" or error_code != 0:
                    print(f"[groups/invite] 腾讯接口返回错误: ErrorCode={error_code}, ErrorInfo={error_info}")
                    if connection and connection.is_connected():
                        connection.rollback()
                    return JSONResponse({"code": 500, "message": f"邀请成员失败: {error_info or '未知错误'}"}, status_code=500)

            print(f"[groups/invite] 腾讯接口调用成功")

            # 4. 邀请成功后，插入数据库（事务已在开始时启动）
            print(f"[groups/invite] 开始插入数据库...")

            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            inserted_count = 0
            failed_members = []

            for member in members:
                unique_member_id = member.get("unique_member_id")
                member_name = member.get("member_name", "")
                group_role = member.get("group_role", 200)

                cursor.execute(
                    "SELECT user_id FROM `group_members` WHERE group_id = %s AND user_id = %s",
                    (group_id, unique_member_id),
                )
                if cursor.fetchone():
                    print(f"[groups/invite] 成员 {unique_member_id} 已在群组中，跳过")
                    failed_members.append({"unique_member_id": unique_member_id, "reason": "已在群组中"})
                    continue

                try:
                    insert_member_sql = """
                        INSERT INTO `group_members` (
                            group_id, user_id, user_name, self_role, join_time, msg_flag,
                            self_msg_flag, readed_seq, unread_num
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    insert_params = (
                        group_id,
                        unique_member_id,
                        member_name if member_name else None,
                        group_role,
                        current_time,
                        0,
                        0,
                        0,
                        0,
                    )

                    cursor.execute(insert_member_sql, insert_params)
                    inserted_count += 1
                    print(f"[groups/invite] 成功插入成员: {unique_member_id}")

                except mysql.connector.Error as e:
                    print(f"[groups/invite] 插入成员 {unique_member_id} 失败: {e}")
                    failed_members.append({"unique_member_id": unique_member_id, "reason": f"数据库错误: {str(e)}"})

            if inserted_count > 0:
                cursor.execute("UPDATE `groups` SET member_num = member_num + %s WHERE group_id = %s", (inserted_count, group_id))
                print(f"[groups/invite] 群组成员数量已更新，新增 {inserted_count} 人")

            connection.commit()
            print(f"[groups/invite] 事务提交成功")

            result = {
                "code": 200,
                "message": "邀请成功",
                "data": {
                    "group_id": group_id,
                    "invited_count": inserted_count,
                    "total_requested": len(members),
                    "failed_members": failed_members if failed_members else None,
                },
            }

            print(f"[groups/invite] 返回结果: {result}")
            print("=" * 80)

            return JSONResponse(result, status_code=200)

        except mysql.connector.Error as e:
            if connection and connection.is_connected():
                connection.rollback()
            error_msg = f"数据库错误: {e}"
            print(f"[groups/invite] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/invite] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/invite] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"数据库操作失败: {str(e)}"}, status_code=500)
        except Exception as e:
            if connection and connection.is_connected():
                connection.rollback()
            error_msg = f"邀请成员时发生异常: {e}"
            print(f"[groups/invite] {error_msg}")
            import traceback

            traceback_str = traceback.format_exc()
            print(f"[groups/invite] 错误堆栈: {traceback_str}")
            app_logger.error(f"[groups/invite] {error_msg}\n{traceback_str}")
            return JSONResponse({"code": 500, "message": f"操作失败: {str(e)}"}, status_code=500)
        finally:
            if cursor:
                cursor.close()
                print("[groups/invite] 游标已关闭")
            if connection and connection.is_connected():
                connection.close()
                print("[groups/invite] 数据库连接已关闭")
                app_logger.info("[groups/invite] Database connection closed after invite members attempt.")

    except Exception as e:
        error_msg = f"解析请求数据时出错: {e}"
        print(f"[groups/invite] {error_msg}")
        import traceback

        traceback_str = traceback.format_exc()
        print(f"[groups/invite] 错误堆栈: {traceback_str}")
        app_logger.error(f"[groups/invite] {error_msg}\n{traceback_str}")
        return JSONResponse({"code": 400, "message": "请求数据格式错误"}, status_code=400)
    finally:
        print("=" * 80)



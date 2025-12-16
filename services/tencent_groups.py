import asyncio
import json
import os
import random
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from common import app_logger
from services.tencent_api import build_tencent_headers, build_tencent_request_url
from services.tencent_sig import generate_tencent_user_sig


TENCENT_API_IDENTIFIER = os.getenv("TENCENT_API_IDENTIFIER")
TENCENT_API_USER_SIG = os.getenv("TENCENT_API_USER_SIG")
TENCENT_API_SECRET_KEY = os.getenv("TENCENT_API_SECRET_KEY")
TENCENT_API_TIMEOUT = float(os.getenv("TENCENT_API_TIMEOUT", "10"))


def normalize_tencent_group_type(raw_type: Optional[str]) -> str:
    default_type = "ChatRoom"
    if not raw_type:
        return default_type

    mapping = {
        "private": "Private",
        "public": "Public",
        "chatroom": "ChatRoom",
        "meeting": "ChatRoom",
        "meetinggroup": "ChatRoom",
        "会议": "ChatRoom",
        "会议群": "ChatRoom",
        "avchatroom": "AVChatRoom",
        "bchatroom": "BChatRoom",
        "community": "Community",
        "work": "Work",
        "class": "Work",
        "group": "Work",
    }

    normalized_key = str(raw_type).strip().lower()
    return mapping.get(normalized_key, default_type)


def normalize_tencent_group_id(group_id: Optional[str]) -> Optional[str]:
    """
    清理群组ID，移除腾讯IM不允许的 @TGS# 前缀。
    腾讯 REST API 不允许群组ID包含 @TGS# 前缀，需要移除。
    """
    if not group_id:
        return group_id

    group_id_str = str(group_id).strip()
    if group_id_str.startswith("@TGS#"):
        group_id_str = group_id_str[5:]

    return group_id_str if group_id_str else None


async def notify_tencent_group_sync(user_id: str, groups: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    将同步到本地数据库的群组数据推送到腾讯 REST API。
    注意：腾讯 REST API 要求使用管理员账号作为 identifier，而不是普通用户账号。
    """
    print(f"[notify_tencent_group_sync] 函数被调用: user_id={user_id}, groups数量={len(groups) if groups else 0}")
    app_logger.info(f"notify_tencent_group_sync 被调用: user_id={user_id}, groups数量={len(groups) if groups else 0}")

    if not groups:
        return {"status": "skipped", "reason": "empty_groups"}

    admin_identifier = TENCENT_API_IDENTIFIER
    print(f"[notify_tencent_group_sync] TENCENT_API_IDENTIFIER 值: {admin_identifier}")
    app_logger.info(f"TENCENT_API_IDENTIFIER 环境变量值: {admin_identifier}")

    if not admin_identifier:
        error_message = "缺少腾讯 REST API 管理员账号配置 (TENCENT_API_IDENTIFIER)，已跳过同步。"
        app_logger.error(error_message)
        return {"status": "error", "http_status": None, "error": error_message}

    identifier_to_use = str(admin_identifier) if admin_identifier else None
    print(f"[notify_tencent_group_sync] 最终使用的 identifier: {identifier_to_use}, 类型: {type(identifier_to_use)}")
    app_logger.info(f"群组同步使用管理员账号作为 identifier: {identifier_to_use} (原始 user_id: {user_id})")

    usersig_to_use: Optional[str] = None
    sig_error: Optional[str] = None
    if TENCENT_API_SECRET_KEY:
        try:
            print(
                f"[notify_tencent_group_sync] 准备为管理员账号生成 UserSig: identifier={identifier_to_use}, type={type(identifier_to_use)}"
            )
            usersig_to_use = generate_tencent_user_sig(identifier_to_use)
            print(f"[notify_tencent_group_sync] UserSig 生成成功，长度: {len(usersig_to_use) if usersig_to_use else 0}")
            app_logger.info(f"为管理员账号 {identifier_to_use} 生成 UserSig 成功")
        except Exception as e:
            sig_error = f"自动生成管理员 UserSig 失败: {e}"
            print(f"[notify_tencent_group_sync] UserSig 生成失败: {sig_error}")
            app_logger.error(sig_error)

    if not usersig_to_use:
        print(f"[notify_tencent_group_sync] 使用配置的 TENCENT_API_USER_SIG")
        usersig_to_use = TENCENT_API_USER_SIG

    if not usersig_to_use:
        error_message = "缺少可用的管理员 UserSig，已跳过腾讯 REST API 同步。"
        app_logger.error(error_message)
        return {"status": "error", "http_status": None, "error": error_message}

    url = build_tencent_request_url(identifier=identifier_to_use, usersig=usersig_to_use)
    if not url:
        msg = "腾讯 REST API 未配置，跳过同步"
        app_logger.warning(msg)
        return {"status": "skipped", "reason": "missing_configuration", "message": msg}

    if sig_error:
        masked_error = sig_error.replace(usersig_to_use or "", "***")
        app_logger.warning(masked_error)

    def _prepare_group_for_tencent(group: Dict[str, Any]) -> Dict[str, Any]:
        prepared = dict(group)
        group_type = prepared.get("group_type") or prepared.get("Type")
        normalized_type = normalize_tencent_group_type(group_type)
        prepared["group_type"] = normalized_type
        prepared["Type"] = normalized_type
        prepared["Name"] = prepared.get("Name") or prepared.get("group_name") or prepared.get("name")
        if not prepared["Name"]:
            prepared["Name"] = f"group_{prepared.get('group_id') or random.randint(1, 2**31 - 1)}"

        owner = prepared.get("Owner_Account") or prepared.get("owner_identifier") or user_id
        if owner:
            prepared["Owner_Account"] = owner

        return prepared

    payload_groups = [_prepare_group_for_tencent(group) for group in groups]
    app_logger.info(f"Tencent REST API payload preview: {payload_groups}")

    def validate_and_log_url(current_url: str) -> Dict[str, Any]:
        parsed_url = urllib.parse.urlparse(current_url)
        query_dict = urllib.parse.parse_qs(parsed_url.query, keep_blank_values=True)
        sdkappid_values = query_dict.get("sdkappid", [])
        if not sdkappid_values or not sdkappid_values[0]:
            error_message = "腾讯 REST API 请求 URL 缺少 sdkappid，已跳过同步。"
            app_logger.error(error_message + f" URL: {current_url}")
            return {"error": error_message}

        def mask_value(value: str, keep: int = 4) -> str:
            if not value:
                return value
            if len(value) <= keep:
                return "*" * len(value)
            return value[:keep] + "*" * (len(value) - keep)

        masked_query = {
            key: [mask_value(val[0]) if key in {"usersig", "identifier", "Authorization"} else val[0]]
            for key, val in query_dict.items()
        }
        app_logger.info(f"Tencent REST API 请求 URL: {parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}")
        app_logger.info(f"Tencent REST API 请求 Query 参数: {masked_query}")
        return {"query": query_dict}

    def build_group_payload(group: Dict[str, Any]) -> Dict[str, Any]:
        raw_group_id = group.get("GroupId") or group.get("group_id")
        cleaned_group_id = normalize_tencent_group_id(raw_group_id)

        if raw_group_id and raw_group_id != cleaned_group_id:
            app_logger.info(f"群组ID已清理: 原始ID='{raw_group_id}' -> 清理后ID='{cleaned_group_id}'")

        payload: Dict[str, Any] = {
            "Owner_Account": group.get("Owner_Account"),
            "Type": normalize_tencent_group_type(group.get("Type") or group.get("group_type")),
            "GroupId": cleaned_group_id,
            "Name": group.get("Name"),
        }

        optional_fields = {
            "Introduction": ["introduction", "Introduction"],
            "Notification": ["notification", "Notification"],
            "FaceUrl": ["face_url", "FaceUrl"],
            "ApplyJoinOption": ["add_option", "ApplyJoinOption"],
            "MaxMemberCount": ["max_member_num", "MaxMemberCount"],
            "AppDefinedData": ["AppDefinedData", "app_defined_data"],
        }

        for target_key, source_keys in optional_fields.items():
            for source_key in source_keys:
                value = group.get(source_key)
                if value not in (None, "", []):
                    payload[target_key] = value
                    break

        def convert_role_to_tencent(role_value):
            if role_value is None:
                return None
            if isinstance(role_value, str):
                role_value = role_value.strip()
                if role_value.upper() in ["OWNER", "400"]:
                    return "Owner"
                if role_value.upper() in ["ADMIN", "300"]:
                    return "Admin"
                return "Member"
            if isinstance(role_value, (int, float)):
                if role_value == 400:
                    return "Owner"
                if role_value == 300:
                    return "Admin"
                return "Member"
            return "Member"

        member_list = []
        member_info = group.get("member_info")
        if isinstance(member_info, dict):
            member_account = member_info.get("user_id") or member_info.get("Member_Account")
            if member_account:
                member_entry = {"Member_Account": member_account}
                role = member_info.get("self_role") or member_info.get("Role")
                tencent_role = convert_role_to_tencent(role)
                if tencent_role:
                    member_entry["Role"] = tencent_role
                member_list.append(member_entry)

        members = group.get("members") or group.get("MemberList")
        if isinstance(members, list):
            for member in members:
                if not isinstance(member, dict):
                    continue
                member_account = member.get("user_id") or member.get("Member_Account")
                if member_account:
                    entry = {"Member_Account": member_account}
                    role = member.get("self_role") or member.get("Role") or member.get("group_role")
                    tencent_role = convert_role_to_tencent(role)
                    if tencent_role:
                        entry["Role"] = tencent_role
                    member_list.append(entry)

        if member_list:
            payload["MemberList"] = member_list
        owner_account = payload.get("Owner_Account")
        if owner_account:
            owner_present = any(m.get("Member_Account") == owner_account for m in member_list)
            if not owner_present:
                payload.setdefault("MemberList", []).append({"Member_Account": owner_account, "Role": "Owner"})

        return payload

    headers = build_tencent_headers()

    def send_http_request(current_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(url=current_url, data=encoded_payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=TENCENT_API_TIMEOUT) as response:
                raw_body = response.read()
                text_body = raw_body.decode("utf-8", errors="replace")
                try:
                    parsed_body = json.loads(text_body)
                except json.JSONDecodeError:
                    parsed_body = None
                return {"status": "success", "http_status": response.status, "response": parsed_body or text_body}
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            app_logger.error(f"Tencent REST API 同步失败 (HTTP {e.code}): {body}")
            return {"status": "error", "http_status": e.code, "error": body}
        except urllib.error.URLError as e:
            app_logger.error(f"Tencent REST API 调用异常: {e}")
            return {"status": "error", "http_status": None, "error": str(e)}
        except Exception as exc:
            app_logger.exception(f"Tencent REST API 未知异常: {exc}")
            return {"status": "error", "http_status": None, "error": str(exc)}

    def build_update_group_payload(group_payload: Dict[str, Any]) -> Dict[str, Any]:
        update_payload = {"GroupId": group_payload.get("GroupId"), "Name": group_payload.get("Name")}
        optional_fields = {
            "Introduction": group_payload.get("Introduction"),
            "Notification": group_payload.get("Notification"),
            "FaceUrl": group_payload.get("FaceUrl"),
            "MaxMemberCount": group_payload.get("MaxMemberCount"),
            "ApplyJoinOption": group_payload.get("ApplyJoinOption"),
        }
        for key, value in optional_fields.items():
            if value is not None:
                update_payload[key] = value
        return update_payload

    def send_group_welcome_message(group_payload: Dict[str, Any]) -> None:
        group_id = group_payload.get("GroupId")
        if not group_id:
            app_logger.warning("send_group_welcome_message: 缺少 GroupId，跳过发送欢迎消息")
            return

        group_name = group_payload.get("Name") or group_payload.get("group_name") or f"{group_id}"
        welcome_text = f"欢迎大家来到{group_name}里面"

        message_url = build_tencent_request_url(
            identifier=identifier_to_use, usersig=usersig_to_use, path_override="v4/group_open_http_svc/send_group_msg"
        )
        if not message_url:
            app_logger.error(f"[send_group_welcome_message] 构建 send_group_msg URL 失败，group_id={group_id}")
            print(f"[send_group_welcome_message] FAILED -> url missing, group_id={group_id}")
            return

        random_value = random.randint(1, 2**31 - 1)
        message_payload: Dict[str, Any] = {
            "GroupId": group_id,
            "Random": random_value,
            "From_Account": identifier_to_use,
            "MsgBody": [{"MsgType": "TIMTextElem", "MsgContent": {"Text": welcome_text}}],
        }

        welcome_result = send_http_request(message_url, message_payload)
        app_logger.info(f"[send_group_welcome_message] 响应: {welcome_result}")

        if welcome_result.get("status") == "success" and isinstance(welcome_result.get("response"), dict):
            resp = welcome_result.get("response")
            action_status = resp.get("ActionStatus")
            if action_status == "OK":
                print(f"[send_group_welcome_message] SUCCESS -> group_id={group_id}")
                app_logger.info(f"[send_group_welcome_message] 群 {group_id} 欢迎消息发送成功 resp={resp}")
            else:
                error_info = resp.get("ErrorInfo")
                error_code = resp.get("ErrorCode")
                print(f"[send_group_welcome_message] FAIL -> group_id={group_id}, error={error_info}, code={error_code}")
                app_logger.warning(
                    f"[send_group_welcome_message] 群 {group_id} 欢迎消息失败 code={error_code}, info={error_info}, resp={resp}"
                )
        else:
            print(f"[send_group_welcome_message] REQUEST FAIL -> group_id={group_id}, error={welcome_result.get('error')}")
            app_logger.error(f"[send_group_welcome_message] 群 {group_id} 欢迎消息请求失败: {welcome_result}")

    def send_single_group(group_payload: Dict[str, Any]) -> Dict[str, Any]:
        group_id = group_payload.get("GroupId", "unknown")
        current_url = build_tencent_request_url(identifier=identifier_to_use, usersig=usersig_to_use)
        if not current_url:
            return {"status": "error", "http_status": None, "error": "腾讯 REST API 未配置有效 URL"}

        validation = validate_and_log_url(current_url)
        if "error" in validation:
            return {"status": "error", "http_status": None, "error": validation["error"]}

        parsed_url = urllib.parse.urlparse(current_url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        actual_identifier = query_params.get("identifier", [None])[0]
        app_logger.info(f"实际使用的 identifier (从 URL 提取): {actual_identifier}, 期望的管理员账号: {identifier_to_use}")

        result = send_http_request(current_url, group_payload)

        if result.get("status") == "success" and isinstance(result.get("response"), dict):
            parsed_body = result.get("response")
            action_status = parsed_body.get("ActionStatus")
            error_code = parsed_body.get("ErrorCode")
            error_info = parsed_body.get("ErrorInfo")
            app_logger.info(f"[send_single_group] import_group 响应 group_id={group_id}: {parsed_body}")
            if action_status == "OK":
                send_group_welcome_message(group_payload)
                return result
            if action_status == "FAIL" and error_code == 10021:
                # 群组已存在 -> 改走更新 API
                if "/import_group" in current_url:
                    update_path = current_url.replace("/import_group", "/modify_group_base_info")
                elif "/group_open_http_svc/import_group" in current_url:
                    update_path = current_url.replace(
                        "/group_open_http_svc/import_group", "/group_open_http_svc/modify_group_base_info"
                    )
                else:
                    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                    query_str = parsed_url.query
                    update_path = f"{base_url}/v4/group_open_http_svc/modify_group_base_info" + (
                        f"?{query_str}" if query_str else ""
                    )

                update_payload = build_update_group_payload(group_payload)
                update_result = send_http_request(update_path, update_payload)

                if update_result.get("status") == "success" and isinstance(update_result.get("response"), dict):
                    update_body = update_result.get("response")
                    update_action_status = update_body.get("ActionStatus")
                    if update_action_status == "OK":
                        return update_result
                    return update_result

                return result

            error_message = f"腾讯 API 错误: ErrorCode={error_code}, ErrorInfo={error_info}"
            app_logger.error(f"[send_single_group] {error_message}, group_id={group_id}, identifier={actual_identifier}")
            result["status"] = "error"
            result["error"] = error_message
            result["error_code"] = error_code
            result["error_info"] = error_info
            return result

        return result

    loop = asyncio.get_running_loop()
    tasks = []
    for group in payload_groups:
        group_payload = build_group_payload(group)
        task = loop.run_in_executor(None, send_single_group, group_payload)
        tasks.append(task)

    group_results = await asyncio.gather(*tasks)

    success_count = sum(1 for result in group_results if result.get("status") == "success")
    error_count = len(group_results) - success_count
    overall_status = "success" if error_count == 0 else ("partial" if success_count > 0 else "error")

    return {"status": overall_status, "success_count": success_count, "error_count": error_count, "results": group_results}



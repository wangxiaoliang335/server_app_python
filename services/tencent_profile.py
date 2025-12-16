import asyncio
import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from common import app_logger
from services.tencent_api import build_tencent_headers, build_tencent_request_url
from services.tencent_sig import generate_tencent_user_sig


TENCENT_API_USER_SIG = os.getenv("TENCENT_API_USER_SIG")
TENCENT_API_SECRET_KEY = os.getenv("TENCENT_API_SECRET_KEY")
TENCENT_API_TIMEOUT = float(os.getenv("TENCENT_API_TIMEOUT", "10"))
TENCENT_PROFILE_API_URL = os.getenv("TENCENT_PROFILE_API_URL")
TENCENT_PROFILE_API_PATH = os.getenv("TENCENT_PROFILE_API_PATH", "v4/profile/portrait_set")


async def notify_tencent_user_profile(
    identifier: str, *, name: Optional[str] = None, avatar_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    同步腾讯 IM 用户资料（昵称/头像）。
    - identifier: 目标用户的 IM Identifier
    - name/avatar_url: 二者至少一个
    """
    if not identifier:
        return {"status": "error", "error": "缺少腾讯用户 Identifier"}

    profile_items: List[Dict[str, Any]] = []
    if name:
        profile_items.append({"Tag": "Tag_Profile_IM_Nick", "Value": name})
    if avatar_url:
        profile_items.append({"Tag": "Tag_Profile_IM_Image", "Value": avatar_url})

    if not profile_items:
        return {"status": "skipped", "reason": "empty_profile_items"}

    usersig_to_use: Optional[str] = None
    sig_error: Optional[str] = None
    if TENCENT_API_SECRET_KEY:
        try:
            usersig_to_use = generate_tencent_user_sig(identifier)
        except Exception as e:
            sig_error = f"自动生成用户 UserSig 失败: {e}"
            app_logger.error(sig_error)

    if not usersig_to_use:
        usersig_to_use = TENCENT_API_USER_SIG

    if not usersig_to_use:
        error_message = "缺少可用的 UserSig，已跳过腾讯用户资料同步。"
        app_logger.error(error_message)
        return {"status": "error", "http_status": None, "error": error_message}

    url = build_tencent_request_url(
        identifier=identifier,
        usersig=usersig_to_use,
        url_override=TENCENT_PROFILE_API_URL,
        path_override=TENCENT_PROFILE_API_PATH,
    )
    if not url:
        msg = "腾讯用户资料接口未配置，跳过同步"
        app_logger.warning(msg)
        return {"status": "skipped", "reason": "missing_configuration", "message": msg}

    headers = build_tencent_headers()
    payload = {"From_Account": identifier, "ProfileItem": profile_items}

    if sig_error:
        app_logger.warning(sig_error)

    def _send_request() -> Dict[str, Any]:
        encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url=url, data=encoded_payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=TENCENT_API_TIMEOUT) as response:
                raw_body = response.read()
                text_body = raw_body.decode("utf-8", errors="replace")
                try:
                    parsed_body = json.loads(text_body)
                except json.JSONDecodeError:
                    parsed_body = None
                result = {"status": "success", "http_status": response.status, "response": parsed_body or text_body}
                app_logger.info(f"Tencent 用户资料同步成功: {result}")
                return result
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            app_logger.error(f"Tencent 用户资料同步失败 (HTTP {e.code}): {body}")
            return {"status": "error", "http_status": e.code, "error": body}
        except urllib.error.URLError as e:
            app_logger.error(f"Tencent 用户资料接口调用异常: {e}")
            return {"status": "error", "http_status": None, "error": str(e)}
        except Exception as exc:
            app_logger.exception(f"Tencent 用户资料接口未知异常: {exc}")
            return {"status": "error", "http_status": None, "error": str(exc)}

    return await asyncio.to_thread(_send_request)

import asyncio
import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from common import app_logger
from services.tencent_api import build_tencent_headers, build_tencent_request_url
from services.tencent_sig import generate_tencent_user_sig


TENCENT_API_USER_SIG = os.getenv("TENCENT_API_USER_SIG")
TENCENT_API_SECRET_KEY = os.getenv("TENCENT_API_SECRET_KEY")
TENCENT_API_TIMEOUT = float(os.getenv("TENCENT_API_TIMEOUT", "10"))
TENCENT_PROFILE_API_URL = os.getenv("TENCENT_PROFILE_API_URL")
TENCENT_PROFILE_API_PATH = os.getenv("TENCENT_PROFILE_API_PATH", "v4/profile/portrait_set")


async def notify_tencent_user_profile(
    identifier: str, *, name: Optional[str] = None, avatar_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    同步腾讯 IM 用户资料（昵称/头像）。
    返回结构尽量保持与历史实现一致。
    """
    if not identifier:
        return {"status": "error", "error": "缺少腾讯用户 Identifier"}

    profile_items: List[Dict[str, Any]] = []
    if name:
        profile_items.append({"Tag": "Tag_Profile_IM_Nick", "Value": name})
    if avatar_url:
        profile_items.append({"Tag": "Tag_Profile_IM_Image", "Value": avatar_url})

    if not profile_items:
        return {"status": "skipped", "reason": "empty_profile_items"}

    usersig_to_use: Optional[str] = None
    sig_error: Optional[str] = None
    if TENCENT_API_SECRET_KEY:
        try:
            usersig_to_use = generate_tencent_user_sig(identifier)
        except Exception as e:
            sig_error = f"自动生成用户 UserSig 失败: {e}"
            app_logger.error(sig_error)

    if not usersig_to_use:
        usersig_to_use = TENCENT_API_USER_SIG

    if not usersig_to_use:
        error_message = "缺少可用的 UserSig，已跳过腾讯用户资料同步。"
        app_logger.error(error_message)
        return {"status": "error", "error": error_message}

    url = build_tencent_request_url(
        identifier=identifier,
        usersig=usersig_to_use,
        url_override=TENCENT_PROFILE_API_URL,
        path_override=TENCENT_PROFILE_API_PATH,
    )
    if not url:
        msg = "腾讯用户资料接口未配置，跳过同步"
        app_logger.warning(msg)
        return {"status": "skipped", "reason": "missing_configuration", "message": msg}

    headers = build_tencent_headers()
    payload = {"From_Account": identifier, "ProfileItem": profile_items}

    if sig_error:
        masked_error = sig_error.replace(usersig_to_use or "", "***")
        app_logger.warning(masked_error)

    def _send_request() -> Dict[str, Any]:
        encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(url=url, data=encoded_payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=TENCENT_API_TIMEOUT) as response:
                raw_body = response.read()
                text_body = raw_body.decode("utf-8", errors="replace")
                try:
                    parsed_body = json.loads(text_body)
                except json.JSONDecodeError:
                    parsed_body = None

                result = {"status": "success", "http_status": response.status, "response": parsed_body or text_body}
                app_logger.info(f"Tencent 用户资料同步成功: {result}")
                return result
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            app_logger.error(f"Tencent 用户资料同步失败 (HTTP {e.code}): {body}")
            return {"status": "error", "http_status": e.code, "error": body}
        except urllib.error.URLError as e:
            app_logger.error(f"Tencent 用户资料接口调用异常: {e}")
            return {"status": "error", "http_status": None, "error": str(e)}
        except Exception as exc:
            app_logger.exception(f"Tencent 用户资料接口未知异常: {exc}")
            return {"status": "error", "http_status": None, "error": str(exc)}

    return await asyncio.to_thread(_send_request)



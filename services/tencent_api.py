import os
import random
import urllib.parse
from typing import Dict, Optional

from common import app_logger


TENCENT_API_URL = os.getenv("TENCENT_API_URL")
TENCENT_API_BASE_URL = os.getenv("TENCENT_API_BASE_URL")
TENCENT_API_PATH = os.getenv("TENCENT_API_PATH")
TENCENT_API_SDK_APP_ID = os.getenv("TENCENT_API_SDK_APP_ID")
TENCENT_API_IDENTIFIER = os.getenv("TENCENT_API_IDENTIFIER")
TENCENT_API_USER_SIG = os.getenv("TENCENT_API_USER_SIG")
TENCENT_API_TOKEN = os.getenv("TENCENT_API_TOKEN")


def build_tencent_request_url(
    identifier: Optional[str] = None,
    usersig: Optional[str] = None,
    *,
    url_override: Optional[str] = None,
    path_override: Optional[str] = None,
    base_override: Optional[str] = None,
) -> Optional[str]:
    """
    生成腾讯 REST API 的完整请求 URL。
    优先使用 TENCENT_API_URL，其次使用 base + path + query 参数。
    """
    if url_override is not None:
        selected_url = url_override
    elif path_override is not None:
        selected_url = None
    else:
        selected_url = TENCENT_API_URL

    selected_base = base_override or TENCENT_API_BASE_URL
    selected_path = path_override if path_override is not None else TENCENT_API_PATH

    effective_identifier = identifier or TENCENT_API_IDENTIFIER
    effective_usersig = usersig or TENCENT_API_USER_SIG

    extra_query: Dict[str, str] = {}

    if selected_url:
        parsed = urllib.parse.urlparse(selected_url)
        if parsed.scheme and parsed.netloc:
            existing_query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

            def pick_single(values):
                if isinstance(values, list):
                    return values[0] if values else ""
                return values

            normalized_query: Dict[str, str] = {k: pick_single(v) for k, v in existing_query.items()}

            def ensure_query_param(key: str, value: Optional[str], force: bool = False):
                if value is not None:
                    if force or (key not in normalized_query or not normalized_query[key]):
                        normalized_query[key] = value

            ensure_query_param("sdkappid", TENCENT_API_SDK_APP_ID)
            # 强制覆盖 identifier 和 usersig，确保使用传入的值
            ensure_query_param("identifier", effective_identifier, force=True)
            ensure_query_param("usersig", effective_usersig, force=True)
            ensure_query_param("contenttype", "json")
            if "random" not in normalized_query or not normalized_query["random"]:
                normalized_query["random"] = str(random.randint(1, 2**31 - 1))

            if "sdkappid" not in normalized_query or not normalized_query["sdkappid"]:
                app_logger.error("腾讯 REST API URL 缺少 sdkappid 且未配置 TENCENT_API_SDK_APP_ID，无法构建完整 URL。")
                return None

            rebuilt_query = urllib.parse.urlencode(normalized_query)
            rebuilt_url = urllib.parse.urlunparse(parsed._replace(query=rebuilt_query))
            return rebuilt_url

        if parsed.path:
            computed_path = parsed.path.lstrip("/")
            if parsed.scheme and not parsed.netloc:
                combined = parsed.scheme
                if computed_path:
                    combined = f"{parsed.scheme}/{computed_path}"
                selected_path = combined
            else:
                selected_path = parsed.path
        if parsed.netloc and not selected_base and parsed.scheme:
            selected_base = f"{parsed.scheme}://{parsed.netloc}"
        if parsed.query:
            for key, values in urllib.parse.parse_qs(parsed.query, keep_blank_values=True).items():
                if values:
                    extra_query[key] = values[0]

    if not selected_base and TENCENT_API_URL:
        parsed_base_source = urllib.parse.urlparse(TENCENT_API_URL)
        if parsed_base_source.scheme and parsed_base_source.netloc:
            selected_base = f"{parsed_base_source.scheme}://{parsed_base_source.netloc}"

    if not selected_base:
        app_logger.error(
            "构建腾讯 REST API URL 失败：缺少 base URL。"
            f" selected_url={selected_url}, selected_base={selected_base}, selected_path={selected_path}"
        )
        return None

    path = (selected_path or "").strip("/")
    base = selected_base.rstrip("/")
    url = f"{base}/{path}" if path else base

    if not (TENCENT_API_SDK_APP_ID and effective_identifier and effective_usersig):
        # 缺少拼装 query 所需的参数，则直接返回 base/path
        return url

    query_params = {
        "sdkappid": TENCENT_API_SDK_APP_ID,
        "identifier": effective_identifier,
        "usersig": effective_usersig,
        "random": random.randint(1, 2**31 - 1),
        "contenttype": "json",
    }
    for key, value in extra_query.items():
        query_params.setdefault(key, value)

    final_url = f"{url}?{urllib.parse.urlencode(query_params)}"
    if not final_url.lower().startswith(("http://", "https://")):
        app_logger.error(f"构建腾讯 REST API URL 失败，结果缺少协议: {final_url}")
        return None
    app_logger.debug(f"构建腾讯 REST API URL: base={base}, path={path}, final={final_url}")
    return final_url


def build_tencent_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {"Content-Type": "application/json; charset=utf-8"}
    if TENCENT_API_TOKEN:
        headers["Authorization"] = f"Bearer {TENCENT_API_TOKEN}"

    sanitized_headers: Dict[str, str] = {}
    for key, value in headers.items():
        try:
            value.encode("latin-1")
            sanitized_headers[key] = value
        except UnicodeEncodeError:
            app_logger.warning(f"Tencent REST API header {key} 包含非 Latin-1 字符，已跳过该字段。")
    return sanitized_headers


def resolve_tencent_identifier(connection, *, id_number: Optional[str] = None, phone: Optional[str] = None) -> Optional[str]:
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        if id_number:
            cursor.execute("SELECT teacher_unique_id FROM ta_teacher WHERE id_card = %s", (id_number,))
            row = cursor.fetchone()
            if row:
                identifier = row.get("teacher_unique_id")
                if identifier:
                    return identifier
        if phone:
            cursor.execute("SELECT teacher_unique_id FROM ta_teacher WHERE phone = %s", (phone,))
            row = cursor.fetchone()
            if row:
                identifier = row.get("teacher_unique_id")
                if identifier:
                    return identifier
    except Exception as e:
        app_logger.error(f"解析腾讯 Identifier 时发生错误: {e}")
    finally:
        if cursor:
            cursor.close()
    return id_number or phone



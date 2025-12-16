import base64
import hashlib
import hmac
import json
import os
import time
import zlib


def generate_tencent_user_sig(identifier: str, expire: int = 86400) -> str:
    """
    生成腾讯 IM UserSig。

    依赖环境变量：
    - TENCENT_API_SDK_APP_ID
    - TENCENT_API_SECRET_KEY
    """
    sdk_app_id = os.getenv("TENCENT_API_SDK_APP_ID")
    secret_key = os.getenv("TENCENT_API_SECRET_KEY")
    if not (sdk_app_id and secret_key):
        raise ValueError("缺少 TENCENT_API_SDK_APP_ID 或 TENCENT_API_SECRET_KEY 配置，无法生成 UserSig。")

    sdk_app_id_int = int(sdk_app_id)
    current_time = int(time.time())

    data_to_sign = [
        f"TLS.identifier:{identifier}",
        f"TLS.sdkappid:{sdk_app_id_int}",
        f"TLS.time:{current_time}",
        f"TLS.expire:{expire}",
        "",
    ]
    content = "\n".join(data_to_sign)

    digest = hmac.new(secret_key.encode("utf-8"), content.encode("utf-8"), hashlib.sha256).digest()
    signature = base64.b64encode(digest).decode("utf-8")

    sig_doc = {
        "TLS.ver": "2.0",
        "TLS.identifier": identifier,
        "TLS.sdkappid": sdk_app_id_int,
        "TLS.expire": expire,
        "TLS.time": current_time,
        "TLS.sig": signature,
    }

    json_data = json.dumps(sig_doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    compressed = zlib.compress(json_data)
    return base64.b64encode(compressed).decode("utf-8")



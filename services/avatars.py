import datetime
import os
import time
import traceback
from typing import Optional

try:
    import oss2  # type: ignore[import-not-found]
except ImportError:
    oss2 = None

from common import app_logger


def _get_image_dir() -> str:
    # 与 app.py 保持默认一致；也允许通过环境变量覆盖
    return os.getenv("IMAGE_DIR", "/var/www/images")


def build_public_url_from_local_path(relative_path: Optional[str]) -> Optional[str]:
    """
    如果配置了 LOCAL_AVATAR_BASE_URL，则根据本地相对路径拼接可访问的 HTTP 地址。
    """
    if not relative_path:
        return None

    base_url = os.getenv("LOCAL_AVATAR_BASE_URL")
    if not base_url:
        return None

    base = base_url.rstrip("/")
    cleaned = str(relative_path).lstrip("/")
    public_url = f"{base}/{cleaned}"
    app_logger.debug(f"[build_public_url_from_local_path] 生成URL: {public_url}")
    return public_url


def resolve_local_avatar_file_path(avatar_path: Optional[str]) -> Optional[str]:
    """
    根据数据库中存储的 avatar 字段推断本地文件路径。
    当 avatar 已经是 URL 时返回 None。
    """
    if not avatar_path:
        return None

    path_str = str(avatar_path).strip()
    if not path_str:
        return None

    lowered = path_str.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return None

    if os.path.isabs(path_str):
        return path_str

    return os.path.join(_get_image_dir(), path_str)


def upload_avatar_to_oss(avatar_bytes: bytes, object_name: str) -> Optional[str]:
    """
    上传头像文件到阿里云 OSS，返回可访问的 URL。
    """
    if not avatar_bytes:
        app_logger.error("upload_avatar_to_oss: avatar_bytes 为空")
        return None

    if oss2 is None:
        app_logger.error("upload_avatar_to_oss: oss2 模块未安装，无法上传到 OSS")
        return None

    endpoint = os.getenv("ALIYUN_OSS_ENDPOINT")
    bucket_name = os.getenv("ALIYUN_OSS_BUCKET")
    access_key_id = os.getenv("ALIYUN_OSS_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIYUN_OSS_ACCESS_KEY_SECRET")
    base_url = os.getenv("ALIYUN_OSS_BASE_URL")

    if not all([endpoint, bucket_name, access_key_id, access_key_secret]):
        app_logger.error("upload_avatar_to_oss: OSS 配置缺失，请检查环境变量")
        return None

    normalized_object_name = str(object_name or "").lstrip("/")
    if not normalized_object_name:
        normalized_object_name = f"avatars/{int(time.time())}.png"

    try:
        auth = oss2.Auth(access_key_id, access_key_secret)
        bucket = oss2.Bucket(auth, endpoint, bucket_name)

        expire_time = datetime.datetime.utcnow() + datetime.timedelta(days=36500)  # 100年
        expires_header = expire_time.strftime("%a, %d %b %Y %H:%M:%S GMT")

        headers = {
            "Expires": expires_header,
            "Cache-Control": "max-age=3153600000",
        }
        bucket.put_object(normalized_object_name, avatar_bytes, headers=headers)

        if base_url:
            base = base_url.rstrip("/")
            return f"{base}/{normalized_object_name}"

        endpoint_host = endpoint.replace("https://", "").replace("http://", "").strip("/")
        return f"https://{bucket_name}.{endpoint_host}/{normalized_object_name}"
    except Exception as exc:
        app_logger.error(f"upload_avatar_to_oss: 上传失败 object={normalized_object_name}, error={exc}")
        app_logger.debug(traceback.format_exc())
        return None


def save_avatar_locally(avatar_bytes: bytes, object_name: str) -> Optional[str]:
    """
    OSS 上传失败时，将头像保存到本地 IMAGE_DIR/avatars 下，返回相对路径（用于写入数据库）。
    """
    if not avatar_bytes:
        return None

    filename = os.path.basename(str(object_name or "")) or f"{int(time.time())}.png"
    image_dir = _get_image_dir()
    local_dir = os.path.join(image_dir, "avatars")
    os.makedirs(local_dir, exist_ok=True)
    file_path = os.path.join(local_dir, filename)

    try:
        with open(file_path, "wb") as f:
            f.write(avatar_bytes)
        return os.path.join("avatars", filename).replace("\\", "/")
    except Exception as exc:
        app_logger.error(f"save_avatar_locally: 保存失败 path={file_path}, error={exc}")
        app_logger.debug(traceback.format_exc())
        return None



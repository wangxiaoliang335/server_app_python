"""
OSS upload helpers extracted from app.py to reduce module size and avoid router<->app circular imports.
"""

import datetime
import os
import traceback
from typing import Optional

from common import app_logger

try:
    import oss2  # type: ignore
except ImportError:
    oss2 = None


ALIYUN_OSS_ENDPOINT = os.getenv("ALIYUN_OSS_ENDPOINT")
ALIYUN_OSS_BUCKET = os.getenv("ALIYUN_OSS_BUCKET")
ALIYUN_OSS_ACCESS_KEY_ID = os.getenv("ALIYUN_OSS_ACCESS_KEY_ID")
ALIYUN_OSS_ACCESS_KEY_SECRET = os.getenv("ALIYUN_OSS_ACCESS_KEY_SECRET")
ALIYUN_OSS_BASE_URL = os.getenv("ALIYUN_OSS_BASE_URL")  # 可选，自定义 CDN 或访问域名


def upload_excel_to_oss(excel_bytes: bytes, object_name: str) -> Optional[str]:
    """
    上传Excel文件到阿里云 OSS，返回可访问的 URL。
    """
    print(f"[upload_excel_to_oss] ========== 开始上传Excel文件到OSS ==========")
    app_logger.info(f"[upload_excel_to_oss] ========== 开始上传Excel文件到OSS ==========")
    print(f"[upload_excel_to_oss] 📋 输入参数:")
    print(f"[upload_excel_to_oss]   - object_name: {object_name}")
    print(f"[upload_excel_to_oss]   - excel_bytes大小: {len(excel_bytes) if excel_bytes else 0} bytes")
    app_logger.info(
        f"[upload_excel_to_oss] 📋 输入参数: object_name={object_name}, excel_bytes大小={len(excel_bytes) if excel_bytes else 0} bytes"
    )

    if not excel_bytes:
        error_msg = "upload_excel_to_oss: excel_bytes 为空"
        app_logger.error(error_msg)
        print(f"[upload_excel_to_oss] 错误: {error_msg}")
        return None

    print(f"[upload_excel_to_oss] 检查oss2模块... oss2={oss2}")
    if oss2 is None:
        error_msg = "upload_excel_to_oss: oss2 模块未安装，无法上传到 OSS"
        app_logger.error(error_msg)
        print(f"[upload_excel_to_oss] 错误: {error_msg}")
        return None

    print(f"[upload_excel_to_oss] 检查OSS配置...")
    print(f"[upload_excel_to_oss]   ALIYUN_OSS_ENDPOINT: {ALIYUN_OSS_ENDPOINT}")
    print(f"[upload_excel_to_oss]   ALIYUN_OSS_BUCKET: {ALIYUN_OSS_BUCKET}")
    print(
        f"[upload_excel_to_oss]   ALIYUN_OSS_ACCESS_KEY_ID: {'已设置' if ALIYUN_OSS_ACCESS_KEY_ID else '未设置'}"
    )
    print(
        f"[upload_excel_to_oss]   ALIYUN_OSS_ACCESS_KEY_SECRET: {'已设置' if ALIYUN_OSS_ACCESS_KEY_SECRET else '未设置'}"
    )
    print(f"[upload_excel_to_oss]   ALIYUN_OSS_BASE_URL: {ALIYUN_OSS_BASE_URL}")

    if not all([ALIYUN_OSS_ENDPOINT, ALIYUN_OSS_BUCKET, ALIYUN_OSS_ACCESS_KEY_ID, ALIYUN_OSS_ACCESS_KEY_SECRET]):
        error_msg = "upload_excel_to_oss: OSS 配置缺失，请检查环境变量"
        app_logger.error(error_msg)
        print(f"[upload_excel_to_oss] 错误: {error_msg}")
        print(f"[upload_excel_to_oss] 配置检查结果:")
        print(f"[upload_excel_to_oss]   - ALIYUN_OSS_ENDPOINT存在: {bool(ALIYUN_OSS_ENDPOINT)}")
        print(f"[upload_excel_to_oss]   - ALIYUN_OSS_BUCKET存在: {bool(ALIYUN_OSS_BUCKET)}")
        print(f"[upload_excel_to_oss]   - ALIYUN_OSS_ACCESS_KEY_ID存在: {bool(ALIYUN_OSS_ACCESS_KEY_ID)}")
        print(f"[upload_excel_to_oss]   - ALIYUN_OSS_ACCESS_KEY_SECRET存在: {bool(ALIYUN_OSS_ACCESS_KEY_SECRET)}")
        return None

    normalized_object_name = object_name.lstrip("/")
    print(f"[upload_excel_to_oss] 标准化对象名称: {normalized_object_name}")

    try:
        print(f"[upload_excel_to_oss] 创建OSS认证对象...")
        auth = oss2.Auth(ALIYUN_OSS_ACCESS_KEY_ID, ALIYUN_OSS_ACCESS_KEY_SECRET)
        print(f"[upload_excel_to_oss] 创建OSS Bucket对象...")
        bucket = oss2.Bucket(auth, ALIYUN_OSS_ENDPOINT, ALIYUN_OSS_BUCKET)

        # 设置过期时间为100年后
        expire_time = datetime.datetime.utcnow() + datetime.timedelta(days=36500)  # 100年 = 36500天
        expires_header = expire_time.strftime("%a, %d %b %Y %H:%M:%S GMT")

        headers = {"Expires": expires_header, "Cache-Control": "max-age=3153600000"}

        print(f"[upload_excel_to_oss] 设置过期时间: {expires_header} (100年后)")
        print(f"[upload_excel_to_oss] ☁️ 开始上传文件到OSS...")
        app_logger.info(f"[upload_excel_to_oss] ☁️ 开始上传文件到OSS: {normalized_object_name}")
        bucket.put_object(normalized_object_name, excel_bytes, headers=headers)
        print(f"[upload_excel_to_oss] ✅ 文件上传成功！")
        app_logger.info(f"[upload_excel_to_oss] ✅ 文件上传成功: {normalized_object_name}")

        print(f"[upload_excel_to_oss] 🔗 开始生成访问URL...")
        app_logger.info(f"[upload_excel_to_oss] 🔗 开始生成访问URL...")
        if ALIYUN_OSS_BASE_URL:
            base = ALIYUN_OSS_BASE_URL.rstrip("/")
            url = f"{base}/{normalized_object_name}"
            print(f"[upload_excel_to_oss] ✅ 使用自定义BASE_URL生成URL: {url}")
            app_logger.info(f"[upload_excel_to_oss] ✅ 使用自定义BASE_URL生成URL: {url}")
            print(f"[upload_excel_to_oss] ========== 上传完成，返回URL ==========")
            app_logger.info(f"[upload_excel_to_oss] ========== 上传完成，返回URL: {url} ==========")
            return url

        endpoint_host = ALIYUN_OSS_ENDPOINT.replace("https://", "").replace("http://", "").strip("/")
        url = f"https://{ALIYUN_OSS_BUCKET}.{endpoint_host}/{normalized_object_name}"
        print(f"[upload_excel_to_oss] ✅ 使用默认格式生成URL: {url}")
        app_logger.info(f"[upload_excel_to_oss] ✅ 使用默认格式生成URL: {url}")
        print(f"[upload_excel_to_oss] ========== 上传完成，返回URL ==========")
        app_logger.info(f"[upload_excel_to_oss] ========== 上传完成，返回URL: {url} ==========")
        return url
    except Exception as exc:
        error_msg = f"upload_excel_to_oss: 上传失败 object={normalized_object_name}, error={exc}"
        app_logger.error(error_msg)
        print(f"[upload_excel_to_oss] 异常: {error_msg}")
        print(f"[upload_excel_to_oss] 异常类型: {type(exc).__name__}")
        print(f"[upload_excel_to_oss] 异常堆栈:\\n{traceback.format_exc()}")
        return None



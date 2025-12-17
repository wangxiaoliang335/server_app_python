import logging
import os
import datetime
from logging.handlers import TimedRotatingFileHandler

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse


def safe_json_response(data: dict, status_code: int = 200):
    return JSONResponse(jsonable_encoder(data), status_code=status_code)


def format_notification_time(notification: dict) -> dict:
    """
    将通知记录中的 datetime/date/time 字段转换为字符串，便于 JSON 序列化。
    - 常见字段：created_at / updated_at / read_at
    """
    if not isinstance(notification, dict):
        return notification

    def _fmt(v):
        if isinstance(v, datetime.datetime):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(v, datetime.date):
            return v.strftime("%Y-%m-%d")
        if isinstance(v, datetime.time):
            return v.strftime("%H:%M:%S")
        return v

    for k in ("created_at", "updated_at", "read_at"):
        if k in notification:
            notification[k] = _fmt(notification.get(k))
    return notification


def _build_app_logger() -> logging.Logger:
    # 使用绝对路径，避免因启动工作目录不同导致日志落到意外的位置
    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    # 创建一个 TimedRotatingFileHandler，每天 (midnight) 轮转，保留 30 天的日志
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "app.log"),
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)

    logger = logging.getLogger("teacher-assistant")
    logger.setLevel(logging.INFO)
    # 避免重复添加 handler（例如 reload 时重复导入）
    if not any(
        isinstance(h, TimedRotatingFileHandler) and getattr(h, "baseFilename", None) == file_handler.baseFilename
        for h in logger.handlers
    ):
        logger.addHandler(file_handler)
    logger.propagate = False
    return logger


app_logger = _build_app_logger()



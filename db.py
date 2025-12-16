from __future__ import annotations

from typing import Optional

import mysql.connector
from mysql.connector import Error

from common import app_logger


DB_CONFIG = {
    "host": "rm-uf65y451aa995i174io.mysql.rds.aliyuncs.com",
    "database": "teacher_assistant",
    "user": "ta_user",
    "password": "Ta_0909DB&",
}


def get_db_connection() -> Optional[mysql.connector.MySQLConnection]:
    """获取数据库连接"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        app_logger.info("Database connection established.")
        return connection
    except Error as e:
        app_logger.error(f"Error connecting to MySQL: {e}")
        return None



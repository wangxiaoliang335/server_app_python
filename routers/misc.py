import redis
import mysql.connector
from mysql.connector import Error

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from common import app_logger, safe_json_response
from db import get_db_connection


router = APIRouter()

# Redis 连接
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)


def get_max_code_from_mysql(connection):
    # """从 MySQL 找最大号码"""
    print(" get_max_code_from_mysql 111\n")
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("SELECT MAX(CAST(id AS UNSIGNED)) AS max_id FROM ta_school")
        print(" get_max_code_from_mysql 222\n")
        row = cursor.fetchone()
        print(" get_max_code_from_mysql 333\n", row)
        if row and row["max_id"] is not None:
            return int(row["max_id"])
        return 0


def generate_unique_code():
    # """生成唯一 6 位数字"""
    connection = get_db_connection()
    if connection is None:
        app_logger.error("generate_unique_code failed: Database connection error.")
        raise RuntimeError("数据库连接失败")

    print(" generate_unique_code 111\n")

    # 先从 Redis 缓存取
    max_code = r.get("unique_max_code")
    if max_code:
        new_code = int(max_code) + 1
    else:
        # Redis 没缓存，从 MySQL 查
        new_code = get_max_code_from_mysql(connection) + 1

    print(" get_max_code_from_mysql leave")
    if new_code >= 1000000:
        raise ValueError("6位数字已用完")

    code_str = f"{new_code:06d}"

    print(" INSERT INTO ta_school\n")

    cursor = None
    # 写入 MySQL
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("INSERT INTO ta_school (id) VALUES (%s)", (new_code,))
        connection.commit()
        cursor.close()
    except mysql.connector.errors.IntegrityError:
        # 如果主键冲突，递归重试
        return generate_unique_code()
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if connection and connection.is_connected():
            connection.close()

    # 更新 Redis 缓存
    r.set("unique_max_code", new_code)
    print(" INSERT INTO code_str:", code_str, "\n")
    return code_str


@router.get("/unique6digit")
async def unique_code_api():
    try:
        code = generate_unique_code()
        return JSONResponse({"code": code, "status": "ok"})
    except Exception as e:
        return JSONResponse({"error": str(e), "status": "fail"}, status_code=500)


@router.get("/wallpapers")
async def list_wallpapers(request: Request):
    """
    获取所有壁纸列表 (支持筛选、排序)
    Query Parameters:
        - is_enabled (int, optional)
        - resolution (str, optional)
        - sort_by (str, optional)
        - order (str, optional)
    """
    connection = get_db_connection()
    if connection is None:
        app_logger.error("List wallpapers failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500, "wallpapers": []}}, status_code=500)

    cursor = None
    try:
        # 1. 获取查询参数
        is_enabled_filter = request.query_params.get("is_enabled")
        resolution_filter = request.query_params.get("resolution")
        sort_by = request.query_params.get("sort_by", "created_at")
        order = request.query_params.get("order", "desc")

        # 转类型
        try:
            is_enabled_filter = int(is_enabled_filter) if is_enabled_filter is not None else None
        except ValueError:
            is_enabled_filter = None

        # 2. 验证排序参数
        valid_sort_fields = ["created_at", "updated_at", "id"]
        valid_orders = ["asc", "desc"]
        if sort_by not in valid_sort_fields:
            sort_by = "created_at"
        if order not in valid_orders:
            order = "desc"

        # 3. 构建 SQL
        base_columns = "id, title, image_url, resolution, file_size, file_type, uploader_id, is_enabled, created_at, updated_at"
        base_query = f"SELECT {base_columns} FROM ta_wallpaper WHERE 1=1"
        filters, params = [], []

        if is_enabled_filter is not None:
            filters.append("AND is_enabled = %s")
            params.append(is_enabled_filter)
        if resolution_filter:
            filters.append("AND resolution = %s")
            params.append(resolution_filter)

        order_clause = f"ORDER BY {sort_by} {order}"
        final_query = base_query + " " + " ".join(filters) + " " + order_clause

        # 4. 执行
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        wallpapers = cursor.fetchall()

        app_logger.info(f"Fetched {len(wallpapers)} wallpapers.")
        return safe_json_response({"data": {"message": "获取壁纸列表成功", "code": 200, "wallpapers": wallpapers}})
    except Error as e:
        app_logger.error(f"Database error during fetching wallpapers: {e}")
        return JSONResponse({"data": {"message": "获取壁纸列表失败", "code": 500, "wallpapers": []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching wallpapers: {e}")
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500, "wallpapers": []}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching wallpapers.")



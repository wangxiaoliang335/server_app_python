import datetime
import json
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


@router.get("/wallpaper-library")
async def get_wallpaper_library():
    """
    获取壁纸库列表（系统共享壁纸）
    返回所有客户端共享的系统壁纸列表
    """
    connection = get_db_connection()
    if connection is None:
        app_logger.error("Get wallpaper library failed: Database connection error.")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500, "wallpapers": []}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 查询所有壁纸，按创建时间倒序排列
        sql = """
            SELECT id, name, image_url, created_at, updated_at
            FROM wallpaper_library
            ORDER BY created_at DESC
        """
        cursor.execute(sql)
        wallpapers = cursor.fetchall()

        # 格式化时间字段
        for wallpaper in wallpapers:
            for key, value in wallpaper.items():
                if isinstance(value, datetime.datetime):
                    wallpaper[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        app_logger.info(f"Fetched {len(wallpapers)} wallpapers from library.")
        return JSONResponse({
            "data": {
                "message": "获取壁纸库列表成功",
                "code": 200,
                "wallpapers": wallpapers
            }
        }, status_code=200)
    except Error as e:
        app_logger.error(f"Database error during fetching wallpaper library: {e}")
        return JSONResponse({"data": {"message": f"获取壁纸库列表失败: {str(e)}", "code": 500, "wallpapers": []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching wallpaper library: {e}", exc_info=True)
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500, "wallpapers": []}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching wallpaper library.")


@router.get("/class-wallpapers")
async def get_class_wallpapers(group_id: str = None):
    """
    获取班级群的班级壁纸列表
    查询参数:
        group_id: 班级群组ID（必填）
    """
    if not group_id:
        app_logger.warning("[GetClassWallpapers] 缺少 group_id")
        return JSONResponse({"data": {"message": "群组ID必须提供", "code": 400, "wallpapers": []}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("[GetClassWallpapers] 数据库连接失败")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500, "wallpapers": []}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 查询该班级群的所有壁纸，按创建时间倒序排列
        sql = """
            SELECT id, group_id, name, image_url, is_current, source, created_at, updated_at
            FROM class_wallpaper
            WHERE group_id = %s
            ORDER BY created_at DESC
        """
        cursor.execute(sql, (group_id,))
        wallpapers = cursor.fetchall()

        # 格式化时间字段
        for wallpaper in wallpapers:
            for key, value in wallpaper.items():
                if isinstance(value, datetime.datetime):
                    wallpaper[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        app_logger.info(f"[GetClassWallpapers] 查询成功 - group_id={group_id}, 数量={len(wallpapers)}")
        return JSONResponse({
            "data": {
                "message": "获取班级壁纸列表成功",
                "code": 200,
                "group_id": group_id,
                "wallpapers": wallpapers
            }
        }, status_code=200)
    except Error as e:
        app_logger.error(f"[GetClassWallpapers] 数据库错误 - group_id={group_id}, error={e}")
        return JSONResponse({"data": {"message": f"获取班级壁纸列表失败: {str(e)}", "code": 500, "wallpapers": []}}, status_code=500)
    except Exception as e:
        app_logger.error(f"[GetClassWallpapers] 未知错误 - group_id={group_id}, error={e}", exc_info=True)
        return JSONResponse({"data": {"message": "内部服务器错误", "code": 500, "wallpapers": []}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"[GetClassWallpapers] 数据库连接已关闭 - group_id={group_id}")


@router.post("/class-wallpapers/download")
async def download_wallpaper_to_class(request: Request):
    """
    从壁纸库下载壁纸到班级壁纸
    请求体 JSON:
    {
        "group_id": "班级群组ID",
        "wallpaper_id": 壁纸库中的壁纸ID
    }
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"data": {"message": "无效的 JSON 请求体", "code": 400}}, status_code=400)

    group_id = data.get("group_id")
    wallpaper_id = data.get("wallpaper_id")

    if not group_id:
        app_logger.warning("[DownloadWallpaperToClass] 缺少 group_id")
        return JSONResponse({"data": {"message": "群组ID必须提供", "code": 400}}, status_code=400)

    if not wallpaper_id:
        app_logger.warning("[DownloadWallpaperToClass] 缺少 wallpaper_id")
        return JSONResponse({"data": {"message": "壁纸ID必须提供", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("[DownloadWallpaperToClass] 数据库连接失败")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # 1. 从壁纸库获取壁纸信息
        cursor.execute(
            "SELECT id, name, image_url FROM wallpaper_library WHERE id = %s",
            (wallpaper_id,)
        )
        library_wallpaper = cursor.fetchone()

        if not library_wallpaper:
            app_logger.warning(f"[DownloadWallpaperToClass] 壁纸不存在 - wallpaper_id={wallpaper_id}")
            return JSONResponse({"data": {"message": "壁纸不存在", "code": 404}}, status_code=404)

        # 2. 检查是否已经下载过（根据 image_url 判断）
        cursor.execute(
            "SELECT id FROM class_wallpaper WHERE group_id = %s AND image_url = %s",
            (group_id, library_wallpaper["image_url"])
        )
        existing = cursor.fetchone()

        if existing:
            app_logger.info(f"[DownloadWallpaperToClass] 壁纸已存在 - group_id={group_id}, wallpaper_id={wallpaper_id}")
            return JSONResponse({
                "data": {
                    "message": "壁纸已存在",
                    "code": 200,
                    "wallpaper_id": existing["id"]
                }
            }, status_code=200)

        # 3. 插入到班级壁纸表（is_current=0，source='library'）
        cursor.execute(
            """
            INSERT INTO class_wallpaper (group_id, name, image_url, is_current, source)
            VALUES (%s, %s, %s, 0, 'library')
            """,
            (group_id, library_wallpaper["name"], library_wallpaper["image_url"])
        )
        new_wallpaper_id = cursor.lastrowid
        connection.commit()

        app_logger.info(f"[DownloadWallpaperToClass] 下载成功 - group_id={group_id}, wallpaper_id={wallpaper_id}, new_id={new_wallpaper_id}")
        return JSONResponse({
            "data": {
                "message": "下载壁纸成功",
                "code": 200,
                "wallpaper_id": new_wallpaper_id,
                "name": library_wallpaper["name"],
                "image_url": library_wallpaper["image_url"]
            }
        }, status_code=200)
    except Error as e:
        connection.rollback()
        app_logger.error(f"[DownloadWallpaperToClass] 数据库错误 - group_id={group_id}, wallpaper_id={wallpaper_id}, error={e}")
        return JSONResponse({"data": {"message": f"数据库操作失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"[DownloadWallpaperToClass] 未知错误 - group_id={group_id}, wallpaper_id={wallpaper_id}, error={e}", exc_info=True)
        return JSONResponse({"data": {"message": f"操作失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"[DownloadWallpaperToClass] 数据库连接已关闭 - group_id={group_id}")


@router.post("/class-wallpapers/set-current")
async def set_class_wallpaper_current(request: Request):
    """
    设置班级壁纸为当前使用的壁纸（远程更改该班级的桌面壁纸）
    请求体 JSON:
    {
        "group_id": "班级群组ID",
        "wallpaper_id": 班级壁纸ID
    }
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"data": {"message": "无效的 JSON 请求体", "code": 400}}, status_code=400)

    group_id = data.get("group_id")
    wallpaper_id = data.get("wallpaper_id")

    if not group_id:
        app_logger.warning("[SetClassWallpaperCurrent] 缺少 group_id")
        return JSONResponse({"data": {"message": "群组ID必须提供", "code": 400}}, status_code=400)

    if not wallpaper_id:
        app_logger.warning("[SetClassWallpaperCurrent] 缺少 wallpaper_id")
        return JSONResponse({"data": {"message": "壁纸ID必须提供", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("[SetClassWallpaperCurrent] 数据库连接失败")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # 1. 验证壁纸是否存在且属于该群组
        cursor.execute(
            "SELECT id, name, image_url FROM class_wallpaper WHERE id = %s AND group_id = %s",
            (wallpaper_id, group_id)
        )
        wallpaper = cursor.fetchone()

        if not wallpaper:
            app_logger.warning(f"[SetClassWallpaperCurrent] 壁纸不存在或不属于该群组 - group_id={group_id}, wallpaper_id={wallpaper_id}")
            return JSONResponse({"data": {"message": "壁纸不存在或不属于该群组", "code": 404}}, status_code=404)

        # 2. 先将该群组的所有壁纸设置为非当前（is_current=0）
        cursor.execute(
            "UPDATE class_wallpaper SET is_current = 0 WHERE group_id = %s",
            (group_id,)
        )

        # 3. 将指定的壁纸设置为当前（is_current=1）
        cursor.execute(
            "UPDATE class_wallpaper SET is_current = 1 WHERE id = %s AND group_id = %s",
            (wallpaper_id, group_id)
        )

        connection.commit()

        app_logger.info(f"[SetClassWallpaperCurrent] 设置成功 - group_id={group_id}, wallpaper_id={wallpaper_id}")
        
        # 4. 通过WebSocket推送壁纸更改通知到该班级的所有在线客户端
        # 这里需要导入相关的WebSocket管理模块
        try:
            from ws.manager import connections
            # 获取群组的所有成员（这里简化处理，实际可能需要查询群组成员）
            # 可以通过 group_id 找到对应的 class_code，然后推送
            # 暂时先记录日志，后续可以完善推送逻辑
            app_logger.info(f"[SetClassWallpaperCurrent] 需要推送壁纸更改通知 - group_id={group_id}")
        except Exception as e:
            app_logger.warning(f"[SetClassWallpaperCurrent] 推送通知失败: {e}")

        return JSONResponse({
            "data": {
                "message": "设置班级壁纸成功",
                "code": 200,
                "group_id": group_id,
                "wallpaper_id": wallpaper_id,
                "wallpaper": {
                    "id": wallpaper["id"],
                    "name": wallpaper["name"],
                    "image_url": wallpaper["image_url"]
                }
            }
        }, status_code=200)
    except Error as e:
        connection.rollback()
        app_logger.error(f"[SetClassWallpaperCurrent] 数据库错误 - group_id={group_id}, wallpaper_id={wallpaper_id}, error={e}")
        return JSONResponse({"data": {"message": f"数据库操作失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"[SetClassWallpaperCurrent] 未知错误 - group_id={group_id}, wallpaper_id={wallpaper_id}, error={e}", exc_info=True)
        return JSONResponse({"data": {"message": f"操作失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"[SetClassWallpaperCurrent] 数据库连接已关闭 - group_id={group_id}")


@router.post("/class-wallpapers/apply-weekly")
async def apply_weekly_wallpaper(request: Request):
    """
    应用一周壁纸设置
    保存并启用一周壁纸配置，应用后系统会根据当前日期自动切换对应的壁纸
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"data": {"message": "无效的 JSON 请求体", "code": 400}}, status_code=400)

    group_id = data.get("group_id")
    weekly_wallpapers = data.get("weekly_wallpapers")

    if not group_id:
        app_logger.warning("[ApplyWeeklyWallpaper] 缺少 group_id")
        return JSONResponse({"data": {"message": "群组ID必须提供", "code": 400}}, status_code=400)

    if not weekly_wallpapers or not isinstance(weekly_wallpapers, dict):
        app_logger.warning("[ApplyWeeklyWallpaper] 缺少 weekly_wallpapers 或格式错误")
        return JSONResponse({"data": {"message": "一周壁纸配置必须提供", "code": 400}}, status_code=400)

    # 验证所有7天是否都设置了壁纸
    required_days = ["0", "1", "2", "3", "4", "5", "6"]
    missing_days = []
    for day in required_days:
        if day not in weekly_wallpapers or not weekly_wallpapers[day]:
            missing_days.append(day)

    if missing_days:
        day_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        missing_names = [day_names[int(day)] for day in missing_days]
        app_logger.warning(f"[ApplyWeeklyWallpaper] 缺少壁纸配置 - group_id={group_id}, missing_days={missing_days}")
        return JSONResponse({
            "data": {
                "message": f"参数错误：请为所有日期设置壁纸，缺少：{', '.join(missing_names)}",
                "code": 400
            }
        }, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("[ApplyWeeklyWallpaper] 数据库连接失败")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # 使用 INSERT ... ON DUPLICATE KEY UPDATE 来插入或更新
        cursor.execute(
            """
            INSERT INTO class_weekly_wallpapers (
                group_id, is_enabled, monday_url, tuesday_url, wednesday_url,
                thursday_url, friday_url, saturday_url, sunday_url
            ) VALUES (%s, 1, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                is_enabled = 1,
                monday_url = VALUES(monday_url),
                tuesday_url = VALUES(tuesday_url),
                wednesday_url = VALUES(wednesday_url),
                thursday_url = VALUES(thursday_url),
                friday_url = VALUES(friday_url),
                saturday_url = VALUES(saturday_url),
                sunday_url = VALUES(sunday_url),
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                group_id,
                weekly_wallpapers.get("0"),  # Monday
                weekly_wallpapers.get("1"),  # Tuesday
                weekly_wallpapers.get("2"),  # Wednesday
                weekly_wallpapers.get("3"),  # Thursday
                weekly_wallpapers.get("4"),  # Friday
                weekly_wallpapers.get("5"),  # Saturday
                weekly_wallpapers.get("6"),  # Sunday
            )
        )

        connection.commit()

        # 根据当前日期，自动设置对应的壁纸为当前壁纸
        # Python weekday(): Monday=0, Tuesday=1, ..., Sunday=6
        current_weekday = datetime.datetime.now().weekday()
        current_url = weekly_wallpapers.get(str(current_weekday))

        # 如果该URL在class_wallpaper表中存在，设置为当前壁纸
        if current_url:
            cursor.execute(
                """
                SELECT id FROM class_wallpaper
                WHERE group_id = %s AND image_url = %s
                LIMIT 1
                """,
                (group_id, current_url)
            )
            existing_wallpaper = cursor.fetchone()
            
            if existing_wallpaper:
                # 先将该群组的所有壁纸设置为非当前
                cursor.execute(
                    "UPDATE class_wallpaper SET is_current = 0 WHERE group_id = %s",
                    (group_id,)
                )
                # 设置当前日期的壁纸为当前壁纸
                cursor.execute(
                    "UPDATE class_wallpaper SET is_current = 1 WHERE id = %s",
                    (existing_wallpaper["id"],)
                )
                connection.commit()

        app_logger.info(f"[ApplyWeeklyWallpaper] 应用成功 - group_id={group_id}, current_weekday={current_weekday}")
        return JSONResponse({
            "data": {
                "message": "一周壁纸应用成功",
                "code": 200
            }
        }, status_code=200)
    except Error as e:
        connection.rollback()
        app_logger.error(f"[ApplyWeeklyWallpaper] 数据库错误 - group_id={group_id}, error={e}")
        return JSONResponse({"data": {"message": f"数据库操作失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"[ApplyWeeklyWallpaper] 未知错误 - group_id={group_id}, error={e}", exc_info=True)
        return JSONResponse({"data": {"message": f"操作失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"[ApplyWeeklyWallpaper] 数据库连接已关闭 - group_id={group_id}")


@router.get("/class-wallpapers/weekly-config")
async def get_weekly_wallpaper_config(group_id: str = None):
    """
    获取一周壁纸配置
    获取指定班级群的一周壁纸配置信息，用于初始化时加载已保存的配置
    """
    if not group_id:
        app_logger.warning("[GetWeeklyWallpaperConfig] 缺少 group_id")
        return JSONResponse({
            "data": {
                "message": "群组ID必须提供",
                "code": 400,
                "is_enabled": False,
                "weekly_wallpapers": None
            }
        }, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("[GetWeeklyWallpaperConfig] 数据库连接失败")
        return JSONResponse({
            "data": {
                "message": "数据库连接失败",
                "code": 500,
                "is_enabled": False,
                "weekly_wallpapers": None
            }
        }, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT is_enabled, monday_url, tuesday_url, wednesday_url,
                   thursday_url, friday_url, saturday_url, sunday_url
            FROM class_weekly_wallpapers
            WHERE group_id = %s
            """,
            (group_id,)
        )
        config = cursor.fetchone()

        if not config:
            app_logger.info(f"[GetWeeklyWallpaperConfig] 未找到配置 - group_id={group_id}")
            return JSONResponse({
                "data": {
                    "message": "查询成功",
                    "code": 200,
                    "is_enabled": False,
                    "weekly_wallpapers": None
                }
            }, status_code=200)

        # 构建返回的一周壁纸配置对象
        weekly_wallpapers = {
            "0": config.get("monday_url") or "",
            "1": config.get("tuesday_url") or "",
            "2": config.get("wednesday_url") or "",
            "3": config.get("thursday_url") or "",
            "4": config.get("friday_url") or "",
            "5": config.get("saturday_url") or "",
            "6": config.get("sunday_url") or ""
        }

        is_enabled = bool(config.get("is_enabled", 0))

        app_logger.info(f"[GetWeeklyWallpaperConfig] 查询成功 - group_id={group_id}, is_enabled={is_enabled}")
        return JSONResponse({
            "data": {
                "message": "查询成功",
                "code": 200,
                "is_enabled": is_enabled,
                "weekly_wallpapers": weekly_wallpapers
            }
        }, status_code=200)
    except Error as e:
        app_logger.error(f"[GetWeeklyWallpaperConfig] 数据库错误 - group_id={group_id}, error={e}")
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500,
                "is_enabled": False,
                "weekly_wallpapers": None
            }
        }, status_code=500)
    except Exception as e:
        app_logger.error(f"[GetWeeklyWallpaperConfig] 未知错误 - group_id={group_id}, error={e}", exc_info=True)
        return JSONResponse({
            "data": {
                "message": "内部服务器错误",
                "code": 500,
                "is_enabled": False,
                "weekly_wallpapers": None
            }
        }, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"[GetWeeklyWallpaperConfig] 数据库连接已关闭 - group_id={group_id}")


@router.post("/class-wallpapers/disable-weekly")
async def disable_weekly_wallpaper(request: Request):
    """
    停用一周壁纸功能
    当用户点击"设为壁纸"设置单张壁纸时，需要先停用一周壁纸功能
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"data": {"message": "无效的 JSON 请求体", "code": 400}}, status_code=400)

    group_id = data.get("group_id")

    if not group_id:
        app_logger.warning("[DisableWeeklyWallpaper] 缺少 group_id")
        return JSONResponse({"data": {"message": "群组ID必须提供", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("[DisableWeeklyWallpaper] 数据库连接失败")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # 更新 is_enabled 为 0，保留配置数据
        cursor.execute(
            """
            UPDATE class_weekly_wallpapers
            SET is_enabled = 0, updated_at = CURRENT_TIMESTAMP
            WHERE group_id = %s
            """,
            (group_id,)
        )

        if cursor.rowcount == 0:
            # 如果记录不存在，说明从未配置过一周壁纸，直接返回成功
            app_logger.info(f"[DisableWeeklyWallpaper] 记录不存在，无需停用 - group_id={group_id}")
        else:
            connection.commit()
            app_logger.info(f"[DisableWeeklyWallpaper] 停用成功 - group_id={group_id}")

        return JSONResponse({
            "data": {
                "message": "一周壁纸功能已停用",
                "code": 200
            }
        }, status_code=200)
    except Error as e:
        connection.rollback()
        app_logger.error(f"[DisableWeeklyWallpaper] 数据库错误 - group_id={group_id}, error={e}")
        return JSONResponse({"data": {"message": f"数据库操作失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"[DisableWeeklyWallpaper] 未知错误 - group_id={group_id}, error={e}", exc_info=True)
        return JSONResponse({"data": {"message": f"操作失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"[DisableWeeklyWallpaper] 数据库连接已关闭 - group_id={group_id}")


@router.get("/duty-roster")
async def get_duty_roster(group_id: str = None):
    """
    获取值日表数据
    获取指定班级群的值日表数据
    """
    if not group_id:
        app_logger.warning("[GetDutyRoster] 缺少 group_id")
        return JSONResponse({
            "data": {
                "message": "群组ID必须提供",
                "code": 400
            }
        }, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("[GetDutyRoster] 数据库连接失败")
        return JSONResponse({
            "data": {
                "message": "数据库连接失败",
                "code": 500
            }
        }, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT rows_data, requirement_row_index
            FROM duty_roster
            WHERE group_id = %s
            """,
            (group_id,)
        )
        result = cursor.fetchone()

        if not result:
            app_logger.info(f"[GetDutyRoster] 未找到值日表数据 - group_id={group_id}")
            return JSONResponse({
                "data": {
                    "message": "获取成功",
                    "code": 200,
                    "rows": [],
                    "requirement_row_index": -1
                }
            }, status_code=200)

        # 解析JSON数据
        rows = json.loads(result.get("rows_data", "[]"))
        requirement_row_index = result.get("requirement_row_index", -1)

        app_logger.info(f"[GetDutyRoster] 查询成功 - group_id={group_id}, rows_count={len(rows)}")
        return JSONResponse({
            "data": {
                "message": "获取成功",
                "code": 200,
                "rows": rows,
                "requirement_row_index": requirement_row_index
            }
        }, status_code=200)
    except json.JSONDecodeError as e:
        app_logger.error(f"[GetDutyRoster] JSON解析错误 - group_id={group_id}, error={e}")
        return JSONResponse({
            "data": {
                "message": "数据格式错误",
                "code": 500
            }
        }, status_code=500)
    except Error as e:
        app_logger.error(f"[GetDutyRoster] 数据库错误 - group_id={group_id}, error={e}")
        return JSONResponse({
            "data": {
                "message": f"查询失败: {str(e)}",
                "code": 500
            }
        }, status_code=500)
    except Exception as e:
        app_logger.error(f"[GetDutyRoster] 未知错误 - group_id={group_id}, error={e}", exc_info=True)
        return JSONResponse({
            "data": {
                "message": "内部服务器错误",
                "code": 500
            }
        }, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"[GetDutyRoster] 数据库连接已关闭 - group_id={group_id}")


@router.post("/duty-roster")
async def save_duty_roster(request: Request):
    """
    保存值日表数据
    保存值日表数据到服务器
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"data": {"message": "无效的 JSON 请求体", "code": 400}}, status_code=400)

    group_id = data.get("group_id")
    rows = data.get("rows")
    requirement_row_index = data.get("requirement_row_index", -1)

    if not group_id:
        app_logger.warning("[SaveDutyRoster] 缺少 group_id")
        return JSONResponse({"data": {"message": "群组ID必须提供", "code": 400}}, status_code=400)

    if rows is None:
        app_logger.warning("[SaveDutyRoster] 缺少 rows")
        return JSONResponse({"data": {"message": "值日表数据必须提供", "code": 400}}, status_code=400)

    if not isinstance(rows, list):
        app_logger.warning("[SaveDutyRoster] rows 格式错误")
        return JSONResponse({"data": {"message": "值日表数据格式错误，必须是数组", "code": 400}}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("[SaveDutyRoster] 数据库连接失败")
        return JSONResponse({"data": {"message": "数据库连接失败", "code": 500}}, status_code=500)

    cursor = None
    try:
        # 将二维数组转换为JSON字符串
        rows_json = json.dumps(rows, ensure_ascii=False)

        cursor = connection.cursor(dictionary=True)

        # 使用 INSERT ... ON DUPLICATE KEY UPDATE 来插入或更新
        cursor.execute(
            """
            INSERT INTO duty_roster (group_id, rows_data, requirement_row_index)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                rows_data = VALUES(rows_data),
                requirement_row_index = VALUES(requirement_row_index),
                updated_at = CURRENT_TIMESTAMP
            """,
            (group_id, rows_json, requirement_row_index)
        )

        connection.commit()

        app_logger.info(f"[SaveDutyRoster] 保存成功 - group_id={group_id}, rows_count={len(rows)}, requirement_row_index={requirement_row_index}")
        return JSONResponse({
            "data": {
                "message": "保存成功",
                "code": 200
            }
        }, status_code=200)
    except json.JSONEncodeError as e:
        connection.rollback()
        app_logger.error(f"[SaveDutyRoster] JSON编码错误 - group_id={group_id}, error={e}")
        return JSONResponse({"data": {"message": "数据格式错误", "code": 500}}, status_code=500)
    except Error as e:
        connection.rollback()
        app_logger.error(f"[SaveDutyRoster] 数据库错误 - group_id={group_id}, error={e}")
        return JSONResponse({"data": {"message": f"数据库操作失败: {str(e)}", "code": 500}}, status_code=500)
    except Exception as e:
        connection.rollback()
        app_logger.error(f"[SaveDutyRoster] 未知错误 - group_id={group_id}, error={e}", exc_info=True)
        return JSONResponse({"data": {"message": f"操作失败: {str(e)}", "code": 500}}, status_code=500)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info(f"[SaveDutyRoster] 数据库连接已关闭 - group_id={group_id}")



import datetime
import json
import os

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from common import app_logger
from db import get_db_connection
from services.tencent_sig import generate_tencent_user_sig


router = APIRouter()


@router.post("/tencent/callback")
async def tencent_im_callback(request: Request):
    """
    腾讯IM回调接口
    接收腾讯IM的各种事件通知，包括群组解散、成员变动等
    """
    print("=" * 80)
    print("[tencent/callback] ========== 收到腾讯IM回调请求 ==========")
    print(f"[tencent/callback] 请求时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[tencent/callback] 请求来源IP: {request.client.host if request.client else 'Unknown'}")
    print(f"[tencent/callback] 请求方法: {request.method}")
    print(f"[tencent/callback] 请求路径: {request.url.path}")
    app_logger.info("=" * 80)
    app_logger.info("[tencent/callback] ========== 收到腾讯IM回调请求 ==========")
    app_logger.info(f"[tencent/callback] 请求时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    app_logger.info(f"[tencent/callback] 请求来源IP: {request.client.host if request.client else 'Unknown'}")

    try:
        body = await request.json()
        print(f"[tencent/callback] 收到腾讯IM回调数据:")
        print(f"[tencent/callback] {json.dumps(body, ensure_ascii=False, indent=2)}")
        app_logger.info(f"[tencent/callback] 收到腾讯IM回调数据: {json.dumps(body, ensure_ascii=False)}")

        # 获取回调类型
        callback_command = body.get("CallbackCommand")
        print(f"[tencent/callback] 回调类型: {callback_command}")
        app_logger.info(f"[tencent/callback] 回调类型: {callback_command}")

        if not callback_command:
            print("[tencent/callback] ⚠️ 警告: 回调数据中缺少 CallbackCommand")
            app_logger.warning("[tencent/callback] 回调数据中缺少 CallbackCommand")
            print("[tencent/callback] 返回成功响应（避免腾讯IM重试）")
            return JSONResponse({"ActionStatus": "OK", "ErrorCode": 0, "ErrorInfo": "OK"})

        # 处理群组解散回调
        if callback_command == "Group.CallbackAfterGroupDestroyed":
            print("[tencent/callback] ✅ 检测到群组解散回调: Group.CallbackAfterGroupDestroyed")
            app_logger.info("[tencent/callback] 检测到群组解散回调")

            # 获取群组ID
            group_id = body.get("GroupId")
            operator_account = body.get("Operator_Account", "Unknown")
            event_time = body.get("EventTime", "Unknown")

            print(f"[tencent/callback] 回调详情:")
            print(f"[tencent/callback]   - GroupId: {group_id}")
            print(f"[tencent/callback]   - Operator_Account: {operator_account}")
            print(f"[tencent/callback]   - EventTime: {event_time}")
            app_logger.info(
                f"[tencent/callback] 回调详情 - GroupId: {group_id}, Operator: {operator_account}, EventTime: {event_time}"
            )

            if not group_id:
                print("[tencent/callback] ⚠️ 警告: 群组解散回调中缺少 GroupId")
                app_logger.warning("[tencent/callback] 群组解散回调中缺少 GroupId")
                print("[tencent/callback] 返回成功响应（避免腾讯IM重试）")
                return JSONResponse({"ActionStatus": "OK", "ErrorCode": 0, "ErrorInfo": "OK"})

            print(f"[tencent/callback] 🔄 开始处理群组解散: group_id={group_id}")
            app_logger.info(f"[tencent/callback] 开始处理群组解散: group_id={group_id}")

            # 连接数据库
            print(f"[tencent/callback] 📊 连接数据库...")
            connection = get_db_connection()
            if connection is None or not connection.is_connected():
                print("[tencent/callback] ❌ 错误: 数据库连接失败")
                app_logger.error("[tencent/callback] 数据库连接失败")
                print("[tencent/callback] 返回成功响应（避免腾讯IM重试）")
                return JSONResponse({"ActionStatus": "OK", "ErrorCode": 0, "ErrorInfo": "OK"})
            print(f"[tencent/callback] ✅ 数据库连接成功")

            cursor = None
            deleted_rooms = 0
            deleted_members = 0
            deleted_groups = 0
            deleted_room_members = 0
            try:
                cursor = connection.cursor(dictionary=True)

                # 检查群组是否存在
                print(f"[tencent/callback] 🔍 检查群组 {group_id} 是否存在于本地数据库...")
                cursor.execute("SELECT group_id, group_name, member_num FROM `groups` WHERE group_id = %s", (group_id,))
                group_info = cursor.fetchone()

                if not group_info:
                    print(f"[tencent/callback] ⚠️ 群组 {group_id} 在本地数据库中不存在，无需处理")
                    app_logger.info(f"[tencent/callback] 群组 {group_id} 在本地数据库中不存在，无需处理")
                    print("[tencent/callback] 返回成功响应（避免腾讯IM重试）")
                    return JSONResponse({"ActionStatus": "OK", "ErrorCode": 0, "ErrorInfo": "OK"})

                print(
                    f"[tencent/callback] ✅ 找到群组: {group_info.get('group_name', 'N/A')} (成员数: {group_info.get('member_num', 0)})"
                )
                app_logger.info(f"[tencent/callback] 找到群组: {group_info}")

                # 开始事务（如果连接已经在事务中，先提交或回滚）
                print(f"[tencent/callback] 🔄 检查并开始数据库事务...")
                try:
                    connection.start_transaction()
                    print(f"[tencent/callback] ✅ 新事务已开始")
                except Exception as e:
                    error_msg = str(e)
                    if "Transaction already in progress" in error_msg or "already in progress" in error_msg.lower():
                        print(f"[tencent/callback] ⚠️  连接已在事务中，先提交当前事务...")
                        try:
                            connection.commit()
                            connection.start_transaction()
                            print(f"[tencent/callback] ✅ 已提交旧事务并开始新事务")
                        except Exception as commit_error:
                            print(f"[tencent/callback] ⚠️  提交旧事务失败: {commit_error}，尝试回滚...")
                            connection.rollback()
                            connection.start_transaction()
                            print(f"[tencent/callback] ✅ 已回滚旧事务并开始新事务")
                    else:
                        raise

                # 1. 删除群组成员
                print(f"[tencent/callback] 🗑️  步骤1: 删除群组 {group_id} 的所有成员...")
                cursor.execute("DELETE FROM `group_members` WHERE group_id = %s", (group_id,))
                deleted_members = cursor.rowcount
                print(f"[tencent/callback] ✅ 删除了 {deleted_members} 个群组成员")
                app_logger.info(f"[tencent/callback] 删除了 {deleted_members} 个群组成员")

                # 2. 删除群组
                print(f"[tencent/callback] 🗑️  步骤2: 删除群组 {group_id}...")
                cursor.execute("DELETE FROM `groups` WHERE group_id = %s", (group_id,))
                deleted_groups = cursor.rowcount
                print(f"[tencent/callback] ✅ 删除了 {deleted_groups} 个群组")
                app_logger.info(f"[tencent/callback] 删除了 {deleted_groups} 个群组")

                # 3. 删除临时语音房间（如果存在）
                print(f"[tencent/callback] 🗑️  步骤3: 检查并删除临时语音房间...")
                cursor.execute("SELECT room_id FROM `temp_voice_rooms` WHERE group_id = %s", (group_id,))
                room_ids = [row["room_id"] for row in cursor.fetchall()]

                if room_ids:
                    print(f"[tencent/callback] 找到 {len(room_ids)} 个临时语音房间，room_ids: {room_ids}")
                    placeholders = ", ".join(["%s"] * len(room_ids))
                    cursor.execute(
                        f"DELETE FROM `temp_voice_room_members` WHERE room_id IN ({placeholders})", room_ids
                    )
                    deleted_room_members = cursor.rowcount
                    print(f"[tencent/callback] ✅ 删除了 {deleted_room_members} 个临时语音房间成员")

                    cursor.execute("DELETE FROM `temp_voice_rooms` WHERE group_id = %s", (group_id,))
                    deleted_rooms = cursor.rowcount
                    print(f"[tencent/callback] ✅ 删除了 {deleted_rooms} 个临时语音房间")
                    app_logger.info(
                        f"[tencent/callback] 删除了 {deleted_rooms} 个临时语音房间和 {deleted_room_members} 个成员"
                    )
                else:
                    print(f"[tencent/callback] ℹ️  未找到临时语音房间，跳过")

                # 提交事务
                print(f"[tencent/callback] 💾 提交数据库事务...")
                connection.commit()
                print(f"[tencent/callback] ✅ 群组 {group_id} 解散处理完成！")
                print(f"[tencent/callback] 📊 处理结果统计:")
                print(f"[tencent/callback]   - 删除成员数: {deleted_members}")
                print(f"[tencent/callback]   - 删除群组数: {deleted_groups}")
                print(f"[tencent/callback]   - 删除临时房间数: {deleted_rooms}")
                app_logger.info(
                    f"[tencent/callback] 群组 {group_id} 解散处理完成，删除了 {deleted_members} 个成员和 {deleted_groups} 个群组"
                )

            except Exception as e:
                if connection and connection.is_connected():
                    print(f"[tencent/callback] ⚠️  发生错误，回滚事务...")
                    connection.rollback()
                traceback_str = __import__("traceback").format_exc()
                print(f"[tencent/callback] ❌ 处理群组解散时发生错误: {e}")
                print(f"[tencent/callback] 错误堆栈:\n{traceback_str}")
                app_logger.error(f"[tencent/callback] 处理群组解散时发生错误: {e}", exc_info=True)
            finally:
                if cursor:
                    cursor.close()
                    print(f"[tencent/callback] 🔒 数据库游标已关闭")
                if connection and connection.is_connected():
                    connection.close()
                    print(f"[tencent/callback] 🔒 数据库连接已关闭")

            # 返回成功响应给腾讯IM
            print(f"[tencent/callback] 📤 返回成功响应给腾讯IM")
            print("=" * 80)
            app_logger.info("[tencent/callback] ========== 回调处理完成 ==========")
            return JSONResponse({"ActionStatus": "OK", "ErrorCode": 0, "ErrorInfo": "OK"})

        # 其他类型的回调（可以在这里扩展）
        print(f"[tencent/callback] ⚠️  收到未处理的回调类型: {callback_command}")
        print(f"[tencent/callback] 完整回调数据: {json.dumps(body, ensure_ascii=False, indent=2)}")
        app_logger.info(f"[tencent/callback] 收到未处理的回调类型: {callback_command}")
        app_logger.info(f"[tencent/callback] 完整回调数据: {body}")
        print(f"[tencent/callback] 📤 返回成功响应（避免腾讯IM重试）")
        print("=" * 80)
        return JSONResponse({"ActionStatus": "OK", "ErrorCode": 0, "ErrorInfo": "OK"})

    except json.JSONDecodeError as e:
        print(f"[tencent/callback] ❌ JSON解析失败: {e}")
        app_logger.error(f"[tencent/callback] JSON解析失败: {e}")
        print(f"[tencent/callback] 📤 返回成功响应（避免腾讯IM重试）")
        print("=" * 80)
        return JSONResponse({"ActionStatus": "OK", "ErrorCode": 0, "ErrorInfo": "OK"})
    except Exception as e:
        traceback_str = __import__("traceback").format_exc()
        print(f"[tencent/callback] ❌ 处理回调时发生异常: {e}")
        print(f"[tencent/callback] 错误堆栈:\n{traceback_str}")
        app_logger.error(f"[tencent/callback] 处理回调时发生异常: {e}", exc_info=True)
        print(f"[tencent/callback] 📤 返回成功响应（避免腾讯IM重试）")
        print("=" * 80)
        return JSONResponse({"ActionStatus": "OK", "ErrorCode": 0, "ErrorInfo": "OK"})


@router.post("/tencent/user_sig")
async def create_tencent_user_sig(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"data": {"message": "请求体必须为 JSON", "code": 400}}, status_code=400)

    identifier = body.get("identifier") or body.get("user_id")
    expire = body.get("expire", 86400)

    if not identifier:
        return JSONResponse({"data": {"message": "缺少 identifier 参数", "code": 400}}, status_code=400)

    try:
        expire_int = int(expire)
        if expire_int <= 0:
            raise ValueError("expire must be positive")
    except (ValueError, TypeError):
        return JSONResponse({"data": {"message": "expire 参数必须为正整数", "code": 400}}, status_code=400)

    try:
        user_sig = generate_tencent_user_sig(identifier, expire_int)
    except ValueError as config_error:
        app_logger.error(f"生成 UserSig 配置错误: {config_error}")
        return JSONResponse({"data": {"message": str(config_error), "code": 500}}, status_code=500)
    except Exception as e:
        app_logger.exception(f"生成 UserSig 时发生异常: {e}")
        return JSONResponse({"data": {"message": f"生成 UserSig 失败: {e}", "code": 500}}, status_code=500)

    response_data = {
        "identifier": identifier,
        "sdk_app_id": os.getenv("TENCENT_API_SDK_APP_ID"),
        "expire": expire_int,
        "user_sig": user_sig,
    }
    return JSONResponse({"data": response_data, "code": 200})


@router.post("/getUserSig")
async def get_user_sig(request: Request):
    """
    获取腾讯 IM UserSig 接口
    客户端调用：POST /getUserSig
    支持 JSON 格式：{"user_id": "xxx"} 或表单格式：user_id=xxx
    返回格式：{"data": {"user_sig": "...", "usersig": "...", "sig": "..."}, "code": 200}
    """
    user_id = None
    expire = 86400

    # 尝试解析 JSON / Form
    try:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            body = await request.json()
            user_id = body.get("user_id") or body.get("identifier")
            expire = body.get("expire", 86400)
        else:
            form_data = await request.form()
            user_id_val = form_data.get("user_id") or form_data.get("identifier")
            if user_id_val:
                user_id = str(user_id_val) if not isinstance(user_id_val, str) else user_id_val
            if form_data.get("expire"):
                expire_val = form_data.get("expire")
                expire = str(expire_val) if not isinstance(expire_val, str) else expire_val
    except Exception as e:
        print(f"[getUserSig] 解析请求失败: {e}")
        app_logger.error(f"解析请求失败: {e}")
        return JSONResponse({"data": {"message": "请求格式错误", "code": 400}}, status_code=400)

    if not user_id:
        return JSONResponse({"data": {"message": "缺少 user_id 参数", "code": 400}}, status_code=400)

    try:
        expire_int = int(expire)
        if expire_int <= 0:
            raise ValueError("expire must be positive")
    except (ValueError, TypeError):
        return JSONResponse({"data": {"message": "expire 参数必须为正整数", "code": 400}}, status_code=400)

    try:
        user_sig = generate_tencent_user_sig(user_id, expire_int)
        print(f"[getUserSig] 为 user_id={user_id} 生成 UserSig 成功，长度: {len(user_sig)}")
        app_logger.info(f"为 user_id={user_id} 生成 UserSig 成功")
    except ValueError as config_error:
        app_logger.error(f"生成 UserSig 配置错误: {config_error}")
        return JSONResponse({"data": {"message": str(config_error), "code": 500}}, status_code=500)
    except Exception as e:
        app_logger.exception(f"生成 UserSig 时发生异常: {e}")
        return JSONResponse({"data": {"message": f"生成 UserSig 失败: {e}", "code": 500}}, status_code=500)

    response_data = {"user_sig": user_sig, "usersig": user_sig, "sig": user_sig}
    return JSONResponse({"data": response_data, "code": 200})



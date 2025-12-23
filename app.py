import asyncio
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware

from common import app_logger
from ws.manager import heartbeat_checker, load_active_temp_rooms_from_db, stop_event

# 加载 .env（保持与旧行为一致）
load_dotenv()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """
    应用生命周期：
    - 启动时加载临时房间状态并启动心跳检测任务
    - 关闭时停止心跳检测任务
    """
    stop_event.clear()
    load_active_temp_rooms_from_db()

    hb_task = asyncio.create_task(heartbeat_checker())
    app_logger.info("应用启动：心跳检测已启动")

    yield

    app_logger.info("应用关闭：准备停止心跳检测")
    stop_event.set()
    hb_task.cancel()
    try:
        await hb_task
    except asyncio.CancelledError:
        app_logger.info("heartbeat_checker 已安全停止")


app = FastAPI(lifespan=lifespan)


async def log_request_info(request: Request, call_next):
    client_host = request.client.host if request.client else "Unknown"
    app_logger.info(f"Incoming request: {request.method} {request.url} from {client_host}")
    response = await call_next(request)
    return response


app.add_middleware(BaseHTTPMiddleware, dispatch=log_request_info)


# ===== routers (拆分后的模块) =====
from routers.auth import router as auth_router  # noqa: E402
from routers.calendar import router as calendar_router  # noqa: E402
from routers.groups import router as groups_router  # noqa: E402
from routers.messages import router as messages_router  # noqa: E402
from routers.misc import router as misc_router  # noqa: E402
from routers.schools import router as schools_router  # noqa: E402
from routers.scores import router as scores_router  # noqa: E402
from routers.seat_schedule import router as seat_schedule_router  # noqa: E402
from routers.teachers import router as teachers_router  # noqa: E402
from routers.tencent import router as tencent_router  # noqa: E402
from routers.temp_rooms import router as temp_rooms_router  # noqa: E402
from routers.users import router as users_router  # noqa: E402
from ws.endpoint import router as ws_router  # noqa: E402


app.include_router(auth_router)
app.include_router(calendar_router)
app.include_router(misc_router)
app.include_router(scores_router)
app.include_router(seat_schedule_router)
app.include_router(temp_rooms_router)
app.include_router(groups_router)
app.include_router(tencent_router)
app.include_router(messages_router)
app.include_router(users_router)
app.include_router(schools_router)
app.include_router(teachers_router)
app.include_router(ws_router)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "5000"))
    reload_flag = os.getenv("RELOAD", "true").lower() in {"1", "true", "yes", "y"}

    print(f"服务已启动: http://{host}:{port}")
    uvicorn.run("app:app", host=host, port=port, reload=reload_flag)



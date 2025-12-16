import ast
from pathlib import Path
import shutil

APP = Path('app.py')
lines = APP.read_text(encoding='utf-8', errors='ignore').splitlines(True)
mod = ast.parse(''.join(lines))

ws_node = None
for node in mod.body:
    if isinstance(node, ast.AsyncFunctionDef) and node.name == 'websocket_endpoint':
        ws_node = node
        break
if not ws_node:
    raise SystemExit('websocket_endpoint not found')

starts = [ws_node.lineno]
for d in ws_node.decorator_list:
    if getattr(d, 'lineno', None):
        starts.append(d.lineno)
start = min(starts)
end = ws_node.end_lineno

chunk = ''.join(lines[start-1:end])
chunk = chunk.replace('@app.websocket', '@router.websocket')

header = """\
import asyncio
import datetime
import json
import os
import random
import shutil
import ssl
import struct
import time
import time as time_module
import traceback
import urllib
import uuid
from typing import Any, Dict, List, Optional

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    httpx = None
    HAS_HTTPX = False

import mysql.connector
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from common import app_logger
from db import get_db_connection
from realtime.srs import (
    SRS_APP,
    SRS_BASE_URL,
    SRS_HTTPS_PORT,
    SRS_PORT,
    SRS_SERVER,
    SRS_USE_HTTPS,
    SRS_WEBRTC_API_URL,
)
from ws.helpers import convert_datetime, convert_group_type_to_int, notify_temp_room_closed
from ws.manager import (
    active_temp_rooms,
    connections,
    safe_close,
    safe_del,
    safe_send_bytes,
    safe_send_text,
)

router = APIRouter()


async def notify_tencent_group_sync(*args, **kwargs):
    # 寤惰繜瀵煎叆锛岄伩鍏?ws.endpoint -> app 寰幆瀵煎叆
    from app import notify_tencent_group_sync as _impl
    return await _impl(*args, **kwargs)

"""

OUT = Path('ws'); OUT.mkdir(exist_ok=True)
(OUT / 'endpoint.py').write_text(header + chunk, encoding='utf-8')

bak = APP.with_suffix('.py.ws.bak')
if not bak.exists():
    shutil.copyfile(APP, bak)

# delete from app.py
lines2 = lines[:]
del lines2[start-1:end]

# add router include
text2 = ''.join(lines2)
lines2 = text2.splitlines(True)
import_line = 'from ws.endpoint import router as ws_router\n'
include_line = 'app.include_router(ws_router)\n'

if import_line not in lines2:
    # insert after other routers imports
    insert_at = None
    for i, ln in enumerate(lines2):
        if ln.startswith('from routers.'):
            insert_at = i + 1
    if insert_at is None:
        for i, ln in enumerate(lines2):
            if ln.startswith('app = FastAPI'):
                insert_at = i + 1
                break
    if insert_at is None:
        insert_at = 0
    lines2.insert(insert_at, import_line)

if include_line not in lines2:
    insert_at = None
    for i, ln in enumerate(lines2):
        if ln.strip() == 'app.include_router(temp_rooms_router)':
            insert_at = i + 1
            break
    if insert_at is None:
        for i, ln in enumerate(lines2):
            if ln.strip().startswith('app.include_router('):
                insert_at = i + 1
    if insert_at is None:
        insert_at = 0
    lines2.insert(insert_at, include_line)

APP.write_text(''.join(lines2), encoding='utf-8')

print('moved websocket_endpoint lines', start, end)
print('wrote ws/endpoint.py and updated app.py; backup:', bak)

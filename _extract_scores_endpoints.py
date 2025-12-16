import ast
from pathlib import Path
import shutil

APP = Path('app.py')
SRC = APP.read_text(encoding='utf-8', errors='ignore').splitlines(True)
module = ast.parse(''.join(SRC))

want = {
  'api_save_student_scores',
  'api_get_student_scores',
  'api_get_student_score',
  'api_set_student_score_comment',
  'api_set_student_score_value',
  'api_save_group_scores',
  'api_get_group_scores',
}

nodes = []
for node in module.body:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in want:
        if not getattr(node, 'end_lineno', None):
            raise RuntimeError('Missing end_lineno; need Python 3.8+')
        starts = [node.lineno]
        for d in node.decorator_list:
            if getattr(d, 'lineno', None):
                starts.append(d.lineno)
        start = min(starts)
        end = node.end_lineno
        nodes.append((node.name, start, end))

missing = want - {n for n,_,_ in nodes}
if missing:
    raise SystemExit(f'missing endpoint defs in app.py: {missing}')

nodes.sort(key=lambda x: x[1])

# write routers/scores.py
routers_dir = Path('routers'); routers_dir.mkdir(exist_ok=True)
out = [
  'import asyncio\n',
  'import datetime\n',
  'import json\n',
  'import os\n',
  'import time\n',
  'import traceback\n',
  'from typing import Any, Dict, List, Optional\n',
  '\n',
  'import mysql.connector\n',
  'from fastapi import APIRouter, Query, Request, UploadFile\n',
  'from fastapi.responses import JSONResponse\n',
  '\n',
  'from common import app_logger, safe_json_response\n',
  'from db import get_db_connection\n',
  'from services.scores import parse_excel_file_url, save_student_scores, save_group_scores\n',
  'from services.oss_upload import upload_excel_to_oss\n',
  '\n\n',
  'router = APIRouter()\n\n\n',
]

for name, start, end in nodes:
    chunk = ''.join(SRC[start-1:end])
    chunk = chunk.replace('@app.', '@router.')
    out.append(chunk)
    if not chunk.endswith('\n'):
        out.append('\n')
    out.append('\n\n')

(routers_dir / 'scores.py').write_text(''.join(out), encoding='utf-8')

# backup app.py
bak = APP.with_suffix('.py.scores.bak')
if not bak.exists():
    shutil.copyfile(APP, bak)

# remove endpoint blocks from app.py
SRC2 = SRC[:]
for _, start, end in sorted(nodes, key=lambda x: x[1], reverse=True):
    del SRC2[start-1:end]

text2 = ''.join(SRC2)
lines2 = text2.splitlines(True)

# ensure router import/include
import_line = 'from routers.scores import router as scores_router\n'
include_line = 'app.include_router(scores_router)\n'

# insert import in router import section
if import_line not in lines2:
    # find last existing "from routers." import in the router block
    insert_at = None
    for i, ln in enumerate(lines2):
        if ln.startswith('from routers.'):
            insert_at = i + 1
    if insert_at is None:
        # fallback: after app = FastAPI(...)
        for i, ln in enumerate(lines2):
            if ln.startswith('app = FastAPI'):
                insert_at = i + 1
                break
    if insert_at is None:
        insert_at = 0
    lines2.insert(insert_at, import_line)

# insert include near other include_router
if include_line not in lines2:
    insert_at = None
    for i, ln in enumerate(lines2):
        if ln.strip() == 'app.include_router(misc_router)':
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

print('moved endpoints:', nodes)
print('wrote routers/scores.py and updated app.py; backup:', bak)

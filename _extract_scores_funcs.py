import ast
from pathlib import Path
import shutil

APP = Path("app.py")
SRC = APP.read_text(encoding="utf-8", errors="ignore").splitlines(True)  # keep newlines
mod = ast.parse("".join(SRC))

want = {"parse_excel_file_url","save_student_scores","save_group_scores"}
func_nodes = []
for node in mod.body:
    if isinstance(node, ast.FunctionDef) and node.name in want:
        if not hasattr(node, "end_lineno") or node.end_lineno is None:
            raise RuntimeError("Python AST missing end_lineno; need Python 3.8+")
        func_nodes.append((node.name, node.lineno, node.end_lineno))

missing = want - {n for n,_,_ in func_nodes}
if missing:
    raise SystemExit(f"missing functions in app.py: {missing}")

# extract in original order
func_nodes.sort(key=lambda x: x[1])

header = [
    '"""Scores-related DB helpers extracted from app.py to reduce module size."""\n',
    'import datetime\n',
    'import json\n',
    'import os\n',
    'import time\n',
    'import traceback\n',
    'from typing import Any, Dict, List, Optional\n',
    '\n',
    'import mysql.connector\n',
    '\n',
    'from common import app_logger\n',
    'from db import get_db_connection\n',
    '\n\n',
]

extracted = []
for name, start, end in func_nodes:
    extracted.extend(SRC[start-1:end])
    extracted.append('\n\n')

services_dir = Path('services')
services_dir.mkdir(exist_ok=True)
(services_dir / '__init__.py').write_text('', encoding='utf-8')
(services_dir / 'scores.py').write_text(''.join(header + extracted), encoding='utf-8')

# backup app.py
bak = APP.with_suffix('.py.bak')
if not bak.exists():
    shutil.copyfile(APP, bak)

# remove function blocks from app.py (reverse order to keep indices)
SRC2 = SRC[:]
for name, start, end in sorted(func_nodes, key=lambda x: x[1], reverse=True):
    del SRC2[start-1:end]

# ensure import line exists near common/db imports
text2 = ''.join(SRC2)
lines2 = text2.splitlines(True)
insert_line = 'from services.scores import parse_excel_file_url, save_student_scores, save_group_scores\n'
if insert_line not in text2:
    idx = None
    for i, ln in enumerate(lines2):
        if ln.startswith('from common import app_logger'):
            idx = i + 1
            break
    if idx is None:
        for i, ln in enumerate(lines2):
            if ln.startswith('from db import get_db_connection'):
                idx = i + 1
                break
    if idx is None:
        idx = 0
    lines2.insert(idx, insert_line)

APP.write_text(''.join(lines2), encoding='utf-8')

print('extracted:', func_nodes)
print('wrote services/scores.py and updated app.py; backup:', bak)

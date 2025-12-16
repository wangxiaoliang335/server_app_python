import ast
from pathlib import Path
import shutil

APP = Path('app.py')
SRC = APP.read_text(encoding='utf-8', errors='ignore').splitlines(True)
module = ast.parse(''.join(SRC))

target = None
for node in module.body:
    if isinstance(node, ast.FunctionDef) and node.name == 'upload_excel_to_oss':
        if not getattr(node, 'end_lineno', None):
            raise RuntimeError('Missing end_lineno')
        starts = [node.lineno]
        for d in node.decorator_list:
            if getattr(d, 'lineno', None):
                starts.append(d.lineno)
        target = (min(starts), node.end_lineno)
        break

if not target:
    print('upload_excel_to_oss not found; nothing to do')
    raise SystemExit(0)

start, end = target
bak = APP.with_suffix('.py.oss_excel.bak')
if not bak.exists():
    shutil.copyfile(APP, bak)

SRC2 = SRC[:]
del SRC2[start-1:end]
APP.write_text(''.join(SRC2), encoding='utf-8')
print('removed upload_excel_to_oss lines', start, end, '; backup:', bak)

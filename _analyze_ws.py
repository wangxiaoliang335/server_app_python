import ast
from pathlib import Path

src = Path('app.py').read_text(encoding='utf-8', errors='ignore')
mod = ast.parse(src)

def find_func(name):
    for node in mod.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None

fn = find_func('websocket_endpoint')
if not fn:
    raise SystemExit('websocket_endpoint not found')

# collect local defs (assigned names)
local = set()
params = {a.arg for a in fn.args.args + fn.args.posonlyargs + fn.args.kwonlyargs}
if fn.args.vararg: params.add(fn.args.vararg.arg)
if fn.args.kwarg: params.add(fn.args.kwarg.arg)

class LocalCollector(ast.NodeVisitor):
    def visit_FunctionDef(self, node):
        local.add(node.name)
        # don't walk into nested functions
    def visit_AsyncFunctionDef(self, node):
        local.add(node.name)
    def visit_ClassDef(self, node):
        local.add(node.name)
    def visit_Assign(self, node):
        for t in node.targets:
            for n in ast.walk(t):
                if isinstance(n, ast.Name):
                    local.add(n.id)
        self.generic_visit(node.value)
    def visit_AnnAssign(self, node):
        for n in ast.walk(node.target):
            if isinstance(n, ast.Name):
                local.add(n.id)
        if node.value:
            self.generic_visit(node.value)
    def visit_AugAssign(self, node):
        for n in ast.walk(node.target):
            if isinstance(n, ast.Name):
                local.add(n.id)
        self.generic_visit(node.value)
    def visit_For(self, node):
        for n in ast.walk(node.target):
            if isinstance(n, ast.Name):
                local.add(n.id)
        self.generic_visit(node.iter)
        for s in node.body:
            self.visit(s)
        for s in node.orelse:
            self.visit(s)
    def visit_AsyncFor(self, node):
        self.visit_For(node)
    def visit_With(self, node):
        for item in node.items:
            if item.optional_vars:
                for n in ast.walk(item.optional_vars):
                    if isinstance(n, ast.Name):
                        local.add(n.id)
            self.generic_visit(item.context_expr)
        for s in node.body:
            self.visit(s)
    def visit_AsyncWith(self, node):
        self.visit_With(node)
    def visit_ExceptHandler(self, node):
        if node.name:
            local.add(node.name)
        for s in node.body:
            self.visit(s)

LocalCollector().visit(fn)

# collect names loaded
loaded = set()
class LoadCollector(ast.NodeVisitor):
    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load):
            loaded.add(node.id)

LoadCollector().visit(fn)

builtins = set(dir(__builtins__))
free = sorted(x for x in loaded if x not in local and x not in params and x not in builtins)

print('websocket_endpoint lines:', fn.lineno, fn.end_lineno)
print('free var count:', len(free))
for x in free:
    print(x)

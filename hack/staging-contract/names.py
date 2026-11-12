"""One table of object names against the three implementations of name_rule (see README.md):
the runner's (runner/kaos_hpc_runner/staging.py), the in-job stage-in's (runner/kaos_hpc_runner/payload/stagein.py)
and the staging tool's (stager.py of the platform repository). Also checks that the two copies of the table,
names.json here and next to the staging tool, are the same file.

usage: names.py <runner dir> <dir of the staging tool (stager.py, names.json)>
"""
import ast, json, os, sys
runner, tool = sys.argv[1:3]
here = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(here, 'names.json'), 'rb') as f:
    mine = f.read()
with open(os.path.join(tool, 'names.json'), 'rb') as f:
    theirs = f.read()
ok = mine == theirs
print('%s: names.json is the same file in both repositories' % ('PASS' if ok else 'FAIL'))
sys.path.insert(0, runner)
sys.path.insert(0, tool)
from kaos_hpc_runner import staging   # noqa: E402
import stager                         # noqa: E402


def function_source(path, name):
    """The text of one top-level function of a file (the stage-in is a program and cannot be imported)."""
    with open(path) as f:
        src = f.read()
    node = next(n for n in ast.parse(src).body if isinstance(n, ast.FunctionDef) and n.name == name)
    return '\n'.join(src.splitlines()[node.lineno - 1:node.end_lineno])


stagein = {}
exec(function_source(os.path.join(runner, 'kaos_hpc_runner', 'payload', 'stagein.py'), 'name_rule'), stagein)
rules = (('runner', staging.name_rule), ('stage-in', stagein['name_rule']), ('staging tool', stager.name_rule))
table = json.loads(mine.decode())['names']
for who, rule in rules:
    wrong = [(p[:40], s, v, rule(p, s)) for p, s, v in table if rule(p, s) != v]
    print('%s: %s agrees with all %d names%s' % ('FAIL' if wrong else 'PASS', who, len(table),
                                                  ': %r' % wrong[:3] if wrong else ''))
    ok = ok and not wrong


def body(fn_src):
    # the lines of the function after its docstring: the rule itself
    node = ast.parse(fn_src).body[0]
    return ast.dump(ast.Module(body=node.body[1:], type_ignores=[]))


same = len({body(function_source(p, 'name_rule')) for p in (
    os.path.join(runner, 'kaos_hpc_runner', 'staging.py'),
    os.path.join(runner, 'kaos_hpc_runner', 'payload', 'stagein.py'), os.path.join(tool, 'stager.py'))}) == 1
print('%s: the three name_rule functions are the same code' % ('PASS' if same else 'FAIL'))
sys.exit(0 if ok and same else 1)

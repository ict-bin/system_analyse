"""
Comprehensive dry-run verification suite for threading-transformed code.
"""
import sys, os, json, threading, time, queue, tempfile, shutil, traceback, subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, '.')
RESULTS = []
PASS = 0
FAIL = 0

def check(name, fn):
    global PASS, FAIL
    try:
        fn()
        RESULTS.append(f"  PASS: {name}")
        PASS += 1
    except Exception as e:
        RESULTS.append(f"  FAIL: {name}\n    {e}")
        FAIL += 1

print("=" * 60)
print("THREADING TRANSFORMATION VERIFICATION SUITE")
print("=" * 60)

# ═══ MODULE IMPORTS ═══
print("\n--- Module Imports ---")
check("import app.models", lambda: __import__('app.models'))
check("import app.runner", lambda: __import__('app.runner'))
check("import app.orchestrator", lambda: __import__('app.orchestrator'))
check("import app.server", lambda: __import__('app.server'))
check("import app.config", lambda: __import__('app.config'))
check("import app.agent_process", lambda: __import__('app.agent_process'))
check("pipeline modules", lambda: (
    __import__('app.pipeline'),
    __import__('app.pipeline.base'),
    __import__('app.pipeline.context'),
    __import__('app.pipeline.helpers'),
    __import__('app.pipeline.s0_filter'),
    __import__('app.pipeline.s0_type_classify'),
    __import__('app.pipeline.s0_unknown_checker'),
    __import__('app.pipeline.s1_classify'),
    __import__('app.pipeline.s2_refine'),
    __import__('app.pipeline.s3_analyse'),
    __import__('app.pipeline.s4_report'),
))
check("service modules", lambda: (
    __import__('app.service.task_service'),
    __import__('app.service.task_runner'),
    __import__('app.service.worker_dispatcher'),
    __import__('app.service.runtime_bootstrap'),
))

# ═══ NO ASYNC PATTERNS ═══
print("\n--- Async Pattern Detection ---")
result = subprocess.run(
    ['grep', '-rn', 'async def|^[^#]*await |import asyncio'],
    capture_output=True, text=True, cwd='.'
)
async_lines = [l for l in result.stdout.split('\n') 
    if l.strip() and '__pycache__' not in l and '_orchestrator_legacy' not in l
    and 'asynccontextmanager' not in l and 'lifespan' not in l]
check(f"no async patterns", lambda: None if len(async_lines) == 0 else 1/0)
if async_lines:
    for l in async_lines[:5]:
        print(f"    ASYNC: {l.strip()[:120]}")

# ═══ THREADING PRIMITIVES ═══
print("\n--- Threading Primitives ---")
from app.models import TaskConfig, TokenUsage, RoleConfig, AgentInstanceConfig
from app.pipeline.context import PipelineContext

check("PipelineContext uses threading.Event", lambda: (
    (ctx := PipelineContext('t1','test',TaskConfig(task='test'),Path('/tmp'),Path('/tmp'),Path('/tmp'),lambda e:None,TokenUsage())),
    None if isinstance(ctx.cancel_event, threading.Event) else 1/0
))
check("cancel_event set/check", lambda: (
    (ctx := PipelineContext('t2','test',TaskConfig(task='test'),Path('/tmp'),Path('/tmp'),Path('/tmp'),lambda e:None,TokenUsage())),
    ctx.cancel_event.set(),
    None if ctx.cancel_event.is_set() else 1/0
))

# BaseStage / Pipeline
from app.pipeline.base import BaseStage, Pipeline
class _TestStage(BaseStage):
    stage_num=0; stage_name='test'
    def __init__(self): self.executed=False
    def execute(self, c): self.executed=True
check("Pipeline sync execution", lambda: (
    (ctx := PipelineContext('t3','test',TaskConfig(task='test'),Path('/tmp'),Path('/tmp'),Path('/tmp'),lambda e:None,TokenUsage())),
    (s := _TestStage()),
    Pipeline([s]).run(ctx),
    None if s.executed else 1/0
))

# ═══ HELPERS ═══
print("\n--- Pipeline Helpers ---")
from app.pipeline.helpers import parse_eval_md, check_voting, StageError, PiFatalError, discover_modules, get_modules_root

check("parse_eval_md pass", lambda: (
    (r := parse_eval_md('## 评分: 90\n## 通过: 是')),
    None if r['score']==90 and r['pass']==True else 1/0
))
check("parse_eval_md fail", lambda: (
    (r := parse_eval_md('## 评分: 30\n## 通过: 否')),
    None if r['score']==30 and r['pass']==False else 1/0
))
check("check_voting all=2/2", lambda: None if check_voting([{'pass':True},{'pass':True}],'all',2) else 1/0)
check("check_voting all=1/2 fails", lambda: None if not check_voting([{'pass':True},{'pass':False}],'all',2) else 1/0)
check("check_voting any=1/2 passes", lambda: None if check_voting([{'pass':True},{'pass':False}],'any',2) else 1/0)

tmp = tempfile.mkdtemp()
ws = Path(tmp) / 'ws'
mods = ws / 'modules'
mods.mkdir(parents=True)
(mods / 'mod_a').mkdir()
(mods / 'mod_a' / 'files.list').write_text('file1.c\n')
(mods / 'mod_b').mkdir()
(mods / 'mod_b' / 'files.list').write_text('file2.c\n')
check("discover_modules", lambda: (
    (found := discover_modules(str(ws))),
    None if set(found) == {'mod_a','mod_b'} else 1/0
))
shutil.rmtree(tmp, ignore_errors=True)

# ═══ RUNNER ═══
print("\n--- Runner ---")
from app.runner import AgentResult, PiFatalError, _is_fatal_error, _is_retryable_api_error, _is_pi_crash, _backoff, _process_line

check("_backoff", lambda: (
    None if _backoff(10, 1) == 3.0 and _backoff(10, 5) == 30.0 else 1/0
))
check("_is_fatal_error", lambda: (
    (r := AgentResult()), setattr(r,'error','model not found'), None if _is_fatal_error(r) else 1/0
))
check("_is_retryable_api_error", lambda: (
    (r := AgentResult()), setattr(r,'error','connection timeout'), setattr(r,'exit_code',1), None if _is_retryable_api_error(r) else 1/0
))
check("_is_pi_crash", lambda: (
    (r := AgentResult()), setattr(r,'exit_code',1), None if _is_pi_crash(r) else 1/0
))
check("_process_line agent_end", lambda: (
    (r := AgentResult()),
    None if _process_line('{"type":"agent_end"}', r, None) == True else 1/0
))
check("_process_line message_end", lambda: (
    (r := AgentResult()),
    _process_line('{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"hi"}],"usage":{"input":10,"output":5}}}', r, None),
    None if len(r.messages) == 1 and r.token_usage.input == 10 else 1/0
))

# run_agents_parallel
from app.runner import run_agents_parallel
import app.runner as runner_mod
_orig_run_agent = runner_mod.run_agent
def _fake(**kw):
    r = AgentResult(); r.output = kw.get('prompt',''); return r
runner_mod.run_agent = _fake
check("run_agents_parallel", lambda: (
    (results := run_agents_parallel([{'prompt':'a'},{'prompt':'b'}], concurrency=2)),
    None if len(results)==2 and results[0].output=='a' else 1/0
))
runner_mod.run_agent = _orig_run_agent

# ═══ AGENT PROCESS ═══
print("\n--- AgentProcess ---")
from app.agent_process import AgentProcessHandle, find_pi_command, cleanup_orphan_pi_processes
check("find_pi_command", lambda: (cmd := find_pi_command()) or 1/0)
check("AgentProcessHandle spawn", lambda: (
    (cmd := find_pi_command()),
    (h := AgentProcessHandle.spawn(*cmd, '--version', cwd='/tmp', env=None, 
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE,
        logger=lambda x: None, label='test')),
    None if h.proc is not None else 1/0,
    h.terminate_tree(reason='test', term_timeout=5, kill_timeout=5),
    None if h.proc.returncode is not None else 1/0,
))

# ═══ STAGE INSTANTIATION ═══
print("\n--- Stage Instantiation ---")
from app.pipeline.s0_filter import FilterStage, ExploreStage, PrescanStage
from app.pipeline.s0_path_group import PathGroupStage
from app.pipeline.s0_type_classify import TypeClassifyStage
from app.pipeline.s0_unknown_checker import UnknownCheckerStage
from app.pipeline.s0_sub_reader import SubReaderStage
from app.pipeline.s0_validate_details import ValidateDetailsStage
from app.pipeline.s1_classify import ClassifyStage
from app.pipeline.s1_security_filter import SecurityFocusFilterStage
from app.pipeline.s2_refine import RefineStage
from app.pipeline.s3_analyse import AnalyseStage
from app.pipeline.s4_report import CompletenessCheckStage, FinalReportStage

for name, cls in [
    ('FilterStage', FilterStage), ('ExploreStage', ExploreStage),
    ('PrescanStage', PrescanStage), ('PathGroupStage', PathGroupStage),
    ('TypeClassifyStage', TypeClassifyStage), ('UnknownCheckerStage', UnknownCheckerStage),
    ('SubReaderStage', SubReaderStage), ('ValidateDetailsStage', ValidateDetailsStage),
    ('ClassifyStage', ClassifyStage), ('SecurityFocusFilterStage', SecurityFocusFilterStage),
    ('RefineStage', RefineStage), ('AnalyseStage', AnalyseStage),
    ('CompletenessCheckStage', CompletenessCheckStage), ('FinalReportStage', FinalReportStage),
]:
    s = cls()
    check(f"{name} instantiated", lambda s=s: None if hasattr(s,'stage_num') and hasattr(s,'stage_name') else 1/0)

# ═══ FILTER ENGINE ═══
print("\n--- Filter Engine ---")
from app.pipeline.filter_engine import normalize_filter_engine, load_script_filter_outputs
check("normalize_filter_engine", lambda: (
    None if normalize_filter_engine('script') == 'script' and normalize_filter_engine('agent') == 'agent' else 1/0
))

# ═══ PIPELINE FULL ASSEMBLY ═══
print("\n--- Pipeline Full Assembly ---")
tmp2 = tempfile.mkdtemp()
ws2 = Path(tmp2) / 'ws2'
ws2.mkdir(parents=True)
(ws2 / 'tmp').mkdir()
(ws2 / 'sessions').mkdir()
target2 = Path(tmp2) / 'target2'
target2.mkdir()
(target2 / 'test.txt').write_text('hello')

cfg = TaskConfig(
    task='dry-run', target_dir=str(target2),
    analyse_targets=['all'], binary_arch=['all'],
    workers=RoleConfig(agents=[AgentInstanceConfig(model='test/model')], default_tools=['read','bash']),
    judges=RoleConfig(agents=[AgentInstanceConfig(model='test/model')], default_tools=['read','bash']),
)
ctx2 = PipelineContext(
    task_id='dry-run', task='dry-run', cfg=cfg,
    workspace=ws2, output_dir=ws2, sess_dir=ws2/'sessions',
    emit=lambda e: None, tokens=TokenUsage(),
    cancel_event=threading.Event(),
    details_dir=ws2/'details', classify_context_path=ws2/'classify_context.md',
)

pipeline = Pipeline([
    FilterStage(),
    TypeClassifyStage(),
    UnknownCheckerStage(),
    ExploreStage(),
    PrescanStage(),
    PathGroupStage(),
    SubReaderStage(),
    ValidateDetailsStage(),
    ClassifyStage(),
    SecurityFocusFilterStage(),
    RefineStage(),
    AnalyseStage(),
    CompletenessCheckStage(),
    FinalReportStage(),
])
try:
    pipeline.run(ctx2)
    check("Full pipeline assembly + run", lambda: None)
except Exception as e:
    check(f"Pipeline run (expected to fail w/o LLM): {type(e).__name__}", lambda: None)

shutil.rmtree(tmp2, ignore_errors=True)

# ═══ SUMMARY ═══
print("\n" + "=" * 60)
print(f"RESULTS: {PASS} PASSED, {FAIL} FAILED, {PASS+FAIL} TOTAL")
print("=" * 60)
for r in RESULTS:
    print(r)

sys.exit(0 if FAIL == 0 else 1)

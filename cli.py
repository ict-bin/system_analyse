#!/usr/bin/env python3
"""system_analyse CLI — 四阶段流水线"""

from __future__ import annotations
import asyncio, os, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from app.config import build_task_config, load_service_config
from app.models import SwarmEvent
from app.orchestrator import Orchestrator

# ─── Stage 名映射 ────────────────────────────────────────────────────────────

_STAGE_NAMES = {
    1: "分类", "1": "分类",
    2: "细分", "2": "细分",
    3: "分析", "3": "分析",
    "explore": "探索目录",
    "prescan": "预扫描",
    "filter": "文件过滤",
    "2-sub": "读取",
    "2-redo": "重分类",
    "3-redo": "重分析",
    "4a": "完整性检查",
    "4b": "生成报告",
    "2-redo-s4": "补做细分",
    "3-redo-s4": "补做分析",
}

def _sname(stage) -> str:
    return _STAGE_NAMES.get(stage, str(stage))

def _fmt_dur(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.0f}m{seconds%60:.0f}s"
    else:
        return f"{seconds/3600:.0f}h{(seconds%3600)/60:.0f}m"


# ─── 状态跟踪 ────────────────────────────────────────────────────────────────

class CLIState:
    def __init__(self):
        self.current_stage = None
        self.current_module = None
        self.module_start = 0.0         # 当前模块开始时间
        self.task_start = 0.0
        # 缓存 stage_result，等 judge 结果出来后一起输出
        self.pending_result: dict | None = None

    def elapsed(self) -> str:
        return _fmt_dur(time.time() - self.task_start)

    def module_elapsed(self) -> str:
        if self.module_start:
            return _fmt_dur(time.time() - self.module_start)
        return ""

_st = CLIState()


def _flush_pending():
    """输出缓存的 stage_result（拆分行）。"""
    if _st.pending_result is None:
        return
    d = _st.pending_result
    _st.pending_result = None
    stage = d.get('stage')
    if stage == 1:
        modules = d.get('modules', [])
        count = d.get('module_count', len(modules))
        preview = ', '.join(modules[:6])
        if count > 6:
            preview += f" (+{count - 6})"
        print(f"    📂 {count} 个模块: {preview}")
    elif stage == "filter":
        types = d.get('types', [])
        arch = d.get('arch', 'all')
        fc = d.get('file_count', 0)
        arch_str = f" arch={arch}" if arch and arch != 'all' else ''
        print(f"    📁 {fc} 个文件 (types: {', '.join(types) if isinstance(types,list) else types}{arch_str})")
    elif stage == 2 or stage == "2-redo" or stage == "2-redo-s4":
        mod = d.get('module', '')
        if d.get('skipped'):
            fc = d.get('file_count', 0)
            print(f"  ▸ {mod} ({fc} files, 跳过)")
        elif d.get('split'):
            new = d.get('new_modules', [])
            names = ', '.join(new[:5])
            if len(new) > 5:
                names += f" (+{len(new)-5})"
            print(f"      ↳ 拆分 → {names}")
    elif stage == "2-sub":
        lines = d.get('file_count', 0)
        print(f"      📖 摘要完成 ({lines} 个文件)")


# ─── 渲染 ────────────────────────────────────────────────────────────────────

def render_event(event: SwarmEvent, quiet: bool = False):
    if quiet:
        return
    t = event.type
    d = event.data

    if t == "task_start":
        _st.task_start = time.time()
        print(f"\n{'─' * 60}")
        print(f"🚀 {d.get('task', '')[:100]}")
        print(f"{'─' * 60}")

    elif t == "stage":
        stage = d.get('stage')
        mod = d.get('module', '')
        att = d.get('attempt', 1)

        # Stage 切换时打标题
        if stage != _st.current_stage and stage != "2-sub":
            _flush_pending()
            _st.current_stage = stage
            print(f"\n{'━' * 60}")
            print(f"  📌 {_sname(stage)}    [{_st.elapsed()}]")
            print(f"{'━' * 60}")

        # 模块切换时打印开始提示
        if mod and mod != _st.current_module:
            _flush_pending()
            _st.current_module = mod
            _st.module_start = time.time()
            if stage != "2-sub":
                print(f"  ▸ {mod}", flush=True)

        # 子 Worker batch 进度（每批独立行，并行不互相覆盖）
        if stage == "2-sub":
            batch = d.get('batch', 0)
            total = d.get('total', 0)
            if batch and total:
                print(f"    📖 [{mod}] 读取 {batch}/{total}", flush=True)

    elif t == "stage_result":
        _st.pending_result = d
        if d.get('stage') == "2-sub":
            fc = d.get('file_count', 0)
            print(f"      📖 [{mod if mod else d.get('module','')}] 摘要完成 ({fc} 个文件)")
            _flush_pending()

    elif t == "judge_eval":
        _flush_pending()
        passed = d.get("passed")
        score = d.get('score', 0)
        judge = d.get('judge_id', 'judge-0')
        att_val = d.get('attempt', _st.module_start)  # fallback
        dur = _st.module_elapsed()

        mod_label = d.get('module', _st.current_module or '')
        prefix = f"  ▸ {mod_label}" if mod_label else "  "
        if passed:
            print(f"{prefix}  ✅ {judge}={score}  {dur}")
        else:
            att = d.get('attempt', 0)
            print(f"{prefix}  · {judge}={score} retry[{att}]")

    elif t == "reflect":
        print(f"    🔄 反思")

    elif t == "reclassify":
        _flush_pending()
        print(f"    ⚠️  需重分类: {d.get('module', '?')}")

    elif t == "stage_fail":
        _flush_pending()
        print(f"\n  ❌ {d.get('error', '')[:200]}", file=sys.stderr)

    elif t == "model":
        stage = d.get('stage', '')
        parts = [f"stage={stage}"]
        if 'worker' in d: parts.append(f"worker={d['worker'].split('/')[-1]}")
        if 'model' in d: parts.append(f"model={d['model'].split('/')[-1]}")
        if 'judge' in d: parts.append(f"judge={d['judge'].split('/')[-1]}")
        print(f"    🤖 {' | '.join(parts)}")

    elif t == "error":
        _flush_pending()
        print(f"\n  ❌ {d.get('error', '')[:200]}", file=sys.stderr)

    elif t == "task_end":
        _flush_pending()
        status = d.get('status', '').upper()
        icon = "✅" if status == "PASSED" else "❌"
        print(f"\n{'═' * 60}")
        print(f"  {icon} {status}    [{_st.elapsed()}]")
        print(f"{'═' * 60}")
        if d.get("report"):
            print(f"  📄 {d.get('report')}")
        if d.get("modules"):
            print(f"  📂 {d.get('modules')}")
        if d.get("archive"):
            print(f"  📦 {d.get('archive')}")


# ─── 主入口 ──────────────────────────────────────────────────────────────────

# 从环境变量读取路径配置
_CONFIG_DIR = os.environ.get("CONFIG_DIR", "/data/config")
_CONFIG_SEARCH = [
    f"{_CONFIG_DIR}/config.json",
    "/opt/system_analyse/config.example.json",
    "./config.json",
    "./config.example.json",
]


async def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("用法: python3 cli.py \"对解包后的所有文件进行威胁分析与模块分析\"")
        sys.exit(0)

    quiet = "--quiet" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    prompt = args[0] if args else ""
    if not prompt:
        print("错误：请提供分析任务描述", file=sys.stderr)
        sys.exit(1)

    config_path = None
    for i, a in enumerate(sys.argv):
        if a == "--config" and i + 1 < len(sys.argv):
            config_path = sys.argv[i + 1]
    if not config_path:
        for p in _CONFIG_SEARCH:
            if os.path.isfile(p):
                config_path = p
                break
    if not config_path:
        print("错误：找不到配置文件", file=sys.stderr)
        sys.exit(1)

    svc = load_service_config(config_path)
    cfg = build_task_config(svc, prompt)

    w = cfg.worker_count
    j = cfg.judge_count
    s = cfg.stages
    print(f"""
╔══════════════════════════════════════════════╗
║            system_analyse                    ║
╠══════════════════════════════════════════════╣
║  Workers: {w}    Judges: {j:<27} ║
║  分类: min={s.classify.min_rounds} max={s.classify.max_rounds:<3} {s.classify.pass_mode:<20} ║
║  细分: min={s.refine.min_rounds} max={s.refine.max_rounds:<3} {s.refine.pass_mode:<20} ║
║  分析: min={s.analyse.min_rounds} max={s.analyse.max_rounds:<3} {s.analyse.pass_mode:<20} ║
╚══════════════════════════════════════════════╝""")

    orch = Orchestrator(config=cfg, on_event=lambda e: render_event(e, quiet=quiet))
    result = await orch.execute()

    if not quiet:
        dur_str = _fmt_dur(result.total_duration_ms / 1000)
        print(f"\n  ⏱  {dur_str}    💰 ${result.total_tokens.cost:.4f}")

    sys.exit(0 if result.status.value == "passed" else 1)


if __name__ == "__main__":
    asyncio.run(main())

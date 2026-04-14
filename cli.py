#!/usr/bin/env python3
"""
system_analyse CLI

解包文件挂载到 /data/target（只读），配置挂载到 /data/config，输出挂载到 /data/output。
用户只需提供一句话任务描述。
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.config import build_task_config, load_service_config
from app.models import SwarmEvent
from app.orchestrator import Orchestrator


def render_event(event: SwarmEvent, quiet: bool = False):
    if quiet:
        return
    t = event.type
    d = event.data

    if t == "task_start":
        print(f"\n🚀 Task: {event.task_id}")
        print(f"   {d.get('task', '')[:120]}")
        for a in d.get("agents", []):
            print(f"   • {a}")
    elif t == "round_start":
        print(f"\n{'━' * 60}\n  Round {d.get('round')}\n{'━' * 60}")
    elif t == "worker_start":
        print(f"  🔧 {d.get('worker_id')} ({d.get('model', '')}) starting...")
    elif t == "worker_phase":
        phase = d.get('phase', '?')
        if phase == 'A':
            print(f"     Phase A done: {d.get('module_count', 0)} modules found")
        elif phase == 'refine':
            splits = d.get('split_into', [])
            print(f"     Refine: {d.get('module', '?')} → {', '.join(splits)}")
        elif phase == 'refine_done':
            print(f"     Refine done: {d.get('module_count', 0)} final modules")
        elif phase == 'B':
            print(f"     Phase B: {d.get('module', '?')} analyzed")
        else:
            print(f"     Phase {phase}: {d.get('module', '')}")
    elif t == "worker_done":
        mc = d.get('module_count', 0)
        mods = d.get('modules', [])
        suffix = ', '.join(mods[:5]) + ('...' if mc > 5 else '')
        print(f"  ✅ {d.get('worker_id')} done [{mc} modules: {suffix}]")
    elif t == "judge_start":
        print(f"  ⚖️  {d.get('judge_id')} ({d.get('model', '')}) evaluating...")
    elif t == "judge_step":
        step = d.get('step')
        icon = "✅" if d.get('passed') else "❌"
        if step == 1:
            print(f"     {icon} {d.get('judge_id')}→{d.get('worker_id')} Step1: classification")
        elif step == 2:
            print(f"     {icon} {d.get('judge_id')}→{d.get('worker_id')} "
                  f"Step2: {d.get('module', '?')} ({d.get('score')}/100)")
    elif t == "judge_eval":
        icon = "✅" if d.get("passed") else "❌"
        print(f"     {icon} {d.get('judge_id')}→{d.get('worker_id')}: "
              f"{'PASS' if d.get('passed') else 'FAIL'} ({d.get('score')}/100)")
    elif t == "judge_summary":
        print(f"     📊 {d.get('judge_id', '?')}: best={d.get('best')}, "
              f"passed={d.get('overall_passed')}")
    elif t == "round_end":
        s = "✅ PASSED" if d.get("passed") else "❌ FAILED"
        print(f"\n  ➜ {s}  ({d.get('pass_count')}/{d.get('total_judges')} judges)")
        if d.get("best_worker"):
            print(f"     Best: {d.get('best_worker')}")
    elif t == "round_reflection":
        print(f"  🔄 {d.get('message', 'Forcing reflection round')}")
    elif t == "task_end":
        print(f"\n{'═' * 60}")
        print(f"📋 {event.task_id}: {d.get('status', '').upper()}")
        if d.get("archive"):
            print(f"   📦 Archive: {d.get('archive')}")
        if d.get("result_file"):
            print(f"   📄 Result:  {d.get('result_file')}")
    elif t == "error":
        print(f"\n❗ Error: {d.get('error')}", file=sys.stderr)


CONFIG_SEARCH_PATHS = [
    "/data/config/config.json",
    "/opt/system_analyse/config.example.json",
    "./config.json",
    "./config.example.json",
]


def find_service_config() -> str:
    for p in CONFIG_SEARCH_PATHS:
        if os.path.isfile(p):
            return p
    raise FileNotFoundError(
        "找不到服务配置文件。请在以下位置之一放置 config.json：\n"
        + "\n".join(f"  - {p}" for p in CONFIG_SEARCH_PATHS)
    )


async def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("""用法:
  python3 cli.py "对解包后的所有文件进行威胁分析与模块分析"

解包文件挂载到 /data/target（只读），配置挂载到 /data/config，输出到 /data/output。

选项:
  --config <path>    指定服务配置文件（默认自动搜索）
  --quiet            安静模式
""")
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
        config_path = find_service_config()

    svc = load_service_config(config_path)
    cfg = build_task_config(svc, prompt)

    print(f"""
╔═══════════════════════════════════════════════════════════╗
║              system_analyse                               ║
╠═══════════════════════════════════════════════════════════╣
║  Target:  /data/target                                    ║
║  Workers: {cfg.worker_count:<5}  Judges: {cfg.judge_count:<33} ║
║  Rounds:  {cfg.min_rounds}~{cfg.max_rounds:<44} ║
╚═══════════════════════════════════════════════════════════╝""")
    for i, a in enumerate(cfg.workers.agents):
        print(f"  worker-{i}: {a.model}")
    for i, a in enumerate(cfg.judges.agents):
        print(f"  judge-{i}:  {a.model}")

    orch = Orchestrator(config=cfg, on_event=lambda e: render_event(e, quiet=quiet))
    result = await orch.execute()

    print(f"\n📊 Summary:")
    print(f"   Status:   {result.status.value}")
    print(f"   Rounds:   {len(result.rounds)}")
    print(f"   Duration: {result.total_duration_ms / 1000:.1f}s")
    print(f"   Cost:     ${result.total_tokens.cost:.4f}")

    sys.exit(0 if result.status.value == "passed" else 1)


if __name__ == "__main__":
    asyncio.run(main())

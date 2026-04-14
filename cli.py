#!/usr/bin/env python3
"""system_analyse CLI — 五阶段流水线"""

from __future__ import annotations
import asyncio, os, sys
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
    elif t == "stage":
        stage = d.get('stage')
        mod = d.get('module', '')
        att = d.get('attempt', 1)
        mod_str = f" [{mod}]" if mod else ""
        att_str = f" (attempt {att})" if att > 1 else ""
        print(f"\n  ▶ Stage {stage}{mod_str}{att_str}")
    elif t == "stage_result":
        stage = d.get('stage')
        if stage == 1:
            print(f"    → {d.get('module_count', 0)} modules: {', '.join(d.get('modules', [])[:8])}")
        elif stage == 3:
            if d.get('split'):
                print(f"    → Split: {d.get('module', '?')} → {', '.join(d.get('new_modules', []))}")
            else:
                print(f"    → No split needed")
        elif stage == 4:
            print(f"    → Analyzed: {d.get('module', '?')}")
    elif t == "stage_fail":
        print(f"\n  ❗ FAILED: {d.get('error', d.get('message', ''))}", file=sys.stderr)
    elif t == "judge_eval":
        icon = "✅" if d.get("passed") else "❌"
        mod = f" [{d.get('module')}]" if d.get('module') else ""
        print(f"    {icon} {d.get('judge_id', '?')} S{d.get('stage', '?')}{mod}: "
              f"{d.get('score', 0)}/100")
    elif t == "reclassify":
        print(f"  🔄 Reclassify needed: {d.get('module', '?')}")
    elif t == "reflect":
        mod = f" [{d.get('module')}]" if d.get('module') else ""
        rnd = d.get('round', 1)
        mn = d.get('min_rounds', '?')
        print(f"    🔍 Reflect S{d.get('stage', '?')}{mod} ({rnd}/{mn})")
    elif t == "task_end":
        print(f"\n{'═' * 60}")
        print(f"📋 {event.task_id}: {d.get('status', '').upper()}")
        if d.get("archive"):
            print(f"   📦 Archive: {d.get('archive')}")
        if d.get("result_file"):
            print(f"   📄 Result:  {d.get('result_file')}")
    elif t == "error":
        print(f"\n❗ Error: {d.get('error')}", file=sys.stderr)


CONFIG_SEARCH = ["/data/config/config.json", "/opt/system_analyse/config.example.json",
                 "./config.json", "./config.example.json"]


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
        for p in CONFIG_SEARCH:
            if os.path.isfile(p):
                config_path = p
                break
    if not config_path:
        print("错误：找不到配置文件", file=sys.stderr)
        sys.exit(1)

    svc = load_service_config(config_path)
    cfg = build_task_config(svc, prompt)

    print(f"""
╔═══════════════════════════════════════════════════════════╗
║              system_analyse                               ║
╠═══════════════════════════════════════════════════════════╣
║  Target:  /data/target                                    ║
║  Workers: {cfg.worker_count:<5}  Judges: {cfg.judge_count:<33} ║
║  Classify: min={cfg.stages.classify.min_rounds} max={cfg.stages.classify.max_rounds} mode={cfg.stages.classify.pass_mode:<15} ║
║  Refine:   min={cfg.stages.refine.min_rounds} max={cfg.stages.refine.max_rounds} mode={cfg.stages.refine.pass_mode:<15} ║
║  Analyse:  min={cfg.stages.analyse.min_rounds} max={cfg.stages.analyse.max_rounds} mode={cfg.stages.analyse.pass_mode:<15} ║
╚═══════════════════════════════════════════════════════════╝""")

    orch = Orchestrator(config=cfg, on_event=lambda e: render_event(e, quiet=quiet))
    result = await orch.execute()

    print(f"\n📊 Summary:")
    print(f"   Status:   {result.status.value}")
    print(f"   Duration: {result.total_duration_ms / 1000:.1f}s")
    print(f"   Cost:     ${result.total_tokens.cost:.4f}")
    sys.exit(0 if result.status.value == "passed" else 1)


if __name__ == "__main__":
    asyncio.run(main())

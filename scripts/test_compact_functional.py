#!/usr/bin/env python3
"""test_compact_functional.py — 真实触发 pi 原生 /compact 的功能测试。

背景：runner.py 修复前把"压缩"实现成发一条假 prompt（"请...只回复 COMPACTION_OK"），
pi 把它当用户消息，LLM 回一句 COMPACTION_OK，原生压缩从未执行 → 无限循环。
修复后改用 pi 的 `{"type":"compact"}` RPC 命令。

真实业务很难撞到压缩阈值，本脚本人为构造大上下文会话来验证：
  1. 用真实 LLM 跑 2 轮，每轮注入大段文本，把会话撑到几万 token
  2. 直接调用 _run_compact_command（发 pi 原生 compact RPC 命令）
  3. 断言：success=True / estimated_tokens_after < tokens_before /
     会话 JSONL 出现 CompactionEntry / 不会话里残留 "COMPACTION_OK" 假回复

运行方式（在 runner pod 内，pi + models.json 已就绪）：
    python3 scripts/test_compact_functional.py \\
        --model local_minimax/MiniMax/MiniMax-M2.5 \\
        --cwd /tmp/compact_test
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import runner
from app.logging_utils import configure_container_logging


def _build_large_filler(n_chars: int) -> str:
    """构造大段无意义但确定的文本撑大上下文（避免被模型过度精简）。"""
    unit = "这是一段用于撑大上下文窗口的测试填充文本，编号 %d。".encode("utf-8")
    out = []
    i = 0
    total = 0
    while total < n_chars:
        i += 1
        s = (unit.replace(b"%d", str(i).encode("utf-8"))).decode("utf-8") + "\n"
        out.append(s)
        total += len(s.encode("utf-8"))
    return "".join(out)


def _session_token_stats(session_file: str) -> int | None:
    """粗估会话当前 token 数（用 get_session_stats RPC 取 contextUsage.tokens）。"""
    import subprocess
    pi_cmd = runner._find_pi_command()
    args = runner._build_args(pi_cmd, model="", tools=[], thinking_level="off",
                              session_file=session_file)
    args = [a for a in args if a not in ("--model", "")]  # 用会话里已有模型
    try:
        proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, cwd="/tmp")
        cmd = json.dumps({"type": "get_session_stats"}) + "\n"
        proc.stdin.write(cmd.encode("utf-8")); proc.stdin.flush()
        tokens = None
        import select
        fd = proc.stdout.fileno()
        deadline = time.time() + 30
        while time.time() < deadline:
            r, _, _ = select.select([fd], [], [], 2.0)
            if not r:
                continue
            line = b""
            while b"\n" not in line:
                ch = proc.stdout.read(1)
                if not ch:
                    break
                line += ch
            if not line:
                break
            try:
                ev = json.loads(line.decode("utf-8"))
            except Exception:
                continue
            if ev.get("type") == "response" and ev.get("command") == "get_session_stats":
                cu = (ev.get("data") or {}).get("contextUsage") or {}
                tokens = cu.get("tokens")
                break
        proc.stdin.close()
        proc.terminate()
        return tokens
    except Exception:
        return None


def _count_compaction_entries(session_file: str) -> int:
    p = Path(session_file)
    if not p.exists():
        return 0
    n = 0
    for ln in p.read_text("utf-8", errors="replace").splitlines():
        try:
            e = json.loads(ln)
        except Exception:
            continue
        if e.get("type") == "compaction":
            n += 1
    return n


def _session_has_compaction_ok(session_file: str) -> bool:
    p = Path(session_file)
    if not p.exists():
        return False
    return "COMPACTION_OK" in p.read_text("utf-8", errors="replace")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=os.environ.get("SA_TEST_MODEL", "local_minimax/MiniMax/MiniMax-M2.5"))
    ap.add_argument("--cwd", default="/tmp/compact_test")
    ap.add_argument("--filler-chars", type=int, default=60000,
                    help="每轮注入的填充文本字节数（默认 60k，约 1.5w token）")
    args = ap.parse_args()

    configure_container_logging("01-system_analyse")
    workdir = Path(args.cwd)
    workdir.mkdir(parents=True, exist_ok=True)
    session_file = str(workdir / "compact_test_session.jsonl")
    if os.path.exists(session_file):
        os.remove(session_file)

    print(f"[setup] model={args.model} cwd={workdir} session={session_file}")
    print(f"[setup] filler={args.filler_chars} bytes/轮")

    # ── 1. 跑 2 轮真实 LLM，每轮 prompt 里塞大段填充文本撑大上下文 ──
    tools = ["read"]
    for rnd in (1, 2):
        filler = _build_large_filler(args.filler_chars)
        prompt = (
            f"下面是一段测试填充文本，请用一句话概括其主旨（第{rnd}轮）：\n\n{filler}"
        )
        print(f"[round {rnd}] 注入 {len(filler.encode('utf-8'))} bytes 填充，调用 run_agent...")
        r = runner.run_agent(
            prompt,
            model=args.model,
            tools=tools,
            cwd=str(workdir),
            session_file=session_file,
            max_retries=-1,
            pi_max_retries=-1,
        )
        if r.error:
            print(f"[round {rnd}] 失败: {r.error[:300]}")
            return 2
        print(f"[round {rnd}] 完成，output 长度={len(r.output or '')}")

    # ── 2. 压缩前快照 ──
    before_tokens = _session_token_stats(session_file)
    before_entries = _count_compaction_entries(session_file)
    print(f"[before] contextUsage.tokens={before_tokens} compaction_entries={before_entries}")

    # ── 3. 直接调用 _run_compact_command（发 pi 原生 compact RPC 命令）──
    pi_cmd = runner._find_pi_command()
    print("[compact] 调用 _run_compact_command（pi 原生 compact RPC）...")
    t0 = time.time()
    result = runner._run_compact_command(
        pi_cmd=pi_cmd,
        model=args.model,
        tools=tools,
        thinking_level="off",
        session_file=session_file,
        cwd=str(workdir),
        env=None,
        cancel_event=None,
        max_retries=-1,
        retry_delay=3.0,
        pi_max_retries=-1,
        pi_retry_delay=5.0,
    )
    dt = time.time() - t0
    print(f"[compact] 返回: {result}  ({dt:.1f}s)")

    # ── 4. 断言 ──
    ok = True
    if not result.get("success"):
        print(f"[FAIL] compact 未成功: {result.get('error')}")
        ok = False
    after_tokens = result.get("estimated_tokens_after")
    before_reported = result.get("tokens_before")
    if before_reported and after_tokens and not (after_tokens < before_reported):
        print(f"[FAIL] 上下文未缩减: before={before_reported} after={after_tokens}")
        ok = False
    else:
        print(f"[OK] 上下文缩减: before={before_reported} after={after_tokens}")

    after_entries = _count_compaction_entries(session_file)
    if after_entries <= before_entries:
        print(f"[FAIL] 会话未新增 CompactionEntry: before={before_entries} after={after_entries}")
        ok = False
    else:
        print(f"[OK] 会话新增 CompactionEntry: {before_entries} -> {after_entries}")

    # 关键回归断言：会话里绝不能残留假"COMPACTION_OK"回复（旧 bug 的特征）
    if _session_has_compaction_ok(session_file):
        print("[FAIL] 会话里出现 COMPACTION_OK —— 退化成了假 prompt 压缩（旧 bug）")
        ok = False
    else:
        print("[OK] 会话无 COMPACTION_OK 残留（确认走的是原生 compact，非假 prompt）")

    print("\n=== 结果 ===")
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

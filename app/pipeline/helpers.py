"""
pipeline/helpers.py — 各阶段共用的底层函数
（从原 orchestrator.py 提取）
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

# ── 公共：运行 pi agent（带重试） ─────────────────────────────────────────────
from ..runner import run_agent, AgentResult  # noqa: E402


class StageError(Exception):
    pass


class PiFatalError(StageError):
    pass


def check_agent_result(ar: AgentResult, context: str = "") -> None:
    if ar.fatal:
        msg = f"pi 致命错误（不可重试）: {ar.error or ar.output or 'unknown'}"
        if context:
            msg = f"[{context}] {msg}"
        raise PiFatalError(msg)
    if ar.error and not ar.output:
        msg = f"pi 进程崩溃 (exit=1): {ar.error}"
        if context:
            msg = f"[{context}] {msg}"
        raise StageError(msg)


async def run_agent_checked(context: str = "", **kwargs) -> AgentResult:
    ar = await run_agent(**kwargs)
    check_agent_result(ar, context)
    return ar


# ── 模块目录发现 ──────────────────────────────────────────────────────────────

def get_modules_root(workspace: str | Path) -> Path:
    """返回 modules 子目录（若存在），否则返回 workspace 本身。"""
    workspace = Path(workspace)
    m = workspace / "modules"
    if m.is_dir():
        # 确认至少有一个模块有 files.list
        if any((m / d / "files.list").exists() for d in m.iterdir() if d.is_dir()):
            return m
    return workspace


def discover_modules(workspace: str | Path) -> list[str]:
    """返回 workspace 下所有有 files.list 的目录名（叶节点模块）。"""
    root = get_modules_root(str(workspace))
    result = []
    for d in sorted(root.iterdir()):
        if d.is_dir() and not d.name.startswith(".") and (d / "files.list").exists():
            result.append(d.name)
    return result


# ── Judge 输出解析 ─────────────────────────────────────────────────────────────

def parse_eval_md(output: str) -> dict:
    """
    解析 Judge 的 Markdown 输出，提取 score/pass/feedback。
    返回 {"score": int, "pass": bool, "feedback": str}
    """
    score = 0
    pass_val: bool | None = None
    feedback = output[:1000]

    # 查找 "## 评分: N" 或 "Score: N"（取最后一个）
    for m in re.finditer(r"(?:##\s*评分|Score)\s*[：:]\s*(\d+)", output, re.IGNORECASE):
        score = int(m.group(1))

    # 查找 "## 通过: 是/否" 或 "Pass: True/False"
    for m in re.finditer(
        r"(?:##\s*通过|Pass)\s*[：:]\s*(是|否|True|False)",
        output, re.IGNORECASE
    ):
        val = m.group(1).lower()
        pass_val = val in ("是", "true")

    if pass_val is None:
        pass_val = score >= 75

    # score=0 + 明确否 → 直接 fail
    if score == 0 and pass_val is False:
        return {"score": 0, "pass": False, "feedback": feedback}

    # RESULT:PASS 但没有 score → Judge 格式违规
    if pass_val is True and score == 0:
        return {"score": 0, "pass": False,
                "feedback": "Judge 格式违规：声明通过但评分为 0"}

    return {"score": score, "pass": pass_val, "feedback": feedback}


def check_voting(results: list[dict], pass_mode: str, judge_count: int) -> bool:
    """根据投票模式判断是否通过。"""
    passes = sum(1 for r in results if r.get("pass"))
    if pass_mode == "any":
        return passes >= 1
    elif pass_mode == "majority":
        return passes > judge_count / 2
    else:  # "all"
        return passes == judge_count


# ── prompt 加载 ────────────────────────────────────────────────────────────────

def load_prompt(prompt_dir: str, name: str) -> str:
    for ext in [".md", ".txt", ""]:
        p = Path(prompt_dir) / f"{name}{ext}"
        if p.exists():
            return p.read_text(encoding="utf-8").strip()
    return ""

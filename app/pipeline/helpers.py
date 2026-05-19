"""
pipeline/helpers.py — 各阶段共用的底层函数
（从原 orchestrator.py 提取）
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import PipelineContext

# ── 公共：运行 pi agent（带重试） ─────────────────────────────────────────────
from ..runner import run_agent, AgentResult  # noqa: E402

_DEFAULT_AGENT_HEARTBEAT_INTERVAL_SECONDS = max(
    5.0,
    float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_AGENT_HEARTBEAT_INTERVAL_SECONDS", "30")),
)
_DEFAULT_AGENT_TIMEOUT_SECONDS = max(
    60.0,
    float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_AGENT_TIMEOUT_SECONDS", "1800")),
)
_DEFAULT_MODEL_STUCK_TIMEOUT_SECONDS: float = max(
    30.0,
    float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_MODEL_STUCK_TIMEOUT_SECONDS", "300")),
)
_DEFAULT_MODEL_STUCK_MAX_RETRIES: int = max(
    1,
    int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_MODEL_STUCK_MAX_RETRIES", "3")),
)



class StageError(Exception):
    pass


class PiFatalError(StageError):
    pass


# ── 粒度约束工具 ────────────────────────────────────────────────────────────────

def build_granularity_hint(granularity: str) -> str:
    """生成粒度约束段落，统一追加到 S2/S3 Worker 和 Judge 的 system prompt 末尾。

    重要：不再使用“文件数量阈值”作为拆分/禁拆条件。

    粗粒度（coarse）— "一个协议 / 一个服务 / 一个守护进程 = 一个模块"
      拆分触发条件（同时满足全部）：
        1. 模块内存在完全独立的顶层协议或服务（不同 RFC / 不同守护进程）
        2. 两者之间无直接调用或数据依赖
        3. 拆分后边界更符合“一个协议/一个服务/一个 daemon”
      绝对禁止拆分的场景（任一满足则禁拆）：
        - 同协议的 client / server / config / parser / utils / v4-v6变体
        - 同协议族的子协议（OSPFv2 + OSPFv3、ICMPv4 + ICMPv6、AH + ESP）
        - 库文件 + 使用该库的协议实现代码
      口诀："同一个 RFC / 同一个 daemon" → 同一个模块

    细粒度（fine）— "一个组件做一件事 = 一个模块"
      拆分触发条件（满足任一）：
        1. 文件功能属于不同子系统（client/server/parser/config 等任意组合）
        2. 建议子模块显示出清晰、稳定、可命名的职责边界
      不拆分条件（满足任一）：
        1. 所有文件属于同一协议/功能
        2. 强行拆分会破坏职责内聚性
      口诀："一个组件做一件事" → 一个模块
    """
    if granularity == "coarse":
        return (
            "\n\n---\n"
            "# ⚠️ 粒度约束（粗粒度模式，最高优先级，覆盖上方所有拆分规则）\n\n"
            "当前任务配置为 **粗粒度（coarse）** 模式，模块边界 = 完整协议 / 完整服务 / 独立守护进程。\n\n"
            "## 拆分触发条件（三条必须同时满足）\n"
            "1. 模块内存在**完全独立的顶层协议或服务**（属于不同 RFC 标准或不同守护进程）\n"
            "2. 两者之间**无直接调用或数据依赖**\n"
            "3. 拆分后边界更符合 **一个协议 / 一个服务 / 一个 daemon**\n\n"
            "## 绝对禁止拆分（满足任一则禁拆）\n"
            "- 同协议的 client / server / config / parser / utils — 必须合并\n"
            "- 同协议族的子版本变体（OSPFv2 + OSPFv3 → `ospf`；ICMPv4 + ICMPv6 → `icmp`）\n"
            "- 库文件与使用该库的协议实现（如 libssl + TLS 握手代码）\n"
            "- **上方 prompt 中基于文件数的拆分/禁拆规则在粗粒度模式下全部无效，必须忽略**\n\n"
            "## 正确示范 ✅\n"
            "- `ssh`（ssh_server + ssh_client + ssh_config）→ **不拆**\n"
            "- `iccp`（iccp_client + iccp_server + etrunk）→ **不拆**\n"
            "- `tls`（libssl + libcrypto + libtls）→ **不拆**\n"
            "- `routing`（bgp + ospf + isis）→ **拆**（三个独立协议：`bgp` / `ospf` / `isis`）\n\n"
            "## 错误示范 ❌\n"
            "- `ssh_server` + `ssh_client`（同协议拆碎）\n"
            "- `icmp_parse` + `icmp_send`（同协议子功能拆碎）\n"
            "- `ospfv2` + `ospfv3`（同协议版本变体拆碎）\n"
        )
    if granularity == "fine":
        return (
            "\n\n---\n"
            "# 粒度约束（细粒度模式）\n\n"
            "当前任务配置为 **细粒度（fine）** 模式。\n"
            "- 只要文件功能属于不同子系统（client/server/parser/config 等）→ 可拆分\n"
            "- 只要建议子模块显示出清晰、稳定、可命名的职责边界 → 可拆分\n"
            "- 若所有文件仍然服务于同一职责，或强行拆分会破坏内聚性 → 不拆\n"
            "- **不要使用文件数量作为拆分或不拆分依据**\n"
        )
    return ""


def check_agent_result(ar: AgentResult, context: str = "") -> None:
    if ar.fatal:
        msg = f"pi 致命错误（不可重试）: {ar.error or ar.output or 'unknown'}"
        if context:
            msg = f"[{context}] {msg}"
        raise PiFatalError(msg)
    if ar.error and not ar.output:
        err_lower = (ar.error or "").lower()
        if "context length" in err_lower and "input tokens" in err_lower:
            msg = f"智能体输入超长: {ar.error}"
        else:
            msg = f"pi 进程崩溃 (exit=1): {ar.error}"
        if context:
            msg = f"[{context}] {msg}"
        raise StageError(msg)


async def run_agent_checked(context: str = "", **kwargs) -> AgentResult:
    ar = await run_agent(**kwargs)
    check_agent_result(ar, context)
    return ar


def _get_session_mtime(session_file: "str | None") -> float:
    """session 文件最后修改时间（unix timestamp），文件不存在返回 0.0。"""
    if not session_file:
        return 0.0
    try:
        return os.path.getmtime(session_file)
    except OSError:
        return 0.0


async def run_agent_with_stage_guard(
    *,
    ctx: "PipelineContext",
    stage: str,
    context: str,
    heartbeat_payload_factory=None,
    heartbeat_interval: float | None = None,
    timeout_seconds: float | None = None,  # deprecated — kept for call-site compat
    **kwargs,
) -> AgentResult:
    """Run an agent with heartbeat events and backend-model stuck detection.

    卡死检测设计原则：
    - 必须基于 session 文件的 mtime 变化判断模型是否有输出
    - 必须不获取 mtime 时立即重置计时器 --- 防止两次输出间隔当刻正好跳过心跳间隔
    - 只允许 stuck_timeout 秒内持续无 token 输出才触发，不是当弹就发
    - 激活时向同一 session 发送「继续」，不是用原来 prompt
      「继续」仅在 session 已有 assistant 内容时发送，否则重发原 prompt
    - 重试次数上限 model_stuck_max_retries（与 pi_max_retries 独立）
    - 这里的重试不与 run_agent 内部的 pi_max_retries 重叠——前者针对“无响应”，后者针对“崩溃”
    """
    heartbeat_every = max(1.0, float(heartbeat_interval or _DEFAULT_AGENT_HEARTBEAT_INTERVAL_SECONDS))

    # ── stuck 检测参数 ───────────────────────────────────────────────────────
    _stuck_timeout_raw = getattr(ctx.cfg, "model_stuck_timeout", _DEFAULT_MODEL_STUCK_TIMEOUT_SECONDS)
    try:
        stuck_timeout = float(_stuck_timeout_raw)
    except (TypeError, ValueError):
        stuck_timeout = _DEFAULT_MODEL_STUCK_TIMEOUT_SECONDS
    if stuck_timeout <= 0:
        stuck_timeout = 0.0  # 0 = disabled

    _stuck_max_raw = getattr(ctx.cfg, "model_stuck_max_retries", _DEFAULT_MODEL_STUCK_MAX_RETRIES)
    try:
        stuck_max_retries = max(0, int(_stuck_max_raw))
    except (TypeError, ValueError):
        stuck_max_retries = _DEFAULT_MODEL_STUCK_MAX_RETRIES

    session_file_path: str = str(kwargs.get("session_file") or "")
    stuck_retry_count = 0
    run_kwargs = dict(kwargs)

    def _payload(heartbeat_index: int) -> dict:
        if callable(heartbeat_payload_factory):
            payload = heartbeat_payload_factory(heartbeat_index)
            if isinstance(payload, dict):
                return dict(payload)
        return {}

    # ── outer loop: stuck-retry 外层 ─────────────────────────────────────────
    while True:
        agent_task = asyncio.create_task(run_agent_checked(context=context, **run_kwargs))
        heartbeat_index = 0
        started_monotonic = time.monotonic()

        # 每次启动新 pi 时重新读取 mtime 基线
        last_mtime = _get_session_mtime(session_file_path)
        last_active_monotonic = time.monotonic()

        try:
            # ── inner loop: heartbeat + mtime 监控 ──────────────────────────
            while True:
                try:
                    return await asyncio.wait_for(
                        asyncio.shield(agent_task),
                        timeout=heartbeat_every,
                    )
                except asyncio.TimeoutError:
                    heartbeat_index += 1
                    elapsed = time.monotonic() - started_monotonic

                    # ── mtime 变化检查：只要文件新写入就重置活跃时刻 ──
                    cur_mtime = _get_session_mtime(session_file_path)
                    if cur_mtime != last_mtime:
                        last_mtime = cur_mtime
                        last_active_monotonic = time.monotonic()

                    idle_secs = time.monotonic() - last_active_monotonic

                    # ── 心跳事件 ──────────────────────────────────────────────
                    payload = _payload(heartbeat_index)
                    payload["elapsed_seconds"] = round(elapsed, 3)
                    ctx.emit_event("stage", stage=stage, **payload)

                    # ── stuck 检测：仅在 stuck_timeout>0 且真正无输出时触发 ──
                    if stuck_timeout > 0 and idle_secs >= stuck_timeout:
                        if stuck_retry_count >= stuck_max_retries:
                            agent_task.cancel()
                            await asyncio.gather(agent_task, return_exceptions=True)
                            raise StageError(
                                f"[{context}] 后端模型无响应超过 {idle_secs:.0f}s，"
                                f"已激活重试 {stuck_retry_count} 次，放弃"
                            )

                        # kill 当前 pi 进程
                        agent_task.cancel()
                        await asyncio.gather(agent_task, return_exceptions=True)

                        stuck_retry_count += 1
                        ctx.emit_event(
                            "log", level="warn",
                            msg=(
                                f"[{context}] 后端模型 {idle_secs:.0f}s 无 token 输出，"
                                f"发送激活指令 "
                                f"(第 {stuck_retry_count}/{stuck_max_retries} 次)"
                            ),
                        )

                        # 准备新调用参数：有 assistant 内容就发「继续」，否则重发原 prompt
                        run_kwargs = dict(kwargs)
                        from ..runner import _session_has_assistant_content  # noqa: PLC0415
                        if session_file_path and _session_has_assistant_content(session_file_path):
                            run_kwargs["prompt"] = "继续"

                        break  # 跳出内层心跳循环，进入外层重试

        finally:
            # agent_task 已当场取消时这里是 no-op；异常传播时确保清理
            if not agent_task.done():
                agent_task.cancel()
                await asyncio.gather(agent_task, return_exceptions=True)

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
    """返回 workspace/modules/ 下所有 files.list 非空的叶节点模块名。

    同时自动展平 S2 Worker 可能创建的嵌套子模块目录：
      modules/<parent>/<sub>/files.list  →  modules/<sub>/files.list
    只处理二级嵌套（parent 本身无 files.list，但其内部子目录有 files.list）。
    """
    root = get_modules_root(str(workspace))
    # ── 展平嵌套子模块（二级）─────────────────────────────────────────
    for parent in sorted(root.iterdir()):
        if not parent.is_dir() or parent.name.startswith("."):
            continue
        if module_has_nonempty_files(parent):
            continue  # parent 本身有 files.list，不是容器目录
        nested = [
            sub for sub in parent.iterdir()
            if sub.is_dir() and not sub.name.startswith(".")
            and module_has_nonempty_files(sub)
        ]
        for sub in nested:
            target = root / sub.name
            if not target.exists():
                try:
                    shutil.move(str(sub), str(target))
                except Exception:
                    pass
            else:
                # 目标已存在：将 sub 的 files.list 去重追加到 target，再删除 sub
                try:
                    existing = set(read_module_files(target))
                    for f in read_module_files(sub):
                        if f not in existing:
                            with open(str(target / "files.list"), "a", encoding="utf-8") as _fh:
                                _fh.write(f + "\n")
                    shutil.rmtree(str(sub), ignore_errors=True)
                except Exception:
                    pass
    # ── 收集第一层叶节点模块 ──────────────────────────────────────────
    result = []
    for d in sorted(root.iterdir()):
        if d.is_dir() and not d.name.startswith(".") and module_has_nonempty_files(d):
            result.append(d.name)
    return result


def read_module_files(mod_dir: str | Path) -> list[str]:
    """读取模块 files.list，返回去空白后的相对路径列表。"""
    mod_dir = Path(mod_dir)
    try:
        raw = (mod_dir / "files.list").read_text("utf-8").splitlines()
    except OSError:
        return []
    return [line.strip() for line in raw if line.strip()]


def module_has_nonempty_files(mod_dir: str | Path) -> bool:
    """模块是否存在非空 files.list。"""
    return bool(read_module_files(mod_dir))


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

def load_prompt(source, name: str, role: str | None = None) -> str:
    if role and hasattr(source, "get_prompt"):
        try:
            prompt = source.get_prompt(role, name)
            if isinstance(prompt, str) and prompt.strip():
                return prompt.strip()
        except Exception:
            pass

    # source 可能是完整 cfg 对象，而不是 prompt_dir 字符串。
    # 此时必须回退到 workers/judges.system_prompt_dir，不能对 str(cfg) 拼路径，
    # 否则会把整段任务配置当成文件名，触发 [Errno 36] File name too long。
    prompt_dir = ""
    if role and hasattr(source, role):
        try:
            role_obj = getattr(source, role)
            prompt_dir = str(getattr(role_obj, "system_prompt_dir", "") or "")
        except Exception:
            prompt_dir = ""
    if not prompt_dir:
        prompt_dir = str(source or "")

    for ext in [".md", ".txt", ""]:
        p = Path(prompt_dir) / f"{name}{ext}"
        if p.exists():
            return p.read_text(encoding="utf-8").strip()
    return ""


def load_granularity_prompt(source, base_name: str, granularity: str, role: str | None = None) -> str:
    """按粒度加载独立提示词；找不到时再回退到通用提示词。"""
    gran = (granularity or "fine").strip().lower()
    candidates: list[str] = []
    if gran == "coarse":
        candidates.append(base_name.replace("step2_", "step2_coarse_").replace("step3_", "step3_coarse_"))
        candidates.append(base_name.replace("reflect_", "reflect_coarse_"))
    elif gran == "fine":
        candidates.append(base_name.replace("step2_", "step2_fine_").replace("step3_", "step3_fine_"))
        candidates.append(base_name.replace("reflect_", "reflect_fine_"))

    # judges: step2_check_refine -> step2_check_coarse_refine / step2_check_fine_refine
    if base_name.startswith("step2_check_"):
        suffix = base_name[len("step2_check_"):]
        if gran == "coarse":
            candidates.insert(0, f"step2_check_coarse_{suffix}")
        elif gran == "fine":
            candidates.insert(0, f"step2_check_fine_{suffix}")
    if base_name.startswith("step3_check_"):
        suffix = base_name[len("step3_check_"):]
        if gran == "coarse":
            candidates.insert(0, f"step3_check_coarse_{suffix}")
        elif gran == "fine":
            candidates.insert(0, f"step3_check_fine_{suffix}")

    # 去重保序
    seen = set()
    ordered = []
    for c in candidates:
        if c and c not in seen and c != base_name:
            seen.add(c)
            ordered.append(c)

    for name in ordered:
        p = load_prompt(source, name, role)
        if p:
            return p
    return load_prompt(source, base_name, role)


# ── 通用小工具 ─────────────────────────────────────────────────────────────────

def max_iter(s_cfg) -> int:
    """max_rounds=-1 时返回一个很大的数（≈无限）。"""
    return s_cfg.max_rounds if s_cfg.max_rounds > 0 else 999_999


def max_rounds_exceeded_treated_as_passed(cfg) -> bool:
    action = str(getattr(cfg, "max_rounds_exceeded_action", "treat_as_passed") or "treat_as_passed").strip().lower()
    return action == "treat_as_passed"


def get_module_deleted_files(mod_dir: Path) -> set[str]:
    """Read modules/<mod>/deleted/files.list; return set. Empty if absent."""
    p = mod_dir / "deleted" / "files.list"
    if not p.exists():
        return set()
    return {ln.strip() for ln in p.read_text("utf-8", errors="replace").splitlines() if ln.strip()}


async def archive_module_deletions(
    workspace: "Path",
    mod_name: str,
    mod_dir: "Path",
    lock: "asyncio.Lock",
    ctx: "PipelineContext",
) -> int:
    """Archive modules/<mod>/deleted/files.list → workspace/deleted.list (lock-protected).

    删除 deleted/ 子目录。返回归档文件数（无 deleted/ 时返回 0）。
    """
    deleted_dir = mod_dir / "deleted"
    if not deleted_dir.exists():
        return 0
    deleted_flist = deleted_dir / "files.list"
    files: list[str] = []
    if deleted_flist.exists():
        files = [ln.strip() for ln in
                 deleted_flist.read_text("utf-8", errors="replace").splitlines()
                 if ln.strip()]
    if files:
        async with lock:
            with open(str(workspace / "deleted.list"), "a", encoding="utf-8") as f:
                for fp in files:
                    f.write(fp + "\n")
        ctx.emit_event("log", level="info",
                       msg=f"[deleted] 模块 {mod_name}: 归档 {len(files)} 个排除文件")
    shutil.rmtree(str(deleted_dir), ignore_errors=True)
    return len(files)




def process_module_recover(mod_dir):
    """Move recover/files.list entries back to files.list (after Judge wrong-delete review).

    Returns list of restored file paths. Empty if no recover/ exists.
    """
    nl = chr(10)
    recover_flist = mod_dir / 'recover' / 'files.list'
    if not recover_flist.exists():
        return []
    recover_files = [
        ln.strip()
        for ln in recover_flist.read_text('utf-8', errors='replace').splitlines()
        if ln.strip()
    ]
    if not recover_files:
        shutil.rmtree(str(mod_dir / 'recover'), ignore_errors=True)
        return []
    recover_set = set(recover_files)
    deleted_flist = mod_dir / 'deleted' / 'files.list'
    if deleted_flist.exists():
        remaining = [
            ln.strip()
            for ln in deleted_flist.read_text('utf-8', errors='replace').splitlines()
            if ln.strip() and ln.strip() not in recover_set
        ]
        if remaining:
            deleted_flist.write_text(nl.join(remaining) + nl, encoding='utf-8')
        else:
            deleted_flist.unlink(missing_ok=True)
            deleted_dir = mod_dir / 'deleted'
            if deleted_dir.exists() and not any(deleted_dir.iterdir()):
                deleted_dir.rmdir()
    files_list = mod_dir / 'files.list'
    with open(str(files_list), 'a', encoding='utf-8') as f:
        f.write(nl.join(recover_files) + nl)
    shutil.rmtree(str(mod_dir / 'recover'), ignore_errors=True)
    return recover_files


def list_split_candidate_modules(mod_dir: "Path") -> list[str]:
    """列出 modules/<mod>/split/ 下的候选子模块名（不含 _merge_to）。"""
    split_dir = mod_dir / "split"
    if not split_dir.exists() or not split_dir.is_dir():
        return []
    names: list[str] = []
    for d in sorted(split_dir.iterdir()):
        if not d.is_dir() or d.name.startswith("_"):
            continue
        if module_has_nonempty_files(d):
            names.append(d.name)
    return names


def split_plan_exists(mod_dir: "Path") -> bool:
    return bool(list_split_candidate_modules(mod_dir) or (mod_dir / "split" / "_merge_to").exists())


def read_split_merge_targets(mod_dir: "Path") -> dict[str, list[str]]:
    """读取 split/_merge_to/<target>/files.list。"""
    merge_root = mod_dir / "split" / "_merge_to"
    result: dict[str, list[str]] = {}
    if not merge_root.exists() or not merge_root.is_dir():
        return result
    for d in sorted(merge_root.iterdir()):
        if not d.is_dir():
            continue
        files = read_module_files(d)
        if files:
            result[d.name] = files
    return result


def _write_unique_files(path: "Path", files: list[str]) -> None:
    uniq = sorted({f.strip() for f in files if str(f).strip()})
    path.parent.mkdir(parents=True, exist_ok=True)
    if uniq:
        path.write_text("\n".join(uniq) + "\n", encoding="utf-8")
    else:
        path.write_text("", encoding="utf-8")



def commit_split_plan(workspace: "Path", mod_name: str) -> dict[str, list[str] | bool]:
    """将 modules/<mod>/split 下的候选拆分结果正式提交到 modules/ 根目录。

    支持：
    - modules/<mod>/split/<child>/files.list      → 新子模块或保留父模块(mod_name)
    - modules/<mod>/split/_merge_to/<dst>/files.list → 并入其他模块

    返回：
      {
        "applied": bool,
        "new_modules": [...],
        "merged_targets": [...],
        "retained_parent": bool,
      }
    """
    mods_root = get_modules_root(str(workspace))
    mod_dir = mods_root / mod_name
    split_dir = mod_dir / "split"
    if not split_dir.exists() or not split_dir.is_dir():
        return {"applied": False, "new_modules": [], "merged_targets": [], "retained_parent": False}

    snapshot_path = workspace / ".s2_snapshots" / f"{mod_name}.snapshot"
    snap_files = set(read_module_files(snapshot_path)) if snapshot_path.exists() else set()
    deleted_files = set(read_module_files(mod_dir / "deleted"))

    child_map: dict[str, set[str]] = {}
    retained_parent = False
    for child in list_split_candidate_modules(mod_dir):
        files = set(read_module_files(split_dir / child))
        if files:
            child_map[child] = files
            if child == mod_name:
                retained_parent = True

    merge_map = {name: set(files) for name, files in read_split_merge_targets(mod_dir).items()}

    covered = set().union(*child_map.values()) if child_map else set()
    for files in merge_map.values():
        covered |= files
    covered |= deleted_files

    if snap_files and covered != snap_files:
        missing = sorted(snap_files - covered)
        extra = sorted(covered - snap_files)
        raise StageError(
            f"split 提交前校验失败: missing={len(missing)} extra={len(extra)}"
            + (f" missing示例={missing[:5]}" if missing else "")
            + (f" extra示例={extra[:5]}" if extra else "")
        )

    new_modules: list[str] = []
    merged_targets: list[str] = []

    for child, files in child_map.items():
        target_dir = mods_root / child
        target_file = target_dir / "files.list"
        if child == mod_name:
            _write_unique_files(target_file, sorted(files))
        else:
            existing = set(read_module_files(target_dir)) if target_file.exists() else set()
            _write_unique_files(target_file, sorted(existing | files))
            new_modules.append(child)

    for target, files in merge_map.items():
        target_dir = mods_root / target
        target_file = target_dir / "files.list"
        existing = set(read_module_files(target_dir)) if target_file.exists() else set()
        _write_unique_files(target_file, sorted(existing | files))
        merged_targets.append(target)

    if mod_name not in child_map:
        if (mod_dir / "deleted").exists():
            (mod_dir / "files.list").unlink(missing_ok=True)
        else:
            shutil.rmtree(str(mod_dir), ignore_errors=True)
    else:
        _write_unique_files(mod_dir / "files.list", sorted(child_map[mod_name]))

    shutil.rmtree(str(split_dir), ignore_errors=True)
    return {
        "applied": True,
        "new_modules": sorted(set(new_modules)),
        "merged_targets": sorted(set(merged_targets)),
        "retained_parent": retained_parent,
    }


def restore_module_for_retry(
    mod_name: str,
    mod_dir: "Path",
    workspace: "Path",
    refined_set: set[str],
) -> None:
    """重试前恢复模块状态（split 草稿模式）。

    1. 恢复快照 → mod_dir/files.list
    2. 删除 mod_dir/split/（上一轮候选拆分草稿）
    3. 清空 mod_dir/deleted/（如存在）
    4. 清理 workspace 根下与此模块相关的孤儿目录（路径写错导致）
    """
    snapshot_path = workspace / ".s2_snapshots" / f"{mod_name}.snapshot"

    if snapshot_path.exists():
        mod_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(snapshot_path), str(mod_dir / "files.list"))

    split_dir = mod_dir / "split"
    if split_dir.exists():
        shutil.rmtree(str(split_dir), ignore_errors=True)

    deleted_dir = mod_dir / "deleted"
    if deleted_dir.exists():
        shutil.rmtree(str(deleted_dir), ignore_errors=True)

    _clean_orphan_dirs(workspace, mod_name, refined_set)


def _clean_orphan_dirs(
    workspace: "Path",
    mod_name: str,
    refined_set: set[str],
) -> list[str]:
    """清理 workspace 根下路径拼写错误产生的孤儿模块目录。

    孤儿目录是 Worker bash 脚本将 modules/<name> 写成 modules<name>
    （缺少 /）时在 workspace 根下产生的，check_module.sh 找不到它们，
    导致校验永远报 MISSING。

    返回被清理的目录名列表（供日志记录）。
    """
    cleaned: list[str] = []
    for d in workspace.iterdir():
        if not d.is_dir():
            continue
        if d.name.startswith("."):
            continue
        if d.name == "modules":
            continue
        if d.name in refined_set:
            continue
        # 孤儿目录判定：有 files.list、无快照、名称包含 mod_name
        fl = d / "files.list"
        snap = workspace / ".s2_snapshots" / f"{d.name}.snapshot"
        if fl.exists() and not snap.exists() and mod_name.lower() in d.name.lower():
            shutil.rmtree(str(d), ignore_errors=True)
            cleaned.append(d.name)
    return cleaned


def fix_orphan_dirs_before_judge(
    workspace: "Path",
    mod_name: str,
    refined_set: set[str],
) -> list[str]:
    """Judge 运行前扫描并自动修复 workspace 根下的孤儿模块目录。

    若发现名称含 mod_name 的孤儿目录（有 files.list 但无快照），
    尝试将其 move 进 modules/ 下正确位置，保留内容供 Judge 校验。

    返回被修复的目录名列表（供日志/feedback 记录）。
    """
    mods_root = get_modules_root(str(workspace))
    fixed: list[str] = []
    for d in workspace.iterdir():
        if not d.is_dir():
            continue
        if d.name.startswith("."):
            continue
        if d.name == "modules":
            continue
        if d.name in refined_set:
            continue
        fl = d / "files.list"
        snap = workspace / ".s2_snapshots" / f"{d.name}.snapshot"
        if not fl.exists() or snap.exists():
            continue
        if mod_name.lower() not in d.name.lower():
            continue
        # 尝试推断正确目标名（去掉可能多余的前缀 "modules"）
        correct_name = d.name
        if correct_name.lower().startswith("modules"):
            correct_name = correct_name[len("modules"):].lstrip("_-")
        if not correct_name:
            correct_name = d.name
        target = mods_root / correct_name
        if not target.exists():
            try:
                shutil.move(str(d), str(target))
                fixed.append(f"{d.name} → modules/{correct_name}")
            except Exception:
                # move 失败则直接删除孤儿目录，避免污染 check
                shutil.rmtree(str(d), ignore_errors=True)
                fixed.append(f"{d.name} (removed, move failed)")
        else:
            # 目标已存在，将孤儿文件追加进去后删除孤儿目录
            try:
                orphan_files = [ln.strip() for ln in fl.read_text("utf-8").splitlines() if ln.strip()]
                target_fl = target / "files.list"
                existing = set()
                if target_fl.exists():
                    existing = {ln.strip() for ln in target_fl.read_text("utf-8").splitlines() if ln.strip()}
                with open(str(target_fl), "a", encoding="utf-8") as f:
                    for fp in orphan_files:
                        if fp not in existing:
                            f.write(fp + "\n")
                shutil.rmtree(str(d), ignore_errors=True)
                fixed.append(f"{d.name} merged → modules/{correct_name}")
            except Exception:
                shutil.rmtree(str(d), ignore_errors=True)
                fixed.append(f"{d.name} (removed, merge failed)")
    return fixed


def build_s2_diagnose_report(
    workspace: "Path",
    mod_name: str,
    missing_files: list[str],
) -> str:
    """对 Judge 报告的每个 MISSING 文件，Python 侧自动定位其当前物理位置。

    生成结构化诊断报告，写入 workspace/.diagnose/ 目录，
    由调用方将文件路径注入 Worker 的 retry prompt。
    返回诊断文件的绝对路径（字符串）。
    """
    import json as _json

    diagnose_dir = workspace / ".diagnose"
    diagnose_dir.mkdir(exist_ok=True)

    mods_root = get_modules_root(str(workspace))
    lines: list[str] = [
        f"# S2 Refine 诊断报告：模块 `{mod_name}`",
        "",
        "## MISSING 文件定位",
        "",
    ]

    # 预构建：所有 modules/*/files.list 的内容索引（文件→模块名列表）
    file_to_mods: dict[str, list[str]] = {}
    for flist in mods_root.glob("*/files.list"):
        mod = flist.parent.name
        for f in flist.read_text("utf-8", errors="replace").splitlines():
            f = f.strip()
            if f:
                file_to_mods.setdefault(f, []).append(mod)

    # 读取 workspace/deleted.list
    deleted_set: set[str] = set()
    deleted_path = workspace / "deleted.list"
    if deleted_path.exists():
        deleted_set = {ln.strip() for ln in deleted_path.read_text("utf-8", errors="replace").splitlines() if ln.strip()}

    orphan_index: dict[str, list[str]] = {}
    for d in workspace.iterdir():
        if not d.is_dir() or d.name.startswith(".") or d.name == "modules":
            continue
        fl = d / "files.list"
        if fl.exists():
            for f in fl.read_text("utf-8", errors="replace").splitlines():
                f = f.strip()
                if f:
                    orphan_index.setdefault(f, []).append(d.name)

    all_truly_missing: list[str] = []
    all_in_mods: list[tuple[str, list[str]]] = []
    all_in_orphans: list[tuple[str, list[str]]] = []
    all_in_deleted: list[str] = []

    for rel in (missing_files or [])[:30]:
        if rel in deleted_set:
            all_in_deleted.append(rel)
        elif rel in file_to_mods:
            all_in_mods.append((rel, file_to_mods[rel]))
        elif rel in orphan_index:
            all_in_orphans.append((rel, orphan_index[rel]))
        else:
            all_truly_missing.append(rel)

    if all_in_mods:
        lines.append("### ✅ 文件已在其他模块中（可能竞态导致暂时不可见，重新运行 check_module.sh 应通过）")
        for f, mods in all_in_mods:
            lines.append(f"- `{f}` → 位于 `modules/{mods[0]}/files.list`")
        lines.append("")

    if all_in_orphans:
        lines.append("### ⚠️ 文件在 workspace 根下孤儿目录中（路径写错！需要修复）")
        for f, dirs in all_in_orphans:
            lines.append(f"- `{f}` → 位于孤儿目录 `{dirs[0]}/files.list`")
        lines.append("")
        lines.append("**修复方法（孤儿目录）**：")
        orphan_dirs = {d for _, dirs in all_in_orphans for d in dirs}
        for od in sorted(orphan_dirs):
            correct = od.lstrip("modules").lstrip("_-") or od
            lines.append(f"```bash")
            lines.append(f"mkdir -p modules/{correct}")
            lines.append(f"cat {od}/files.list >> modules/{correct}/files.list")
            lines.append(f"sort -u modules/{correct}/files.list -o modules/{correct}/files.list")
            lines.append(f"rm -rf {od}")
            lines.append(f"```")
        lines.append("")

    if all_in_deleted:
        lines.append("### ℹ️ 文件在 workspace/deleted.list 中（已标记排除，check_module.sh 应自动放行）")
        for f in all_in_deleted:
            lines.append(f"- `{f}`")
        lines.append("")

    if all_truly_missing:
        lines.append("### ❌ 文件真正丢失（未在任何模块/孤儿目录/deleted.list 中）")
        for f in all_truly_missing:
            phys = workspace / "target" / f
            exists = "物理文件存在✓" if phys.exists() else "物理文件也不存在❌"
            lines.append(f"- `{f}` ({exists})")
        lines.append("")
        lines.append("**修复方法（真正丢失）**：从快照恢复后重新拆分")
        lines.append(f"```bash")
        lines.append(f"cp .s2_snapshots/{mod_name}.snapshot modules/{mod_name}/files.list")
        lines.append(f"# 然后重新执行拆分脚本")
        lines.append(f"```")

    lines += [
        "",
        "## workspace 根目录状态（孤儿目录检查）",
        "",
    ]
    orphan_dirs_found = [
        d.name for d in workspace.iterdir()
        if d.is_dir() and not d.name.startswith(".")
        and d.name != "modules"
        and (d / "files.list").exists()
        and not (workspace / ".s2_snapshots" / f"{d.name}.snapshot").exists()
    ]
    if orphan_dirs_found:
        lines.append(f"⚠️ 发现 {len(orphan_dirs_found)} 个孤儿目录（有 files.list 但不在 modules/ 下）：")
        for od in orphan_dirs_found:
            cnt = len([l for l in (workspace / od / "files.list").read_text("utf-8", errors="replace").splitlines() if l.strip()])
            lines.append(f"- `{od}/` ({cnt} 个文件)")
    else:
        lines.append("✅ workspace 根目录无孤儿目录")

    # 写入文件并返回路径
    import time as _time
    ts = int(_time.time())
    out_path = diagnose_dir / f"{mod_name}_{ts}.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return str(out_path)


def enforce_filter_constraint(workspace: "Path", filtered_files: set[str]) -> int:
    """删除所有 modules/*/files.list 中不属于 filtered_files 白名单的行。

    返回删除的行数。若 filtered_files 为空（未配置过滤）则跳过。
    """
    if not filtered_files:
        return 0
    removed = 0
    mods_root = get_modules_root(str(workspace))
    for flist_path in mods_root.glob("*/files.list"):
        lines = [l.strip() for l in flist_path.read_text("utf-8", errors="replace").splitlines()]
        kept = [l for l in lines if not l or l in filtered_files]
        if len(kept) < len(lines):
            extra = len(lines) - len(kept)
            removed += extra
            # 删除空模块目录
            if not any(l for l in kept):
                import shutil as _shutil
                _shutil.rmtree(str(flist_path.parent), ignore_errors=True)
            else:
                flist_path.write_text("\n".join(kept).strip() + "\n", encoding="utf-8")
    return removed


def extract_result(output: str) -> str:
    """从 <result>…</result> 提取结果，否则返回原始输出。"""
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    return m.group(1).strip() if m else output


def archive_file(output_dir: Path, name: str, content: str) -> None:
    """将内容写入 output_dir/name（中间件存档）。"""
    try:
        (output_dir / name).write_text(content, encoding="utf-8")
    except OSError:
        pass


def write_judge_feedback(
    workspace: "Path",
    stage_key: str,
    module_name: "str | None",
    attempt: int,
    judge_results: list[dict],
) -> Path:
    """将本轮所有 Judge 的完整意见（不截断）写入独立文件，返回相对于 workspace 的路径。

    存储结构：
      有模块：workspace/judge_output/<stage_key>/<module_name>/feedback_a<attempt>.md
      无模块：workspace/judge_output/<stage_key>/feedback_a<attempt>.md

    stage_key 约定：
      s1_classify    — Stage 1 粗分类（全局，module_name=None）
      s1_security    — Stage 1.5 安全维度过滤（全局，module_name=None）
      s2_refine      — Stage 2 细分（按模块）
      s3_analyse     — Stage 3 分析（按模块）
      s4_completeness — Stage 4a 完整性检查（全局，module_name=None）
      s4_report      — Stage 4b 最终报告（全局，module_name=None）
    """
    if module_name:
        out_dir = workspace / "judge_output" / stage_key / module_name
    else:
        out_dir = workspace / "judge_output" / stage_key
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / f"feedback_a{attempt}.md"
    lines: list[str] = [f"# Judge 评审意见（第 {attempt} 轮）\n"]
    for i, r in enumerate(judge_results):
        passed_str = "✅ 通过" if r.get("pass") else "❌ 不通过"
        lines.append(f"## Judge-{i}  {passed_str}  分数={r.get('score', '?')}\n")
        lines.append((r.get("feedback") or "(无意见)") + "\n")
    out_file.write_text("\n".join(lines), encoding="utf-8")

    # 返回相对于 workspace 的 Path（Worker prompt 中可直接 read 该路径）
    return out_file.relative_to(workspace)


# ─── ELF / 文件预读 ──────────────────────────────────────────────────────────

SUB_BATCH_SIZE = 20        # 每个子 Worker 处理的文件数
SUB_WORKER_THRESHOLD = 20  # 文件数超过此值启用主从模式


def pre_read_file(fullpath: str) -> tuple[str, list[str]]:
    """返回 (file_type, top_strings)。ELF 只读前 128KB，文本读全文（限 4MB）。"""
    ELF_MAGIC = b"\x7fELF"
    MIN_STR = 5
    MAX_ELF = 131_072
    MAX_TEXT = 4 * 1024 * 1024

    def _strings(data: bytes) -> list[str]:
        out, cur = [], []
        for b in data:
            c = chr(b)
            if c.isprintable() and c not in ('\n', '\r'):
                cur.append(c)
            else:
                if len(cur) >= MIN_STR:
                    out.append(''.join(cur))
                cur = []
        if len(cur) >= MIN_STR:
            out.append(''.join(cur))
        return out

    try:
        with open(fullpath, 'rb') as f:
            magic = f.read(4)
            if magic == ELF_MAGIC:
                f.seek(0)
                data = f.read(MAX_ELF)
                strs = _strings(data)
                filtered = [s for s in strs
                            if len(s) >= 5
                            and not s.startswith('/')
                            and not s.startswith('.')
                            and ' ' not in s[:3]]
                return ('ELF', filtered[:200])
            else:
                f.seek(0)
                raw = f.read(MAX_TEXT)
                try:
                    text = raw.decode('utf-8', errors='ignore')
                except Exception:
                    return ('binary', [])
                lines = [l.strip() for l in text.splitlines() if l.strip()][:120]
                return ('text', lines)
    except (OSError, IOError):
        return ('unknown', [])


def read_one_elf(fullpath: str) -> dict:
    """ELF 三层提取：nm 导出/导入符号 + readelf 依赖库 + strings 头部。

    根据 ELF 类型自适应选择 nm 参数：
    - ET_DYN=3 (.so 共享库): nm -D 读动态符号表（.dynsym）
    - ET_REL=1 (.ko 内核模块/可重定位): nm 读所有符号（.ko 无 .dynsym）
    - ET_EXEC=2 (可执行文件): nm -D 优先，为空则用 nm
    """
    res: dict = {"exports": [], "imports": [], "needed": [], "strings_head": []}
    try:
        # 读 ELF 类型以决定 nm 参数
        import struct as _struct
        with open(fullpath, "rb") as _f:
            _hdr = _f.read(18)
        _ei_data = _hdr[5] if len(_hdr) > 5 else 1  # 1=LE, 2=BE
        _etype_fmt = ">H" if _ei_data == 2 else "<H"
        _etype = _struct.unpack_from(_etype_fmt, _hdr, 0x10)[0] if len(_hdr) >= 18 else 0
        # ET_REL=1(.ko), ET_EXEC=2, ET_DYN=3(.so)
        _nm_args = ["nm", "-D", fullpath] if _etype == 3 else ["nm", fullpath]

        r = subprocess.run(_nm_args, capture_output=True, text=True, timeout=15)
        for line in r.stdout.splitlines():
            p = line.split()
            if len(p) >= 3:
                st, sn = p[-2], p[-1]
                if st in ('T', 't'):
                    res["exports"].append(sn)
                elif st == 'U':
                    res["imports"].append(sn)
            elif len(p) == 2 and p[0] == 'U':
                res["imports"].append(p[1])
        res["exports"] = res["exports"][:300]
        res["imports"] = res["imports"][:150]
        r = subprocess.run(["readelf", "-d", fullpath],
                           capture_output=True, text=True, timeout=15)
        for line in r.stdout.splitlines():
            if "NEEDED" in line:
                m = re.search(r'\[([^\]]+)\]', line)
                if m:
                    res["needed"].append(m.group(1))
        r = subprocess.run(["strings", "-n", "6", fullpath],
                           capture_output=True, text=True, timeout=15)
        # 过滤：只保留标识符/路径类字符串（排除二进制垃圾）
        _ident_re = re.compile(r'^[A-Za-z_/][A-Za-z0-9_./:@\-]{4,}$')
        _filtered = [s for s in r.stdout.splitlines() if _ident_re.match(s)]
        res["strings_head"] = _filtered[:50]
    except Exception:
        pass
    return res


def pre_read_module(target_dir: str, mod_dir: Path) -> str:
    """预读模块所有文件，注入结构化内容到 system prompt。

    ELF: nm 导出符号 + 导入符号 + readelf 依赖库 + strings 头部。
    文本: 直接读取内容（限总计 150KB）。
    返回带 '__HAS_TEXT__\\n' 前缀（如有非 ELF 文件），供调用方决定 tools。
    """
    try:
        flist = (mod_dir / "files.list").read_text("utf-8").strip().splitlines()
    except OSError:
        return "(files.list 不可读)"
    files = [l.strip() for l in flist if l.strip()]
    if not files:
        return "(模块文件列表为空)"

    def _read_one(relpath: str):
        fp = str(Path(target_dir) / relpath)
        try:
            with open(fp, 'rb') as f:
                magic = f.read(4)
        except OSError:
            return relpath, 'missing', {}
        if magic == b'\x7fELF':
            return relpath, 'ELF', read_one_elf(fp)
        else:
            try:
                with open(fp, encoding='utf-8', errors='replace') as f:
                    content_full = f.read()
                return relpath, 'text', {"content": content_full}
            except Exception:
                return relpath, 'binary', {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futs = [(rp, pool.submit(_read_one, rp)) for rp in files]

    TEXT_TOTAL_CHAR_LIMIT = 150_000
    TEXT_FILE_CHAR_LIMIT = 8_000
    text_chars_used = 0
    has_text_files = False
    truncated_files: list[str] = []

    parts = []
    for rp, fut in futs:
        try:
            _, ftype, data = fut.result(timeout=20)
        except Exception:
            ftype, data = 'unknown', {}
        parts.append(f"### {rp}")
        if ftype == 'ELF':
            exports = data.get('exports', [])
            imports = data.get('imports', [])
            needed = data.get('needed', [])
            sh = data.get('strings_head', [])
            parts.append("类型: ELF 共享库 (AArch64)")
            if needed:
                parts.append(f"依赖库: {', '.join(needed)}")
            if exports:
                parts.append(f"导出函数 ({len(exports)}个, 对外攻击面):")
                parts.append("```")
                parts.extend(exports)
                parts.append("```")
            if imports:
                parts.append(f"外部调用 ({len(imports)}个, 含潜在危险函数):")
                parts.append("```")
                parts.extend(imports)
                parts.append("```")
            if sh:
                parts.append(f"strings头部 ({len(sh)}行):")
                parts.append("```")
                parts.extend(sh)
                parts.append("```")
        elif ftype == 'text':
            has_text_files = True
            full = data.get('content', '')
            if text_chars_used >= TEXT_TOTAL_CHAR_LIMIT:
                truncated_files.append(rp)
                parts.append("类型: 文本文件")
                parts.append("〔内容已略去（总预算已满），可用 read 工具获取完整内容〕")
            else:
                remaining = TEXT_TOTAL_CHAR_LIMIT - text_chars_used
                take = min(len(full), TEXT_FILE_CHAR_LIMIT, remaining)
                snippet = full[:take]
                total_lines = full.count('\n') + 1
                shown_lines = snippet.count('\n') + 1
                text_chars_used += take
                is_cut = take < len(full)
                cut_note = (f"  (前{shown_lines}行/{total_lines}行，已截断"
                            f"，余下内容可用 read 工具获取)") if is_cut else f"  ({total_lines}行)"
                parts.append(f"类型: 文本文件{cut_note}:")
                parts.append("```")
                parts.extend(snippet.splitlines())
                parts.append("```")
        elif ftype == 'missing':
            parts.append("(文件不存在 target_dir)")
        else:
            parts.append(f"类型: {ftype}")

    if truncated_files:
        parts.append("")
        parts.append(f"⚠️ 以下 {len(truncated_files)} 个文件因总内容超限未展示，"
                     f"可用 read 工具直接读取：")
        for tf in truncated_files:
            parts.append(f"  - target/{tf}")

    result_str = '\n'.join(parts)
    prefix = '__HAS_TEXT__\n' if has_text_files else ''
    return prefix + result_str


async def collect_file_summaries(
    ctx: "PipelineContext",
    mod_name: str,
    mod_dir: Path,
    sub_prompt_template: str,
    parallel: int = 1,
    sub_model: str = "",
    target_dir: str = "/data/target",
    files_override: "list[str] | None" = None,
) -> str:
    """
    主从模式：子 Worker 并行分批读取文件，返回合并的文件摘要字符串。

    files_override: 若指定，只处理这些文件（用于 details/ 存在时只补充不清晰的文件），
                    而非 mod_dir/files.list 中的全部文件。
    """
    """主从模式：子 Worker 并行分批读取文件，返回合并的文件摘要字符串。"""
    w_base = ctx.make_w_base()
    if files_override is not None:
        files = [f for f in files_override if f.strip()]
    else:
        flist_path = mod_dir / "files.list"
        files = [l.strip() for l in flist_path.read_text("utf-8").splitlines() if l.strip()]

    batches: list[list[str]] = []
    for i in range(0, len(files), SUB_BATCH_SIZE):
        batches.append(files[i:i + SUB_BATCH_SIZE])

    ctx.emit_event("stage", stage="2-sub",
                   module=mod_name, batches=len(batches), files=len(files),
                   parallel=parallel)

    semaphore = asyncio.Semaphore(max(1, parallel))
    results: list[str | None] = [None] * len(batches)
    loop = asyncio.get_event_loop()

    async def _run_batch(idx: int, batch: list[str]) -> None:
        async with semaphore:
            ctx.emit_event("stage", stage="2-sub",
                           module=mod_name, batch=idx + 1, total=len(batches))

            pre_reads: list[tuple[str, list[str]]] = []
            for relpath in batch:
                fullpath = os.path.join(target_dir, relpath)
                ftype, lines = await loop.run_in_executor(None, pre_read_file, fullpath)
                pre_reads.append((ftype, lines))

            parts = [f"以下是 {len(batch)} 个文件的内容摘要，直接分析，无需再读文件：\n"]
            for relpath, (ftype, lines) in zip(batch, pre_reads):
                fname = os.path.basename(relpath)
                parts.append(f"\n=== {fname} ({ftype}) ===")
                parts.append(f"路径: {relpath}")
                if lines:
                    content_preview = '\n'.join(lines[:40])
                    parts.append(f"内容:\n{content_preview}")
                else:
                    parts.append("内容: (空文件或无法读取)")
            prompt = '\n'.join(parts)
            session_file = ctx.session_path(
                "sub_read",
                mod_name,
                f"batch{idx + 1}.jsonl",
            )

            ar = await run_agent_with_stage_guard(
                ctx=ctx,
                stage="2-sub",
                context=f"s2-sub-{mod_name}-batch{idx+1}",
                heartbeat_payload_factory=lambda beat, module=mod_name, batch_no=idx + 1, total=len(batches), session=session_file: {
                    "module": module,
                    "batch": batch_no,
                    "total": total,
                    "heartbeat": beat,
                    "session_file": session,
                },
                prompt=prompt,
                model=sub_model or w_base.get("model", ""),
                tools=[],
                system_prompt=sub_prompt_template,
                cwd=w_base["cwd"],
                thinking_level=w_base.get("thinking_level", "off"),
                session_file=session_file,
                cancel_event=w_base.get("cancel_event"),
                max_retries=w_base.get("max_retries", 3),
                retry_delay=w_base.get("retry_delay", 10),
                pi_max_retries=w_base.get("pi_max_retries", -1),
                pi_retry_delay=w_base.get("pi_retry_delay", 10),
            )
            ctx.tokens += ar.token_usage
            if ar.output:
                raw = re.sub(r'<result>.*?</result>', '', ar.output, flags=re.DOTALL).strip()
                results[idx] = raw
            else:
                results[idx] = '\n'.join(
                    f"{f} | unknown | (分析失败) | -" for f in batch)

    await asyncio.gather(*[_run_batch(i, b) for i, b in enumerate(batches)])

    all_lines = []
    for r in results:
        if r:
            for line in r.splitlines():
                line = line.strip()
                if line and '|' in line:
                    all_lines.append(line)

    header = (f"文件清单（共 {len(all_lines)} 个文件）\n"
              f"格式: 路径 | 类型 | 功能摘要 | 核心技术标识 | 建议子模块")
    merged = header + '\n' + '\n'.join(all_lines)
    ctx.emit_event("stage_result", stage="2-sub",
                   module=mod_name, file_count=len(all_lines))
    return merged


# ─── 输出后处理工具 ──────────────────────────────────────────────────────────


# ─── Details JSON — 加载与格式化工具 ─────────────────────────────────────────

import json as _json

# details JSON 中视为"摘要不足"的占位值（lower()后比较）
_INSUFFICIENT_SUMMARIES = frozenset({
    "", "n/a", "unknown", "binary", "elf binary", "elf binary, see symbols",
    "see symbols", "无法解析", "内容为空或无法解析", "(空文件或无法读取)",
})

# 文本类型集合（需要读取实际文件内容做安全分析）
_TEXT_TYPES_UPPER = frozenset({
    "C_SOURCE", "CPP_SOURCE", "HEADER", "SCRIPT_SHELL", "SCRIPT_PYTHON",
    "SCRIPT_LUA", "SCRIPT_PERL", "SCRIPT_TCL", "SCRIPT_AWK",
    "CONFIG_JSON", "CONFIG_YAML", "CONFIG_XML", "CONFIG_INI", "CONFIG_TOML",
    "CONFIG_ENV", "CONFIG_CONF", "CONFIG_NGINX", "CONFIG_PROPERTIES",
    "NETWORK_MODEL", "DATABASE_SQL",
    "TEXT",   # pre_read_file 的历史类型值
})


def _detail_path(details_dir: "Path", rel_path: str) -> "Path":
    """返回 details/<rel_path>.json 的路径。"""
    safe = rel_path.lstrip("/")
    return details_dir / (safe + ".json")


def load_detail_json(details_dir: "Path", rel_path: str) -> "dict | None":
    """加载单个文件的 details JSON，不存在或解析失败返回 None。"""
    p = _detail_path(details_dir, rel_path)
    if not p.exists():
        return None
    try:
        return _json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def is_detail_sufficient(detail: "dict | None") -> bool:
    """
    判断 details JSON 中的摘要是否充分（不充分时 S2 需补充 LLM 分析）。

    充分条件（满足以下任一）：
    - ELF 且 symbols 列表非空
    - 源码 且 functions 列表非空
    - summary 非空且不在占位值列表中，且 confidence != "low"
    """
    if detail is None:
        return False
    ftype = str(detail.get("type") or "").upper()
    if ftype == "UNKNOWN":
        return False
    summary = str(detail.get("summary") or "").strip().lower()
    symbols = detail.get("symbols") or []
    functions = detail.get("functions") or []
    confidence = str(detail.get("confidence") or "").lower()
    if ftype == "ELF":
        return bool(symbols) or (summary and summary not in _INSUFFICIENT_SUMMARIES)
    if ftype in ("C_SOURCE", "CPP_SOURCE", "HEADER"):
        return bool(functions) or (summary and summary not in _INSUFFICIENT_SUMMARIES)
    if confidence == "low":
        return bool(symbols) or bool(functions)
    return bool(summary) and summary not in _INSUFFICIENT_SUMMARIES


def format_detail_as_summary_line(detail: dict, rel_path: str) -> str:
    """
    将 details JSON 格式化为 S2 sub_reader 兼容的5列管道分隔行。
    输出格式：路径 | 类型 | 功能摘要 | 核心技术标识(3-5个) | 建议子模块
    """
    path = detail.get("path") or rel_path
    ftype = str(detail.get("type") or "unknown")
    summary = str(detail.get("summary") or "(摘要缺失)").strip()
    keywords = detail.get("keywords") or []
    if not keywords:
        syms = (detail.get("symbols") or [])[:5]
        fns = (detail.get("functions") or [])[:5]
        keywords = syms or fns
    kw_str = "、".join(str(k) for k in keywords[:5]) if keywords else "-"
    suggested = str(
        detail.get("suggested_module") or detail.get("suggested_submodule") or "unknown"
    )
    return f"{path} | {ftype} | {summary} | {kw_str} | {suggested}"


def load_details_for_module(
    details_dir: "Path",
    files: "list[str]",
    target_dir: str = "",
) -> "tuple[str, list[str]]":
    """
    从 details/ 目录批量加载模块文件摘要。

    返回:
      summary_str   — 格式化5列管道分隔行字符串（与 collect_file_summaries 输出兼容）
      unclear_files — details JSON 不存在或摘要不足的文件列表（需 LLM 补充）

    不充分的文件摘要行标注 [需补充]，让 Worker 知晓可读原文件。
    """
    lines: list[str] = []
    unclear_files: list[str] = []
    for rel in files:
        detail = load_detail_json(details_dir, rel)
        if is_detail_sufficient(detail):
            lines.append(format_detail_as_summary_line(detail, rel))
        else:
            unclear_files.append(rel)
            ftype = str((detail or {}).get("type") or "unknown")
            placeholder = "[需补充] 摘要不足，Worker 可用 read target/<path> 读取原文件"
            lines.append(f"{rel} | {ftype} | {placeholder} | - | unknown")
    if not lines:
        return "", unclear_files
    header = (
        f"文件清单（共 {len(lines)} 个文件，其中 {len(unclear_files)} 个需补充）\n"
        f"格式: 路径 | 类型 | 功能摘要 | 核心技术标识 | 建议子模块"
    )
    return header + "\n" + "\n".join(lines), unclear_files


def _render_fallback_file(
    parts: "list[str]",
    rp: str,
    ftype: str,
    data: dict,
    truncated_files: "list[str]",
    text_total_limit: int,
    text_file_limit: int,
    text_chars_used: int,
) -> int:
    """将原始读取结果（fallback）渲染到 parts，返回本次消耗的文本字符数。"""
    if ftype == "ELF":
        exports = data.get("exports", [])
        imports_l = data.get("imports", [])
        needed = data.get("needed", [])
        sh = data.get("strings_head", [])
        parts.append("类型: ELF（fallback 原始读取）")
        if needed:
            parts.append(f"依赖库: {', '.join(needed)}")
        if exports:
            parts.append(f"导出函数 ({len(exports)}个):")
            parts.append("```")
            parts.extend(exports)
            parts.append("```")
        if imports_l:
            parts.append(f"外部调用 ({len(imports_l)}个):")
            parts.append("```")
            parts.extend(imports_l)
            parts.append("```")
        if sh:
            parts.append(f"strings头部 ({len(sh)}行):")
            parts.append("```")
            parts.extend(sh)
            parts.append("```")
        return 0
    elif ftype == "text":
        full = data.get("content", "")
        if text_chars_used >= text_total_limit:
            truncated_files.append(rp)
            parts.append("类型: 文本文件")
            parts.append("〔内容已略去（总预算已满），可用 read 工具获取完整内容〕")
            return 0
        remaining = text_total_limit - text_chars_used
        take = min(len(full), text_file_limit, remaining)
        snippet = full[:take]
        total_lines = full.count("\n") + 1
        shown_lines = snippet.count("\n") + 1
        is_cut = take < len(full)
        cut_note = (
            f"  (前{shown_lines}行/{total_lines}行，已截断，余下内容可用 read 工具获取)"
            if is_cut else f"  ({total_lines}行)"
        )
        parts.append(f"类型: 文本文件{cut_note}:")
        parts.append("```")
        parts.extend(snippet.splitlines())
        parts.append("```")
        return take
    elif ftype == "missing":
        parts.append("(文件不存在 target_dir)")
        return 0
    else:
        parts.append(f"类型: {ftype}")
        return 0


def pre_read_module_with_details(
    target_dir: str,
    mod_dir: "Path",
    details_dir: "Path | None" = None,
) -> str:
    """
    预读模块所有文件，优先复用 details/ JSON 避免重复 I/O 和符号提取。

    每个文件的处理策略（按优先级）：
    1. details_dir 为 None 或不存在            → fallback：完整读取（等同旧 pre_read_module）
    2. details/<path>.json 不存在              → fallback：原始读取（nm/readelf/文本）
    3. type == ELF                             → 直接用 details 符号（不重跑 nm/readelf）
    4. type 为文本类（C_SOURCE/SCRIPT/CONFIG） → details 摘要前缀 + 读取实际文件内容
    5. 其他类型（UNKNOWN/BINARY/CERT等）       → 只输出 details 摘要，不读文件

    返回同 pre_read_module：带 '__HAS_TEXT__\\n' 前缀（如有文本文件）
    """
    # 策略1：details_dir 为 None 或不存在 → 完全 fallback
    if details_dir is None or not details_dir.exists():
        return pre_read_module(target_dir, mod_dir)

    try:
        flist = (mod_dir / "files.list").read_text("utf-8").strip().splitlines()
    except OSError:
        return "(files.list 不可读)"
    files = [l.strip() for l in flist if l.strip()]
    if not files:
        return "(模块文件列表为空)"

    TEXT_TOTAL_CHAR_LIMIT = 150_000
    TEXT_FILE_CHAR_LIMIT = 8_000
    text_chars_used = 0
    has_text_files = False
    truncated_files: list[str] = []
    parts: list[str] = []

    def _raw_read_one(relpath: str):
        fp = str(Path(target_dir) / relpath)
        try:
            with open(fp, "rb") as f:
                magic = f.read(4)
        except OSError:
            return relpath, "missing", {}
        if magic == b"\x7fELF":
            return relpath, "ELF", read_one_elf(fp)
        try:
            with open(fp, encoding="utf-8", errors="replace") as f:
                content_full = f.read()
            return relpath, "text", {"content": content_full}
        except Exception:
            return relpath, "binary", {}

    # 预先确定哪些文件需要原始读取（并行处理）
    detail_cache: dict[str, "dict | None"] = {}
    needs_raw: list[str] = []
    for rp in files:
        detail = load_detail_json(details_dir, rp)
        detail_cache[rp] = detail
        ftype_d = str((detail or {}).get("type") or "").upper()
        # 策略2：details 不存在 → 原始读取
        # 策略4：文本类 → 需要读实际内容
        if detail is None or ftype_d in _TEXT_TYPES_UPPER or ftype_d == "":
            needs_raw.append(rp)

    raw_results: dict[str, tuple] = {}
    if needs_raw:
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            futs = [(rp, pool.submit(_raw_read_one, rp)) for rp in needs_raw]
        for rp, fut in futs:
            try:
                raw_results[rp] = fut.result(timeout=20)
            except Exception:
                raw_results[rp] = (rp, "unknown", {})

    for rp in files:
        parts.append(f"### {rp}")
        detail = detail_cache[rp]
        ftype_d = str((detail or {}).get("type") or "").upper()

        # 策略2：details 不存在 → 原始读取渲染
        if detail is None:
            _, ftype_r, data = raw_results.get(rp, (rp, "unknown", {}))
            consumed = _render_fallback_file(
                parts, rp, ftype_r, data,
                truncated_files, TEXT_TOTAL_CHAR_LIMIT, TEXT_FILE_CHAR_LIMIT,
                text_chars_used,
            )
            if ftype_r == "text":
                has_text_files = True
                text_chars_used += consumed
            continue

        # 策略3：ELF → 直接用 details 符号（核心优化，零额外 I/O）
        if ftype_d == "ELF":
            exports = detail.get("symbols") or []
            imports_l = detail.get("imports") or []
            needed = detail.get("needed") or []
            sh = detail.get("strings_head") or detail.get("strings") or []
            parts.append("类型: ELF（符号来自预处理 details）")
            if needed:
                parts.append(f"依赖库: {', '.join(str(x) for x in needed)}")
            if exports:
                parts.append(f"导出函数 ({len(exports)}个, 对外攻击面):")
                parts.append("```")
                parts.extend(str(s) for s in exports[:300])
                parts.append("```")
            if imports_l:
                parts.append(f"外部调用 ({len(imports_l)}个, 含潜在危险函数):")
                parts.append("```")
                parts.extend(str(s) for s in imports_l[:150])
                parts.append("```")
            if sh:
                parts.append(f"strings头部 ({len(sh)}行):")
                parts.append("```")
                parts.extend(str(s) for s in sh[:50])
                parts.append("```")
            if not exports and not imports_l:
                summary = str(detail.get("summary") or "").strip()
                if summary:
                    parts.append(f"摘要（details）: {summary}")
            continue

        # 策略4：文本类 → details 摘要前缀 + 读取实际文件内容
        if ftype_d in _TEXT_TYPES_UPPER or ftype_d == "":
            has_text_files = True
            summary = str(detail.get("summary") or "").strip()
            functions = detail.get("functions") or []
            if summary:
                parts.append(f"摘要（details）: {summary}")
            if functions:
                parts.append(
                    f"函数列表（details）: {', '.join(str(f) for f in functions[:20])}"
                )
            _, ftype_r, data = raw_results.get(rp, (rp, "text", {}))
            consumed = _render_fallback_file(
                parts, rp, "text", data,
                truncated_files, TEXT_TOTAL_CHAR_LIMIT, TEXT_FILE_CHAR_LIMIT,
                text_chars_used,
            )
            text_chars_used += consumed
            continue

        # 策略5：其他类型（BINARY/CERT/ARCHIVE/UNKNOWN等）→ 只输出 details 摘要
        summary = str(detail.get("summary") or "").strip()
        parts.append(f"类型: {detail.get('type', 'unknown')}")
        if summary:
            parts.append(f"摘要（details）: {summary}")

    if truncated_files:
        parts.append("")
        parts.append(
            f"⚠️ 以下 {len(truncated_files)} 个文件因总内容超限未展示，"
            f"可用 read 工具直接读取："
        )
        for tf in truncated_files:
            parts.append(f"  - target/{tf}")

    result_str = "\n".join(parts)
    prefix = "__HAS_TEXT__\n" if has_text_files else ""
    return prefix + result_str


def write_failure_report(
    report_path: Path,
    task_id: str,
    status_value: str,
    error: str,
    duration_ms: float,
    modules: list[str],
    modules_root: str,
) -> None:
    """任务失败/错误时生成 final_report.md，记录失败原因和已完成进度。"""
    normalized_status = str(status_value or "").strip().lower()
    if normalized_status == "cancelled":
        reason_title = "取消原因"
        fallback_error = "任务被取消"
    elif normalized_status == "failed":
        reason_title = "失败原因"
        fallback_error = "任务执行失败"
    else:
        reason_title = "错误原因"
        fallback_error = "unknown error"

    lines = [
        "# 固件系统威胁分析总报告",
        "",
        f"> ⚠️ **任务状态：{status_value.upper()}**",
        "",
        f"## {reason_title}",
        "",
        "```",
        f"{error or fallback_error}",
        "```",
        "",
        f"- 任务ID: {task_id}",
        f"- 耗时: {duration_ms / 1000:.1f}s",
        "",
        "## 已完成的模块",
        "",
    ]
    if modules:
        lines.append("| 模块 | 文件数 | 报告 |")
        lines.append("|------|--------|------|")
        for mod in modules:
            mod_dir = Path(modules_root) / mod
            flist = mod_dir / "files.list"
            report = mod_dir / "module_report.md"
            fc = 0
            if flist.exists():
                try:
                    fc = sum(1 for l in flist.read_text("utf-8").splitlines() if l.strip())
                except OSError:
                    pass
            has_report = "✅" if report.exists() and report.stat().st_size > 100 else "❌"
            lines.append(f"| {mod} | {fc} | {has_report} |")
        lines.append("")
        lines.append(f"**已发现 {len(modules)} 个模块**")
    else:
        lines.append("*尚未完成模块分类*")
    lines.append("")
    try:
        report_path.write_text("\n".join(lines), encoding="utf-8")
    except OSError:
        pass


def generate_modules_list(modules_dir: Path, output_path: Path) -> None:
    """生成 modules.list：按风险等级排序，每行一个模块名。"""
    RISK_ORDER = {"严重": 0, "高": 1, "中": 2, "低": 3, "信息": 4, "未知": 5}
    entries: list[tuple[str, int, str]] = []

    for mod_dir in sorted(modules_dir.iterdir()):
        if not mod_dir.is_dir():
            continue
        mod_name = mod_dir.name
        risk_level = "未知"
        risk_score = 0
        report = mod_dir / "module_report.md"
        if report.exists():
            text = report.read_text("utf-8", errors="replace")[:2000]
            m = re.search(r'RISK_LEVEL:\s*(.+?)\s*-->', text)
            if m:
                risk_level = m.group(1).strip()
            m = re.search(r'RISK_SCORE:\s*(\d+)', text)
            if m:
                risk_score = min(int(m.group(1)), 100)
        entries.append((risk_level, risk_score, mod_name))

    entries.sort(key=lambda e: (RISK_ORDER.get(e[0], 5), -e[1]))
    output_path.write_text(
        "\n".join(name for _, _, name in entries) + "\n", encoding="utf-8")


def strip_target_prefix(output_dir: Path, target_dir: str) -> None:
    """将输出文件中的容器绝对路径 /data/target/… 替换为相对路径。"""
    prefix = target_dir.rstrip("/") + "/"
    for p in output_dir.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix not in (".list", ".md", ".txt", ".json"):
            continue
        try:
            text = p.read_text(encoding="utf-8")
            if prefix in text:
                p.write_text(text.replace(prefix, ""), encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            pass

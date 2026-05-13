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
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import PipelineContext

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

def load_prompt(source, name: str, role: str | None = None) -> str:
    if role and hasattr(source, "get_prompt"):
        try:
            prompt = source.get_prompt(role, name)
            if isinstance(prompt, str) and prompt.strip():
                return prompt.strip()
        except Exception:
            pass
    prompt_dir = str(source or "")
    for ext in [".md", ".txt", ""]:
        p = Path(prompt_dir) / f"{name}{ext}"
        if p.exists():
            return p.read_text(encoding="utf-8").strip()
    return ""


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


def restore_module_for_retry(
    mod_name: str,
    mod_dir: "Path",
    workspace: "Path",
    refined_set: set[str],
) -> None:
    """重试前恢复模块状态（Python 接管，不靠 Worker bash 自清理）。

    1. 恢复快照 → mod_dir/files.list
    2. rm -rf 上一轮 Worker 新建的子模块（不在 refined_set 中的新增模块）
    3. 清空 mod_dir/deleted/（如存在）
    """
    mods_root = get_modules_root(str(workspace))
    snapshot_path = workspace / ".s2_snapshots" / f"{mod_name}.snapshot"

    # 1. 恢复快照
    if snapshot_path.exists():
        mod_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(snapshot_path), str(mod_dir / "files.list"))

    # 2. 删除上一轮新建的子模块
    current_mods = set(discover_modules(str(workspace)))
    for m in current_mods:
        if m == mod_name:
            continue
        if m in refined_set:
            continue
        # 任何不属于已完成模块的模块——如果它是上一轮这个模块的拆分产物就删除
        # 简单判断：迎合 mod_name 前缀，或者它的快照不存在（说明是本轮新建）
        sub_snap = workspace / ".s2_snapshots" / f"{m}.snapshot"
        is_new_sub = (m.startswith(mod_name + "_")
                      or m.startswith(mod_name)
                      or not sub_snap.exists())
        if is_new_sub:
            shutil.rmtree(str(mods_root / m), ignore_errors=True)

    # 3. 清空 deleted/
    deleted_dir = mod_dir / "deleted"
    if deleted_dir.exists():
        shutil.rmtree(str(deleted_dir), ignore_errors=True)


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
    """ELF 三层提取：nm 导出/导入符号 + readelf 依赖库 + strings 头部。"""
    res: dict = {"exports": [], "imports": [], "needed": [], "strings_head": []}
    try:
        r = subprocess.run(["nm", "-D", fullpath],
                           capture_output=True, text=True, timeout=15)
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
        res["strings_head"] = r.stdout.splitlines()[:50]
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
) -> str:
    """主从模式：子 Worker 并行分批读取文件，返回合并的文件摘要字符串。"""
    w_base = ctx.make_w_base()
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

            ar = await run_agent_checked(
                context=f"s2-sub-{mod_name}-batch{idx+1}",
                prompt=prompt,
                model=sub_model or w_base.get("model", ""),
                tools=[],
                system_prompt=sub_prompt_template,
                cwd=w_base["cwd"],
                thinking_level=w_base.get("thinking_level", "off"),
                session_file=str(ctx.sess_dir / f"sub-{mod_name}-batch{idx+1}.jsonl"),
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
    lines = [
        "# 固件系统威胁分析总报告",
        "",
        f"> ⚠️ **任务状态：{status_value.upper()}**",
        "",
        "## 失败原因",
        "",
        "```",
        f"{error or 'unknown error'}",
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

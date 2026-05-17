"""
pipeline/s0_sub_reader.py — Stage 0.2: 全量文件预读，生成 details/ JSON

入: ctx.filtered_files / workspace/filtered_files.txt
    ctx.file_catalog（可选，提供类型信息）
出: workspace/details/<path>.json（每个文件对应一个 JSON）
    ctx.details_dir = workspace/details/
    ctx.classify_context_path = workspace/classify_context.md

模式：
  python_only（默认）: Python 直接提取，零 LLM
    - ELF: nm/readelf 提取 symbols/imports/needed/strings_head
    - C/C++ 源码: ctags（若可用）或 grep 提取函数名
    - 其他文本: 读前4KB 作为 preview
  llm_assist: Python 提取 + LLM 生成语义摘要（仅对信息不足的文件触发）

checkpoint: s0_sub_reader（整体），per-file 靠 JSON 文件存在性做幂等
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import re
import subprocess
import time
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
from .helpers import read_one_elf, run_agent_with_stage_guard, load_prompt

# 源码函数签名 grep 模式（C/C++）
_C_FUNC_RE = re.compile(
    r"^(?:static\s+|inline\s+|extern\s+|__attribute__[^)]+\)\s*)?"
    r"[\w\s\*<>:,]+\s+(\w+)\s*\([^;{]*\)\s*(?:const\s*)?\{",
    re.MULTILINE,
)

# 文本类型集合（需要读取内容）
_TEXT_EXTS = frozenset({
    ".c", ".h", ".cpp", ".cc", ".cxx", ".hpp", ".hh", ".hxx", ".inc",
    ".s", ".S", ".asm",
    ".sh", ".bash", ".py", ".lua", ".pl", ".rb", ".tcl", ".awk",
    ".conf", ".cfg", ".ini", ".json", ".yaml", ".yml", ".xml",
    ".toml", ".env", ".properties",
    ".yang", ".mib", ".proto", ".asn", ".asn1",
    ".sql", ".md", ".txt", ".rst",
})

_ELF_MAGIC = b"\x7fELF"
MAX_TEXT_BYTES = 8192   # 每个文本文件最多读 8KB


def _extract_python_info(full_path: str, rel_path: str, ftype: str) -> dict:
    """Python 侧提取文件信息，无需 LLM。"""
    result: dict = {
        "path": rel_path,
        "type": ftype or "UNKNOWN",
        "summary": "",
        "symbols": None,
        "imports": None,
        "needed": None,
        "strings_head": None,
        "functions": None,
        "keywords": [],
        "suggested_module": "unknown",
        "confidence": "low",
    }

    try:
        with open(full_path, "rb") as f:
            magic = f.read(4)
    except OSError:
        result["summary"] = "文件不存在或无法读取"
        return result

    # ── ELF 文件 ─────────────────────────────────────────────────────────
    if magic == _ELF_MAGIC:
        result["type"] = "ELF"
        elf_data = read_one_elf(full_path)
        result["symbols"] = elf_data.get("exports", [])
        result["imports"] = elf_data.get("imports", [])
        result["needed"] = elf_data.get("needed", [])
        result["strings_head"] = elf_data.get("strings_head", [])
        # 从符号和依赖库推断摘要（简单规则，无 LLM）
        needed = result["needed"] or []
        exports = result["symbols"] or []
        keywords = []
        for lib in needed[:5]:
            kw = lib.split(".")[0].replace("lib", "")
            if len(kw) > 2:
                keywords.append(kw)
        for sym in exports[:5]:
            if len(sym) > 4:
                keywords.append(sym)
        result["keywords"] = list(dict.fromkeys(keywords))[:5]
        # 检测 ELF 类型以生成更准确的摘要
        _ext_ko = Path(rel_path).suffix.lower() in (".ko",) or ftype in ("KO", "KERNEL_MODULE")
        if exports:
            if _ext_ko:
                result["summary"] = (
                    f"内核模块，导出 {len(exports)} 个符号"
                    + (f"，示例：{', '.join(exports[:3])}" if exports[:3] else "")
                )
            else:
                result["summary"] = f"ELF 共享库，导出 {len(exports)} 个函数，依赖 {', '.join(needed[:3]) or '无'}"
            result["confidence"] = "medium"
        else:
            result["summary"] = "ELF 二进制，符号表为空（可能已 strip）"
            result["confidence"] = "low"
        return result

    # ── 文本/源码文件 ─────────────────────────────────────────────────────
    ext = Path(rel_path).suffix.lower()
    if ext in _TEXT_EXTS or ftype in (
        "C_SOURCE", "CPP_SOURCE", "HEADER", "SCRIPT_SHELL", "SCRIPT_PYTHON",
        "CONFIG_JSON", "CONFIG_YAML", "CONFIG_XML", "CONFIG_INI", "NETWORK_MODEL",
        "DATABASE_SQL", "TEXT_UNKNOWN",
    ):
        try:
            with open(full_path, encoding="utf-8", errors="replace") as f:
                content = f.read(MAX_TEXT_BYTES)
        except OSError:
            return result

        # C/C++ 源码：提取函数名
        if ext in (".c", ".h", ".cpp", ".cc", ".cxx", ".hpp", ".hh", ".hxx"):
            fns = list(dict.fromkeys(_C_FUNC_RE.findall(content)))[:30]
            result["functions"] = fns if fns else []
            result["type"] = "C_SOURCE" if ext == ".c" else (
                "HEADER" if ext in (".h", ".hpp", ".hh", ".hxx") else "CPP_SOURCE"
            )
            if fns:
                result["keywords"] = fns[:5]
                result["summary"] = f"C/C++ 源文件，包含函数: {', '.join(fns[:5])}"
                result["confidence"] = "medium"
            else:
                result["summary"] = f"C/C++ 源文件，未提取到函数名（可能为纯声明文件）"
                result["confidence"] = "low"

        # Python 脚本：提取函数和类名
        elif ext == ".py":
            result["type"] = "SCRIPT_PYTHON"
            fns = re.findall(r"^(?:def|class)\s+(\w+)", content, re.MULTILINE)
            fns = list(dict.fromkeys(fns))[:20]
            result["functions"] = fns
            result["keywords"] = fns[:5]
            if fns:
                result["summary"] = f"Python 脚本，定义: {', '.join(fns[:5])}"
                result["confidence"] = "medium"
            else:
                result["summary"] = "Python 脚本，未提取到函数定义"
                result["confidence"] = "low"

        # Shell 脚本：提取函数名
        elif ext in (".sh", ".bash"):
            result["type"] = "SCRIPT_SHELL"
            fns = re.findall(r"^(\w[\w_-]*)\s*\(\s*\)", content, re.MULTILINE)
            fns = list(dict.fromkeys(fns))[:20]
            result["functions"] = fns
            keywords = re.findall(r'\b(iptables|ip6tables|nft|brctl|route|ifconfig|sysctl|'
                                  r'openvpn|ipsec|pppd|hostapd|wpa_supplicant)\b',
                                  content[:2000])
            result["keywords"] = list(dict.fromkeys(keywords + fns))[:5]
            result["summary"] = f"Shell 脚本，{len(content.splitlines())} 行"
            if keywords:
                result["summary"] += f"，包含关键词: {', '.join(keywords[:3])}"
                result["confidence"] = "medium"
            else:
                result["confidence"] = "low"

        # 配置文件：提取关键配置项
        elif ext in (".json", ".yaml", ".yml", ".conf", ".cfg", ".ini", ".xml"):
            result["type"] = ftype or "CONFIG_CONF"
            keys = re.findall(r'"(\w[\w_-]{2,})"', content[:2000])[:10]
            result["keywords"] = list(dict.fromkeys(keys))[:5]
            result["summary"] = f"配置文件，{len(content.splitlines())} 行"
            result["confidence"] = "medium" if keys else "low"

        else:
            result["summary"] = f"文本文件，{len(content.splitlines())} 行"
            result["confidence"] = "low"
        return result

    # ── 其他二进制 ────────────────────────────────────────────────────────
    result["summary"] = f"二进制文件（类型: {ftype or 'unknown'}），无法进一步提取"
    result["confidence"] = "low"
    return result


def _get_file_type_from_catalog(catalog: dict, rel_path: str) -> str:
    """从 file_catalog 获取文件类型。"""
    for f in catalog.get("files", []):
        if f.get("path") == rel_path:
            return f.get("type", "")
    return ""


def _write_detail_json(detail_path: Path, data: dict) -> None:
    """原子写入 detail JSON 文件（tmp→rename 保证原子性）。

    tmp 文件名 = detail_path.name + ".tmp"，避免 with_suffix() 对多点文件名的歧义。
    例: libssl.so.1.1.json → libssl.so.1.1.json.tmp
    """
    import json as _json
    data["generated_at"] = _utc_now()
    # 追加 .tmp 而非替换后缀，对含多个点的文件名（libssl.so.1.1）更安全
    tmp = detail_path.parent / (detail_path.name + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(_json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.rename(detail_path)


def _utc_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class SubReaderStage(BaseStage):
    """Stage 0.2: 全量文件预读 → workspace/details/*.json"""

    stage_num = 0
    stage_name = "文件预读"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        cfg = ctx.cfg
        workspace = ctx.workspace

        # ── checkpoint 跳过（整体）───────────────────────────────────────
        # ctx.details_dir 和 ctx.classify_context_path 已由 orchestrator 初始化，无需重赋
        if cp and cp.is_done("s0_sub_reader"):
            ctx.emit_event("log", level="info",
                           msg=f"[S0-SubReader] checkpoint 已完成，跳过"
                               f"（details 目录: {ctx.details_dir}）")
            return

        # ── details/ 目录始终提前创建（保证目录可见，即使后续 early return）────
        # ctx.details_dir 已由 orchestrator 初始化为正确路径，直接使用
        details_dir = ctx.details_dir   # = workspace / "details"
        details_dir.mkdir(exist_ok=True)
        ctx.emit_event("log", level="info",
                       msg=f"[S0-SubReader] details 目录: {details_dir}")

        # ── 读取文件列表 ──────────────────────────────────────────────────
        ff = workspace / "filtered_files.txt"
        if not ff.exists():
            ctx.emit_event("log", level="warn",
                           msg="[S0-SubReader] filtered_files.txt 不存在，跳过")
            if cp:
                cp.mark_done("s0_sub_reader", skipped="no_filtered_files")
            return

        files = [l.strip() for l in ff.read_text(encoding="utf-8").splitlines() if l.strip()]
        if not files:
            if cp:
                cp.mark_done("s0_sub_reader", skipped="empty_file_list")
            return

        ctx.emit_event("stage", stage="sub_reader", file_count=len(files))

        # ── 确定模式和并行度 ──────────────────────────────────────────────
        mode = getattr(cfg, "sub_reader_mode", "python_only")
        batch_size = getattr(cfg, "sub_reader_batch_size", 20)
        parallel = max(1, getattr(cfg, "parallel_sub_workers", 4))
        catalog = ctx.file_catalog or {}

        # ── Python 提取（并行线程池） ─────────────────────────────────────
        loop = asyncio.get_event_loop()
        semaphore = asyncio.Semaphore(min(parallel, 32))
        target_dir = cfg.target_dir
        processed = 0
        skipped = 0
        start_ts = time.time()

        async def _process_one(rel: str) -> None:
            nonlocal processed, skipped
            detail_path = details_dir / (rel.lstrip("/") + ".json")
            # 幂等：已存在则跳过
            if detail_path.exists():
                skipped += 1
                return
            async with semaphore:
                ftype = _get_file_type_from_catalog(catalog, rel)
                data = await loop.run_in_executor(
                    None, _extract_python_info,
                    os.path.join(target_dir, rel), rel, ftype
                )
                _write_detail_json(detail_path, data)
                processed += 1

        await asyncio.gather(*[_process_one(f) for f in files])

        elapsed = time.time() - start_ts
        ctx.emit_event("log", level="info",
                       msg=f"[S0-SubReader] Python 提取完成: "
                           f"新增 {processed}，跳过(已存在) {skipped}，"
                           f"共 {len(files)} 个文件，耗时 {elapsed:.1f}s")

        # ── LLM 辅助摘要（llm_assist 模式，只补充信息不足的文件）───────────
        if mode == "llm_assist":
            await self._llm_supplement(ctx, files, details_dir, batch_size)

        # ── 生成 classify_context.md（供 ClassifyStage 使用）────────────
        await self._build_classify_context(ctx, files, details_dir)

        ctx.emit_event("stage_result", stage="sub_reader",
                       total=len(files), processed=processed, skipped=skipped)
        if cp:
            cp.mark_done("s0_sub_reader",
                         total=len(files),
                         processed=processed)

    async def _llm_supplement(
        self,
        ctx: PipelineContext,
        files: list[str],
        details_dir: Path,
        batch_size: int,
    ) -> None:
        """对 confidence=low 的文件，用 LLM 补充语义摘要。"""
        from .helpers import load_detail_json, is_detail_sufficient
        cfg = ctx.cfg
        sub_prompt = load_prompt(cfg, "step0_sub_reader", "workers")
        if not sub_prompt:
            return

        threshold = getattr(cfg, "sub_reader_llm_threshold", 0)
        low_conf_files = []
        for rel in files:
            d = load_detail_json(details_dir, rel)
            if not is_detail_sufficient(d):
                low_conf_files.append(rel)

        if not low_conf_files:
            return
        if threshold > 0 and len(low_conf_files) <= threshold:
            return

        ctx.emit_event("log", level="info",
                       msg=f"[S0-SubReader] LLM 补充 {len(low_conf_files)} 个低置信度文件")

        loop = asyncio.get_event_loop()
        sem = asyncio.Semaphore(max(1, getattr(cfg, "parallel_sub_workers", 4)))

        for i in range(0, len(low_conf_files), batch_size):
            batch = low_conf_files[i: i + batch_size]
            async with sem:
                parts = [f"以下是 {len(batch)} 个文件的结构化数据，请生成语义摘要：\n"]
                for rel in batch:
                    d = load_detail_json(details_dir, rel) or {}
                    ftype = d.get("type", "unknown")
                    syms = (d.get("symbols") or [])[:10]
                    fns = (d.get("functions") or [])[:10]
                    needed = (d.get("needed") or [])[:5]
                    parts.append(f"\n--- {rel} ---")
                    parts.append(f"类型: {ftype}")
                    if syms:
                        parts.append(f"导出符号: {', '.join(syms)}")
                    if fns:
                        parts.append(f"函数: {', '.join(fns)}")
                    if needed:
                        parts.append(f"依赖: {', '.join(needed)}")

                batch_no = i // batch_size + 1
                ar = await run_agent_with_stage_guard(
                    ctx=ctx,
                    stage="sub_reader_llm",
                    context=f"s0-sub-reader-llm-batch{i // batch_size + 1}",
                    heartbeat_payload_factory=lambda beat, batch_no=batch_no, batch_size=len(batch): {
                        "heartbeat": beat,
                        "batch": batch_no,
                        "batch_size": batch_size,
                    },
                    prompt="\n".join(parts),
                    model=cfg.workers.model_for("sub_read"),
                    system_prompt=sub_prompt,
                    tools=[],
                    cwd=str(ctx.workspace),
                    thinking_level="off",
                    session_file=str(ctx.sess_dir / f"sub-reader-llm-{i // batch_size + 1}.jsonl"),
                    cancel_event=ctx.cancel_event,
                    max_retries=cfg.agent_max_retries,
                    retry_delay=cfg.agent_retry_delay,
                    pi_max_retries=cfg.pi_max_retries,
                    pi_retry_delay=cfg.pi_retry_delay,
                )
                ctx.tokens += ar.token_usage

                # 解析并回写
                if ar.output:
                    raw = re.sub(r"<result>.*?</result>", "", ar.output, flags=re.DOTALL)
                    for line in raw.splitlines():
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            try:
                                item = json.loads(line)
                                path = item.get("path", "")
                                if path:
                                    dp = details_dir / (path.lstrip("/") + ".json")
                                    if dp.exists():
                                        existing = json.loads(dp.read_text(encoding="utf-8"))
                                        if item.get("summary"):
                                            existing["summary"] = item["summary"]
                                        if item.get("keywords"):
                                            existing["keywords"] = item["keywords"]
                                        if item.get("suggested_module"):
                                            existing["suggested_module"] = item["suggested_module"]
                                        if item.get("confidence"):
                                            existing["confidence"] = item["confidence"]
                                        _write_detail_json(dp, existing)
                            except Exception:
                                pass

    async def _build_classify_context(
        self,
        ctx: PipelineContext,
        files: list[str],
        details_dir: Path,
    ) -> None:
        """
        生成 workspace/classify_context.md：
        按文件类型汇总，供 ClassifyStage Worker 快速了解文件组成。
        """
        from collections import defaultdict
        from .helpers import load_detail_json

        type_groups: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for rel in files:
            d = load_detail_json(details_dir, rel) or {}
            ftype = d.get("type", "UNKNOWN")
            summary = d.get("summary", "")
            suggested = d.get("suggested_module", "unknown")
            type_groups[ftype].append((rel, summary, suggested))  # type: ignore[arg-type]

        lines = [
            "# 文件类型汇总（classify_context.md）",
            "",
            "> 由 SubReaderStage 自动生成，供 ClassifyStage Worker 参考。",
            "> 完整 details JSON 在 `details/<path>.json`，可用 `read` 工具按需查阅。",
            "",
            f"**总计：{len(files)} 个文件，{len(type_groups)} 种类型**",
            "",
        ]

        # 按类型分组展示（最多展示每组前10个文件）
        MAX_SHOW = 10
        for ftype, entries in sorted(type_groups.items(), key=lambda x: -len(x[1])):
            lines.append(f"## {ftype}（{len(entries)} 个文件）")
            lines.append("")
            # 按建议模块分组
            mod_map: dict[str, list[str]] = defaultdict(list)
            for e in entries:
                rel, summary, suggested = e[0], e[1], e[2]
                mod_map[suggested].append(rel)
            for mod, mod_files in sorted(mod_map.items(), key=lambda x: -len(x[1])):
                lines.append(f"### 建议模块: `{mod}`（{len(mod_files)} 个）")
                for f in mod_files[:MAX_SHOW]:
                    lines.append(f"- `{f}`")
                if len(mod_files) > MAX_SHOW:
                    lines.append(f"- ...（共 {len(mod_files)} 个，余下见 details/）")
                lines.append("")

        # ctx.classify_context_path 已由 orchestrator 初始化，直接写入
        ctx.classify_context_path.write_text("\n".join(lines), encoding="utf-8")
        ctx.emit_event("log", level="info",
                       msg=f"[S0-SubReader] classify_context.md 已生成 ({len(lines)} 行)")

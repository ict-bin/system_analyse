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

import ast
import threading
import queue
import time
import concurrent.futures
import json
import os
import re
import subprocess
import threading
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
_C_INCLUDE_RE = re.compile(r"^\s*#\s*include\s*[<\"]([^>\"]+)[>\"]", re.MULTILINE)
_C_CALL_RE = re.compile(r"\b([A-Za-z_]\w*)\s*\(")
_C_CALL_EXCLUDE = frozenset({
    "if", "for", "while", "switch", "return", "sizeof", "typeof", "alignof",
    "case", "do", "else", "static_assert", "offsetof", "container_of",
})

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

        # C/C++ 源码：提取函数定义、函数调用、include 依赖
        if ext in (".c", ".h", ".cpp", ".cc", ".cxx", ".hpp", ".hh", ".hxx"):
            # 函数名提取改用 tree-sitter（线性、不回溯），替代灵难性回溯的 _C_FUNC_RE
            from .func_extract import extract_cpp_functions
            fns = list(dict.fromkeys(f["name"] for f in extract_cpp_functions(content)))[:80]
            includes = list(dict.fromkeys(_C_INCLUDE_RE.findall(content)))[:80]
            include_keys = [Path(item).stem for item in includes if Path(item).stem]
            calls_raw = _C_CALL_RE.findall(content)
            defined = set(fns)
            calls = []
            for name in calls_raw:
                if name in defined or name in _C_CALL_EXCLUDE:
                    continue
                if name.startswith("__") and name.endswith("__"):
                    continue
                calls.append(name)
            calls = list(dict.fromkeys(calls))[:150]
            result["functions"] = fns if fns else []
            result["symbols"] = fns if fns else []
            result["imports"] = calls
            result["needed"] = include_keys[:80]
            result["source_imports"] = {
                "language": "c_cpp",
                "includes": includes[:80],
                "include_keys": include_keys[:80],
                "calls": calls[:150],
            }
            result["type"] = "C_SOURCE" if ext == ".c" else (
                "HEADER" if ext in (".h", ".hpp", ".hh", ".hxx") else "CPP_SOURCE"
            )
            if fns or includes or calls:
                result["keywords"] = list(dict.fromkeys(fns[:5] + include_keys[:5] + calls[:5]))[:8]
                parts = []
                if fns:
                    parts.append(f"定义函数: {', '.join(fns[:5])}")
                if includes:
                    parts.append(f"包含头文件: {', '.join(includes[:5])}")
                if calls:
                    parts.append(f"调用外部符号: {', '.join(calls[:5])}")
                result["summary"] = "C/C++ 源文件，" + "；".join(parts)
                result["confidence"] = "medium"
            else:
                result["summary"] = f"C/C++ 源文件，未提取到函数/依赖关系"
                result["confidence"] = "low"

        # Python 脚本：提取函数/类定义、import/from import、调用名
        elif ext == ".py":
            result["type"] = "SCRIPT_PYTHON"
            fns = re.findall(r"^(?:def|class)\s+(\w+)", content, re.MULTILINE)
            import_modules: list[str] = []
            imported_names: list[str] = []
            call_names: list[str] = []
            try:
                tree = ast.parse(content)
                for node in ast.walk(tree):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                        fns.append(node.name)
                    elif isinstance(node, ast.Import):
                        for alias in node.names:
                            import_modules.append(alias.name)
                            imported_names.append(alias.asname or alias.name.split(".")[-1])
                    elif isinstance(node, ast.ImportFrom):
                        if node.module:
                            import_modules.append(node.module)
                        for alias in node.names:
                            imported_names.append(alias.asname or alias.name)
                            if node.module:
                                import_modules.append(f"{node.module}.{alias.name}")
                    elif isinstance(node, ast.Call):
                        fn = node.func
                        if isinstance(fn, ast.Name):
                            call_names.append(fn.id)
                        elif isinstance(fn, ast.Attribute):
                            call_names.append(fn.attr)
            except SyntaxError:
                # fallback regex already populated fns
                import_modules.extend(re.findall(r"^\s*import\s+([\w\.]+)", content, re.MULTILINE))
                import_modules.extend(re.findall(r"^\s*from\s+([\w\.]+)\s+import\s+", content, re.MULTILINE))
            fns = list(dict.fromkeys(fns))[:80]
            import_modules = list(dict.fromkeys(m for m in import_modules if m))[:120]
            imported_names = list(dict.fromkeys(n for n in imported_names if n))[:120]
            call_names = list(dict.fromkeys(n for n in call_names if n and n not in set(fns)))[:150]
            result["functions"] = fns
            result["symbols"] = fns
            result["imports"] = list(dict.fromkeys(imported_names + call_names))[:150]
            result["needed"] = import_modules
            result["source_imports"] = {
                "language": "python",
                "modules": import_modules,
                "imported_names": imported_names,
                "calls": call_names,
            }
            result["keywords"] = list(dict.fromkeys(fns[:5] + imported_names[:5] + import_modules[:5]))[:8]
            if fns or import_modules:
                parts = []
                if fns:
                    parts.append(f"定义: {', '.join(fns[:5])}")
                if import_modules:
                    parts.append(f"导入模块: {', '.join(import_modules[:5])}")
                if call_names:
                    parts.append(f"调用: {', '.join(call_names[:5])}")
                result["summary"] = "Python 脚本，" + "；".join(parts)
                result["confidence"] = "medium"
            else:
                result["summary"] = "Python 脚本，未提取到函数定义或导入关系"
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

    def execute(self, ctx: PipelineContext) -> None:
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
        semaphore = threading.BoundedSemaphore(min(parallel, 32))
        target_dir = cfg.target_dir
        processed = 0
        skipped = 0
        start_ts = time.time()

        def _process_one(rel: str) -> None:
            nonlocal processed, skipped
            detail_path = details_dir / (rel.lstrip("/") + ".json")
            # 幂等：已存在则跳过
            if detail_path.exists():
                skipped += 1
                return
            with semaphore:
                ftype = _get_file_type_from_catalog(catalog, rel)
                data = loop.run_in_executor(
                    None, _extract_python_info,
                    os.path.join(target_dir, rel), rel, ftype
                )
                _write_detail_json(detail_path, data)
                processed += 1

        # [THREAD] replaced: # GATHER   # *[_process_one(f) for f in files])

        elapsed = time.time() - start_ts
        ctx.emit_event("log", level="info",
                       msg=f"[S0-SubReader] Python 提取完成: "
                           f"新增 {processed}，跳过(已存在) {skipped}，"
                           f"共 {len(files)} 个文件，耗时 {elapsed:.1f}s")

        # ── LLM 辅助摘要（llm_assist 模式，只补充信息不足的文件）───────────
        if mode == "llm_assist":
            self._llm_supplement(ctx, files, details_dir, batch_size)

        # ── 生成 classify_context.md（供 ClassifyStage 使用）────────────
        self._build_classify_context(ctx, files, details_dir)

        ctx.emit_event("stage_result", stage="sub_reader",
                       total=len(files), processed=processed, skipped=skipped)
        if cp:
            cp.mark_done("s0_sub_reader",
                         total=len(files),
                         processed=processed)

    def _llm_supplement(
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

        sem = threading.BoundedSemaphore(max(1, getattr(cfg, "parallel_sub_workers", 4)))

        for i in range(0, len(low_conf_files), batch_size):
            batch = low_conf_files[i: i + batch_size]
            with sem:
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
                ar = run_agent_with_stage_guard(
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
                    task_pi_dir=cfg.role_pi_dir("workers"),
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
                                import traceback
                                traceback.print_exc()
                                pass

    def _build_classify_context(
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

        # 目录结构先验：粗分类必须优先参考真实目录边界。
        path_tree: dict[str, int] = defaultdict(int)
        top_tree: dict[str, int] = defaultdict(int)
        for rel in files:
            parts = [p for p in rel.replace("\\", "/").split("/") if p]
            if len(parts) >= 2:
                top_tree[parts[0]] += 1
            if len(parts) >= 3:
                path_tree["/".join(parts[:2])] += 1
            elif len(parts) >= 2:
                path_tree[parts[0]] += 1
            else:
                path_tree["__root__"] += 1
        if path_tree:
            lines.extend([
                "## 目录结构先验（粗分类优先参考）",
                "",
                "> 大多数固件/源码树的目录边界就是最可靠的初始模块边界；除非 details/ 符号和依赖关系明确证明跨目录属于同一功能，否则 Stage1 应优先按目录聚合。",
                "",
                "### 一级目录",
            ])
            for path, count in sorted(top_tree.items(), key=lambda x: (-x[1], x[0]))[:80]:
                lines.append(f"- `{path}`：{count} 个文件")
            lines.extend(["", "### 二级目录/根分组"])
            for path, count in sorted(path_tree.items(), key=lambda x: (-x[1], x[0]))[:120]:
                lines.append(f"- `{path}`：{count} 个文件")
            lines.append("")

        # 导入导出/NEEDED 先验：为 Stage1/S2 的模块边界提供二进制依赖依据。
        lib_exports: dict[str, int] = defaultdict(int)
        lib_imports: dict[str, int] = defaultdict(int)
        lib_needed: dict[str, set[str]] = defaultdict(set)
        for rel in files:
            d = load_detail_json(details_dir, rel) or {}
            exports = d.get("exports") or d.get("symbols") or []
            imports = d.get("imports") or []
            needed = d.get("needed") or []
            if exports or imports or needed:
                base = Path(rel.replace("\\", "/")).name
                lib_exports[base] += len(exports)
                lib_imports[base] += len(imports)
                for item in needed[:20]:
                    lib_needed[base].add(str(item))
        if lib_exports or lib_imports or lib_needed:
            lines.extend([
                "## ELF/SO 导入导出依赖先验（模块划分必须参考）",
                "",
                "> `导入多、导出少` 的文件通常是上层业务/入口模块；`导出多、被 NEEDED/符号引用多` 的文件通常是底层库/公共能力。Stage1/S2 不应只看文件名，应结合这些关系决定模块边界。",
                "",
                "| 文件 | 导出符号数 | 导入符号数 | NEEDED 示例 |",
                "|---|---:|---:|---|",
            ])
            all_libs = sorted(set(lib_exports) | set(lib_imports) | set(lib_needed), key=lambda k: (-(lib_exports[k] + lib_imports[k] + len(lib_needed[k])), k))[:120]
            for base in all_libs:
                needed_sample = ", ".join(sorted(lib_needed.get(base, set()))[:8])
                lines.append(f"| `{base}` | {lib_exports.get(base, 0)} | {lib_imports.get(base, 0)} | {needed_sample or '-'} |")
            lines.append("")

        # 源码导入导出关系先验：C/C++/Python 等源码项目也能参与模块构图。
        source_rows: list[tuple[str, int, int, str]] = []
        for rel in files:
            d = load_detail_json(details_dir, rel) or {}
            source_imports = d.get("source_imports") or {}
            if not source_imports:
                continue
            exports = d.get("symbols") or d.get("functions") or []
            imports = d.get("imports") or []
            needed = d.get("needed") or []
            lang = source_imports.get("language") or d.get("type") or "source"
            source_rows.append((rel, len(exports), len(imports) + len(needed), str(lang)))
        if source_rows:
            lines.extend([
                "## 源码导入导出依赖先验（C/C++/Python 等）",
                "",
                "> 源码文件也会提取定义函数/类、函数调用、#include、Python import/from import，并参与后续模块依赖图构建。",
                "",
                "| 文件 | 语言/类型 | 导出定义数 | 导入/调用/包含数 |",
                "|---|---|---:|---:|",
            ])
            for rel, export_count, import_count, lang in sorted(source_rows, key=lambda x: (-(x[1] + x[2]), x[0]))[:120]:
                lines.append(f"| `{rel}` | {lang} | {export_count} | {import_count} |")
            lines.append("")

        # 按类型分组展示（最多展示每组前10个文件）
        MAX_SHOW = 10

        # ── 路径推断模块名对照（复用 PathGroupStage v2 已算好的结果）───
        _path_module = ctx.path_group_map  # {file_path: module_name}
        _path_module_counts: dict[str, int] = defaultdict(int)
        for _mod in _path_module.values():
            _path_module_counts[_mod] += 1

        # ── 路径推断摘要表 ─────────────────────────────────────────
        lines.extend([
            "",
            "## 路径推断模块 vs LLM推断模块 对照",
            "",
            "> 路径推断由 PathGroupStage v2 基于目录边界生成，",
            "> LLM 推断由 SubReader 基于文件内容分析生成。",
            "> 两种建议均可参考；通常路径推断更贴近项目既有目录结构。",
            "",
            "| 路径推断模块 | 文件数 | LLM推断匹配度 |",
            "|---|---:|---|",
        ])
        for _pm in sorted(_path_module_counts.keys(), key=lambda k: -_path_module_counts[k]):
            lines.append(f"| `{_pm}` | {_path_module_counts[_pm]} | - |")
        lines.append("")
        lines.append(f"> 共 {len(_path_module_counts)} 个路径模块，{sum(_path_module_counts.values())} 个文件")
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

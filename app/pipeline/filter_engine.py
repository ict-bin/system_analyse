from __future__ import annotations

import json
import math
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

from .helpers import StageError, discover_modules, run_agent_with_stage_guard


_SAFE_MODULE_RE = re.compile(r"[^a-zA-Z0-9._-]+")
_CONTEXT_WINDOW_BY_MODEL = {
    "gpt-5.4": 128_000,
    "gpt-5.4-mini": 128_000,
    "gpt-5.5": 256_000,
    "gpt-5.3-codex": 128_000,
    "gpt-5.2": 200_000,
    "MiniMax-M2.7": 128_000,
}
_DEFAULT_CONTEXT_WINDOW = 128_000
_BATCH_INPUT_RATIO = 0.5
_BATCH_SAFETY_RATIO = 0.8
_MIN_BATCH_TOKEN_LIMIT = 4_000
_PATH_HINTS = (
    "src", "source", "lib", "libs", "bin", "sbin", "usr", "web", "www", "api",
    "service", "services", "config", "conf", "etc", "plugin", "plugins",
    "proto", "protocol", "parser", "auth", "crypto", "security", "ipc",
)


@dataclass
class TreeNode:
    relative_path: str
    node_type: str
    size: int
    suffix: str
    depth: int
    path_hint: list[str]

    def to_prompt_line(self) -> str:
        hint = ",".join(self.path_hint) if self.path_hint else "-"
        return (
            f"{self.node_type}|path={self.relative_path}|size={self.size}|"
            f"suffix={self.suffix or '-'}|depth={self.depth}|hint={hint}"
        )


@dataclass
class FilterEngineStats:
    file_count: int
    module_count: int
    batch_count: int
    fallback_reason: str = ""


def normalize_filter_engine(value: str | None) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if candidate in {"script", "agent"} else "script"


def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    ascii_chars = sum(1 for ch in text if ord(ch) < 128)
    non_ascii_chars = len(text) - ascii_chars
    return max(1, math.ceil(ascii_chars / 4) + non_ascii_chars)


def model_context_window(model: str) -> int:
    normalized = str(model or "").strip()
    for key, value in _CONTEXT_WINDOW_BY_MODEL.items():
        if key in normalized:
            return value
    return _DEFAULT_CONTEXT_WINDOW


def build_tree_nodes(target_dir: str | Path) -> list[TreeNode]:
    root = Path(target_dir)
    nodes: list[TreeNode] = []
    for current_root, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in {".git", ".svn", "__pycache__"})
        current_path = Path(current_root)
        for dirname in dirnames:
            full = current_path / dirname
            rel = full.relative_to(root).as_posix()
            nodes.append(TreeNode(
                relative_path=rel,
                node_type="dir",
                size=0,
                suffix="",
                depth=len(Path(rel).parts),
                path_hint=_path_hints(rel),
            ))
        for filename in sorted(filenames):
            full = current_path / filename
            rel = full.relative_to(root).as_posix()
            try:
                size = full.stat().st_size
            except OSError:
                size = 0
            nodes.append(TreeNode(
                relative_path=rel,
                node_type="file",
                size=size,
                suffix=full.suffix.lower(),
                depth=len(Path(rel).parts),
                path_hint=_path_hints(rel),
            ))
    nodes.sort(key=lambda item: (item.relative_path.count("/"), item.relative_path))
    return nodes


def batch_tree_nodes(nodes: list[TreeNode], token_limit: int) -> list[list[TreeNode]]:
    if not nodes:
        return [[]]
    batches: list[list[TreeNode]] = []
    current: list[TreeNode] = []
    current_tokens = 0
    for node in nodes:
        line_tokens = estimate_tokens(node.to_prompt_line()) + 8
        if current and current_tokens + line_tokens > token_limit:
            batches.append(current)
            current = []
            current_tokens = 0
        if line_tokens > token_limit and not current:
            batches.append([node])
            continue
        current.append(node)
        current_tokens += line_tokens
    if current:
        batches.append(current)
    return batches


def validate_filter_outputs(
    *,
    filtered_files: list[str],
    module_map: dict[str, list[str]],
) -> None:
    filtered_set = {item for item in filtered_files if item}
    seen_files: dict[str, str] = {}
    seen_modules: set[str] = set()
    if not filtered_set:
        raise StageError("agent filter produced empty filtered_files.txt")
    for raw_module, files in module_map.items():
        module = sanitize_module_name(raw_module)
        if not module:
            raise StageError("agent filter produced empty module name")
        if module in seen_modules:
            raise StageError(f"agent filter produced duplicate module name: {module}")
        seen_modules.add(module)
        if not files:
            raise StageError(f"agent filter produced empty module: {module}")
        for rel_path in files:
            if rel_path not in filtered_set:
                raise StageError(f"module file not found in filtered_files.txt: {module} -> {rel_path}")
            owner = seen_files.get(rel_path)
            if owner and owner != module:
                raise StageError(f"file assigned to multiple modules: {rel_path} ({owner}, {module})")
            seen_files[rel_path] = module


def sanitize_module_name(value: str) -> str:
    normalized = _SAFE_MODULE_RE.sub("_", str(value or "").strip().lower()).strip("._-")
    return normalized[:120]


def write_filter_outputs(
    *,
    workspace: Path,
    filtered_files: list[str],
    module_map: dict[str, list[str]],
) -> list[str]:
    modules_root = workspace / "modules"
    if modules_root.exists():
        shutil.rmtree(modules_root)
    modules_root.mkdir(parents=True, exist_ok=True)

    normalized_map: dict[str, list[str]] = {}
    for module, files in module_map.items():
        safe_module = sanitize_module_name(module)
        normalized_map[safe_module] = sorted(dict.fromkeys(files))
        mod_dir = modules_root / safe_module
        mod_dir.mkdir(parents=True, exist_ok=True)
        (mod_dir / "files.list").write_text(
            "\n".join(normalized_map[safe_module]) + "\n",
            encoding="utf-8",
        )

    module_names = sorted(normalized_map.keys())
    (workspace / "modules.list").write_text("\n".join(module_names) + "\n", encoding="utf-8")
    (workspace / "filtered_files.txt").write_text(
        "\n".join(sorted(dict.fromkeys(filtered_files))) + "\n",
        encoding="utf-8",
    )
    return module_names


def _path_hints(relative_path: str) -> list[str]:
    lowered = relative_path.lower()
    return [hint for hint in _PATH_HINTS if f"/{hint}" in f"/{lowered}" or lowered.startswith(f"{hint}/")]


def _batch_prompt(
    *,
    batch_index: int,
    batch_count: int,
    module_granularity: str,
    analyse_targets: list[str],
    security_focus_categories: list[str],
    tree_lines: list[str],
) -> str:
    return (
        "你正在执行“文件树级过滤+叶子模块划分”。\n"
        "只允许依据路径、目录结构、后缀、大小、层级和 path hint 判断，禁止读取文件内容。\n\n"
        f"当前批次: {batch_index}/{batch_count}\n"
        f"analyse_targets={analyse_targets}\n"
        f"security_focus_categories={security_focus_categories}\n"
        f"module_granularity={module_granularity}\n\n"
        "输出必须是 JSON，对象格式如下：\n"
        "{\n"
        '  "items": [\n'
        '    {"path":"相对路径","include":true,"module":"最低层级叶子模块名","reason":"保留或丢弃原因"}\n'
        "  ]\n"
        "}\n\n"
        "规则：\n"
        "1. 只对 file 节点输出 items，目录节点只用于辅助判断。\n"
        "2. 丢弃时 include=false，module 置为空字符串。\n"
        "3. 保留时 module 必须是后续可直接使用的叶子模块名，不要输出父模块或空名。\n"
        "4. 输出里的 path 必须原样引用输入路径。\n\n"
        "文件树节点列表：\n"
        + "\n".join(tree_lines)
    )


def _merge_prompt(
    *,
    module_granularity: str,
    analyse_targets: list[str],
    security_focus_categories: list[str],
    batch_outputs: list[dict],
) -> str:
    return (
        "你正在执行全局模块归并，需要把多个批次的过滤结果整理成最终模块产物。\n"
        "不要新增不在输入里的文件。\n"
        "可以做模块同义归并、小碎片合并和命名统一，但必须保持叶子模块粒度。\n\n"
        f"analyse_targets={analyse_targets}\n"
        f"security_focus_categories={security_focus_categories}\n"
        f"module_granularity={module_granularity}\n\n"
        "输出必须是 JSON，对象格式如下：\n"
        "{\n"
        '  "filtered_files": ["a/b.c"],\n'
        '  "modules": [\n'
        '    {"module":"模块名","files":["a/b.c","x/y.c"],"reason":"合并说明"}\n'
        "  ]\n"
        "}\n\n"
        "批次结果如下：\n"
        f"{json.dumps(batch_outputs, ensure_ascii=False, indent=2)}"
    )


def _parse_json_output(raw: str, context: str) -> dict:
    text = str(raw or "").strip()
    if not text:
        raise StageError(f"{context} returned empty output")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}\s*$", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError as exc:
                raise StageError(f"{context} json parse failed: {exc}") from exc
        raise StageError(f"{context} json parse failed")


def run_agent_filter_engine(ctx) -> FilterEngineStats:
    cfg = ctx.cfg
    workspace = ctx.workspace
    selected_engine = normalize_filter_engine(getattr(cfg, "filter_engine", "script"))
    ctx.selected_filter_engine = selected_engine
    ctx.effective_filter_engine = selected_engine

    classify_model = cfg.workers.model_for("classify")
    judge_model = cfg.judges.model_for("classify") or classify_model
    context_window = model_context_window(classify_model)
    token_limit = max(
        _MIN_BATCH_TOKEN_LIMIT,
        int(context_window * _BATCH_INPUT_RATIO * _BATCH_SAFETY_RATIO),
    )
    nodes = build_tree_nodes(cfg.target_dir)
    batches = batch_tree_nodes(nodes, token_limit)
    file_nodes = [node for node in nodes if node.node_type == "file"]
    ctx.emit_event(
        "stage",
        stage="filter-engine",
        selected_engine=selected_engine,
        effective_engine="agent",
        model=classify_model,
        judge_model=judge_model,
        context_window=context_window,
        batch_input_token_limit=token_limit,
        node_count=len(nodes),
        file_node_count=len(file_nodes),
        batch_count=len(batches),
    )

    batch_outputs: list[dict] = []
    for idx, batch in enumerate(batches, start=1):
        session_file = ctx.session_path("filter-engine", f"batch-{idx}.jsonl")
        payload = {
            "batch_index": idx,
            "batch_count": len(batches),
            "session_file": session_file,
        }
        ctx.emit_event("stage", stage="filter-tree-batch", **payload)
        ar = run_agent_with_stage_guard(
            ctx=ctx,
            stage="filter-tree-batch",
            context=f"filter-tree-batch-{idx}",
            heartbeat_payload_factory=lambda beat, p=dict(payload): {**p, "heartbeat": beat},
            prompt=_batch_prompt(
                batch_index=idx,
                batch_count=len(batches),
                module_granularity=cfg.module_granularity,
                analyse_targets=list(cfg.analyse_targets),
                security_focus_categories=list(cfg.security_focus_categories),
                tree_lines=[node.to_prompt_line() for node in batch],
            ),
            model=classify_model,
            system_prompt="你是文件过滤与功能模块划分智能体，严格输出 JSON，不要输出额外解释。",
            session_file=session_file,
            cwd=str(workspace),
            tools=cfg.workers.default_tools,
            env={**os.environ, "TMPDIR": str(ctx.task_tmp), "HOME": str(workspace)},
            task_pi_dir=getattr(cfg, "task_pi_dir", ""),
            thinking_level="off",
            cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries,
            retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries,
            pi_retry_delay=cfg.pi_retry_delay,
        )
        ctx.tokens += ar.token_usage
        parsed = _parse_json_output(ar.output, f"filter batch {idx}")
        items = parsed.get("items")
        if not isinstance(items, list):
            raise StageError(f"filter batch {idx} items missing")
        batch_outputs.append({
            "batch_index": idx,
            "items": items,
        })
        ctx.emit_event(
            "stage_result",
            stage="filter-tree-batch",
            batch_index=idx,
            batch_count=len(batches),
            item_count=len(items),
            session_file=session_file,
        )

    merge_session = ctx.session_path("filter-engine", "merge.jsonl")
    ctx.emit_event("stage", stage="filter-merge", session_file=merge_session, batch_count=len(batch_outputs))
    merge_result = run_agent_with_stage_guard(
        ctx=ctx,
        stage="filter-merge",
        context="filter-merge",
        heartbeat_payload_factory=lambda beat: {"heartbeat": beat, "session_file": merge_session},
        prompt=_merge_prompt(
            module_granularity=cfg.module_granularity,
            analyse_targets=list(cfg.analyse_targets),
            security_focus_categories=list(cfg.security_focus_categories),
            batch_outputs=batch_outputs,
        ),
        model=judge_model,
        system_prompt="你是全局模块归并智能体，严格输出 JSON，不要输出额外解释。",
        session_file=merge_session,
        cwd=str(workspace),
        tools=cfg.judges.default_tools,
            env={**os.environ, "TMPDIR": str(ctx.task_tmp), "HOME": str(workspace)},
            task_pi_dir=getattr(cfg, "task_pi_dir", ""),
            thinking_level="off",
        cancel_event=ctx.cancel_event,
        max_retries=cfg.agent_max_retries,
        retry_delay=cfg.agent_retry_delay,
        pi_max_retries=cfg.pi_max_retries,
        pi_retry_delay=cfg.pi_retry_delay,
    )
    ctx.tokens += merge_result.token_usage
    merged = _parse_json_output(merge_result.output, "filter merge")
    filtered_files = merged.get("filtered_files")
    modules = merged.get("modules")
    if not isinstance(filtered_files, list) or not isinstance(modules, list):
        raise StageError("filter merge missing filtered_files/modules")

    module_map: dict[str, list[str]] = {}
    for item in modules:
        if not isinstance(item, dict):
            continue
        module_name = sanitize_module_name(item.get("module"))
        files = [str(path).strip() for path in (item.get("files") or []) if str(path).strip()]
        if not module_name or not files:
            continue
        module_map.setdefault(module_name, [])
        module_map[module_name].extend(files)

    normalized_filtered = [
        str(path).strip()
        for path in filtered_files
        if str(path).strip()
    ]
    validate_filter_outputs(filtered_files=normalized_filtered, module_map=module_map)
    module_names = write_filter_outputs(
        workspace=workspace,
        filtered_files=normalized_filtered,
        module_map=module_map,
    )
    ctx.filtered_files = sorted(dict.fromkeys(normalized_filtered))
    ctx.filter_count = len(ctx.filtered_files)
    ctx.classified_modules = module_names
    ctx.effective_filter_engine = "agent"
    return FilterEngineStats(
        file_count=ctx.filter_count,
        module_count=len(module_names),
        batch_count=len(batches),
    )


def load_script_filter_outputs(workspace: Path, ctx) -> None:
    filtered_path = workspace / "filtered_files.txt"
    if filtered_path.exists():
        lines = [l.strip() for l in filtered_path.read_text("utf-8", errors="replace").splitlines() if l.strip()]
        ctx.filtered_files = lines
        ctx.filter_count = len(lines)
        (workspace / ".filtered_backup.txt").write_text(
            filtered_path.read_text("utf-8"),
            encoding="utf-8",
        )
    ctx.classified_modules = discover_modules(workspace)

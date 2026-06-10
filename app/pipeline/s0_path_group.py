"""
pipeline/s0_path_group.py — Stage 0.3: 路径先验分组（纯 Python，无 LLM） v2

v2 改进: 利用项目已有目录结构推断模块边界，而非取最深目录名。
  好项目已经按功能划分好了目录层级:
    src/storage/buffer/  → storage 模块（非 buffer）
    src/storage/page/    → storage 模块（非 page）
    contrib/adminpack/   → adminpack 模块

算法:
  1. 找所有文件的公共前缀 → 项目根，剥离
  2. 剥离通用结构前缀 (src/, contrib/, include/, lib/, bin/...)
  3. 在剩余路径中找"功能模块边界"——即深度>=2 的公共父目录
     - 同级目录数 > 1 且每个 >= MIN_FILES 的 → 独立模块
     - 同级只有一个或太小 → 合并到父级
  4. 共享库/内核模块 → 单独处理（special_groups，保持不变）
"""

from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext

# ── 通用结构分段（无功能语义，会被跳过）─────────────────────────────
_SKIP_SEGMENTS = frozenset({
    "bin", "sbin", "usr", "lib", "lib32", "lib64", "lib64d",
    "libexec", "local", "etc", "var", "opt", "run", "tmp",
    "proc", "sys", "dev", "home", "root", "mnt", "media",
    "srv", "share", "include", "src", "source", "build",
    "install", "out", "output", "release", "debug", "target",
    "objs", "obj", "objects", ".", "..", "",
    # ── 以下在开源项目中常见，但对应源码目录结构本身就带功能语义 ──
    # "common", "utils", "tools", "test", "tests", "examples"  ← 保留不跳过
})

# ── 源码树结构的顶层容器（剥离后获得功能子树）─────────────────────
_STRUCTURAL_ROOTS = frozenset({
    "src", "contrib", "include", "lib", "bin", "tools",
    "platform", "app", "apps", "modules", "plugins",
    "extensions", "extension", "third_party", "vendor",
    "gausskernel", "common", "backend", "frontend",
    "interfaces",
})

# ── 共享库/内核模块模式 ───────────────────────────────────────────
import re
_SHARED_LIB_RE = re.compile(
    r"^lib.+\.(so|a)([\.\-_\d]|$)"
    r"|^lib.+\.ko(\.xz|\.gz|\.zst)?$"
    r"|^.+\.ko(\.xz|\.gz|\.zst)?$",
    re.IGNORECASE,
)
_LIB_BASE_RE = re.compile(r"^(lib[a-zA-Z0-9_+\-]+?)(?:\.so|\.ko|\.a|[-_]\d)", re.IGNORECASE)

# ── 一个"模块"至少要有这么多文件，否则合并到父级 ──────────────────
_MIN_MODULE_FILES = 3


def _is_shared_lib(rel_path: str) -> bool:
    return bool(_SHARED_LIB_RE.match(Path(rel_path.replace("\\", "/")).name))


def _soname_prefix(rel_path: str) -> str | None:
    m = _LIB_BASE_RE.match(Path(rel_path.replace("\\", "/")).name)
    return m.group(1).lower() if m else None


def _common_prefix(paths: list[str]) -> str:
    """找所有路径的最长公共目录前缀（不含文件名）。"""
    if not paths:
        return ""
    def _parts(p: str) -> list[str]:
        return p.replace("\\", "/").rstrip("/").split("/")[:-1]  # strip filename
    common = _parts(paths[0])
    for p in paths[1:]:
        parts = _parts(p)
        i = 0
        while i < min(len(common), len(parts)) and common[i] == parts[i]:
            i += 1
        common = common[:i]
    return "/".join(common)


def _is_meaningful(name: str) -> bool:
    """目录名是否携带功能语义（不是通用容器）。"""
    clean = name.lower().lstrip("0123456789.-_")
    return clean not in _SKIP_SEGMENTS and len(clean) >= 2


# ── 核心：推断功能模块边界 ────────────────────────────────────────

def _strip_common_prefixes(rel_paths: list[str]) -> list[list[str]]:
    """
    对每个文件路径:
      1. 剥离文件名
      2. 剥离项目公共前缀（所有文件的最长公共路径）
      3. 剥离最外层通用结构前缀 (src/contrib/include...)
    返回每个文件的功能子路径（分段列表）。
    """
    if not rel_paths:
        return []

    project_root = _common_prefix(rel_paths)
    root_segments = project_root.split("/") if project_root else []

    stripped: list[list[str]] = []
    for rel in rel_paths:
        clean = rel.replace("\\", "/")
        # 剥离项目公共前缀
        if root_segments and clean.startswith(project_root + "/"):
            clean = clean[len(project_root) + 1:]
        elif root_segments:
            # 尝试部分匹配
            for i in range(min(len(root_segments), 3), 0, -1):
                prefix = "/".join(root_segments[:i]) + "/"
                if clean.startswith(prefix):
                    clean = clean[len(prefix):]
                    break

        # 去掉文件名
        parts = clean.split("/")[:-1]
        if not parts:
            stripped.append([])
            continue

        # 剥离最前面若干层通用结构前缀，但最多剥 3 层
        strip_count = 0
        while strip_count < 3 and parts:
            head = parts[0].lower()
            if head in _STRUCTURAL_ROOTS:
                parts.pop(0)
                strip_count += 1
            else:
                break

        stripped.append(parts)

    return stripped


def _find_module_boundary(path_parts_lists: list[list[str]]) -> dict[int, str]:
    """
    给定所有文件的功能子路径，找出每个深度级别中各目录的文件数。
    返回 {file_index: module_name} 的映射。

    算法:
      对每个文件的功能子路径:
        - 路径为空 → group="__root__"
        - 取第一个有意义段作为模块边界
        - 然后做第二轮合并: 同级目录中子级文件数 < _MIN_MODULE_FILES 的，
          合并到父级模块中
    """
    # 第一遍: 每个文件取其第一个有意义段作为候选模块
    result: dict[int, str] = {}
    for idx, parts in enumerate(path_parts_lists):
        if not parts:
            result[idx] = "__root__"
            continue
        # 跳过开头的无意义段
        start = 0
        while start < len(parts) and not _is_meaningful(parts[start]):
            start += 1
        if start >= len(parts):
            result[idx] = parts[-1].lower() if parts else "__root__"
        else:
            result[idx] = parts[start].lower()

    # 第二遍: 统计每个模块的文件数
    module_counts: dict[str, int] = defaultdict(int)
    for idx, mod in result.items():
        module_counts[mod] += 1

    # 第三遍: 合并过小的模块到父级
    for idx, parts in enumerate(path_parts_lists):
        current_mod = result.get(idx, "__root__")
        if module_counts.get(current_mod, 0) >= _MIN_MODULE_FILES:
            continue  # 已经足够大
        if not parts:
            continue
        # 往上找一级: 如果有父级且父级文件数 >= MIN → 合并
        start = 0
        while start < len(parts) and not _is_meaningful(parts[start]):
            start += 1
        if start + 1 < len(parts):
            parent_level = parts[start + 1].lower()
            # 检查同级兄弟的文件数: 所有以当前段为后缀的文件
            sibling_count = 0
            for idx2, parts2 in enumerate(path_parts_lists):
                s2 = 0
                while s2 < len(parts2) and not _is_meaningful(parts2[s2]):
                    s2 += 1
                if s2 < len(parts2) and parts2[s2].lower() == parent_level:
                    sibling_count += 1
            if sibling_count >= _MIN_MODULE_FILES:
                result[idx] = parent_level
            else:
                # 父级也不够 → 继续往上
                # 用 segments 中更靠近根的那一层
                # 其实就是取路径中第一个有意义段作为模块
                # 但如果所有兄弟都不够 → 尝试再往上
                pass

    return result


def _build_path_groups_v2(
    files: list[str],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """
    返回:
      normal_groups  {module_name: [rel_path, ...]}
      special_groups {lib_prefix: [rel_path, ...]}
    """
    normal_files: list[str] = []
    special_groups: dict[str, list[str]] = defaultdict(list)

    for rel in files:
        if _is_shared_lib(rel):
            key = _soname_prefix(rel) or "__unmatched_shared__"
            special_groups[key].append(rel)
        else:
            normal_files.append(rel)

    if not normal_files:
        return {}, dict(special_groups)

    stripped = _strip_common_prefixes(normal_files)
    boundaries = _find_module_boundary(stripped)

    normal_groups: dict[str, list[str]] = defaultdict(list)
    for idx, rel in enumerate(normal_files):
        mod = boundaries.get(idx, "__root__")
        normal_groups[mod].append(rel)

    return dict(normal_groups), dict(special_groups)


def _render_markdown_v2(
    normal_groups: dict[str, list[str]],
    special_groups: dict[str, list[str]],
    max_sample: int = 8,
) -> str:
    lines: list[str] = [
        "## 路径先验分组（Path-Inferred Groups）v2",
        "",
        "> 算法: 利用项目已有目录结构推断功能模块边界。",
        "> 剥离项目公共前缀 + 通用结构前缀后，取第 1~3 层有意义目录作为模块名。",
        "> 特殊文件（共享库/内核模块）单独列出，需 LLM 判断归属。",
        "",
    ]

    if normal_groups:
        lines += ["### 推断模块（可按需合并/拆分）", ""]
        for gname, flist in sorted(normal_groups.items(), key=lambda x: -len(x[1])):
            lines.append(f"**[{gname}]** — {len(flist)} 个文件")
            for f in flist[:max_sample]:
                lines.append(f"  - `{f}`")
            if len(flist) > max_sample:
                lines.append(f"  - ... 共 {len(flist)} 个文件")
            lines.append("")
    else:
        lines += ["### 推断模块", "", "（无）", ""]

    if special_groups:
        lines += ["### 特殊文件（共享库 / 内核模块，需 LLM 判断归属）", ""]
        for gname, flist in sorted(special_groups.items(), key=lambda x: -len(x[1])):
            lines.append(f"**[{gname}]** — {len(flist)} 个文件")
            for f in flist[:max_sample]:
                lines.append(f"  - `{f}`")
            if len(flist) > max_sample:
                lines.append(f"  - ... 共 {len(flist)} 个文件")
            lines.append("")
    else:
        lines += ["### 特殊文件", "", "（无）", ""]

    total_groups = len(normal_groups) + len(special_groups)
    total_files = sum(len(v) for v in normal_groups.values()) + sum(len(v) for v in special_groups.values())
    lines += [
        f"**汇总**: {len(normal_groups)} 个推断模块 + {len(special_groups)} 个特殊文件组"
        f"，覆盖 {total_files} 个文件",
        "",
    ]
    return "\n".join(lines)


class PathGroupStage(BaseStage):
    """Stage 0.3: 路径先验分组 — 利用项目目录结构推断模块边界"""

    stage_num = 0
    stage_name = "路径先验分组"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint

        if cp and cp.is_done("s0_pathgroup"):
            p = ctx.workspace / "prescan" / "path_groups.md"
            if p.exists():
                pg_content = p.read_text("utf-8", errors="replace")
                separator = "\n\n---\n\n" if ctx.prescan_summary else ""
                ctx.prescan_summary = ctx.prescan_summary + separator + pg_content[:2000]
            ctx.emit_event("log", level="info", msg="[S0-PathGroup] checkpoint已完成，跳过")
            return

        if not ctx.filtered_files:
            if cp:
                cp.mark_done("s0_pathgroup", skipped="no_filtered_files")
            return

        ctx.emit_event("stage", stage="path_group", file_count=len(ctx.filtered_files))

        normal_groups, special_groups = _build_path_groups_v2(ctx.filtered_files)

        # ── 存入 ctx 供 SubReader classify_context.md 使用 ─────────────
        ctx.path_group_map.clear()
        for _mod, _flist in normal_groups.items():
            for _f in _flist:
                ctx.path_group_map[_f] = _mod
        for _mod, _flist in special_groups.items():
            for _f in _flist:
                ctx.path_group_map[_f] = f"[特殊]{_mod}"

        md = _render_markdown_v2(normal_groups, special_groups)

        prescan_dir = ctx.workspace / "prescan"
        prescan_dir.mkdir(parents=True, exist_ok=True)
        out_path = prescan_dir / "path_groups.md"
        out_path.write_text(md, encoding="utf-8")

        # 仅注入统计摘要（不注入完整文件列表）
        group_summary_lines: list[str] = ["### 路径先验分组摘要（完整列表见 prescan/path_groups.md）\n"]
        for group_name, files in sorted(normal_groups.items(), key=lambda x: -len(x[1])):
            group_summary_lines.append(f"- {len(files):>4} 个文件 → [{group_name}]")
        for group_name, files in sorted(special_groups.items(), key=lambda x: -len(x[1])):
            group_summary_lines.append(f"- {len(files):>4} 个文件 → [{group_name}]（特殊路径组）")
        group_summary = "\n".join(group_summary_lines)

        separator = "\n\n---\n\n" if ctx.prescan_summary else ""
        ctx.prescan_summary = ctx.prescan_summary + separator + group_summary

        ctx.emit_event("stage_result", stage="path_group",
                       normal_groups=len(normal_groups),
                       special_groups=len(special_groups))

        if cp:
            cp.mark_done("s0_pathgroup",
                         normal_groups=len(normal_groups),
                         special_groups=len(special_groups))

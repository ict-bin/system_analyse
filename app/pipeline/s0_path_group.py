"""
pipeline/s0_path_group.py — Stage 0.3: 路径先验分组（纯 Python，无 LLM）

入: ctx.filtered_files （filtered_files.txt 的相对路径列表）
出: workspace/prescan/path_groups.md
    追加到 ctx.prescan_summary

算法:
  1. 按文件名特征识别共享库/内核模块 → special 组
     匹配规则: lib*.so*, lib*.ko*, *.ko, *.ko.xz, *.ko.gz 等
     特殊组内再按 so/ko 库基名前缀二次分组 (libssl.so.1.1 → libssl)
  2. 其余文件按「最近有意义目录名」分组
     - 跳过通用容器目录: bin sbin usr lib lib64 local etc var
       opt run tmp proc sys dev home root mnt media srv
     - 取路径中最深（最末）一个「有意义」的目录节点
     - 若路径只有文件名（根目录文件）→ group "__root__"
  3. 输出 path_groups.md 到 workspace/prescan/，并追加到 prescan_summary

特殊库识别策略（无需人工配置）:
  - 文件名以 "lib" 开头且扩展名含 .so / .ko / .a
  - 例: libssl.so.1.1, libpthread-2.31.so, iptables.ko.xz
  - 非 lib 前缀的 .ko / .ko.xz / .ko.gz 也归入 special（内核模块）
  - 通过 _is_shared_lib() 判断，无目录配置依赖
"""
from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext

# 这些目录节点本身不携带有效功能语义，跳过
_SKIP_SEGMENTS = frozenset({
    "bin", "sbin", "usr", "lib", "lib32", "lib64", "lib64d",
    "libexec", "local", "etc", "var", "opt", "run", "tmp",
    "proc", "sys", "dev", "home", "root", "mnt", "media",
    "srv", "share", "include", "src", "source", "build",
    "install", "out", "output", "release", "debug", "target",
    "objs", "obj", "objects", ".", "..", "",
})

# 共享库/内核模块文件名模式
# 匹配: libssl.so, libssl.so.1.1, libfoo.ko, libbar.ko.xz, iptables.ko.gz
_SHARED_LIB_RE = re.compile(
    r"^lib.+\.(so|a)([\.\-_\d]|$)"      # lib*.so*, lib*.a (静态库)
    r"|^lib.+\.ko(\.xz|\.gz|\.zst)?$"   # lib*.ko*
    r"|^.+\.ko(\.xz|\.gz|\.zst)?$",     # *.ko* (内核模块，不限 lib 前缀)
    re.IGNORECASE,
)

# 提取共享库基名: libssl.so.1.1.1k → libssl
_LIB_BASE_RE = re.compile(r"^(lib[a-zA-Z0-9_+\-]+?)(?:\.so|\.ko|\.a|[-_]\d)", re.IGNORECASE)


def _is_shared_lib(rel_path: str) -> bool:
    """按文件名模式判断是否为共享库 / 内核模块，无需目录配置。"""
    basename = Path(rel_path.replace("\\", "/")).name
    return bool(_SHARED_LIB_RE.match(basename))


def _soname_prefix(rel_path: str) -> str | None:
    """从路径中提取 lib 基础名 (libssl.so.1.1 → libssl)，失败返回 None。"""
    basename = Path(rel_path.replace("\\", "/")).name
    m = _LIB_BASE_RE.match(basename)
    return m.group(1).lower() if m else None


def _infer_group(rel_path: str) -> str:
    """从相对路径推断组名（取最深有意义的目录节点）。"""
    parts = [p for p in rel_path.replace("\\", "/").split("/")[:-1] if p]
    for part in reversed(parts):
        clean = part.lower().lstrip("0123456789.-_")
        if clean and clean not in _SKIP_SEGMENTS and len(clean) >= 2:
            return part.lower()
    return "__root__"


def _build_path_groups(
    files: list[str],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """
    返回:
      normal_groups  {group_name: [rel_path, ...]}   — 路径可直接推断所属
      special_groups {lib_prefix_or_key: [rel_path, ...]} — 共享库/内核模块，需 LLM 判断
    """
    normal_groups: dict[str, list[str]] = defaultdict(list)
    special_groups: dict[str, list[str]] = defaultdict(list)

    for rel in files:
        if _is_shared_lib(rel):
            key = _soname_prefix(rel) or "__unmatched_shared__"
            special_groups[key].append(rel)
        else:
            key = _infer_group(rel)
            normal_groups[key].append(rel)

    return dict(normal_groups), dict(special_groups)


def _render_markdown(
    normal_groups: dict[str, list[str]],
    special_groups: dict[str, list[str]],
    max_sample: int = 8,
) -> str:
    lines: list[str] = [
        "## 路径先验分组（Path-Inferred Groups）",
        "",
        "> 本节由纯 Python 路径分析自动生成，无需 LLM 参与。",
        "> 「直接路径组」可直接采用为初始模块；「特殊路径组」（共享库/内核模块）需结合功能语义判断归属。",
        "> 共享库识别基于文件名模式（lib*.so* / *.ko*），无需目录配置。",
        "",
    ]

    # ── 直接路径组 ──
    if normal_groups:
        lines += ["### 直接路径组（建议直接采用为初始模块）", ""]
        for gname, flist in sorted(normal_groups.items(), key=lambda x: -len(x[1])):
            lines.append(f"**[{gname}]** — {len(flist)} 个文件")
            for f in flist[:max_sample]:
                lines.append(f"  - `{f}`")
            if len(flist) > max_sample:
                lines.append(f"  - ... 共 {len(flist)} 个文件（仅展示前 {max_sample} 个）")
            lines.append("")
    else:
        lines += ["### 直接路径组", "", "（无）", ""]

    # ── 特殊路径组（共享库/内核模块）──
    if special_groups:
        lines += ["### 特殊文件（共享库 / 内核模块，需 LLM 判断归属）", ""]
        for gname, flist in sorted(special_groups.items(), key=lambda x: -len(x[1])):
            lines.append(f"**[{gname}]** — {len(flist)} 个文件")
            for f in flist[:max_sample]:
                lines.append(f"  - `{f}`")
            if len(flist) > max_sample:
                lines.append(f"  - ... 共 {len(flist)} 个文件（仅展示前 {max_sample} 个）")
            lines.append("")
    else:
        lines += ["### 特殊文件（共享库 / 内核模块）", "", "（无）", ""]

    total_groups = len(normal_groups) + len(special_groups)
    total_files = sum(len(v) for v in normal_groups.values()) + sum(len(v) for v in special_groups.values())
    lines += [
        f"**汇总**: {len(normal_groups)} 个直接路径组 + {len(special_groups)} 个特殊文件组"
        f"，覆盖 {total_files} 个文件，合计 {total_groups} 组",
        "",
    ]

    return "\n".join(lines)


class PathGroupStage(BaseStage):
    """Stage 0.3: 路径先验分组（纯 Python，无 LLM 调用，始终执行）"""

    stage_num = 0
    stage_name = "路径先验分组"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint

        # ── checkpoint 跳过（PathGroup 是纯Python，运行很快，但续跑时 prescan_summary 需要重建） ──
        if cp and cp.is_done("s0_pathgroup"):
            # 从磁盘重建 prescan_summary
            p = ctx.workspace / "prescan" / "path_groups.md"
            if p.exists():
                pg_content = p.read_text("utf-8", errors="replace")
                separator = "\n\n---\n\n" if ctx.prescan_summary else ""
                # 只注入重要的摘要部分（避免重复运行分组逻辑）
                ctx.prescan_summary = ctx.prescan_summary + separator + pg_content[:2000]
            ctx.emit_event("log", level="info", msg="[S0-PathGroup] checkpoint已完成，跳过")
            return

        if not ctx.filtered_files:
            if cp:
                cp.mark_done("s0_pathgroup", skipped="no_filtered_files")
            return

        ctx.emit_event("stage", stage="path_group", file_count=len(ctx.filtered_files))

        normal_groups, special_groups = _build_path_groups(ctx.filtered_files)

        md = _render_markdown(normal_groups, special_groups)

        # 写入 prescan 目录
        prescan_dir = ctx.workspace / "prescan"
        prescan_dir.mkdir(parents=True, exist_ok=True)
        out_path = prescan_dir / "path_groups.md"
        out_path.write_text(md, encoding="utf-8")

        # 追加到 prescan_summary（ClassifyStage 会注入进 prompt）
        # ⚠️ 只追加统计摘要，不追加完整文件列表，避免 prompt 过大（38KB → token 爆炸）
        # path_groups.md 完整内容已写入磁盘，Worker 可通过 read 工具按需获取
        # 这里只注入「模块名 → 文件数」的极简摘要，而非完整路径列表
        group_summary_lines: list[str] = ["### 路径先验分组摘要（完整文件列表见 prescan/path_groups.md）\n"]
        for group_name, files in sorted(normal_groups.items(), key=lambda x: -len(x[1])):
            group_summary_lines.append(f"- [{group_name}] {len(files)} 个文件")
        for group_name, files in sorted(special_groups.items(), key=lambda x: -len(x[1])):
            group_summary_lines.append(f"- [{group_name}] {len(files)} 个文件（特殊路径组）")
        group_summary = "\n".join(group_summary_lines)
        separator = "\n\n---\n\n" if ctx.prescan_summary else ""
        ctx.prescan_summary = ctx.prescan_summary + separator + group_summary

        ctx.emit_event(
            "stage_result",
            stage="path_group",
            normal_groups=len(normal_groups),
            special_groups=len(special_groups),
            total_files=len(ctx.filtered_files),
        )

        # ── 写 checkpoint ────────────────────────────────────────────────────────
        if cp:
            cp.mark_done("s0_pathgroup",
                         normal_groups=len(normal_groups),
                         special_groups=len(special_groups))

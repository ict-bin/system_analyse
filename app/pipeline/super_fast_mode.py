"""
pipeline/super_fast_mode.py — 超快速模式

从原始 stage 代码完整拷贝 Worker 逻辑, 仅删除 Judge + 反思循环,
用 Python 格式校验替代 Judge 评审。
"""

from __future__ import annotations

import importlib
import logging
import re
import shutil
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

from .base import BaseStage, Pipeline
from .context import PipelineContext
from .helpers import (
    discover_modules, get_modules_root,
    module_has_nonempty_files, read_module_files, read_one_elf,
    run_agent_with_stage_guard, StageError,
    enforce_filter_constraint, generate_modules_list, strip_target_prefix,
    max_iter, max_rounds_exceeded_treated_as_passed,
)
from .s0_filter import FilterStage

# ── 动态加载原始 stage 模块, 避免重复定义 ──
_s1 = importlib.import_module(".s1_classify", package="app.pipeline")
_s2 = importlib.import_module(".s2_refine", package="app.pipeline")
_s4 = importlib.import_module(".s4_report", package="app.pipeline")
# load_prompt 来自 helpers
from .helpers import load_prompt as _hf_load_prompt, load_granularity_prompt as _hf_load_gran, build_granularity_hint as _hf_gran_hint

_log = logging.getLogger("sa.super_fast")

###############################################################################
# Python 校验 (替代 Judge)
###############################################################################

def _v_classify(workspace: Path) -> tuple[bool, list[str]]:
    """只校验格式: modules/ 下存在至少一个非空 files.list"""
    errors = []
    mr = get_modules_root(str(workspace))
    flists = list(mr.glob("*/files.list"))
    if not flists:
        return False, ["modules/ 下无 files.list"]
    has_content = False
    for fl in flists:
        if any(l.strip() for l in fl.read_text("utf-8").splitlines()):
            has_content = True; break
    if not has_content:
        return False, ["所有 files.list 为空"]
    return True, []

def _v_refine(mod_dir: Path) -> tuple[bool, list[str]]:
    """只校验格式: 有变动时 split/deleted 目录结构合法, 无 orphan 文件"""
    errors = []
    snap = mod_dir / ".snapshot"
    if not snap.exists() or snap.is_dir():
        return True, []  # 无快照 = 未细分, 通过
    snap_f = set(l.strip() for l in snap.read_text("utf-8").splitlines() if l.strip())
    if not snap_f: return True, []
    kept = set(read_module_files(mod_dir))
    deleted = set()
    df = mod_dir / "deleted" / "files.list"
    if df.exists():
        deleted = set(l.strip() for l in df.read_text("utf-8").splitlines() if l.strip())
    split_f = set()
    sd = mod_dir / "split"
    if sd.exists() and sd.is_dir():
        for c in sd.iterdir():
            if c.is_dir() and not c.name.startswith("_"):
                fl = c / "files.list"
                if fl.exists():
                    split_f |= set(l.strip() for l in fl.read_text("utf-8").splitlines() if l.strip())
    covered = kept | split_f | deleted
    extra = covered - snap_f  # 只检查多余 (格式错误), 不检查缺失 (质量)
    if extra:
        errors.append(f"多余 {len(extra)} 个文件不在快照中: {sorted(extra)[:5]}")
        return False, errors
    return True, []

def _v_analyse(mod_dir: Path) -> tuple[bool, list[str]]:
    errors = []
    rp = mod_dir / "module_report.md"
    if not rp.exists(): return False, ["module_report.md 不存在"]
    text = rp.read_text("utf-8", errors="replace")
    for tag in ["RISK_LEVEL:", "RISK_SCORE:", "## 1.", "## 5.", "<result>"]:
        if tag not in text: errors.append(f"缺少 {tag}")
    return len(errors) == 0, errors

def _v_report(rp: Path) -> tuple[bool, list[str]]:
    errors = []
    if not rp.exists(): return False, ["final_report.md 不存在"]
    text = rp.read_text("utf-8", errors="replace")
    for s in [f"## {i}." for i in range(1, 8)]:
        if s not in text: errors.append(f"缺少 {s}")
    return len(errors) == 0, errors

###############################################################################
# 共享: Worker + Py校验 循环 (替代 W+J 的 J 部分)
###############################################################################

def _run_w(ctx, stage, s_cfg, validate_fn, vargs,
           prompt_parts, w_sys, w_model, w_session, w_base):
    mi = max_iter(s_cfg)
    fb = ""
    for a in range(mi):
        ctx.emit_event("stage", stage=stage, attempt=a + 1)
        parts = list(prompt_parts)
        if fb: parts.append("\n\n# ⚠️ 上轮格式校验失败\n\n" + fb)
        ar = run_agent_with_stage_guard(
            ctx=ctx, stage=stage,
            context=f"sf-{stage}-a{a+1}",
            prompt="\n".join(parts),
            model=w_model, system_prompt=w_sys, **w_base,
        )
        ctx.tokens += ar.token_usage
        ok, errs = validate_fn(*vargs)
        ctx.emit_event("stage_result", stage=stage, attempt=a+1, passed=ok, errors=errs)
        if ok and a + 1 >= s_cfg.min_rounds: return
        if ok and a + 1 < s_cfg.min_rounds: return
        if errs:
            fb = f"## 格式校验失败 (第{a+1}轮)\n" + "\n".join(f"- {e}" for e in errs) + "\n\n请修正后重新输出。"
    if max_rounds_exceeded_treated_as_passed(ctx.cfg):
        ctx.emit_event("log", level="warn", msg=f"[SF-{stage}] 达最大轮数, 强制通过"); return
    raise StageError(f"SF {stage} 校验不通过 (max={mi})")

###############################################################################
# S1: 粗分类 — 拷贝 s1_classify.ClassifyStage.execute(), 删 Judge
###############################################################################

class SuperFastClassifyStage(BaseStage):
    stage_num, stage_name = 1, "快速粗分类"

    def execute(self, ctx):
        cfg, ws, s_cfg = ctx.cfg, ctx.workspace, ctx.cfg.stages.classify
        if not ctx.filtered_files:
            ctx.emit_event("log", level="warn", msg="[SF-S1] 无过滤文件"); return

        # ── 写入 classify_framework.sh (原始 S1 依赖) ──
        _s1._write_classify_framework(ws)

        # ── Worker setup (与原始完全一致) ──
        classify_prompt = _hf_load_prompt(cfg, "step1_classify", "workers")
        classify_model = cfg.workers.model_for("classify")
        classify_session = ctx.session_path("classify.jsonl")
        ctx.emit_event("stage", stage=1, mode="super_fast")

        w_base = dict(
            tools=cfg.workers.default_tools, cwd=str(ws), thinking_level="off",
            session_file=classify_session, cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries, retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries, pi_retry_delay=cfg.pi_retry_delay,
            task_pi_dir=cfg.role_pi_dir("workers"),
        )

        # ── 构建 prompt (与原始一致, 加 super_fast 提示) ──
        prompt_parts = [cfg.task]
        prompt_parts.append(
            f"\n\n# ⚠️ 工作目录\n\n{ws}\n"
            f"filtered_files.txt: {ws}/filtered_files.txt\n"
            f"prescan: {ws}/prescan/\n"
            f"输出: modules/<模块名>/files.list\n"
            f"classify_framework.sh 已就绪, 直接 bash classify_framework.sh\n"
            f"⚠️ super_fast_mode: details/ 和 classify_context.md 不可用, 不要 read。\n"
            f"⚠️ 本阶段只做文件分类，不分析威胁。威胁分析由后续 stage 负责。\n"
            f"写入 modules/<模块名>/files.list 后立即结束。"
        )
        if ctx.prescan_summary:
            prompt_parts.append("\n\n# 预扫描摘要\n\n" + ctx.prescan_summary)
        pg = ws / "prescan" / "path_groups.md"
        if pg.exists():
            prompt_parts.append("\n\n# 路径分组见 prescan/path_groups.md, 优先采用。")

        granularity = getattr(cfg, "module_granularity", "fine")
        if granularity == "coarse":
            prompt_parts.append(
                "\n\n# ⚠️ 粗粒度: 每个完整协议/服务 → 一个模块\n"
                "同协议 client+server+config → 必须合并"
            )

        _run_w(ctx, "classify", s_cfg, _v_classify, (ws,),
               prompt_parts, classify_prompt, classify_model, classify_session, w_base)

        if ctx.filtered_files:
            enforce_filter_constraint(ws, set(ctx.filtered_files))
        ctx.classified_modules = discover_modules(str(ws))
        ctx.emit_event("stage_result", stage=1, modules=ctx.classified_modules)

###############################################################################
# S2: 细分类 — 拷贝 s2_refine 的单模块 Worker, 删 Judge
###############################################################################

class SuperFastRefineStage(BaseStage):
    stage_num, stage_name = 2, "快速细分类"

    def execute(self, ctx):
        cfg, ws = ctx.cfg, ctx.workspace
        modules = discover_modules(str(ws))
        if not modules: ctx.refined_modules = []; return

        granularity = getattr(cfg, "module_granularity", "fine") or "fine"
        parallel = max(1, cfg.parallel_modules)
        ctx.emit_event("stage", stage=2, mode="super_fast", modules=modules)

        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futs = {pool.submit(self._one, ctx, m, granularity): m for m in modules}
            for fut in as_completed(futs):
                try: fut.result(timeout=1800)
                except Exception as e:
                    ctx.emit_event("log", level="error",
                                   msg=f"[SF-S2] {futs[fut]} 失败: {e}")

        ctx.refined_modules = discover_modules(str(ws))
        ctx.emit_event("stage_result", stage=2, modules=ctx.refined_modules)

    def _one(self, ctx, mod_name, granularity):
        cfg, ws = ctx.cfg, ctx.workspace
        mr = get_modules_root(str(ws)); mod_dir = mr / mod_name
        files = read_module_files(mod_dir)
        if not files: shutil.rmtree(str(mod_dir), ignore_errors=True); return

        s_cfg = cfg.stages.refine

        # Worker setup (与原始 s2_refine 一致)
        from .helpers import load_granularity_prompt, build_granularity_hint
        w_sys = load_granularity_prompt(cfg, "step2_refine", granularity, "workers")
        gh = build_granularity_hint(granularity)
        if gh and gh not in w_sys: w_sys += gh
        w_model = cfg.workers.model_for("refine")
        w_session = ctx.session_path("refine", f"{mod_name}.jsonl")

        parts = [
            f"检查 `{mod_name}` 是否需细分。\n"
            f"拆分 → modules/{mod_name}/split/<子模块>/files.list\n"
            f"合并 → modules/{mod_name}/split/_merge_to/<目标>/files.list\n"
            f"排除 → modules/{mod_name}/deleted/files.list",
        ]
        es = _elf_summ(files, cfg.target_dir)
        if es: parts.append("\n\n## ELF 符号\n\n" + es)
        ss = _src_summ(files, cfg.target_dir)
        if ss: parts.append("\n\n## 源码函数名\n\n" + ss)

        # 快照
        snap = mod_dir / ".snapshot"; fl = mod_dir / "files.list"
        if not snap.exists() or snap.is_dir():
            if fl.exists(): shutil.copy2(str(fl), str(snap))

        w_base = dict(
            tools=cfg.workers.default_tools, cwd=str(ws), thinking_level="off",
            session_file=w_session, cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries, retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries, pi_retry_delay=cfg.pi_retry_delay,
            task_pi_dir=cfg.role_pi_dir("workers"),
        )
        _run_w(ctx, "refine", s_cfg, _v_refine, (mod_dir,),
               parts, w_sys, w_model, w_session, w_base)

        if snap.exists() and snap.is_file(): snap.unlink(missing_ok=True)
        _commit_r(mod_dir, ws)

###############################################################################
# S3: 分析 — 拷贝 s3_analyse 的单模块 Worker, 删 Judge
###############################################################################

class SuperFastAnalyseStage(BaseStage):
    stage_num, stage_name = 3, "快速分析"

    def execute(self, ctx):
        cfg, ws = ctx.cfg, ctx.workspace
        modules = discover_modules(str(ws))
        if not modules: return
        granularity = getattr(cfg, "module_granularity", "fine") or "fine"
        parallel = max(1, cfg.parallel_modules)
        ctx.emit_event("stage", stage=3, mode="super_fast", modules=modules)
        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futs = {pool.submit(self._one, ctx, m, granularity): m for m in modules}
            for fut in as_completed(futs):
                try: fut.result(timeout=1800)
                except Exception as e:
                    ctx.emit_event("log", level="error", msg=f"[SF-S3] {futs[fut]} 失败: {e}")
        mr = get_modules_root(str(ws))
        ctx.analysed_modules = [d.name for d in mr.iterdir()
                                if d.is_dir() and (d/"module_report.md").exists()
                                and module_has_nonempty_files(d)]
        ctx.emit_event("stage_result", stage=3, modules=ctx.analysed_modules)

    def _one(self, ctx, mod_name, granularity):
        cfg, ws = ctx.cfg, ctx.workspace
        mr = get_modules_root(str(ws)); mod_dir = mr / mod_name
        files = read_module_files(mod_dir)
        if not files: return
        s_cfg = cfg.stages.analyse
        from .helpers import load_granularity_prompt, build_granularity_hint
        w_sys = load_granularity_prompt(cfg, "step3_analyse", granularity, "workers")
        gh = build_granularity_hint(granularity)
        if gh and gh not in w_sys: w_sys += gh
        w_model = cfg.workers.model_for("analyse")
        w_session = ctx.session_path("analyse", f"{mod_name}.jsonl")
        es = _elf_summ(files, cfg.target_dir)
        w_sys = w_sys.replace("{{PRE_READ_CONTENT}}",
                               "## 文件符号\n\n" + es if es else "（无 ELF 文件）")
        w_base = dict(
            tools=["write"], cwd=str(ws), thinking_level="off",
            session_file=w_session, cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries, retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries, pi_retry_delay=cfg.pi_retry_delay,
            task_pi_dir=cfg.role_pi_dir("workers"),
        )
        _run_w(ctx, "analyse", s_cfg, _v_analyse, (mod_dir,),
               [f"分析 `{mod_name}`, 写 modules/{mod_name}/module_report.md。"],
               w_sys, w_model, w_session, w_base)

###############################################################################
# S4: 报告 — 拷贝 s4_report 的单 Worker, 删 Judge/完整性检查
###############################################################################

class SuperFastReportStage(BaseStage):
    stage_num, stage_name = 4, "快速报告"

    def execute(self, ctx):
        cfg, ws = ctx.cfg, ctx.workspace
        s_cfg = cfg.stages.final_check
        ctx.emit_event("stage", stage=4, mode="super_fast")
        w_sys = _hf_load_prompt(cfg, "step4_final_report", "workers")
        w_model = cfg.workers.model_for("report")
        w_session = ctx.session_path("report.jsonl")
        w_base = dict(
            tools=["read", "bash", "write"], cwd=str(ws), thinking_level="off",
            session_file=w_session, cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries, retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries, pi_retry_delay=cfg.pi_retry_delay,
            task_pi_dir=cfg.role_pi_dir("workers"),
        )
        _run_w(ctx, "report", s_cfg, _v_report, (ws / "final_report.md",),
               ["生成总报告:\n1. ls -d modules/*/\n2. read modules/*/module_report.md\n3. 写 final_report.md"],
               w_sys, w_model, w_session, w_base)
        ctx.emit_event("stage_result", stage=4)

###############################################################################
# 辅助
###############################################################################

# refine 摘要 prompt 体积上限：防止超大模块（如 2021 文件的 other 兑底模块）
# 生成几百 KB 摘要擑爆 LLM 上下文窗口 → 反复 compaction → refine 退化卡死。
_SUMM_MAX_FILES = 300
_SUMM_MAX_CHARS = 40000


def _elf_summ(files, target_dir):
    lines = []
    total = 0
    processed = 0
    for rp in files:
        if processed >= _SUMM_MAX_FILES or total >= _SUMM_MAX_CHARS:
            break
        ext = Path(rp).suffix.lower()
        fp = str(Path(target_dir) / rp)
        if ext not in {".so", ".ko", ".o", ".a", ".elf", ".axf"}:
            try:
                with open(fp, "rb") as f:
                    if f.read(4) != b"\x7fELF": continue
            except OSError: continue
        try:
            elf = read_one_elf(fp)
            ex, im, nd = elf.get("exports", []), elf.get("imports", []), elf.get("needed", [])
            if ex or im or nd:
                block = [f"**{rp}**"]
                if ex: block.append(f"  exports({len(ex)}): {', '.join(str(s) for s in ex[:20])}")
                if im: block.append(f"  imports({len(im)}): {', '.join(str(s) for s in im[:20])}")
                if nd: block.append(f"  needed: {', '.join(str(s) for s in nd)}")
                block.append("")
                lines.extend(block)
                total += sum(len(x) + 1 for x in block)
                processed += 1
        except Exception: pass
    return "\n".join(lines)

def _src_summ(files, target_dir):
    # 函数名提取：C/C++ 用 tree-sitter，sh/py 用安全线性正则（按语言分派）。
    # 并对超大模块限制摘要体积（文件数 + 总字符双上限），防止 prompt 擑爆上下文。
    from .func_extract import extract_function_names, _CPP_EXTS, _SH_EXTS, _PY_EXTS
    supported = _CPP_EXTS | _SH_EXTS | _PY_EXTS
    src_files = [rp for rp in files if Path(rp).suffix.lower() in supported]
    lines = []
    total = 0
    shown = 0
    truncated = 0
    for idx, rp in enumerate(src_files):
        if shown >= _SUMM_MAX_FILES or total >= _SUMM_MAX_CHARS:
            truncated = len(src_files) - idx
            break
        try:
            with open(str(Path(target_dir)/rp), "r", encoding="utf-8", errors="replace") as f:
                content = f.read(64*1024)
        except (OSError, UnicodeDecodeError): continue
        funcs = extract_function_names(rp, content, limit=200)
        if funcs:
            line = f"**{rp}**: {', '.join(funcs[:20])}"
            if len(funcs) > 20: line += f" ... (共{len(funcs)}个)"
            lines.append(line); total += len(line) + 1; shown += 1
    if truncated > 0:
        lines.append(f"\n... (模块文件较多，已截断函数名摘要以控制上下文；另有约 {truncated} 个源码文件未列出）")
    return "\n".join(lines)

def _commit_r(mod_dir, ws):
    mr = get_modules_root(str(ws))
    sd = mod_dir / "split"
    if sd.exists() and sd.is_dir():
        for c in sorted(sd.iterdir()):
            if c.is_dir() and not c.name.startswith("_"):
                tgt = mr / c.name; tgt.mkdir(parents=True, exist_ok=True)
                sf = c / "files.list"
                if sf.exists():
                    tf = tgt / "files.list"
                    ex = set(l.strip() for l in tf.read_text("utf-8").splitlines() if l.strip()) if tf.exists() else set()
                    nf = set(l.strip() for l in sf.read_text("utf-8").splitlines() if l.strip())
                    tf.write_text("\n".join(sorted(ex|nf))+"\n", encoding="utf-8")
                    mf = mod_dir / "files.list"
                    if mf.exists():
                        rem = [l.strip() for l in mf.read_text("utf-8").splitlines() if l.strip() if l not in nf]
                        if rem: mf.write_text("\n".join(sorted(rem))+"\n", encoding="utf-8")
                        else: mf.unlink(missing_ok=True)
        shutil.rmtree(str(sd), ignore_errors=True)
    dd = mod_dir / "deleted"
    if dd.exists() and dd.is_dir():
        df = dd / "files.list"
        if df.exists():
            dfs = [l.strip() for l in df.read_text("utf-8").splitlines() if l.strip()]
            if dfs:
                with open(str(ws/"deleted.list"), "a", encoding="utf-8") as f:
                    for fp in dfs: f.write(fp+"\n")
        shutil.rmtree(str(dd), ignore_errors=True)
    if not (mod_dir / "files.list").exists():
        shutil.rmtree(str(mod_dir), ignore_errors=True)

def build_super_fast_pipeline():
    return [FilterStage(), SuperFastClassifyStage(), SuperFastRefineStage(),
            SuperFastAnalyseStage(), SuperFastReportStage()]


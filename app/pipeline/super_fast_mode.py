"""
pipeline/super_fast_mode.py — 超快速模式 (独立文件, 完全解耦)

零侵入设计:
  - 不修改任何原有文件
  - 可随时删除此文件, 系统完全不受影响
  - 所有代码自包含 (含独立编排器)

核心原则:
  1. 保留 Worker (W) LLM — 每个阶段仍调用 LLM Worker 完成任务
  2. 无 Judge (J) — 用 Python 脚本校验输出格式, 替代所有 LLM 评审
  3. 无 SubReader — 不生成 details/, Worker 只看文件名+符号+路径
  4. 输出格式与标准流水线完全一致

使用方式:
  from app.pipeline.super_fast_mode import SuperFastOrchestrator

  orch = SuperFastOrchestrator(
      target_dir="/data/target",
      output_dir="/data/output",
      config=cfg,        # ServiceConfig 实例 (复用, 不修改)
  )
  result = orch.run("task-id")

  或者通过 CLI 直接调用:
    python super_fast_mode.py /data/target /data/output task-001
"""

from __future__ import annotations

import json as _json
import logging
import os
import re
import shutil
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, TYPE_CHECKING

# ── 只引用原有代码中稳定的工具函数 (不会因删除此文件而受影响) ──
from .base import BaseStage, Pipeline
from .context import PipelineContext
from .helpers import (
    build_granularity_hint,
    discover_modules,
    enforce_filter_constraint,
    generate_modules_list,
    get_modules_root,
    max_rounds_exceeded_treated_as_passed,
    module_has_nonempty_files,
    read_module_files,
    read_one_elf,
    run_agent_with_stage_guard,
    StageError,
    strip_target_prefix,
)
from .s0_filter import FilterStage

if TYPE_CHECKING:
    from ..models import ServiceConfig, TokenUsage

_log = logging.getLogger("sa.super_fast")

# ═══════════════════════════════════════════════════════════════════════════════
# 精简版 Worker System Prompt (只做当前阶段的事, 比标准 prompt 短 10x)
# ═══════════════════════════════════════════════════════════════════════════════

_SF_SYS_CLASSIFY = """\
你是系统分析专家。按文件名和路径分组文件到模块, 不读文件内容, 不分析威胁。
用 write 工具写入 modules/<模块名>/files.list, 零遗漏。一次完成, 用 <result>done</result> 结束。"""

_SF_SYS_REFINE = """\
你是系统分析专家。根据 ELF 符号/函数名前缀拆分子模块。
操作: split/<子模块>/files.list, _merge_to/<目标>/files.list, deleted/files.list。
不读文件内容, 用 <result>done</result> 结束。"""

_SF_SYS_ANALYSE = """\
你是安全分析专家。生成 module_report.md:
<!-- RISK_LEVEL: 高/中/低/信息 -->\n<!-- RISK_SCORE: 0-100 -->\n## 1.模块风险等级\n## 2.文件清单\n## 3.模块功能概述\n## 4.分类合理性自检\n## 5.威胁分析(STRIDE)\n## 6.对外暴露面评估\n<result>摘要</result>
ELF 符号已预注入, 不读文件, 用 write 工具, 写完即止。"""

_SF_SYS_REPORT = """\
你是报告专家。读 modules/*/module_report.md 生成 final_report.md。
必须含 7 章节: ## 1.分析概况 ## 2.模块清单 ## 3.高风险威胁清单 ## 4.攻击面汇总 ## 5.STRIDE统计 ## 6.修复建议 ## 7.结论
用 <result>done</result> 结束。"""


# ═══════════════════════════════════════════════════════════════════════════════
# 自包含编排器 (不修改 orchestrator.py)
# ═══════════════════════════════════════════════════════════════════════════════

def _make_id() -> str:
    return f"task-{int(time.time())}-{uuid.uuid4().hex[:8]}"


class SuperFastOrchestrator:
    """独立编排器: 目录初始化 → 快速度流水线 → 输出组装 → 归档。

    与 Orchestrator 零耦合, 完全独立。
    """

    def __init__(
        self,
        target_dir: str,
        output_dir: str,
        config: "ServiceConfig",
        on_event: Callable | None = None,
    ):
        self.target_dir = target_dir
        self.output_dir = output_dir
        self.cfg = config
        self._on_event = on_event
        self._cancel = threading.Event()

    def _emit(self, event_type: str, **data) -> None:
        if self._on_event:
            try:
                from ..models import SwarmEvent
                self._on_event(SwarmEvent(type=event_type, task_id=data.get("task_id", ""), data=data))
            except Exception:
                pass

    def abort(self) -> None:
        self._cancel.set()

    def run(self, task_id: str | None = None) -> dict:
        """执行超快速分析, 返回结果字典 (兼容 TaskResult 字段)。"""
        cfg = self.cfg
        task_id = task_id or _make_id()
        start = time.time()
        self._cancel.clear()

        _log.info("[super_fast_mode] 启动超快速流水线 task_id=%s target=%s",
                  task_id, self.target_dir)

        # ── 目录初始化 (与 Orchestrator 一致) ─────────────────────────
        out_dir = Path(os.path.abspath(self.output_dir)) / task_id
        run_dir = out_dir / "run"
        run_dir.mkdir(parents=True, exist_ok=True)
        final_out = out_dir / "output"
        final_out.mkdir(parents=True, exist_ok=True)
        sess_dir = run_dir / "sessions"
        sess_dir.mkdir(exist_ok=True)
        workspace = run_dir / "workspace"
        workspace.mkdir(exist_ok=True)
        (workspace / "tmp").mkdir(exist_ok=True)
        target_link = workspace / "target"
        if not target_link.exists():
            try:
                target_link.symlink_to(os.path.abspath(self.target_dir))
            except OSError:
                pass

        flag_path = final_out / "flag"
        flag_path.write_text("0", encoding="utf-8")

        # pi settings.json
        try:
            pi_dir = workspace / ".pi"
            pi_dir.mkdir(exist_ok=True)
            (pi_dir / "settings.json").write_text(_json.dumps({
                "defaultThinkingLevel": "off",
                "compaction": {"enabled": True, "reserveTokens": 8192, "keepRecentTokens": 50000},
            }, indent=2), encoding="utf-8")
        except Exception:
            pass

        result = {
            "task_id": task_id, "status": "running", "error": None,
            "total_duration_ms": 0.0, "total_tokens": {"input": 0, "output": 0, "cost": 0.0},
        }

        # ── 构建 TaskConfig 兼容对象 ──────────────────────────────────
        task_cfg = _build_task_config_like(cfg, self.target_dir, self.output_dir)
        from ..models import TokenUsage
        tokens = TokenUsage()

        def _emit(event):
            if self._on_event:
                try:
                    from ..models import SwarmEvent
                    self._on_event(event)
                except Exception:
                    pass

        ctx = PipelineContext(
            task_id=task_id, task="super_fast_analysis",
            cfg=task_cfg, workspace=workspace, output_dir=run_dir,
            sess_dir=sess_dir, final_out_dir=final_out, flag_path=flag_path,
            emit=_emit, tokens=tokens, cancel_event=self._cancel,
        )

        # ── 组装并运行流水线 ──────────────────────────────────────────
        pipeline = Pipeline(build_super_fast_pipeline())
        status = "passed"
        error_msg = None

        try:
            pipeline.run(ctx)
            if ctx.filter_count == 0:
                _write_zero_report(final_out, task_id, cfg)
        except StageError as e:
            status = "failed"
            error_msg = str(e)
        except Exception as e:
            if getattr(e, '__class__', None) and e.__class__.__name__ == 'CancelledError':
                status = "cancelled"
                error_msg = "任务被取消"
            else:
                status = "error"
                error_msg = str(e)

        result["status"] = status
        result["error"] = error_msg
        result["total_duration_ms"] = (time.time() - start) * 1000

        # ── 输出组装 (与 Orchestrator 一致) ───────────────────────────
        self._assemble_output(ctx, final_out, status, error_msg)

        flag_path.write_text("1" if status == "passed" else "0", encoding="utf-8")

        # 归档
        (run_dir / "result.json").write_text(_json.dumps(result, indent=2), encoding="utf-8")
        archive_path = str(run_dir / "archive")
        shutil.make_archive(archive_path, "zip", str(run_dir.parent), run_dir.name)

        return result

    def _assemble_output(self, ctx, final_out, status, error_msg):
        """组装输出文件。"""
        workspace = ctx.workspace

        # final_report.md
        report_dst = final_out / "final_report.md"
        src_report = workspace / "final_report.md"
        if src_report.exists():
            shutil.copy2(str(src_report), str(report_dst))
        elif status != "passed":
            _write_failure_report(report_dst, ctx.task_id, status, error_msg or "")

        # modules/
        out_modules = final_out / "modules"
        modules_root = get_modules_root(str(workspace))
        if modules_root.exists():
            if out_modules.exists():
                shutil.rmtree(str(out_modules), ignore_errors=True)
            shutil.copytree(str(modules_root), str(out_modules),
                            dirs_exist_ok=True, symlinks=True)

        # modules.list
        if out_modules.exists():
            generate_modules_list(out_modules, final_out / "modules.list")

        # 路径清洗
        strips_target = getattr(ctx.cfg, "target_dir", self.target_dir)
        strip_target_prefix(final_out, strips_target)


# ═══════════════════════════════════════════════════════════════════════════════
# TaskConfig 兼容对象: 模拟 models.TaskConfig 但不依赖它被修改
# ═══════════════════════════════════════════════════════════════════════════════

def _build_task_config_like(svc: "ServiceConfig", target_dir: str, output_dir: str):
    """从 ServiceConfig 构建兼容 TaskConfig 的对象 (不修改 models.py)。"""
    from dataclasses import dataclass as _dc

    @_dc
    class _FakeStageLoop:
        min_rounds: int = 1
        max_rounds: int = -1
        pass_mode: str = "all"

    @_dc
    class _FakeStages:
        classify: _FakeStageLoop = field(default_factory=_FakeStageLoop)
        security_filter: _FakeStageLoop = field(default_factory=_FakeStageLoop)
        refine: _FakeStageLoop = field(default_factory=_FakeStageLoop)
        analyse: _FakeStageLoop = field(default_factory=_FakeStageLoop)
        final_check: _FakeStageLoop = field(default_factory=_FakeStageLoop)

        def __post_init__(self):
            stages_cfg = getattr(svc, "stages", None)
            if stages_cfg:
                for name in ("classify", "refine", "analyse", "final_check"):
                    sc = getattr(stages_cfg, name, None)
                    if sc:
                        setattr(self, name, _FakeStageLoop(
                            min_rounds=getattr(sc, "min_rounds", 1),
                            max_rounds=getattr(sc, "max_rounds", -1),
                            pass_mode=getattr(sc, "pass_mode", "all"),
                        ))

    @_dc
    class _FakeCfg:
        task: str = "super_fast_analysis"
        target_dir: str = target_dir
        output_dir: str = output_dir
        analyse_targets: list = field(default_factory=lambda: getattr(svc, "analyse_targets", ["all"]))
        binary_arch: list = field(default_factory=lambda: getattr(svc, "binary_arch", ["all"]))
        security_focus_categories: list = field(default_factory=lambda: getattr(svc, "security_focus_categories", ["all"]))
        module_granularity: str = "fine"
        continue_on_module_failure: bool = True
        parallel_modules: int = 2
        parallel_sub_workers: int = 1
        stages: _FakeStages = field(default_factory=_FakeStages)
        workers: object = None
        judges: object = None
        agent_max_retries: int = 3
        agent_retry_delay: float = 3.0
        pi_max_retries: int = 3
        pi_retry_delay: float = 5.0
        max_rounds_exceeded_action: str = "treat_as_passed"
        enable_final_check: bool = False
        prompt_overrides: object = None
        start_stage: int = 0
        resume_workspace: str = ""

        def __post_init__(self):
            self.module_granularity = getattr(svc, "module_granularity", "fine") or "fine"
            self.parallel_modules = getattr(svc, "parallel_modules", 2)
            self.parallel_sub_workers = getattr(svc, "parallel_sub_workers", 1)
            self.agent_max_retries = getattr(svc, "agent_max_retries", 3)
            self.agent_retry_delay = getattr(svc, "agent_retry_delay", 3.0)
            self.pi_max_retries = getattr(svc, "pi_max_retries", 3)
            self.pi_retry_delay = getattr(svc, "pi_retry_delay", 5.0)
            self.continue_on_module_failure = getattr(svc, "continue_on_module_failure", True)
            self.max_rounds_exceeded_action = getattr(svc, "max_rounds_exceeded_action", "treat_as_passed")
            self.workers = getattr(svc, "workers", None)
            self.judges = getattr(svc, "judges", None)
            self.prompt_overrides = getattr(svc, "prompt_overrides", None)

        def role_pi_dir(self, role: str) -> str:
            return ""

    return _FakeCfg()


# ═══════════════════════════════════════════════════════════════════════════════
# Python 格式校验 (替代 Judge LLM)
# ═══════════════════════════════════════════════════════════════════════════════

def _validate_classify_output(workspace: Path) -> tuple[bool, list[str]]:
    errors: list[str] = []
    filtered_txt = workspace / "filtered_files.txt"
    if not filtered_txt.exists():
        return False, ["filtered_files.txt 不存在"]
    all_files = set(l.strip() for l in filtered_txt.read_text("utf-8").splitlines() if l.strip())
    modules_root = get_modules_root(str(workspace))
    classified: set[str] = set()
    for flist in modules_root.glob("*/files.list"):
        classified |= set(l.strip() for l in flist.read_text("utf-8").splitlines() if l.strip())
    deleted_txt = workspace / "deleted.list"
    if deleted_txt.exists():
        classified |= set(l.strip() for l in deleted_txt.read_text("utf-8").splitlines() if l.strip())
    missing = sorted(all_files - classified)
    if missing:
        errors.append(f"缺失 {len(missing)} 个文件未分类: {missing[:10]}")
        return False, errors
    return True, []


def _validate_refine_output(mod_dir: Path) -> tuple[bool, list[str]]:
    errors: list[str] = []
    snapshot = mod_dir / ".snapshot"
    if not snapshot.exists() or snapshot.is_dir():
        return True, []
    snap_files = set(l.strip() for l in snapshot.read_text("utf-8").splitlines() if l.strip())
    if not snap_files:
        return True, []
    kept = set(read_module_files(mod_dir))
    deleted = set()
    del_fl = mod_dir / "deleted" / "files.list"
    if del_fl.exists():
        deleted = set(l.strip() for l in del_fl.read_text("utf-8").splitlines() if l.strip())
    split_files = set()
    split_dir = mod_dir / "split"
    if split_dir.exists() and split_dir.is_dir():
        for child in split_dir.iterdir():
            if child.is_dir() and not child.name.startswith("_"):
                fl = child / "files.list"
                if fl.exists():
                    split_files |= set(l.strip() for l in fl.read_text("utf-8").splitlines() if l.strip())
    missing = snap_files - (kept | split_files | deleted)
    extra = (kept | split_files | deleted) - snap_files
    if missing:
        errors.append(f"缺失 {len(missing)} 文件: {sorted(missing)[:5]}")
    if extra:
        errors.append(f"多余 {len(extra)} 文件: {sorted(extra)[:5]}")
    return len(missing) == 0 and len(extra) == 0, errors


def _validate_analyse_output(mod_dir: Path) -> tuple[bool, list[str]]:
    errors: list[str] = []
    report = mod_dir / "module_report.md"
    if not report.exists():
        return False, ["module_report.md 不存在"]
    text = report.read_text("utf-8", errors="replace")
    for tag in ["RISK_LEVEL:", "RISK_SCORE:", "## 1.", "## 5.", "<result>"]:
        if tag not in text:
            errors.append(f"缺少 {tag}")
    return len(errors) == 0, errors


def _validate_report_output(report_path: Path) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not report_path.exists():
        return False, ["final_report.md 不存在"]
    text = report_path.read_text("utf-8", errors="replace")
    for sec in ["## 1.", "## 2.", "## 3.", "## 4.", "## 5.", "## 6.", "## 7."]:
        if sec not in text:
            errors.append(f"缺少章节 {sec}")
    return len(errors) == 0, errors


# ═══════════════════════════════════════════════════════════════════════════════
# Worker + Python 校验循环 (替代 W+J 中的 J)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_worker_with_py_validate(
    ctx: PipelineContext,
    stage: str,
    worker_prompt: str,
    worker_model: str,
    worker_sys: str,
    worker_session: str,
    validate_fn,
    validate_args: tuple,
    min_rounds: int,
    max_rounds: int,
    w_base: dict,
) -> None:
    """运行 Worker LLM + Python 格式校验 (替代 Judge)。

    最多重试 max_rounds 次 (默认 3)。
    """
    max_r = max_rounds if max_rounds > 0 else 3
    feedback = ""

    for attempt in range(max_r):
        ctx.emit_event("stage", stage=stage, attempt=attempt + 1)

        parts = [worker_prompt]
        if feedback:
            parts.append("\n\n# ⚠️ 上轮格式校验失败\n\n" + feedback)

        ar = run_agent_with_stage_guard(
            ctx=ctx, stage=stage,
            context=f"sf-{stage}-a{attempt + 1}",
            prompt="\n".join(parts),
            model=worker_model,
            system_prompt=worker_sys,
            **w_base,
        )
        ctx.tokens += ar.token_usage

        passed, errors = validate_fn(*validate_args)
        ctx.emit_event("stage_result", stage=stage, attempt=attempt + 1,
                       passed=passed, errors=errors)

        if passed and attempt + 1 >= min_rounds:
            return
        if passed and attempt + 1 < min_rounds:
            return  # min_rounds > 1 时允许提前通过
        if errors:
            feedback = (f"## 格式校验失败 (第{attempt + 1}轮)\n" +
                        "\n".join(f"- {e}" for e in errors) +
                        "\n\n请修正后重新输出。")

    if max_rounds_exceeded_treated_as_passed(ctx.cfg):
        ctx.emit_event("log", level="warn",
                       msg=f"[SF-{stage}] 达最大轮数，按配置强制通过")
        return
    raise StageError(f"SuperFast {stage} 格式校验未通过 (max_rounds={max_r})")


# ═══════════════════════════════════════════════════════════════════════════════
# S1: 粗分类 (Worker LLM, 无 Judge)
# ═══════════════════════════════════════════════════════════════════════════════

class SuperFastClassifyStage(BaseStage):
    stage_num = 1
    stage_name = "快速粗分类"

    def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg
        ws = ctx.workspace
        if not ctx.filtered_files:
            ctx.emit_event("log", level="warn", msg="[SF-S1] 无过滤文件"); return

        sc = getattr(cfg.stages, "classify", None)
        min_r = getattr(sc, "min_rounds", 1) if sc else 1
        max_r = getattr(sc, "max_rounds", -1) if sc else -1

        ctx.emit_event("stage", stage=1, mode="super_fast")

        w_sys = _SF_SYS_CLASSIFY
        w_model = cfg.workers.model_for("classify")
        sess = ctx.session_path("classify.jsonl")

        parts = [cfg.task,
                 f"\n\n工作目录: `{ws}`\nfiltered_files.txt → modules/<模块>/files.list"]
        if ctx.prescan_summary:
            parts.append("\n\n# 预扫描摘要\n\n" + ctx.prescan_summary)
        pg = ws / "prescan" / "path_groups.md"
        if pg.exists():
            parts.append("\n\n# 路径分组见 prescan/path_groups.md，优先采用。")

        w_base = dict(
            tools=cfg.workers.default_tools, cwd=str(ws),
            thinking_level="off", session_file=sess,
            cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries, retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries, pi_retry_delay=cfg.pi_retry_delay,
            task_pi_dir=cfg.role_pi_dir("workers"),
        )

        _run_worker_with_py_validate(
            ctx, "classify", "\n".join(parts), w_model, w_sys, sess,
            _validate_classify_output, (ws,), min_r, max_r, w_base,
        )

        if ctx.filtered_files:
            enforce_filter_constraint(ws, set(ctx.filtered_files))
        ctx.classified_modules = discover_modules(str(ws))
        ctx.emit_event("stage_result", stage=1, modules=len(ctx.classified_modules))


# ═══════════════════════════════════════════════════════════════════════════════
# S2: 细分类 (Worker LLM, 无 Judge, 不看文件内容)
# ═══════════════════════════════════════════════════════════════════════════════

class SuperFastRefineStage(BaseStage):
    stage_num = 2
    stage_name = "快速细分类"

    def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg; ws = ctx.workspace
        modules = discover_modules(str(ws))
        if not modules:
            ctx.refined_modules = []; return

        sc = getattr(cfg.stages, "refine", None)
        min_r = getattr(sc, "min_rounds", 1) if sc else 1
        max_r = getattr(sc, "max_rounds", -1) if sc else -1

        parallel = max(1, cfg.parallel_modules)
        ctx.emit_event("stage", stage=2, mode="super_fast", modules=len(modules))

        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futs = {pool.submit(self._refine_one, ctx, m): m for m in modules}
            for fut in as_completed(futs):
                try: fut.result(timeout=1800)
                except Exception as e:
                    ctx.emit_event("log", level="error",
                                   msg=f"[SF-S2] {futs[fut]} 失败: {e}")

        ctx.refined_modules = discover_modules(str(ws))
        ctx.emit_event("stage_result", stage=2, modules=len(ctx.refined_modules))

    def _refine_one(self, ctx, mod_name):
        cfg = ctx.cfg; ws = ctx.workspace
        mr = get_modules_root(str(ws))
        mod_dir = mr / mod_name
        files = read_module_files(mod_dir)
        if not files:
            shutil.rmtree(str(mod_dir), ignore_errors=True); return

        sc = getattr(cfg.stages, "refine", None)
        min_r = getattr(sc, "min_rounds", 1) if sc else 1
        max_r = getattr(sc, "max_rounds", -1) if sc else -1
        gran = getattr(cfg, "module_granularity", "fine") or "fine"

        w_sys = _SF_SYS_REFINE
        gh = build_granularity_hint(gran)
        if gh and gh not in w_sys: w_sys += gh
        w_model = cfg.workers.model_for("refine")
        sess = ctx.session_path("refine", f"{mod_name}.jsonl")

        # 构建 prompt: 文件名 + ELF符号 + 源码函数名 (不看文件详细内容)
        parts = [
            f"检查 `{mod_name}` 是否需细分。",
            f"拆分 → `modules/{mod_name}/split/<child>/files.list`",
            f"合并 → `modules/{mod_name}/split/_merge_to/<target>/files.list`",
            f"排除 → `modules/{mod_name}/deleted/files.list`",
        ]
        es = _elf_summary(files, cfg.target_dir)
        if es: parts.append("\n\n## ELF 符号\n\n" + es)
        ss = _src_func_summary(files, cfg.target_dir)
        if ss: parts.append("\n\n## 源码函数名\n\n" + ss)

        # 快照
        snap = mod_dir / ".snapshot"
        if not snap.exists() or snap.is_dir():
            fl = mod_dir / "files.list"
            if fl.exists(): shutil.copy2(str(fl), str(snap))

        w_base = dict(
            tools=cfg.workers.default_tools, cwd=str(ws),
            thinking_level="off", session_file=sess,
            cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries, retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries, pi_retry_delay=cfg.pi_retry_delay,
            task_pi_dir=cfg.role_pi_dir("workers"),
        )

        _run_worker_with_py_validate(
            ctx, "refine", "\n".join(parts), w_model, w_sys, sess,
            _validate_refine_output, (mod_dir,), min_r, max_r, w_base,
        )

        if snap.exists() and snap.is_file():
            snap.unlink(missing_ok=True)
        _commit_refine(mod_dir, ws)


# ═══════════════════════════════════════════════════════════════════════════════
# S3: STRIDE 分析 (Worker LLM, 无 Judge)
# ═══════════════════════════════════════════════════════════════════════════════

class SuperFastAnalyseStage(BaseStage):
    stage_num = 3
    stage_name = "快速分析"

    def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg; ws = ctx.workspace
        modules = discover_modules(str(ws))
        if not modules: return

        sc = getattr(cfg.stages, "analyse", None)
        min_r = getattr(sc, "min_rounds", 1) if sc else 1
        max_r = getattr(sc, "max_rounds", -1) if sc else -1

        parallel = max(1, cfg.parallel_modules)
        ctx.emit_event("stage", stage=3, mode="super_fast", modules=len(modules))

        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futs = {pool.submit(self._analyse_one, ctx, m): m for m in modules}
            for fut in as_completed(futs):
                try: fut.result(timeout=1800)
                except Exception as e:
                    ctx.emit_event("log", level="error",
                                   msg=f"[SF-S3] {futs[fut]} 失败: {e}")

        mr = get_modules_root(str(ws))
        ctx.analysed_modules = [
            d.name for d in mr.iterdir()
            if d.is_dir() and (d / "module_report.md").exists()
            and module_has_nonempty_files(d)
        ]
        ctx.emit_event("stage_result", stage=3, modules=len(ctx.analysed_modules))

    def _analyse_one(self, ctx, mod_name):
        cfg = ctx.cfg; ws = ctx.workspace
        mr = get_modules_root(str(ws))
        mod_dir = mr / mod_name
        files = read_module_files(mod_dir)
        if not files: return

        sc = getattr(cfg.stages, "analyse", None)
        min_r = getattr(sc, "min_rounds", 1) if sc else 1
        max_r = getattr(sc, "max_rounds", -1) if sc else -1
        gran = getattr(cfg, "module_granularity", "fine") or "fine"

        w_sys = _SF_SYS_ANALYSE
        gh = build_granularity_hint(gran)
        if gh and gh not in w_sys: w_sys += gh
        w_model = cfg.workers.model_for("analyse")
        sess = ctx.session_path("analyse", f"{mod_name}.jsonl")

        # ELF 符号预注入
        es = _elf_summary(files, cfg.target_dir)
        w_sys = w_sys.replace("{{PRE_READ_CONTENT}}",
                               "## 文件符号\n\n" + es if es else "（无 ELF 文件）")

        w_base = dict(
            tools=["write"], cwd=str(ws),
            thinking_level="off", session_file=sess,
            cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries, retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries, pi_retry_delay=cfg.pi_retry_delay,
            task_pi_dir=cfg.role_pi_dir("workers"),
        )

        _run_worker_with_py_validate(
            ctx, "analyse",
            f"分析 `{mod_name}` 安全威胁, 写入 `modules/{mod_name}/module_report.md`。",
            w_model, w_sys, sess,
            _validate_analyse_output, (mod_dir,), min_r, max_r, w_base,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# S4: 报告 (Worker LLM, 无 Judge)
# ═══════════════════════════════════════════════════════════════════════════════

class SuperFastReportStage(BaseStage):
    stage_num = 4
    stage_name = "快速报告"

    def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg; ws = ctx.workspace

        sc = getattr(cfg.stages, "final_check", None)
        min_r = getattr(sc, "min_rounds", 1) if sc else 1
        max_r = getattr(sc, "max_rounds", -1) if sc else -1

        ctx.emit_event("stage", stage=4, mode="super_fast")

        w_sys = _SF_SYS_REPORT
        w_model = cfg.workers.model_for("report")
        sess = ctx.session_path("report.jsonl")

        w_base = dict(
            tools=["read", "bash", "write"], cwd=str(ws),
            thinking_level="off", session_file=sess,
            cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries, retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries, pi_retry_delay=cfg.pi_retry_delay,
            task_pi_dir=cfg.role_pi_dir("workers"),
        )

        _run_worker_with_py_validate(
            ctx, "report",
            "生成总报告:\n1. `ls -d modules/*/`\n2. `read modules/*/module_report.md`\n3. 写入 `final_report.md`",
            w_model, w_sys, sess,
            _validate_report_output, (ws / "final_report.md",),
            min_r, max_r, w_base,
        )

        ctx.emit_event("stage_result", stage=4)


# ═══════════════════════════════════════════════════════════════════════════════
# 辅助: ELF 符号 / 源码函数名 (不看文件内容)
# ═══════════════════════════════════════════════════════════════════════════════

def _elf_summary(files: list[str], target_dir: str) -> str:
    lines: list[str] = []
    for rp in files:
        ext = Path(rp).suffix.lower()
        fp = str(Path(target_dir) / rp)
        if ext in {".so", ".ko", ".o", ".a", ".elf", ".axf"}:
            pass
        else:
            try:
                with open(fp, "rb") as f:
                    if f.read(4) != b"\x7fELF": continue
            except OSError: continue
        try:
            elf = read_one_elf(fp)
            ex = elf.get("exports", []); im = elf.get("imports", []); nd = elf.get("needed", [])
            if ex or im or nd:
                lines.append(f"**{rp}**")
                if ex: lines.append(f"  exports({len(ex)}): {', '.join(str(s) for s in ex[:20])}")
                if im: lines.append(f"  imports({len(im)}): {', '.join(str(s) for s in im[:20])}")
                if nd: lines.append(f"  needed: {', '.join(str(s) for s in nd)}")
                lines.append("")
        except Exception: pass
    return "\n".join(lines)


def _src_func_summary(files: list[str], target_dir: str) -> str:
    src_exts = {".c", ".h", ".cpp", ".cc", ".cxx", ".hpp", ".hh", ".hxx",
                ".inc", ".inl", ".S", ".s", ".asm"}
    func_re = re.compile(
        r'^\s*(?:static\s+)?(?:inline\s+)?(?:virtual\s+)?(?:const\s+)?'
        r'(?:\w+(?:\s*::\s*)?)+(?:\s*\*)?\s+(\w{3,})\s*\(',
        re.MULTILINE,
    )
    lines: list[str] = []
    for rp in files:
        if Path(rp).suffix.lower() not in src_exts: continue
        try:
            with open(str(Path(target_dir) / rp), "r", encoding="utf-8", errors="replace") as f:
                content = f.read(64 * 1024)
        except (OSError, UnicodeDecodeError): continue
        funcs = [m.group(1) for m in func_re.finditer(content)
                 if m.group(1) not in ("if","for","while","switch","return","sizeof","else","case","break","continue")]
        if funcs:
            lines.append(f"**{rp}**: {', '.join(funcs[:20])}")
            if len(funcs) > 20: lines[-1] += f" ... (共{len(funcs)}个)"
    return "\n".join(lines)


def _commit_refine(mod_dir: Path, workspace: Path) -> None:
    """提交 S2 细分结果。"""
    mr = get_modules_root(str(workspace))
    sd = mod_dir / "split"
    if sd.exists() and sd.is_dir():
        for child in sorted(sd.iterdir()):
            if child.is_dir() and not child.name.startswith("_"):
                tgt = mr / child.name; tgt.mkdir(parents=True, exist_ok=True)
                sf = child / "files.list"
                if sf.exists():
                    tf = tgt / "files.list"
                    ex_set = set(l.strip() for l in tf.read_text("utf-8").splitlines() if l.strip()) if tf.exists() else set()
                    nf = set(l.strip() for l in sf.read_text("utf-8").splitlines() if l.strip())
                    tf.write_text("\n".join(sorted(ex_set | nf)) + "\n", encoding="utf-8")
                    mf = mod_dir / "files.list"
                    if mf.exists():
                        rem = [l.strip() for l in mf.read_text("utf-8").splitlines() if l.strip() and l not in nf]
                        if rem: mf.write_text("\n".join(sorted(rem)) + "\n", encoding="utf-8")
                        else: mf.unlink(missing_ok=True)
        shutil.rmtree(str(sd), ignore_errors=True)

    dd = mod_dir / "deleted"
    if dd.exists() and dd.is_dir():
        df = dd / "files.list"
        if df.exists():
            dfs = [l.strip() for l in df.read_text("utf-8").splitlines() if l.strip()]
            if dfs:
                with open(str(workspace / "deleted.list"), "a", encoding="utf-8") as f:
                    for fp in dfs: f.write(fp + "\n")
        shutil.rmtree(str(dd), ignore_errors=True)

    if not (mod_dir / "files.list").exists():
        shutil.rmtree(str(mod_dir), ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 输出辅助
# ═══════════════════════════════════════════════════════════════════════════════

def _write_zero_report(out_dir: Path, task_id: str, svc) -> None:
    (out_dir / "final_report.md").write_text(
        f"# 分析任务已完成（过滤结果为 0 个文件）\n\n"
        f"**任务 ID**：`{task_id}`\n\n"
        f"## 原因\n\n"
        f"Stage 0 文件过滤未找到符合条件的文件。\n\n"
        f"## 当前配置\n\n"
        f"- `binary_arch`：`{getattr(svc, 'binary_arch', ['all'])}`\n"
        f"- `analyse_targets`：`{getattr(svc, 'analyse_targets', ['all'])}`\n\n",
        encoding="utf-8",
    )


def _write_failure_report(path: Path, task_id: str, status: str, error: str) -> None:
    path.write_text(
        f"# 固件系统威胁分析总报告\n\n"
        f"> ⚠️ 任务状态：{status.upper()}\n\n"
        f"## 原因\n\n```\n{error}\n```\n\n"
        f"- 任务ID: {task_id}\n",
        encoding="utf-8",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 流水线组装
# ═══════════════════════════════════════════════════════════════════════════════

def build_super_fast_pipeline() -> list[BaseStage]:
    """构建超快速流水线: S0(Filter) → S1(Classify) → S2(Refine) → S3(Analyse) → S4(Report)"""
    return [
        FilterStage(),
        SuperFastClassifyStage(),
        SuperFastRefineStage(),
        SuperFastAnalyseStage(),
        SuperFastReportStage(),
    ]

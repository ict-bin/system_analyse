"""
orchestrator.py — 薄层流水线编排器 v3

职责：
  1. 目录初始化（out_dir / run_dir / workspace / sess_dir / task_tmp）
  2. 构建 PipelineContext
  3. 组装 Pipeline 并运行
  4. 错误处理：生成 failure report，写 flag=0/1
  5. 归档 run_dir → archive.zip
"""
from __future__ import annotations

import threading
import logging
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Callable

_log = logging.getLogger("sa.orchestrator")

from .config import get_service_yaml
from .service.llm_provider_sync import sync_providers_to_pi, validate_pi_models_file
from .models import (
    TaskConfig, TaskResult, TaskStatus, TokenUsage, SwarmEvent,
)
from .pipeline import (
    PipelineContext, Pipeline,
    FilterStage, ExploreStage, PrescanStage, PathGroupStage,
    TypeClassifyStage, UnknownCheckerStage, SubReaderStage, ValidateDetailsStage,
    ClassifyStage,
    SecurityFocusFilterStage,
    RefineStage,
    AnalyseStage,
    CompletenessCheckStage, FinalReportStage,
    StageError, PiFatalError,
)
# super_fast_mode 可选导入 (独立文件, 删除即失效)
try:
    from .pipeline.super_fast_mode import build_super_fast_pipeline
except ImportError:
    build_super_fast_pipeline = None
from .task_version import ensure_task_format_version
from .pipeline.helpers import (
    discover_modules, get_modules_root,
    write_failure_report, generate_modules_list, strip_target_prefix,
)
from .pipeline.evaluation import EvaluationRecorder


def make_id() -> str:
    return f"task-{int(time.time())}-{uuid.uuid4().hex[:8]}"


def _source_is_empty(target_dir: str) -> bool:
    """熔断判定：源码目录为空 / 不存在 / 仅含系统清单(task-metadata.json) → True。

    上游可能未交付源码（input 仅含 manifest，或目录缺失）。早退优化：遇到第一个
    可分析文件即返回 False，避免遍历大目录。出错时保守返回 False（不熔断，交由后续处理）。
    """
    try:
        p = Path(target_dir)
        if not p.exists():
            return True
        if p.is_file():
            return p.name == "task-metadata.json"
        for item in p.rglob("*"):
            try:
                if item.is_file() and item.name != "task-metadata.json":
                    return False
            except OSError:
                continue
        return True
    except OSError:
        return False


class Orchestrator:
    """
    薄层编排器：初始化目录，构建 PipelineContext，运行 Pipeline。
    """

    def __init__(
        self,
        config: TaskConfig,
        on_event: Callable[[SwarmEvent], None] | None = None,
        *,
        skip_provider_sync: bool = False,
    ):
        self.cfg = config
        self._on_event = on_event
        self._cancel_event: threading.Event | None = None
        # TaskRunner 已在 _prepare_task_execution 写好 models.json（手动=模型配置中心 sk /
        # 下发=网关+wsk 替换），此处跳过 configcenter 同步以免覆盖已替换的 wsk。
        self._skip_provider_sync = skip_provider_sync

    def _emit(self, event_type: str, task_id: str, **data) -> None:
        if self._on_event:
            try:
                self._on_event(SwarmEvent(type=event_type, task_id=task_id, data=data))
            except Exception:
                import traceback
                traceback.print_exc()
                pass

    def stop(self) -> None:
        if self._cancel_event:
            self._cancel_event.set()

    # ── 向后兼容旧接口 ──────────────────────────────────────────────────────
    def abort(self) -> None:
        self.stop()

    def _print_task_config(self, cfg: "TaskConfig", task_id: str) -> None:
        """任务启动前将完整运行配置逐行打印到日志，并发射 task_config_print 事件。"""
        # ── 构建配置行列表 ────────────────────────────────────────────────────
        w_agents = [
            {"model": a.model, "tools": a.tools, "thinking_level": a.thinking_level}
            for a in (cfg.workers.agents or [])
        ]
        j_agents = [
            {"model": a.model, "tools": a.tools, "thinking_level": a.thinking_level}
            for a in (cfg.judges.agents or [])
        ]

        def _stage_str(sc) -> str:
            return f"min_rounds={sc.min_rounds}, max_rounds={sc.max_rounds}, pass_mode={sc.pass_mode}"

        lines: list[str] = [
            f"task_id                    = {task_id}",
            f"target_dir                 = {cfg.target_dir}",
            f"output_dir                 = {cfg.output_dir}",
            f"analyse_targets            = {cfg.analyse_targets}",
            f"binary_arch                = {cfg.binary_arch}",
            f"security_focus_categories  = {cfg.security_focus_categories}",
            f"module_granularity         = {cfg.module_granularity}",
            f"continue_on_module_failure = {cfg.continue_on_module_failure}",
            f"parallel_modules           = {cfg.parallel_modules}",
            f"parallel_sub_workers       = {cfg.parallel_sub_workers}",
            f"stages.classify            = {_stage_str(cfg.stages.classify)}",
            f"stages.refine              = {_stage_str(cfg.stages.refine)}",
            f"stages.analyse             = {_stage_str(cfg.stages.analyse)}",
            f"stages.final_check         = {_stage_str(cfg.stages.final_check)}",
            f"workers.agents             = {w_agents}",
            f"workers.stage_models       = {dict(cfg.workers.stage_models)}",
            f"workers.default_tools      = {cfg.workers.default_tools}",
            f"workers.default_thinking   = {cfg.workers.default_thinking_level}",
            f"judges.agents              = {j_agents}",
            f"judges.stage_models        = {dict(cfg.judges.stage_models)}",
            f"agent_max_retries          = {cfg.agent_max_retries}",
            f"agent_retry_delay          = {cfg.agent_retry_delay}s",
            f"pi_max_retries             = {cfg.pi_max_retries}",
            f"pi_retry_delay             = {cfg.pi_retry_delay}s",
            f"super_fast_mode            = {cfg.super_fast_mode}",
        ]

        # ── 逐行写入日志 ─────────────────────────────────────────────────────
        _log.info("[任务配置] ══════════════════════════════ 任务运行配置 ══════════════════════════════")
        for line in lines:
            _log.info("[任务配置] %s", line)
        _log.info("[任务配置] ══════════════════════════════════════════════════════════════════════════")

        # ── 发射 task_config_print 事件（供 stages_json 记录） ────────────────
        self._emit("task_config_print", task_id, lines=lines)

    def execute(self, task_id: str | None = None) -> TaskResult:
        cfg = self.cfg
        task_id = task_id or make_id()
        start = time.time()
        self._cancel_event = threading.Event()

        # ── 同步配置中心的 LLM Provider → pi models.json ─────────────────────
        # TaskRunner 已准备 models.json 时跳过（避免覆盖已替换的 wsk / 手动 sk）
        svc = get_service_yaml()
        if not self._skip_provider_sync:
            sync_ok = sync_providers_to_pi(
                base_url=svc.configcenter.base_url,
                token=svc.auth_service.service_machine_token,
                timeout=svc.configcenter.timeout,
            )
            if not sync_ok:
                raise RuntimeError("LLM Provider 从配置中心同步失败，拒绝继续使用旧 models.json")
        validation = validate_pi_models_file()
        import logging as _log
        _logger = _log.getLogger("sa.orchestrator")
        _logger.info(
            "LLM Provider runtime models ready: path=%s providers=%s models=%s",
            validation["path"],
            validation["provider_count"],
            validation["model_count"],
        )
        for model_summary in validation["models"]:
            _logger.info(
                "runtime model source=configcenter provider=%s model=%s contextWindow=%s contextLength=%s",
                model_summary["provider_key"],
                model_summary["model_id"],
                model_summary["contextWindow"],
                model_summary["contextLength"],
            )

        # ── 目录初始化（统一逻辑，不再区分 resume/fresh 模式）────────────────
        # 断点续跑通过 workspace/.checkpoint/ 目录中的标记文件驱动，
        # 不再依赖 cfg.start_stage / cfg.resume_workspace。
        out_dir = Path(os.path.abspath(cfg.output_dir)) / task_id
        # 任务格式版本校验：不兼容版本自动清空重建
        ensure_task_format_version(out_dir)
        run_dir = out_dir / "run"
        run_dir.mkdir(exist_ok=True)
        final_out_dir = out_dir / "output"
        final_out_dir.mkdir(exist_ok=True)
        sess_dir = run_dir / "sessions"
        sess_dir.mkdir(exist_ok=True)
        workspace = run_dir / "workspace"
        workspace.mkdir(exist_ok=True)
        task_tmp = workspace / "tmp"
        task_tmp.mkdir(exist_ok=True)
        target_link = workspace / "target"
        if not target_link.exists():
            try:
                target_link.symlink_to(os.path.abspath(cfg.target_dir))
            except OSError:
                pass

        flag_path = final_out_dir / "flag"
        flag_path.write_text("0", encoding="utf-8")  # 失败默认值，成功时覆盖

        # ── 生成 workspace 级 pi settings.json（问题4：提升 compaction 保留窗口）─────────
        # 默认 keepRecentTokens=20k 太小，S2 多轮后关键错误信息被压缩。
        # 提升到 40k 确保 Worker 始终能看到最近 2-3 轮的完整错误输出。
        try:
            pi_settings_dir = workspace / ".pi"
            pi_settings_dir.mkdir(exist_ok=True)
            import json as _json
            pi_settings = {
                "defaultThinkingLevel": "off",
                "compaction": {
                    "enabled": True,
                    "reserveTokens": 8192,
                    "keepRecentTokens": 50000,
                },
            }
            (pi_settings_dir / "settings.json").write_text(
                _json.dumps(pi_settings, indent=2), encoding="utf-8"
            )
        except Exception as _pi_cfg_err:
            _log.warning("pi settings.json 写入失败（非致命）: %s", _pi_cfg_err)

        result = TaskResult(
            task_id=task_id,
            status=TaskStatus.RUNNING,
            task=cfg.task,
            config_snapshot=cfg.model_dump(),
        )

        self._emit("task_start", task_id, task=cfg.task)

        # 熔断：源码目录为空/不存在/仅含系统清单 → 直接 PASS（项目为空，无需分析）。
        # 上游可能未交付源码（input 仅含 manifest，或目录缺失），跳过整条流水线。
        if _source_is_empty(cfg.target_dir):
            self._emit("circuit_break_empty_source", task_id, target_dir=cfg.target_dir)
            _log.info("[熔断] 源码目录为空/不存在，项目为空无需分析，直接 PASS: %s", cfg.target_dir)
            _empty_report = (
                "# 分析任务已完成（项目为空，无需分析）\n\n"
                f"源码目录为空或不存在：`{cfg.target_dir}`\n\n"
                "目标中没有可供分析的源码/二进制文件（上游可能未交付源码，或仅包含"
                "元数据/清单文件）。已跳过全部分析阶段。\n\n"
                "## 结论\n\n项目为空，无需分析。\n"
            )
            try:
                (final_out_dir / "final_report.md").write_text(_empty_report, encoding="utf-8")
            except OSError:
                pass
            result.status = TaskStatus.PASSED
            result.total_tokens = TokenUsage()
            result.total_duration_ms = (time.time() - start) * 1000
            try:
                flag_path.write_text("1", encoding="utf-8")
            except OSError:
                pass
            try:
                (run_dir / "result.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")
                shutil.make_archive(str(run_dir / "archive"), "zip", str(run_dir.parent), run_dir.name)
            except Exception:
                _log.warning("[熔断] 归档失败（非致命）", exc_info=True)
            self._emit("task_end", task_id, status=result.status.value,
                       report=str(final_out_dir / "final_report.md"), modules="",
                       archive=str(run_dir / "archive") + ".zip")
            return result

        # ── 打印运行配置 ──────────────────────────────────────────────────────
        self._print_task_config(cfg, task_id)

        # ── 构建 PipelineContext ──────────────────────────────────────────────
        tokens = TokenUsage()
        evaluator = EvaluationRecorder(task_id=task_id, run_dir=run_dir)

        def _emit_event(event: SwarmEvent) -> None:
            if self._on_event:
                try:
                    self._on_event(event)
                except Exception:
                    import traceback
                    traceback.print_exc()
                    pass

        ctx = PipelineContext(
            task_id=task_id,
            task=cfg.task,
            cfg=cfg,
            workspace=workspace,
            output_dir=run_dir,       # 中间件存档目录（judge 评审文件等）
            sess_dir=sess_dir,
            final_out_dir=final_out_dir,
            flag_path=flag_path,
            emit=_emit_event,
            tokens=tokens,
            evaluator=evaluator,
            cancel_event=self._cancel_event,
            # ── 预处理阶段目录（在 orchestrator 层统一初始化，保证永不为 None）───────
            details_dir=workspace / "details",
            classify_context_path=workspace / "classify_context.md",
        )

        # ── 组装并运行 Pipeline ───────────────────────────────────────────────
        if cfg.super_fast_mode and build_super_fast_pipeline is not None:
            pipeline = Pipeline(build_super_fast_pipeline())
        else:
            pipeline = Pipeline([
                FilterStage(),
                TypeClassifyStage(),         # S0.1: 文件类型识别 → file_catalog.json
                UnknownCheckerStage(),       # S0.2: UNKNOWN 类型识别
                ExploreStage(),              # S0.3: 目录探索 → keywords.txt
                PrescanStage(),              # S0.4: 预扫描 → keyword_summary.txt
                PathGroupStage(),            # S0.5: 路径先验分组
                SubReaderStage(),            # S0.6: 全量文件预读 → details/*.json
                ValidateDetailsStage(),      # S0.7: 校验 details/ 完整性
                ClassifyStage(),
                SecurityFocusFilterStage(),  # S1.5: 安全维度过滤 + 无用模块过滤
                RefineStage(),
                AnalyseStage(),
                CompletenessCheckStage(),
                FinalReportStage(),
            ])

        try:
            pipeline.run(ctx)
            result.status = TaskStatus.PASSED
            result.total_tokens = ctx.tokens
            # S0 过滤结果为 0 文件：流水线已正常终止，写说明报告
            if ctx.filter_count == 0:
                _zero_report = (
                    f"# 分析任务已完成（过滤结果为 0 个文件）\n\n"
                    f"**任务 ID**：`{task_id}`\n\n"
                    f"## 原因\n\n"
                    f"Stage 0 文件过滤阶段未找到符合条件的文件，"
                    f"流水线已在过滤阶段正常终止，未执行后续分析。\n\n"
                    f"## 当前配置\n\n"
                    f"- `binary_arch`：`{cfg.binary_arch}`\n"
                    f"- `analyse_targets`：`{cfg.analyse_targets}`\n"
                    f"- `target_dir`：`{cfg.target_dir}`\n\n"
                    f"## 建议操作\n\n"
                    f"1. 确认目标固件的实际 ELF 架构（可使用 `readelf -h` 扫描）\n"
                    f"2. 在任务配置中将 `binary_arch` 调整为实际架构，"
                    f"例如 `[\"powerpc\"]`、`[\"mips\"]`、`[\"all\"]`\n"
                    f"3. 确认 `analyse_targets` 包含目标文件类型（如 `binary`、`source`）\n"
                    f"4. 重新创建任务\n"
                )
                (final_out_dir / "final_report.md").write_text(
                    _zero_report, encoding="utf-8")
                self._emit("task_zero_files", task_id,
                           binary_arch=cfg.binary_arch,
                           analyse_targets=cfg.analyse_targets)
        except (StageError, PiFatalError) as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            result.total_tokens = ctx.tokens
            self._emit("stage_fail", task_id, error=str(e))
        except Exception as e:
            if getattr(e, '__class__', None) and e.__class__.__name__ == 'CancelledError':
                result.status = TaskStatus.FAILED
                result.error = "任务被取消"
                result.total_tokens = ctx.tokens
                self._emit("stage_fail", task_id, error="cancelled")
            else:
                result.status = TaskStatus.ERROR
                result.error = str(e)
                result.total_tokens = ctx.tokens
                self._emit("error", task_id, error=str(e))

        result.total_duration_ms = (time.time() - start) * 1000

        # ── 写模块依赖图 ─────────────────────────────────────────────────
        try:
            if ctx.module_dependency_graph:
                import json as _json
                (final_out_dir / "module_dependency_graph.json").write_text(
                    _json.dumps(ctx.module_dependency_graph, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
        except Exception as _dep_err:
            _log.warning("module dependency graph write failed: %s", _dep_err)
        try:
            evaluator.write_summary(
                task_status=result.status.value,
                error=result.error,
            )
        except Exception as _eval_exc:
            import logging as _log
            _log.getLogger("sa.orchestrator").warning(
                "evaluation summary write failed: %s", _eval_exc
            )

        # ── 组装输出目录（失败时也执行，写 failure report）────────────────────
        final_mods = discover_modules(str(workspace))

        # 失败/错误时补写 final_report.md
        report_dst = final_out_dir / "final_report.md"
        if not report_dst.exists() and result.status in (TaskStatus.FAILED, TaskStatus.ERROR):
            write_failure_report(
                report_dst,
                task_id=task_id,
                status_value=result.status.value,
                error=result.error or "",
                duration_ms=result.total_duration_ms,
                modules=final_mods,
                modules_root=str(get_modules_root(str(workspace))),
            )

        # modules.list（如尚未生成）
        modules_out = final_out_dir / "modules"
        if modules_out.exists() and not (final_out_dir / "modules.list").exists():
            generate_modules_list(modules_out, final_out_dir / "modules.list")

        # 路径清洗（确保幂等）
        if modules_out.exists():
            strip_target_prefix(modules_out, cfg.target_dir)
        if report_dst.exists():
            strip_target_prefix(report_dst.parent, cfg.target_dir)

        # 归档 run_dir
        (run_dir / "result.json").write_text(
            result.model_dump_json(indent=2), encoding="utf-8")
        archive_path = str(run_dir / "archive")
        shutil.make_archive(archive_path, "zip", str(run_dir.parent), run_dir.name)

        # flag: 成功=1，失败/错误=0
        try:
            flag_path.write_text(
                "1" if result.status == TaskStatus.PASSED else "0",
                encoding="utf-8")
        except OSError:
            pass

        self._emit("task_end", task_id,
                   status=result.status.value,
                   report=str(report_dst),
                   modules=str(modules_out),
                   archive=f"{archive_path}.zip")

        result_payload = result.model_dump(mode="json")
        result_payload["program_error_modules"] = list(getattr(ctx, "program_error_modules", []) or [])
        result_payload["soft_failed_modules"] = list(getattr(ctx, "soft_failed_modules", []) or [])
        return result

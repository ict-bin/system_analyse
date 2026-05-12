"""
orchestrator.py — 薄层流水线编排器 v3

职责：
  1. 目录初始化（out_dir / run_dir / workspace / sess_dir / task_tmp）
  2. 构建 PipelineContext
  3. 组装 Pipeline 并运行（支持 resume start_stage）
  4. 错误处理：生成 failure report，写 flag=0/1
  5. 归档 run_dir → archive.zip
"""
from __future__ import annotations

import asyncio
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Callable

from .config import get_service_yaml
from .service.llm_provider_sync import sync_providers_to_pi
from .models import (
    TaskConfig, TaskResult, TaskStatus, TokenUsage, SwarmEvent,
)
from .pipeline import (
    PipelineContext, Pipeline,
    FilterStage, ExploreStage, PrescanStage, PathGroupStage,
    ClassifyStage,
    RefineStage,
    AnalyseStage,
    CompletenessCheckStage, FinalReportStage,
    StageError, PiFatalError,
)
from .pipeline.helpers import (
    discover_modules, get_modules_root,
    write_failure_report, generate_modules_list, strip_target_prefix,
)
from .pipeline.evaluation import EvaluationRecorder


def make_id() -> str:
    return f"task-{int(time.time())}-{uuid.uuid4().hex[:8]}"


class Orchestrator:
    """
    薄层编排器：初始化目录，构建 PipelineContext，运行 Pipeline。
    """

    def __init__(
        self,
        config: TaskConfig,
        on_event: Callable[[SwarmEvent], None] | None = None,
    ):
        self.cfg = config
        self._on_event = on_event
        self._cancel_event: asyncio.Event | None = None

    def _emit(self, event_type: str, task_id: str, **data) -> None:
        if self._on_event:
            try:
                self._on_event(SwarmEvent(type=event_type, task_id=task_id, data=data))
            except Exception:
                pass

    def stop(self) -> None:
        if self._cancel_event:
            self._cancel_event.set()

    # ── 向后兼容旧接口 ──────────────────────────────────────────────────────
    def abort(self) -> None:
        self.stop()

    async def execute(self, task_id: str | None = None) -> TaskResult:
        cfg = self.cfg
        task_id = task_id or make_id()
        start = time.time()
        self._cancel_event = asyncio.Event()

        # ── 同步配置中心的 LLM Provider → pi models.json ─────────────────────
        try:
            svc = get_service_yaml()
            await sync_providers_to_pi(
                base_url=svc.configcenter.base_url,
                token=svc.auth_service.service_machine_token,
                timeout=svc.configcenter.timeout,
            )
        except Exception as _sync_err:
            import logging as _log
            _log.getLogger("sa.orchestrator").warning(
                "LLM Provider 同步失败，使用已有 models.json: %s", _sync_err
            )

        # ── 目录初始化 ────────────────────────────────────────────────────────
        if cfg.resume_workspace and cfg.start_stage > 1:
            # Resume 模式：复用已有 workspace（跳过 S0-S2）
            workspace = Path(os.path.abspath(cfg.resume_workspace))
            run_dir = workspace.parent
            out_dir = run_dir.parent
            task_id = out_dir.name
            final_out_dir = out_dir / "output"
            final_out_dir.mkdir(exist_ok=True)
            sess_dir = run_dir / "sessions"
            sess_dir.mkdir(exist_ok=True)
            task_tmp = workspace / "tmp"
            task_tmp.mkdir(exist_ok=True)
        else:
            out_dir = Path(os.path.abspath(cfg.output_dir)) / task_id
            out_dir.mkdir(parents=True, exist_ok=True)
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

        result = TaskResult(
            task_id=task_id,
            status=TaskStatus.RUNNING,
            task=cfg.task,
            config_snapshot=cfg.model_dump(),
        )

        self._emit("task_start", task_id, task=cfg.task)

        # ── 构建 PipelineContext ──────────────────────────────────────────────
        tokens = TokenUsage()
        evaluator = EvaluationRecorder(task_id=task_id, run_dir=run_dir)

        def _emit_event(event: SwarmEvent) -> None:
            if self._on_event:
                try:
                    self._on_event(event)
                except Exception:
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
        )

        # ── 组装并运行 Pipeline ───────────────────────────────────────────────
        pipeline = Pipeline([
            FilterStage(),
            ExploreStage(),
            PrescanStage(),
            PathGroupStage(),
            ClassifyStage(),
            RefineStage(),
            AnalyseStage(),
            CompletenessCheckStage(),
            FinalReportStage(),
        ])

        try:
            await pipeline.run(ctx, start_stage=cfg.start_stage)
            result.status = TaskStatus.PASSED
            result.total_tokens = ctx.tokens
        except (StageError, PiFatalError) as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            result.total_tokens = ctx.tokens
            self._emit("stage_fail", task_id, error=str(e))
        except asyncio.CancelledError:
            result.status = TaskStatus.FAILED
            result.error = "任务被取消"
            result.total_tokens = ctx.tokens
            self._emit("stage_fail", task_id, error="cancelled")
        except Exception as e:
            result.status = TaskStatus.ERROR
            result.error = str(e)
            result.total_tokens = ctx.tokens
            self._emit("error", task_id, error=str(e))

        result.total_duration_ms = (time.time() - start) * 1000
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

        return result

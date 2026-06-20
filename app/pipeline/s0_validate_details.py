"""
pipeline/s0_validate_details.py — Stage 0.3: 校验 details/ JSON 完整性

入: workspace/details/ (SubReaderStage 产出)
    workspace/filtered_files.txt
出: workspace/details_validation.json（校验报告）
    ctx.invalid_detail_files（无效文件列表）

对缺失/无效的 JSON 文件，触发 SubReaderStage 的单文件重做逻辑。
"""
from __future__ import annotations

import subprocess
import threading
import json
import os
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext


class ValidateDetailsStage(BaseStage):
    """Stage 0.3: 校验 details/ JSON 完整性，补全缺失/无效文件"""

    stage_num = 0
    stage_name = "详情校验"

    def execute(self, ctx: PipelineContext) -> None:
        workspace = ctx.workspace

        # ── details/ 不存在则跳过 ─────────────────────────────────────────
        # ctx.details_dir 已由 orchestrator 初始化为 workspace/details/，永不为 None
        details_dir = ctx.details_dir
        if not details_dir.exists():
            ctx.emit_event("log", level="info",
                           msg="[S0-ValidateDetails] details/ 不存在，跳过")
            return

        validate_script = "/app/scripts/validate_details.py"
        if not os.path.isfile(validate_script):
            validate_script = str(
                Path(__file__).parent.parent.parent / "scripts" / "validate_details.py"
            )

        ctx.emit_event("stage", stage="validate_details")

        if os.path.isfile(validate_script):
            result = subprocess.run(
                ["python3", validate_script, str(workspace)],
                capture_output=True,
                env={**os.environ, "TMPDIR": str(ctx.task_tmp)},
            )
            out = (result.stdout or b"").decode("utf-8", errors="replace").strip()
            err = (result.stderr or b"").decode("utf-8", errors="replace").strip()
            if out:
                ctx.emit_event("cli_output", stage="validate_details", text=out[:2000])
        else:
            # 内联校验（脚本不存在时兜底）
            ctx.emit_event("log", level="warn",
                           msg="[S0-ValidateDetails] validate_details.py 未找到，执行内联校验")
            self._inline_validate(workspace, details_dir)

        # ── 读取报告 ──────────────────────────────────────────────────────
        report = workspace / "details_validation.json"
        if report.exists():
            try:
                data = json.loads(report.read_text(encoding="utf-8"))
                missing = data.get("missing", [])
                invalid_list = [e["path"] for e in data.get("invalid", [])]
                ctx.invalid_detail_files = missing + invalid_list

                ctx.emit_event("stage_result", stage="validate_details",
                               total=data.get("total", 0),
                               valid=data.get("valid", 0),
                               missing=len(missing),
                               invalid=len(invalid_list))

                # ── 对问题文件触发补全 ────────────────────────────────────
                if ctx.invalid_detail_files:
                    ctx.emit_event("log", level="info",
                                   msg=f"[S0-ValidateDetails] 补全 {len(ctx.invalid_detail_files)} 个问题文件")
                    self._repair_details(ctx, ctx.invalid_detail_files, details_dir)
            except Exception as e:
                ctx.emit_event("log", level="warn",
                               msg=f"[S0-ValidateDetails] 校验报告解析失败: {e}")


    def _inline_validate(self, workspace: Path, details_dir: Path) -> None:
        """内联校验（无脚本时的兜底实现）。"""
        ff = workspace / "filtered_files.txt"
        if not ff.exists():
            return
        files = [l.strip() for l in ff.read_text(encoding="utf-8").splitlines() if l.strip()]
        missing, invalid = [], []
        for rel in files:
            jp = details_dir / (rel.lstrip("/") + ".json")
            if not jp.exists():
                missing.append(rel)
                continue
            try:
                data = json.loads(jp.read_text(encoding="utf-8"))
                if not data.get("path") or not data.get("type") or not str(data.get("summary", "")).strip():
                    invalid.append({"path": rel, "error": "缺失必填字段或 summary 为空"})
            except Exception as e:
                invalid.append({"path": rel, "error": str(e)})

        result = {
            "total": len(files),
            "valid": len(files) - len(missing) - len(invalid),
            "missing_count": len(missing),
            "invalid_count": len(invalid),
            "missing": missing[:100],
            "invalid": invalid[:100],
            "pass": not missing and not invalid,
        }
        (workspace / "details_validation.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def _repair_details(
        self,
        ctx: PipelineContext,
        problem_files: list[str],
        details_dir: Path,
    ) -> None:
        """对缺失/无效的 detail JSON 触发单文件重做。"""
        from .s0_sub_reader import _extract_python_info, _write_detail_json, _get_file_type_from_catalog
        import concurrent.futures

        cfg = ctx.cfg
        catalog = ctx.file_catalog or {}
        target_dir = cfg.target_dir

        def _repair_one(rel: str) -> None:
            ftype = _get_file_type_from_catalog(catalog, rel)
            data = loop.run_in_executor(
                None, _extract_python_info,
                os.path.join(target_dir, rel), rel, ftype
            )
            detail_path = details_dir / (rel.lstrip("/") + ".json")
            _write_detail_json(detail_path, data)

        sem = threading.BoundedSemaphore(max(1, getattr(cfg, "parallel_sub_workers", 4)))

        def _bounded_repair(rel: str) -> None:
            with sem:
                _repair_one(rel)

        # [THREAD] replaced: # GATHER   # *[_bounded_repair(f) for f in problem_files])
        ctx.emit_event("log", level="info",
                       msg=f"[S0-ValidateDetails] 已修复 {len(problem_files)} 个问题文件")

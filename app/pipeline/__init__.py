"""
pipeline/__init__.py
"""
from .context import PipelineContext
from .base import BaseStage, Pipeline
from .helpers import (
    StageError, PiFatalError,
    run_agent_checked, check_agent_result,
    get_modules_root, discover_modules,
    parse_eval_md, check_voting,
    load_prompt,
    max_iter, extract_result, archive_file,
    SUB_BATCH_SIZE, SUB_WORKER_THRESHOLD,
    pre_read_file, read_one_elf, pre_read_module, collect_file_summaries,
    write_failure_report, generate_modules_list, strip_target_prefix,
)
from .s0_filter import FilterStage, ExploreStage, PrescanStage
from .s0_path_group import PathGroupStage
from .s1_classify import ClassifyStage
from .s1_security_filter import SecurityFocusFilterStage
from .s2_refine import RefineStage
from .s3_analyse import AnalyseStage
from .s4_report import CompletenessCheckStage, FinalReportStage
from .evaluation import EvaluationRecorder

__all__ = [
    # 上下文与基类
    "PipelineContext", "BaseStage", "Pipeline",
    "EvaluationRecorder",
    # 工具函数与常量
    "StageError", "PiFatalError",
    "run_agent_checked", "check_agent_result",
    "get_modules_root", "discover_modules",
    "parse_eval_md", "check_voting", "load_prompt",
    "max_iter", "extract_result", "archive_file",
    "SUB_BATCH_SIZE", "SUB_WORKER_THRESHOLD",
    "pre_read_file", "read_one_elf", "pre_read_module", "collect_file_summaries",
    "write_failure_report", "generate_modules_list", "strip_target_prefix",
    # 阶段
    "FilterStage", "ExploreStage", "PrescanStage",
    "PathGroupStage",
    "ClassifyStage",
    "SecurityFocusFilterStage",
    "RefineStage",
    "AnalyseStage",
    "CompletenessCheckStage", "FinalReportStage",
]

"""
pipeline/__init__.py
"""
from .checkpoint import CheckpointManager
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
    pre_read_file, read_one_elf, pre_read_module, pre_read_module_with_details,
    collect_file_summaries,
    load_detail_json, is_detail_sufficient, format_detail_as_summary_line,
    load_details_for_module,
    write_failure_report, generate_modules_list, strip_target_prefix,
    fix_orphan_dirs_before_judge, build_s2_diagnose_report,
)
from .filter_engine import normalize_filter_engine
from .s0_filter import FilterStage, ExploreStage, PrescanStage
from .s0_path_group import PathGroupStage
from .s0_type_classify import TypeClassifyStage
from .s0_unknown_checker import UnknownCheckerStage
from .s0_sub_reader import SubReaderStage
from .s0_validate_details import ValidateDetailsStage
from .s1_classify import ClassifyStage
from .s1_security_filter import SecurityFocusFilterStage
from .s2_refine import RefineStage
from .s3_analyse import AnalyseStage
from .s4_report import CompletenessCheckStage, FinalReportStage
from .evaluation import EvaluationRecorder

__all__ = [
    "CheckpointManager",
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
    "pre_read_file", "read_one_elf", "pre_read_module", "pre_read_module_with_details",
    "collect_file_summaries",
    "load_detail_json", "is_detail_sufficient", "format_detail_as_summary_line",
    "load_details_for_module",
    "write_failure_report", "generate_modules_list", "strip_target_prefix",
    "fix_orphan_dirs_before_judge", "build_s2_diagnose_report",
    "normalize_filter_engine",
    # 阶段
    "FilterStage", "ExploreStage", "PrescanStage",
    "PathGroupStage",
    "TypeClassifyStage", "UnknownCheckerStage", "SubReaderStage", "ValidateDetailsStage",
    "ClassifyStage",
    "SecurityFocusFilterStage",
    "RefineStage",
    "AnalyseStage",
    "CompletenessCheckStage", "FinalReportStage",
]

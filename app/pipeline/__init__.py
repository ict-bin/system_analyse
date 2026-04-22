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
)
from .s0_filter import FilterStage, ExploreStage, PrescanStage
from .s1_classify import ClassifyStage
from .s2_refine import RefineStage
from .s3_analyse import AnalyseStage
from .s4_report import CompletenessCheckStage, FinalReportStage

__all__ = [
    # 上下文
    "PipelineContext", "BaseStage", "Pipeline",
    # 工具
    "StageError", "PiFatalError",
    "run_agent_checked", "check_agent_result",
    "get_modules_root", "discover_modules",
    "parse_eval_md", "check_voting", "load_prompt",
    # 阶段
    "FilterStage", "ExploreStage", "PrescanStage",
    "ClassifyStage",
    "RefineStage",
    "AnalyseStage",
    "CompletenessCheckStage", "FinalReportStage",
]

"""
system_analyse — 数据模型
"""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ─── Agent 实例配置 ───────────────────────────────────────────────────────────

class AgentInstanceConfig(BaseModel):
    model: str = Field(..., description="该实例使用的 LLM 模型")
    tools: Optional[list[str]] = Field(default=None)
    system_prompt: Optional[str] = Field(default=None)
    thinking_level: Optional[str] = Field(default=None)


class RoleConfig(BaseModel):
    default_model: str = Field(default="")
    default_tools: list[str] = Field(default_factory=lambda: ["read", "bash", "edit", "write"])
    system_prompt_dir: str = Field(default="./prompts/workers")
    default_thinking_level: str = Field(default="off")
    agents: list[AgentInstanceConfig] = Field(default_factory=list)


# ─── 服务配置 ─────────────────────────────────────────────────────────────────

class ServiceConfig(BaseModel):
    max_rounds: int = Field(default=3, ge=1, le=10)
    min_rounds: int = Field(default=2, ge=1, le=10)
    pass_threshold: Optional[int] = Field(default=None)
    agent_max_retries: int = Field(default=100)
    agent_retry_delay: float = Field(default=30.0)

    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)

    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")

    context: str = Field(default="")
    criteria: str = Field(default="")


# ─── 运行时任务 ───────────────────────────────────────────────────────────────

class TaskConfig(BaseModel):
    task: str = Field(..., description="用户的一句话 prompt")
    target_dir: str = Field(default="/data/target", description="解包目录路径")
    source_file: str = Field(default="", description="兼容字段：用于归档命名")
    function_name: str = Field(default="", description="兼容字段：用于归档命名")
    cwd: str = Field(default="/data/target")

    max_rounds: int = Field(default=3)
    min_rounds: int = Field(default=2)
    pass_threshold: Optional[int] = Field(default=None)
    agent_max_retries: int = Field(default=100)
    agent_retry_delay: float = Field(default=30.0)
    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)
    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")
    context: str = Field(default="")
    criteria: str = Field(default="")

    @property
    def worker_count(self) -> int:
        return len(self.workers.agents)

    @property
    def judge_count(self) -> int:
        return len(self.judges.agents)


# ─── Token 统计 ───────────────────────────────────────────────────────────────

class TokenUsage(BaseModel):
    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    cost: float = 0.0

    def __iadd__(self, other: TokenUsage) -> TokenUsage:
        self.input += other.input
        self.output += other.output
        self.cache_read += other.cache_read
        self.cache_write += other.cache_write
        self.cost += other.cost
        return self


# ─── Worker 结果 ──────────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"


class WorkerResult(BaseModel):
    worker_id: str
    model: str = ""
    output: str = ""                        # Phase A 的 <result> 摘要
    output_dir: str = ""                    # Worker 输出目录（含模块子文件夹）
    modules: list[str] = Field(default_factory=list)  # 发现的模块名列表
    token_usage: TokenUsage = Field(default_factory=TokenUsage)
    error: Optional[str] = None


# ─── Judge 结果 ──────────────────────────────────────────────────────────────

class ModuleEvaluation(BaseModel):
    """Judge 对单个模块的评价"""
    module_name: str
    passed: bool = False
    score: int = 0
    feedback: str = ""


class WorkerEvaluation(BaseModel):
    """Judge 对单个 Worker 的完整评价"""
    worker_id: str
    classification_ok: bool = False         # Step 1: 文件分类完整性
    classification_feedback: str = ""
    module_evals: list[ModuleEvaluation] = Field(default_factory=list)  # Step 2
    overall_passed: bool = False            # Step 3: 综合评分
    overall_score: int = 0
    overall_feedback: str = ""


class JudgeSummary(BaseModel):
    best_worker_id: str = ""
    reasoning: str = ""
    overall_passed: bool = False


class JudgeRoundResult(BaseModel):
    judge_id: str
    model: str = ""
    evaluations: list[WorkerEvaluation] = Field(default_factory=list)
    summary: Optional[JudgeSummary] = None
    token_usage: TokenUsage = Field(default_factory=TokenUsage)


# ─── 轮次和任务结果 ──────────────────────────────────────────────────────────

class RoundResult(BaseModel):
    round: int
    worker_results: list[WorkerResult] = Field(default_factory=list)
    judge_results: list[JudgeRoundResult] = Field(default_factory=list)
    pass_count: int = 0
    total_judges: int = 0
    passed: bool = False
    best_worker_id: str = ""
    feedback_to_workers: str = ""


class TaskResult(BaseModel):
    task_id: str
    status: TaskStatus = TaskStatus.RUNNING
    task: str
    config_snapshot: Optional[dict] = None
    rounds: list[RoundResult] = Field(default_factory=list)
    final_output: str = ""
    total_duration_ms: float = 0
    total_tokens: TokenUsage = Field(default_factory=TokenUsage)
    error: Optional[str] = None


class SwarmEvent(BaseModel):
    type: str
    task_id: str
    data: dict = Field(default_factory=dict)


def make_id() -> str:
    return f"task-{int(time.time())}-{uuid.uuid4().hex[:8]}"

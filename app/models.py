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

# Worker/Judge 可配置的阶段名（用于 stage_models 键）
# Workers: explore / classify / refine / sub_read / analyse / report
# Judges:  classify / refine / analyse / completeness / report

WORKER_STAGES = ["explore", "classify", "refine", "sub_read", "analyse", "report"]
JUDGE_STAGES  = ["classify", "refine", "analyse", "completeness", "report"]


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
    stage_models: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "各阶段模型覆盖，优先级高于 agents[0]。"
            "Workers: explore/classify/refine/sub_read/analyse/report。"
            "Judges: classify/refine/analyse/completeness/report"
        )
    )

    def model_for(self, stage: str) -> str:
        """获取指定阶段的模型名。未配置时回退到 agents[0].model。"""
        if stage in self.stage_models:
            return self.stage_models[stage]
        return self.agents[0].model if self.agents else self.default_model


# ─── 服务配置 ─────────────────────────────────────────────────────────────────

class StageLoopConfig(BaseModel):
    """单个阶段的循环控制"""
    min_rounds: int = Field(default=2, description="最少运行轮数（强制反思）")
    max_rounds: int = Field(default=5, description="最多迭代轮数，-1=无限")
    pass_mode: str = Field(default="majority", description="majority=半数以上, all=全部judge通过")


class StagesConfig(BaseModel):
    classify: StageLoopConfig = Field(default_factory=lambda: StageLoopConfig(min_rounds=2, max_rounds=5))
    refine: StageLoopConfig = Field(default_factory=lambda: StageLoopConfig(min_rounds=2, max_rounds=3))
    analyse: StageLoopConfig = Field(default_factory=lambda: StageLoopConfig(min_rounds=2, max_rounds=5))
    final_check: StageLoopConfig = Field(default_factory=lambda: StageLoopConfig(min_rounds=1, max_rounds=1))


# ─── 分析目标文件类型 ───────────────────────────────────────────────────

# 支持的 ELF 架构及对应的 file 命令关键词
BINARY_ARCH = {
    "x86":     ["Intel 80386", "i386", "i486", "i586", "i686"],
    "x86_64":  ["x86-64", "AMD x86-64"],
    "arm":     ["ARM,", "EABI", "ARM EABI", "ARM, EABI"],
    "aarch64": ["ARM aarch64", "AArch64", "aarch64"],
    "mips":    ["MIPS"],
    "mips64":  ["MIPS64", "MIPS 64"],
    "ppc":     ["PowerPC", "Power PC"],
    "ppc64":   ["64-bit PowerPC", "PowerPC64"],
    "riscv":   ["RISC-V"],
    "s390":    ["IBM S/390", "S390"],
}

# 支持的分析类型（可组合）
ANALYSE_TYPES = {
    "binary": {
        "desc": "ELF 可执行文件、共享库、内核模块",
        "extensions": [".so", ".ko", ".o", ".a", ".elf", ".axf"],
        "magic": ["ELF"],
    },
    "script": {
        "desc": "Shell/Python/Lua 等脚本",
        "extensions": [".sh", ".bash", ".py", ".lua", ".pl", ".rb", ".tcl", ".awk", ".sed"],
        "magic": ["shell script", "Python script", "Lua script", "Perl script"],
    },
    "config": {
        "desc": "配置文件",
        "extensions": [".conf", ".cfg", ".ini", ".json", ".yaml", ".yml",
                       ".xml", ".toml", ".properties", ".env"],
        "magic": [],
    },
    "firmware": {
        "desc": "固件/Boot/硬件相关",
        "extensions": [".bin", ".img", ".dtb", ".dts", ".rom", ".fw",
                       ".fpga", ".hex", ".srec", ".ubifs", ".cramfs", ".squashfs"],
        "magic": ["firmware", "boot", "device tree", "U-Boot"],
    },
    "crypto": {
        "desc": "证书/密钥/签名",
        "extensions": [".pem", ".crt", ".cer", ".key", ".csr", ".p12", ".pfx",
                       ".sig", ".cms", ".crl"],
        "magic": ["certificate", "PEM", "private key"],
    },
    "database": {
        "desc": "数据库/Schema",
        "extensions": [".db", ".sqlite", ".sqlite3", ".sql", ".mdb", ".ldb"],
        "magic": ["SQLite"],
    },
    "web": {
        "desc": "Web 前端/服务端",
        "extensions": [".html", ".htm", ".css", ".js", ".jsx", ".ts",
                       ".php", ".jsp", ".vue", ".svg"],
        "magic": ["HTML"],
    },
    "network_model": {
        "desc": "网络模型/协议定义",
        "extensions": [".yang", ".mib", ".asn", ".asn1", ".proto", ".protobuf",
                       ".xsd", ".wsdl", ".ncf"],
        "magic": [],
    },
    "document": {
        "desc": "文档/日志",
        "extensions": [".md", ".txt", ".rst", ".log", ".csv", ".pdf"],
        "magic": [],
    },
    "archive": {
        "desc": "压缩包/安装包",
        "extensions": [".tar", ".gz", ".tgz", ".bz2", ".xz", ".zip", ".rar",
                       ".rpm", ".deb", ".ipk", ".cpio"],
        "magic": ["gzip", "tar archive", "Zip archive", "RPM", "cpio"],
    },
}


def get_analyse_filter(types: list[str]) -> dict:
    """根据分析类型列表生成过滤规则。
    返回 {"extensions": [".so", ...], "magic": ["ELF", ...], "all": False}
    """
    if "all" in types or not types:
        return {"extensions": [], "magic": [], "all": True}

    exts: list[str] = []
    magics: list[str] = []
    for t in types:
        info = ANALYSE_TYPES.get(t)
        if info:
            exts.extend(info["extensions"])
            magics.extend(info["magic"])
    return {"extensions": sorted(set(exts)), "magic": sorted(set(magics)), "all": False}


# ─── 服务配置 ───────────────────────────────────────────────────────

class ServiceConfig(BaseModel):
    analyse_targets: list[str] = Field(
        default=["all"],
        description="分析目标文件类型，可组合: binary/script/config/firmware/crypto/database/web/network_model/document/archive/all"
    )
    binary_arch: list[str] = Field(
        default=["all"],
        description="binary 类型的架构过滤，只在 analyse_targets 含 binary 时生效: all/x86/x86_64/arm/aarch64/mips/mips64/ppc/ppc64/riscv/s390"
    )
    parallel_modules: int = Field(default=1, description="Stage 2/3 并行处理的模块数，默认 1（串行）")
    parallel_sub_workers: int = Field(default=1, description="单模块内子 Worker 并行数，默认 1（串行）")
    agent_max_retries: int = Field(default=100, description="API 错误最大重试次数，-1=无限")
    agent_retry_delay: float = Field(default=30.0, description="API 重试首次等待秒数")
    pi_max_retries: int = Field(default=-1, description="pi 进程启动/崩溃最大重试次数，-1=无限")
    pi_retry_delay: float = Field(default=10.0, description="pi 进程重试首次等待秒数")

    stages: StagesConfig = Field(default_factory=StagesConfig)

    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)

    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")
    start_stage: int = Field(default=1, description="从指定阶段开始（1=全流程，3=跳过S1/S2直接S3）")
    resume_workspace: str = Field(default="", description="已有的 workspace 路径，start_stage>1 时使用")


# ─── 运行时任务 ───────────────────────────────────────────────────────────────

class TaskConfig(BaseModel):
    task: str = Field(..., description="用户的一句话 prompt")
    target_dir: str = Field(default="/data/target", description="解包目录路径")
    source_file: str = Field(default="", description="兼容字段：用于归档命名")
    function_name: str = Field(default="", description="兼容字段：用于归档命名")
    cwd: str = Field(default="/data/target")

    agent_max_retries: int = Field(default=100, description="API 错误最大重试次数，-1=无限")
    agent_retry_delay: float = Field(default=30.0, description="API 重试首次等待秒数")
    pi_max_retries: int = Field(default=-1, description="pi 进程启动/崩溃最大重试次数，-1=无限")
    pi_retry_delay: float = Field(default=10.0, description="pi 进程重试首次等待秒数")
    analyse_targets: list[str] = Field(default=["all"], description="分析目标类型")
    binary_arch: list[str] = Field(default=["all"], description="binary 架构过滤")
    parallel_modules: int = Field(default=1, description="Stage 2/3 并行处理的模块数，默认 1（串行）")
    parallel_sub_workers: int = Field(default=1, description="单模块内子 Worker 并行数，默认 1（串行）")
    stages: StagesConfig = Field(default_factory=StagesConfig)
    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)
    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")
    # 恢复运行：跳过前 N-1 阶段，直接从第 start_stage 阶段开始
    # start_stage=3 时必须同时指定 resume_workspace 指向已有 workspace 路径
    start_stage: int = Field(default=1, description="从指定阶段开始（1=全流程，3=跳过S1/S2直接S3）")
    resume_workspace: str = Field(default="", description="已有的 workspace 路径，start_stage>1 时使用")

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

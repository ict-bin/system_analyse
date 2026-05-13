"""
system_analyse — 数据模型
"""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


MAX_ROUNDS_EXCEEDED_ACTIONS = {
    "treat_as_passed",
    "treat_as_failed",
}


def normalize_max_rounds_exceeded_action(value: str | None) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in MAX_ROUNDS_EXCEEDED_ACTIONS:
        return candidate
    return "treat_as_passed"


# ─── Agent 实例配置 ───────────────────────────────────────────────────────────

# Worker/Judge 可配置的阶段名（用于 stage_models 键）
# Workers: explore / classify / refine / sub_read / analyse / report
# Judges:  classify / refine / analyse / completeness / report

WORKER_STAGES = ["explore", "classify", "refine", "sub_read", "analyse", "report"]
JUDGE_STAGES  = ["classify", "refine", "analyse", "completeness", "report"]

WORKER_PROMPT_KEYS = [
    "default",
    "step1_explore",
    "step1_classify",
    "reflect_classify",
    "step2_sub_read",
    "step2_refine",
    "reflect_refine",
    "step2_reclassify",
    "step3_analyse",
    "reflect_analyse",
    "step4_final_report",
    "reflect_report",
]
JUDGE_PROMPT_KEYS = [
    "default",
    "step1_check_classify",
    "step2_check_refine",
    "step3_check_analyse",
    "step4_check_completeness",
    "step4_check_report",
]


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


class PromptOverrideItem(BaseModel):
    content: str = Field(default="", description="Prompt 文本内容")
    source: str = Field(default="default", description="default|project")
    default_content: str = Field(default="", description="默认 Prompt 文本内容")


class PromptOverrideConfig(BaseModel):
    workers: dict[str, PromptOverrideItem] = Field(default_factory=dict)
    judges: dict[str, PromptOverrideItem] = Field(default_factory=dict)

    def get_prompt(self, role: str, key: str) -> str:
        group = self.workers if role == "workers" else self.judges
        item = group.get(key)
        if not item:
            return ""
        return str(item.content or "")


# ─── 服务配置 ─────────────────────────────────────────────────────────────────

class StageLoopConfig(BaseModel):
    """单个阶段的循环控制"""
    min_rounds: int = Field(default=2, description="最少运行轮数（强制反思）")
    max_rounds: int = Field(default=5, description="最多迭代轮数，-1=无限")
    pass_mode: str = Field(default="majority", description="majority=半数以上, all=全部judge通过")


class StagesConfig(BaseModel):
    classify: StageLoopConfig = Field(default_factory=lambda: StageLoopConfig(min_rounds=2, max_rounds=5))
    security_filter: StageLoopConfig = Field(
        default_factory=lambda: StageLoopConfig(min_rounds=1, max_rounds=3, pass_mode="all"),
        description="安全维度过滤阶段（S1后），security_focus_categories != ['all'] 时生效",
    )
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
    "source": {
        "desc": "C/C++ 源代码、汇编",
        "extensions": [".c", ".h", ".cpp", ".cc", ".cxx", ".c++",
                       ".hpp", ".hh", ".hxx", ".inc", ".inl", ".ipp",
                       ".S", ".s", ".asm"],
        "magic": [],
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


# ─── 安全分析维度 ──────────────────────────────────────────────────────────────
# 每个类别包含：
#   name        - 显示名称
#   desc        - 简短描述
#   keywords    - 预扫描关键词
#   includes    - 目标范围：哪些代码属于此维度（用于生成分类提示词）
#   boundary_note - 边界判断规则（通用语言，替代场景化硬编码规则）

SECURITY_CATEGORIES: dict[str, dict] = {
    "network_protocol": {
        "name": "网络协议解析",
        "desc": "TCP/IP 协议栈、报文解析与编解码、socket 通信、TLS/SSL、MQTT/CoAP/QUIC 等协议实现",
        "keywords": ["socket", "tcp", "udp", "ip", "tls", "ssl", "mqtt", "coap", "quic", "dhcp",
                     "dns", "http", "ftp", "snmp", "netlink", "packet", "proto", "protocol"],
        "includes": (
            "HTTP/gRPC/REST/TLS 等网络通信框架及其客户端/服务端实现、"
            "协议解析与编解码、socket 封装层、会话状态机、"
            "网络连接管理模块、协议驱动层、网络 API 接口定义"
        ),
        "boundary_note": (
            "凡直接实现或调用网络协议的代码均属于目标范围，"
            "包括框架调用层（如 gRPC 客户端/服务端、REST API 层），"
            "不限于底层协议解析器本身。"
            "排除：与网络完全无关的纯内存操作、纯本地存储、纯 UI 渲染代码。"
        ),
    },
    "file_parsing": {
        "name": "文件格式处理",
        "desc": "文件读写、格式解析（ZIP/Image/PDF/XML/JSON）、上传下载处理、文件系统操作",
        "keywords": ["parse", "unzip", "extract", "file", "read", "write", "stream",
                     "xml", "json", "pdf", "image", "upload", "download", "fs", "inode"],
        "includes": (
            "文件格式解析库（ZIP/tar/image/OCI层）、压缩/解压缩、"
            "序列化/反序列化、文件上传下载处理、文件系统挂载与操作"
        ),
        "boundary_note": (
            "凡读写或解析文件内容（含容器镜像层、压缩包、配置文件）的代码均属于目标范围。"
            "排除：与文件无关的纯网络收发、纯计算逻辑。"
        ),
    },
    "auth_access": {
        "name": "认证与访问控制",
        "desc": "登录认证、token/session 管理、权限校验、ACL、证书处理、PAM",
        "keywords": ["auth", "login", "token", "session", "acl", "permission", "role",
                     "cert", "pam", "credential", "passwd", "user", "access"],
        "includes": (
            "认证与授权库、权限校验模块、TLS 证书管理、"
            "RBAC/ACL 实现、凭证存储与验证、身份令牌处理"
        ),
        "boundary_note": (
            "凡直接实现身份验证、权限校验或凭证管理的代码属于目标范围。"
            "排除：与认证无关的业务逻辑、纯 UI 渲染。"
        ),
    },
    "crypto": {
        "name": "密码学操作",
        "desc": "加解密、签名验签、密钥管理、随机数生成、哈希运算",
        "keywords": ["aes", "rsa", "ecc", "hmac", "sha", "md5", "encrypt", "decrypt",
                     "sign", "verify", "key", "cipher", "random", "prng", "hash"],
        "includes": (
            "密码算法实现（对称/非对称/哈希）、密钥生成与存储、"
            "证书生成与验证、安全随机数生成器、密钥派生函数"
        ),
        "boundary_note": (
            "凡直接调用密码算法或进行密钥生命周期管理的代码属于目标范围。"
            "排除：仅使用加密结果的业务逻辑（如验证成功后的流程控制）。"
        ),
    },
    "ipc": {
        "name": "进程间通信",
        "desc": "Unix socket、管道、消息队列、共享内存、D-Bus、RPC/gRPC",
        "keywords": ["pipe", "fifo", "mqueue", "shm", "shmem", "dbus", "rpc", "grpc",
                     "ipc", "socket", "uds", "semaphore", "mutex", "signal"],
        "includes": (
            "IPC 通信框架、Unix Domain Socket 封装、消息队列与序列化、"
            "共享内存管理、D-Bus/gRPC 服务接口定义、进程间同步原语"
        ),
        "boundary_note": (
            "凡实现进程间数据传递或同步的代码属于目标范围，"
            "包括 gRPC 服务接口（当以进程间通信视角使用时）。"
            "排除：网络层协议（归 network_protocol）、纯业务逻辑。"
        ),
    },
    "config_parsing": {
        "name": "配置与脚本解析",
        "desc": "XML/JSON/YAML/INI 配置解析器、命令行参数处理、环境变量读取、脚本解释器",
        "keywords": ["config", "conf", "ini", "yaml", "yml", "toml", "environ", "getenv",
                     "argv", "optarg", "getopt", "cmdline", "param"],
        "includes": (
            "配置文件解析器（JSON/YAML/INI/TOML）、"
            "命令行参数处理、环境变量读取、配置热重载机制"
        ),
        "boundary_note": (
            "凡解析外部配置输入的代码属于目标范围。"
            "排除：使用配置值的业务逻辑（如根据配置项决定行为的代码）。"
        ),
    },
    "input_handling": {
        "name": "输入处理与验证",
        "desc": "用户输入边界、命令注入点、缓冲区操作、格式化字符串、输入校验",
        "keywords": ["input", "scanf", "gets", "fgets", "sprintf", "snprintf", "strcat",
                     "strcpy", "memcpy", "memmove", "sscanf", "format", "sanitize", "validate"],
        "includes": (
            "用户输入接收与校验层、命令参数解析、"
            "字符串格式化操作、缓冲区边界检查逻辑"
        ),
        "boundary_note": (
            "凡接收并处理外部（用户/网络/文件）输入且存在注入/溢出风险点的代码属于目标范围。"
        ),
    },
    "privilege_process": {
        "name": "权限与进程管理",
        "desc": "setuid/setgid、特权提升、进程创建与控制、信号处理、能力管理",
        "keywords": ["setuid", "setgid", "seteuid", "setegid", "fork", "exec", "spawn",
                     "prctl", "capability", "cap_set", "signal", "sigaction", "chroot"],
        "includes": (
            "特权操作实现（setuid/capability）、进程 fork/exec 逻辑、"
            "命名空间管理、cgroup 控制、沙箱隔离机制"
        ),
        "boundary_note": (
            "凡涉及特权操作、进程隔离或系统调用边界的代码属于目标范围。"
            "排除：无特权的普通业务逻辑。"
        ),
    },
    "web_api": {
        "name": "Web 与 API 接口",
        "desc": "HTTP 请求/响应处理、REST/SOAP 接口、CGI/FastCGI、URL 路由、Web 框架",
        "keywords": ["http", "https", "url", "uri", "rest", "api", "cgi", "fastcgi",
                     "request", "response", "route", "header", "cookie", "web"],
        "includes": (
            "HTTP 服务端路由与中间件、REST API 控制器、"
            "请求鉴权/限流/过滤、Web 框架集成层"
        ),
        "boundary_note": (
            "凡实现对外 HTTP/REST/Web 服务的代码属于目标范围，"
            "包括 API 路由定义、请求处理器、响应序列化。"
            "排除：纯后端业务逻辑（无 HTTP 接口暴露）。"
        ),
    },
    "memory_manage": {
        "name": "内存管理",
        "desc": "malloc/free、内存映射、引用计数、内存池、与溢出相关的低层操作",
        "keywords": ["malloc", "free", "calloc", "realloc", "mmap", "munmap", "brk",
                     "alloc", "heap", "pool", "refcount", "overflow", "buffer"],
        "includes": (
            "自定义内存分配器、内存池实现、引用计数管理、"
            "内存映射操作、与堆溢出直接相关的缓冲区操作"
        ),
        "boundary_note": (
            "凡直接管理内存分配/释放或存在内存安全风险（溢出/UAF）的底层代码属于目标范围。"
            "排除：仅使用标准 malloc/free 的普通业务代码。"
        ),
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

class SelfReflectionConfig(BaseModel):
    """Self-reflection 自省分析配置。"""
    enabled: bool = Field(default=False, description="任务结束后自动触发自省分析")
    model: str = Field(
        default="",
        description="自省分析使用的 LLM 模型，留空时使用 workers.agents[0].model"
    )
    output_dir: str = Field(
        default="/data/self-reflection",
        description="自省报告存储目录（容器内绝对路径）"
    )
    max_session_lines: int = Field(
        default=1000,
        description="每个 session jsonl 最多读取的行数，防止 context 过大"
    )


class ServiceConfig(BaseModel):
    max_rounds_exceeded_action: str = Field(
        default="treat_as_passed",
        description="达到最大轮次且评审仍未通过时的处理策略：treat_as_passed/treat_as_failed",
    )
    analyse_targets: list[str] = Field(
        default=["all"],
        description="分析目标文件类型，可组合: binary/script/source/config/firmware/crypto/database/web/network_model/document/archive/all"
    )
    binary_arch: list[str] = Field(
        default=["all"],
        description="binary 类型的架构过滤，只在 analyse_targets 含 binary 时生效: all/x86/x86_64/arm/aarch64/mips/mips64/ppc/ppc64/riscv/s390"
    )
    security_focus_categories: list[str] = Field(
        default=["all"],
        description=(
            "安全分析维度过滤，S1 分类时只保留与指定维度相关的模块。"
            "可选: all(不过滤)/network_protocol/file_parsing/auth_access/crypto/"
            "ipc/config_parsing/input_handling/privilege_process/web_api/memory_manage"
        ),
    )
    module_granularity: str = Field(
        default="fine",
        description=(
            "模块划分粒度: fine=子组件级（当前默认），"
            "coarse=协议/服务/功能级（同一协议/功能的所有代码归为一个模块）"
        ),
    )
    parallel_modules: int = Field(default=20, description="Stage 2/3 并行处理的模块数，默认 20")
    parallel_sub_workers: int = Field(default=4, description="单模块内子 Worker 并行数，默认 4")
    agent_max_retries: int = Field(default=5, description="API 错误最大重试次数，-1=无限")
    agent_retry_delay: float = Field(default=30.0, description="API 重试首次等待秒数")
    pi_max_retries: int = Field(default=3, description="pi 进程启动/崩溃最大重试次数，-1=无限")
    pi_retry_delay: float = Field(default=10.0, description="pi 进程重试首次等待秒数")

    stages: StagesConfig = Field(default_factory=StagesConfig)

    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)
    prompt_overrides: PromptOverrideConfig = Field(default_factory=PromptOverrideConfig)

    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")
    start_stage: int = Field(default=0, description="从指定阶段开始（0=全流程，3=跳过S0/S1/S2直接S3）")
    resume_workspace: str = Field(default="", description="已有的 workspace 路径，start_stage>0 时使用")
    self_reflection: SelfReflectionConfig = Field(
        default_factory=SelfReflectionConfig,
        description="自省分析配置"
    )


# ─── 运行时任务 ───────────────────────────────────────────────────────────────

class TaskConfig(BaseModel):
    task: str = Field(..., description="用户的一句话 prompt")
    target_dir: str = Field(default="/data/target", description="解包目录路径")
    source_file: str = Field(default="", description="兼容字段：用于归档命名")
    function_name: str = Field(default="", description="兼容字段：用于归档命名")
    cwd: str = Field(default="/data/target")

    max_rounds_exceeded_action: str = Field(default="treat_as_passed")
    agent_max_retries: int = Field(default=5, description="API 错误最大重试次数，-1=无限")
    agent_retry_delay: float = Field(default=30.0, description="API 重试首次等待秒数")
    pi_max_retries: int = Field(default=3, description="pi 进程启动/崩溃最大重试次数，-1=无限")
    pi_retry_delay: float = Field(default=10.0, description="pi 进程重试首次等待秒数")
    analyse_targets: list[str] = Field(default=["all"], description="分析目标类型")
    binary_arch: list[str] = Field(default=["all"], description="binary 架构过滤")
    security_focus_categories: list[str] = Field(
        default=["all"],
        description="安全分析维度过滤，S1 分类时只保留相关模块。all=不过滤",
    )
    module_granularity: str = Field(
        default="fine",
        description="模块划分粒度: fine=子组件级，coarse=协议/服务/功能级",
    )
    parallel_modules: int = Field(default=20, description="Stage 2/3 并行处理的模块数，默认 20")
    parallel_sub_workers: int = Field(default=4, description="单模块内子 Worker 并行数，默认 4")
    stages: StagesConfig = Field(default_factory=StagesConfig)
    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)
    prompt_overrides: PromptOverrideConfig = Field(default_factory=PromptOverrideConfig)
    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")
    # 恢复运行：跳过前 N-1 阶段，直接从第 start_stage 阶段开始
    # start_stage=3 时必须同时指定 resume_workspace 指向已有 workspace 路径
    start_stage: int = Field(default=0, description="从指定阶段开始（0=全流程，3=跳过S0/S1/S2直接S3）")
    resume_workspace: str = Field(default="", description="已有的 workspace 路径，start_stage>0 时使用")
    self_reflection: SelfReflectionConfig = Field(
        default_factory=SelfReflectionConfig,
        description="自省分析配置"
    )

    @property
    def worker_count(self) -> int:
        return len(self.workers.agents)

    @property
    def judge_count(self) -> int:
        return len(self.judges.agents)

    def get_prompt(self, role: str, key: str) -> str:
        return self.prompt_overrides.get_prompt(role, key)


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

"""
system_analyse — 配置加载
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from .models import AgentInstanceConfig, RoleConfig, ServiceConfig, TaskConfig, normalize_max_rounds_exceeded_action

logger = logging.getLogger("sa.config")

# 容器内固定挂载路径（可通过环境变量覆盖）
# ENV: TARGET_DIR, CONFIG_DIR, OUTPUT_DIR
TARGET_DIR = os.environ.get("TARGET_DIR", "/data/target")
CONFIG_DIR = os.environ.get("CONFIG_DIR", "/data/config")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/data/output")

SERVICE_YAML_PATH = os.environ.get("SERVICE_YAML", "/app/service.yaml")


# ─── service.yaml 数据类 ─────────────────────────────────────────────────────

@dataclass
class DbConfig:
    host: str = "127.0.0.1"
    port: int = 3306
    username: str = "secflow"
    password: str = ""
    name: str = "secflow"
    table_prefix: str = "secflow_"
    pool_size: int = 5
    max_overflow: int = 10

    @property
    def url(self) -> str:
        return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.name}?charset=utf8mb4"


@dataclass
class AuthConfig:
    host: str = "secflow-platform-auth"
    port: int = 80
    validate_token_path: str = "/api/auth/validate-token"
    service_machine_token: str = ""
    timeout: int = 10
    token_cache_enabled: bool = True
    token_cache_ttl_minutes: int = 15

    @property
    def validate_url(self) -> str:
        return f"http://{self.host}:{self.port}{self.validate_token_path}"


@dataclass
class MenuLevelConfig:
    name: Optional[str] = None
    name_en: Optional[str] = None


@dataclass
class MenuConfig:
    id: str = "app-system-analyse"
    path: str = "/app/system-analyse"
    icon: str = "scan-search"
    order: int = 103
    level1: MenuLevelConfig = field(default_factory=MenuLevelConfig)
    level2: MenuLevelConfig = field(default_factory=MenuLevelConfig)
    level3: MenuLevelConfig = field(default_factory=MenuLevelConfig)


@dataclass
class RegistryConfig:
    enabled: bool = True
    menu_service_url: str = "http://secflow-platform-menu:80"
    service_id: str = "secflow-app-system-analyse"
    service_name: str = "二进制系统分析服务"
    host: str = "secflow-app-system-analyse"
    port: int = 80
    maturity: str = "已上线"
    description: str = ""
    api_prefix: str = "/api/app/system-analyse"
    unregister_on_shutdown: bool = False
    heartbeat_interval_seconds: int = 30
    menu: MenuConfig = field(default_factory=MenuConfig)


@dataclass
class AppConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False


@dataclass
class ConfigCenterConfig:
    base_url: str = "http://secflow-platform-configcenter/api/configcenter"
    timeout: int = 30


@dataclass
class ServiceYaml:
    database: DbConfig = field(default_factory=DbConfig)
    auth_service: AuthConfig = field(default_factory=AuthConfig)
    registry: RegistryConfig = field(default_factory=RegistryConfig)
    app: AppConfig = field(default_factory=AppConfig)
    configcenter: ConfigCenterConfig = field(default_factory=ConfigCenterConfig)


def _parse_menu(raw: Dict[str, Any]) -> MenuConfig:
    def parse_level(d: Any) -> MenuLevelConfig:
        if isinstance(d, dict):
            return MenuLevelConfig(name=d.get("name"), name_en=d.get("name_en"))
        return MenuLevelConfig()
    return MenuConfig(
        id=raw.get("id", "app-system-analyse"),
        path=raw.get("path", "/app/system-analyse"),
        icon=raw.get("icon", "scan-search"),
        order=int(raw.get("order", 103)),
        level1=parse_level(raw.get("level1", {})),
        level2=parse_level(raw.get("level2", {})),
        level3=parse_level(raw.get("level3", {})),
    )


def load_service_yaml(yaml_path: str = SERVICE_YAML_PATH) -> ServiceYaml:
    """Load service.yaml config. Falls back to defaults if file not found."""
    p = Path(yaml_path)
    if not p.is_file():
        logger.warning("service.yaml not found at %s, using defaults", yaml_path)
        return ServiceYaml()
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        logger.warning("Failed to parse service.yaml: %s, using defaults", exc)
        return ServiceYaml()

    db_raw = raw.get("database", {})
    db = DbConfig(
        host=db_raw.get("host", "127.0.0.1"),
        port=int(db_raw.get("port", 3306)),
        username=db_raw.get("username", "secflow"),
        password=db_raw.get("password", ""),
        name=db_raw.get("name", "secflow"),
        table_prefix=db_raw.get("table_prefix", "secflow_"),
        pool_size=int(db_raw.get("pool_size", 5)),
        max_overflow=int(db_raw.get("max_overflow", 10)),
    )

    auth_raw = raw.get("auth_service", {})
    auth = AuthConfig(
        host=auth_raw.get("host", "secflow-platform-auth"),
        port=int(auth_raw.get("port", 80)),
        validate_token_path=auth_raw.get("validate_token_path", "/api/auth/validate-token"),
        service_machine_token=auth_raw.get("service_machine_token", ""),
        timeout=int(auth_raw.get("timeout", 10)),
        token_cache_enabled=bool(auth_raw.get("token_cache_enabled", True)),
        token_cache_ttl_minutes=int(auth_raw.get("token_cache_ttl_minutes", 15)),
    )

    reg_raw = raw.get("registry", {})
    registry = RegistryConfig(
        enabled=bool(reg_raw.get("enabled", True)),
        menu_service_url=reg_raw.get("menu_service_url", "http://secflow-platform-menu:80"),
        service_id=reg_raw.get("service_id", "secflow-app-system-analyse"),
        service_name=reg_raw.get("service_name", "二进制系统分析服务"),
        host=reg_raw.get("host", "secflow-app-system-analyse"),
        port=int(reg_raw.get("port", 80)),
        maturity=reg_raw.get("maturity", "已上线"),
        description=reg_raw.get("description", ""),
        api_prefix=reg_raw.get("api_prefix", "/api/app/system-analyse"),
        unregister_on_shutdown=bool(reg_raw.get("unregister_on_shutdown", False)),
        heartbeat_interval_seconds=int(reg_raw.get("heartbeat_interval_seconds", 30)),
        menu=_parse_menu(reg_raw.get("menu", {})),
    )

    app_raw = raw.get("app", {})
    app_cfg = AppConfig(
        host=app_raw.get("host", "0.0.0.0"),
        port=int(app_raw.get("port", 8080)),
        debug=bool(app_raw.get("debug", False)),
    )

    cc_raw = raw.get("configcenter_service", raw.get("configcenter", {}))
    configcenter = ConfigCenterConfig(
        base_url=cc_raw.get("base_url", "http://secflow-platform-configcenter/api/configcenter"),
        timeout=int(cc_raw.get("timeout", 30)),
    )

    return ServiceYaml(database=db, auth_service=auth, registry=registry, app=app_cfg, configcenter=configcenter)


# Module-level singleton
_service_yaml: Optional[ServiceYaml] = None


def get_service_yaml() -> ServiceYaml:
    global _service_yaml
    if _service_yaml is None:
        _service_yaml = load_service_yaml()
    return _service_yaml


def load_service_config(config_path: str) -> ServiceConfig:
    p = Path(config_path)
    if not p.is_file():
        raise FileNotFoundError(f"服务配置文件不存在: {config_path}")
    raw = json.loads(p.read_text(encoding="utf-8"))
    return ServiceConfig(**raw)


def build_task_config(svc: ServiceConfig, prompt: str, cwd: str = "") -> TaskConfig:
    """从服务配置 + 用户 prompt 构造运行时 TaskConfig。"""
    effective_cwd = cwd or TARGET_DIR
    cfg = TaskConfig(
        task=prompt,
        target_dir=effective_cwd,
        cwd=effective_cwd,
        source_file="firmware",
        function_name="analyse",
        max_rounds_exceeded_action=normalize_max_rounds_exceeded_action(
            getattr(svc, "max_rounds_exceeded_action", None)
        ),
        agent_max_retries=svc.agent_max_retries,
        agent_retry_delay=svc.agent_retry_delay,
        pi_max_retries=svc.pi_max_retries,
        pi_retry_delay=svc.pi_retry_delay,
        analyse_targets=svc.analyse_targets,
        binary_arch=svc.binary_arch,
        parallel_modules=svc.parallel_modules,
        parallel_sub_workers=svc.parallel_sub_workers,
        stages=svc.stages.model_copy(deep=True),
        workers=svc.workers.model_copy(deep=True),
        judges=svc.judges.model_copy(deep=True),
        output_dir=svc.output_dir,
        archive_dir=svc.archive_dir,
        result_dir=svc.result_dir,
        start_stage=svc.start_stage,
        resume_workspace=svc.resume_workspace,
    )

    _backfill_role(cfg.workers)
    _backfill_role(cfg.judges)
    return cfg


def _backfill_role(role: RoleConfig) -> None:
    for agent in role.agents:
        if not agent.model:
            agent.model = role.default_model
        if agent.tools is None:
            agent.tools = role.default_tools[:]
        if agent.thinking_level is None:
            agent.thinking_level = role.default_thinking_level


def load_system_prompts(prompt_dir: str, count: int) -> list[str]:
    prompt_dir = os.path.abspath(prompt_dir)
    prompts: list[str] = [""] * count
    if not os.path.isdir(prompt_dir):
        return prompts
    files: dict[str, str] = {}
    for f in sorted(Path(prompt_dir).glob("*.md")):
        files[f.stem] = f.read_text(encoding="utf-8").strip()
    default_text = files.get("default", "")
    prompts = [default_text] * count
    for i in range(count):
        for prefix in [f"worker-{i}", f"judge-{i}", f"{i}"]:
            if prefix in files:
                prompts[i] = files[prefix]
                break
    return prompts


def resolve_system_prompt(
    agent_idx: int,
    agent_cfg: AgentInstanceConfig,
    prompts_from_dir: list[str],
) -> str:
    if agent_cfg.system_prompt:
        return agent_cfg.system_prompt
    if agent_idx < len(prompts_from_dir):
        return prompts_from_dir[agent_idx]
    return ""

"""
system_analyse — 配置加载
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .models import AgentInstanceConfig, RoleConfig, ServiceConfig, TaskConfig

# 容器内固定挂载路径
TARGET_DIR = "/data/target"


def load_service_config(config_path: str) -> ServiceConfig:
    p = Path(config_path)
    if not p.is_file():
        raise FileNotFoundError(f"服务配置文件不存在: {config_path}")
    raw = json.loads(p.read_text(encoding="utf-8"))
    return ServiceConfig(**raw)


def build_task_config(svc: ServiceConfig, prompt: str) -> TaskConfig:
    """从服务配置 + 用户 prompt 构造运行时 TaskConfig。"""
    cfg = TaskConfig(
        task=prompt,
        target_dir=TARGET_DIR,
        cwd=TARGET_DIR,
        source_file="firmware",
        function_name="analyse",
        agent_max_retries=svc.agent_max_retries,
        agent_retry_delay=svc.agent_retry_delay,
        pi_max_retries=svc.pi_max_retries,
        pi_retry_delay=svc.pi_retry_delay,
        analyse_targets=svc.analyse_targets,
        stages=svc.stages.model_copy(deep=True),
        workers=svc.workers.model_copy(deep=True),
        judges=svc.judges.model_copy(deep=True),
        output_dir=svc.output_dir,
        archive_dir=svc.archive_dir,
        result_dir=svc.result_dir,
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

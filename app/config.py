"""
system_analyse — 配置加载 + prompt 解析
"""

from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path

from .models import AgentInstanceConfig, RoleConfig, ServiceConfig, TaskConfig


def load_service_config(config_path: str) -> ServiceConfig:
    p = Path(config_path)
    if not p.is_file():
        raise FileNotFoundError(f"服务配置文件不存在: {config_path}")
    raw = json.loads(p.read_text(encoding="utf-8"))
    return ServiceConfig(**raw)


def build_task_config(svc: ServiceConfig, prompt: str, cwd: str = "/data/target") -> TaskConfig:
    """从服务配置 + 用户 prompt 构造运行时 TaskConfig。"""
    target_dir = parse_target_dir(prompt) or cwd

    cfg = TaskConfig(
        task=prompt,
        target_dir=target_dir,
        cwd=target_dir,
        source_file=os.path.basename(target_dir.rstrip("/")),
        function_name="analyse",
        max_rounds=svc.max_rounds,
        min_rounds=svc.min_rounds,
        pass_threshold=svc.pass_threshold,
        agent_max_retries=svc.agent_max_retries,
        agent_retry_delay=svc.agent_retry_delay,
        workers=svc.workers.model_copy(deep=True),
        judges=svc.judges.model_copy(deep=True),
        output_dir=svc.output_dir,
        archive_dir=svc.archive_dir,
        result_dir=svc.result_dir,
        context=svc.context,
        criteria=svc.criteria,
    )

    _backfill_role(cfg.workers)
    _backfill_role(cfg.judges)

    if cfg.pass_threshold is None:
        cfg.pass_threshold = math.ceil(cfg.judge_count / 2)

    return cfg


def parse_target_dir(prompt: str) -> str:
    """
    从用户 prompt 中提取解包路径。

    支持的格式：
      "软件包解包路径在 /data/target/firmware，对解包后的所有文件进行威胁分析"
      "解包目录 /tmp/unpacked 的威胁分析"
      "分析 /data/target 下的所有文件"
    """
    patterns = [
        r'(?:路径[在为是]|目录[在为是]?|解包到|位于)\s*([/\w._-]+)',
        r'(?:分析|扫描)\s+([/\w._-]+)\s*(?:下|中|的)',
        r'(/(?:data|tmp|home|opt)[/\w._-]+)',
    ]
    for pat in patterns:
        m = re.search(pat, prompt)
        if m:
            path = m.group(1)
            if '/' in path:
                return path
    return ""


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

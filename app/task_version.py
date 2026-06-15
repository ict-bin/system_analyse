"""
task_version.py — 任务格式版本控制

每个任务根目录下的 .task_version 文件记录创建该任务时的代码版本。
大版本（V1.x → V2.x）表示任务目录格式发生了不兼容变更，旧任务目录必须清空重建。

版本号命名规则:
  - V1.0: 初始格式（.snapshot 为文件；无 .task_version）
  - V2.0: .snapshot 始终为文件（S2 每次强制规范化），代码不允许目录形态；引入 .task_version

使用方式:
  1. 在 orchestrator.execute() 和 task_service 的 resume/restart 入口调用
     ensure_task_format_version(task_root)
  2. 当发生不兼容的文件布局变更时，递增大版本号并在此文件注释中记录变更说明
"""

from __future__ import annotations

import shutil
import logging
from pathlib import Path

logger = logging.getLogger("sa.task_version")

# ── 当前任务格式版本 ──────────────────────────────────────────────────────────
# 增变大版本号的场景（不兼容变更）:
#   V2.0: .snapshot 始终为文件（S2 每次 _create_snapshot_file 强制规范化为文件）；引入 .task_version
TASK_FORMAT_VERSION = "2.0"

_VERSION_FILE_NAME = ".task_version"


def get_task_root(output_path: str | None, task_id: str) -> Path | None:
    """从 output_path + task_id 推导任务根目录。"""
    if not output_path:
        return None
    return Path(output_path) / task_id


def read_task_version(task_root: Path) -> str | None:
    """读取任务目录中记录的版本号，不存在返回 None。"""
    version_file = task_root / _VERSION_FILE_NAME
    if not version_file.exists():
        return None
    try:
        return version_file.read_text("utf-8").strip()
    except Exception:
        return None


def _major_version(version: str | None) -> int:
    """提取大版本号整数。无法解析返回 0。"""
    if not version:
        return 0
    try:
        return int(version.strip().lstrip("vV").split(".")[0])
    except (ValueError, IndexError):
        return 0


def is_task_format_compatible(task_root: Path) -> tuple[bool, str | None, str | None]:
    """检查任务目录格式是否与当前版本兼容。

    Returns:
        (compatible, existing_version, required_version)
        兼容时返回 (True, version, version)，不兼容返回 (False, existing, required)
    """
    existing = read_task_version(task_root)
    if existing is None:
        return False, None, TASK_FORMAT_VERSION
    if _major_version(existing) != _major_version(TASK_FORMAT_VERSION):
        return False, existing, TASK_FORMAT_VERSION
    return True, existing, TASK_FORMAT_VERSION


def ensure_task_format_version(task_root: Path) -> None:
    """校验任务目录格式版本，不兼容时清空目录并重建。

    触发清空的条件：
      1. .task_version 文件不存在（判定为旧版本任务）
      2. 大版本号不一致（V1.x → V2.x 等不兼容变更）

    清空后创建空目录并写入当前版本号。
    """
    version_file = task_root / _VERSION_FILE_NAME
    current_major = _major_version(TASK_FORMAT_VERSION)
    existing_version = read_task_version(task_root)
    existing_major = _major_version(existing_version)

    need_clear = False
    reason = ""

    if existing_version is None:
        need_clear = True
        reason = "missing .task_version file (pre-V2.0 task)"
    elif existing_major != current_major:
        need_clear = True
        reason = (
            f"version mismatch: existing={existing_version} "
            f"current={TASK_FORMAT_VERSION}"
        )

    if need_clear and task_root.exists():
        logger.warning(
            "Task format version check: clearing task directory (%s), reason: %s",
            task_root, reason,
        )
        try:
            shutil.rmtree(str(task_root))
        except OSError as exc:
            logger.error(
                "Failed to clear incompatible task directory %s: %s",
                task_root, exc,
            )
            raise

    task_root.mkdir(parents=True, exist_ok=True)
    try:
        version_file.write_text(TASK_FORMAT_VERSION, encoding="utf-8")
    except OSError as exc:
        logger.warning(
            "Failed to write .task_version to %s: %s", task_root, exc
        )

"""
app/tools/__init__.py — LangChain 工具集，完全替代 pi 内置工具

实现 pi 的全部内置工具：read / bash / write / edit / grep / find
接口设计原则：
  - 每个工具通过工厂函数创建，绑定 cwd 和 env
  - 工具名与 pi 完全对齐，使 prompt 无需任何改动
  - bash 工具支持多行脚本、超时、完整 stdout+stderr 捕获
  - read 工具支持目录列表、行范围、大文件截断
  - write 工具自动创建父目录，支持追加模式
  - edit 工具精确 str_replace（替换第一个匹配项）
  - grep 工具优先使用系统 grep，回退到 Python 实现
  - find 工具支持文件/目录/全部三种类型
"""
from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Optional

from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel, Field


# ═══════════════════════════════════════════════════════════════════════
# Bash Tool
# ═══════════════════════════════════════════════════════════════════════

class _BashInput(BaseModel):
    command: str = Field(description="要执行的 Shell 命令（支持多行脚本）")


def _make_bash_func(cwd: str, env: dict | None):
    _cwd = os.path.abspath(cwd)
    _env = dict(env) if env else os.environ.copy()

    def run_bash(command: str) -> str:
        try:
            proc = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=_cwd,
                env=_env,
                timeout=300,
            )
            out = proc.stdout or ""
            err = proc.stderr or ""
            combined = out
            if err.strip():
                combined += ("\nSTDERR:\n" if out.strip() else "") + err
            if proc.returncode != 0:
                combined += f"\n[exit code: {proc.returncode}]"
            return combined.strip() or "(empty output)"
        except subprocess.TimeoutExpired:
            return "Error: command timed out (300s limit)"
        except Exception as exc:
            return f"Error executing command: {exc}"

    return run_bash


# ═══════════════════════════════════════════════════════════════════════
# Read Tool
# ═══════════════════════════════════════════════════════════════════════

class _ReadInput(BaseModel):
    path: str = Field(description="要读取的文件路径（绝对路径或相对于工作目录）")
    start_line: Optional[int] = Field(None, description="起始行号（1-indexed，可选）")
    end_line: Optional[int] = Field(None, description="结束行号（1-indexed，含，可选）")


def _make_read_func(cwd: str):
    _cwd = os.path.abspath(cwd)
    MAX_BYTES = 512 * 1024  # 512KB 截断

    def read_file(path: str, start_line: int | None = None, end_line: int | None = None) -> str:
        try:
            p = Path(path) if os.path.isabs(path) else Path(_cwd) / path
            if not p.exists():
                return f"Error: file not found: {path}"
            if p.is_dir():
                entries = sorted(p.iterdir(), key=lambda x: (x.is_file(), x.name))
                lines = [e.name + ("/" if e.is_dir() else "") for e in entries[:500]]
                total = sum(1 for _ in p.iterdir())
                note = f"\n[showing {min(500, total)} of {total} entries]" if total > 500 else ""
                return "\n".join(lines) + note
            size = p.stat().st_size
            if size > MAX_BYTES and not (start_line or end_line):
                content = p.read_bytes()[:MAX_BYTES].decode("utf-8", errors="replace")
                return content + f"\n\n[TRUNCATED: file is {size} bytes, showing first {MAX_BYTES} bytes]"
            text = p.read_text(encoding="utf-8", errors="replace")
            if start_line or end_line:
                all_lines = text.splitlines(keepends=True)
                s = max(0, (start_line or 1) - 1)
                e = end_line if end_line else len(all_lines)
                return "".join(all_lines[s:e])
            return text
        except Exception as exc:
            return f"Error reading {path}: {exc}"

    return read_file


# ═══════════════════════════════════════════════════════════════════════
# Write Tool
# ═══════════════════════════════════════════════════════════════════════

class _WriteInput(BaseModel):
    path: str = Field(description="要写入的文件路径（绝对路径或相对于工作目录）")
    content: str = Field(description="要写入的内容")
    append: bool = Field(False, description="是否追加模式（True=追加，False=覆盖）")


def _make_write_func(cwd: str):
    _cwd = os.path.abspath(cwd)

    def write_file(path: str, content: str, append: bool = False) -> str:
        try:
            p = Path(path) if os.path.isabs(path) else Path(_cwd) / path
            p.parent.mkdir(parents=True, exist_ok=True)
            if append:
                with open(p, "a", encoding="utf-8") as f:
                    f.write(content)
                return f"Appended {len(content)} chars to {p.name}"
            else:
                p.write_text(content, encoding="utf-8")
                return f"Written {len(content)} chars to {p.name}"
        except Exception as exc:
            return f"Error writing {path}: {exc}"

    return write_file


# ═══════════════════════════════════════════════════════════════════════
# Edit (str_replace) Tool
# ═══════════════════════════════════════════════════════════════════════

class _EditInput(BaseModel):
    path: str = Field(description="要编辑的文件路径")
    old_str: str = Field(description="要替换的精确字符串")
    new_str: str = Field(description="替换后的字符串")


def _make_edit_func(cwd: str):
    _cwd = os.path.abspath(cwd)

    def edit_file(path: str, old_str: str, new_str: str) -> str:
        try:
            p = Path(path) if os.path.isabs(path) else Path(_cwd) / path
            if not p.exists():
                return f"Error: file not found: {path}"
            content = p.read_text(encoding="utf-8")
            if old_str not in content:
                # 尝试忽略行尾差异
                content_normalized = content.replace("\r\n", "\n")
                old_normalized = old_str.replace("\r\n", "\n")
                if old_normalized not in content_normalized:
                    # 显示上下文帮助 LLM 定位
                    preview = content[:500]
                    return (
                        f"Error: string not found in {p.name}.\n"
                        f"File preview (first 500 chars):\n{preview}"
                    )
                content = content_normalized
                old_str = old_normalized
            total = content.count(old_str)
            new_content = content.replace(old_str, new_str, 1)
            p.write_text(new_content, encoding="utf-8")
            note = f" (found {total} occurrences, replaced first)" if total > 1 else ""
            return f"Edited {p.name}{note}"
        except Exception as exc:
            return f"Error editing {path}: {exc}"

    return edit_file


# ═══════════════════════════════════════════════════════════════════════
# Grep Tool
# ═══════════════════════════════════════════════════════════════════════

class _GrepInput(BaseModel):
    pattern: str = Field(description="搜索的正则表达式或字面量模式")
    path: str = Field(description="要搜索的文件或目录")
    recursive: bool = Field(True, description="是否递归搜索目录")
    case_sensitive: bool = Field(False, description="是否区分大小写")
    max_results: int = Field(200, description="最大返回结果行数")


def _make_grep_func(cwd: str):
    _cwd = os.path.abspath(cwd)

    def grep(
        pattern: str,
        path: str,
        recursive: bool = True,
        case_sensitive: bool = False,
        max_results: int = 200,
    ) -> str:
        p = Path(path) if os.path.isabs(path) else Path(_cwd) / path

        # 优先使用系统 grep（更快）
        try:
            flags = [] if case_sensitive else ["-i"]
            r_flag = ["-r"] if recursive and p.is_dir() else []
            cmd = ["grep", "-n"] + flags + r_flag + ["--", pattern, str(p)]
            result = subprocess.run(
                cmd, capture_output=True, text=True, cwd=_cwd, timeout=30
            )
            if result.returncode == 0:
                lines = result.stdout.splitlines()[:max_results]
                suffix = f"\n[truncated at {max_results} results]" if len(result.stdout.splitlines()) > max_results else ""
                return "\n".join(lines) + suffix or "No matches found"
            if result.returncode == 1:
                return "No matches found"
            # grep error, fall through to Python
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        except Exception:
            pass

        # Python fallback
        try:
            flags_re = 0 if case_sensitive else re.IGNORECASE
            try:
                pat = re.compile(pattern, flags_re)
            except re.error:
                pat = re.compile(re.escape(pattern), flags_re)

            if not p.exists():
                return f"Error: path not found: {path}"
            files = [p] if p.is_file() else (
                list(p.rglob("*")) if recursive else list(p.glob("*"))
            )
            results = []
            for fp in files:
                if not fp.is_file():
                    continue
                try:
                    for i, line in enumerate(
                        fp.read_text(encoding="utf-8", errors="replace").splitlines(), 1
                    ):
                        if pat.search(line):
                            results.append(f"{fp}:{i}: {line}")
                            if len(results) >= max_results:
                                results.append(f"[truncated at {max_results}]")
                                return "\n".join(results)
                except Exception:
                    pass
            return "\n".join(results) or "No matches found"
        except Exception as exc:
            return f"Error grepping {path}: {exc}"

    return grep


# ═══════════════════════════════════════════════════════════════════════
# Find Tool
# ═══════════════════════════════════════════════════════════════════════

class _FindInput(BaseModel):
    path: str = Field(description="要搜索的根目录")
    pattern: str = Field("*", description="文件名 glob 模式")
    type: str = Field("f", description="类型：'f'=文件，'d'=目录，'a'=全部")
    max_results: int = Field(1000, description="最大返回数量")


def _make_find_func(cwd: str):
    _cwd = os.path.abspath(cwd)

    def find_files(path: str, pattern: str = "*", type: str = "f", max_results: int = 1000) -> str:
        try:
            p = Path(path) if os.path.isabs(path) else Path(_cwd) / path
            if not p.exists():
                return f"Error: path not found: {path}"
            results = []
            for item in sorted(p.rglob(pattern)):
                if type == "f" and not item.is_file():
                    continue
                if type == "d" and not item.is_dir():
                    continue
                results.append(str(item))
                if len(results) >= max_results:
                    results.append(f"[truncated at {max_results}]")
                    break
            return "\n".join(results) or "No matches found"
        except Exception as exc:
            return f"Error finding in {path}: {exc}"

    return find_files


# ═══════════════════════════════════════════════════════════════════════
# Tool Factory — 核心对外接口
# ═══════════════════════════════════════════════════════════════════════

def make_tools(
    tool_names: list[str],
    cwd: str = ".",
    env: dict | None = None,
) -> list[BaseTool]:
    """
    根据工具名列表创建 LangChain 工具集，绑定 cwd 与 env。

    tool_names 支持 pi 的全部内置工具名：
      read, write, bash, edit, str_replace, grep, find
      read_file, write_file（别名）
    """
    _cwd = os.path.abspath(cwd)

    # 工厂：按需创建，避免不必要的函数闭包
    def _bash():
        return StructuredTool.from_function(
            func=_make_bash_func(_cwd, env),
            name="bash",
            description=(
                "在工作目录中执行 bash/shell 命令。"
                "支持多行脚本、管道、重定向。返回 stdout 和 stderr。"
                "注意：不要使用 cd 命令，所有路径请用绝对路径或相对于工作目录的路径。"
            ),
            args_schema=_BashInput,
        )

    def _read():
        return StructuredTool.from_function(
            func=_make_read_func(_cwd),
            name="read",
            description=(
                "读取文件内容。支持文本文件，大文件自动截断。"
                "传入目录路径时列出目录内容。"
                "可选 start_line/end_line 参数读取指定行范围。"
            ),
            args_schema=_ReadInput,
        )

    def _read_file():
        return StructuredTool.from_function(
            func=_make_read_func(_cwd),
            name="read_file",
            description="读取文件内容（read 的别名）。",
            args_schema=_ReadInput,
        )

    def _write():
        return StructuredTool.from_function(
            func=_make_write_func(_cwd),
            name="write",
            description=(
                "将内容写入文件。自动创建父目录。"
                "append=True 时追加内容，否则覆盖。"
            ),
            args_schema=_WriteInput,
        )

    def _write_file():
        return StructuredTool.from_function(
            func=_make_write_func(_cwd),
            name="write_file",
            description="写入文件（write 的别名）。",
            args_schema=_WriteInput,
        )

    def _edit():
        return StructuredTool.from_function(
            func=_make_edit_func(_cwd),
            name="edit",
            description=(
                "精确替换文件中的指定文本（str_replace 模式）。"
                "old_str 必须与文件内容完全匹配。只替换第一个匹配项。"
            ),
            args_schema=_EditInput,
        )

    def _str_replace():
        return StructuredTool.from_function(
            func=_make_edit_func(_cwd),
            name="str_replace",
            description="精确替换文件中的文本（edit 的别名）。",
            args_schema=_EditInput,
        )

    def _grep():
        return StructuredTool.from_function(
            func=_make_grep_func(_cwd),
            name="grep",
            description=(
                "在文件或目录中搜索匹配模式的行。"
                "支持正则表达式，返回带行号的匹配结果。"
            ),
            args_schema=_GrepInput,
        )

    def _find():
        return StructuredTool.from_function(
            func=_make_find_func(_cwd),
            name="find",
            description=(
                "在目录树中查找文件或目录。"
                "支持 glob 模式，返回匹配的路径列表。"
            ),
            args_schema=_FindInput,
        )

    _tool_builders = {
        "bash":       _bash,
        "read":       _read,
        "read_file":  _read_file,
        "write":      _write,
        "write_file": _write_file,
        "edit":       _edit,
        "str_replace": _str_replace,
        "grep":       _grep,
        "find":       _find,
    }

    seen: set[str] = set()
    result: list[BaseTool] = []
    for name in tool_names:
        builder = _tool_builders.get(name)
        if builder is None:
            continue  # 跳过未知工具名
        tool = builder()
        if tool.name not in seen:
            result.append(tool)
            seen.add(tool.name)
    return result

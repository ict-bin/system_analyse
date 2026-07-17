"""按语言提取源码函数（名 / 体）。

设计目标：彻底消除"函数名提取"的正则灾难性回溯（catastrophic backtracking），
该回溯曾导致 runner 主进程单线程钉满 CPU、持有 GIL、饿死注册心跳 → runner 僵死。

策略（按语言分派）：
  - C/C++ 源码        : tree-sitter (tree-sitter-cpp)  —— 线性时间、对病态输入鲁棒、不回溯。
                        tree-sitter 不可用时降级到"逐行 + 跳过超长行 + 无嵌套量词"的安全线性正则。
  - sh / py 等脚本    : 安全线性正则匹配函数名及函数体（无嵌套量词，逐行/配平，均有长度上限）。

所有路径均为 O(content_size)，且对单行/单函数体设置硬上限，保证不会出现 CPU 爆炸。
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger("sa.func_extract")

# ── 扩展名分类 ────────────────────────────────────────────────────────────────
_CPP_EXTS = {".c", ".h", ".cpp", ".cc", ".cxx", ".hpp", ".hh", ".hxx", ".inc", ".inl"}
_SH_EXTS = {".sh", ".bash"}
_PY_EXTS = {".py"}

# 安全上限（防病态输入）
_MAX_LINE_LEN = 2000          # 超过此长度的单行直接跳过（压缩/生成代码/base64）
_MAX_BODY_LEN = 8000          # 单个函数体最多保留字符数
_MAX_BODY_SCAN = 200_000      # 函数体配平扫描的最大字符数

_CONTROL_KW = frozenset({
    "if", "for", "while", "switch", "return", "sizeof", "typeof", "alignof",
    "else", "case", "break", "continue", "do", "goto", "typedef", "struct",
    "union", "enum", "class", "namespace", "template", "static_assert",
})


# ═══════════════════════════════════════════════════════════════════════════════
# C / C++ —— tree-sitter（主）+ 安全正则（降级）
# ═══════════════════════════════════════════════════════════════════════════════

_ts_parser = None          # 缓存的 Parser 实例
_ts_ready: bool | None = None  # None=未初始化, True=可用, False=不可用


def _get_cpp_parser():
    """惰性初始化 tree-sitter C++ Parser；不可用时返回 None（降级到正则）。"""
    global _ts_parser, _ts_ready
    if _ts_ready is not None:
        return _ts_parser
    try:
        from tree_sitter import Language, Parser  # type: ignore
        import tree_sitter_cpp  # type: ignore

        lang = Language(tree_sitter_cpp.language())
        try:
            _ts_parser = Parser(lang)          # tree-sitter >= 0.23 风格
        except TypeError:
            _ts_parser = Parser()              # 旧版风格
            _ts_parser.language = lang
        _ts_ready = True
    except Exception as exc:  # pragma: no cover - 仅在缺依赖时
        logger.warning("tree-sitter 不可用，C/C++ 函数提取降级为安全正则: %s", exc)
        _ts_parser = None
        _ts_ready = False
    return _ts_parser


def _cpp_decl_name(node) -> str | None:
    """从 function_definition / declaration 节点下钻到 function_declarator 取函数名。"""
    d = node.child_by_field_name("declarator")
    steps = 0
    while d is not None and d.type != "function_declarator" and steps < 12:
        nd = d.child_by_field_name("declarator")
        if nd is None or nd is d:
            break
        d = nd
        steps += 1
    if d is None or d.type != "function_declarator":
        return None
    nm = d.child_by_field_name("declarator")
    if nm is None:
        return None
    text = nm.text.decode("utf-8", "replace").strip()
    return text or None


def extract_cpp_functions(content: str, limit: int = 200) -> list[dict]:
    """用 tree-sitter 提取 C/C++ 函数（定义 + 原型）。线性时间，不回溯。"""
    parser = _get_cpp_parser()
    if parser is None:
        return _extract_cpp_functions_fallback(content, limit)
    try:
        tree = parser.parse(content.encode("utf-8", "replace"))
    except Exception:
        return _extract_cpp_functions_fallback(content, limit)
    out: list[dict] = []
    seen: set[str] = set()
    stack = [tree.root_node]
    while stack and len(out) < limit:
        n = stack.pop()
        if n.type in ("function_definition", "declaration"):
            name = _cpp_decl_name(n)
            if name and name not in seen:
                seen.add(name)
                item = {"name": name}
                if n.type == "function_definition":
                    body = n.text.decode("utf-8", "replace")
                    item["body"] = body[:_MAX_BODY_LEN]
                out.append(item)
        # children 逆序入栈以保持源码出现顺序
        for c in reversed(n.children):
            stack.append(c)
    if len(out) < limit:
        for item in _extract_cpp_functions_fallback(content, limit):
            name = str(item.get("name") or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            out.append(item)
            if len(out) >= limit:
                break
    return out


# 降级用安全线性正则：要求 `<类型token> <名字>(`，逐行扫描、跳过超长行，无嵌套量词。
_CPP_FALLBACK_RE = re.compile(r"[A-Za-z_]\w*[ \t\*&>]+([A-Za-z_]\w*)[ \t]*\(")


def _extract_cpp_functions_fallback(content: str, limit: int = 200) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for line in content.split("\n"):
        if len(line) > _MAX_LINE_LEN:
            continue
        m = _CPP_FALLBACK_RE.search(line)
        if not m:
            continue
        name = m.group(1)
        if name in _CONTROL_KW or name in seen:
            continue
        seen.add(name)
        out.append({"name": name})
        if len(out) >= limit:
            break
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# Shell —— 安全线性正则（名 + 体）
# ═══════════════════════════════════════════════════════════════════════════════

# 匹配 `name() {` 或 `function name {`；无嵌套量词，逐行锚定。
_SH_FUNC_RE = re.compile(
    r"^[ \t]*(?:function[ \t]+)?([A-Za-z_]\w*)[ \t]*\(\)[ \t]*\{"
    r"|^[ \t]*function[ \t]+([A-Za-z_]\w*)[ \t]*\{",
    re.MULTILINE,
)


def _brace_body(content: str, brace_pos: int) -> str:
    """从 '{' 起做花括号配平提取函数体（线性扫描 + 硬上限）。"""
    depth = 0
    i = brace_pos
    n = len(content)
    end_scan = min(n, brace_pos + _MAX_BODY_SCAN)
    while i < end_scan:
        c = content[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return content[brace_pos:i + 1][:_MAX_BODY_LEN]
        i += 1
    return content[brace_pos:min(n, brace_pos + _MAX_BODY_LEN)]


def extract_shell_functions(content: str, limit: int = 200) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for m in _SH_FUNC_RE.finditer(content):
        name = m.group(1) or m.group(2)
        if not name or name in seen:
            continue
        seen.add(name)
        brace_pos = content.find("{", m.start(), m.end())
        body = _brace_body(content, brace_pos) if brace_pos >= 0 else ""
        out.append({"name": name, "body": body})
        if len(out) >= limit:
            break
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# Python —— 安全线性正则（名 + 体，按缩进界定）
# ═══════════════════════════════════════════════════════════════════════════════

_PY_DEF_RE = re.compile(r"^([ \t]*)(?:async[ \t]+)?def[ \t]+([A-Za-z_]\w*)[ \t]*\(", re.MULTILINE)


def extract_python_functions(content: str, limit: int = 200) -> list[dict]:
    lines = content.split("\n")
    out: list[dict] = []
    seen: set[str] = set()
    for m in _PY_DEF_RE.finditer(content):
        name = m.group(2)
        if not name or name in seen:
            continue
        seen.add(name)
        line_no = content.count("\n", 0, m.start())
        indent = len(m.group(1).expandtabs())
        body_lines = [lines[line_no]] if line_no < len(lines) else []
        for ln in lines[line_no + 1:]:
            if ln.strip() == "":
                body_lines.append(ln)
                continue
            cur_indent = len(ln) - len(ln.lstrip())
            if cur_indent <= indent:
                break
            body_lines.append(ln)
            if len(body_lines) > 500:
                break
        out.append({"name": name, "body": "\n".join(body_lines)[:_MAX_BODY_LEN]})
        if len(out) >= limit:
            break
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# 分派器
# ═══════════════════════════════════════════════════════════════════════════════

def extract_functions(rel_path: str, content: str, limit: int = 200) -> list[dict]:
    """按文件扩展名分派：C/C++→tree-sitter，sh/py→安全正则。返回 [{name, body?}, ...]。"""
    ext = Path(rel_path).suffix.lower()
    if ext in _CPP_EXTS:
        return extract_cpp_functions(content, limit)
    if ext in _SH_EXTS:
        return extract_shell_functions(content, limit)
    if ext in _PY_EXTS:
        return extract_python_functions(content, limit)
    return []


def extract_function_names(rel_path: str, content: str, limit: int = 200) -> list[str]:
    """仅返回函数名列表（用于摘要/提示）。"""
    return [f["name"] for f in extract_functions(rel_path, content, limit)]

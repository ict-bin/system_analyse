"""Module dependency graph builder for secflow-app-system-analyse.

Builds a deterministic module-level dependency graph from Stage0 details/*.json
and final modules/*/files.list. The graph is persisted to SQLite for backend
queries/forensics and to JSON for frontend rendering.
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

from .helpers import get_modules_root, load_detail_json, read_module_files


def _norm_symbol(value: object) -> str:
    text = str(value or "").strip()
    # Strip common symbol version suffixes: foo@@GLIBC_2.4 / foo@Base
    text = re.sub(r"@{1,2}[^@\s]+$", "", text)
    return text


def _lib_stem(path: str) -> str:
    name = Path(path.replace("\\", "/")).name.lower()
    name = re.sub(r"\.(so|ko|a)([.\-_0-9a-z]*)$", "", name)
    name = re.sub(r"[-_]\d[.\-_0-9a-z]*$", "", name)
    return name


def _source_module_keys(rel_path: str) -> set[str]:
    """Keys that may identify a source file as provider of imports.

    Examples:
      src/foo/bar.py   -> foo.bar, bar, src.foo.bar
      include/net/ip.h -> ip, net/ip, net.ip
      lib/a/b.c        -> b, a.b
    """
    p = Path(rel_path.replace("\\", "/"))
    no_suffix = str(p.with_suffix("")).replace("\\", "/")
    parts = [part for part in no_suffix.split("/") if part]
    keys: set[str] = set()
    if not parts:
        return keys
    keys.add(parts[-1].lower())
    for n in (2, 3):
        if len(parts) >= n:
            keys.add("/".join(parts[-n:]).lower())
            keys.add(".".join(parts[-n:]).lower())
    # Drop common root containers for Python import matching.
    for idx, part in enumerate(parts):
        if part in {"src", "lib", "libs", "python", "py", "app", "apps"} and idx + 1 < len(parts):
            tail = parts[idx + 1:]
            keys.add(".".join(tail).lower())
            keys.add("/".join(tail).lower())
    return {k for k in keys if k}


def _module_key_candidates(value: object) -> set[str]:
    text = str(value or "").strip().strip('"\'')
    if not text:
        return set()
    normalized = text.replace("\\", "/").replace("::", ".").strip("./")
    lowered = normalized.lower()
    keys = {lowered, lowered.replace("/", "."), lowered.replace(".", "/")}
    base = Path(lowered).name
    stem = Path(base).stem
    if base:
        keys.add(base)
    if stem:
        keys.add(stem)
    parts = [p for p in re.split(r"[./]+", lowered) if p]
    for n in (1, 2, 3):
        if len(parts) >= n:
            keys.add(".".join(parts[-n:]))
            keys.add("/".join(parts[-n:]))
    return {k for k in keys if k}


def _is_external_import_key(key: str) -> bool:
    root = re.split(r"[./]", key, maxsplit=1)[0].lower()
    return root in {
        "stdio", "stdlib", "string", "stdint", "stdbool", "unistd", "fcntl", "sys", "linux",
        "errno", "pthread", "time", "math", "vector", "string", "map", "set", "iostream",
        "os", "sys", "re", "json", "typing", "pathlib", "datetime", "asyncio", "logging",
        "collections", "itertools", "functools", "subprocess", "sqlite3", "sqlalchemy", "fastapi",
        "pydantic", "pytest", "numpy", "pandas", "requests", "httpx",
    }


def _needed_key(value: object) -> str:
    return _lib_stem(str(value or ""))


def _detail_lists(detail: dict[str, Any] | None) -> tuple[list[str], list[str], list[str]]:
    if not isinstance(detail, dict):
        return [], [], []
    exports = detail.get("exports") or detail.get("symbols") or []
    imports = detail.get("imports") or []
    needed = detail.get("needed") or []
    return (
        [_norm_symbol(x) for x in exports if _norm_symbol(x)],
        [_norm_symbol(x) for x in imports if _norm_symbol(x)],
        [_needed_key(x) for x in needed if _needed_key(x)],
    )


def _risk_from_report(mod_dir: Path) -> tuple[str, int]:
    report = mod_dir / "module_report.md"
    risk_level = "未知"
    risk_score = 0
    if report.exists():
        text = report.read_text("utf-8", errors="replace")[:3000]
        m = re.search(r"RISK_LEVEL:\s*([^>\n]+)", text, flags=re.I)
        if m:
            risk_level = m.group(1).strip()
        m = re.search(r"RISK_SCORE:\s*(\d+)", text, flags=re.I)
        if m:
            risk_score = min(int(m.group(1)), 100)
    return risk_level, risk_score


def build_module_dependency_graph(
    workspace: Path,
    details_dir: Path,
    sqlite_path: Path | None = None,
    json_path: Path | None = None,
) -> dict[str, Any]:
    """Build and persist module dependency graph.

    Edge direction: source -> target means source imports/needs symbols or
    libraries exported/owned by target, i.e. source depends on target.
    """
    modules_root = get_modules_root(workspace)
    modules = [m for m in sorted(modules_root.iterdir()) if m.is_dir() and read_module_files(m)]

    module_files: dict[str, list[str]] = {m.name: read_module_files(m) for m in modules}
    file_owner: dict[str, str] = {}
    for mod, files in module_files.items():
        for rel in files:
            file_owner[rel] = mod

    export_owner: dict[str, set[str]] = defaultdict(set)
    lib_owner: dict[str, set[str]] = defaultdict(set)
    source_module_owner: dict[str, set[str]] = defaultdict(set)
    file_detail_cache: dict[str, dict[str, Any] | None] = {}

    for mod, files in module_files.items():
        for rel in files:
            detail = load_detail_json(details_dir, rel) if details_dir and details_dir.exists() else None
            file_detail_cache[rel] = detail
            exports, _imports, _needed = _detail_lists(detail)
            for sym in exports:
                export_owner[sym].add(mod)
            lib_owner[_lib_stem(rel)].add(mod)
            for key in _source_module_keys(rel):
                source_module_owner[key].add(mod)

    edge_payload: dict[tuple[str, str], dict[str, Any]] = {}
    for src_mod, files in module_files.items():
        for rel in files:
            detail = file_detail_cache.get(rel)
            _exports, imports, needed = _detail_lists(detail)
            for sym in imports:
                for dst_mod in export_owner.get(sym, set()):
                    if dst_mod == src_mod:
                        continue
                    item = edge_payload.setdefault((src_mod, dst_mod), {
                        "source": src_mod,
                        "target": dst_mod,
                        "weight": 0,
                        "symbols": [],
                        "needed": [],
                        "files": [],
                        "edge_kinds": [],
                    })
                    item["weight"] += 1
                    if sym not in item["symbols"] and len(item["symbols"]) < 30:
                        item["symbols"].append(sym)
                    if "symbol_import" not in item["edge_kinds"]:
                        item["edge_kinds"].append("symbol_import")
                    if rel not in item["files"] and len(item["files"]) < 30:
                        item["files"].append(rel)
            for lib in needed:
                for dst_mod in lib_owner.get(lib, set()):
                    if dst_mod == src_mod:
                        continue
                    item = edge_payload.setdefault((src_mod, dst_mod), {
                        "source": src_mod,
                        "target": dst_mod,
                        "weight": 0,
                        "symbols": [],
                        "needed": [],
                        "files": [],
                        "edge_kinds": [],
                    })
                    item["weight"] += 5
                    if lib not in item["needed"] and len(item["needed"]) < 30:
                        item["needed"].append(lib)
                    if "binary_needed" not in item["edge_kinds"]:
                        item["edge_kinds"].append("binary_needed")
                    if rel not in item["files"] and len(item["files"]) < 30:
                        item["files"].append(rel)
                # Source language import/header dependency matching.
                for key in _module_key_candidates(lib):
                    if _is_external_import_key(key):
                        continue
                    for dst_mod in source_module_owner.get(key, set()):
                        if dst_mod == src_mod:
                            continue
                        item = edge_payload.setdefault((src_mod, dst_mod), {
                            "source": src_mod,
                            "target": dst_mod,
                            "weight": 0,
                            "symbols": [],
                            "needed": [],
                            "files": [],
                            "edge_kinds": [],
                        })
                        item["weight"] += 3
                        if lib not in item["needed"] and len(item["needed"]) < 30:
                            item["needed"].append(lib)
                        if "source_import" not in item["edge_kinds"]:
                            item["edge_kinds"].append("source_import")
                        if rel not in item["files"] and len(item["files"]) < 30:
                            item["files"].append(rel)

    edges = sorted(edge_payload.values(), key=lambda e: (e["source"], e["target"]))
    out_deg: dict[str, int] = {m: 0 for m in module_files}
    in_deg: dict[str, int] = {m: 0 for m in module_files}
    total_weight: dict[str, int] = {m: 0 for m in module_files}
    for e in edges:
        out_deg[e["source"]] += 1
        in_deg[e["target"]] += 1
        total_weight[e["source"]] += int(e["weight"])

    nodes = []
    for mod in sorted(module_files):
        risk_level, risk_score = _risk_from_report(modules_root / mod)
        dependency_count = out_deg.get(mod, 0)
        reverse_dependency_count = in_deg.get(mod, 0)
        # Less dependencies => more likely outer boundary; expose this as bonus for sorting/UI.
        dependency_risk_bonus = max(0, 20 - min(20, dependency_count * 4))
        nodes.append({
            "id": mod,
            "module_name": mod,
            "file_count": len(module_files[mod]),
            "risk_level": risk_level,
            "risk_score": risk_score,
            "dependency_count": dependency_count,
            "reverse_dependency_count": reverse_dependency_count,
            "dependency_weight": total_weight.get(mod, 0),
            "dependency_risk_bonus": dependency_risk_bonus,
            "outer_layer_score": dependency_risk_bonus + max(0, 10 - min(10, reverse_dependency_count)),
        })

    graph = {
        "version": 1,
        "direction": "source_depends_on_target",
        "summary": {
            "module_count": len(nodes),
            "edge_count": len(edges),
            "symbol_export_count": len(export_owner),
            "source_module_key_count": len(source_module_owner),
        },
        "nodes": nodes,
        "edges": edges,
    }

    if sqlite_path:
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        _write_sqlite(sqlite_path, graph, module_files)
    if json_path:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(graph, ensure_ascii=False, indent=2), encoding="utf-8")
    return graph


def _write_sqlite(sqlite_path: Path, graph: dict[str, Any], module_files: dict[str, list[str]]) -> None:
    if sqlite_path.exists():
        sqlite_path.unlink()
    conn = sqlite3.connect(str(sqlite_path))
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE modules (module_name TEXT PRIMARY KEY, file_count INTEGER, risk_level TEXT, risk_score INTEGER, dependency_count INTEGER, reverse_dependency_count INTEGER, dependency_weight INTEGER, dependency_risk_bonus INTEGER, outer_layer_score INTEGER)")
        cur.execute("CREATE TABLE edges (source_module TEXT, target_module TEXT, weight INTEGER, symbols_json TEXT, needed_json TEXT, files_json TEXT, edge_kinds_json TEXT, PRIMARY KEY(source_module, target_module))")
        cur.execute("CREATE TABLE module_files (module_name TEXT, rel_path TEXT, PRIMARY KEY(module_name, rel_path))")
        for n in graph.get("nodes", []):
            cur.execute(
                "INSERT INTO modules VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (n["module_name"], n["file_count"], n["risk_level"], n["risk_score"], n["dependency_count"], n["reverse_dependency_count"], n["dependency_weight"], n["dependency_risk_bonus"], n["outer_layer_score"]),
            )
        for e in graph.get("edges", []):
            cur.execute(
                "INSERT INTO edges VALUES (?, ?, ?, ?, ?, ?, ?)",
                (e["source"], e["target"], e["weight"], json.dumps(e.get("symbols", []), ensure_ascii=False), json.dumps(e.get("needed", []), ensure_ascii=False), json.dumps(e.get("files", []), ensure_ascii=False), json.dumps(e.get("edge_kinds", []), ensure_ascii=False)),
            )
        for mod, files in module_files.items():
            for rel in files:
                cur.execute("INSERT OR IGNORE INTO module_files VALUES (?, ?)", (mod, rel))
        conn.commit()
    finally:
        conn.close()

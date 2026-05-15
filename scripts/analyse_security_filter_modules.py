#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

SECURITY_HINTS = {
    "network_protocol": [
        "network", "grpc", "rest", "http", "socket", "router", "packet", "protocol",
        "api", "service", "request", "response", "stream",
    ],
    "file_parsing": [
        "parse", "parser", "json", "yaml", "toml", "spec", "tar", "archive", "decode", "encode",
    ],
    "input_handling": [
        "cmd", "cli", "args", "config", "hook", "cgroup", "device", "volume", "mount", "runtime",
    ],
    "web_api": [
        "http", "rest", "grpc", "api", "server", "route", "handler", "request", "response",
    ],
}

USELESS_HINTS = [
    "test", "mock", "stub", "fake", "fixture", "readme", "license", "cmake", "makefile",
    "jenkins", ".github", "locale", "i18n", ".po", ".pot",
]


def load_detail(details_dir: Path, rel: str) -> dict:
    p = details_dir / f"{rel}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def score_file(rel: str, detail: dict) -> tuple[int, list[str], list[str]]:
    text = " ".join([
        rel.lower(),
        str(detail.get("summary", "")).lower(),
        " ".join(str(x).lower() for x in detail.get("functions", [])[:20]),
        " ".join(str(x).lower() for x in detail.get("symbols", [])[:20]),
        " ".join(str(x).lower() for x in detail.get("imports", [])[:20]),
        " ".join(str(x).lower() for x in detail.get("keywords", [])[:20]),
    ])
    cats = []
    hits = []
    score = 0
    for cat, kws in SECURITY_HINTS.items():
        for kw in kws:
            if kw in text:
                score += 1
                cats.append(cat)
                hits.append(kw)
                break
    return score, sorted(set(cats)), sorted(set(hits))[:8]


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: analyse_security_filter_modules.py <workspace> <output_md>", file=sys.stderr)
        return 2
    workspace = Path(sys.argv[1])
    out = Path(sys.argv[2])
    modules_root = workspace / "modules"
    details_dir = workspace / "details"
    mods = sorted([d for d in modules_root.iterdir() if d.is_dir()]) if modules_root.exists() else []

    lines: list[str] = [
        "# 模块安全过滤预判摘要",
        "",
        "> 本文件由脚本预先生成，供 S1.5 Worker/Judge 参考。",
        "> 规则：先看模块内文件信息，再决定是否删除；模块名只能作为弱信号。",
        "",
    ]
    for mod in mods:
        fl = mod / "files.list"
        files = [ln.strip() for ln in fl.read_text(encoding="utf-8", errors="replace").splitlines() if ln.strip()] if fl.exists() else []
        cat_counter = Counter()
        type_counter = Counter()
        top_hits = Counter()
        security_files = 0
        useless_files = 0
        sample_relevant: list[str] = []
        for rel in files:
            d = load_detail(details_dir, rel)
            ftype = str(d.get("type", "UNKNOWN"))
            type_counter[ftype] += 1
            score, cats, hits = score_file(rel, d)
            if score > 0:
                security_files += 1
                if len(sample_relevant) < 6:
                    sample_relevant.append(rel)
            lrel = rel.lower()
            if any(h in lrel for h in USELESS_HINTS):
                useless_files += 1
            for c in cats:
                cat_counter[c] += 1
            for h in hits:
                top_hits[h] += 1

        ratio = (security_files / len(files)) if files else 0.0
        if security_files == 0 and useless_files >= max(1, len(files) // 2):
            verdict = "倾向删除（脚本预判：主要为无用/非安全相关内容）"
        elif security_files > 0:
            verdict = "倾向保留（脚本预判：存在安全相关文件，禁止仅凭模块名删除）"
        else:
            verdict = "需要人工判断（脚本无法确认，默认宁可保留）"

        lines.extend([
            f"## 模块 `{mod.name}`",
            "",
            f"- 文件数：{len(files)}",
            f"- 命中安全相关文件：{security_files} ({ratio:.0%})",
            f"- 疑似无用文件：{useless_files}",
            f"- 类型分布：" + ", ".join(f"{k}:{v}" for k, v in type_counter.most_common(6)),
            f"- 安全维度命中：" + (", ".join(f"{k}:{v}" for k, v in cat_counter.most_common()) or "无"),
            f"- 关键词样本：" + (", ".join(k for k, _ in top_hits.most_common(8)) or "无"),
            f"- 预判：**{verdict}**",
        ])
        if sample_relevant:
            lines.append("- 安全相关样本文件：")
            for rel in sample_relevant:
                lines.append(f"  - `{rel}`")
        lines.append("")

    out.write_text("\n".join(lines), encoding="utf-8")
    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

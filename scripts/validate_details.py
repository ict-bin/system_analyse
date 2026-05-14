#!/usr/bin/env python3
"""
validate_details.py — 校验 workspace/details/ 中 JSON 文件的完整性

用法:
  python3 validate_details.py <workspace_dir> [--fix]

输出:
  workspace/details_validation.json — 校验报告
  stdout — 摘要信息

退出码:
  0 — 全部有效
  1 — 存在无效/缺失的 JSON
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REQUIRED_FIELDS = {"path", "type", "summary"}


def validate(workspace_dir: str) -> dict:
    ws = Path(workspace_dir)
    ff = ws / "filtered_files.txt"
    details_dir = ws / "details"

    if not ff.exists():
        print("[validate_details] filtered_files.txt 不存在", flush=True)
        return {"valid": 0, "missing": 0, "invalid": 0, "errors": []}

    files = [l.strip() for l in ff.read_text(encoding="utf-8").splitlines() if l.strip()]
    missing: list[str] = []
    invalid: list[dict] = []
    valid = 0

    for rel in files:
        jp = details_dir / (rel.lstrip("/") + ".json")
        if not jp.exists():
            missing.append(rel)
            continue
        try:
            data = json.loads(jp.read_text(encoding="utf-8"))
        except Exception as e:
            invalid.append({"path": rel, "error": f"JSON 解析失败: {e}"})
            continue
        # 检查必填字段
        miss_fields = REQUIRED_FIELDS - set(data.keys())
        if miss_fields:
            invalid.append({"path": rel, "error": f"缺失字段: {miss_fields}"})
            continue
        # 检查 summary 非空
        if not str(data.get("summary", "")).strip():
            invalid.append({"path": rel, "error": "summary 为空"})
            continue
        valid += 1

    result = {
        "total": len(files),
        "valid": valid,
        "missing_count": len(missing),
        "invalid_count": len(invalid),
        "missing": missing[:100],   # 最多记录100个
        "invalid": invalid[:100],
        "pass": len(missing) == 0 and len(invalid) == 0,
    }

    report_path = ws / "details_validation.json"
    report_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[validate_details] 总计 {len(files)} 个文件: "
          f"有效 {valid}，缺失 {len(missing)}，无效 {len(invalid)}", flush=True)
    if missing[:5]:
        print(f"[validate_details] 缺失示例: {missing[:5]}", flush=True)
    if invalid[:5]:
        print(f"[validate_details] 无效示例: {[e['path'] for e in invalid[:5]]}", flush=True)

    return result


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python3 validate_details.py <workspace_dir>")
        sys.exit(1)
    r = validate(sys.argv[1])
    sys.exit(0 if r["pass"] else 1)

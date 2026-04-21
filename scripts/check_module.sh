#!/bin/bash
# check_module.sh — Stage 2 本地模块一致性检查（不做全局计数）
# 用法: bash check_module.sh <target_dir> <module_workspace_dir>
# 只验证：当前模块目录下所有 files.list 中的文件在 target 中实际存在
# 不涉及全局 1157 文件计数（并行场景下全局计数不可靠）

TARGET_DIR="${1:-/data/target}"
MOD_DIR="${2:-.}"

MISSING=0
TOTAL=0
CHECKED_MODS=()

for flist in "$MOD_DIR"/files.list "$MOD_DIR"/*/files.list; do
    [ -f "$flist" ] || continue
    modname=$(basename "$(dirname "$flist")")
    CHECKED_MODS+=("$modname")
    while IFS= read -r relpath; do
        [ -z "$relpath" ] && continue
        TOTAL=$((TOTAL + 1))
        if [ ! -f "$TARGET_DIR/$relpath" ]; then
            echo "MISSING: $relpath"
            MISSING=$((MISSING + 1))
        fi
    done < "$flist"
done

echo "检查模块: ${CHECKED_MODS[*]}"
echo "文件总数: $TOTAL"
echo "Missing files: $MISSING"

if [ "$MISSING" -gt 0 ]; then
    echo "❌ 本地完整性检查失败"
    exit 1
else
    echo "✅ 本地完整性检查通过"
fi

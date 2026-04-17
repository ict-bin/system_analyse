#!/bin/bash
# check_classification.sh — 检查所有目标文件是否都已被分类
# 用法: bash check_classification.sh <target_dir> <workspace_dir>
# files.list 存相对路径；filtered_files.txt 也存相对路径

TARGET_DIR="${1:-/data/target}"
WORKSPACE_DIR="${2:-.}"

# ── 确定目标文件列表（相对路径）──
FILTERED="$WORKSPACE_DIR/filtered_files.txt"
if [ -f "$FILTERED" ]; then
    sort "$FILTERED" > /tmp/all_files.txt
    TOTAL=$(wc -l < /tmp/all_files.txt)
    echo "使用过滤文件列表: $FILTERED ($TOTAL 个)"
else
    find "$TARGET_DIR" -type f | sed "s|^${TARGET_DIR}/||" | sort > /tmp/all_files.txt
    TOTAL=$(wc -l < /tmp/all_files.txt)
fi

# ── 收集 files.list（兼容 */files.list 和 modules/*/files.list）──
# 统一去掉可能残留的绝对路径前缀
cat "$WORKSPACE_DIR"/*/files.list "$WORKSPACE_DIR"/modules/*/files.list 2>/dev/null \
    | sed '/^$/d' | sed "s|^${TARGET_DIR}/||" | sort -u > /tmp/classified_files.txt
CLASSIFIED_COUNT=$(wc -l < /tmp/classified_files.txt)

# ── 模块列表 ──
MODULES=""
for flist in "$WORKSPACE_DIR"/*/files.list "$WORKSPACE_DIR"/modules/*/files.list; do
    [ -f "$flist" ] || continue
    MOD=$(basename "$(dirname "$flist")")
    MODULES="$MODULES $MOD"
done

# ── 未分类 ──
comm -23 /tmp/all_files.txt /tmp/classified_files.txt > /tmp/missing_files.txt
MISSING_COUNT=$(wc -l < /tmp/missing_files.txt)

# ── 重复分类 ──
cat "$WORKSPACE_DIR"/*/files.list "$WORKSPACE_DIR"/modules/*/files.list 2>/dev/null \
    | sed '/^$/d' | sed "s|^${TARGET_DIR}/||" | sort | uniq -d > /tmp/dup_files.txt
DUP_COUNT=$(wc -l < /tmp/dup_files.txt)

echo "=== Classification Check ==="
echo "Target files: $TOTAL"
echo "Classified files: $CLASSIFIED_COUNT"
echo "Modules:$MODULES"
echo "Missing files: $MISSING_COUNT"
echo "Duplicate files: $DUP_COUNT"

if [ "$MISSING_COUNT" -gt 0 ]; then
    echo ""
    echo "=== MISSING FILES ==="
    head -50 /tmp/missing_files.txt
    [ "$MISSING_COUNT" -gt 50 ] && echo "... and $((MISSING_COUNT - 50)) more"
fi

if [ "$DUP_COUNT" -gt 0 ]; then
    echo ""
    echo "=== DUPLICATE FILES ==="
    head -20 /tmp/dup_files.txt
fi

echo ""
if [ "$MISSING_COUNT" -eq 0 ] && [ "$DUP_COUNT" -eq 0 ]; then
    echo "RESULT: PASS"
else
    echo "RESULT: FAIL"
fi

rm -f /tmp/all_files.txt /tmp/classified_files.txt /tmp/missing_files.txt /tmp/dup_files.txt

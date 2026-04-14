#!/bin/bash
# check_classification.sh — Judge 调用：检查所有文件是否都已分类
# 用法: bash check_classification.sh <target_dir> <workspace_dir>
# 输出: PASS / FAIL + 详情

TARGET_DIR="${1:-/data/target}"
WORKSPACE_DIR="${2:-.}"

# 收集 target 下所有文件（绝对路径）
ALL_FILES=$(find "$TARGET_DIR" -type f | sort)
TOTAL=$(echo "$ALL_FILES" | wc -l)

# 收集所有 files.list 中的文件
CLASSIFIED=""
MODULES=""
for flist in "$WORKSPACE_DIR"/*/files.list; do
    [ -f "$flist" ] || continue
    MOD=$(basename "$(dirname "$flist")")
    MODULES="$MODULES $MOD"
    while IFS= read -r line; do
        [ -n "$line" ] && CLASSIFIED="$CLASSIFIED
$line"
    done < "$flist"
done

CLASSIFIED_SORTED=$(echo "$CLASSIFIED" | sed '/^$/d' | sort -u)
CLASSIFIED_COUNT=$(echo "$CLASSIFIED_SORTED" | wc -l)

# 找未分类的文件
MISSING=$(comm -23 <(echo "$ALL_FILES") <(echo "$CLASSIFIED_SORTED"))
MISSING_COUNT=$(echo "$MISSING" | sed '/^$/d' | wc -l)

# 找重复分类的文件
DUPLICATES=$(echo "$CLASSIFIED" | sed '/^$/d' | sort | uniq -d)
DUP_COUNT=$(echo "$DUPLICATES" | sed '/^$/d' | wc -l)

echo "=== Classification Check ==="
echo "Target files: $TOTAL"
echo "Classified files: $CLASSIFIED_COUNT"
echo "Modules:$MODULES"
echo "Missing files: $MISSING_COUNT"
echo "Duplicate files: $DUP_COUNT"

if [ "$MISSING_COUNT" -gt 0 ]; then
    echo ""
    echo "=== MISSING FILES ==="
    echo "$MISSING" | sed '/^$/d' | head -50
    if [ "$MISSING_COUNT" -gt 50 ]; then
        echo "... and $((MISSING_COUNT - 50)) more"
    fi
fi

if [ "$DUP_COUNT" -gt 0 ]; then
    echo ""
    echo "=== DUPLICATE FILES ==="
    echo "$DUPLICATES" | head -20
fi

echo ""
if [ "$MISSING_COUNT" -eq 0 ] && [ "$DUP_COUNT" -eq 0 ]; then
    echo "RESULT: PASS"
else
    echo "RESULT: FAIL"
fi

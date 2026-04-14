#!/bin/bash
# check_outputs.sh — Judge 调用：检查所有子文件夹是否都有 module_report.md
# 用法: bash check_outputs.sh <workspace_dir>
# 输出: PASS / FAIL + 缺失列表

WORKSPACE_DIR="${1:-.}"

echo "=== Output Check ==="

TOTAL=0
PASS=0
FAIL_LIST=""

for mod_dir in "$WORKSPACE_DIR"/*/; do
    [ -d "$mod_dir" ] || continue
    MOD=$(basename "$mod_dir")

    # 跳过非模块目录
    [ -f "$mod_dir/files.list" ] || continue

    TOTAL=$((TOTAL + 1))
    FILE_COUNT=$(wc -l < "$mod_dir/files.list" 2>/dev/null || echo 0)

    if [ -f "$mod_dir/module_report.md" ]; then
        REPORT_SIZE=$(wc -c < "$mod_dir/module_report.md")
        if [ "$REPORT_SIZE" -gt 100 ]; then
            PASS=$((PASS + 1))
            echo "  ✅ $MOD ($FILE_COUNT files, report ${REPORT_SIZE}B)"
        else
            FAIL_LIST="$FAIL_LIST $MOD"
            echo "  ❌ $MOD ($FILE_COUNT files, report too small: ${REPORT_SIZE}B)"
        fi
    else
        FAIL_LIST="$FAIL_LIST $MOD"
        echo "  ❌ $MOD ($FILE_COUNT files, NO module_report.md)"
    fi
done

echo ""
echo "Total modules: $TOTAL"
echo "Complete: $PASS"
echo "Missing/incomplete:$FAIL_LIST"
echo ""

if [ "$PASS" -eq "$TOTAL" ] && [ "$TOTAL" -gt 0 ]; then
    echo "RESULT: PASS"
else
    echo "RESULT: FAIL"
fi

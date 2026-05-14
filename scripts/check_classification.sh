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

# ★ 减去已确认排除的文件（workspace/deleted.list）
if [ -f "$WORKSPACE_DIR/deleted.list" ]; then
    sort "$WORKSPACE_DIR/deleted.list" > /tmp/confirmed_deleted.txt
    DELETED_COUNT=$(wc -l < /tmp/confirmed_deleted.txt)
    comm -23 /tmp/all_files.txt /tmp/confirmed_deleted.txt > /tmp/all_files_adj.txt
    mv /tmp/all_files_adj.txt /tmp/all_files.txt
    TOTAL=$(wc -l < /tmp/all_files.txt)
    echo "已排除确认删除文件: $DELETED_COUNT 个，剩余工作集: $TOTAL 个"
fi

# ★ 减去提议删除文件（workspace/deleted/files.list，S1 Worker 暂存，待 Judge 审核）
if [ -f "$WORKSPACE_DIR/deleted/files.list" ]; then
    sort "$WORKSPACE_DIR/deleted/files.list" > /tmp/proposed_deleted.txt
    PROPOSED_COUNT=$(wc -l < /tmp/proposed_deleted.txt)
    comm -23 /tmp/all_files.txt /tmp/proposed_deleted.txt > /tmp/all_files_adj.txt
    mv /tmp/all_files_adj.txt /tmp/all_files.txt
    TOTAL=$(wc -l < /tmp/all_files.txt)
    echo "提议删除 (deleted/files.list): $PROPOSED_COUNT 个，调整后工作集: $TOTAL 个"
    rm -f /tmp/proposed_deleted.txt
fi

# ── 收集 files.list（兼容 */files.list 和 modules/*/files.list，排除 deleted/ 和 recover/）──
{
  for flist in "$WORKSPACE_DIR"/*/files.list "$WORKSPACE_DIR"/modules/*/files.list; do
    [ -f "$flist" ] || continue
    _d=$(basename "$(dirname "$flist")")
    [ "$_d" = "deleted" ] && continue
    [ "$_d" = "recover" ] && continue
    cat "$flist"
  done
} | sed '/^$/d' | sed "s|^${TARGET_DIR}/||" | sort -u > /tmp/classified_files.txt
CLASSIFIED_COUNT=$(wc -l < /tmp/classified_files.txt)

# ── 模块列表（排除 deleted/ recover/）──
MODULES=""
for flist in "$WORKSPACE_DIR"/*/files.list "$WORKSPACE_DIR"/modules/*/files.list; do
    [ -f "$flist" ] || continue
    _d=$(basename "$(dirname "$flist")")
    [ "$_d" = "deleted" ] && continue
    [ "$_d" = "recover" ] && continue
    MOD=$(basename "$(dirname "$flist")")
    MODULES="$MODULES $MOD"
done

# ── 未分类 ──
comm -23 /tmp/all_files.txt /tmp/classified_files.txt > /tmp/missing_files.txt
MISSING_COUNT=$(wc -l < /tmp/missing_files.txt)

# ── 重复分类（排除 deleted/ recover/）──
{
  for flist in "$WORKSPACE_DIR"/*/files.list "$WORKSPACE_DIR"/modules/*/files.list; do
    [ -f "$flist" ] || continue
    _d=$(basename "$(dirname "$flist")")
    [ "$_d" = "deleted" ] && continue
    [ "$_d" = "recover" ] && continue
    cat "$flist"
  done
} | sed '/^$/d' | sed "s|^${TARGET_DIR}/||" | sort | uniq -d > /tmp/dup_files.txt
DUP_COUNT=$(wc -l < /tmp/dup_files.txt)

echo "=== Classification Check ==="
echo "Target files: $TOTAL"
echo "Classified files: $CLASSIFIED_COUNT"
echo "Modules:$MODULES"
echo "Missing files: $MISSING_COUNT"
echo "Duplicate files: $DUP_COUNT"

# 诊断：missing 过多时检查目录结构，给 Worker 明确反馈
if [ "$MISSING_COUNT" -gt 100 ]; then
    echo ""
    echo "=== DIAGNOSIS: 目录结构异常 ==="
    if [ -d "$WORKSPACE_DIR/modules" ] && [ "$(ls -A "$WORKSPACE_DIR/modules" 2>/dev/null)" ]; then
        echo "modules/ 已存在且非空"
    else
        echo "⚠ 错误原因: modules/ 目录不存在或为空！"
        echo "  Worker 可能将模块写到了错误位置，当前 workspace 根目录内容:"
        ls "$WORKSPACE_DIR/" | grep -vE '^filtered_files|^keywords|^\.'
        echo "  必须修正: 将所有模块目录移入 modules/ 下"
        echo "  正确结构: modules/<模块名>/files.list"
    fi
fi

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

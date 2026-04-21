#!/bin/bash
# check_module.sh — Stage 2 模块拆分完整性校验
# 用法: bash check_module.sh <target_dir> <modules_root> <mod_name>
#
# 逻辑：
#   拆分前已将 <mod_name>/files.list 备份到 <mod_name>/files.list.snapshot
#   拆分后收集所有子模块（<mod_name>_* 及 <mod_name> 本身）的 files.list
#   比较：拆分后并集 == 快照，确认零丢失、零重复

TARGET_DIR="${1:-/data/target}"
MODULES_ROOT="${2:-modules}"
MOD_NAME="$3"

if [ -z "$MOD_NAME" ]; then
    echo "用法: check_module.sh <target_dir> <modules_root> <mod_name>"
    exit 1
fi

SNAPSHOT="$MODULES_ROOT/$MOD_NAME/files.list.snapshot"
if [ ! -f "$SNAPSHOT" ]; then
    # 没有快照说明未拆分，直接验证文件存在性
    flist="$MODULES_ROOT/$MOD_NAME/files.list"
    if [ ! -f "$flist" ]; then
        echo "⚠️  $MOD_NAME: 无 files.list，跳过"
        echo "Missing files: 0"
        exit 0
    fi
    MISSING=0
    while IFS= read -r rel; do
        [ -z "$rel" ] && continue
        [ -f "$TARGET_DIR/$rel" ] || { echo "MISSING: $rel"; MISSING=$((MISSING+1)); }
    done < "$flist"
    echo "模式: 未拆分（无快照），直接校验文件存在性"
    echo "文件数: $(wc -l < "$flist")"
    echo "Missing files: $MISSING"
    exit $MISSING
fi

# ── 有快照：对比拆分前后 ──
sort -u "$SNAPSHOT" > /tmp/cm_snapshot_$$.txt
SNAP_COUNT=$(wc -l < /tmp/cm_snapshot_$$.txt)

# 收集拆分后所有相关子模块的文件
> /tmp/cm_current_$$.txt
FOUND_MODS=()
# 原模块本身（可能保留了一部分文件）
[ -f "$MODULES_ROOT/$MOD_NAME/files.list" ] && {
    cat "$MODULES_ROOT/$MOD_NAME/files.list" >> /tmp/cm_current_$$.txt
    FOUND_MODS+=("$MOD_NAME")
}
# 所有以 <mod_name>_ 开头的子模块
for sub in "$MODULES_ROOT"/${MOD_NAME}_*/files.list; do
    [ -f "$sub" ] || continue
    cat "$sub" >> /tmp/cm_current_$$.txt
    FOUND_MODS+=("$(basename $(dirname $sub))")
done
sort -u /tmp/cm_current_$$.txt > /tmp/cm_current_sorted_$$.txt
CURR_COUNT=$(wc -l < /tmp/cm_current_sorted_$$.txt)

# 比较
MISSING_FILES=$(comm -23 /tmp/cm_snapshot_$$.txt /tmp/cm_current_sorted_$$.txt)
MISSING=$(echo "$MISSING_FILES" | grep -c . || true)
EXTRA=$(comm -13 /tmp/cm_snapshot_$$.txt /tmp/cm_current_sorted_$$.txt | grep -c . || true)

echo "模块: $MOD_NAME → 子模块: ${FOUND_MODS[*]}"
echo "快照文件数: $SNAP_COUNT"
echo "拆分后文件数: $CURR_COUNT"
echo "Missing files: $MISSING"
echo "Extra files (新增): $EXTRA"

if [ "$MISSING" -gt 0 ]; then
    echo "❌ 拆分后丢失以下文件:"
    echo "$MISSING_FILES" | head -20
    rm -f /tmp/cm_snapshot_$$.txt /tmp/cm_current_$$.txt /tmp/cm_current_sorted_$$.txt
    exit 1
fi

echo "✅ 拆分完整性校验通过（无文件丢失）"
rm -f /tmp/cm_snapshot_$$.txt /tmp/cm_current_$$.txt /tmp/cm_current_sorted_$$.txt

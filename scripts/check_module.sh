#!/bin/bash
# check_module.sh — Stage 2 拆分完整性校验
# 用法: bash check_module.sh <target_dir> <modules_root> <mod_name>
#
# 快照保存在 <modules_root>/../.s2_snapshots/<mod_name>.snapshot
# 两种情况：
# A) 有快照（已尝试拆分）：对比快照 vs 所有 <mod_name>_* 子模块（原模块本身若存在也计入）
# B) 无快照（未拆分）：直接验证 <mod_name>/files.list 文件存在性

TARGET_DIR="${1:-/data/target}"
MODULES_ROOT="${2:-modules}"
MOD_NAME="$3"

if [ -z "$MOD_NAME" ]; then
    echo "用法: check_module.sh <target_dir> <modules_root> <mod_name>"
    exit 1
fi

# 快照在 workspace/.s2_snapshots/
WORKSPACE=$(dirname "$MODULES_ROOT")
SNAPSHOT="$WORKSPACE/.s2_snapshots/$MOD_NAME.snapshot"

# ── 情况 B：无快照，原模块未拆分 ──
if [ ! -f "$SNAPSHOT" ]; then
    flist="$MODULES_ROOT/$MOD_NAME/files.list"
    if [ ! -f "$flist" ]; then
        echo "❌ $MOD_NAME: 无快照且无 files.list（模块可能已被错误删除）"
        echo "Missing files: -1"
        exit 1
    fi
    MISSING=0
    TOTAL=0
    while IFS= read -r rel; do
        [ -z "$rel" ] && continue
        TOTAL=$((TOTAL + 1))
        [ -f "$TARGET_DIR/$rel" ] || { echo "MISSING: $rel"; MISSING=$((MISSING+1)); }
    done < "$flist"
    echo "模式: 未拆分（无快照），验证文件存在性"
    echo "文件数: $TOTAL"
    echo "Missing files: $MISSING"
    [ "$MISSING" -eq 0 ] && echo "✅ 通过" || echo "❌ 失败"
    exit $MISSING
fi

# ── 情况 A：有快照，对比拆分结果 ──
sort -u "$SNAPSHOT" > /tmp/cm_snap_$$.txt
SNAP_COUNT=$(wc -l < /tmp/cm_snap_$$.txt)

# 收集所有子模块（原模块本身 + <mod_name>_* 前缀子模块）
> /tmp/cm_curr_$$.txt
FOUND_MODS=()

# 原模块目录（拆分后可能删除，也可能保留空壳）
if [ -f "$MODULES_ROOT/$MOD_NAME/files.list" ]; then
    grep -v "^$" "$MODULES_ROOT/$MOD_NAME/files.list" >> /tmp/cm_curr_$$.txt 2>/dev/null || true
    FOUND_MODS+=("$MOD_NAME")
fi

# 所有以 <mod_name>_ 开头的子模块
for sub in "$MODULES_ROOT"/${MOD_NAME}_*/files.list; do
    [ -f "$sub" ] || continue
    grep -v "^$" "$sub" >> /tmp/cm_curr_$$.txt
    FOUND_MODS+=("$(basename "$(dirname "$sub")")")
done

sort -u /tmp/cm_curr_$$.txt > /tmp/cm_curr_sorted_$$.txt
CURR_COUNT=$(wc -l < /tmp/cm_curr_sorted_$$.txt)

MISSING_LIST=$(comm -23 /tmp/cm_snap_$$.txt /tmp/cm_curr_sorted_$$.txt)
MISSING=$(printf '%s' "$MISSING_LIST" | grep -c '[^[:space:]]' || true)

echo "快照文件数: $SNAP_COUNT"
echo "子模块: ${FOUND_MODS[*]}"
echo "拆分后总文件数: $CURR_COUNT"
echo "Missing files: $MISSING"

if [ "$MISSING" -gt 0 ]; then
    echo "❌ 丢失文件:"
    echo "$MISSING_LIST" | head -20
fi

rm -f /tmp/cm_snap_$$.txt /tmp/cm_curr_$$.txt /tmp/cm_curr_sorted_$$.txt
[ "$MISSING" -eq 0 ] && echo "✅ 通过" || echo "❌ 失败"
exit $MISSING

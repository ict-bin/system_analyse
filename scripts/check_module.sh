#!/bin/bash
# check_module.sh — Stage 2 拆分/迁移完整性校验
# 用法: bash check_module.sh <target_dir> <modules_root> <mod_name>
#
# 快照在 <modules_root>/../.s2_snapshots/<mod_name>.snapshot
# 校验逻辑:
#   快照文件 = (当前模块+子模块) ∪ (已迁移到其他模块的文件)
#   真正丢失 = 快照文件 - 所有模块文件之并集

TARGET_DIR="${1:-/data/target}"
MODULES_ROOT="${2:-modules}"
MOD_NAME="$3"

if [ -z "$MOD_NAME" ]; then
    echo "用法: check_module.sh <target_dir> <modules_root> <mod_name>"
    exit 1
fi

WORKSPACE=$(dirname "$MODULES_ROOT")
SNAPSHOT="$WORKSPACE/.s2_snapshots/$MOD_NAME.snapshot"

# ── 无快照：原模块未处理，验证文件存在性 ──
if [ ! -f "$SNAPSHOT" ]; then
    flist="$MODULES_ROOT/$MOD_NAME/files.list"
    if [ ! -f "$flist" ]; then
        echo "❌ $MOD_NAME: 无快照且无 files.list"
        echo "Missing files: -1"
        exit 1
    fi
    MISSING=0; TOTAL=0
    while IFS= read -r rel; do
        [ -z "$rel" ] && continue
        TOTAL=$((TOTAL+1))
        [ -f "$TARGET_DIR/$rel" ] || { echo "MISSING: $rel"; MISSING=$((MISSING+1)); }
    done < "$flist"
    echo "模式: 未处理（无快照）"
    echo "文件数: $TOTAL  Missing files: $MISSING"
    [ "$MISSING" -eq 0 ] && echo "✅ 通过" || echo "❌ 失败"
    exit $MISSING
fi

# ── 有快照：三步校验 ──
sort -u "$SNAPSHOT" > /tmp/cm_snap_$$.txt
SNAP_COUNT=$(wc -l < /tmp/cm_snap_$$.txt)

# Step 1: 收集本模块 + split 候选子模块 + split/_merge_to/* + 本模块 deleted/
> /tmp/cm_local_$$.txt
[ -f "$MODULES_ROOT/$MOD_NAME/files.list" ] && \
    grep -v "^$" "$MODULES_ROOT/$MOD_NAME/files.list" >> /tmp/cm_local_$$.txt 2>/dev/null
for sub in "$MODULES_ROOT/$MOD_NAME"/split/*/files.list; do
    [ -f "$sub" ] && grep -v "^$" "$sub" >> /tmp/cm_local_$$.txt
done
for sub in "$MODULES_ROOT/$MOD_NAME"/split/_merge_to/*/files.list; do
    [ -f "$sub" ] && grep -v "^$" "$sub" >> /tmp/cm_local_$$.txt
done
# ★ 将 deleted/ 子文件夹中的文件也视为已处理（待归档的排除文件）
if [ -f "$MODULES_ROOT/$MOD_NAME/deleted/files.list" ]; then
    grep -v "^$" "$MODULES_ROOT/$MOD_NAME/deleted/files.list" >> /tmp/cm_local_$$.txt
    echo "  + deleted/ 提议排除: $(wc -l < "$MODULES_ROOT/$MOD_NAME/deleted/files.list") 个文件"
fi
sort -u /tmp/cm_local_$$.txt > /tmp/cm_local_sorted_$$.txt
LOCAL_COUNT=$(wc -l < /tmp/cm_local_sorted_$$.txt)

# Step 2: 找出在快照中但不在本模块/split草稿中的文件（可能已迁移）
comm -23 /tmp/cm_snap_$$.txt /tmp/cm_local_sorted_$$.txt > /tmp/cm_maybe_migrated_$$.txt
MAYBE_MIGRATED=$(wc -l < /tmp/cm_maybe_migrated_$$.txt)

# Step 3: 在所有其他模块中搜索这些文件（验证是否真的迁移了）
TRULY_MISSING=0
MIGRATED_OK=0
if [ "$MAYBE_MIGRATED" -gt 0 ]; then
    # 收集所有其他模块的文件
    cat "$MODULES_ROOT"/*/files.list 2>/dev/null \
        | grep -v "^$" | sort -u > /tmp/cm_all_mods_$$.txt

    # ★ 将 workspace/deleted.list（已确认排除）也纳入计算
    WORKSPACE=$(dirname "$MODULES_ROOT")
    if [ -f "$WORKSPACE/deleted.list" ]; then
        sort -u "$WORKSPACE/deleted.list" >> /tmp/cm_all_mods_$$.txt
        sort -u /tmp/cm_all_mods_$$.txt -o /tmp/cm_all_mods_$$.txt
    fi

    while IFS= read -r rel; do
        [ -z "$rel" ] && continue
        if grep -qxF "$rel" /tmp/cm_all_mods_$$.txt; then
            MIGRATED_OK=$((MIGRATED_OK+1))
        else
            echo "MISSING: $rel"
            TRULY_MISSING=$((TRULY_MISSING+1))
        fi
    done < /tmp/cm_maybe_migrated_$$.txt
fi

echo "快照文件数: $SNAP_COUNT"
echo "本模块+split草稿: $LOCAL_COUNT"
echo "已迁移到其他模块: $MIGRATED_OK"
echo "Missing files: $TRULY_MISSING"

rm -f /tmp/cm_snap_$$.txt /tmp/cm_local_$$.txt /tmp/cm_local_sorted_$$.txt \
      /tmp/cm_maybe_migrated_$$.txt /tmp/cm_all_mods_$$.txt

[ "$TRULY_MISSING" -eq 0 ] && echo "✅ 通过" || echo "❌ 失败"
exit $TRULY_MISSING

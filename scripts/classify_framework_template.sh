#!/usr/bin/env bash
# ================================================================
# classify_framework.sh — 由系统生成，基础设施部分请勿修改
#
# 用法：
#   bash classify_framework.sh          # 全量分类（清空重建 modules/）
#   bash classify_framework.sh --check  # 仅验证覆盖率，不重跑分类（快）
#
# 你的任务：仅在 ↓↓↓ 和 ↑↑↑ 标记之间填写 classify_file() 函数体
# ================================================================
WORKSPACE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="$WORKSPACE/filtered_files.txt"
MODULES_DIR="$WORKSPACE/modules"
DELETED_DIR="$WORKSPACE/deleted"

# ================================================================
# 【填写区】classify_file() 函数
# 输入：$1 = 文件相对路径（来自 filtered_files.txt 的单行）
# 输出：echo 一个模块名（小写+下划线，如 bgp、tls、container）
#       特殊值 "deleted" → 写入 deleted/files.list，由 Judge 审核
# ================================================================
classify_file() {
    local f="$1"
    # ↓↓↓ 在此填写分类逻辑（仅改此函数体）↓↓↓

    echo "other"

    # ↑↑↑ 在此填写分类逻辑 ↑↑↑
}
# ================================================================
# 以下为基础设施代码（请勿修改）
# ================================================================

_sa_run() {
    if [[ ! -f "$SOURCE" ]]; then
        echo "[ERROR] filtered_files.txt 不存在：$SOURCE" >&2
        return 1
    fi

    # 清空并重建（幂等，每次全量运行都从零开始）
    rm -rf "$MODULES_DIR" "$DELETED_DIR"
    mkdir -p "$MODULES_DIR" "$DELETED_DIR"

    local total=0 write_fail=0
    while IFS= read -r file || [[ -n "$file" ]]; do
        [[ -z "$file" ]] && continue
        ((total++))

        # 调用分类函数；函数执行出错时降级为 "other"
        local mod
        mod=$(classify_file "$file" 2>/dev/null) || mod="other"

        # 规范化：去首尾空白、转小写、空格→下划线、空值/null→other
        mod="${mod#"${mod%%[![:space:]]*}"}"
        mod="${mod%"${mod##*[![:space:]]}"}"
        mod="${mod,,}"
        mod="${mod// /_}"
        [[ -z "$mod" || "$mod" == "null" ]] && mod="other"

        if [[ "$mod" == "deleted" ]]; then
            printf '%s\n' "$file" >> "$DELETED_DIR/files.list" \
                || { echo "[WARN] 写入 deleted/files.list 失败：$file" >&2; ((write_fail++)); }
        else
            mkdir -p "$MODULES_DIR/$mod"
            printf '%s\n' "$file" >> "$MODULES_DIR/$mod/files.list" \
                || { echo "[WARN] 写入 $mod/files.list 失败：$file" >&2; ((write_fail++)); }
        fi
    done < "$SOURCE"

    # 去重排序（静默，不产生额外 stderr）
    while IFS= read -r -d '' flist; do
        sort -u "$flist" -o "$flist" 2>/dev/null
    done < <(find "$MODULES_DIR" "$DELETED_DIR" -name "files.list" -print0 2>/dev/null)

    echo "[classify] 处理完成：total=$total  write_fail=$write_fail"
    _sa_report
}

_sa_report() {
    local src_total classified deleted missing_count missing_list

    src_total=$(wc -l < "$SOURCE" 2>/dev/null || echo 0)
    classified=$(cat "$MODULES_DIR"/*/files.list 2>/dev/null | sort -u | wc -l || echo 0)
    deleted=0
    [[ -f "$DELETED_DIR/files.list" ]] && deleted=$(wc -l < "$DELETED_DIR/files.list")

    echo ""
    echo "=== 分类统计 ==="
    while IFS= read -r -d '' d; do
        [[ -f "$d/files.list" ]] || continue
        local cnt
        cnt=$(wc -l < "$d/files.list" 2>/dev/null || echo 0)
        [[ $cnt -gt 0 ]] && printf "  %-30s %d\n" "$(basename "$d"):" "$cnt"
    done < <(find "$MODULES_DIR" -mindepth 1 -maxdepth 1 -type d -print0 2>/dev/null | sort -z)
    [[ $deleted -gt 0 ]] && printf "  %-30s %d\n" "[deleted]:" "$deleted"

    echo ""
    printf "总文件: %d  已分类: %d  deleted: %d  合计: %d\n" \
        "$src_total" "$classified" "$deleted" "$((classified + deleted))"

    # 覆盖率检查（无 stderr 噪音）
    missing_list=$(comm -23 \
        <(sort "$SOURCE" 2>/dev/null) \
        <(cat "$MODULES_DIR"/*/files.list "$DELETED_DIR/files.list" 2>/dev/null | sort -u) \
        2>/dev/null)
    missing_count=$(printf '%s' "$missing_list" | grep -c '' 2>/dev/null || echo 0)
    [[ -z "$missing_list" ]] && missing_count=0

    echo ""
    if [[ $missing_count -eq 0 ]]; then
        echo "✅ 覆盖率 100% — 分类完成，请输出 <result> 结束"
    else
        echo "❌ 遗漏 $missing_count 个文件（全部列出）："
        printf '%s\n' "$missing_list"
        echo ""
        if [[ $missing_count -le 20 ]]; then
            echo "提示：遗漏较少（≤20），建议逐条 append 后用 --check 验证："
            echo "  echo 'path/to/file' >> modules/<模块名>/files.list"
            echo "  bash classify_framework.sh --check"
        else
            echo "提示：遗漏较多（>20），建议改进 classify_file() 后重新运行："
            echo "  bash classify_framework.sh"
        fi
    fi
}

_sa_check() {
    if [[ ! -f "$SOURCE" ]]; then
        echo "[ERROR] filtered_files.txt 不存在：$SOURCE" >&2
        return 1
    fi
    if [[ ! -d "$MODULES_DIR" ]]; then
        echo "[ERROR] modules/ 目录不存在，请先运行 bash classify_framework.sh 完成初始分类" >&2
        return 1
    fi

    local src_total classified deleted missing_count missing_list

    src_total=$(wc -l < "$SOURCE" 2>/dev/null || echo 0)
    classified=$(cat "$MODULES_DIR"/*/files.list 2>/dev/null | sort -u | wc -l || echo 0)
    deleted=0
    [[ -f "$DELETED_DIR/files.list" ]] && deleted=$(wc -l < "$DELETED_DIR/files.list")

    missing_list=$(comm -23 \
        <(sort "$SOURCE" 2>/dev/null) \
        <(cat "$MODULES_DIR"/*/files.list "$DELETED_DIR/files.list" 2>/dev/null | sort -u) \
        2>/dev/null)
    missing_count=$(printf '%s' "$missing_list" | grep -c '' 2>/dev/null || echo 0)
    [[ -z "$missing_list" ]] && missing_count=0

    printf "当前：总文件=%d  已分类=%d  deleted=%d\n" "$src_total" "$classified" "$deleted"

    if [[ $missing_count -eq 0 ]]; then
        echo "✅ 覆盖率 100% — 分类完成，请输出 <result> 结束"
        return 0
    else
        echo "❌ 遗漏 $missing_count 个文件（全部列出）："
        printf '%s\n' "$missing_list"
        return 1
    fi
}

# ── 入口 ─────────────────────────────────────────────────────────
case "${1:-}" in
    --check) _sa_check ;;
    *)       _sa_run   ;;
esac

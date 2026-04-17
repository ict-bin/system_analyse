#!/bin/bash
# prescan_files.sh — 用 Worker 生成的关键词批量扫描文件
# 用法: bash prescan_files.sh <target_dir> <workspace_dir>
# 前提: workspace_dir/keywords.txt 已存在（由 Worker 生成）
# 输出:
#   keyword_summary.txt       关键词→文件数统计
#   unknown_sample.txt        无法识别文件的抽样
#   prescan/<keyword>.list    每个关键词对应的文件列表

set -e
TARGET_DIR="${1:-/data/target}"
WORKSPACE="${2:-.}"
KEYWORDS_FILE="$WORKSPACE/keywords.txt"

if [ ! -f "$KEYWORDS_FILE" ]; then
    echo "ERROR: $KEYWORDS_FILE 不存在，跳过预扫描"
    exit 1
fi

mkdir -p "$WORKSPACE/prescan"

TOTAL=$(find "$TARGET_DIR" -type f | wc -l)
echo "=== 预扫描 ==="
echo "文件总数: $TOTAL"

# 读取关键词列表，构造 grep 正则
KEYWORDS=$(cat "$KEYWORDS_FILE" | tr '\n' '|' | sed 's/|$//')
echo "关键词: $(wc -l < "$KEYWORDS_FILE") 个"
echo "正则: $KEYWORDS"

# ── 判断扫描模式 ──
# 抽样 20 个文件名，检查是否含关键词
SEMANTIC=0
for f in $(find "$TARGET_DIR" -type f | head -20); do
    name=$(basename "$f" | tr '[:upper:]' '[:lower:]')
    if echo "$name" | grep -qiE "$KEYWORDS"; then
        SEMANTIC=$((SEMANTIC + 1))
    fi
done
echo "文件名语义: $SEMANTIC/20"

if [ "$SEMANTIC" -ge 10 ]; then
    SCAN_MODE="filename"
else
    SCAN_MODE="content"
fi
echo "模式: $SCAN_MODE"

# ── 批量扫描 ──
SCANNED=0
find "$TARGET_DIR" -type f | while IFS= read -r filepath; do
    SCANNED=$((SCANNED + 1))
    [ $((SCANNED % 5000)) -eq 0 ] && echo "  进度: $SCANNED / $TOTAL" >&2

    kw=""

    # 先按文件名匹配
    name=$(basename "$filepath" | tr '[:upper:]' '[:lower:]')
    kw=$(echo "$name" | grep -oiE "$KEYWORDS" | head -1 | tr '[:upper:]' '[:lower:]')

    # 文件名无匹配 → 扫描内容
    if [ -z "$kw" ] && [ "$SCAN_MODE" = "content" ]; then
        ftype=$(file -b "$filepath" 2>/dev/null)
        if echo "$ftype" | grep -qi "text\|script\|xml\|json\|ascii"; then
            kw=$(head -30 "$filepath" 2>/dev/null | grep -oiE "$KEYWORDS" | head -1 | tr '[:upper:]' '[:lower:]')
        fi
        if [ -z "$kw" ]; then
            kw=$(strings "$filepath" 2>/dev/null | head -50 | grep -oiE "$KEYWORDS" | head -1 | tr '[:upper:]' '[:lower:]')
        fi
    fi

    # 输出相对路径（去掉 TARGET_DIR 前缀）
    relpath=$(echo "$filepath" | sed "s|^${TARGET_DIR}/||")

    if [ -n "$kw" ]; then
        echo "$relpath" >> "$WORKSPACE/prescan/$kw.list"
    else
        echo "$relpath" >> "$WORKSPACE/prescan/unknown.list"
    fi
done

# ── 生成摘要 ──
{
    echo "=== 预扫描摘要 ==="
    echo "目标: $TARGET_DIR"
    echo "总数: $TOTAL"
    echo "模式: $SCAN_MODE"
    echo ""
    echo "关键词 | 文件数"
    echo "-------|-------"

    CLASSIFIED=0
    for listfile in "$WORKSPACE"/prescan/*.list; do
        [ -f "$listfile" ] || continue
        kw=$(basename "$listfile" .list)
        count=$(wc -l < "$listfile")
        CLASSIFIED=$((CLASSIFIED + count))
        echo "$kw | $count"
    done | sort -t'|' -k2 -rn

    echo ""
    echo "已分类: $CLASSIFIED / $TOTAL"
    UNKNOWN=0
    [ -f "$WORKSPACE/prescan/unknown.list" ] && UNKNOWN=$(wc -l < "$WORKSPACE/prescan/unknown.list")
    echo "未识别: $UNKNOWN"
} > "$WORKSPACE/keyword_summary.txt"

# ── 未识别文件抽样 ──
if [ -f "$WORKSPACE/prescan/unknown.list" ] && [ "$(wc -l < "$WORKSPACE/prescan/unknown.list")" -gt 0 ]; then
    {
        echo "=== 未识别文件抽样（前 15 个）==="
        head -15 "$WORKSPACE/prescan/unknown.list" | while IFS= read -r f; do
            echo "--- $(basename "$f") ---"
            ftype=$(file -b "$f" 2>/dev/null | head -1)
            echo "类型: $ftype"
            if echo "$ftype" | grep -qi "text\|script\|ascii"; then
                head -5 "$f" 2>/dev/null
            else
                strings "$f" 2>/dev/null | head -5
            fi
            echo ""
        done
    } > "$WORKSPACE/unknown_sample.txt"
fi

echo ""
echo "=== 完成 ==="
cat "$WORKSPACE/keyword_summary.txt"

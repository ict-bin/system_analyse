#!/bin/bash
# prescan_files.sh — 用 Worker 生成的关键词批量扫描文件
# 用法: bash prescan_files.sh <target_dir> <workspace_dir>
# 优先使用 workspace_dir/filtered_files.txt（过滤后的文件列表）
# 输出:
#   keyword_summary.txt       关键词→文件数统计
#   prescan/<keyword>.list    每个关键词对应的文件列表（相对路径）

set -e
TARGET_DIR="${1:-/data/target}"
WORKSPACE="${2:-.}"
KEYWORDS_FILE="$WORKSPACE/keywords.txt"

if [ ! -f "$KEYWORDS_FILE" ]; then
    echo "ERROR: $KEYWORDS_FILE 不存在，跳过预扫描"
    exit 1
fi

mkdir -p "$WORKSPACE/prescan"

# ── 确定文件列表来源 ──
FILTERED="$WORKSPACE/filtered_files.txt"
if [ -f "$FILTERED" ]; then
    TOTAL=$(wc -l < "$FILTERED")
    echo "=== 预扫描（过滤列表：$TOTAL 个文件，filename 模式）==="
    USE_FILTERED=1
    SCAN_MODE="filename"   # 二进制ELF文件 strings 无意义且极慢，强制用文件名
else
    TOTAL=$(find "$TARGET_DIR" -type f | wc -l)
    echo "=== 预扫描（全量：$TOTAL 个文件）==="
    USE_FILTERED=0
    # 抽样判断模式
    SEMANTIC=0
    for rel in $(find "$TARGET_DIR" -type f | head -20 | sed "s|^${TARGET_DIR}/||"); do
        name=$(basename "$rel" | tr '[:upper:]' '[:lower:]')
        echo "$name" | grep -qiE "$(tr '\n' '|' < "$KEYWORDS_FILE" | sed 's/|$//')" \
            && SEMANTIC=$((SEMANTIC + 1))
    done
    [ "$SEMANTIC" -ge 10 ] && SCAN_MODE="filename" || SCAN_MODE="content"
    echo "文件名语义: $SEMANTIC/20，模式: $SCAN_MODE"
fi

# ── 读取关键词（构建 pattern）──
KEYWORDS=$(tr '\n' '|' < "$KEYWORDS_FILE" | sed 's/|$//')
echo "关键词: $(wc -l < "$KEYWORDS_FILE") 个，pattern 长度: ${#KEYWORDS}"

# ── 批量扫描（filename 模式：纯 grep，极快）──
echo "  扫描中..."

if [ "$USE_FILTERED" = "1" ]; then
    INPUT="$FILTERED"
else
    INPUT="/tmp/prescan_input_$$.txt"
    find "$TARGET_DIR" -type f | sed "s|^${TARGET_DIR}/||" > "$INPUT"
fi

# 按关键词逐个 grep 文件名（不读文件内容）
while IFS= read -r kw; do
    [ -z "$kw" ] && continue
    # grep 文件名中含此关键词的行，追加到对应 list
    grep -i "$kw" "$INPUT" >> "$WORKSPACE/prescan/$kw.list" 2>/dev/null || true
done < "$KEYWORDS_FILE"

# content 模式额外扫描内容（全量时才用）
if [ "$SCAN_MODE" = "content" ] && [ "$USE_FILTERED" != "1" ]; then
    echo "  content 扫描（strings）中..."
    # 找出尚未被任何关键词匹配的文件
    cat "$WORKSPACE"/prescan/*.list 2>/dev/null | sort -u > /tmp/prescan_matched_$$.txt
    sort "$INPUT" > /tmp/prescan_all_$$.txt
    comm -23 /tmp/prescan_all_$$.txt /tmp/prescan_matched_$$.txt > /tmp/prescan_unmatched_$$.txt
    rm -f /tmp/prescan_matched_$$.txt /tmp/prescan_all_$$.txt

    while IFS= read -r relpath; do
        [ -z "$relpath" ] && continue
        kw=$(strings "$TARGET_DIR/$relpath" 2>/dev/null | head -50 \
             | grep -oiE "$KEYWORDS" | head -1 | tr '[:upper:]' '[:lower:]')
        if [ -n "$kw" ]; then
            echo "$relpath" >> "$WORKSPACE/prescan/$kw.list"
        else
            echo "$relpath" >> "$WORKSPACE/prescan/unknown.list"
        fi
    done < /tmp/prescan_unmatched_$$.txt
    rm -f /tmp/prescan_unmatched_$$.txt
fi

# 已被关键词匹配但未归入 unknown 的未匹配文件
if [ "$SCAN_MODE" = "filename" ]; then
    cat "$WORKSPACE"/prescan/*.list 2>/dev/null | sort -u > /tmp/prescan_matched_$$.txt
    sort "$INPUT" > /tmp/prescan_all_$$.txt
    comm -23 /tmp/prescan_all_$$.txt /tmp/prescan_matched_$$.txt \
        >> "$WORKSPACE/prescan/unknown.list" 2>/dev/null || true
    rm -f /tmp/prescan_matched_$$.txt /tmp/prescan_all_$$.txt
fi

[ "$USE_FILTERED" != "1" ] && rm -f "$INPUT"

# 去重
for f in "$WORKSPACE"/prescan/*.list; do
    [ -f "$f" ] && sort -u "$f" -o "$f"
done

# ── 生成摘要 ──
{
    echo "=== 预扫描摘要 ==="
    echo "来源: $([ "$USE_FILTERED" = "1" ] && echo "filtered_files.txt" || echo "全量")"
    echo "总数: $TOTAL，模式: $SCAN_MODE"
    echo ""
    echo "关键词 | 文件数"
    echo "-------|-------"
    for listfile in "$WORKSPACE"/prescan/*.list; do
        [ -f "$listfile" ] || continue
        kw=$(basename "$listfile" .list)
        count=$(wc -l < "$listfile")
        [ "$count" -gt 0 ] && echo "$kw | $count"
    done | sort -t'|' -k2 -rn
    echo ""
    UNKNOWN=0
    [ -f "$WORKSPACE/prescan/unknown.list" ] && UNKNOWN=$(wc -l < "$WORKSPACE/prescan/unknown.list")
    echo "未识别: $UNKNOWN"
} > "$WORKSPACE/keyword_summary.txt"

echo ""
echo "=== 完成 ==="
cat "$WORKSPACE/keyword_summary.txt"

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
    echo "=== 预扫描（过滤列表：$TOTAL 个文件）==="
    USE_FILTERED=1
else
    TOTAL=$(find "$TARGET_DIR" -type f | wc -l)
    echo "=== 预扫描（全量：$TOTAL 个文件）==="
    USE_FILTERED=0
fi

# ── 读取关键词 ──
KEYWORDS=$(tr '\n' '|' < "$KEYWORDS_FILE" | sed 's/|$//')
KW_COUNT=$(wc -l < "$KEYWORDS_FILE")
echo "关键词: $KW_COUNT 个"

# ── 构建文件列表 ──
if [ "$USE_FILTERED" = "1" ]; then
    INPUT="$FILTERED"
else
    INPUT="/tmp/prescan_input_$$.txt"
    find "$TARGET_DIR" -type f | sed "s|^${TARGET_DIR}/||" > "$INPUT"
fi

# ── 扫描函数：文件名 + ELF前64KB符号（快速）──
# 对 ELF 二进制：只读前 65536 字节，捕获段头+符号表
# 对文本文件：直接读前30行
scan_file() {
    local relpath="$1"
    local fullpath="$TARGET_DIR/$relpath"
    local name kw

    name=$(basename "$relpath" | tr '[:upper:]' '[:lower:]')

    # 第1步：文件名匹配（最快）
    kw=$(echo "$name" | grep -oiE "$KEYWORDS" | head -1 | tr '[:upper:]' '[:lower:]')
    if [ -n "$kw" ]; then
        echo "$kw|$relpath"
        return
    fi

    # 第2步：读文件内容（只读前64KB，ELF符号表在头部）
    kw=$(dd if="$fullpath" bs=65536 count=1 2>/dev/null \
         | strings -n 5 2>/dev/null \
         | grep -oiE "$KEYWORDS" | head -1 | tr '[:upper:]' '[:lower:]')
    if [ -n "$kw" ]; then
        echo "$kw|$relpath"
    else
        echo "unknown|$relpath"
    fi
}
export -f scan_file
export KEYWORDS TARGET_DIR

echo "  扫描中（xargs -P8 并行，每文件只读前64KB）..."

# xargs -P8 并行扫描，每文件独立输出 "keyword|relpath"
# 结果统一收集到临时文件
TMP_RESULT="/tmp/prescan_result_$$.txt"
< "$INPUT" xargs -P8 -I{} bash -c 'scan_file "$@"' _ {} > "$TMP_RESULT" 2>/dev/null

# ── 分发到各关键词 list ──
while IFS='|' read -r kw relpath; do
    [ -z "$relpath" ] && continue
    echo "$relpath" >> "$WORKSPACE/prescan/$kw.list"
done < "$TMP_RESULT"
rm -f "$TMP_RESULT"

# 去重
for f in "$WORKSPACE"/prescan/*.list; do
    [ -f "$f" ] && sort -u "$f" -o "$f"
done

[ "$USE_FILTERED" != "1" ] && rm -f "$INPUT"

# ── 生成摘要 ──
{
    echo "=== 预扫描摘要 ==="
    echo "来源: $([ "$USE_FILTERED" = "1" ] && echo "filtered_files.txt" || echo "全量")"
    echo "总数: $TOTAL"
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

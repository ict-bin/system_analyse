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
echo "关键词: $(wc -l < "$KEYWORDS_FILE") 个"

# ── 扫描函数（按文件类型区分策略）──
scan_file() {
    local relpath="$1"
    local fullpath="$TARGET_DIR/$relpath"
    local name ext kw

    [ -f "$fullpath" ] || { echo "unknown|$relpath"; return; }

    name=$(basename "$relpath" | tr '[:upper:]' '[:lower:]')
    ext="${name##*.}"

    # 第1步：文件名匹配（最快，所有类型都先试）
    kw=$(echo "$name" | grep -oiE "$KEYWORDS" | head -1 | tr '[:upper:]' '[:lower:]')
    if [ -n "$kw" ]; then
        echo "$kw|$relpath"
        return
    fi

    # 第2步：按文件类型选择内容读取策略
    # 判断是否为二进制（ELF magic: 前4字节 = \x7fELF）
    magic=$(dd if="$fullpath" bs=4 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n')
    if [ "$magic" = "7f454c46" ]; then
        # ELF 二进制：只读前 128KB（动态符号表通常在头部）
        # 统计各关键词出现次数，取最高频的
        kw=$(dd if="$fullpath" bs=131072 count=1 2>/dev/null \
             | strings -n 5 2>/dev/null \
             | grep -oiE "$KEYWORDS" \
             | tr '[:upper:]' '[:lower:]' \
             | sort | uniq -c | sort -rn \
             | awk 'NR==1{print $2}')
    else
        # 文本文件（脚本/配置/XML等）：读完整文件，取出现最多的关键词
        kw=$(grep -oiE "$KEYWORDS" "$fullpath" 2>/dev/null \
             | tr '[:upper:]' '[:lower:]' \
             | sort | uniq -c | sort -rn \
             | awk 'NR==1{print $2}')
    fi

    if [ -n "$kw" ]; then
        echo "$kw|$relpath"
    else
        echo "unknown|$relpath"
    fi
}
export -f scan_file
export KEYWORDS TARGET_DIR

# ── 构建输入文件列表 ──
if [ "$USE_FILTERED" = "1" ]; then
    INPUT="$FILTERED"
else
    INPUT="/tmp/prescan_input_$$.txt"
    find "$TARGET_DIR" -type f | sed "s|^${TARGET_DIR}/||" > "$INPUT"
fi

echo "  扫描中（xargs -P8 并行）..."
TMP_RESULT="/tmp/prescan_result_$$.txt"
< "$INPUT" xargs -P8 -I{} bash -c 'scan_file "$@"' _ {} > "$TMP_RESULT" 2>/dev/null

# ── 分发到各关键词 list ──
while IFS='|' read -r kw relpath; do
    [ -z "$relpath" ] && continue
    echo "$relpath" >> "$WORKSPACE/prescan/${kw}.list"
done < "$TMP_RESULT"
rm -f "$TMP_RESULT"
[ "$USE_FILTERED" != "1" ] && rm -f "$INPUT"

# 去重
for f in "$WORKSPACE"/prescan/*.list; do
    [ -f "$f" ] && sort -u "$f" -o "$f"
done

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
        cnt=$(wc -l < "$listfile")
        [ "$cnt" -gt 0 ] && echo "$kw | $cnt"
    done | sort -t'|' -k2 -rn
    echo ""
    UNKNOWN=0
    [ -f "$WORKSPACE/prescan/unknown.list" ] && UNKNOWN=$(wc -l < "$WORKSPACE/prescan/unknown.list")
    echo "未识别: $UNKNOWN"
} > "$WORKSPACE/keyword_summary.txt"

echo ""
echo "=== 完成 ==="
cat "$WORKSPACE/keyword_summary.txt"

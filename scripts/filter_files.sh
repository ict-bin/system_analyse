#!/bin/bash
# filter_files.sh — 按分析类型过滤目标文件
# 用法: bash filter_files.sh <target_dir> <output_file> <types...>
# 输出: 相对路径（不含 target_dir 前缀）

set -e
TARGET_DIR="${1:-/data/target}"
OUTPUT_FILE="${2:-filtered_files.txt}"
shift 2
TYPES="$@"

# all = 不过滤
if echo "$TYPES" | grep -qw "all"; then
    find "$TARGET_DIR" -type f | sed "s|^${TARGET_DIR}/||" | sort > "$OUTPUT_FILE"
    TOTAL=$(wc -l < "$OUTPUT_FILE")
    echo "类型: all → $TOTAL 个文件（不过滤）"
    exit 0
fi

echo "=== 文件类型过滤 ==="
echo "类型: $TYPES"

# 构造扩展名匹配列表
EXT_PATTERNS=""
MAGIC_PATTERNS=""

for t in $TYPES; do
    case "$t" in
        binary)
            EXT_PATTERNS="$EXT_PATTERNS .so .ko .o .a .elf .axf"
            MAGIC_PATTERNS="$MAGIC_PATTERNS ELF"
            ;;
        script)
            EXT_PATTERNS="$EXT_PATTERNS .sh .bash .py .lua .pl .rb .tcl .awk .sed"
            MAGIC_PATTERNS="$MAGIC_PATTERNS shell_script Python_script Lua_script Perl_script"
            ;;
        config)
            EXT_PATTERNS="$EXT_PATTERNS .conf .cfg .ini .json .yaml .yml .xml .toml .properties .env"
            ;;
        firmware)
            EXT_PATTERNS="$EXT_PATTERNS .bin .img .dtb .dts .rom .fw .fpga .hex .srec .ubifs .cramfs .squashfs"
            MAGIC_PATTERNS="$MAGIC_PATTERNS firmware boot device_tree U-Boot"
            ;;
        crypto)
            EXT_PATTERNS="$EXT_PATTERNS .pem .crt .cer .key .csr .p12 .pfx .sig .cms .crl"
            MAGIC_PATTERNS="$MAGIC_PATTERNS certificate PEM private_key"
            ;;
        database)
            EXT_PATTERNS="$EXT_PATTERNS .db .sqlite .sqlite3 .sql .mdb .ldb"
            MAGIC_PATTERNS="$MAGIC_PATTERNS SQLite"
            ;;
        web)
            EXT_PATTERNS="$EXT_PATTERNS .html .htm .css .js .jsx .ts .php .jsp .vue .svg"
            MAGIC_PATTERNS="$MAGIC_PATTERNS HTML"
            ;;
        network_model)
            EXT_PATTERNS="$EXT_PATTERNS .yang .mib .asn .asn1 .proto .protobuf .xsd .wsdl .ncf"
            ;;
        document)
            EXT_PATTERNS="$EXT_PATTERNS .md .txt .rst .log .csv .pdf"
            ;;
        archive)
            EXT_PATTERNS="$EXT_PATTERNS .tar .gz .tgz .bz2 .xz .zip .rar .rpm .deb .ipk .cpio"
            MAGIC_PATTERNS="$MAGIC_PATTERNS gzip tar_archive Zip_archive RPM cpio"
            ;;
        *)
            echo "未知类型: $t（忽略）" >&2
            ;;
    esac
done

EXT_PATTERNS=$(echo $EXT_PATTERNS | tr ' ' '\n' | sort -u | tr '\n' ' ')
echo "扩展名: $EXT_PATTERNS"

# ── 第一轮：按扩展名匹配（输出相对路径）──
> "$OUTPUT_FILE"
for ext in $EXT_PATTERNS; do
    find "$TARGET_DIR" -type f -iname "*${ext}" | sed "s|^${TARGET_DIR}/||" >> "$OUTPUT_FILE" 2>/dev/null || true
done

# ── 第二轮：按 magic 匹配 ──
if [ -n "$MAGIC_PATTERNS" ]; then
    sort -u "$OUTPUT_FILE" -o "$OUTPUT_FILE"
    MATCHED_COUNT=$(wc -l < "$OUTPUT_FILE")

    find "$TARGET_DIR" -type f | sed "s|^${TARGET_DIR}/||" | sort > /tmp/all_target_rel.txt
    comm -23 /tmp/all_target_rel.txt "$OUTPUT_FILE" > /tmp/remaining_rel.txt
    REMAINING=$(wc -l < /tmp/remaining_rel.txt)

    if [ "$REMAINING" -gt 0 ]; then
        echo "扩展名匹配: $MATCHED_COUNT，剩余 $REMAINING 个用 magic 检测..."
        MAGIC_REGEX=$(echo $MAGIC_PATTERNS | tr ' ' '|' | sed 's/_/ /g')
        while IFS= read -r rel; do
            ftype=$(file -b "$TARGET_DIR/$rel" 2>/dev/null)
            if echo "$ftype" | grep -qiE "$MAGIC_REGEX"; then
                echo "$rel" >> "$OUTPUT_FILE"
            fi
        done < /tmp/remaining_rel.txt
    fi

    rm -f /tmp/all_target_rel.txt /tmp/remaining_rel.txt
fi

sort -u "$OUTPUT_FILE" -o "$OUTPUT_FILE"
TOTAL=$(wc -l < "$OUTPUT_FILE")
echo "过滤结果: $TOTAL 个文件"

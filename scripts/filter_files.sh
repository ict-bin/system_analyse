#!/bin/bash
# filter_files.sh — 按分析类型+架构过滤目标文件
# 用法: bash filter_files.sh <target_dir> <output_file> [--arch <arch_list>] <types...>
# arch_list: all | arm aarch64 x86 x86_64 mips ...（空格分隔，引号包裹）
# 输出: 相对路径（不含 target_dir 前缀）

set -e
TARGET_DIR="${1:-/data/target}"
OUTPUT_FILE="${2:-filtered_files.txt}"
shift 2

# ── 解析 --arch 参数 ──
ARCH_FILTER="all"
if [ "${1:-}" = "--arch" ]; then
    ARCH_FILTER="$2"
    shift 2
fi
TYPES="$@"

# ── 架构 → file 命令关键词映射 ──
arch_to_patterns() {
    local arch="$1"
    case "$arch" in
        x86)     echo "Intel 80386|i386|i486|i586|i686" ;;
        x86_64)  echo "x86-64|AMD x86-64" ;;
        arm)     echo "ARM,|ARM EABI|EABI5" ;;
        aarch64) echo "ARM aarch64|AArch64|aarch64" ;;
        mips)    echo "MIPS," ;;
        mips64)  echo "MIPS64|MIPS 64" ;;
        ppc)     echo "PowerPC|Power PC" ;;
        ppc64)   echo "64-bit PowerPC|PowerPC64" ;;
        riscv)   echo "RISC-V" ;;
        s390)    echo "IBM S/390|S390" ;;
        *)       echo "" ;;
    esac
}

# ── 构建架构正则 ──
ARCH_REGEX=""
if [ "$ARCH_FILTER" != "all" ]; then
    for a in $ARCH_FILTER; do
        pat=$(arch_to_patterns "$a")
        [ -z "$pat" ] && continue
        [ -n "$ARCH_REGEX" ] && ARCH_REGEX="${ARCH_REGEX}|"
        ARCH_REGEX="${ARCH_REGEX}${pat}"
    done
fi

echo "=== 文件类型过滤 ==="
echo "类型: ${TYPES:-all}"
echo "架构: $ARCH_FILTER"
[ -n "$ARCH_REGEX" ] && echo "架构正则: $ARCH_REGEX"

# ── all = 不按类型过滤 ──
if echo "$TYPES" | grep -qw "all"; then
    find "$TARGET_DIR" -type f | sed "s|^${TARGET_DIR}/||" | sort > "$OUTPUT_FILE"
else
    # ── 第一轮：按扩展名匹配 ──
    EXT_PATTERNS=""
    MAGIC_PATTERNS=""
    for t in $TYPES; do
        case "$t" in
            binary)       EXT_PATTERNS="$EXT_PATTERNS .so .ko .o .a .elf .axf" ; MAGIC_PATTERNS="$MAGIC_PATTERNS ELF" ;;
            script)       EXT_PATTERNS="$EXT_PATTERNS .sh .bash .py .lua .pl .rb .tcl .awk .sed" ; MAGIC_PATTERNS="$MAGIC_PATTERNS shell_script Python_script" ;;
            config)       EXT_PATTERNS="$EXT_PATTERNS .conf .cfg .ini .json .yaml .yml .xml .toml .properties .env" ;;
            firmware)     EXT_PATTERNS="$EXT_PATTERNS .bin .img .dtb .dts .rom .fw .fpga .hex .srec .ubifs .squashfs" ; MAGIC_PATTERNS="$MAGIC_PATTERNS U-Boot" ;;
            crypto)       EXT_PATTERNS="$EXT_PATTERNS .pem .crt .cer .key .csr .p12 .pfx .sig .cms .crl" ;;
            database)     EXT_PATTERNS="$EXT_PATTERNS .db .sqlite .sqlite3 .sql .mdb .ldb" ; MAGIC_PATTERNS="$MAGIC_PATTERNS SQLite" ;;
            web)          EXT_PATTERNS="$EXT_PATTERNS .html .htm .css .js .jsx .ts .php .jsp .vue .svg" ;;
            network_model) EXT_PATTERNS="$EXT_PATTERNS .yang .mib .asn .asn1 .proto .protobuf .xsd .wsdl .ncf" ;;
            document)     EXT_PATTERNS="$EXT_PATTERNS .md .txt .rst .log .csv .pdf" ;;
            archive)      EXT_PATTERNS="$EXT_PATTERNS .tar .gz .tgz .bz2 .xz .zip .rar .rpm .deb .ipk .cpio" ; MAGIC_PATTERNS="$MAGIC_PATTERNS gzip Zip_archive RPM" ;;
        esac
    done
    EXT_PATTERNS=$(echo $EXT_PATTERNS | tr ' ' '\n' | sort -u | tr '\n' ' ')

    > "$OUTPUT_FILE"
    for ext in $EXT_PATTERNS; do
        find "$TARGET_DIR" -type f -iname "*${ext}" | sed "s|^${TARGET_DIR}/||" >> "$OUTPUT_FILE" 2>/dev/null || true
    done

    # ── 第二轮：magic 匹配剩余文件 ──
    if [ -n "$MAGIC_PATTERNS" ]; then
        sort -u "$OUTPUT_FILE" -o "$OUTPUT_FILE"
        find "$TARGET_DIR" -type f | sed "s|^${TARGET_DIR}/||" | sort > /tmp/_all_rel.txt
        comm -23 /tmp/_all_rel.txt "$OUTPUT_FILE" > /tmp/_remaining.txt
        MAGIC_REGEX=$(echo $MAGIC_PATTERNS | tr ' ' '|' | sed 's/_/ /g')
        while IFS= read -r rel; do
            ftype=$(file -b "$TARGET_DIR/$rel" 2>/dev/null)
            echo "$ftype" | grep -qiE "$MAGIC_REGEX" && echo "$rel" >> "$OUTPUT_FILE"
        done < /tmp/_remaining.txt
        rm -f /tmp/_all_rel.txt /tmp/_remaining.txt
    fi

    sort -u "$OUTPUT_FILE" -o "$OUTPUT_FILE"
fi

# ── 架构过滤（只对 binary 类型有效，用 file 命令检测 ELF 架构）──
if [ -n "$ARCH_REGEX" ]; then
    echo "开始架构过滤..."
    BEFORE=$(wc -l < "$OUTPUT_FILE")
    > /tmp/_arch_filtered.txt
    while IFS= read -r rel; do
        ftype=$(file -b "$TARGET_DIR/$rel" 2>/dev/null)
        # 只过滤 ELF 文件，非 ELF 文件直接保留
        if echo "$ftype" | grep -q "ELF"; then
            echo "$ftype" | grep -qiE "$ARCH_REGEX" && echo "$rel" >> /tmp/_arch_filtered.txt
        else
            echo "$rel" >> /tmp/_arch_filtered.txt
        fi
    done < "$OUTPUT_FILE"
    mv /tmp/_arch_filtered.txt "$OUTPUT_FILE"
    AFTER=$(wc -l < "$OUTPUT_FILE")
    echo "架构过滤: $BEFORE → $AFTER 个文件 (过滤掉 $((BEFORE - AFTER)) 个)"
fi

TOTAL=$(wc -l < "$OUTPUT_FILE")
echo "过滤结果: $TOTAL 个文件"

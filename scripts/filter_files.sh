#!/bin/bash
# filter_files.sh — 按分析类型+架构过滤目标文件
# 用法: bash filter_files.sh <target_dir> <output_file> [--arch <arch_list>] <types...>
# arch_list: all | "arm aarch64" | "x86 x86_64" ...（空格分隔，引号包裹）
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

echo "=== 文件类型过滤 ==="
echo "类型: ${TYPES:-all}  架构: $ARCH_FILTER"

# ── all = 不按类型过滤 ──
if echo "$TYPES" | grep -qw "all"; then
    find "$TARGET_DIR" -type f | sed "s|^${TARGET_DIR}/||" | sort > "$OUTPUT_FILE"
else
    EXT_PATTERNS=""
    MAGIC_PATTERNS=""
    for t in $TYPES; do
        case "$t" in
            binary)       EXT_PATTERNS="$EXT_PATTERNS .so .ko .o .a .elf .axf" ; MAGIC_PATTERNS="$MAGIC_PATTERNS ELF" ;;
            script)       EXT_PATTERNS="$EXT_PATTERNS .sh .bash .py .lua .pl .rb .tcl .awk .sed" ;;
            config)       EXT_PATTERNS="$EXT_PATTERNS .conf .cfg .ini .json .yaml .yml .xml .toml .properties .env" ;;
            firmware)     EXT_PATTERNS="$EXT_PATTERNS .bin .img .dtb .dts .rom .fw .fpga .hex .srec .ubifs .squashfs" ;;
            crypto)       EXT_PATTERNS="$EXT_PATTERNS .pem .crt .cer .key .csr .p12 .pfx .sig .cms .crl" ;;
            database)     EXT_PATTERNS="$EXT_PATTERNS .db .sqlite .sqlite3 .sql .mdb .ldb" ;;
            web)          EXT_PATTERNS="$EXT_PATTERNS .html .htm .css .js .jsx .ts .php .jsp .vue .svg" ;;
            network_model) EXT_PATTERNS="$EXT_PATTERNS .yang .mib .asn .asn1 .proto .protobuf .xsd .wsdl .ncf" ;;
            document)     EXT_PATTERNS="$EXT_PATTERNS .md .txt .rst .log .csv .pdf" ;;
            archive)      EXT_PATTERNS="$EXT_PATTERNS .tar .gz .tgz .bz2 .xz .zip .rar .rpm .deb .ipk .cpio" ;;
        esac
    done
    EXT_PATTERNS=$(echo $EXT_PATTERNS | tr ' ' '\n' | sort -u | tr '\n' ' ')

    > "$OUTPUT_FILE"
    for ext in $EXT_PATTERNS; do
        find "$TARGET_DIR" -type f -iname "*${ext}" | sed "s|^${TARGET_DIR}/||" >> "$OUTPUT_FILE" 2>/dev/null || true
    done
    sort -u "$OUTPUT_FILE" -o "$OUTPUT_FILE"
fi

BEFORE=$(wc -l < "$OUTPUT_FILE")
echo "类型过滤后: $BEFORE 个文件"

# ── 架构过滤（仅对 ELF 有效）──
if [ "$ARCH_FILTER" = "all" ]; then
    echo "架构: 不过滤"
    echo "过滤结果: $BEFORE 个文件"
    exit 0
fi

# ELF e_machine 值 → 架构名映射（十进制）
# 3=x86  40=arm  62=x86_64  183=aarch64  8=mips  20=ppc  243=riscv  22=s390
elf_arch_match() {
    local filepath="$1"
    # 读 ELF magic (4字节) + e_machine (偏移0x12, 2字节 little-endian)
    python3 - "$filepath" << 'PYEOF'
import sys, struct, os

path = sys.argv[1]
MACHINE_MAP = {
    3:   "x86",
    40:  "arm",
    62:  "x86_64",
    183: "aarch64",
    8:   "mips",
    20:  "ppc",
    21:  "ppc64",
    243: "riscv",
    22:  "s390",
}
try:
    with open(path, "rb") as f:
        magic = f.read(4)
        if magic != b'\x7fELF':
            print("noelf")
            sys.exit(0)
        f.seek(0x12)
        em = struct.unpack_from("<H", f.read(2))[0]
        print(MACHINE_MAP.get(em, f"unknown_{em}"))
except Exception:
    print("noelf")
PYEOF
}

# 构建目标架构集合
TARGET_ARCHS=$(echo "$ARCH_FILTER" | tr ' ' '\n' | sort -u | tr '\n' ' ')
echo "目标架构: $TARGET_ARCHS"

# 路径关键词快速匹配（利用目录名）
# aarch64/arm64 目录 → aarch64；arm/ → arm；x86_64/ → x86_64 等
path_arch_hint() {
    local rel="$1"
    case "$rel" in
        *aarch64*|*arm64*)  echo "aarch64" ;;
        */arm/*)            echo "arm" ;;
        *x86_64*|*amd64*)   echo "x86_64" ;;
        */x86/*)            echo "x86" ;;
        *mips64*)           echo "mips64" ;;
        */mips/*)           echo "mips" ;;
        *ppc64*)            echo "ppc64" ;;
        */ppc/*)            echo "ppc" ;;
        *riscv*)            echo "riscv" ;;
        *)                  echo "" ;;
    esac
}

echo "开始架构过滤（$BEFORE 个文件）..."
> /tmp/_arch_pass.txt

CHECKED=0
while IFS= read -r rel; do
    CHECKED=$((CHECKED + 1))
    [ $((CHECKED % 500)) -eq 0 ] && echo "  进度: $CHECKED / $BEFORE" >&2

    # 1. 路径快速判断
    hint=$(path_arch_hint "$rel")
    if [ -n "$hint" ]; then
        for ta in $TARGET_ARCHS; do
            [ "$ta" = "$hint" ] && echo "$rel" >> /tmp/_arch_pass.txt && continue 2
        done
        # 路径明确指示了非目标架构，直接跳过
        continue
    fi

    # 2. 读 ELF header 判断
    arch=$(elf_arch_match "$TARGET_DIR/$rel" 2>/dev/null)
    if [ "$arch" = "noelf" ]; then
        # 非 ELF（脚本/配置等），直接保留
        echo "$rel" >> /tmp/_arch_pass.txt
        continue
    fi

    for ta in $TARGET_ARCHS; do
        [ "$ta" = "$arch" ] && echo "$rel" >> /tmp/_arch_pass.txt && continue 2
    done
    # 不匹配任何目标架构，过滤掉
done < "$OUTPUT_FILE"

mv /tmp/_arch_pass.txt "$OUTPUT_FILE"
AFTER=$(wc -l < "$OUTPUT_FILE")
echo "架构过滤: $BEFORE → $AFTER 个文件（过滤掉 $((BEFORE - AFTER)) 个）"
echo "过滤结果: $AFTER 个文件"

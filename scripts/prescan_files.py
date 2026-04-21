#!/usr/bin/env python3
"""
prescan_files.py — 快速预扫描文件关键词分类
策略:
  - 文件名先匹配（最快）
  - ELF 二进制：读前128KB提取可打印字符串（符号表通常在头部）
  - 文本文件：读全文
  - 关键词要求至少4字符（避免 rr/sr 等短词误匹配）
  - 统计词频，取最高频关键词
  - multiprocessing.Pool 并行
"""
import os
import re
import sys
import struct
from pathlib import Path
from multiprocessing import Pool
from collections import Counter

TARGET_DIR = ""
KEYWORDS = []
KW_PATTERN = None

ELF_MAGIC = b"\x7fELF"
MAX_ELF_BYTES = 131072   # 前128KB
MIN_STR_LEN = 5          # strings 最小长度


def extract_strings_from_bytes(data: bytes) -> str:
    """从二进制数据中提取可打印字符串（类似 strings 命令）"""
    result = []
    current = []
    for b in data:
        c = chr(b)
        if c.isprintable() and c != '\n':
            current.append(c)
        else:
            if len(current) >= MIN_STR_LEN:
                result.append("".join(current))
            current = []
    if len(current) >= MIN_STR_LEN:
        result.append("".join(current))
    return " ".join(result)


def scan_file(relpath: str) -> tuple[str, str]:
    """扫描单个文件，返回 (keyword, relpath)"""
    fullpath = os.path.join(TARGET_DIR, relpath)
    name = os.path.basename(relpath).lower()

    # 1. 文件名匹配
    m = KW_PATTERN.search(name)
    if m:
        return (m.group().lower(), relpath)

    # 2. 读取文件内容
    try:
        with open(fullpath, "rb") as f:
            header = f.read(4)
            if header == ELF_MAGIC:
                # ELF：读前128KB提取字符串
                f.seek(0)
                data = f.read(MAX_ELF_BYTES)
                text = extract_strings_from_bytes(data)
            else:
                # 文本文件：读全文（限制4MB防止超大文件）
                f.seek(0)
                raw = f.read(4 * 1024 * 1024)
                try:
                    text = raw.decode("utf-8", errors="ignore")
                except Exception:
                    text = extract_strings_from_bytes(raw)
    except (OSError, IOError):
        return ("unknown", relpath)

    # 3. 统计关键词频率，取最高频
    matches = KW_PATTERN.findall(text.lower())
    if not matches:
        return ("unknown", relpath)
    kw, _ = Counter(matches).most_common(1)[0]
    return (kw, relpath)


def main():
    global TARGET_DIR, KEYWORDS, KW_PATTERN

    if len(sys.argv) < 3:
        print("用法: python3 prescan_files.py <target_dir> <workspace_dir>")
        sys.exit(1)

    TARGET_DIR = sys.argv[1].rstrip("/")
    workspace = Path(sys.argv[2])
    keywords_file = workspace / "keywords.txt"
    filtered_file = workspace / "filtered_files.txt"

    if not keywords_file.exists():
        print(f"ERROR: {keywords_file} 不存在，跳过预扫描")
        sys.exit(1)

    # 读取关键词，过滤掉长度<4的短词和非功能性词
    BLACKLIST = {
        'aarch64', 'x86_64', 'x86', 'arm', 'mips', 'ppc',  # 架构名
        'squashfs', 'lzma', 'bzip', 'gzip', 'xz', 'rpm', 'rpmdb', 'tar',  # 打包格式
        'huawei', 'cisco', 'juniper', 'nokia',  # 品牌名
        'python', 'lua', 'perl', 'ruby',  # 脚本语言
        'module', 'modules', 'kernel', 'firmware', 'upgrade',  # 过于通用
        'yang', 'yin', 'conf', 'json', 'yaml', 'proto',  # 配置格式
    }
    raw_kws = [l.strip().lower() for l in keywords_file.read_text().splitlines() if l.strip()]
    KEYWORDS = [k for k in raw_kws if len(k) >= 4 and k not in BLACKLIST]
    skipped = len(raw_kws) - len(KEYWORDS)
    if skipped:
        print(f"  跳过 {skipped} 个无效关键词（过短或非功能性）")

    # 关键词正则（词边界匹配，防止子串误匹配）
    # 使用 \b 词边界，但部分关键词如 "bgp" 在 libbgp.so 里也要能匹配
    # 不用 \b（对 _ 分隔的命名无效），改用字母边界
    # 允许: libbras_dhcp.so 里的 dhcp, DHCP_SERVER 里的 DHCP, libbgp.so 里的 bgp
    pattern_str = r'(?<![a-zA-Z])(' + '|'.join(re.escape(k) for k in sorted(KEYWORDS, key=len, reverse=True)) + r')(?![a-zA-Z])'
    KW_PATTERN = re.compile(pattern_str, re.IGNORECASE)

    # 构建文件列表
    if filtered_file.exists():
        files = [l.strip() for l in filtered_file.read_text().splitlines() if l.strip()]
        print(f"=== 预扫描（过滤列表：{len(files)} 个文件）===")
    else:
        files = []
        for root, _, fnames in os.walk(TARGET_DIR):
            for fn in fnames:
                relpath = os.path.relpath(os.path.join(root, fn), TARGET_DIR)
                files.append(relpath)
        print(f"=== 预扫描（全量：{len(files)} 个文件）===")

    print(f"关键词: {len(KEYWORDS)} 个（已过滤过短词）")

    # 并行扫描
    prescan_dir = workspace / "prescan"
    prescan_dir.mkdir(exist_ok=True)

    print("  扫描中（multiprocessing.Pool 并行）...")
    workers = min(8, os.cpu_count() or 4)
    with Pool(workers, initializer=_pool_init,
              initargs=(TARGET_DIR, KEYWORDS, pattern_str)) as pool:
        results = pool.map(scan_file, files, chunksize=50)

    # 分发到各关键词 list
    buckets: dict[str, list[str]] = {}
    for kw, relpath in results:
        buckets.setdefault(kw, []).append(relpath)

    for kw, paths in buckets.items():
        list_file = prescan_dir / f"{kw}.list"
        with open(list_file, "w") as f:
            f.write("\n".join(sorted(set(paths))) + "\n")

    # 生成摘要
    summary_lines = ["=== 预扫描摘要 ===",
                     f"来源: {'filtered_files.txt' if filtered_file.exists() else '全量'}",
                     f"总数: {len(files)}",
                     "",
                     "关键词 | 文件数",
                     "-------|-------"]
    rows = [(kw, len(paths)) for kw, paths in buckets.items() if kw != "unknown"]
    rows.sort(key=lambda x: -x[1])
    for kw, cnt in rows:
        summary_lines.append(f"{kw} | {cnt}")
    unknown_cnt = len(buckets.get("unknown", []))
    summary_lines.append(f"\n未识别: {unknown_cnt}")

    summary_text = "\n".join(summary_lines)
    (workspace / "keyword_summary.txt").write_text(summary_text)
    print("\n=== 完成 ===")
    print(summary_text)


def _pool_init(target_dir, keywords, pattern_str):
    global TARGET_DIR, KEYWORDS, KW_PATTERN
    TARGET_DIR = target_dir
    KEYWORDS = keywords
    KW_PATTERN = re.compile(pattern_str, re.IGNORECASE)


if __name__ == "__main__":
    main()

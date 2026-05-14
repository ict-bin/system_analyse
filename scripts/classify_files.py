#!/usr/bin/env python3
"""
classify_files.py — 文件类型识别脚本

对 filtered_files.txt 中的每个文件读取 magic header + 扩展名，生成：
  workspace/file_catalog.json  — 结构化分类结果（含 unknown 分组）
  workspace/unknown_files.txt  — UNKNOWN 类型文件路径（供 unknown_checker 处理）

用法:
  python3 classify_files.py <target_dir> <workspace_dir>

输出格式 file_catalog.json:
  {
    "total": 1523,
    "filtered_count": 1287,
    "files": [
      {"path": "lib64/libssl.so.1.1", "type": "ELF", "arch": "aarch64"},
      {"path": "etc/nginx.conf",      "type": "CONFIG_NGINX"},
      {"path": "bin/busybox",         "type": "ELF", "arch": "x86_64"},
      {"path": "data/model.bin",      "type": "UNKNOWN"}
    ],
    "unknown_count": 12,
    "type_summary": {"ELF": 450, "SCRIPT_SHELL": 120, "UNKNOWN": 12}
  }
"""
from __future__ import annotations

import json
import os
import struct
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ── ELF magic & e_machine → arch 映射 ────────────────────────────────────────
ELF_MAGIC = b"\x7fELF"
ELF_MACHINE_MAP = {
    3:   "x86",
    40:  "arm",
    62:  "x86_64",
    183: "aarch64",
    8:   "mips",
    20:  "ppc",
    21:  "ppc64",
    243: "riscv",
    22:  "s390",
    253: "loongarch",
}

# ── 扩展名 → 类型映射 ─────────────────────────────────────────────────────────
EXT_MAP: dict[str, str] = {
    # 源码
    ".c":    "C_SOURCE",     ".h":    "HEADER",
    ".cpp":  "CPP_SOURCE",   ".cc":   "CPP_SOURCE",
    ".cxx":  "CPP_SOURCE",   ".c++":  "CPP_SOURCE",
    ".hpp":  "HEADER",       ".hh":   "HEADER",
    ".hxx":  "HEADER",       ".inc":  "HEADER",
    ".inl":  "HEADER",       ".ipp":  "HEADER",
    ".s":    "ASM",          ".S":    "ASM",
    ".asm":  "ASM",
    # 脚本
    ".sh":   "SCRIPT_SHELL", ".bash": "SCRIPT_SHELL",
    ".py":   "SCRIPT_PYTHON",
    ".lua":  "SCRIPT_LUA",
    ".pl":   "SCRIPT_PERL",  ".pm":   "SCRIPT_PERL",
    ".rb":   "SCRIPT_RUBY",
    ".tcl":  "SCRIPT_TCL",
    ".awk":  "SCRIPT_AWK",
    ".sed":  "SCRIPT_SED",
    # 配置
    ".conf": "CONFIG_CONF",  ".cfg":  "CONFIG_CONF",
    ".ini":  "CONFIG_INI",   ".env":  "CONFIG_ENV",
    ".json": "CONFIG_JSON",
    ".yaml": "CONFIG_YAML",  ".yml":  "CONFIG_YAML",
    ".xml":  "CONFIG_XML",
    ".toml": "CONFIG_TOML",
    ".properties": "CONFIG_PROPERTIES",
    # 网络模型
    ".yang": "NETWORK_MODEL", ".mib":  "NETWORK_MODEL",
    ".asn":  "NETWORK_MODEL", ".asn1": "NETWORK_MODEL",
    ".proto": "NETWORK_MODEL", ".protobuf": "NETWORK_MODEL",
    ".xsd":  "NETWORK_MODEL", ".wsdl": "NETWORK_MODEL",
    ".ncf":  "NETWORK_MODEL",
    # 证书/密钥
    ".pem":  "CRYPTO_CERT",  ".crt":  "CRYPTO_CERT",
    ".cer":  "CRYPTO_CERT",  ".key":  "CRYPTO_KEY",
    ".csr":  "CRYPTO_CERT",  ".p12":  "CRYPTO_CERT",
    ".pfx":  "CRYPTO_CERT",  ".sig":  "CRYPTO_SIG",
    ".cms":  "CRYPTO_CERT",  ".crl":  "CRYPTO_CERT",
    # 数据库
    ".db":   "DATABASE",     ".sqlite": "DATABASE",
    ".sqlite3": "DATABASE",  ".sql":  "DATABASE_SQL",
    # Web
    ".html": "WEB_HTML",     ".htm":  "WEB_HTML",
    ".css":  "WEB_CSS",
    ".js":   "WEB_JS",       ".jsx":  "WEB_JS",
    ".ts":   "WEB_TS",
    ".php":  "WEB_PHP",      ".jsp":  "WEB_JSP",
    ".vue":  "WEB_VUE",      ".svg":  "WEB_SVG",
    # 文档
    ".md":   "DOCUMENT",     ".rst":  "DOCUMENT",
    ".txt":  "DOCUMENT",     ".log":  "LOG",
    ".csv":  "DATA_CSV",     ".pdf":  "DOCUMENT_PDF",
    # 归档
    ".tar":  "ARCHIVE",      ".gz":   "ARCHIVE",
    ".tgz":  "ARCHIVE",      ".bz2":  "ARCHIVE",
    ".xz":   "ARCHIVE",      ".zip":  "ARCHIVE",
    ".rar":  "ARCHIVE",      ".rpm":  "PACKAGE_RPM",
    ".deb":  "PACKAGE_DEB",  ".ipk":  "PACKAGE_IPK",
    ".cpio": "ARCHIVE",
    # 固件/硬件
    ".bin":  "BINARY_BLOB",  ".img":  "FIRMWARE_IMG",
    ".dtb":  "FIRMWARE_DTB", ".dts":  "FIRMWARE_DTS",
    ".rom":  "FIRMWARE_ROM", ".fw":   "FIRMWARE",
    ".hex":  "FIRMWARE_HEX", ".srec": "FIRMWARE_SREC",
    # ELF 扩展名（magic 优先，此为兜底）
    ".so":   "ELF",          ".ko":   "ELF",
    ".o":    "ELF",          ".a":    "STATIC_LIB",
    ".elf":  "ELF",          ".axf":  "ELF",
}

# ── shebang 首行 → 类型 ───────────────────────────────────────────────────────
SHEBANG_MAP = [
    (b"python",  "SCRIPT_PYTHON"),
    (b"perl",    "SCRIPT_PERL"),
    (b"ruby",    "SCRIPT_RUBY"),
    (b"lua",     "SCRIPT_LUA"),
    (b"node",    "SCRIPT_JS"),
    (b"php",     "SCRIPT_PHP"),
    (b"tclsh",   "SCRIPT_TCL"),
    (b"bash",    "SCRIPT_SHELL"),
    (b"sh",      "SCRIPT_SHELL"),
    (b"ash",     "SCRIPT_SHELL"),
    (b"dash",    "SCRIPT_SHELL"),
    (b"zsh",     "SCRIPT_SHELL"),
]

# 其他 magic 前缀
MAGIC_MAP = [
    (b"PK\x03\x04",     "ARCHIVE_ZIP"),
    (b"\x1f\x8b",       "ARCHIVE_GZ"),
    (b"BZh",            "ARCHIVE_BZ2"),
    (b"\xfd7zXZ\x00",   "ARCHIVE_XZ"),
    (b"RPMS",           "PACKAGE_RPM"),
    (b"!<arch>",        "STATIC_LIB"),
    (b"MZ",             "EXE_WIN"),
    (b"\xca\xfe\xba\xbe", "ELF"),   # macOS fat binary（当 ELF 处理）
]


def classify_file(target_dir: str, rel_path: str) -> dict:
    """对单个文件做类型分类，返回 {path, type, arch?}。"""
    full = os.path.join(target_dir, rel_path)
    result: dict = {"path": rel_path, "type": "UNKNOWN"}

    # 先读文件头（256 字节足够判断 magic）
    try:
        with open(full, "rb") as f:
            header = f.read(256)
    except OSError:
        result["type"] = "MISSING"
        return result

    # 1. ELF magic
    if header[:4] == ELF_MAGIC:
        result["type"] = "ELF"
        try:
            # EI_DATA (offset 0x05): 1=little-endian, 2=big-endian
            ei_data = header[5] if len(header) > 5 else 1
            fmt = ">H" if ei_data == 2 else "<H"
            em = struct.unpack_from(fmt, header, 0x12)[0]
            arch = ELF_MACHINE_MAP.get(em, f"unknown_e_machine_{em}")
        except Exception:
            arch = "unknown"
        result["arch"] = arch
        return result

    # 2. shebang
    if header[:2] == b"#!":
        line = header.split(b"\n", 1)[0].lower()
        for kw, ftype in SHEBANG_MAP:
            if kw in line:
                result["type"] = ftype
                return result
        result["type"] = "SCRIPT_SHELL"   # 兜底
        return result

    # 3. 其他 magic
    for magic, ftype in MAGIC_MAP:
        if header[:len(magic)] == magic:
            result["type"] = ftype
            return result

    # 4. 扩展名映射
    ext = Path(rel_path).suffix.lower()
    if ext in EXT_MAP:
        result["type"] = EXT_MAP[ext]
        return result

    # 5. 尝试判断是否为可读文本（config/script 兜底）
    try:
        header.decode("utf-8")
        # 可读文本，但扩展名未知
        result["type"] = "TEXT_UNKNOWN"
    except UnicodeDecodeError:
        result["type"] = "UNKNOWN"

    return result


def main(target_dir: str, workspace_dir: str) -> None:
    ws = Path(workspace_dir)
    ff = ws / "filtered_files.txt"
    if not ff.exists():
        print(f"[classify_files] filtered_files.txt not found in {workspace_dir}, skip", flush=True)
        return

    files = [l.strip() for l in ff.read_text(encoding="utf-8").splitlines() if l.strip()]
    print(f"[classify_files] 分类 {len(files)} 个文件...", flush=True)

    results: list[dict] = [None] * len(files)
    with ThreadPoolExecutor(max_workers=16) as pool:
        futs = {pool.submit(classify_file, target_dir, f): i for i, f in enumerate(files)}
        for fut in as_completed(futs):
            idx = futs[fut]
            try:
                results[idx] = fut.result()
            except Exception as e:
                results[idx] = {"path": files[idx], "type": "UNKNOWN", "error": str(e)}

    type_counter: Counter = Counter(r["type"] for r in results)
    unknown_files = [r["path"] for r in results if r["type"] in ("UNKNOWN", "TEXT_UNKNOWN")]

    catalog = {
        "total": len(files),
        "filtered_count": len(files),
        "files": results,
        "unknown_count": len(unknown_files),
        "type_summary": dict(type_counter.most_common()),
    }

    catalog_path = ws / "file_catalog.json"
    catalog_path.write_text(
        json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[classify_files] 写入 file_catalog.json ({len(files)} 个文件)", flush=True)
    print(f"[classify_files] 类型分布: {dict(type_counter.most_common(10))}", flush=True)

    if unknown_files:
        unknown_path = ws / "unknown_files.txt"
        unknown_path.write_text("\n".join(unknown_files) + "\n", encoding="utf-8")
        print(f"[classify_files] unknown_files.txt: {len(unknown_files)} 个未识别文件", flush=True)
    else:
        (ws / "unknown_files.txt").unlink(missing_ok=True)
        print("[classify_files] 无 UNKNOWN 文件", flush=True)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("用法: python3 classify_files.py <target_dir> <workspace_dir>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])

"""Fix s1_security_filter.py:
1. Worker prompt: explicitly allow deleting all modules
2. Judge prompt: note that 0-module result is valid + display "（无）" when empty
"""
import re

path = "app/pipeline/s1_security_filter.py"
with open(path, "rb") as f:
    content = f.read().decode("utf-8")

# Normalize line endings
content = content.replace("\r\n", "\n")

# ── Fix 1: Worker first-round prompt ────────────────────────────────────────
# Add "deleting all modules is valid" note after the second f-string
OLD_WORKER_LINE = (
    '                    f"## \u5f53\u524d\u5168\u90e8\u6a21\u5757\uff08\u5171 {len(current_modules)} \u4e2a\uff09\\n\\n"\n'
)
NEW_WORKER_LINE = (
    '                    f"\u26a0\ufe0f **\u5220\u9664\u5168\u90e8\u6a21\u5757\u662f\u5408\u6cd5\u4e14\u6b63\u786e\u7684\u7ed3\u679c**\uff1a\u82e5\u6240\u6709\u6a21\u5757\u5747\u4e0e\u5b89\u5168\u7ef4\u5ea6\u65e0\u5173\uff0c\\n"\n'
    '                    f"\u5e94\u5168\u90e8\u5220\u9664\uff0c`modules/` \u4e3a\u7a7a\u76ee\u5f55\u662f\u5b8c\u5168\u5408\u6cd5\u7684\u8f93\u51fa\u3002\u4e0d\u8981\u56e0\u62c5\u5fc3\u6d41\u6c34\u7ebf\u7a7a\u8ddd\u800c\u4fdd\u7559\u65e0\u5173\u6a21\u5757\u3002\\n\\n"\n'
    '                    f"## \u5f53\u524d\u5168\u90e8\u6a21\u5757\uff08\u5171 {len(current_modules)} \u4e2a\uff09\\n\\n"\n'
)

if OLD_WORKER_LINE in content:
    content = content.replace(OLD_WORKER_LINE, NEW_WORKER_LINE, 1)
    print("Worker prompt: REPLACED")
else:
    print("Worker prompt: NOT FOUND, searching...")
    idx = content.find("\u5f53\u524d\u5168\u90e8\u6a21\u5757\uff08\u5171")
    if idx >= 0:
        print(repr(content[idx-60:idx+200]))

# ── Fix 2: Judge prompt — add valid-empty note ───────────────────────────────
OLD_JUDGE_HDR = (
    '                    f"## \u8fc7\u6ee4\u524d\uff08\u5907\u4efd\uff09\uff1a{len(backup_mods)} \u4e2a\u6a21\u5757\\n\\n"\n'
)
NEW_JUDGE_HDR = (
    '                    f"\u26a0\ufe0f **\u7a7a\u7ed3\u679c\u662f\u5408\u6cd5\u7684**\uff1a\u82e5\u5168\u90e8\u6a21\u5757\u5747\u4e0e\u5b89\u5168\u7ef4\u5ea6\u65e0\u5173\uff0c\u8fc7\u6ee4\u540e 0 \u4e2a\u6a21\u5757\u4fdd\u7559\u662f\u5b8c\u5168\u6b63\u786e\u7684\u7ed3\u679c\u3002\\n\\n"\n'
    '                    f"## \u8fc7\u6ee4\u524d\uff08\u5907\u4efd\uff09\uff1a{len(backup_mods)} \u4e2a\u6a21\u5757\\n\\n"\n'
)

if OLD_JUDGE_HDR in content:
    content = content.replace(OLD_JUDGE_HDR, NEW_JUDGE_HDR, 1)
    print("Judge header: REPLACED")
else:
    print("Judge header: NOT FOUND")
    idx = content.find("\u8fc7\u6ee4\u524d\uff08\u5907\u4efd\uff09")
    print(repr(content[idx-30:idx+100]))

# ── Fix 3: Judge prompt — display "(无)" when 0 modules retained ─────────────
OLD_KEPT = (
    '                    + f"\\n\\n## \u8fc7\u6ee4\u540e\uff08\u5f53\u524d\uff09\uff1a{len(kept_modules)} \u4e2a\u6a21\u5757\\n\\n"\n'
    '                    + "\\n".join(f"- `{m}`" for m in sorted(kept_modules))\n'
)
NEW_KEPT = (
    '                    + f"\\n\\n## \u8fc7\u6ee4\u540e\uff08\u5f53\u524d\uff09\uff1a{len(kept_modules)} \u4e2a\u6a21\u5757"\n'
    '                    + ("\\n\\n\uff08\u65e0\u2014\u2014\u6240\u6709\u6a21\u5757\u5df2\u5220\u9664\uff09" if not kept_modules else\n'
    '                       "\\n\\n" + "\\n".join(f"- `{m}`" for m in sorted(kept_modules)))\n'
)

if OLD_KEPT in content:
    content = content.replace(OLD_KEPT, NEW_KEPT, 1)
    print("Kept modules display: REPLACED")
else:
    print("Kept modules display: NOT FOUND")
    idx = content.find("\u8fc7\u6ee4\u540e\uff08\u5f53\u524d\uff09")
    print(repr(content[idx-30:idx+300]))

# Write back (keep original line endings style)
with open(path, "w", encoding="utf-8", newline="\n") as f:
    f.write(content)
print("Done")

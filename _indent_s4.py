"""Indent the W+J loop body under 'if not _zero_modules_mode:' block."""
path = "app/pipeline/s4_report.py"
with open(path, encoding="utf-8") as f:
    lines = f.readlines()

# Find the block to indent:
# Start: line after 'if not _zero_modules_mode:'
# End: line before '# ── 组装输出目录 ──'
start_marker = "        if not _zero_modules_mode:\n"
end_marker = "        # ── 组装输出目录"

start_idx = None
end_idx = None
for i, line in enumerate(lines):
    if line == start_marker and start_idx is None:
        start_idx = i + 1  # indent from next line
    if end_marker in line and start_idx is not None and end_idx is None:
        end_idx = i
        break

print(f"indent range: lines {start_idx+1} to {end_idx+1} (0-indexed {start_idx}..{end_idx-1})")
print(f"start line: {repr(lines[start_idx][:80])}")
print(f"end line: {repr(lines[end_idx][:80])}")

# Add 4 spaces to each line in [start_idx, end_idx)
new_lines = []
for i, line in enumerate(lines):
    if start_idx <= i < end_idx:
        # Only indent non-empty lines
        if line.strip():
            new_lines.append("    " + line)
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
print("Done")

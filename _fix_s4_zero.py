"""
Fix s4_report.py FinalReportStage to handle 0-module case:
1. When 0 modules after security filter: write explanation to workspace/final_report.md
2. Skip the entire W+J loop  
3. Fall through to the output assembly so output/ gets populated
"""
import re

path = "app/pipeline/s4_report.py"
with open(path, encoding="utf-8") as f:
    content = f.read()

# Normalize line endings
content = content.replace("\r\n", "\n")

lines = content.split("\n")

# Find key lines (0-indexed)
execute_start = None
output_assembly_start = None
raise_error_line = None

for i, line in enumerate(lines):
    if "async def execute" in line and execute_start is None:
        # We want the SECOND execute (FinalReportStage)
        pass
    # Find '# ── 组装输出目录' 
    if "\u7ec4\u88c5\u8f93\u51fa\u76ee\u5f55" in line and output_assembly_start is None:
        output_assembly_start = i
    # Find the raise StageError for 4b max rounds
    if "\u6700\u7ec8\u62a5\u544a\u672a\u901a\u8fc7" in line and raise_error_line is None:
        raise_error_line = i

print(f"output_assembly_start: line {output_assembly_start+1}")
print(f"raise_error_line: line {raise_error_line+1}")

# The second execute() starts around line 383 (1-indexed)
# We need to insert the 0-module fast path right after the variable setup
# and before the checkpoint check, and also add an "if not _zero_modules_mode:"
# guard around the W+J loop.

# Strategy: 
# 1. Insert the 0-module detection + workspace report write right after 
#    "final_out_dir = ctx.final_out_dir" in the second execute()
# 2. Wrap lines from "# ── checkpoint 跳过" down to end of W+J loop 
#    (line before "# ── 组装输出目录") inside "if not _zero_modules_mode:"

# Find "final_out_dir = ctx.final_out_dir" in second execute (after line 383)
insert_after = None
for i in range(383, len(lines)):
    if "final_out_dir = ctx.final_out_dir" in lines[i]:
        insert_after = i
        break

print(f"insert_after: line {insert_after+1}")
print(f"next line: {repr(lines[insert_after+1])}")

# Build the 0-module detection block to insert (no indent change needed, just insert)
zero_block = [
    "",
    "        # \u2500\u2500 0 \u6a21\u5757\u5feb\u901f\u8def\u5f84\uff1a\u5b89\u5168\u8fc7\u6ee4\u540e\u65e0\u76f8\u5173\u6a21\u5757\uff0c\u8df3\u8fc7 LLM\uff0c\u76f4\u63a5\u8fdb\u884c\u8f93\u51fa\u7ec4\u88c5 \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500",
    "        _sec_cats: list = getattr(cfg, \"security_focus_categories\", [\"all\"])",
    "        _all_mods = discover_modules(str(workspace))",
    "        _zero_modules_mode = bool(not _all_mods and \"all\" not in _sec_cats)",
    "        if _zero_modules_mode:",
    "            _no_mod_report = (",
    "                f\"# \u5206\u6790\u4efb\u52a1\u5df2\u5b8c\u6210\uff08\u8fc7\u6ee4\u540e\u6ca1\u6709\u7b26\u5408\u8981\u6c42\u7684\u6a21\u5757\uff09\\n\\n\"",
    "                f\"\u7ecf Stage 1.5 \u5b89\u5168\u7ef4\u5ea6\u8fc7\u6ee4\uff0c\u76ee\u6807\u4e2d\u6240\u6709\u6a21\u5757\u5747\u4e0e\u6307\u5b9a\u5b89\u5168\u7ef4\u5ea6\u65e0\u5173\uff0c\u65e0\u9700\u8fdb\u884c\u540e\u7eed\u5206\u6790\u3002\\n\\n\"",
    "                f\"## \u6307\u5b9a\u5b89\u5168\u7ef4\u5ea6\\n\\n\"",
    "                + \"\\n\".join(f\"- `{c}`\" for c in _sec_cats)",
    "                + \"\\n\\n\u76ee\u6807\u4e2d\u4e0d\u5305\u542b\u4e0e\u6307\u5b9a\u5b89\u5168\u7ef4\u5ea6\u76f8\u5173\u7684\u7ec4\u4ef6\u3002\"",
    "                f\"\u82e5\u9700\u5206\u6790\u5168\u91cf\u5185\u5bb9\uff0c\u53ef\u5c06 `security_focus_categories` \u8bbe\u7f6e\u4e3a `[\\\"all\\\"]` \u91cd\u65b0\u8fd0\u884c\u4efb\u52a1\u3002\\n\"",
    "            )",
    "            (workspace / \"final_report.md\").write_text(_no_mod_report, encoding=\"utf-8\")",
    "            ctx.emit_event(\"log\", level=\"info\",",
    "                           msg=\"[S4b] 0 \u4e2a\u5b89\u5168\u76f8\u5173\u6a21\u5757\uff0c\u5df2\u5199\u5165\u8bf4\u660e\u62a5\u544a\uff0c\u8df3\u8fc7 LLM\uff0c\u7ee7\u7eed\u7ec4\u88c5\u8f93\u51fa\u76ee\u5f55\")",
    "        else:",
]

# Find checkpoint check line (should be right after insert_after+1)
checkpoint_line = insert_after + 1
while checkpoint_line < len(lines) and not lines[checkpoint_line].strip():
    checkpoint_line += 1
print(f"checkpoint_line: line {checkpoint_line+1}: {repr(lines[checkpoint_line][:60])}")

# The body of the W+J loop (from checkpoint_line to output_assembly_start-1)
# needs to be indented by 4 spaces to become the else: branch
body_start = checkpoint_line
body_end = output_assembly_start  # exclusive

print(f"W+J loop body: lines {body_start+1} to {body_end}")

# Build new content:
# 1. Lines 0..insert_after (inclusive)
# 2. zero_block
# 3. Lines body_start..body_end-1 each indented by 4 spaces
# 4. Lines body_end..end

new_lines = []

# Part 1: up to and including insert_after
new_lines.extend(lines[:insert_after+1])

# Part 2: zero-module detection block
new_lines.extend(zero_block)

# Part 3: W+J body, indented 4 spaces more
for i in range(body_start, body_end):
    line = lines[i]
    if line.strip():  # non-empty
        new_lines.append("    " + line)
    else:
        new_lines.append(line)

# Part 4: output assembly and rest
new_lines.extend(lines[body_end:])

new_content = "\n".join(new_lines)
with open(path, "w", encoding="utf-8", newline="\n") as f:
    f.write(new_content)
print(f"Done. Total lines: {len(new_lines)}")

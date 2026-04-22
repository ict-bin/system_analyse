# Stage 3 性能分析与优化方案

## 当前状态（2026-04-22）

- 已完成：72 / 199 模块（实时）
- 串行累计耗时：~10h（72模块）
- 平均：9m03s / 模块
- 剩余预估：~11h（2并行）

---

## 根因分析

### 1. Tool Call 爆炸：42.6次/模块（核心瓶颈）

```
mod                              msgs  tools files   in_tok  out_tok
network_services_security         148     94     5  4,380,078   24,930
base_libraries_business           134     88     5  1,121,837   20,824
layer2_lacp                       124     79     6  1,556,754   19,187
...
total (72模块)：3,067 tool_calls, 66.8M in_tokens
```

每次 tool call = 1次 LLM 推理（含 thinking block）≈ 30-180秒

### 2. Tool Call 构成分析

| 类型 | 数量 | 占比 | 原因 |
|------|------|------|------|
| 冗余工具（readelf/nm/objdump）| 1,551 | 50.6% | LLM 自主选择更多工具 |
| 多余 strings 重复调用 | ~600 | 20% | LLM 不断重新分析 |
| 错误路径尝试 | 130 | 4.2% | 初始路径解析错误 |
| 有效调用（正确strings+write）| ~786 | 25% | 实际需要的 |

**结论：75% 的 tool call 是完全可以消除的。**

### 3. 路径解析失败（130次浪费调用）

每个模块开始时 LLM 先读错路径（`workspace/files.list` 而非 `modules/<mod>/files.list`），
然后 find 定位，再用正确路径。平均浪费 1.8 次调用 × 72模块 = 130次。

### 4. Token 累积爆炸

由于 session 历史随 tool call 线性增长，越靠后的 tool call 携带的 context 越大：
- 第 1 次调用：~2K tokens input
- 第 50 次调用：~50K tokens input  
- 第 94 次调用（network_services_security）：~90K tokens input
- 72模块合计：66.8M cumulative input tokens ← 极低效

---

## 优化方案（按优先级）

### 【优化1】Stage 3 文件预注入（影响最大，预估节省 85% 时间）

**原理**：和 Stage 2 sub-worker 完全一样，在 Python 侧预读文件内容注入 prompt，
Worker 不需要任何 tool call，直接输出报告。

**实现**：
```python
# _orchestrator_legacy.py - Stage 3 worker 调用前
def _pre_read_module(target_dir, mod_dir) -> str:
    """Python侧预读所有文件内容，注入到 Worker prompt。"""
    files = (mod_dir / "files.list").read_text().strip().splitlines()
    parts = [f"# 模块文件内容（已预读，直接分析，无需工具调用）\n"]
    for filepath in files[:20]:  # 最多20个文件防止超长
        abs_path = Path(target_dir) / filepath
        parts.append(f"\n## 文件: {filepath}")
        if abs_path.exists():
            magic = abs_path.read_bytes(4)
            if magic[:4] == b'\x7fELF':  # ELF
                import subprocess
                result = subprocess.run(
                    ["strings", "-n", "6", str(abs_path)],
                    capture_output=True, text=True, timeout=10
                )
                lines = result.stdout.splitlines()[:200]  # 200行足够
                parts.append(f"(ELF二进制, strings输出 {len(lines)} 行)")
                parts.append("```")
                parts.append("\n".join(lines))
                parts.append("```")
            else:  # 文本文件
                try:
                    text = abs_path.read_text(errors='replace')[:8000]
                    parts.append("(文本文件)")
                    parts.append("```")
                    parts.append(text)
                    parts.append("```")
                except:
                    parts.append("(无法读取)")
    return "\n".join(parts)
```

**修改 step3_analyse.md**：
- 说明文件内容已在 prompt 中提供
- 明确写报告到 `modules/<模块名>/module_report.md`（含精确模块名）
- 告知不需要调用任何工具（直接写报告）

**Worker 调用改为 `tools=[]`**（或只允许 `write`）

**预期效果**：
- tool calls: 42.6次 → 1次（只写报告）
- input tokens: ~900K/模块 → ~30K/模块（节省 97%）
- 耗时: avg 9min → avg 1-2min
- 总 Stage 3 时间: 15h → 2-3h

---

### 【优化2】parallel_modules = 4（影响中等，2x加速）

当前：`parallel_modules=2`
建议：`parallel_modules=4`

配合预注入（每请求更轻），GLM-5 的 max_model_len=202752 足够 4 个并发 30K token 的请求。
每 GPU 对话并发不超过 4 是安全的（视显存而定）。

---

### 【优化3】step3_analyse.md 提示词强化

即使没有预注入，也要改进：

```markdown
## ⚠️ 关键约束

1. **只允许使用 `strings -n 6 <路径> | head -200`，禁止 readelf/nm/objdump**
2. **每个文件只调用 strings 一次**
3. **报告必须写入 `modules/EXACT_NAME/module_report.md`**（不要用绝对路径）
4. **禁止在 workspace 根目录写文件**
```

---

### 【优化4】Stage 2 过度拆分问题（影响下次 full run）

当前：199个模块（平均5.8文件/模块，大量1-2文件模块）
建议：在 step2_refine.md 中限制最小粒度：

```markdown
## 拆分约束
- 每个子模块最少 4 个文件（否则不必要拆分）
- 如果拆分后某子模块文件数 < 4，合并相近子模块
- 过度拆分会显著增加 Stage 3 耗时（每个模块≥7min overhead）
```

---

## 当前测试run 预期完成时间

| 指标 | 值 |
|------|-----|
| 当前进度 | ~72/199 |
| 已耗时 | ~9h |
| 剩余预估 | 10-13h |
| 大模块风险 | system_management_base(29文件)可能 45-60min |
| 总预期完成 | 明天 05:00-08:00 |

---

## 下次 full run 优化后预期

| 指标 | 当前 | 优化后 |
|------|------|--------|
| avg/模块 | 9min | 1.5min |
| Stage 3 总时 | ~15h | 2.5h |
| Stage 3 tokens | 66M/72模块 | 2M/72模块 |
| parallel | 2 | 4 |
| 整体完成 | 15h | 3h |

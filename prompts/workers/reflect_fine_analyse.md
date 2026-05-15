你已完成模块分析报告，现在请进行自我审查。

# 自查清单

请逐项确认，发现问题**立即修正**：

1. **文件覆盖**：`files.list` 中的每个文件是否都在 `module_report.md` 中被分析？
   - 再次 `read files.list`，逐行对照报告，查漏补缺

2. **分析准确性**：每个文件的功能描述是否与实际内容一致？
   - 先查 `details/<path>.json` 的 `summary`/`functions`/`symbols` 字段，确认描述与 details 信息匹配
   - 若描述与 details 摘要明显矛盾，查 details JSON 更正，**不要直接 read 源文件**（除非 details 不存在或摘要为空/[需补充]）
   - ELF 文件：基于 details 中的 symbols/imports 字段核查，禁止用 nm/readelf
   - 文本文件：details 中的 summary/functions 通常已够，只有需要具体行号时才 read

3. **分类自检**：是否在报告中做了分类合理性判断？
   - 如发现有文件不属于本模块，是否标注了 `[分类问题]`？
   - 如分类合理，是否标注了 `[分类合理]`？

4. **威胁分析**：STRIDE 六个维度是否都覆盖到？
   - 每个威胁是否标注了文件位置？
   - 风险等级是否合理？

5. **报告格式**：`module_report.md` 是否包含完整的五个章节？
   - 文件清单 / 模块功能 / 分类自检 / STRIDE / 暴露面评估

**如果一切正确，说明确认结论。如果发现问题，修正 `module_report.md` 后说明修改内容。**

用 `<result>自查结论</result>` 包裹结果。

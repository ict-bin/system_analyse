你已完成粗分类，现在请自查**覆盖率**。

# 自查脚本

直接运行：

```bash
# filtered_files.txt 是权威基准，它由过滤阶段生成，只包含应分类的文件
if [ -f filtered_files.txt ]; then
    sort filtered_files.txt > /tmp/a.txt
else
    # 如果没有 filtered_files.txt，用 target/ 符号链接扫描
    find target -type f | sed 's|^target/||' | sort > /tmp/a.txt
fi
TOTAL=$(wc -l < /tmp/a.txt)
cat modules/*/files.list 2>/dev/null | sort -u > /tmp/c.txt
CLASSIFIED=$(wc -l < /tmp/c.txt)
echo "总文件: $TOTAL  已分类: $CLASSIFIED"

if [ "$TOTAL" -ne "$CLASSIFIED" ]; then
    echo "⚠️ 有遗漏！差额: $((TOTAL - CLASSIFIED))"
    comm -23 /tmp/a.txt /tmp/c.txt > /tmp/missing.txt
    echo "遗漏文件前 30 行："
    head -30 /tmp/missing.txt
fi
```

- 如有遗漏 → 编写补充脚本将遗漏文件归入已有模块或新建模块
- 如有重复 → `sort -u modules/<模块>/files.list -o modules/<模块>/files.list`
- ⚠️ files.list 中必须是**相对路径**（不含 `/data/target/` 前缀，也不含 `target/` 前缀）

用 `<result>自查结论（覆盖率）</result>` 结束。

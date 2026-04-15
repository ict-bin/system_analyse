你已完成粗分类，现在请自查**覆盖率**。

# 自查脚本

直接运行：

```bash
TOTAL=$(find /data/target -type f | wc -l)
CLASSIFIED=$(cat */files.list 2>/dev/null | sort -u | wc -l)
echo "总文件: $TOTAL  已分类: $CLASSIFIED"

if [ "$TOTAL" -ne "$CLASSIFIED" ]; then
    echo "⚠️ 有遗漏！差额: $((TOTAL - CLASSIFIED))"
    cat */files.list | sort -u > /tmp/c.txt
    find /data/target -type f | sort > /tmp/a.txt
    comm -23 /tmp/a.txt /tmp/c.txt > /tmp/missing.txt
    echo "遗漏文件前 30 行："
    head -30 /tmp/missing.txt
    echo "--- 按目录统计 ---"
    cat /tmp/missing.txt | awk -F/ '{print $4}' | sort | uniq -c | sort -rn | head -20
fi
```

- 如有遗漏 → 编写补充脚本将遗漏文件归入已有模块或新建模块
- 如有重复 → `sort -u files.list -o files.list`
- **不要读取文件内容**

用 `<result>自查结论（覆盖率）</result>` 结束。

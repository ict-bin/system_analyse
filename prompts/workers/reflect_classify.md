你已完成粗分类，现在请快速自查**覆盖率**。

# 自查（只检查覆盖率，不检查精度）

```bash
# 1. 总文件数
find /data/target -type f | wc -l

# 2. 已分类文件数（去重）
cat */files.list 2>/dev/null | sort -u | wc -l

# 3. 未分类文件
cat */files.list 2>/dev/null | sort -u > /tmp/classified.txt
find /data/target -type f | sort > /tmp/all.txt
comm -23 /tmp/all.txt /tmp/classified.txt > /tmp/missing.txt
wc -l /tmp/missing.txt
head -50 /tmp/missing.txt
```

- 如有遗漏文件，按路径关键词归入已有模块或创建新模块
- 如有重复分类，用 `sort files.list | uniq > files.list.tmp && mv files.list.tmp files.list` 去重
- **不要读取文件内容**，只看路径

用 `<result>自查结论（覆盖率）</result>` 结束。

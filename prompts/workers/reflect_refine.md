你已完成模块细分判断，现在请**仅验证文件完整性**。

# ⚠️ 重要：不要推翻你的拆分决策

你的拆分/不拆分决定已经被评审员认可。自查的目的**仅仅是确认文件没有丢失**，不是重新决定是否拆分。

# 自查步骤

## 1. 文件完整性校验（必做）

```bash
bash /opt/system_analyse/scripts/check_classification.sh /data/target .
```

查看输出：
- `Missing files: 0` → ✅ 文件完整
- `Missing files: N` → ❌ 有文件丢失，需要修复

## 2. 如果有文件丢失

找出丢失的文件并归入现有模块：
```bash
# 查看丢失文件
cat /tmp/missing_files.txt | head -20

# 归入合适的模块
cat /tmp/missing_files.txt >> <合适模块>/files.list
sort -u <合适模块>/files.list -o <合适模块>/files.list
```

## 3. 如果文件完整

直接确认即可。**不要**修改任何目录结构。

用 `<result>自查结论：文件完整/已修复N个遗漏</result>` 结束。

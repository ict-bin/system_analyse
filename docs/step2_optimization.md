# Step 2 优化方案

## 核心思路
将 Step 2 从"LLM 逐模块交互式决策"改为"脚本预处理 + LLM 只处理边界情况"。

## 改动 1：orchestrator 小模块自动跳过

文件数 ≤ 5 的模块自动通过 Stage 2，不调 Worker/Judge。

```python
# orchestrator.py Stage 2 循环开始处
REFINE_SKIP_THRESHOLD = 5

while modules_to_refine:
    mod_name = modules_to_refine.pop(0)
    mod_dir = _get_modules_root(str(workspace)) / mod_name
    flist = mod_dir / "files.list"
    if flist.exists():
        file_count = sum(1 for l in flist.read_text("utf-8").splitlines() if l.strip())
        if file_count <= REFINE_SKIP_THRESHOLD:
            self._emit("stage_result", task_id, stage=2, module=mod_name,
                       split=False, new_modules=[], skipped=True)
            refined_modules.add(mod_name)
            continue
    # ... 正常的 Worker+Judge 流程
```

## 改动 2：step2_refine.md — 强制用脚本处理大模块

```markdown
# 核心原则
1. **文件数 > 20 时必须用 bash 脚本拆分**，禁止手动逐文件分析
2. **不读文件内容**（和 Step 1 一样），只看文件名/路径关键词
3. **拆分前后文件总数必须一致**

# 大模块快速拆分模板
​```bash
#!/bin/bash
BEFORE=$(wc -l < files.list)
# 按关键词分组
while IFS= read -r f; do
  name=$(basename "$f" | tr '[:upper:]' '[:lower:]')
  case "$name" in
    *bgp*)  echo "$f" >> ../bgp_scripts/files.list ;;
    *ospf*) echo "$f" >> ../ospf_scripts/files.list ;;
    *)      echo "$f" >> ../misc_scripts/files.list ;;
  esac
done < files.list
# 校验
AFTER=$(cat ../bgp_scripts/files.list ../ospf_scripts/files.list ../misc_scripts/files.list | wc -l)
[ "$BEFORE" -eq "$AFTER" ] && echo "OK" || echo "MISMATCH"
​```
```

## 改动 3：step2_check_refine.md — Judge 用脚本做确定性校验

Judge 必须先运行 `check_classification.sh` 验证文件完整性，再做合理性判断。
文件丢失 → 直接 0 分。（已实现）

## 预期效果

| 场景 | 优化前 | 优化后 |
|------|--------|--------|
| 5 文件以下的模块（约占 40%） | Worker+Judge 至少 1 轮 | **跳过，0 调用** |
| 20+ 文件的大模块 | Worker LLM 逐行分析 | **Worker 写脚本一次完成** |
| Judge 评审 | 纯 LLM 判断，波动大 | **脚本校验文件完整 + LLM 判断合理性** |

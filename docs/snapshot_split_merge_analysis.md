# 模块文件快照、切分、合并、删除 — 完整实现分析

## 一、核心数据结构（文件系统）

```
workspace/
├── .s2_snapshots/               # 快照目录
│   └── <mod_name>.snapshot      # 模块首次进入 S2 Refine 时的 files.list 拷贝
├── modules/                     # 模块根目录
│   ├── <mod_name>/
│   │   ├── files.list           # 当前模块包含的文件（每行一个相对路径）
│   │   ├── files.list.lock      # ★ Worker bash 用 flock 的文件锁（空文件，0 字节）
│   │   ├── split/               # S2 Refine Worker 产出拆分草稿
│   │   │   ├── <child>/
│   │   │   │   └── files.list   # 候选新子模块的文件清单
│   │   │   └── _merge_to/
│   │   │       └── <target>/
│   │   │           └── files.list # 候选合并到已有模块的文件清单
│   │   ├── deleted/
│   │   │   └── files.list       # 本轮提议排除/删除的文件
│   │   └── recover/
│   │       └── files.list       # Judge 认为 Worker 误删、要求恢复的文件
│   └── ...
└── deleted.list                 # 全局已确认排除文件（append-only）
```

## 二、快照（.s2_snapshots）

### 2.1 创建时机（3 处）

| 位置 | 条件 | 代码 |
|------|------|------|
| `s2_refine.py:238` `_refine_one()` | 模块首次进入 S2 Refine，snapshot 不存在 | `shutil.copy2(str(mod_dir / "files.list"), str(snapshot_path))` |
| `helpers.py:601` `commit_split_plan()` | split/merge 提交后新子模块快照不存在 | `shutil.copy2(str(child_flist), str(child_snapshot))` |
| `s2_refine.py` `_global_completeness_check()` | 补分类后遍历所有模块补快照 | `shutil.copy2(str(_flist), str(_snap))` |

### 2.2 作用

1. **check_module.sh 校验基线**：拆分后的文件集合必须覆盖快照中的所有文件（快照 = split 子模块 ∪ merge 目标 ∪ deleted）
2. **重试恢复**：`restore_module_for_retry()` 从快照恢复原始 `files.list`，删除上轮的 split/deleted
3. **提交前校验**：`commit_split_plan()` 保证 `covered == snap_files`，防止丢文件或凭空新增

### 2.3 当前问题

```
问题 A: 快照 ≠ 真实初始状态
快照在模块首次进入 S2 时创建，但此时 files.list 可能已被 S1 Classify 修正过多次。
快照时间点和模块创建时间点不一致，导致"原始文件列表"语义模糊。

问题 B: 快照不可变但 files.list 可变
S2 多轮重试中 files.list 被反复覆盖（从快照恢复 → Worker 修改 → Judge 不通过 → 再次恢复）。
但快照本身只存一次（`if not snapshot_path.exists()`），后续变更不会回溯到快照。

问题 C: redo 模块缺快照
_global_completeness_check 的 reclassify 创建新模块后，
依赖队列中的 _refine_one() 来补快照。如果队列未执行完就 cancel，快照永久缺失。
```

## 三、模块切分（split/）

### 3.1 完整流程

```text
_refine_one(mod_name):
  1. 创建快照 (如果不存在)
  2. 检查 checkpoint → 已完成则跳过
  3. 加载 details/ 文件摘要 → file_summary
  4. for attempt in range(max_iter):
     a. restore_module_for_retry()  ← 恢复 files.list + 删 split/deleted
     b. Worker(step2_refine.md) 读取摘要 + prompt:
        "如需拆分，只能在 modules/<mod>/split/ 下创建候选子模块"
        Worker 通过 bash 命令批量创建 split/<child>/files.list
     c. fix_orphan_dirs_before_judge() ← 修复路径拼写错误
     d. Judge(step2_check_refine.md):
        - 运行 check_module.sh
        - 读取 split/ 目录内容
        - 评估拆分合理性（粒度、内聚性、是否有跨组件拆分）
     e. 投票判定:
        - if voted_pass and min_rounds met:
            → commit_split_plan(workspace, mod_name)
            → archive_module_deletions() → deleted.list
            → 新子模块入队 (_queue.put(nm))
            → 写 checkpoint
            → return  ← 模块完成
        - if voted_pass but min_rounds not met:
            → 注入 reflect_prompt + judge 反馈 → 继续下一轮
        - if voted_fail:
            → process_module_recover() ← 从 deleted/ 恢复误删文件
            → write_judge_feedback() → judge_output/
            → 构建 feedback (read judge_output + guidance)
            → 继续下一轮
     f. if forced_pass (max_rounds exceeded):
        → commit_split_plan + archive + checkpoint → return
     g. raise StageError (max_rounds reached without pass)
```

### 3.2 commit_split_plan 详细过程

```python
commit_split_plan(workspace, mod_name):
  1. 收集所有 split/<child>/files.list → child_map
  2. 收集所有 split/_merge_to/<target>/files.list → merge_map
  3. 收集 deleted/files.list → deleted_files
  4. 计算 covered = child_map ∪ merge_map ∪ deleted_files
  5. ★ 校验: covered == snap_files（快照文件）
     - missing: 快照中有 but 未被覆盖 → StageError 拒绝提交
     - extra: 快照中没有 but 出现了 → StageError 拒绝提交
  6. 写入子模块:
     - child == mod_name: 覆盖原 files.list（保留父模块）
     - child != mod_name: 创建/追加到 modules/<child>/files.list
  7. 写入合并目标: 追加到 modules/<target>/files.list
  8. 父模块处理:
     - 如果 mod_name 不在 child_map 中:
       - 有 deleted/: delete files.list（模块变空壳）
       - 无 deleted/: rmtree(mod_dir)（模块完全消失）
     - 如果在: 覆盖 files.list
  9. 删除 split/ 草稿
  10. ★ 为新子模块 + merge 目标创建快照
```

### 3.3 当前问题

```
问题 D: Worker bash 路径拼写错误
Worker 用 bash 创建 split/ 子目录时可能写成 modules<mod>/split/<child>
(缺少 /），导致文件落在 workspace 根下而非 modules/ 下。
check_module.sh 找不到它们 → 永远 MISSING。
→ 通过 fix_orphan_dirs_before_judge() 自动修复，但这是治标。

问题 E: Commit 前的覆盖度校验过于严格
covered == snap_files 要求完全相等。
但 snapshot 可能在 S1 Classify 时创建得不准（多/少文件），
导致合法拆分被 StageError 拒绝。

问题 F: split + merge 同时存在的语义冲突
同时有 split/<child>/ 和 split/_merge_to/<target>/ 时，
同一批文件被分配到两个目的地？check_module.sh 不做去重，
commit 阶段靠 covered == snap_files 保护，但校验前可能已经出错。

问题 G: 多轮重试中 files.list 状态不一致
第 1 轮: 从快照恢复 → Worker 写入 split → Judge 不通过
第 2 轮: 再次从快照恢复 → Worker 重新写 split → 但上轮的 split 残留
       ↑ restore_module_for_retry() 会删 split/ 和 deleted/
      但 workspace/deleted.list 中的历史记录不会被回滚

问题 H: files.list.lock 不被 Python 代码管理
Worker step2_refine.md prompt 指导用 flock + files.list.lock 做写锁，
但 Python 侧完全不感知这个锁。并发情况下:
- Worker A 通过 flock 锁定 db_utils/files.list.lock
- Worker B 的 bash 脚本直接操作另一个模块
- 同一模块的多次 bash 并发访问无 Python 级别保护
```

## 四、模块合并（_merge_to/）

### 4.1 流程

```text
Worker 产出:
  modules/<src>/split/_merge_to/<dst>/files.list
  → 表示 src 模块中的某些文件应并入 dst 模块

commit_split_plan():
  for each _merge_to/<dst>/:
    existing = read_module_files(modules/<dst>/)
    _write_unique_files(modules/<dst>/files.list, existing ∪ files)

与 split 不同: 合并不创建新模块，只追加到已有模块
```

### 4.2 当前问题

```
问题 I: 合并目标不存在时无保护
如果 dst 模块在 S1 Classify 后被 SecurityFilter 删除了，
merge 的目标目录不存在，commit 阶段会创建新目录但无快照。

问题 J: 合并后 dst 的快照未更新
dst 模块已经过 S2 Refine（有 checkpoint），但合并操作
往它的 files.list 追加了新文件。此时 dst 的快照仍是旧的，
导致 check_module.sh 对 dst 模块的校验不准确。

问题 K: 多模块同时 merge 到同一个 dst 无冲突保护
两个 Worker 同时 split 两个不同模块，都 merge 到同一个 dst。
commit 时各自读 existing → 写回，可能互相覆盖。
（但受 asyncio.Queue 顺序性约束，不会完全并发，只是语义上脆弱）
```

## 五、文件删除（deleted/）

### 5.1 流程

```text
Worker 产出 deleted/:
  modules/<mod>/deleted/files.list
  → Worker 认为这些文件不需要安全分析（构建脚本/测试/文档等）

Judge 阶段:
  - check_module.sh 把 deleted/ 中的文件算入"已处理"
  - Judge 检查是否有合理文件被误删

commit 后:
  archive_module_deletions():
    1. 读 deleted/files.list → 追加到 workspace/deleted.list (asyncio.Lock 保护)
    2. rmtree(deleted/)
  ↓
  workspace/deleted.list:
    全局列表，每行一个文件，跨模块累积
    被 check_module.sh 和 _global_completeness_check 读取
    用于过滤"已确认排除的文件"

Judge 误删恢复:
  process_module_recover():
    Judge 认为 Worker 误删 → 在 modules/<mod>/recover/files.list 中列出
    → 从 deleted/ 移回 files.list
    → rmtree(recover/)
```

### 5.2 当前问题

```
问题 L: deleted.list 只增不删
archive_module_deletions() 只 append 到 deleted.list，永不删除。
一旦文件被标记排除，后续任何模块都无法再包含它。
如果误标记，只能手动编辑文件修复。

问题 M: 多轮重试中 deleted/ 被反复归档
第 1 轮: Worker 创建 deleted/ → Judge 不通过 → archive → deleted.list
第 2 轮: Worker 从快照恢复 → 重新创建 deleted/ → archive → deleted.list
同批文件在 deleted.list 中出现多次（重复行），但不影响功能。

问题 N: recover/ 和 deleted/ 的循环
Worker 删 → Judge 恢复 → 下一轮 Worker 又删 → 下一轮 Judge 又恢复
每次恢复都调用 process_module_recover() → 从 deleted/ 移回 files.list
→ 但 deleted.list 中已有记录，check_module.sh 可能错误地认为文件"已排除"
```

## 六、check_module.sh 校验逻辑

### 6.1 三步校验

```bash
有快照:
  Step 1: 收集 LOCAL = files.list ∪ split/*/files.list ∪ split/_merge_to/*/files.list ∪ deleted/files.list
  Step 2: MAYBE_MIGRATED = SNAPSHOT - LOCAL  （在快照中但不在当前模块+草稿中的文件）
  Step 3: 搜索所有其他 modules/*/files.list 和 workspace/deleted.list
          - 找到 → MIGRATED_OK
          - 找不到 → TRULY_MISSING

无快照:
  直接逐行检查 files.list 中的文件在 target_dir 下是否存在
  → 不存在则 MISSING
```

### 6.2 当前问题

```
问题 O: 无快照时只能验证文件存在性，不能验证完整性
返回 "Missing files: -1" 和 "Missing files: 0" 两种状态。
-1 被 Judge 解读为"完全失败"，即使 files.list 完全正确。

问题 P: 有快照时 check_module.sh 校验当前快照 ≠ 初始分类结果
快照是在 S2 refine 开始时创建的，而 files.list 可能已被 S1 修改。
快照不一定等于 S1 的正确分类结果。

问题 Q: check_module.sh 依赖 /tmp/ 临时文件
多个 Judge 并发运行时可能冲突（使用 PID 区分），
但 /tmp/ 清洁失败时有残留。

问题 R: deleted.list 中的文件被无条件跳过
check_module.sh 把 deleted.list 纳入"已处理"集合。
但 deleted.list 是跨模块累积的，如果模块 A 的误删文件在 deleted.list 中，
模块 B 的合法文件恰好同名路径，会被错误跳过。
```

## 七、整体架构问题总结

```
┌─────────────────────────────────────────────────────────┐
│ 核心矛盾                                                 │
├─────────────────────────────────────────────────────────┤
│ 1. 快照是 S2 起点概念，但 S1 和 redo 没有统一快照时机      │
│ 2. split/merge/delete 是 Worker bash 的自由产物，          │
│    Python 只在 commit 时验证，中间无一致性保护              │
│ 3. files.list 被多轮重试反复覆盖，但 deleted.list 只增不删  │
│ 4. 合并目标缺少快照更新，后续 check_module 校验不准确       │
│ 5. 所有校验依赖 bash 脚本和文件系统临时文件，无原子事务保护   │
└─────────────────────────────────────────────────────────┘

建议优化方向:
  A. 快照统一在模块 files.list 首次写入时创建（S1 Classify 产出时）
  B. split/merge/delete 改为 Worker 写 JSON manifest，Python 原子提交
  C. deleted.list 改为 per-module 的 manifest，不与全局混淆
  D. check_module.sh 改为 Python 函数，避免 bash + /tmp 的脆弱性
  E. 所有写操作加 Python 级别锁，避免 bash flock 和 asyncio 双重锁
```

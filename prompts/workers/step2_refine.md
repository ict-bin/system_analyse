你是一位资深的嵌入式系统安全专家，正在进行**精细分类**。

# 任务

检查当前模块的 `files.list`，判断是否需要拆分或重新归类。

# 步骤

1. `read files.list` 查看文件列表
2. 分析文件名和路径，判断是否混杂了不同协议/服务
3. **可以读取关键文件内容**辅助判断（配置文件头部、二进制 `strings` 等）
4. 做出拆分/保留决策

# 判断标准

**需要拆分的情况：**
- 包含多个不同协议（如 BGP + OSPF 混在一起）
- 包含不同服务（如 SSH + Telnet）
- 包含功能完全不同的组件（如初始化脚本 + 加密库）
- 大杂烩模块（如 `system_common/` 包含 50+ 文件且分属不同功能）

**不需要拆分的情况：**
- 同一协议/服务的不同文件（如 bgpd + bgp.conf + libbgp.so）
- 同一功能的配置和二进制
- 文件数 ≤ 5 且功能相关

# 拆分操作

```bash
# 创建新模块
mkdir -p <新模块1>
grep -i '<关键词1>' files.list >> <新模块1>/files.list
mkdir -p <新模块2>
grep -i '<关键词2>' files.list >> <新模块2>/files.list

# 确保不遗漏：检查剩余
cat <新模块1>/files.list <新模块2>/files.list | sort > /tmp/moved.txt
sort files.list > /tmp/orig.txt
comm -23 /tmp/orig.txt /tmp/moved.txt  # 应为空或归入某个模块

# 删除原模块
rm -rf <当前模块名>
```

# 如果不需要拆分

直接说明理由。

用 `<result>拆分/未拆分 + 理由</result>` 结束。

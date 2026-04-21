你是一位嵌入式系统分析专家。你的任务是**快速探索**目标目录并生成分类关键词。

# 任务

探索 `/data/target` 的目录结构和文件特征，生成适合本软件包的分类关键词列表。

⚠️ **不要分类文件！只需要输出关键词列表。** 分类工作由后续步骤完成。

# 步骤

## 1. 探索目录结构

```bash
echo "=== 文件总数 ==="
find /data/target -type f | wc -l
echo "=== 目录结构 ==="
find /data/target -maxdepth 3 -type d | head -50
echo "=== 文件名模式 ==="
find /data/target -type f | shuf | head -30
echo "=== 扩展名分布 ==="
find /data/target -type f | sed 's/.*\.//' | sort | uniq -c | sort -rn | head -20
```

## 2. 抽样文件内容（取 10-20 个有代表性的文件）

```bash
# 抽样不同类型的文件，提取功能关键词
for f in $(find /data/target -type f | shuf | head -20); do
    echo "--- $(basename $f) ---"
    # 文本文件看前几行
    head -20 "$f" 2>/dev/null || strings "$f" 2>/dev/null | head -20
    echo ""
done
```

## 3. 根据观察，生成关键词列表

将关键词写入 `keywords.txt`，每行一个关键词（小写）。关键词必须是**功能性关键词**：
- ✅ 协议名（bgp, ospf, dhcp, ipsec, ssl, snmp, radius, ...）
- ✅ 服务/功能名（firewall, routing, qos, acl, nat, vpn, auth, ...）
- ✅ 系统子系统名（driver, scheduler, watchdog, ...）

❌ **不要包含以下无意义词**：
- 架构名：aarch64, x86_64, arm, mips
- 打包格式：squashfs, lzma, rpm, tar
- 品牌/产品名：huawei, cisco, ne8000, vrp
- 配置格式名：yang, yin, netconf（除非是功能模块的核心词）
- 语言名：python, lua, perl
- 通用路径词：module, modules, lib, usr, opt, kernel（太泛）

```bash
cat > keywords.txt << 'EOF'
bgp
ospf
dhcp
...（根据实际观察填写）
EOF
```

# 输出要求

1. 将关键词写入 `keywords.txt` 文件
2. 用 `<result>关键词数量 + 简述本软件包特征</result>` 结束

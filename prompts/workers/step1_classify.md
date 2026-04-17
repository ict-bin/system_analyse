你是一位嵌入式系统分析专家，正在进行**快速粗分类**。

# 任务

对 `/data/target` 下所有文件按功能归类到模块。

⚠️ **目标是全覆盖、零遗漏。不要求精确，后续阶段会精细化。**

# 核心原则

1. **善用脚本**：**必须**编写 bash 脚本批量处理，**禁止**手动逐文件操作
2. **一次性处理**：写一个完整的分类脚本一次执行完，不要分多轮交互
3. **每个文件只归入一个模块**，用 `files.list` 记录
4. **按功能命名模块**，不要按文件名前缀/编号命名

# ⚠️ files.list 路径格式

**必须使用相对路径**（相对于 `/data/target`），不含前缀。

✅ 正确：`scripts/bgp_init.sh`、`lib/libcrypto.so`
❌ 错误：`/data/target/scripts/bgp_init.sh`

生成方法：`find /data/target -type f | sed 's|^/data/target/||'`

# 分类策略（按优先级）

## 策略 0：如果已有预扫描数据

如果你收到了预扫描摘要，`prescan/` 目录下已有按关键词分组的文件列表（已是相对路径）。直接使用：

```bash
#!/bin/bash
cd <工作目录>

for listfile in prescan/*.list; do
    kw=$(basename "$listfile" .list)
    mkdir -p "$kw"
    cp "$listfile" "$kw/files.list"
done

# 可以合并相近的关键词（如 dhcp+dhcpv6 → dhcp）
```

## 策略 1：目录结构清晰时

如果 `/data/target` 下有子目录且目录名有语义（如 `bgp/`、`configs/`），直接按子目录映射为模块。

## 策略 2：扁平目录或文件名无语义时

必须扫描文件内容关键词来分类：

```bash
#!/bin/bash
cd <工作目录>
KEYWORDS="bgp ospf dhcp ipsec snmp ntp ssh evpn mpls vxlan lacp bfd acl ztp upgrade patch kernel driver"

while IFS= read -r f; do
    rel=$(echo "$f" | sed 's|^/data/target/||')
    kw=$(head -50 "$f" 2>/dev/null | grep -oiE "$(echo $KEYWORDS | tr ' ' '|')" | head -1 | tr '[:upper:]' '[:lower:]')
    [ -z "$kw" ] && kw=$(strings "$f" 2>/dev/null | head -100 | grep -oiE "$(echo $KEYWORDS | tr ' ' '|')" | head -1 | tr '[:upper:]' '[:lower:]')
    [ -z "$kw" ] && kw="unknown"
    mkdir -p "$kw"
    echo "$rel" >> "$kw/files.list"
done < <(find /data/target -type f)
```

## 策略 3：混合策略

先按目录分，再对大目录内的文件按内容关键词细分。

# 校验

```bash
cat */files.list 2>/dev/null | sort -u > /tmp/classified.txt
find /data/target -type f | sed 's|^/data/target/||' | sort > /tmp/all.txt
comm -23 /tmp/all.txt /tmp/classified.txt > /tmp/remaining.txt
echo "遗漏文件: $(wc -l < /tmp/remaining.txt)"
```

如有遗漏，补充处理直到覆盖率 100%。

# 模块命名

小写英文 + 下划线，**按实际功能命名**：
- ✅ `bgp`, `ospf`, `dhcp`, `ipsec`, `system_upgrade`, `kernel_modules`
- ❌ `entry_02_scripts`（包编号不是功能）
- ❌ `network`（太笼统）

# 输出格式

每个模块一个目录，目录下有 `files.list`（**每行一个相对路径**）。

完成后用 `<result>分类摘要（模块数 + 总文件数 + 覆盖率）</result>` 结束。

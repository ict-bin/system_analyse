你是嵌入式系统安全分析专家。

# 任务

分析以下已提取的文件内容，为每个文件输出一行功能描述。

**文件内容已由系统预读提供，无需使用任何工具读取文件。**

# 输出格式

每个文件严格输出一行，用 `|` 分隔四列：

```
<相对路径> | <文件类型> | <主要功能模块> | <关键协议/函数/特征>
```

示例：
```
lib/libbras_dhcp.so | ELF shared library | BRAS地址分配 | DHCPv4/v6服务器、客户端、中继
lib/libbras_radius.so | ELF shared library | BRAS接入认证 | RADIUS、Diameter、EAP认证
lib/libssl.so | ELF shared library | TLS/SSL加密 | TLS握手、证书验证、AES/RSA
scripts/bgp_init.sh | shell script | BGP路由初始化 | BGP会话建立、路由表刷新
conf/ospf.conf | 配置文件 | OSPF路由配置 | area、cost、hello-interval
```

# 注意

- **第4列必须具体**：写出 3-5 个关键协议名/函数名/功能特征，用顿号分隔
- 对 ELF 文件：从提供的字符串中找出最有意义的函数名和协议标识符
- 对文本文件：从内容中提取核心功能关键词
- 不要写泛泛的描述（如"网络库"、"系统工具"），要写具体功能

用 `<result>已完成 N 个文件的分析</result>` 结束。

# BSC 土狗扫描器 v2

扫描 [four.meme](https://four.meme) 平台上新发行的 BSC 代币，自动筛选后推送到 Telegram。

## 数据源

| 数据源 | 用途 | 限流 |
|--------|------|------|
| **BSC 链上事件** | 代币发现（100% 覆盖） | 无（RPC 按区块范围查询） |
| **four.meme API** | 代币详情（价格/持币/社交链接/描述） | ~2 req/s |
| **GeckoTerminal** | K线 OHLCV 数据（含 Bonding Curve 阶段） | ~30 req/min，自动退避重试 |

## 三级筛选管线

| 阶段 | 条件 | 数据源 | 请求开销 |
|------|------|--------|----------|
| **初筛** | 发行 4h~3d、当前价 ≤ $0.00002、持币 ≥ 150、未推送 | 列表 API（批量） | 0 额外请求 |
| **详情筛** | 总量 = 10亿、社交媒体 ≥ 1 | four.meme 详情 | 每候选 1 请求 |
| **K线筛** | 历史最高价 ≤ $0.00014、前 2h 最高价 ≤ $0.00004 | GeckoTerminal OHLCV | 每候选 2 请求 |

逐级收窄，避免不必要的 API 调用。

## 全量覆盖机制

four.meme API 分页上限为 10 页 × 100 = 1000 条，无法一次覆盖 72h 时间窗口内的全部代币（BNB 类代币约 27000+）。

解决方案：**链上事件扫描 + API 数据丰富**

通过读取 BSC 链上 `TokenManager2` 合约的 `TokenCreate` 事件，实现**单轮 100% 覆盖**：

1. **链上扫描**：调用 `eth_getLogs` 按区块范围查询 `TokenCreate` 事件（72h ≈ 86400 块），获取全部代币地址、名称、总量、创建时间
2. **即时筛选**：在事件解码阶段直接过滤总量 ≠ 10 亿的代币，大幅缩小候选集
3. **API 丰富**：通过 four.meme 搜索 API 批量获取价格、持币人数、交易量等实时数据
4. **间隙补全**：链上发现但 API 未覆盖的代币，逐个查询 detail API 补全数据（每轮限 100 个，多轮累积）

技术细节：
- 合约地址：`0x5c952063c7fc8610FFDB798152D69F0B9550762b`（TokenManager2 V2）
- 事件签名：`TokenCreate(address,address,uint256,string,string,uint256,uint256,uint256)`
- 分片大小：2000 块/次（自动降级到 500），约 45-175 次 RPC 调用，耗时 ~30-60 秒
- 自动切换备用 RPC 节点，容错重试

## 热点交叉验证

自动抓取实时社会热点关键词，与代币名称/描述做交叉匹配。匹配热点的代币在通过三级筛选后**优先推送**（加分项，非必要条件）。

| 数据源 | 覆盖范围 | 更新频率 |
|--------|----------|----------|
| **微博热搜** | 中文社交热点 Top50 | 实时 |
| **Google Trends** | 全球搜索趋势（支持多地区） | 每日 |
| **Twitter/X** | 英文社交热门话题 ~60 个 | 实时 |

**匹配逻辑：**
- 热点关键词与代币 name、shortName、description 做子串匹配
- 短关键词（≤3字符）要求精确匹配名称，避免误匹配
- 支持反向匹配（代币名包含在热点词中，如代币 "张雪" 匹配热点 "张雪机车"）
- 按热点排名和来源加权评分，热点分高的代币排在推送队列前面
- 热点数据缓存 15 分钟，避免重复请求

## 快速开始

### 1. 安装依赖

```bash
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

### 2. 配置

```bash
cp config.example.json config.json
```

编辑 `config.json`，填入 Telegram Bot Token 和 Chat ID：

```json
{
    "telegram_bot_token": "你的 Bot Token",
    "telegram_chat_id": "你的 Chat ID"
}
```

### 3. 运行

```bash
python3 scanner.py
```

未配置 Telegram 时，筛选结果会打印到控制台。

## 配置参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `telegram_bot_token` | Telegram Bot Token | - |
| `telegram_chat_id` | 推送目标 Chat ID | - |
| `scan_interval_minutes` | 扫描间隔（分钟） | 15 |
| `max_push_count` | 每轮最多推送数量 | 3 |
| `min_age_hours` | 代币最小年龄（小时） | 2 |
| `max_age_hours` | 代币最大年龄（小时） | 72 |
| `max_price_current` | 当前价上限 (USD) | 0.00002 |
| `max_price_ath` | 历史最高价上限 (USD) | 0.00014 |
| `max_price_2h` | 前2小时最高价上限 (USD) | 0.00004 |
| `required_total_supply` | 要求的代币总量 | 1000000000 |
| `min_holders` | 最小持币地址数 | 150 |
| `min_social_links` | 最小社交媒体数 | 1 |
| `scan_rpc_url` | 链上扫描 RPC 节点（留空自动选择） | - |
| `proxy.enabled` | 是否启用代理 | false |
| `hotspot.enabled` | 是否启用热点匹配 | true |
| `hotspot.weibo` | 启用微博热搜 | true |
| `hotspot.google` | 启用 Google Trends | true |
| `hotspot.google_geos` | Google Trends 地区 | ["US","CN"] |
| `hotspot.twitter` | 启用 Twitter/X Trending | true |
| `trading.enabled` | 是否启用自动交易 | false |
| `trading.rpc_url` | BSC RPC 节点 | (默认自动选择) |
| `trading.slippage_pct` | 交易滑点 (%) | 12 |
| `trading.buy_fraction` | 每次买入占总资金比例 | 0.1 |
| `trading.min_buy_usd` | 最小买入金额 (USD) | 10 |
| `trading.max_buy_usd` | 最大买入金额 (USD) | 100 |
| `trading.tp_trigger_pct` | 止盈触发盈利百分比 | 100 |
| `trading.expire_hours` | 超期清仓时间 (小时) | 72 |
| `trading.monitor_interval_sec` | 盯盘间隔 (秒) | 60 |

所有参数支持热更新（每轮扫描重新读取配置文件）。

## 推送示例

```
🔍 BSC 土狗扫描报告
⏰ 2025-01-01 12:00 UTC

#1 SomeToken (STK)
📄 合约: 0x1234...abcd
💰 当前价: $0.0000012345
📈 前2h最高: $0.0000008000
👥 持币人数: 320
🔗 社交媒体: 2 个
  • Twitter
  • Telegram
🕐 创建: 2025-01-01 10:00 UTC
🔥 热点匹配: 张雪机车(weibo)
📝 这是一个示例代币描述...
🌐 four.meme | BscScan
```

## 注意事项

- 本工具仅用于信息扫描，不构成任何投资建议
- BSC 土狗币风险极高，请自行判断
- GeckoTerminal 免费 API 有限流（~30 req/min），程序内置自动退避重试
- 建议配合代理使用，避免 IP 被限流

## 自动交易模块

### 启用方式

1. 安装 web3 依赖：`pip3 install web3>=6.0.0`
2. 设置钱包私钥环境变量：`export BSC_PRIVATE_KEY=你的私钥`
3. 在 `config.json` 中设置 `trading.enabled: true`

### 买入策略

- 筛选通过的代币自动买入
- 自动检测交易场所：Bonding Curve（four.meme）或 PancakeSwap（已迁移）
- 买入金额：总资金的 1/10，最小 $10，最大 $100
- PancakeSwap 通过 V2 Router 执行 swap，使用 `SupportingFeeOnTransferTokens` 兼容税币
- Bonding Curve 通过 four.meme TokenManager2 的 `buyTokenAMAP` 执行

### 卖出策略

持仓监控每分钟扫描一次价格：

1. **回撤止盈**：盈利超过 100%（翻倍）后触发止盈追踪，当价格回撤到 `(买入价 + 记录最高价) / 2` 时自动卖出
2. **超期清仓**：持仓超过 3 天（72h）且仍未盈利，自动卖出

### 交易通知

买入和卖出都会推送 Telegram 通知，包含代币名称、合约地址、盈亏百分比和 BscScan 交易链接。

### 安全提醒

- **请使用专用钱包**，不要使用存有大量资产的主钱包
- 私钥优先通过环境变量传入，避免明文存储
- 自动交易默认关闭（`trading.enabled: false`），需手动开启
- 土狗币存在蜜罐（honeypot）、rug pull 等风险，自动交易无法完全规避

# BSC Token Scanner v4

扫描 [four.meme](https://four.meme) 平台上新发行的 BSC 代币，四级管线智能筛选后推送到 Telegram，可选自动交易。

## 数据源

| 数据源 | 用途 | 限流 |
|--------|------|------|
| four.meme Search API | 代币发现（批量列表） | ~2 req/s |
| four.meme Detail API | 代币详情（持币/社交链接/描述） | ~2 req/s |
| DexScreener API | K线价格数据（主要，快速） | ~300 req/min |
| GeckoTerminal OHLCV | K线数据（备选，精确） | ~30 req/min，自动退避重试 |
| BSCScan API | 链上真实持仓地址数 + 开发者行为 + 聪明钱追踪 | ~5 req/s |
| 微博/Google/Twitter | 实时热点关键词（加分项） | 各平台独立限流 |

## 四级筛选管线

| 阶段 | 条件 | 数据源 | 请求开销 |
|------|------|--------|----------|
| 初筛 | 币龄≤3天、当前价≤$0.00002、币龄<4h且价>$0.00001排除、持币地址粗筛 | Search API（批量） | 0 额外请求 |
| 详情筛 | 社交媒体≥1、持币(>1h:≥60,≤1h:≥30)、总量=10亿、当前价分段、币龄<4h且价>$0.00001排除 | Detail API + BSCScan | 每候选 1~2 请求 |
| K线筛 | 历史最高价≤$0.00004、前2h最高价≤$0.000023(币龄>1h)、当前价在最高价40%~90%、现价比底价高10%~100%(排除首根K线; ≤1h用全部K线最低价) | DexScreener (主) + GeckoTerminal (备) | 每候选 1~3 请求 |
| 链上行为筛 | 开发者减仓/清仓/撤流动性→排除、聪明钱减仓/清仓→排除、开发者加仓/加流动性→加分、聪明钱加仓→加分 | BSCScan API | 每候选 3~4 请求 |

逐级收窄，避免不必要的 API 调用。

## 筛选规则

1. **社交媒体 ≥ 1**：至少关联 1 个社交媒体（Twitter/Telegram/Website）
   - 币龄 > 1 小时：持币地址数 ≥ 60
   - 币龄 ≤ 1 小时：持币地址数 ≥ 30
2. **价格条件**：总量 10 亿，历史最高价 ≤ 0.00004
   - 币龄 ≤ 1 小时：当前价 ≤ 0.0000045
   - 币龄 > 1 小时：当前价 ≤ 0.00002
   - 币龄 < 4 小时：当前价 > 0.00001 时排除
   - 币龄 > 1 小时：前 2 小时最高价 ≤ 0.000023（通过 GeckoTerminal K线精确计算）
3. **价格区间**：当前价在历史最高价的 40%~90% 之间
4. **底价检查**：当前价比底价高 10%~100%
   - 币龄 > 1 小时：底价 = 排除第1小时K线后所有K线的最低价（排除发行价干扰）
   - 币龄 ≤ 1 小时：底价 = 全部K线的最低价（尚在首小时内，无需排除）
5. **热点新闻**（加分项，不作为筛选条件）：实时抓取社交媒体热点关键词，与代币名称/描述交叉匹配
   - 数据源：微博热搜 Top50、Google Trends（US/CN）、Twitter/X Trending
   - 匹配逻辑：短关键词(≤3字符)精确匹配名称，长关键词子串匹配，支持反向匹配
   - 按热点排名和来源加权评分，匹配到的代币标注 🔥
6. **链上行为分析**（Stage 4，排除+加分）：通过 BSCScan API 追踪开发者和聪明钱的链上行为
   - 排除信号（直接淘汰）：开发者减仓、开发者清仓、开发者撤流动性/减流动性、聪明钱减仓、聪明钱清仓
   - 加分信号（优先开仓）：开发者加仓、开发者加流动性、聪明钱加仓
   - 加分越多的代币在自动买入时优先级越高
   - 需要配置 `bscscan_api_key`，未配置时跳过此阶段
   - 聪明钱地址自动发现（无需手动维护）：
     - BSCScan Top Holders 交叉分析：分析多个热门代币的 Top 50 持有者，在 ≥2 个代币中都是大户的地址自动识别为聪明钱
     - DexScreener 热门代币 Top Traders 追踪
     - 支持手动补充地址（`smart_money.addresses`）
     - 地址库每小时自动刷新

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
| `max_push_count` | 每轮最多推送数量 | 100 |
| `max_age_hours` | 代币最大年龄（小时） | 72 |
| `bscscan_api_key` | BSCScan API Key（可选，获取链上持仓数） | - |
| `proxy.enabled` | 是否启用代理 | false |
| `hotspot.enabled` | 是否启用热点匹配 | true |
| `hotspot.weibo` | 启用微博热搜 | true |
| `hotspot.google` | 启用 Google Trends | true |
| `hotspot.google_geos` | Google Trends 地区 | ["US","CN"] |
| `hotspot.twitter` | 启用 Twitter/X Trending | true |
| `smart_money.enabled` | 是否启用聪明钱追踪 | false |
| `smart_money.addresses` | 手动维护的聪明钱钱包地址列表 | [] |
| `smart_money.min_cross_freq` | Top Holders 交叉分析最小出现频次 | 2 |
| `trading.enabled` | 是否启用自动交易 | false |
| `trading.rpc_url` | BSC RPC 节点 | (默认自动选择) |
| `trading.slippage_pct` | 交易滑点 (%) | 12 |
| `trading.max_positions` | 最大同时持仓数 | 5 |
| `trading.buy_fraction` | 每次买入占 USDT 余额比例 | 0.05 (1/20) |
| `trading.min_buy_usd` | 最小买入金额 (USD) | 5 |
| `trading.max_buy_usd` | 最大买入金额 (USD) | 100 |
| `trading.tp_trigger_pct` | 止盈触发盈利百分比 | 20 |
| `trading.rebuy_cooldown_profit_hours` | 盈利平仓后重买冷却时间 (小时) | 12 |
| `trading.rebuy_cooldown_loss_hours` | 亏损平仓后重买冷却时间 (小时) | 48 |
| `trading.expire_hours` | 超期清仓时间 (小时) | 48 |
| `trading.monitor_interval_sec` | 盯盘间隔 (秒) | 60 |

筛选阈值定义在 `scanner.py` 顶部常量中：

| 常量 | 默认值 | 说明 |
|------|--------|------|
| `MAX_AGE_HOURS` | 72 | 扫描时间窗口（小时） |
| `TOTAL_SUPPLY` | 1,000,000,000 | 代币总量要求（10亿） |
| `MAX_CURRENT_PRICE_OLD` | 0.00002 | 币龄>1h 当前价格上限 |
| `MAX_CURRENT_PRICE_YOUNG` | 0.0000045 | 币龄≤1h 当前价格上限 |
| `MAX_PRICE_UNDER_4H` | 0.00001 | 币龄<4h 当前价格上限 |
| `MAX_HIGH_PRICE` | 0.00004 | 历史最高价上限 |
| `MAX_EARLY_HIGH_PRICE` | 0.000023 | 币龄>1h时前2h最高价上限 |
| `PRICE_RATIO_LOW` | 0.4 | 当前价/最高价 下限 (40%) |
| `PRICE_RATIO_HIGH` | 0.9 | 当前价/最高价 上限 (90%) |
| `FLOOR_RATIO_LOW` | 0.1 | 现价比底价高的下限 (10%) |
| `FLOOR_RATIO_HIGH` | 1.0 | 现价比底价高的上限 (100%) |
| `HOLDERS_THRESHOLD_OLD` | 60 | 币龄>1h 持币地址数阈值 |
| `HOLDERS_THRESHOLD_YOUNG` | 30 | 币龄≤1h 持币地址数阈值 |
| `MIN_SOCIAL_COUNT` | 1 | 最少社交媒体关联数 |

所有配置参数支持热更新（每轮扫描重新读取配置文件）。

## 自动交易模块

### 启用方式

1. 安装 web3 依赖：`pip3 install web3>=6.0.0`
2. 设置钱包私钥环境变量：`export BSC_PRIVATE_KEY=你的私钥`
3. 在 `config.json` 中设置 `trading.enabled: true`

### 买入策略

- 筛选通过的代币自动买入，最多同时持仓 5 个代币
- 自动检测交易场所：Bonding Curve（four.meme）或 PancakeSwap（已迁移）
- 买入金额：USDT 余额的 1/20，最小 $5，最大 $100

### 卖出策略

持仓监控每分钟扫描一次价格：

1. **回撤止盈**：盈利超过 20% 后触发止盈追踪，当价格回撤到 `(买入价 + 记录最高价) / 2` 时自动卖出
2. **超期清仓**：持仓超过 2 天（48h）且仍未盈利，自动卖出
3. **重买冷却**：盈利平仓后 12 小时内不再买入同一代币，亏损平仓后 48 小时内不再买入

### 安全提醒

- **请使用专用钱包**，不要使用存有大量资产的主钱包
- 私钥优先通过环境变量传入，避免明文存储
- 自动交易默认关闭（`trading.enabled: false`），需手动开启
- 土狗币存在蜜罐（honeypot）、rug pull 等风险，自动交易无法完全规避

## 注意事项

- 本工具仅用于信息扫描，不构成任何投资建议
- BSC 土狗币风险极高，请自行判断
- 建议配合代理使用，避免 IP 被限流

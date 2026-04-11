# BSC Token Scanner v5

扫描 [four.meme](https://four.meme) 平台上新发行的 BSC 代币，链上发现 + 队列淘汰制智能筛选后推送到 Telegram，可选自动交易。

## v5 架构：链上发现 + 队列淘汰制

旧版通过 four.meme Search API 拉取代币列表，但实测该 API 只能覆盖平台约 1/3 的代币。v5 改为直接扫链上事件，100% 覆盖。

每 15 分钟执行一次:

1. **链上发现** (~1s): BSC RPC `eth_getLogs` → four.meme `TokenCreated` 事件 → 新代币地址
2. **入场筛** (~35s): four.meme Detail API → 淘汰无社交 / 总量≠10亿
3. **淘汰检查** (~15s): DexScreener 批量查价 + Detail API 查持币数 → 永久淘汰弃盘币
4. **钱包行为分析** (~20s): BscScan tokentx → 开发者行为 + 聪明钱自动发现 + 聪明钱行为追踪
5. **精筛** (~10s): K线/价格比/底价区间 + 钱包行为排除/加分 → 输出推荐

## 数据源

| 数据源 | 用途 | 限流 |
|--------|------|------|
| BSC RPC (publicnode) | 链上 TokenCreated 事件发现 | 无硬限制 |
| four.meme Detail API | 社交链接/进度 | ~5 req/s |
| BscScan API (Etherscan V2) | 链上持币数 + tokentx 开发者行为 + Top Holders 聪明钱自动发现 | ~5 req/s |
| DexScreener API | 批量价格+流动性查询 | ~300 req/min |
| GeckoTerminal OHLCV | K线数据（精筛用） | ~30 req/min |
| 微博/Google/Twitter | 实时热点关键词（加分项） | 各平台独立限流 |

## 淘汰规则（永久剔除）

满足任一条件即从队列中永久移除，不再关注：

| # | 条件 | 说明 |
|---|------|------|
| 1 | 价格从峰值跌 90%+ | 暴跌弃盘 |
| 2 | 持币地址从 30+ 跌破 10 | 大量抛售 |
| 3 | 无社交媒体 | 无运营意愿 |
| 4 | 流动性从 >$1k 跌破 $100 | 流动性枯竭 |
| 5 | 进度 < 1% 且币龄 > 4h | bonding curve 上的死币 |
| 6 | 币龄 > 5min 且最高持币数 < 3 | 无人问津 |
| 7 | 币龄 > 15min 且最高持币数 < 5 | 无人问津 |
| 8 | 币龄 > 1h 且最高持币数 < 10 | 热度不足 |
| 9 | 币龄 > 72h | 超出关注窗口 |

## 精筛规则

对队列中存活代币执行：

1. **持币地址数**：币龄 >1h ≥ 60，≤1h ≥ 30
2. **当前价**：≤1h ≤ $0.0000045，>1h ≤ $0.00002；币龄<4h 且价>$0.00001 时排除
3. **历史最高价** ≤ $0.00004
4. **前 2h 最高价** ≤ $0.000023（币龄 >1h，K线计算）
5. **当前价在最高价 40%~90%**
6. **现价比底价高 10%~100%**
   - 币龄 > 1h：底价 = 排除第1根K线后所有K线的最低价
   - 币龄 ≤ 1h：底价 = 全部K线的最低价
7. **热点匹配**（加分项）：微博热搜/Google Trends/Twitter 关键词与代币名称交叉匹配，标注 🔥
8. **钱包行为**（排除/加分）：
   - 排除信号（直接淘汰）：开发者减仓/清仓/撤池子、聪明钱减仓/清仓
   - 加分信号（优先开仓）：开发者加仓/加池子、聪明钱加仓

## 钱包行为分析

通过 BscScan API 追踪开发者和聪明钱的链上行为。

### 开发者行为判定

- 开发者地址来源：链上 TokenCreated 事件解码的 creator 字段
- 买入判定：从 DEX Router（PancakeSwap V2/V3/Universal）或零地址收到代币
- 卖出判定：转到 DEX Router 或其他非自己地址（转到零地址/死地址视为销毁，不算卖出）
- 清仓判定：卖出占收到总量 ≥ 90%
- 流动性操作：通过 LP token 的 mint/burn 检测（from=0x0 → 加池子，to=0x0 → 撤池子）

### 聪明钱自动发现

聪明钱地址无需手动维护，支持自动发现（1小时刷新）：

- 手动配置：config.json 的 `smart_money.addresses` 数组
- Top Holders 交叉分析：获取 four.meme HOT 列表前10个代币 → BscScan 查每个代币 Top 50 Holders → 在 ≥2 个代币中都是大户的地址自动识别为聪明钱
- 排除已知非聪明钱地址：交易所合约、DEX Router、WBNB、USDT、BUSD、USDC 等

### 聪明钱行为判定

- 买入：从 DEX Router 或零地址收到代币，按地址数计数
- 卖出：转到 DEX Router，按地址数计数

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
| `MAX_EARLY_HIGH_PRICE` | 0.000023 | 前2h最高价上限 |
| `PRICE_RATIO_LOW` | 0.4 | 当前价/最高价 下限 (40%) |
| `PRICE_RATIO_HIGH` | 0.9 | 当前价/最高价 上限 (90%) |
| `FLOOR_RATIO_LOW` | 0.1 | 现价比底价高的下限 (10%) |
| `FLOOR_RATIO_HIGH` | 1.0 | 现价比底价高的上限 (100%) |
| `HOLDERS_THRESHOLD_OLD` | 60 | 币龄>1h 持币地址数阈值 |
| `HOLDERS_THRESHOLD_YOUNG` | 30 | 币龄≤1h 持币地址数阈值 |
| `MIN_SOCIAL_COUNT` | 1 | 最少社交媒体关联数 |
| `ELIM_PRICE_DROP_PCT` | 0.90 | 价格跌幅淘汰阈值 |
| `ELIM_HOLDERS_FLOOR` | 10 | 持币数淘汰下限 |
| `ELIM_LIQ_FLOOR` | 100 | 流动性淘汰下限（USD） |
| `ELIM_EARLY_PEAK_HOLDERS` | 5 | 币龄>15min 最高持币数淘汰下限 |
| `ELIM_TINY_PEAK_HOLDERS` | 3 | 币龄>5min 最高持币数淘汰下限 |
| `ELIM_MID_PEAK_HOLDERS` | 10 | 币龄>1h 最高持币数淘汰下限 |

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

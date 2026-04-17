# BSC Token Scanner v6 — 极速版

扫描 [four.meme](https://four.meme) 平台上新发行的 BSC 代币，链上发现 + 队列淘汰制 + 动量精筛 + 精筛后防线，推送到 Telegram，可选自动交易。

## v6 架构：极速扫描

v5 每轮扫描需要 ~80s（K线、钱包分析、币安动态等），v6 砍掉淘汰阶段的慢环节，将深度检查移到精筛后防线（仅对个位数代币执行），兼顾速度和准确率。

每轮扫描流程:

1. **链上发现** (~1s): BSC RPC `eth_getLogs` → four.meme `TokenCreated` 事件 → 新代币地址
2. **入场筛** (~数秒): four.meme Detail API → 淘汰无社交 / 总量≠10亿 / 币龄>48h
3. **淘汰检查** (~数秒): DexScreener 批量查价(含交易量/买卖笔数) + GeckoTerminal 持币数 + Detail API → 永久淘汰弃盘币
4. **精筛** (瞬时): 动量筛选 — 价格加速度 + 持币增速 + 买卖比 + 回撤保护
5. **精筛后防线** (~数秒): BSCScan Top Holder 集中度 + 开发者行为分析 + 假K线检测 → 排除庄家控盘、跑路币和控盘刷量币
6. **仿盘检测**: 本地统计同名代币数量 (零 API 调用)

## 数据源

| 数据源 | 用途 | 限流 |
|--------|------|------|
| BSC RPC (publicnode) | 链上 TokenCreated 事件发现 | 无硬限制 |
| four.meme Detail API | 社交链接/持币数(bonding curve阶段)/进度/募资额 | ~5 req/s |
| DexScreener API | 批量价格+流动性+交易量+买卖笔数 | ~300 req/min |
| GeckoTerminal Token Info | 持币地址数 (已毕业代币, 链上索引) | ~30 req/min |
| BSCScan API (Etherscan V2) | Top Holder 集中度 + 开发者行为分析 (仅精筛后防线) | ~5 req/s |

### 峰值价格说明

峰值价格（peakPrice）是代币在队列存活期间记录到的最高价格（每轮用 DexScreener 实时价取 max），用于淘汰判断（价格跌 90% 淘汰）和精筛回撤保护。

### 目标价区间

目标价区间: 0.000001 ~ 0.0001。尽可能低价买入，等突破 0.0001 卖出。

峰值价格突破 0.0001 的代币标记为「已突破」，但保留在队列中继续跟踪更新，仅受币龄 >48h 淘汰。已突破代币同时出现在队列存活和已突破 tab 中，可正常参与精筛（毕业通道）。

### 持币数查询方案

持币数是筛选和淘汰的核心指标，但 four.meme 代币的生命周期跨越 bonding curve 和 DEX 两个阶段，没有单一数据源能覆盖全程。当前采用多源互补策略：

**优先级: GeckoTerminal > BSCScan 网页爬取 > four.meme Detail API > 缓存**

| 数据源 | 覆盖阶段 | 说明 |
|--------|----------|------|
| GeckoTerminal `/tokens/{addr}/info` | 已毕业 (DEX 阶段) | 免费、无需 key，链上索引数据，最准确 |
| BSCScan 网页爬取 | 已毕业 (DEX 阶段) | GT 查不到时的降级备选 |
| four.meme Detail API `holderCount` | Bonding curve 阶段 | 平台内部记账，毕业后返回 0 |
| 队列缓存 | 兜底 | 上一轮的持币数，避免数据断档 |

注意：只对已毕业代币（progress ≥ 1）发起 GT/BSCScan 查询，未毕业代币直接用 detail API，避免浪费请求。

## 淘汰规则（永久剔除）

满足任一条件即从队列中永久移除：

| # | 条件 | 说明 |
|---|------|------|
| 1 | 价格从峰值跌 90%+ | 暴跌弃盘 |
| 2 | 持币地址从 ≥30 跌破 10 | 大量抛售 |
| 3 | 持币数从峰值跌 70%+ (峰值≥50) | 僵尸币清理 |
| 4 | 无社交媒体 | 无运营意愿 |
| 5 | 流动性从 >$1k 跌破 $100 (仅已毕业) | 流动性枯竭 |
| 6 | 进度 < 1% 且币龄 > 2h | bonding curve 上的死币 |
| 6b | 进度 < 5% 且币龄 > 4h | 进度停滞 |
| 7 | 进度从峰值跌 50%+ 且币龄 > 6h | 热度消退 |
| 8 | 币龄 > 15min 且最高持币数 < 3 | 无人问津 |
| 9 | 币龄 > 1h 且最高持币数 < 5 | 热度不足 |
| 10 | 币龄 > 48h | 超出关注窗口 |
| 🚀 | 价格突破: 峰值价格 ≥ 0.0001 | 标记为已突破, 保留队列继续跟踪, 仅受币龄淘汰 |

## 精筛规则（动量筛选）

从队列存活币中找"正在起飞"的信号，三通道机制：

### 🚀 火箭通道（快速起飞币）

针对短时间内爆发的高质量币，跳过币龄/动量等需要多轮数据的慢条件，全部满足即通过：

| 条件 | 阈值 | 说明 |
|------|------|------|
| 进度 | ≥ 70% | 资金涌入快，接近毕业 |
| 持币地址数 | ≥ 100 | 真实买盘，非刷量 |
| 募资额 | ≥ 8 BNB | 资金量充足 |
| 币龄 | ≤ 30 分钟 | 刚起飞，不是老币 |
| 当前价 | ≤ 5e-05 | 保留利润空间 |

历史回测（5天数据）：17 命中，3 突破 0.0001，0 暴亏。

### 🎓 毕业通道（刚毕业强势币）

针对快速毕业后继续涨的代币，跳过进度/价格上限/动量等慢条件，全部满足即通过：

| 条件 | 阈值 | 说明 |
|------|------|------|
| 进度 | = 100% (已毕业) | 已完成 bonding curve |
| 币龄 | ≤ 2 小时 | 刚毕业不久，势头还在 |
| 持币地址数 | ≥ 100 | 真实买盘 |
| 流动性 | ≥ $10,000 | 毕业后有足够交易深度 |
| 当前价 | ≤ 0.0003 | 快速毕业币价格可能超过突破线 |
| 回撤保护 | 当前价 ≥ 峰值 × 0.7 | 毕业后没大跌 |

历史回测（5天数据）：7 命中，1 突破，0 亏损，2 个×1.5+。

### 常规通道（动量筛选）

全部条件满足才通过：

| 条件 | 阈值 | 说明 |
|------|------|------|
| 币龄 | ≥ 1 小时 | 太短的币归零风险大，让它先证明自己不是骗局 |
| 币龄 | ≤ 36 小时 | 太老的币失去市场关注，动量不可信 |
| 当前价 | ≤ 0.00005 | 离突破线太近利润空间不够，不追 |
| 持币地址数 | ≥ 15 | 最低持币门槛 |
| 价格动量 | 当前价 ≥ 入队价×1.5 或 ≥ 历史最低价×2.5 | 二选一，有资金进入信号 |
| 回撤保护 | 当前价 ≥ 峰值×0.5 | 从峰值跌超50%不推 |
| 持币增长 | 当前持币 ≥ 入队持币×1.5 或 近3轮持续递增 | 二选一，持续有人买入 |
| 进度 | ≥ 40% 且 < 97%，从 10%+ 跌破 5% 排除 | 低进度表现差；接近毕业/已毕业买入即亏 |
| 流动性 | ≥ $500（已毕业代币） | 排除流动性枯竭的僵尸币 |
| 买卖比 | ≥ 1.2 (买入笔数/卖出笔数) | 买压 > 卖压，有人在吸筹 |
| 价格加速度 | ≥ 15% (最近2轮价格变化率) | 价格正在加速上涨 |
| 进度加速度 | ≥ 5% (最近2轮进度变化) | 资金持续涌入信号 |
| 持币增速 | ≥ 10% (最近2轮持币变化率) | 持币数正在加速增长 |
| 精筛冷却 | 同一代币通过后6轮内不再推送 | 减少重复信号噪音 |
| 仿盘数 | 仅标记, 不排除 | 仿盘多=热门信号, 交给用户判断 |

## 精筛后防线（深度检查）

仅对精筛通过的少量代币（通常个位数）执行，不影响整体扫描速度：

| 检查项 | 阈值 | 说明 |
|--------|------|------|
| Top10 持仓集中度 | ≤ 85% | BSCScan Top Holders，排除庄家控盘 |
| 开发者清仓 | 卖出 ≥ 90% | BSCScan Transfer 分析，开发者跑路信号 |
| 开发者撤池子 | LP token burn | 撤流动性，准备跑路 |
| 假K线检测 | 无影线实体柱 ≥ 80% 或全阳线 ≥ 90% 或脉冲死线 | GeckoTerminal 15min+1min K线，排除控盘刷量币 |

通过防线的代币才会推送到 Telegram 和触发自动交易。

## 待启用的数据源（已实现未接入）

代码中已实现但 `scan_once` 未调用的函数和数据，后续可接入以提升筛选准确率。

> 注意：币安 Web3 API 禁止美国 IP 访问，仅 `token_trading`（韩国首尔服务器）可用，`token_scanner`（GitHub Actions 美国服务器）无法使用。

### 币安 Web3 Token Dynamic API (`fetch_binance_token_dynamic`)

对精筛后防线价值最大的数据源，单次调用即可获取以下字段：

| 字段 | 含义 | 潜在用途 |
|------|------|----------|
| `top10HoldersPercentage` | Top10 持仓占比 | 替代当前失效的 BSCScan `tokenholderlist`，判断庄家控盘 |
| `devHoldingPercent` | 开发者持仓占比 | 开发者持仓过高=跑路风险，过低=已清仓 |
| `smartMoneyHolders` | 聪明钱持仓人数 | 聪明钱在买=正向信号 |
| `smartMoneyHoldingPercent` | 聪明钱持仓占比 | 聪明钱重仓=高置信度 |
| `kolHolders` | KOL 持仓人数 | KOL 关注=有传播潜力 |
| `kolHoldingPercent` | KOL 持仓占比 | KOL 重仓=强信号 |
| `proHolders` | 专业交易者人数 | 专业玩家在场=非纯散户盘 |
| `proHoldingPercent` | 专业交易者持仓占比 | 专业玩家重仓=值得跟 |
| `percentChange1h` | 1h 涨跌幅 | 短期动量参考 |
| `percentChange24h` | 24h 涨跌幅 | 中期趋势参考 |

当前问题：精筛后防线的 Top10 检查用 BSCScan `tokenholderlist` API，但 four.meme 未毕业代币不产生标准 ERC20 Transfer，BSCScan 索引不到 holder 列表，导致 `top10_concentration` 全部为 null（115 条记录无一有值）。用币安 Token Dynamic API 可直接解决。

### 币安聪明钱信号 (`fetch_binance_smart_signals`)

| 字段 | 含义 | 潜在用途 |
|------|------|----------|
| `direction` | 买入/卖出方向 | 聪明钱正在买入=强正向信号 |
| `smartMoneyCount` | 聪明钱数量 | 多个聪明钱同时买入=高置信度 |
| `exitRate` | 退出率 | 退出率高=风险信号 |
| `maxGain` | 最大收益 | 历史收益参考 |
| `tagEvents` | 敏感事件标签 | 风险预警（如 rug pull 标记） |

### 钱包分析 (`batch_wallet_analysis`)

整合开发者行为 + 币安信号 + 聪明钱匹配的完整分析流程，已实现但未在 `scan_once` 中调用。

### DexScreener 未使用字段

当前 `ds_batch_prices` 已提取价格/流动性/交易量/买卖笔数，但 DexScreener API 还返回了以下字段被丢弃：

| 字段 | 含义 | 潜在用途 |
|------|------|----------|
| `priceChange` (m5/h1/h6/h24) | 各时间段涨跌幅 | 多时间维度动量判断，比自己算更准 |
| `fdv` | 完全稀释估值 | 估值过高的币利润空间小 |
| `marketCap` | 市值 | 市值筛选 |
| `boosts.active` | 是否有付费推广 | 有推广=项目方在花钱运营，正向信号 |
| `info.socials` | 社交媒体列表 | 补充 four.meme 的社交数据 |

### 优先级建议

1. 币安 Token Dynamic → 替换 BSCScan Top Holder（解决 top10 数据缺失问题，同时获得聪明钱/KOL/开发者持仓）
2. DexScreener `priceChange` → 零额外 API 调用，直接从现有响应中提取
3. 币安聪明钱信号 → 作为精筛加分项或防线加固
4. DexScreener `boosts` → 零额外调用，项目方付费推广是正向信号

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

编辑 `config.json`，填入 Telegram Bot Token 和 Chat ID。

### 3. 运行

```bash
python3 scanner.py
```

## 配置参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `telegram_bot_token` | Telegram Bot Token | - |
| `telegram_chat_id` | 推送目标 Chat ID | - |
| `scan_interval_minutes` | 扫描间隔（分钟） | 15 |
| `max_push_count` | 每轮最多推送数量 | 100 |
| `bscscan_api_key` | BSCScan API Key（精筛后防线需要） | - |
| `proxy.enabled` | 是否启用代理 | false |
| `trading.enabled` | 是否启用自动交易 | false |

筛选阈值定义在 `scanner.py` 顶部常量区：

| 常量 | 默认值 | 说明 |
|------|--------|------|
| `MAX_AGE_HOURS` | 48 | 扫描时间窗口（小时） |
| `SCAN_INTERVAL_MIN` | 15 | 扫描间隔（分钟） |
| `TOTAL_SUPPLY` | 1,000,000,000 | 代币总量要求（10亿） |
| `QUALITY_MIN_AGE_MIN` | 15 | 精筛: 币龄下限（分钟） |
| `QUALITY_MIN_HOLDERS` | 15 | 精筛: 持币地址数下限 |
| `QUALITY_PRICE_MOMENTUM_VS_ADDED` | 1.5 | 精筛: 当前价/入队价倍数 |
| `QUALITY_PRICE_MOMENTUM_VS_LOW` | 2.5 | 精筛: 当前价/历史最低价倍数 |
| `QUALITY_HOLDERS_GROWTH_VS_ADDED` | 1.5 | 精筛: 当前持币/入队持币倍数 |
| `QUALITY_HOLDERS_CONSEC_ROUNDS` | 3 | 精筛: 持币数连续递增轮数 |
| `QUALITY_MIN_PROGRESS` | 0.40 | 精筛: 进度下限 (40%) |
| `QUALITY_MIN_BUY_SELL_RATIO` | 1.2 | 精筛: 买卖比下限 |
| `QUALITY_MIN_PRICE_ACCEL` | 0.15 | 精筛: 价格加速度下限 (15%) |
| `QUALITY_MIN_PROGRESS_ACCEL` | 0.05 | 精筛: 进度加速度下限 (5%) |
| `QUALITY_MIN_HOLDERS_GROWTH_RATE` | 0.10 | 精筛: 持币增速下限 (10%) |
| `QUALITY_MAX_DRAWDOWN` | 0.50 | 精筛: 回撤保护 (当前价≥峰值×0.5) |
| `QUALITY_COOLDOWN_ROUNDS` | 6 | 精筛: 同一代币冷却轮数 |
| `ROCKET_MIN_PROGRESS` | 0.70 | 火箭通道: 进度下限 (70%) |
| `ROCKET_MIN_HOLDERS` | 100 | 火箭通道: 持币地址数下限 |
| `ROCKET_MIN_RAISED` | 8.0 | 火箭通道: 募资额下限 (BNB) |
| `ROCKET_MAX_AGE_HOURS` | 0.5 | 火箭通道: 币龄上限 (30分钟) |
| `ROCKET_MAX_PRICE` | 0.00005 | 火箭通道: 价格上限 |
| `GRAD_MIN_HOLDERS` | 100 | 毕业通道: 持币地址数下限 |
| `GRAD_MAX_AGE_HOURS` | 2.0 | 毕业通道: 币龄上限 (2小时) |
| `GRAD_MIN_LIQUIDITY` | 10000 | 毕业通道: 流动性下限 ($10k) |
| `GRAD_MAX_PRICE` | 0.0003 | 毕业通道: 价格上限 |
| `GRAD_MIN_DRAWDOWN_RATIO` | 0.70 | 毕业通道: 回撤保护 (当前价≥峰值×0.7) |
| `QUALITY_MAX_TOP10_CONCENTRATION` | 0.85 | 精筛后防线: Top10持仓占比上限 |
| `QUALITY_FAKE_CANDLE_RATIO` | 0.80 | 精筛后防线: 无影线实体柱占比上限 |
| `QUALITY_FAKE_CANDLE_MIN_COUNT` | 4 | 精筛后防线: 假K线检测最少K线数 |
| `QUALITY_FAKE_CANDLE_BULLISH_RATIO` | 0.90 | 精筛后防线: 全阳线占比上限 |
| `QUALITY_FAKE_CANDLE_DEAD_RATIO` | 0.70 | 精筛后防线: 脉冲后死线占比上限 |
| `QUALITY_FAKE_CANDLE_SPIKE_MULTIPLE` | 10 | 精筛后防线: 头部脉冲振幅倍数阈值 |
| `ELIM_PRICE_DROP_PCT` | 0.90 | 价格跌幅淘汰阈值 |
| `ELIM_HOLDERS_FLOOR` | 10 | 持币数淘汰下限 |
| `ELIM_LIQ_FLOOR` | 100 | 流动性淘汰下限（USD） |

所有配置参数支持热更新（每轮扫描重新读取配置文件）。

## 自动交易模块

### 启用方式

1. 安装 web3 依赖：`pip3 install web3>=6.0.0`
2. 在 `config.json` 中设置 `trading.enabled: true` 和私钥
3. 筛选通过的代币自动买入

### 安全提醒

- 请使用专用钱包，不要使用存有大量资产的主钱包
- 土狗币存在蜜罐、rug pull 等风险，自动交易无法完全规避

## 项目结构

```
├── scanner.py          # 主扫描器（链上发现 + 队列淘汰 + 精筛 + 防线 + 推送）
├── trader.py           # 自动交易模块（买入/卖出/持仓监控）
├── config.json         # 运行时配置（不入库）
├── config.example.json # 配置模板
├── queue.json          # 队列状态（代币列表 + 已淘汰记录 + lastBlock）
├── tokens.db           # SQLite 数据库（交易持仓记录）
├── scanner.log         # 运行日志
└── requirements.txt    # Python 依赖
```

## 注意事项

- 本工具仅用于信息扫描，不构成任何投资建议
- BSC 土狗币风险极高，请自行判断
- 建议配合代理使用，避免 IP 被限流

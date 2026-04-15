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
| ⭐ | 明星保护: 峰值价格 ≥ 0.0001 且当前价 ≥ 峰值×30% | 48h 内免除其他淘汰条件 |

## 精筛规则（动量筛选）

从队列存活币中找"正在起飞"的信号，全部条件满足才通过：

| 条件 | 阈值 | 说明 |
|------|------|------|
| 币龄 | ≥ 15 分钟 | 数据稳定后再判断 |
| 持币地址数 | ≥ 15 | 最低持币门槛 |
| 价格动量 | 当前价 ≥ 入队价×1.5 或 ≥ 历史最低价×2.5 | 二选一，有资金进入信号 |
| 回撤保护 | 当前价 ≥ 峰值×0.5 | 从峰值跌超50%不推 |
| 持币增长 | 当前持币 ≥ 入队持币×1.5 或 近3轮持续递增 | 二选一，持续有人买入 |
| 进度 | ≥ 20% 且 < 97%，从 10%+ 跌破 5% 排除 | 低进度表现差；接近毕业/已毕业买入即亏 |
| 流动性 | ≥ $500（已毕业代币） | 排除流动性枯竭的僵尸币 |
| 买卖比 | ≥ 1.2 (买入笔数/卖出笔数) | 买压 > 卖压，有人在吸筹 |
| 价格加速度 | ≥ 5% (最近2轮价格变化率) | 价格正在加速上涨 |
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
| 假K线检测 | 无影线实体柱 ≥ 80% 或全阳线 ≥ 90% | GeckoTerminal 15min K线，排除控盘刷量币 |

通过防线的代币才会推送到 Telegram 和触发自动交易。

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
| `QUALITY_MIN_PROGRESS` | 0.20 | 精筛: 进度下限 (20%) |
| `QUALITY_MIN_BUY_SELL_RATIO` | 1.2 | 精筛: 买卖比下限 |
| `QUALITY_MIN_PRICE_ACCEL` | 0.05 | 精筛: 价格加速度下限 (5%) |
| `QUALITY_MIN_HOLDERS_GROWTH_RATE` | 0.10 | 精筛: 持币增速下限 (10%) |
| `QUALITY_MAX_DRAWDOWN` | 0.50 | 精筛: 回撤保护 (当前价≥峰值×0.5) |
| `QUALITY_COOLDOWN_ROUNDS` | 6 | 精筛: 同一代币冷却轮数 |
| `QUALITY_MAX_TOP10_CONCENTRATION` | 0.85 | 精筛后防线: Top10持仓占比上限 |
| `QUALITY_FAKE_CANDLE_RATIO` | 0.80 | 精筛后防线: 无影线实体柱占比上限 |
| `QUALITY_FAKE_CANDLE_MIN_COUNT` | 4 | 精筛后防线: 假K线检测最少K线数 |
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

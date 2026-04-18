# BSC Token Scanner v6 — 极速版

扫描 [four.meme](https://four.meme) 和 [flap](https://flap.sh/bnb) 平台上新发行的 BSC 代币，链上发现 + 队列淘汰制 + 增量精筛 + 精筛后防线，推送到 Telegram，可选自动交易。

## v6 架构：极速扫描

v5 每轮扫描需要 ~80s（K线、钱包分析、币安动态等），v6 砍掉淘汰阶段的慢环节，将深度检查移到精筛后防线（仅对个位数代币执行），兼顾速度和准确率。

每轮扫描流程:

1. **链上发现** (~1s): BSC RPC `eth_getLogs` → four.meme + flap 合约 `TokenCreated` 事件 → 新代币地址
2. **入场筛** (~数秒): four.meme Detail API (仅 four.meme 代币) / DexScreener (flap 代币) → 淘汰无社交 / 总量≠10亿 / 币龄>48h
3. **淘汰检查** (~数秒): DexScreener 批量查价(含交易量/买卖笔数) + GeckoTerminal 持币数 + Detail API → 永久淘汰弃盘币
4. **精筛** (瞬时): 增量筛选 — 持币增量 + 动力增量(进度/流动性) + 价格增量
5. **仿盘检测**: 本地统计同名代币数量 (零 API 调用)

## 代币来源

两个平台都使用 bonding curve 机制，买走 80% 供应量后迁移到 PancakeSwap。

| 平台 | 合约 | 代币后缀 | Detail API |
|------|------|----------|------------|
| four.meme | `0x5c952063...` (TokenManagerOriginal) | `4444` / `ffff` | ✅ four.meme Detail API |
| flap | `0xe2cE6ab0...` (Portal) | `8888` / `7777` | ❌ 无 Detail API (进度通过链上 `getToken()` 读取) |

## 数据源

| 数据源 | 用途 | 限流 |
|--------|------|------|
| BSC RPC (publicnode) | 链上 TokenCreated 事件发现 + flap getToken() 进度查询 | 无硬限制 |
| four.meme Detail API | 社交链接/持币数(bonding curve阶段)/进度/募资额 | ~5 req/s |
| DexScreener API | 批量价格+流动性+交易量+买卖笔数 | ~300 req/min |
| GeckoTerminal Token Info | 持币地址数 (已毕业代币, 链上索引) | ~30 req/min |

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
| 4 | 无社交媒体 | 无运营意愿 (仅 four.meme, flap 无社交 API) |
| 5 | 流动性从 >$1k 跌破 $100 (仅已毕业) | 流动性枯竭 |
| 6 | 进度 < 1% 且币龄 > 2h | bonding curve 上的死币 |
| 6b | 进度 < 5% 且币龄 > 4h | 进度停滞 |
| 7 | 进度从峰值跌 50%+ 且币龄 > 6h | 热度消退 |
| 8 | 币龄 > 15min 且最高持币数 < 3 | 无人问津 |
| 9 | 币龄 > 1h 且最高持币数 < 5 | 热度不足 |
| 10 | 币龄 > 48h | 超出关注窗口 |
| 🚀 | 价格突破: 峰值价格 ≥ 0.0001 | 标记为已突破, 保留队列继续跟踪, 仅受币龄淘汰 |

## 精筛规则（增量筛选）

从队列存活币中找"正在起飞"的信号，核心思路：近期有真实增长才推。

三条增量条件全部满足即通过（判断窗口：近 1~3 轮，先查 3 轮差 → 2 轮差 → 1 轮差，任一窗口满足即可）：

| 条件 | 阈值 | 说明 |
|------|------|------|
| 持币增量 | 近 1~3 轮增长 ≥ 45 | 有真实买盘涌入 |
| 动力增量（未毕业） | 近 1~3 轮进度增长 ≥ 20% | 资金持续涌入 bonding curve (four.meme 用 Detail API, flap 用链上 getToken) |
| 动力增量（已毕业） | 近 1~3 轮流动性增长 ≥ 5% | 毕业后流动性持续增加 |
| 价格增量 | 近 1~3 轮价格涨幅 ≥ 20% | 价格正在加速上涨 |
| 单调递增约束 | 2~3 轮窗口每轮都递增 | 不允许"先涨后跌但总体涨"的假信号 |
| 仿盘数 | 仅标记, 不排除 | 仿盘多=热门信号, 交给用户判断 |

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
| `QUALITY_BASE_HOLDERS_DELTA` | 10 | 精筛基础: 近 1~3 轮持币数增长下限 |
| `QUALITY_BASE_PROGRESS_DELTA` | 0.08 | 精筛基础: 近 1~3 轮进度增长下限 (8%, 未毕业币) |
| `QUALITY_BASE_LIQUIDITY_GROWTH` | 0.01 | 精筛基础: 近 1~3 轮流动性增长下限 (1%, 已毕业币) |
| `QUALITY_BASE_PRICE_GROWTH` | 0.08 | 精筛基础: 近 1~3 轮价格涨幅下限 (8%) |
| `QUALITY_EXCEL_HOLDERS_DELTA` | 45 | 精筛优秀: 持币数增长优秀线 |
| `QUALITY_EXCEL_PROGRESS_DELTA` | 0.20 | 精筛优秀: 进度增长优秀线 (20%, 未毕业币) |
| `QUALITY_EXCEL_LIQUIDITY_GROWTH` | 0.05 | 精筛优秀: 流动性增长优秀线 (5%, 已毕业币) |
| `QUALITY_EXCEL_PRICE_GROWTH` | 0.20 | 精筛优秀: 价格涨幅优秀线 (20%) |
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

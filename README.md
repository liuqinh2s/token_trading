# BSC Token Scanner v6 — 极速版

扫描 [four.meme](https://four.meme) 平台上新发行的 BSC 代币，链上发现 + 队列淘汰制 + 极简精筛，1 分钟一轮，以快致胜。推送到 Telegram，可选自动交易。

## v6 架构：极速扫描

v5 每轮扫描需要 ~80s（K线、钱包分析、币安动态等），v6 砍掉所有慢环节，1 分钟一轮抢先发现新币。

每 1 分钟执行一次:

1. **链上发现** (~1s): BSC RPC `eth_getLogs` → four.meme `TokenCreated` 事件 → 新代币地址
2. **入场筛** (~数秒): four.meme Detail API → 淘汰无社交 / 总量≠10亿 / 币龄>48h
3. **淘汰检查** (~数秒): DexScreener 批量查价 + Detail API 查持币数 → 永久淘汰弃盘币
4. **精筛** (瞬时): 极简三条件直接过滤
5. **仿盘检测**: 本地统计同名代币数量 (零 API 调用)

### v5 → v6 砍掉的慢环节

- GeckoTerminal K线 (每个代币 2s+)
- BSCScan 钱包行为分析 (开发者/聪明钱)
- 币安 Web3 聪明钱信号 + Token Dynamic
- GMGN 聪明钱地址
- RPC 持币数统计 (改用 four.meme detail 的 holderCount)

## 数据源

| 数据源 | 用途 | 限流 |
|--------|------|------|
| BSC RPC (publicnode) | 链上 TokenCreated 事件发现 | 无硬限制 |
| four.meme Detail API | 社交链接/持币数/进度 | ~5 req/s |
| DexScreener API | 批量价格+流动性查询 | ~300 req/min |

## 淘汰规则（永久剔除）

满足任一条件即从队列中永久移除：

| # | 条件 | 说明 |
|---|------|------|
| 1 | 价格从峰值跌 90%+ | 暴跌弃盘 |
| 2 | 持币地址从 ≥30 跌破 10 | 大量抛售 |
| 3 | 无社交媒体 | 无运营意愿 |
| 4 | 流动性从 >$1k 跌破 $100 | 流动性枯竭 |
| 5 | 进度 < 1% 且币龄 > 2h | bonding curve 上的死币 |
| 5b | 进度 < 5% 且币龄 > 4h | 进度停滞 |
| 6 | 币龄 > 15min 且最高持币数 < 3 | 无人问津 |
| 7 | 币龄 > 1h 且最高持币数 < 5 | 热度不足 |
| 8 | 币龄 > 48h | 超出关注窗口 |

## 精筛规则

| 条件 | 阈值 | 说明 |
|------|------|------|
| 持币地址数 | ≥ 10 | 最低持币门槛 |
| 币龄 | ≤ 5 分钟 | 只抓最新代币 |
| 当前价 | < $0.000006 | 价格上限 |
| 持币地址数趋势 | 近 2 轮递增 | 首轮入队豁免 |
| 价格趋势 | 近 2 轮递增 | 首轮入队豁免 |

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
| `scan_interval_minutes` | 扫描间隔（分钟） | 1 |
| `max_push_count` | 每轮最多推送数量 | 100 |
| `max_age_hours` | 代币最大年龄（小时） | 48 |
| `bscscan_api_key` | BSCScan API Key（可选） | - |
| `proxy.enabled` | 是否启用代理 | false |
| `trading.enabled` | 是否启用自动交易 | false |

筛选阈值定义在 `scanner.py` 顶部常量中：

| 常量 | 默认值 | 说明 |
|------|--------|------|
| `MAX_AGE_HOURS` | 48 | 扫描时间窗口（小时） |
| `SCAN_INTERVAL_MIN` | 1 | 扫描间隔（分钟） |
| `TOTAL_SUPPLY` | 1,000,000,000 | 代币总量要求（10亿） |
| `QUALITY_MAX_AGE_MIN` | 5 | 精筛: 币龄上限（分钟） |
| `QUALITY_MIN_HOLDERS` | 10 | 精筛: 持币地址数下限 |
| `QUALITY_MAX_PRICE` | 0.000006 | 精筛: 当前价上限 (USD) |
| `COPYCAT_MARK_MIN` | 3 | 仿盘数≥3标记 |
| `MIN_SOCIAL_COUNT` | 1 | 最少社交媒体关联数 |
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

## 注意事项

- 本工具仅用于信息扫描，不构成任何投资建议
- BSC 土狗币风险极高，请自行判断
- 建议配合代理使用，避免 IP 被限流

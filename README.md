# BSC 土狗扫描器 v2

扫描 [four.meme](https://four.meme) 平台上新发行的 BSC 代币，自动筛选后推送到 Telegram。

## 数据源

| API | 用途 | 限流 |
|-----|------|------|
| **four.meme** | 代币发现、详情（总量/社交链接/描述）、行情价格 | ~2 req/s |
| **GeckoTerminal** | K线 OHLCV 数据（含 Bonding Curve 阶段） | ~30 req/min，自动退避重试 |

## 三级筛选管线

| 阶段 | 条件 | 数据源 | 请求开销 |
|------|------|--------|----------|
| **初筛** | 发行 2h~3d、当前价 ≤ $0.00003、持币 ≥ 150、未推送 | 列表 API（批量） | 0 额外请求 |
| **详情筛** | 总量 = 10亿、社交媒体 ≥ 1 | four.meme 详情 | 每候选 1 请求 |
| **K线筛** | 前 2h 最高价 ≤ $0.00002 | GeckoTerminal OHLCV | 每候选 2 请求 |

逐级收窄，避免不必要的 API 调用。

## 全量覆盖机制

four.meme API 分页上限为 10 页 × 100 = 1000 条，无法一次覆盖 72h 时间窗口内的全部代币（BNB 类代币约 27000+）。

解决方案：**SQLite 跨轮次累积**

- 每轮扫描将获取到的代币存入本地 `tokens.db` 数据库
- 初筛从数据库查询，而非仅从当前 API 结果筛选
- 每 15 分钟扫一次，每次覆盖最新 ~5h，运行数小时后即实现 72h 全量覆盖
- 自动清理超过 7 天的过期记录

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
| `min_age_hours` | 代币最小年龄（小时） | 4 |
| `max_age_hours` | 代币最大年龄（小时） | 72 |
| `max_price_current` | 当前价上限 (USD) | 0.00003 |
| `max_price_ath` | 历史最高价上限 (USD) | 0.00003 |
| `required_total_supply` | 要求的代币总量 | 1000000000 |
| `min_holders` | 最小持币地址数 | 150 |
| `min_social_links` | 最小社交媒体数 | 1 |
| `proxy.enabled` | 是否启用代理 | false |
| `hotspot.enabled` | 是否启用热点匹配 | true |
| `hotspot.weibo` | 启用微博热搜 | true |
| `hotspot.google` | 启用 Google Trends | true |
| `hotspot.google_geos` | Google Trends 地区 | ["US","CN"] |
| `hotspot.twitter` | 启用 Twitter/X Trending | true |

所有参数支持热更新（每轮扫描重新读取配置文件）。

## 推送示例

```
🔍 BSC 土狗扫描报告
⏰ 2025-01-01 12:00 UTC

#1 SomeToken (STK)
📄 合约: 0x1234...abcd
💰 当前价: $0.0000012345
📈 历史最高: $0.0000008000
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

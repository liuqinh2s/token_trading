# BSC 土狗扫描器

扫描 [four.meme](https://four.meme) 平台上新发行的 BSC 代币，自动筛选后推送到 Telegram。

## 功能

- 定时拉取 four.meme 最新代币（HOT + NEW 双排序合并去重）
- 多维度自动筛选：发币时间、价格、持币人数、媒体链接等
- 筛选结果推送到 Telegram（含合约地址、价格、Bonding Curve 进度等）
- 支持 HTTP/SOCKS5 代理
- 配置文件热更新，无需重启

## 筛选逻辑

初筛（列表数据）：
- 发币时间 < 配置的最大小时数
- 未迁移（仍在 Bonding Curve 阶段）
- 价格 ≤ 配置的最大价格（USD）
- 持币人数 ≥ 配置的最小值

二次筛选（详情数据）：
- 至少有一个关联媒体链接（Twitter / Telegram / 官网）
- 持币人数精确校验

## 快速开始

### 1. 安装依赖

```bash
pip3 install -r requirements.txt
```

### 2. 配置

复制示例配置并填入你的信息：

```bash
cp config.example.json config.json
```

编辑 `config.json`：

```json
{
  "telegram_bot_token": "你的 Bot Token",
  "telegram_chat_id": "你的 Chat ID",
  "scan_interval_minutes": 15,
  "max_push_count": 3,
  "max_age_hours": 48,
  "max_price": 0.00003,
  "min_holders": 150,
  "proxy": {
    "enabled": true,
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
  }
}
```

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `telegram_bot_token` | Telegram Bot Token | - |
| `telegram_chat_id` | 推送目标 Chat ID | - |
| `scan_interval_minutes` | 扫描间隔（分钟） | 15 |
| `max_push_count` | 每轮最多推送数量 | 3 |
| `max_age_hours` | 代币最大年龄（小时） | 48 |
| `max_price` | 最大价格过滤（USD） | 0.00003 |
| `min_holders` | 最小持币人数 | 150 |
| `proxy.enabled` | 是否启用代理 | false |

### 3. 运行

```bash
python3 scanner.py
```

未配置 Telegram 时，筛选结果会打印到控制台。

## 推送示例

```
🔍 BSC 土狗扫描报告
⏰ 2025-01-01 12:00 UTC

#1 SomeToken (STK)
📄 合约: 0x1234...abcd
💰 价格: $0.0000012345
📊 Bonding Curve: 45.2%
👥 持币人数: 320
🕐 创建: 2025-01-01 10:00 UTC
📝 这是一个示例代币描述...
🔗 four.meme | BscScan
```

## 注意事项

- 本工具仅用于信息扫描，不构成任何投资建议
- BSC 土狗币风险极高，请自行判断
- 建议配合代理使用，避免 IP 被限流

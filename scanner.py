"""
BSC Token Scanner v7 — 极速扫描, 以快致胜
数据源: BSC RPC (链上事件) + four.meme API (详情) + DexScreener (价格/涨跌幅/Boost) + GeckoTerminal (持币数)
代币来源: four.meme + flap (BSC 链上两大代币发射平台, 均使用 bonding curve 机制)

v16 架构: 标签制精筛 (基础标签 AND + 加分标签排优先级)
  1. 链上发现 (~1s): BSC RPC eth_getLogs → four.meme + flap 合约 TokenCreated 事件 → 新代币地址
  2. 入场筛 (~数秒): four.meme Detail API + flap.sh 页面 SSR 社交数据 + 链上 totalSupply → 淘汰无社交 / 总量≠10亿 / 币龄>5min
  3. 淘汰检查 (~数秒): DexScreener 批量查价(含涨跌幅/Boost) + GeckoTerminal 持币数 + Detail API → 永久淘汰弃盘币
  3b. K线修正: 对持币≥50 的存活代币拉 GT 15min K线 → 修正 peakPrice + 记录 klineHigh/klineLow (过山车检测)
  4. 精筛 (瞬时): 标签制精筛, 基础标签全部满足(AND) + 加分标签排优先级
  5. 仿盘检测: 本地统计同名代币数量 (零 API 调用)

v16 精筛策略 (标签制: 基础全过 + 加分标签):
  基础标签已注释, 所有代币基础全过
  加分标签: 小涨跌不动/成交额异动/流动性异动 — 三项硬指标, 至少一项才开仓

砍掉的慢环节 (v5 → v6):
  - GeckoTerminal K线 (每个代币 2s+)
  - BSCScan 钱包行为分析 (开发者/聪明钱)
  - 币安 Web3 聪明钱信号 + Token Dynamic
  - GMGN 聪明钱地址
  - BSCScan 持币数查询 (免费 key 不支持 BSC 链)
  - RPC Transfer 日志持币数 (bonding curve 阶段不产生标准 Transfer, 不准确)

淘汰条件 (永久剔除):
  - 诈骗代币黑名单 (已确认的诈骗代币合约地址, 入场即拒)
  - 诈骗开发者黑名单 (已确认的连续发诈骗币的开发者钱包地址, 其所有代币入场即拒)
  - 蹭名币 (symbol/name 命中主流币种黑名单, 如 USDT/BTC/ETH 等)
  - 价格从峰值跌 90%+ (保护: 当前价格<1e-7 视为 API 异常, 跳过)
  - 持币地址从 ≥30 跌破 10
  - 持币数从峰值跌 70%+ (峰值≥50, 清理僵尸币)
  - 无社交媒体 (four.meme 通过 detail API, flap 通过 flap.sh 页面 SSR 提取, 统一淘汰)
  - flap 社交补查超时: 入场时社交抓取失败给 2 轮缓冲, 超时仍无社交则淘汰 (避免低质量 flap 币长期占位)
  - 流动性跌破 $1k (仅已毕业代币)
  - 进度阶梯淘汰: 币龄>1h进度<3%, >2h<7%, >4h<10%, >8h<15%
  - 进度从峰值跌 40 个百分点 (加减法, 无币龄要求)
  - 币龄 > 15min 且最高持币数 < 3
  - 币龄 > 1h 且最高持币数 < 5
  - 币龄 > 2h 且最高持币数 < 8 (清理僵尸币)
  - 币龄 > 48h

精筛条件 (v15: 标签制 — 基础标签 AND + 加分标签排优先级):

  基础标签 (已注释, 全部放行):
  - 持币合格 / 进度合格 / 流动性合格 / K线合格 / 社交合格 / 创建者合格 / 币龄合格

  加分标签 (只三项硬指标):
  - 小涨跌不动: 最低→最高涨幅≤3.5倍 且 最高→最低跌幅≤55%, 横盘≥3h且≤8h(+1)
  - 成交额异动: 阳线 + 增量≥前根×7 + K线涨幅≤100% + 增量≥500u(+1)
  - 流动性异动: 仅已毕业, 阳线 + 增量≥前根×1.2 + 价格涨幅<100% + 增量≥$15k(+1)
"""

from __future__ import annotations

import json
import time
import logging
import re
import sys
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone
from pathlib import Path

try:
    from trader import (init_trader, execute_buys, start_monitor, stop_monitor,
                        _sync_positions_from_wallet)
    _HAS_TRADER = True
except Exception as _trader_err:
    _HAS_TRADER = False
    # 启动时打印具体原因, 方便排查
    import traceback as _tb
    print(f"[WARN] trader 模块导入失败: {_trader_err}")
    _tb.print_exc()

# ===================================================================
#  日志
# ===================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scanner.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

if not _HAS_TRADER:
    log.warning("trader 模块导入失败: %s (自动交易不可用)", _trader_err)

# ===================================================================
#  配置
# ===================================================================
CONFIG_PATH = Path(__file__).parent / "config.json"


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ===================================================================
#  常量 & API
# ===================================================================
FM_SEARCH = "https://four.meme/meme-api/v1/public/token/search"
FM_DETAIL = "https://four.meme/meme-api/v1/private/token/get/v2"
FM_TICKER = "https://four.meme/meme-api/v1/public/ticker"
GT_BASE = "https://api.geckoterminal.com/api/v2"
DS_BASE = "https://api.dexscreener.com"
BSCSCAN_API = "https://api.etherscan.io/v2/api"  # Etherscan V2 API (支持 chainid 参数)
BSC_RPC = "https://bsc-rpc.publicnode.com/"
BINANCE_SMART_SIGNAL = "https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money/ai"
BINANCE_TOKEN_DYNAMIC = "https://web3.binance.com/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai"
BINANCE_TOKEN_META = "https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/dex/market/token/meta/info/ai"

# four.meme 合约地址 (来源: four-flap-meme-sdk v1.9.48)
# TokenManagerOriginal: 0x5c952063c7fc8610FFDB798152D69F0B9550762b (当前唯一活跃合约)
# TokenManagerV1:       0xf7F823d0E790219dBf727bDb971837574655fCB0 (已废弃, 无事件)
# TokenManagerV2:       0x342399a59943B5815849657Aa0e06D7058D9d5C6 (已废弃, 无事件)
# TokenManagerHelper3:  0xF251F83e40a78868FcfA3FA4599Dad6494E46034 (查询辅助合约)
FOUR_MEME_CONTRACT = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"       # TokenManagerOriginal (所有 TokenCreate 事件都在此合约上)
TOKEN_CREATE_TOPIC = "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"      # 旧版 TokenCreate (12 words, 含名称/符号/时间戳, 少量使用)
TOKEN_CREATE_TOPIC_V3 = "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19"  # 新版 TokenCreate (8 words, 无名称/时间戳, 当前主力)
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# flap 合约地址 (BSC 链上代币发射平台, 来源: docs.flap.sh)
# Flap 也使用 bonding curve 机制 (constant product equation), 买走 80% 供应量后迁移到 PancakeSwap
# Portal:    0xe2cE6ab80874Fa9Fa2aAE65D277Dd6B8e65C9De0 (事件发射合约, TokenCreated 事件在此)
# Launchpad: 0x1de460f363AF910f51726DEf188F9004276Bf4bc (主 launchpad 合约)
# 代币后缀: 标准代币 8888, 税币 7777
# 进度获取: 通过 RPC eth_call 调用 Portal 合约 getTokenV5(token) 读取 reserve/supply/price/status + bonding curve 参数
#   进度 = reserve / target_reserve (target_reserve 由 bonding curve 公式 K/(h+1e9-targetSupply)-r 算出)
#   毕业条件: status=4 (DEX)
FLAP_PORTAL_CONTRACT = "0xe2ce6ab80874fa9fa2aae65d277dd6b8e65c9de0"
# TokenCreated(uint256 ts, address creator, uint256 nonce, address token, string name, string symbol, string meta)
FLAP_TOKEN_CREATE_TOPIC = "0x504e7f360b2e5fe33cbaaae4c593bc55305328341bf79009e43e0e3b7f699603"
# flap.sh 代币详情页 (Next.js SSR, metadata 含社交链接)
FLAP_PAGE_URL = "https://flap.sh/bnb/"
# 毕业目标 reserve: 由 Portal getTokenV5() 返回的 bonding curve 参数动态计算 (不再硬编码)

# 所有 TokenCreate 事件监听配置 (新增事件只需在此追加)
# 每项: (合约地址, 事件 topic, 来源标识)
# 注意: 0x7db52723... 是买卖事件 (同一代币多次出现), 不要监听
TOKEN_FACTORIES = [
    (FOUR_MEME_CONTRACT, TOKEN_CREATE_TOPIC, "four.meme"),       # four.meme 旧版 TokenCreate
    (FOUR_MEME_CONTRACT, TOKEN_CREATE_TOPIC_V3, "four.meme"),    # four.meme 新版 TokenCreate (同一合约, 不同 topic)
    (FLAP_PORTAL_CONTRACT, FLAP_TOKEN_CREATE_TOPIC, "flap"),     # flap TokenCreated
]

FM_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Origin": "https://four.meme",
    "Referer": "https://four.meme/",
}
GT_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
DS_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
BINANCE_HEADERS = {
    "Content-Type": "application/json",
    "Accept-Encoding": "identity",
    "User-Agent": "binance-web3/1.1 (Skill)",
}

# ================================================================
#  精筛阈值 — 标签制 (基础标签 AND + 加分标签排优先级)
#
#  v16 架构: 每个数据维度按币龄精细化分层, 币龄越长要求越高
#  基础标签: 全部满足才通过精筛, 目标每天~30个 (AND 逻辑)
#  加分标签: 给通过基础标签的代币排优先级, 高优先级优先买入, 至少一项才开仓
#  持仓队列: 最多 10 个, 每仓 1%
#
#  v14 核心发现 (129 个 ≥3x 代币暴涨前信号剖解):
#    "双动能"(holders↑+price↑同一轮)是唯一强区分信号
#    89/129 (69%) winner 暴涨前出现双动能
#    价格信号比持币信号更领先 — winner 往往先拉盘、后吸引持币
#
#  阈值设定方法论:
#    每个维度都与币龄正相关 — 币龄越长, 要求越高
#    同一 tier 内的条件取最严格的匹配 (max age where condition applies)
#    币龄<15m 的代币不做任何基础标签检查 (给刚出生的代币缓冲期)
# ================================================================
MAX_AGE_HOURS = 48
SCAN_INTERVAL_MIN = 15
TOTAL_SUPPLY = 1_000_000_000

# --- 基础标签: 持币数 (age → min_holders) ---
# 币龄越长持币数要求越高, 反映热度 & 共识程度
# 注: 持币数可造假, 不是决定性因素, 过关即可
TAG_HOLDERS_TIERS = [
    (0,    8),    # <15min:   ≥8
    (0.25, 12),   # ≥15min:  ≥12
    (0.5,  25),   # ≥30min:  ≥25
    (1.0,  60),   # ≥1h:     ≥60
    (2.0,  90),   # ≥2h:     ≥90
    (4.0,  130),  # ≥4h:     ≥130
    (8.0,  180),  # ≥8h:     ≥180
    (16.0, 220),  # ≥16h:    ≥220
    (24.0, 350),  # ≥24h:    ≥350
    (44.0, 500),  # ≥44h:    ≥500
]

# --- 基础标签: 进度 (未毕业, age → min_progress) ---
# 进度反映项目方实力, 迟迟不涨说明项目方实力不够
TAG_PROGRESS_TIERS = [
    (0,    0.01),  # <15min:   ≥1%
    (0.25, 0.04),  # ≥15min:  ≥4%
    (0.5,  0.10),  # ≥30min:  ≥10%
    (1.0,  0.15),  # ≥1h:     ≥15%
    (2.0,  0.25),  # ≥2h:     ≥25%
    (4.0,  0.40),  # ≥4h:     ≥40%
    (8.0,  0.60),  # ≥8h:     ≥60%
    (16.0, 0.80),  # ≥16h:    ≥80%
    (24.0, 0.90),  # ≥24h:    ≥90%
    (44.0, 1.00),  # ≥44h:    ≥100% (必须毕业)
]

# --- 基础标签: 流动性 (已毕业, age → min_liquidity) ---
# 币龄越长流动性要求越高, 反映项目方实力
TAG_LIQUIDITY_TIERS = [
    (0,    500),   # <15min:   ≥$500
    (0.25, 1000),  # ≥15min:  ≥$1k
    (0.5,  2000),  # ≥30min:  ≥$2k
    (1.0,  3000),  # ≥1h:     ≥$3k
    (4.0,  5000),  # ≥4h:     ≥$5k
    (8.0,  8000),  # ≥8h:     ≥$8k
    (16.0, 10000), # ≥16h:    ≥$10k
    (24.0, 15000), # ≥24h:    ≥$15k
    (44.0, 25000), # ≥44h:    ≥$25k
]

# --- 基础标签: K线 (防追高, age → max_price_change vs 首轮价格) ---
TAG_KLINE_TIERS = [
    (0,    1.5),   # <15min:   涨幅 ≤ 150%
    (0.25, 2.0),   # ≥15min:   涨幅 ≤ 200%
    (1.0,  4.0),   # ≥1h:      涨幅 ≤ 400%
    (4.0,  8.0),   # ≥4h:      涨幅 ≤ 800%
    (8.0,  10.0),  # ≥8h:      涨幅 ≤ 1000%
    (16.0, 12.0),  # ≥16h:     涨幅 ≤ 1200%
    (24.0, 15.0),  # ≥24h:     涨幅 ≤ 1500%
    (44.0, 20.0),  # ≥44h:     涨幅 ≤ 2000%
]

# --- 基础标签: 1h买卖数 (age → min_buys, 且 buys > sells) ---
# 交易笔数反映散户圈热度, 有真实换手 vs 刷量
TAG_BUYS_TIERS = [
    (0,    5),     # <15min:   1h买入 ≥ 5
    (0.25, 20),    # ≥15min:  1h买入 ≥ 20
    (0.5,  40),    # ≥30min:  1h买入 ≥ 40
    (1.0,  80),    # ≥1h:     1h买入 ≥ 80
    (2.0,  100),   # ≥2h:     1h买入 ≥ 100
    (4.0,  200),   # ≥4h:     1h买入 ≥ 200
    (8.0,  300),   # ≥8h:     1h买入 ≥ 300
    (16.0, 400),   # ≥16h:    1h买入 ≥ 400
    (24.0, 500),   # ≥24h:    1h买入 ≥ 500
    (44.0, 600),   # ≥44h:    1h买入 ≥ 600
]

# --- 基础标签: 成交额 (age → min_volume_usd, 全部成交额) ---
# 成交额是少数真实指标, 无成交额的币不可信
TAG_VOLUME_TIERS = [
    (0,    200),    # <15min:   ≥$200
    (0.25, 300),    # ≥15min:  ≥$300
    (0.5,  500),    # ≥30min:  ≥$500
    (1.0,  1000),   # ≥1h:     ≥$1000
    (2.0,  1500),   # ≥2h:     ≥$1500
    (4.0,  4000),   # ≥4h:     ≥$4000
    (8.0,  8000),   # ≥8h:     ≥$8000
    (16.0, 15000),  # ≥16h:    ≥$15000
    (24.0, 20000),  # ≥24h:    ≥$20000
    (44.0, 30000),  # ≥44h:    ≥$30000
]

# --- 基础标签: 波动充足 (近3轮振幅有至少一根≥20%) ---
TAG_AMPLITUDE_MIN_RATIO = 0.20    # 振幅 ≥ 20%
TAG_AMPLITUDE_WINDOW = 3          # 检测最近3轮

# --- 基础标签: 趋势向上 (当前价格 ≥ 4轮前价格) ---
TAG_TREND_WINDOW = 4              # 检测最近4轮 (≈1h)

# --- 基础标签: 关联社交媒体 ---
TAG_SOCIAL_MIN_AGE = 0.25        # 币龄≥15min 才检查社交
TAG_SOCIAL_MIN_COUNT = 1         # 至少 1 个关联社交媒体
# flap 社交豁免: flap 代币普遍无社交链接, 当持币和进度足够时豁免社交要求
TAG_FLAP_SOCIAL_EXEMPT_HOLDERS = 30
TAG_FLAP_SOCIAL_EXEMPT_PROGRESS = 0.30
FLAP_SOCIAL_PENDING_MAX_ROUNDS = 2    # flap 社交 pending 最大缓冲轮数

# --- 基础标签: 创建者地址 / 代币地址 ---
# 创建者不在黑名单 (入场筛已过滤)
# 代币不在诈骗黑名单 (入场筛已过滤)

# --- 加分标签: 仿盘数 (age → bonus_score, 不排除只加分) ---
COPYCAT_BONUS_TIERS = [
    (2.0, 3, 1),    # ≥2h: 仿盘≥3 → +1
    (8.0, 5, 2),    # ≥8h: 仿盘≥5 → +2
]
COPYCAT_MARK_MIN = 3    # 仿盘数 ≥3 在前端标记

# --- 加分标签: TOP10 持仓占比 (用于淘汰/后防线, 非加分项) ---
TOP10_CONCENTRATION_MAX = 0.85   # >85% 庄家控盘
TOP10_CONCENTRATION_MIN = 0.20   # <20% 持仓太分散

# --- 加分标签: 持币数 ≥1000 ---
BONUS_HOLDERS_1K = 1000          # 持币数 ≥ 1000 → 加分

# --- 加分标签: 流动性 ≥30k ---
BONUS_LIQUIDITY_30K = 30000     # 流动性 ≥ $30k → 加分

# --- 加分标签: 小涨跌不动 (横盘整理) ---
# 小涨: 最低价到最高价涨幅 ≤ 3.5倍
# 跌不下去: 最高价到最低价跌幅 ≤ 55%
# 横盘时间区间: ≥ 3h 且 ≤ 8h
BONUS_CONSOLIDATION_MAX_GAIN = 3.5       # 横盘区间最大涨幅 (倍)
BONUS_CONSOLIDATION_MAX_DRAWDOWN = 0.55  # 横盘区间最大回撤 (比例)
BONUS_CONSOLIDATION_MIN_HOURS = 3.0      # 横盘最少时间 (小时)
BONUS_CONSOLIDATION_MAX_HOURS = 8.0      # 横盘最多时间 (小时)

# --- 加分标签: 成交额异动 (volume surge) ---
# 要求: 当前15min K线阳线 + 成交额增量≥前1根×7 + K线涨幅≤100% + 增量≥500u
BONUS_VOLUME_SURGE_RATIO = 7            # 成交额增长比例 (7倍)
BONUS_VOLUME_SURGE_MAX_PRICE_GAIN = 1.0 # 价格涨幅上限 (1.0 = 100%)
BONUS_VOLUME_SURGE_MIN_DELTA = 500      # 成交额异动最低金额 ($)

# --- 加分标签: 流动性异动 (liquidity surge, 仅已毕业) ---
# 要求: 当前15min K线阳线 + 流动性增量≥前1根×1.2 + 价格涨幅<100% + 增量≥$15k
BONUS_LIQ_SURGE_RATIO = 1.20            # 流动性增长比例
BONUS_LIQ_SURGE_MAX_PRICE_GAIN = 1.0    # 价格涨幅上限 (1.0 = 100%)
BONUS_LIQ_SURGE_MIN_DELTA = 15000       # 流动性异动最低金额 ($)

# --- 加分标签权重 (只保留三项硬指标加分) ---
BONUS_WEIGHT_CONSOLIDATION = 1   # 小涨跌不动
BONUS_WEIGHT_VOLUME_SURGE = 1    # 成交额异动
BONUS_WEIGHT_LIQ_SURGE = 1       # 流动性异动
# 大盘情绪: 纯 Gas 趋势判定 (当前 Gas 指数 vs 12h前快照)
# Gas 大盘指数: ETH/BSC gasUsedRatio + SOL TPS, 反映多链整体活跃度
# gasUsedRatio: 0~1, 越高=区块越满=链上越活跃; SOL TPS 归一化到 0~1
GAS_INDEX_ETH_WEIGHT = 0.4             # ETH Gas 权重 (主链, 风向标)
GAS_INDEX_BSC_WEIGHT = 0.4             # BSC Gas 权重 (本项目主战场)
GAS_INDEX_SOL_WEIGHT = 0.2             # SOL TPS 权重 (辅助参考)
# Solana TPS 归一化参数: 历史 TPS 范围约 2000~5000, 用 4000 作为满载基准
SOL_TPS_NORMALIZE_BASE = 4000.0
# Solana 公共 RPC (免费, 无需 API key)
SOL_RPC = "https://api.mainnet-beta.solana.com"
# Ethereum 公共 RPC (用于 eth_feeHistory, 免费)
ETH_RPC = "https://ethereum-rpc.publicnode.com"
MIN_SOCIAL_COUNT = 1                   # 入场筛/淘汰: 最少关联社交媒体数 (入场门槛不变)

# 诈骗开发者黑名单: creator 地址命中即拒绝入场 (小写)
# 已确认的连续发诈骗币的开发者钱包地址, 其部署的所有代币一律拦截
DEPLOYER_BLACKLIST: set[str] = set()

# 优质开发者白名单: creator 地址命中则精筛加分 (小写)
# 发过 10x+ 涨幅代币的开发者, 运行时从 queue.json 恢复
DEPLOYER_WHITELIST: set[str] = set()

# 诈骗代币黑名单: 代币合约地址命中即拒绝入场 (小写)
# 已确认的诈骗代币, 直接拦截
TOKEN_BLACKLIST = {
    "0x11ddf7dabff8c29a72be469154bd6494fa184444",  # 诈骗币 (关联 twitter.com/zhaocaidev)
}

# 开发者画像: 自动收集阈值
DEPLOYER_SCAM_DROP_PCT = 0.80          # 一轮内价格暴跌 80% 视为诈骗 (lastPrice→price)
DEPLOYER_SCAM_MIN_TOKENS = 2           # 开发者发过 ≥2 个诈骗币才加入黑名单 (避免误杀)
DEPLOYER_QUALITY_GAIN_X = 10           # 涨幅 ≥ 10x 视为优质 (peakPrice / addedPrice)
DEPLOYER_QUALITY_MIN_PEAK_PRICE = 0.000003  # 起始价格必须 ≥ 0.000003 才算真正大涨 (过滤起始价接近零的假币)

# 蹭名币黑名单: symbol 或 name 命中即拒绝入场 (小写匹配)
# 这些是知名主流币种的名称/符号, 在 meme 平台上发行的同名代币 100% 是蹭热度的假币
FAKE_NAME_BLACKLIST = {
    "usdt", "usdc", "busd", "dai", "tusd", "usdp", "frax", "lusd", "gusd",  # 稳定币
    "btc", "bitcoin", "eth", "ethereum", "bnb", "sol", "solana",             # 主流公链币
    "xrp", "ripple", "ada", "cardano", "doge", "dogecoin", "shib",           # 主流山寨币
    "dot", "polkadot", "avax", "avalanche", "matic", "polygon", "link",      # 主流山寨币
    "uni", "uniswap", "aave", "cake", "pancakeswap",                         # 主流 DeFi
    "wbnb", "weth", "wbtc",                                                   # 包装代币
    "tether", "tether usd", "binance coin", "binance-peg",                   # 全名匹配
}

# 精筛后防线阈值
# 精筛后防线阈值 — 使用 TOP10_CONCENTRATION_MAX (在上方加分标签区定义)

# 淘汰阈值
ELIM_PRICE_DROP_PCT = 0.90             # 价格从峰值跌 90%
ELIM_HOLDERS_FLOOR = 10               # 持币数跌破 10
ELIM_HOLDERS_PEAK_MIN = 30            # 持币数曾达到 30 才触发跌破淘汰
ELIM_HOLDERS_DROP_PCT = 0.70          # 持币数从峰值跌 70% 淘汰 (清理僵尸币, 数据显示 99 个僵尸币占位)
ELIM_HOLDERS_DROP_PEAK_MIN = 50       # 持币数曾达到 50 才触发跌幅淘汰 (避免误杀小币)
ELIM_LIQ_FLOOR = 1000                 # 流动性跌破 $1k (仅已毕业)
ELIM_LIQ_PEAK_MIN = 2000              # 流动性曾达到 $2k 才触发流动性枯竭淘汰 (避免误杀小币)
ELIM_PROGRESS_DROP_ABS = 0.40         # 进度从峰值跌 40 个百分点淘汰 (加减法, 无币龄要求)
# 进度阶梯淘汰 (币龄越长, 容忍度越高)
ELIM_PROGRESS_TIERS = [
    (1.0, 0.03),    # 币龄 > 1h, 进度 < 3%
    (2.0, 0.07),    # 币龄 > 2h, 进度 < 7%
    (4.0, 0.10),    # 币龄 > 4h, 进度 < 10%
    (8.0, 0.15),    # 币龄 > 8h, 进度 < 15%
]
ELIM_EARLY_PEAK_HOLDERS = 3           # 币龄>15min 最高持币数 < 3 淘汰
ELIM_EARLY_AGE_MIN = 0.25             # 15 分钟 = 0.25h
ELIM_MID_PEAK_HOLDERS = 5             # 币龄>1h 最高持币数 < 5 淘汰
ELIM_MID_AGE_HOURS = 1                # 1 小时
ELIM_LATE_PEAK_HOLDERS = 8            # 币龄>2h 最高持币数 < 8 淘汰 (数据验证: 峰值持币8~9的代币偶有后续增长, 留出缓冲)
ELIM_LATE_AGE_HOURS = 2               # 2 小时
ELIM_PRICE_DROP_MIN_PRICE = 1e-7      # 价格暴跌保护: 当前价格低于此值视为 API 异常, 跳过价格跌幅淘汰
ELIM_KLINESINGLE_DROP_PCT = 0.40     # 单根15min K线跌幅 >40% → 过山车币淘汰 (防暴涨后暴跌)

# flap quote token 分类 (用于价格换算)
# 零地址 = 原生 BNB, 需乘 BNB/USD; 稳定币 = 已是 USD 计价, 无需换算; 其他 = 非标代币计价
FLAP_STABLE_QUOTES = {
    "0x55d398326f99059ff775485246999027b3197955",  # USDT
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
    "0x61a10e8556bed032ea176330e7f17d6a12a10000",  # USD1/lisUSD
}

# 队列状态文件
QUEUE_FILE = Path(__file__).parent / "queue.json"

# 已知的 DEX Router 地址 (用于识别买入/卖出行为)
KNOWN_DEX_ROUTERS = {
    "0x10ed43c718714eb63d5aa57b78b54704e256024e",  # PancakeSwap V2 Router
    "0x13f4ea83d0bd40e75c8222255bc855a974568dd4",  # PancakeSwap V3 Router
    "0x1b81d678ffb9c0263b24a97847620c99d213eb14",  # PancakeSwap Universal Router
}

# 零地址/死地址 (转到这些地址不算卖出, 视为销毁)
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
DEAD_ADDRESS = "0x000000000000000000000000000000000000dead"
BURN_ADDRESSES = {ZERO_ADDRESS, DEAD_ADDRESS}

# 排除的已知非聪明钱地址 (交易所/合约/稳定币等)
KNOWN_EXCLUDE_ADDRESSES = {
    ZERO_ADDRESS, DEAD_ADDRESS,
    "0x10ed43c718714eb63d5aa57b78b54704e256024e",  # PancakeSwap V2 Router
    "0x13f4ea83d0bd40e75c8222255bc855a974568dd4",  # PancakeSwap V3 Router
    "0x1b81d678ffb9c0263b24a97847620c99d213eb14",  # PancakeSwap Universal Router
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
    "0x55d398326f99059ff775485246999027b3197955",  # USDT
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
    FOUR_MEME_CONTRACT.lower(),
    "0xd36b6d646ac6e23672e9eedec558164c7f2d6deb",  # four.meme 交易代理合约 (非工厂)
    FLAP_PORTAL_CONTRACT.lower(),                    # flap Portal 合约
}


# ===================================================================
#  HTTP Session
# ===================================================================
def _build_session(proxy_cfg: dict | None = None,
                   extra_headers: dict | None = None) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3, backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=5, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if extra_headers:
        session.headers.update(extra_headers)
    if proxy_cfg and proxy_cfg.get("enabled"):
        session.proxies = {
            "http": proxy_cfg.get("http", ""),
            "https": proxy_cfg.get("https", ""),
        }
        log.info("代理已启用: %s", proxy_cfg.get("https", ""))
    return session


_fm_session: requests.Session = None  # type: ignore
_bnb_usd_price: float = 600.0  # BNB/USD 实时价格, scan_once 中更新
_gt_session: requests.Session = None  # type: ignore
_bsc_session: requests.Session = None  # type: ignore
_bn_session: requests.Session = None  # type: ignore
_ipfs_session: requests.Session = None  # type: ignore  # IPFS 专用 (无重试, 短超时)


def _build_ipfs_session(proxy_cfg: dict | None = None) -> requests.Session:
    """IPFS 专用 session: 不挂 urllib3 Retry, 函数内部已有多网关容错"""
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=3, pool_maxsize=6)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if proxy_cfg and proxy_cfg.get("enabled"):
        session.proxies = {
            "http": proxy_cfg.get("http", ""),
            "https": proxy_cfg.get("https", ""),
        }
    return session


def _ensure_sessions():
    global _fm_session, _gt_session, _bsc_session, _bn_session, _ipfs_session
    if _fm_session is None:
        try:
            cfg = load_config()
            proxy = cfg.get("proxy")
        except Exception:
            proxy = None
        _fm_session = _build_session(proxy, FM_HEADERS)
        _gt_session = _build_session(proxy, GT_HEADERS)
        _bsc_session = _build_session(proxy, DS_HEADERS)
        _bn_session = _build_session(proxy, BINANCE_HEADERS)
        _ipfs_session = _build_ipfs_session(proxy)


# ===================================================================
#  队列状态管理
# ===================================================================
def load_queue() -> dict:
    """加载队列状态 (从 queue.json)"""
    try:
        if QUEUE_FILE.exists():
            with open(QUEUE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            log.info("加载队列: %d 个代币, lastBlock: %s",
                     len(data.get("tokens", [])), data.get("lastBlock", 0))
            return data
    except Exception as e:
        log.warning("队列加载失败: %s", e)
    return {"tokens": [], "eliminated": [], "lastBlock": 0, "lastScanTime": 0,
            "nameIndex": {}}


def save_queue(queue: dict):
    """保存队列状态"""
    if len(queue.get("eliminated", [])) > 1000:
        queue["eliminated"] = queue["eliminated"][-1000:]
    name_idx = queue.get("nameIndex", {})
    now_ms = int(time.time() * 1000)
    max_age_ms = MAX_AGE_HOURS * 3600 * 1000
    for key in list(name_idx.keys()):
        addrs = name_idx[key].get("addrs", [])
        if len(addrs) > 200:
            name_idx[key]["addrs"] = addrs[-200:]
        elif len(addrs) <= 1:
            del name_idx[key]
            continue
        last_seen = name_idx[key].get("lastSeen", 0)
        if last_seen > 0 and now_ms - last_seen > max_age_ms:
            del name_idx[key]
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(queue, f, ensure_ascii=False, indent=2)


def update_deployer_reputation(queue_state: dict,
                                survivors: list[dict],
                                eliminated: list[dict]) -> None:
    """
    更新开发者画像: 从本轮数据中自动收集诈骗/优质开发者地址
    诈骗判定: 一轮内价格暴跌 80%+ (lastPrice→当前price, 说明开发者砸盘/撤池子)
    优质判定: 峰值价格 / 入队价格 ≥ 10x (说明代币有真实增长)
    """
    now_ms = int(time.time() * 1000)
    blacklist = queue_state.setdefault("deployerBlacklist", {})
    whitelist = queue_state.setdefault("deployerWhitelist", {})

    # --- 诈骗开发者收集: 从被淘汰的代币中找一轮内暴跌 80%+ 的 ---
    for t in eliminated:
        creator = (t.get("creator") or "").lower()
        if not creator or creator == ZERO_ADDRESS:
            continue
        last_price = t.get("prevPrice", 0) or t.get("lastPrice", 0)
        current_price = t.get("price", 0)
        # prevPrice 是上一轮的价格, price 是本轮更新后的价格
        # 一轮内暴跌 80%+ = 开发者砸盘/撤池子
        if (last_price > 0 and current_price > 0
                and current_price < last_price * (1 - DEPLOYER_SCAM_DROP_PCT)):
            addr = t.get("address", "")
            if creator not in blacklist:
                blacklist[creator] = {"count": 0, "tokens": [], "firstSeen": now_ms, "lastSeen": now_ms}
            entry = blacklist[creator]
            if addr and addr not in entry["tokens"]:
                entry["tokens"].append(addr)
                entry["count"] = len(entry["tokens"])
                entry["lastSeen"] = now_ms
                drop_pct = (1 - current_price / last_price) * 100
                log.info("🚨 诈骗开发者记录: %s — %s 一轮暴跌%.0f%% (累计%d个诈骗币)",
                         creator[:16], t.get("name") or addr[:16], drop_pct, entry["count"])

    # --- 诈骗开发者收集: 从存活代币中找一轮内暴跌 80%+ 的 (还没被淘汰但已经暴跌) ---
    for t in survivors:
        creator = (t.get("creator") or "").lower()
        if not creator or creator == ZERO_ADDRESS:
            continue
        last_price = t.get("prevPrice", 0) or t.get("lastPrice", 0)
        current_price = t.get("price", 0)
        if (last_price > 0 and current_price > 0
                and current_price < last_price * (1 - DEPLOYER_SCAM_DROP_PCT)):
            addr = t.get("address", "")
            if creator not in blacklist:
                blacklist[creator] = {"count": 0, "tokens": [], "firstSeen": now_ms, "lastSeen": now_ms}
            entry = blacklist[creator]
            if addr and addr not in entry["tokens"]:
                entry["tokens"].append(addr)
                entry["count"] = len(entry["tokens"])
                entry["lastSeen"] = now_ms
                drop_pct = (1 - current_price / last_price) * 100
                log.info("🚨 诈骗开发者记录: %s — %s 一轮暴跌%.0f%% (累计%d个诈骗币)",
                         creator[:16], t.get("name") or addr[:16], drop_pct, entry["count"])

    # --- 优质开发者收集: 从存活+突破代币中找涨幅 ≥ 10x 的 ---
    for t in survivors:
        creator = (t.get("creator") or "").lower()
        if not creator or creator == ZERO_ADDRESS:
            continue
        added_price = t.get("addedPrice", 0)
        peak_price = t.get("peakPrice", 0)
        if (added_price > 0 and peak_price > 0
                and added_price >= DEPLOYER_QUALITY_MIN_PEAK_PRICE
                and peak_price >= added_price * DEPLOYER_QUALITY_GAIN_X):
            addr = t.get("address", "")
            if creator not in whitelist:
                whitelist[creator] = {"count": 0, "tokens": [], "firstSeen": now_ms, "lastSeen": now_ms}
            entry = whitelist[creator]
            if addr and addr not in entry["tokens"]:
                entry["tokens"].append(addr)
                entry["count"] = len(entry["tokens"])
                entry["lastSeen"] = now_ms
                gain_x = peak_price / added_price
                log.info("⭐ 优质开发者记录: %s — %s 涨幅%.1fx (累计%d个优质币)",
                         creator[:16], t.get("name") or addr[:16], gain_x, entry["count"])

    # 更新全局黑名单集合 (供 admission_filter 使用)
    # 只有发过 ≥ DEPLOYER_SCAM_MIN_TOKENS 个诈骗币的开发者才加入运行时黑名单
    DEPLOYER_BLACKLIST.clear()
    for creator, info in blacklist.items():
        if info["count"] >= DEPLOYER_SCAM_MIN_TOKENS:
            DEPLOYER_BLACKLIST.add(creator)

    if DEPLOYER_BLACKLIST:
        log.info("开发者黑名单: %d 个地址 (诈骗币≥%d)", len(DEPLOYER_BLACKLIST), DEPLOYER_SCAM_MIN_TOKENS)
    if whitelist:
        log.info("开发者白名单: %d 个地址 (涨幅≥%dx)", len(whitelist), DEPLOYER_QUALITY_GAIN_X)

    # 更新全局白名单集合 (供 tag_filter 使用)
    DEPLOYER_WHITELIST.clear()
    DEPLOYER_WHITELIST.update(whitelist.keys())


def update_name_index(queue: dict, tokens: list[dict]):
    """
    更新名称索引: 记录近48小时内见过的 name/symbol 及其不同地址数量
    用于仿盘检测, 不受 eliminated 1000 条上限影响
    """
    now_ms = int(time.time() * 1000)
    idx = queue.setdefault("nameIndex", {})
    for t in tokens:
        addr = (t.get("address") or "").lower()
        if not addr:
            continue
        for field in ("symbol", "name"):
            key = _normalize(t.get(field) or "")
            if key and len(key) >= 2:
                entry = idx.setdefault(key, {"addrs": [], "lastSeen": now_ms})
                if addr not in entry["addrs"]:
                    entry["addrs"].append(addr)
                entry["lastSeen"] = now_ms


# ===================================================================
#  仿盘检测 — 本地队列统计同名/近似名代币
# ===================================================================


def _normalize(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[_\-./·・\s]+", " ", text)
    return text


def detect_copycats(tokens: list[dict],
                    all_known: list[dict],
                    name_index: dict = None) -> dict[str, dict]:
    """
    本地仿盘检测: 在 all_known (队列存活+已淘汰+入场被拒) 中统计同名/近似名代币
    同时参考 name_index (持久化的名称索引, 不受 eliminated 1000 条上限影响)
    返回 {address_lower: {count, isCopycat}}
    count: 同名代币数量 (不含自身)
    isCopycat: count >= 3 时标记为有大量仿盘

    匹配逻辑:
    1. 精确匹配: name/symbol 完全相同
    2. 包含匹配: A 的关键词包含在 B 的关键词中, 或反过来
       (被包含的词长度 ≥ 4 才触发, 避免 "AI" 等短词误匹配)
    name 和 symbol 统一建索引, 交叉匹配
    """
    result: dict[str, dict] = {}
    if not tokens:
        return result

    # 包含匹配最小长度: 中文2字符已有辨识度, 英文需要4字符避免 "AI"/"CZ" 误匹配
    def _min_len(s: str) -> int:
        return 2 if any(ord(c) > 127 for c in s) else 4

    # 统一索引: keyword_lower -> set(addresses)
    keyword_index: dict[str, set[str]] = {}

    # 从 all_known 列表建索引
    for t in (all_known or []):
        addr = (t.get("address") or "").lower()
        if not addr:
            continue
        for field in ("symbol", "name"):
            key = _normalize(t.get(field) or "")
            if key and len(key) >= 2:
                keyword_index.setdefault(key, set()).add(addr)

    # 合并持久化的 nameIndex (补充被 eliminated 1000 条上限截断的历史数据)
    if name_index:
        for key, entry in name_index.items():
            addrs = entry.get("addrs", [])
            if key and len(key) >= 2 and addrs:
                existing = keyword_index.setdefault(key, set())
                for a in addrs:
                    existing.add(a.lower())

    if not keyword_index:
        return result

    # 预提取所有索引 key 列表 (用于包含匹配遍历)
    all_keys = list(keyword_index.keys())

    # 对目标代币查仿盘数
    for t in tokens:
        addr = (t.get("address") or "").lower()
        related = set()

        # 收集该代币的所有关键词
        my_keys = set()
        for field in ("symbol", "name"):
            key = _normalize(t.get(field) or "")
            if key and len(key) >= 2:
                my_keys.add(key)

        for my_key in my_keys:
            # 精确匹配
            related.update(keyword_index.get(my_key, set()))

            # 包含匹配: 遍历索引中的所有 key
            for idx_key in all_keys:
                if idx_key == my_key:
                    continue  # 精确匹配已处理
                # my_key 包含 idx_key (idx_key 是短词, 如 "悟道" 在 "亏完才能悟道" 中)
                if len(idx_key) >= _min_len(idx_key) and idx_key in my_key:
                    related.update(keyword_index[idx_key])
                # idx_key 包含 my_key (my_key 是短词, 如 "悟道" 被 "亏完才能悟道" 包含)
                if len(my_key) >= _min_len(my_key) and my_key in idx_key:
                    related.update(keyword_index[idx_key])

        related.discard(addr)

        count = len(related)
        result[addr] = {
            "count": count,
            "isCopycat": count >= 3,
        }

    copycat_count = sum(1 for v in result.values() if v["isCopycat"])
    if copycat_count > 0:
        log.info("仿盘检测: %d 个有大量仿盘 (≥3)", copycat_count)

    return result


# ===================================================================
#  four.meme API 层
# ===================================================================
def fm_detail(address: str) -> dict | None:
    """获取代币详情 (four.meme Detail API)"""
    _ensure_sessions()
    try:
        r = _fm_session.get(FM_DETAIL, params={"address": address}, timeout=20)
        r.raise_for_status()
        d = r.json()
        if d.get("code") != 0 or not d.get("data"):
            return None
        raw = d["data"]
        tp = raw.get("tokenPrice", {})
        social_links = {}
        if raw.get("twitterUrl"):
            social_links["twitter"] = raw["twitterUrl"]
        if raw.get("telegramUrl"):
            social_links["telegram"] = raw["telegramUrl"]
        if raw.get("webUrl"):
            social_links["website"] = raw["webUrl"]
        # 提取创建时间 (毫秒): 优先 launchTime, 回退 createTime / createdAt
        launch_ts = int(raw.get("launchTime", 0) or 0)
        if launch_ts <= 0:
            launch_ts = int(raw.get("createTime", 0) or raw.get("createdAt", 0) or 0)
        return {
            "holders": int(tp.get("holderCount", 0) or 0),
            "price": float(tp.get("price", 0) or 0),
            "totalSupply": int(raw.get("totalAmount", 0) or 0),
            "socialCount": len(social_links),
            "socialLinks": social_links,
            "descr": raw.get("descr", ""),
            "name": raw.get("name", ""),
            "shortName": raw.get("shortName", ""),
            "progress": float(tp.get("progress", 0) or raw.get("progress", 0) or 0),
            "day1Vol": float(tp.get("day1Vol", 0) or raw.get("day1Vol", 0) or 0),
            "liquidity": float(tp.get("liquidity", 0) or 0),
            "raisedAmount": float(tp.get("raisedAmount", 0) or raw.get("raisedAmount", 0) or 0),
            "launchTime": launch_ts,
        }
    except Exception as e:
        log.debug("fm_detail [%s]: %s", address[:20], e)
        return None


def fm_ticker_prices() -> dict[str, float]:
    _ensure_sessions()
    prices: dict[str, float] = {}
    try:
        r = _fm_session.post(FM_TICKER, json={}, timeout=20)
        r.raise_for_status()
        d = r.json()
        if d.get("code") == 0:
            for t in d.get("data", []):
                sym = t.get("symbol", "")
                if sym.endswith("USDT"):
                    base = sym[:-4].upper()
                    try:
                        prices[base] = float(t["price"])
                    except (ValueError, TypeError, KeyError):
                        pass
    except Exception as e:
        log.error("fm_ticker: %s", e)
    return prices


def flap_ipfs_meta(cid: str) -> dict | None:
    """
    通过 IPFS 网关获取 flap 代币元数据 (含社交链接)。
    代币创建时项目方提交的 metadata 存储在 IPFS, CID 在链上 TokenCreated 事件的 meta 字段。
    返回: {"twitter": str|None, "telegram": str|None, "website": str|None, "description": str}
    """
    if not cid or not cid.startswith("baf"):
        return None
    _ensure_sessions()
    # 多网关容错 (已有 3 个网关, 无需 urllib3 Retry; 用 _ipfs_session 避免重试放大超时)
    # 优先用 CID 子域名格式 (4everland 推荐), 回退到路径格式
    gateways = [
        f"https://{cid}.ipfs.4everland.io/",
        f"https://ipfs.io/ipfs/{cid}",
        f"https://gateway.pinata.cloud/ipfs/{cid}",
    ]
    for gw_url in gateways:
        try:
            r = _ipfs_session.get(gw_url, timeout=4)
            if r.status_code != 200:
                continue
            meta = r.json()
            return {
                "twitter": meta.get("twitter") or None,
                "telegram": meta.get("telegram") or None,
                "website": meta.get("website") or None,
                "description": meta.get("description") or "",
            }
        except Exception:
            continue
    return None


def flap_page_meta(address: str) -> dict | None:
    """
    回退方案: 从 flap.sh 代币页面 SSR 数据中提取社交媒体信息。
    flap.sh 是 Next.js SSR 渲染, metadata 直接嵌入 HTML。
    """
    _ensure_sessions()
    try:
        url = f"{FLAP_PAGE_URL}{address}"
        r = _gt_session.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html",
        })
        if r.status_code != 200:
            return None
        html = r.text
        m = re.search(
            r'"metadata":\{[^}]*"twitter":(?:"[^"]*"|null)[^}]*\}',
            html,
        )
        if not m:
            m = re.search(
                r'"metadata":\{[^}]*"telegram":(?:"[^"]*"|null)[^}]*\}',
                html,
            )
        if not m:
            return None
        raw = m.group()
        json_str = "{" + raw.split(":{", 1)[1]
        meta = json.loads(json_str)
        return {
            "twitter": meta.get("twitter") or None,
            "telegram": meta.get("telegram") or None,
            "website": meta.get("website") or None,
            "description": meta.get("description") or "",
        }
    except Exception as e:
        log.debug("flap_page_meta [%s]: %s", address[:16], e)
        return None


def flap_batch_details(tokens: list[dict]) -> dict[str, dict]:
    """
    批量获取 flap 代币社交媒体信息。
    优先用链上事件中的 IPFS CID 查 IPFS 网关 (轻量、稳定),
    IPFS 失败时回退到 flap.sh 页面 SSR 提取。
    tokens: [{"address": ..., "meta": "bafkrei...", ...}]
    返回: {address: {"twitter": ..., "telegram": ..., "website": ..., "description": ...}}
    """
    if not tokens:
        return {}
    results = {}

    def _query(t: dict) -> tuple[str, dict | None]:
        addr = t["address"]
        cid = t.get("meta", "")
        # 优先 IPFS
        d = flap_ipfs_meta(cid) if cid else None
        # 回退 flap.sh 页面
        if d is None:
            d = flap_page_meta(addr)
        return addr, d

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(_query, t) for t in tokens]
        for f in as_completed(futures):
            addr, detail = f.result()
            if detail:
                results[addr] = detail
    return results


# ===================================================================
#  社交质量查询 — Twitter 粉丝数 + TG 群成员数
# ===================================================================


_TWITTER_RESERVED_PATHS = {
    'search', 'i', 'home', 'explore', 'notifications', 'messages',
    'settings', 'signup', 'login', 'logout', 'account', 'privacy',
    'tos', 'rules', 'help', 'about', 'blog', 'intent', 'status',
    'hashtag', 'lists', 'topics', 'bookmarks', 'communities',
    'jobs', 'ads', 'business', 'developers', 'api', 'share',
}


def _extract_twitter_username(url: str) -> str | None:
    """从推特 URL 中提取用户名"""
    if not url:
        return None
    m = re.search(r'(?:twitter\.com|x\.com)/([A-Za-z0-9_]+)(?:/|$|\?)', url)
    if m:
        username = m.group(1)
        if username.lower() not in _TWITTER_RESERVED_PATHS:
            return username
    return None


def _extract_telegram_username(url: str) -> str | None:
    """从 TG URL 中提取用户名"""
    if not url:
        return None
    m = re.search(r't\.me/([A-Za-z0-9_]+)(?:/|$|\?)', url)
    if m:
        return m.group(1)
    m = re.search(r'telegram\.(?:me|dog)/([A-Za-z0-9_]+)(?:/|$|\?)', url)
    if m:
        return m.group(1)
    return None


def _fetch_twitter_followers(username: str) -> int | None:
    """
    通过抓取 X 个人页面的嵌入式 JSON 数据获取粉丝数 (无需认证)。
    X 服务端渲染 HTML 中包含 follow 按钮的 JSON 配置, 里面有 followers_count。
    返回粉丝数, 失败返回 None。
    """
    _ensure_sessions()
    try:
        r = _gt_session.get(
            f"https://x.com/{username}",
            timeout=10,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html",
            },
        )
        if r.status_code != 200:
            return None
        m = re.search(r'"followers_count":(\d+)', r.text)
        if m:
            return int(m.group(1))
    except Exception as e:
        log.debug("Twitter 粉丝数查询失败 [%s]: %s", username, e)
    return None


def _fetch_telegram_members(username: str) -> int | None:
    """
    通过抓取 Telegram 公共预览页获取群成员/频道订阅数。
    公共页面 HTML 中包含 'tgme_page_extra' 元数据。
    返回成员数, 失败返回 None。
    """
    _ensure_sessions()
    try:
        r = _gt_session.get(
            f"https://t.me/{username}",
            timeout=10,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html",
            },
        )
        if r.status_code != 200:
            return None
        html = r.text
        m = re.search(
            r'<div[^>]+class="tgme_page_extra"[^>]*>([^<]+)</div>',
            html,
        )
        if not m:
            return None
        text = m.group(1).strip().lower()
        num_str = re.sub(r'[^\d]', '', text.split()[0])
        if num_str:
            return int(num_str)
    except Exception as e:
        log.debug("TG 成员数查询失败 [%s]: %s", username, e)
    return None


def batch_check_social_quality(candidates: list[dict]) -> None:
    """
    并行查询推特粉丝数和 TG 群成员数, 写入候选代币的 _social_quality 字段。
    - Twitter: 非推文链接且有粉丝数≥100 → 标记质量好
    - TG: 有成员数≥100 → 标记质量好
    写入字段 (不调用则默认为 None):
      _tw_followers: int | None
      _tg_members: int | None
      _social_quality_good: bool (任一渠道≥100)
    """
    if not candidates:
        return

    # 收集需要查询的社交链接
    tw_usernames = {}
    tg_usernames = {}
    for t in candidates:
        social_links = t.get("socialLinks", {})
        twitter_url = social_links.get("twitter", "")
        tg_url = social_links.get("telegram", "")

        if twitter_url and "/status/" not in twitter_url:
            uname = _extract_twitter_username(twitter_url)
            if uname:
                tw_usernames.setdefault(uname, set()).add(t["address"])

        if tg_url:
            uname = _extract_telegram_username(tg_url)
            if uname:
                tg_usernames.setdefault(uname, set()).add(t["address"])

    if not tw_usernames and not tg_usernames:
        return

    tw_results = {}
    tg_results = {}

    with ThreadPoolExecutor(max_workers=5) as pool:
        tw_futures = {
            pool.submit(_fetch_twitter_followers, uname): uname
            for uname in tw_usernames
        }
        for f in as_completed(tw_futures):
            uname = tw_futures[f]
            try:
                count = f.result()
                if count is not None:
                    tw_results[uname] = count
            except Exception:
                pass

        tg_futures = {
            pool.submit(_fetch_telegram_members, uname): uname
            for uname in tg_usernames
        }
        for f in as_completed(tg_futures):
            uname = tg_futures[f]
            try:
                count = f.result()
                if count is not None:
                    tg_results[uname] = count
            except Exception:
                pass

    for t in candidates:
        social_links = t.get("socialLinks", {})
        twitter_url = social_links.get("twitter", "")
        tg_url = social_links.get("telegram", "")

        tw_count = None
        tg_count = None

        if twitter_url and "/status/" not in twitter_url:
            uname = _extract_twitter_username(twitter_url)
            if uname and uname in tw_results:
                tw_count = tw_results[uname]

        if tg_url:
            uname = _extract_telegram_username(tg_url)
            if uname and uname in tg_results:
                tg_count = tg_results[uname]

        t["_tw_followers"] = tw_count
        t["_tg_members"] = tg_count
        good = (
            (tw_count is not None and tw_count >= 100)
            or (tg_count is not None and tg_count >= 100)
        )
        t["_social_quality_good"] = good

    tw_checked = len(tw_results)
    tg_checked = len(tg_results)
    tw_good = sum(1 for v in tw_results.values() if v >= 100)
    tg_good = sum(1 for v in tg_results.values() if v >= 100)
    if tw_checked or tg_checked:
        log.info("社交质量: 推特 %d 个 (%d 达标≥100), TG %d 个 (%d 达标≥100)",
                 tw_checked, tw_good, tg_checked, tg_good)


def flap_get_token_states(addresses: list[str]) -> dict[str, dict]:
    """
    通过 RPC eth_call 调用 Portal 合约的 getTokenV5(address) 读取 flap 代币状态。
    getTokenV5 返回 (ABI 编码的 tuple, 所有数值 18 位精度):
      [0] status (uint256): 0=Invalid, 1=Tradable, 2=InDuel, 3=Killed, 4=DEX
      [1] reserve (uint256): 已筹集 quote token 数量 (wei)
      [2] supply (uint256): 当前流通供应量 (wei)
      [3] price (uint256): 当前代币价格 (wei, quote_token/代币)
      [4] flags (uint256)
      [5] r (uint256): 虚拟 quote reserve (wei)
      [6] h (uint256): 虚拟 token reserve (wei)
      [7] K (uint256): 常数积 (存储值 = 实际K * 1e18)
      [8] targetSupply (uint256): 毕业目标供应量 (wei)
      [9] quoteToken (address): 计价代币地址 (零地址=原生BNB, 否则为ERC-20如USD1/USDT等)
    bonding curve: (x + h)(y + r) = K/1e18, 其中 x=1e9-supply, y=reserve (均为 ether 单位)
    进度 = reserve / target_reserve
    返回: {address: {"reserve": float, "progress": float(0~1), "price_native": float,
                     "graduated": bool, "quote_token": str}}
    quote_token: 零地址=BNB计价(需乘BNB/USD), 稳定币地址=USD计价(无需换算), 其他=非标计价
    """
    if not addresses:
        return {}
    results = {}
    # getTokenV5(address) selector: 0x5c4bc504
    GET_TOKEN_V5_SELECTOR = "0x5c4bc504"

    def _query_one(addr: str) -> tuple[str, dict | None]:
        call_data = GET_TOKEN_V5_SELECTOR + addr[2:].lower().zfill(64)
        res = _rpc_call("eth_call", [
            {"to": FLAP_PORTAL_CONTRACT, "data": call_data},
            "latest",
        ])
        if not res or not res.get("result") or res["result"] == "0x":
            return addr, None
        try:
            raw = res["result"][2:]  # 去掉 0x
            if len(raw) < 576:  # 至少 9 个 word
                return addr, None
            words = [int(raw[i:i+64], 16) for i in range(0, min(len(raw), 768), 64)]

            status = words[0]       # 0=Invalid, 1=Tradable, 2=InDuel, 3=Killed, 4=DEX
            reserve_wei = words[1]  # 已筹 quote token (wei)
            price_wei = words[3]    # 当前价格 (wei, quote_token/代币)

            reserve_native = reserve_wei / 1e18
            price_native = price_wei / 1e18
            graduated = (status == 4)  # DEX 状态 = 已毕业

            # 解析 quote token 地址 (word[9], 零地址=原生BNB)
            quote_token = ZERO_ADDRESS
            if len(words) > 9 and len(raw) >= 640:
                qt_hex = raw[9 * 64 + 24: 9 * 64 + 64]
                quote_token = ("0x" + qt_hex).lower()

            # 计算进度: reserve / target_reserve
            # bonding curve: (x + h)(y + r) = K (ether 单位, K = K_raw / 1e18)
            # 毕业时 supply = targetSupply → x_target = 1e9 - targetSupply
            # target_reserve = K / (x_target + h) - r
            progress = 0.0
            if graduated:
                progress = 1.0
            elif len(words) > 8 and words[5] > 0 and words[7] > 0 and words[8] > 0:
                r_virtual = words[5] / 1e18
                h_tokens = words[6] / 1e18
                K = words[7] / 1e18  # 存储值除以 1e18 得到实际 K
                target_supply = words[8] / 1e18
                x_target = 1e9 - target_supply  # 毕业时剩余 token 数
                denominator = x_target + h_tokens
                if denominator > 0:
                    target_reserve = K / denominator - r_virtual
                    if target_reserve > 0:
                        progress = min(reserve_native / target_reserve, 1.0)
            elif reserve_native > 0:
                # 兜底: 无 bonding curve 参数时, 用经验值估算
                fallback_target = 16.0 if quote_token == ZERO_ADDRESS else 10000.0
                progress = min(reserve_native / fallback_target, 1.0)

            if status == 0:  # Invalid
                return addr, None

            return addr, {
                "reserve": reserve_native,
                "progress": progress,
                "price_native": price_native,
                "quote_token": quote_token,
                "graduated": graduated,
            }
        except Exception as e:
            log.debug("flap_get_token_states 解析失败 [%s]: %s", addr[:16], e)
            return addr, None

    with ThreadPoolExecutor(max_workers=10) as pool:
        for addr, state in pool.map(lambda a: _query_one(a), addresses):
            if state:
                results[addr] = state

    return results


def flap_price_to_usd(price_native: float, quote_token: str) -> float:
    """
    将 flap 链上价格 (quote_token 计价) 转换为 USD。
    - 零地址 = 原生 BNB → 乘以 BNB/USD
    - 稳定币 (USDT/BUSD/USDC/USD1) → 已是 USD, 直接返回
    - 其他代币 → 无法换算, 返回 0 (依赖 DexScreener 提供 USD 价格)
    """
    if price_native <= 0:
        return 0.0
    qt = quote_token.lower()
    if qt == ZERO_ADDRESS:
        # BNB 计价 → 乘以 BNB/USD
        return price_native * _bnb_usd_price
    if qt in FLAP_STABLE_QUOTES:
        # 稳定币计价 → 已是 USD
        return price_native
    # 非标代币计价 (如"币安人生") → 无法换算
    return 0.0


def erc20_total_supplies(addresses: list[str]) -> dict[str, int]:
    """
    通过 RPC eth_call 批量读取 ERC-20 totalSupply()。
    返回: {address: total_supply_int} (已除以 decimals=18, 取整数)
    totalSupply() selector: 0x18160ddd
    """
    if not addresses:
        return {}
    results = {}

    def _query_one(addr: str) -> tuple[str, int | None]:
        res = _rpc_call("eth_call", [
            {"to": addr, "data": "0x18160ddd"},
            "latest",
        ])
        if not res or not res.get("result") or res["result"] == "0x":
            return addr, None
        try:
            raw_val = int(res["result"], 16)
            return addr, raw_val // (10 ** 18)  # 18 位精度
        except Exception:
            return addr, None

    with ThreadPoolExecutor(max_workers=10) as pool:
        for addr, supply in pool.map(lambda a: _query_one(a), addresses):
            if supply is not None:
                results[addr] = supply

    return results


# ===================================================================
#  BSC RPC — 链上事件发现
# ===================================================================
def _rpc_call(method: str, params: list) -> dict | None:
    """BSC RPC JSON-RPC 调用"""
    _ensure_sessions()
    try:
        r = _bsc_session.post(BSC_RPC, json={
            "jsonrpc": "2.0", "method": method, "params": params, "id": 1,
        }, timeout=30, headers={"Content-Type": "application/json"})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("RPC 调用失败 [%s]: %s", method, e)
        return None


def _parse_token_from_log(log_entry: dict, source: str = "four.meme") -> dict | None:
    """
    从 eth_getLogs 返回的单条日志中解析代币信息。
    支持多平台事件格式:
      four.meme 旧版 (12 words): word[0]=creator, word[1]=token, word[6]=timestamp, word[8+]=name/symbol
      four.meme 新版 (8 words):  word[0]=token, word[1]=creator, 无时间戳/名称 (需 detail API 补全)
      flap (动态长度): word[0]=ts, word[1]=creator, word[2]=nonce, word[3]=token, 后跟 name/symbol/meta 动态字符串
    """
    try:
        data = log_entry["data"][2:]  # 去掉 0x 前缀
        if len(data) < 128:
            return None

        n_words = len(data) // 64

        if source == "flap":
            # flap TokenCreated 事件:
            # word[0]=ts(uint256), word[1]=creator(address), word[2]=nonce(uint256),
            # word[3]=token(address), word[4]=offset(name), word[5]=offset(symbol), word[6]=offset(meta)
            # 后跟动态字符串数据
            if n_words < 7:
                return None

            create_ts = int(data[0:64], 16)  # word[0]: 时间戳
            creator_addr = ("0x" + data[88:128]).lower()  # word[1]: creator
            token_addr = ("0x" + data[216:256]).lower()  # word[3]: token

            # 验证时间戳合理性
            TS_MIN, TS_MAX = 1577808000, 1893456000
            if not (TS_MIN <= create_ts <= TS_MAX):
                create_ts = 0

            # 解码 name, symbol, meta (动态字符串)
            name, symbol, meta_cid = "", "", ""
            try:
                # word[4] 是 name 的偏移量 (相对于 data 起始)
                name_offset = int(data[256:320], 16) * 2  # 字节偏移 → hex 字符偏移
                if name_offset + 64 <= len(data):
                    name_len = int(data[name_offset:name_offset + 64], 16)
                    if 0 < name_len < 200:
                        name_data_start = name_offset + 64
                        name = bytes.fromhex(
                            data[name_data_start:name_data_start + name_len * 2]
                        ).decode("utf-8", errors="replace")

                # word[5] 是 symbol 的偏移量
                sym_offset = int(data[320:384], 16) * 2
                if sym_offset + 64 <= len(data):
                    sym_len = int(data[sym_offset:sym_offset + 64], 16)
                    if 0 < sym_len < 100:
                        sym_data_start = sym_offset + 64
                        symbol = bytes.fromhex(
                            data[sym_data_start:sym_data_start + sym_len * 2]
                        ).decode("utf-8", errors="replace")

                # word[6] 是 meta 的偏移量 (IPFS CID 字符串, 如 bafkrei...)
                meta_offset = int(data[384:448], 16) * 2
                if meta_offset + 64 <= len(data):
                    meta_len = int(data[meta_offset:meta_offset + 64], 16)
                    if 0 < meta_len < 200:
                        meta_data_start = meta_offset + 64
                        meta_cid = bytes.fromhex(
                            data[meta_data_start:meta_data_start + meta_len * 2]
                        ).decode("utf-8", errors="replace")
            except Exception:
                pass

            # flap 代币后缀: 标准代币 8888, 税币 7777
            if not token_addr.endswith("8888") and not token_addr.endswith("7777"):
                return None

            return {
                "address": token_addr,
                "creator": creator_addr,
                "createdAt": create_ts * 1000 if create_ts else 0,
                "name": name,
                "symbol": symbol,
                "meta": meta_cid,  # IPFS CID, 含社交媒体元数据
                "block": int(log_entry["blockNumber"], 16),
                "source": "flap",
            }

        # --- four.meme 事件解析 ---
        # 根据 data 长度区分新旧版本
        if n_words >= 10:
            # 旧版 (12 words): word[0]=creator, word[1]=token
            token_addr = ("0x" + data[88:128]).lower()
            creator_addr = ("0x" + data[24:64]).lower()

            # 创建时间戳 — word[6] 位置
            create_ts = 0
            TS_MIN, TS_MAX = 1577808000, 1893456000
            for word_idx in (6, 5, 7, 4):
                try:
                    offset = word_idx * 64
                    if len(data) >= offset + 64:
                        val = int(data[offset:offset + 64], 16)
                        if TS_MIN <= val <= TS_MAX:
                            create_ts = val
                            break
                except Exception:
                    continue

            # 解码名称和符号
            name, symbol = "", ""
            try:
                if len(data) >= 576:
                    name_len = int(data[512:576], 16)  # word[8]
                    if 0 < name_len < 200:
                        name = bytes.fromhex(data[576:576 + name_len * 2]).decode("utf-8", errors="replace")
                    name_words = max(1, (name_len + 31) // 32)
                    sym_len_offset = (9 + name_words) * 64
                    if sym_len_offset + 64 <= len(data):
                        sym_len = int(data[sym_len_offset:sym_len_offset + 64], 16)
                        if 0 < sym_len < 100:
                            symbol = bytes.fromhex(
                                data[sym_len_offset + 64:sym_len_offset + 64 + sym_len * 2]
                            ).decode("utf-8", errors="replace")
            except Exception:
                pass

        elif n_words == 8:
            # 新版 (8 words): word[0]=token, word[1]=creator, 无时间戳/名称
            token_addr = ("0x" + data[24:64]).lower()
            creator_addr = ("0x" + data[88:128]).lower()
            create_ts = 0  # 新版事件无时间戳, 由区块时间戳补全
            name, symbol = "", ""  # 新版事件无名称, 由 detail API 补全

        else:
            return None

        # 只保留 four.meme 代币 (后缀 4444 或 ffff)
        if not token_addr.endswith("4444") and not token_addr.endswith("ffff"):
            return None

        return {
            "address": token_addr,
            "creator": creator_addr,
            "createdAt": create_ts * 1000 if create_ts else 0,  # 毫秒
            "name": name,
            "symbol": symbol,
            "block": int(log_entry["blockNumber"], 16),
            "source": "four.meme",
        }
    except Exception:
        return None


def discover_on_chain(from_block: int) -> tuple[list[dict], int]:
    """
    链上发现: 通过 BSC RPC eth_getLogs 查询所有工厂合约的代币创建事件
    支持多平台 (TOKEN_FACTORIES: four.meme + flap), 新增平台只需在常量区追加即可。
    返回: (新代币列表, 最新区块号)
    """
    # 获取最新区块号
    block_res = _rpc_call("eth_blockNumber", [])
    if not block_res or not block_res.get("result"):
        log.warning("获取最新区块号失败")
        return [], from_block
    latest_block = int(block_res["result"], 16)

    if from_block <= 0:
        # 首次运行: 只扫最近 15 分钟 (~300 blocks, BSC ~3s/block)
        from_block = latest_block - 300

    # 安全上限: 不超过 5000 blocks (~4h, 防止长时间未运行后积压)
    if latest_block - from_block > 5000:
        log.warning("区块跨度过大 (%d), 截断到最近 5000 blocks", latest_block - from_block)
        from_block = latest_block - 5000

    log.info("链上扫描区块 %d ~ %d (%d blocks), 监听 %d 个工厂合约 (four.meme + flap)",
             from_block, latest_block, latest_block - from_block, len(TOKEN_FACTORIES))

    tokens = []
    seen_addrs = set()  # 去重 (理论上不同合约不会创建同一代币, 但防御性编程)
    chunk = 10000
    current = from_block

    while current <= latest_block:
        end = min(current + chunk - 1, latest_block)

        # 对每个工厂合约 + 事件组合分别查询
        for factory_addr, event_topic, source in TOKEN_FACTORIES:
            try:
                res = _rpc_call("eth_getLogs", [{
                    "address": factory_addr,
                    "fromBlock": hex(current),
                    "toBlock": hex(end),
                    "topics": [event_topic],
                }])

                if not res:
                    continue

                if res.get("error"):
                    err_msg = res["error"].get("message", "")
                    if "pruned" in err_msg:
                        continue
                    log.warning("RPC error (%s): %s", factory_addr[:10], err_msg)
                    continue

                for log_entry in (res.get("result") or []):
                    parsed = _parse_token_from_log(log_entry, source)
                    if parsed and parsed["address"] not in seen_addrs:
                        seen_addrs.add(parsed["address"])
                        tokens.append(parsed)

            except Exception as e:
                log.warning("链上扫描异常 (%s): %s", factory_addr[:10], e)

        current = end + 1
        time.sleep(0.1)

    # 按来源统计
    fm_count = sum(1 for t in tokens if t.get("source") == "four.meme")
    flap_count = sum(1 for t in tokens if t.get("source") == "flap")
    log.info("链上发现 %d 个新代币 (four.meme: %d, flap: %d)", len(tokens), fm_count, flap_count)

    # 回退: 对 createdAt=0 的代币, 用区块时间戳补全
    need_block_ts = [t for t in tokens if t.get("createdAt", 0) == 0]
    if need_block_ts:
        block_nums = list({t["block"] for t in need_block_ts})
        log.info("补全 %d 个代币的创建时间 (查 %d 个区块时间戳)", len(need_block_ts), len(block_nums))
        block_ts_map = {}

        def _fetch_block_ts(bn: int) -> tuple[int, int]:
            res = _rpc_call("eth_getBlockByNumber", [hex(bn), False])
            if res and res.get("result"):
                ts = int(res["result"].get("timestamp", "0x0"), 16)
                if ts > 0:
                    return bn, ts
            return bn, 0

        with ThreadPoolExecutor(max_workers=10) as pool:
            for bn, ts in pool.map(_fetch_block_ts, block_nums):
                if ts > 0:
                    block_ts_map[bn] = ts

        now_sec = int(time.time())
        for t in need_block_ts:
            ts = block_ts_map.get(t["block"], 0)
            if ts > 0:
                t["createdAt"] = ts * 1000
                log.info("  补全 %s 创建时间: %d", t.get("name") or t["address"][:16], ts)
            else:
                # 兜底: 区块时间戳也查不到, 用当前时间代替 (至少不会算成 1970 年)
                t["createdAt"] = now_sec * 1000
                log.warning("  补全失败 %s, 使用当前时间兜底", t.get("name") or t["address"][:16])

    return tokens, latest_block


# ===================================================================
#  RPC 查持币地址数 — 通过 eth_getLogs 查 ERC-20 Transfer 事件
#  作为 BSCScan tokenholdercount (PRO 端点) 的免费替代方案
#  通过追踪每个地址的净余额 (转入-转出), 只统计余额>0的地址
# ===================================================================
def rpc_holder_counts(token_infos: list[dict],
                      return_logs: bool = False
                      ) -> dict[str, int] | tuple[dict[str, int], dict[str, list]]:
    """
    RPC 查持币地址数: 通过 eth_getLogs 查 ERC-20 Transfer 事件
    追踪每个地址的净余额 (转入金额 - 转出金额), 只统计余额 > 0 的地址
    token_infos: [{"address": ..., "block": ..., "createdAt": ...}]
    return_logs: True 时同时返回原始日志 (用于聪明钱匹配)
    返回: {address: holder_count} 或 ({address: holder_count}, {address: [logs]})
    """
    result = {}
    logs_map: dict[str, list] = {}
    if not token_infos:
        return (result, logs_map) if return_logs else result

    # 获取当前区块号
    latest_block = 0
    try:
        block_res = _rpc_call("eth_blockNumber", [])
        if block_res and block_res.get("result"):
            latest_block = int(block_res["result"], 16)
    except Exception:
        pass

    def _query_one(info: dict) -> tuple[str, int | None, list]:
        addr = info["address"]
        block = info.get("block", 0)
        try:
            # 确定起始区块: 优先用代币创建区块, 否则用最大范围 (50000块 ≈ 42h)
            if block > 0:
                from_block = max(0, block - 1)
            else:
                from_block = max(0, latest_block - 50000)

            # RPC 限制最大 50000 块, 超过时分段查询
            all_logs = []
            chunk_size = 50000
            current = from_block
            while current <= latest_block:
                end = min(current + chunk_size - 1, latest_block)
                res = _rpc_call("eth_getLogs", [{
                    "address": addr,
                    "fromBlock": hex(current),
                    "toBlock": hex(end),
                    "topics": [ERC20_TRANSFER_TOPIC],
                }])
                if not res or res.get("error"):
                    err_msg = (res or {}).get("error", {}).get("message", "")
                    if "exceed" in err_msg or "range" in err_msg:
                        chunk_size = chunk_size // 2
                        if chunk_size < 1000:
                            break
                        continue
                    break
                all_logs.extend(res.get("result") or [])
                current = end + 1

            if not all_logs:
                return addr, None, []

            # 追踪每个地址的净余额
            balances: dict[str, int] = {}
            for log_entry in all_logs:
                topics = log_entry.get("topics", [])
                if len(topics) < 3:
                    continue
                from_addr = ("0x" + topics[1][26:]).lower()
                to_addr = ("0x" + topics[2][26:]).lower()
                data = log_entry.get("data", "0x0")
                value = int(data, 16) if data and data != "0x" else 0
                if from_addr not in BURN_ADDRESSES:
                    balances[from_addr] = balances.get(from_addr, 0) - value
                if to_addr not in BURN_ADDRESSES:
                    balances[to_addr] = balances.get(to_addr, 0) + value

            exclude = {ZERO_ADDRESS, DEAD_ADDRESS, addr.lower(),
                       FOUR_MEME_CONTRACT.lower()}
            holder_count = sum(1 for a, bal in balances.items()
                               if bal > 0 and a not in exclude)
            return addr, holder_count if holder_count > 0 else None, all_logs

        except Exception:
            return addr, None, []

    # 并发查询 (10 线程)
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(_query_one, info) for info in token_infos]
        for f in as_completed(futures):
            addr, count, raw_logs = f.result()
            if count is not None:
                result[addr] = count
            if return_logs and raw_logs:
                logs_map[addr] = raw_logs

    log.info("RPC 查到 %d/%d 个代币持币数", len(result), len(token_infos))
    return (result, logs_map) if return_logs else result


# ===================================================================
#  BSCScan API — 统一请求封装 (Etherscan V2, chainid=56)
# ===================================================================
BSCSCAN_TIMEOUT = 15
BSCSCAN_MAX_RETRIES = 2


def _bscscan_get(params: dict, api_key: str,
                 timeout: int = BSCSCAN_TIMEOUT) -> dict | None:
    """BSCScan API 统一 GET 请求封装 (Etherscan V2 API, chainid=56)"""
    if not api_key:
        return None
    _ensure_sessions()
    params = {**params, "apikey": api_key, "chainid": "56"}
    for attempt in range(BSCSCAN_MAX_RETRIES + 1):
        try:
            r = _bsc_session.get(BSCSCAN_API, params=params, timeout=timeout)
            if r.status_code == 429:
                wait = 3 * (attempt + 1)
                log.warning("BSCScan 429, 等待 %ds (%d/%d)",
                            wait, attempt + 1, BSCSCAN_MAX_RETRIES + 1)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ReadTimeout:
            if attempt < BSCSCAN_MAX_RETRIES:
                log.debug("BSCScan 超时, 重试 (%d/%d)", attempt + 1, BSCSCAN_MAX_RETRIES)
                time.sleep(2)
                continue
            log.warning("BSCScan 超时, 已达最大重试次数")
        except Exception as e:
            log.debug("BSCScan 请求异常: %s", e)
            if attempt < BSCSCAN_MAX_RETRIES:
                time.sleep(2)
                continue
    return None


def bscscan_holder_count(token_address: str, api_key: str) -> int | None:
    """通过 BSCScan API 获取链上真实持仓地址数"""
    d = _bscscan_get({
        "module": "token", "action": "tokenholdercount",
        "contractaddress": token_address,
    }, api_key)
    if d and d.get("status") == "1" and d.get("result"):
        try:
            return int(d["result"])
        except (ValueError, TypeError):
            pass
    return None


def bscscan_holder_counts_batch(addresses: list[str], api_key: str) -> dict[str, int]:
    """批量查询持币地址数"""
    result = {}
    if not api_key:
        return result
    for addr in addresses:
        count = bscscan_holder_count(addr, api_key)
        if count is not None:
            result[addr] = count
        time.sleep(0.2)
    log.info("BSCScan 查到 %d/%d 个代币持币数", len(result), len(addresses))
    return result


def bscscan_get_token_creator(token_address: str, api_key: str) -> str | None:
    """通过 BSCScan API 获取代币合约创建者地址"""
    d = _bscscan_get({
        "module": "contract", "action": "getcontractcreation",
        "contractaddresses": token_address,
    }, api_key)
    if d and d.get("status") == "1" and d.get("result"):
        return d["result"][0].get("contractCreator", "").lower()
    return None


def bscscan_get_token_transfers(token_address: str, address: str,
                                api_key: str, page: int = 1,
                                offset: int = 100) -> list[dict]:
    """通过 BSCScan API 获取指定地址的代币转账记录"""
    d = _bscscan_get({
        "module": "account", "action": "tokentx",
        "contractaddress": token_address,
        "address": address,
        "page": page, "offset": offset,
        "sort": "desc",
    }, api_key)
    if d and d.get("status") == "1" and d.get("result"):
        return d["result"]
    return []


def bscscan_top_holders(token_address: str, api_key: str,
                        offset: int = 50) -> list[dict]:
    """通过 BSCScan API 获取 Top Holders 列表"""
    d = _bscscan_get({
        "module": "token", "action": "tokenholderlist",
        "contractaddress": token_address,
        "page": 1, "offset": offset,
    }, api_key)
    if d and d.get("status") == "1" and d.get("result"):
        return d["result"]
    return []


def bscscan_top_holders_batch(addresses: list[str], api_key: str,
                               top_n: int = 10) -> dict[str, list[dict]]:
    """
    批量查询代币 Top Holders (用于入队时采集数据)
    返回: {address: [{"addr": "0x...", "balance": 123}, ...]}
    每个代币返回 top_n 个持仓地址及其余额
    限流: BSCScan ~5 req/s, 串行查询, 每次间隔 0.25s
    """
    if not api_key or not addresses:
        return {}
    result = {}
    for token_addr in addresses:
        try:
            raw = bscscan_top_holders(token_addr, api_key, offset=top_n)
            if not raw:
                continue
            holders_list = []
            for h in raw:
                holder_addr = (h.get("TokenHolderAddress") or "").lower()
                if not holder_addr or holder_addr in BURN_ADDRESSES:
                    continue
                balance = int(h.get("TokenHolderQuantity", 0) or 0)
                holders_list.append({
                    "addr": holder_addr,
                    "balance": balance,
                })
            if holders_list:
                result[token_addr] = holders_list[:top_n]
            time.sleep(0.25)  # 限流: 4 req/s, 留余量
        except Exception as e:
            log.debug("Top Holders 查询失败 %s: %s", token_addr[:16], e)
    return result


# ===================================================================
#  链上行为分析 — 开发者行为
# ===================================================================
def analyze_developer_behavior(token_address: str, creator: str,
                               api_key: str) -> dict:
    """
    分析开发者链上行为
    改进: DEX Router 精确匹配, LP token mint/burn 检测, 销毁地址排除
    """
    result = {
        "has_sell": False, "has_buy": False,
        "has_lp_add": False, "has_lp_remove": False,
        "sell_pct": 0.0, "details": [],
        "bonus": 0, "exclude": False,
    }
    if not creator or not api_key:
        return result

    token_lower = token_address.lower()
    creator_lower = creator.lower()

    transfers = bscscan_get_token_transfers(token_address, creator, api_key)
    if not transfers:
        return result

    total_in = 0
    total_out = 0
    buy_count = 0
    sell_count = 0
    lp_add_count = 0
    lp_remove_count = 0

    for tx in transfers:
        from_addr = (tx.get("from") or "").lower()
        to_addr = (tx.get("to") or "").lower()
        value = int(tx.get("value", 0) or 0)
        contract_addr = (tx.get("contractAddress") or "").lower()

        if value <= 0:
            continue

        if contract_addr == token_lower:
            if to_addr == creator_lower:
                total_in += value
                if from_addr in KNOWN_DEX_ROUTERS or from_addr == ZERO_ADDRESS:
                    buy_count += 1
            elif from_addr == creator_lower:
                # 转到零地址/死地址 = 销毁, 不算卖出
                if to_addr in BURN_ADDRESSES:
                    continue
                total_out += value
                if to_addr in KNOWN_DEX_ROUTERS:
                    sell_count += 1
                else:
                    # 转到其他地址也算减仓
                    sell_count += 1
        else:
            # 非该代币的转账 — 检查是否是 LP token 操作
            token_name_lower = (tx.get("tokenName") or "").lower()
            if "lp" in token_name_lower or "pancake" in token_name_lower:
                if from_addr == ZERO_ADDRESS and to_addr == creator_lower:
                    lp_add_count += 1
                elif from_addr == creator_lower and to_addr == ZERO_ADDRESS:
                    lp_remove_count += 1

    if total_in > 0:
        result["sell_pct"] = min(100.0, (total_out / total_in) * 100)

    if sell_count > 0:
        result["has_sell"] = True
        if result["sell_pct"] >= 90:
            result["details"].append(f"开发者清仓 (卖出{result['sell_pct']:.0f}%)")
        else:
            result["details"].append(f"开发者减仓 (卖出{result['sell_pct']:.0f}%)")
        result["exclude"] = True

    if buy_count > 0:
        result["has_buy"] = True
        result["details"].append(f"开发者加仓 ({buy_count}笔)")
        result["bonus"] += 1

    if lp_add_count > 0:
        result["has_lp_add"] = True
        result["details"].append(f"开发者加池子 ({lp_add_count}笔)")
        result["bonus"] += 1

    if lp_remove_count > 0:
        result["has_lp_remove"] = True
        result["details"].append(f"开发者撤池子 ({lp_remove_count}笔)")
        result["exclude"] = True

    return result


# ===================================================================
#  币安 Web3 聪明钱信号
# ===================================================================
# 缓存: {ts: float, signals: dict[str, dict]}
_binance_signal_cache: dict = {"ts": 0, "signals": {}}
BINANCE_SIGNAL_CACHE_TTL = 300  # 5 分钟缓存 (信号更新频率不高)


def fetch_binance_smart_signals() -> dict[str, dict]:
    """
    从币安 Web3 钱包 API 获取 BSC 聪明钱信号
    首次请求超时 5 秒快速失败, 避免 API 不可用时阻塞
    """
    global _binance_signal_cache
    now = time.time()
    if now - _binance_signal_cache["ts"] < BINANCE_SIGNAL_CACHE_TTL and _binance_signal_cache["signals"]:
        return _binance_signal_cache["signals"]

    _ensure_sessions()
    signal_map: dict[str, dict] = {}

    def _process_items(items: list):
        for item in items:
            addr = (item.get("contractAddress") or "").lower()
            if not addr or len(addr) != 42:
                continue
            tag_events = []
            for events in ((item.get("tokenTag") or {}).get("Sensitive Events") or []):
                tag_name = events.get("tagName", "")
                if tag_name:
                    tag_events.append(tag_name)
            signal = {
                "direction": item.get("direction", ""),
                "smartMoneyCount": item.get("smartMoneyCount", 0),
                "exitRate": item.get("exitRate", 0),
                "maxGain": item.get("maxGain", "0"),
                "status": item.get("status", ""),
                "alertPrice": item.get("alertPrice", "0"),
                "currentPrice": item.get("currentPrice", "0"),
                "totalTokenValue": item.get("totalTokenValue", "0"),
                "signalTriggerTime": item.get("signalTriggerTime", 0),
                "ticker": item.get("ticker", ""),
                "tagEvents": tag_events,
            }
            if addr not in signal_map or signal["smartMoneyCount"] > signal_map[addr]["smartMoneyCount"]:
                signal_map[addr] = signal

    try:
        # 第一页用短超时探测可用性
        resp = _bn_session.post(
            BINANCE_SMART_SIGNAL,
            json={"smartSignalType": "", "page": 1, "pageSize": 100, "chainId": "56"},
            timeout=5,
        )
        if resp.status_code != 200:
            log.info("币安聪明钱信号: 不可用 (status=%d), 跳过", resp.status_code)
            _binance_signal_cache = {"ts": now, "signals": signal_map}
            return signal_map

        data = resp.json()
        if data.get("code") != "000000" or not data.get("data"):
            _binance_signal_cache = {"ts": now, "signals": signal_map}
            return signal_map

        first_items = data["data"]
        _process_items(first_items)

        # 继续拉后续页
        if len(first_items) >= 100:
            for page in range(2, 4):
                try:
                    r = _bn_session.post(
                        BINANCE_SMART_SIGNAL,
                        json={"smartSignalType": "", "page": page, "pageSize": 100, "chainId": "56"},
                        timeout=10,
                    )
                    if r.status_code != 200:
                        break
                    d = r.json()
                    if d.get("code") != "000000" or not d.get("data"):
                        break
                    _process_items(d["data"])
                    if len(d["data"]) < 100:
                        break
                    time.sleep(0.3)
                except Exception:
                    break

        if signal_map:
            log.info("币安聪明钱信号: 获取 %d 个 BSC 代币信号", len(signal_map))

    except Exception as e:
        log.info("币安聪明钱信号: 不可用: %s", e)

    _binance_signal_cache = {"ts": now, "signals": signal_map}
    return signal_map


def fetch_binance_token_dynamic(addresses: list[str]) -> dict[str, dict]:
    """
    从币安 Web3 Token Dynamic Data API 批量获取代币动态数据
    连续 3 次失败则判定 API 不可用, 快速放弃
    """
    _ensure_sessions()
    result: dict[str, dict] = {}
    if not addresses:
        return result

    consec_fails = 0
    for i, addr in enumerate(addresses):
        if consec_fails >= 3:
            log.info("币安动态: 连续失败 %d 次, 跳过剩余 %d 个", consec_fails, len(addresses) - i)
            break

        try:
            resp = _bn_session.get(
                BINANCE_TOKEN_DYNAMIC,
                params={"chainId": "56", "contractAddress": addr},
                timeout=5,
            )
            if resp.status_code != 200:
                consec_fails += 1
                continue

            body = resp.json()
            if body.get("code") != "000000" or not body.get("data"):
                consec_fails += 1
                continue

            consec_fails = 0
            d = body["data"]
            result[addr.lower()] = {
                "price": float(d.get("price") or 0),
                "holders": int(d.get("holders") or 0),
                "liquidity": float(d.get("liquidity") or 0),
                "marketCap": float(d.get("marketCap") or 0),
                "volume24h": float(d.get("volume24h") or 0),
                "volume1h": float(d.get("volume1h") or 0),
                "percentChange1h": float(d.get("percentChange1h") or 0),
                "percentChange24h": float(d.get("percentChange24h") or 0),
                "top10Pct": float(d.get("top10HoldersPercentage") or 0),
                "devHoldPct": float(d.get("devHoldingPercent") or 0),
                "smartMoneyHolders": int(d.get("smartMoneyHolders") or 0),
                "smartMoneyHoldPct": float(d.get("smartMoneyHoldingPercent") or 0),
                "kolHolders": int(d.get("kolHolders") or 0),
                "kolHoldPct": float(d.get("kolHoldingPercent") or 0),
                "proHolders": int(d.get("proHolders") or 0),
                "proHoldPct": float(d.get("proHoldingPercent") or 0),
            }
        except Exception:
            consec_fails += 1

        if i < len(addresses) - 1:
            time.sleep(0.25)

    if result:
        log.info("币安代币动态: 获取 %d/%d 个代币数据", len(result), len(addresses))

    return result


# ===================================================================
#  GMGN 聪明钱地址发现 + 持久化
# ===================================================================
def _load_smart_money_file() -> dict:
    """加载聪明钱地址文件, 返回 {address: {tags, firstSeen, lastSeen}}"""
    try:
        if SMART_MONEY_FILE.exists():
            with open(SMART_MONEY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_smart_money_file(data: dict):
    """保存聪明钱地址文件"""
    with open(SMART_MONEY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def fetch_gmgn_smart_money() -> set[str]:
    """
    从 GMGN API 获取 BSC 聪明钱地址, 合并到本地文件
    返回当前所有已知聪明钱地址集合
    """
    _ensure_sessions()
    sm_data = _load_smart_money_file()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    new_count = 0

    try:
        ts = int(time.time())
        cid = str(uuid.uuid4())
        url = (f"{GMGN_API}/v1/user/smartmoney"
               f"?chain=bsc&limit=100&timestamp={ts}&client_id={cid}")
        headers = {"X-APIKEY": GMGN_API_KEY, "Content-Type": "application/json"}

        resp = _bn_session.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            log.info("GMGN 聪明钱: 不可用 (status=%d)", resp.status_code)
            return set(sm_data.keys()) - KNOWN_EXCLUDE_ADDRESSES

        data = resp.json()
        items = (data.get("data") or {}).get("list") or []

        for item in items:
            addr = (item.get("maker") or "").lower()
            if not addr or len(addr) != 42 or addr in KNOWN_EXCLUDE_ADDRESSES:
                continue
            tags = (item.get("maker_info") or {}).get("tags") or []
            if addr in sm_data:
                sm_data[addr]["lastSeen"] = now_str
                # 合并新 tags
                existing_tags = set(sm_data[addr].get("tags", []))
                existing_tags.update(tags)
                sm_data[addr]["tags"] = list(existing_tags)
            else:
                sm_data[addr] = {
                    "tags": tags,
                    "firstSeen": now_str,
                    "lastSeen": now_str,
                }
                new_count += 1

        if new_count > 0:
            log.info("GMGN 聪明钱: 新增 %d 个地址 (累计 %d 个)", new_count, len(sm_data))
        else:
            log.info("GMGN 聪明钱: 无新增 (累计 %d 个)", len(sm_data))

        _save_smart_money_file(sm_data)

    except Exception as e:
        log.info("GMGN 聪明钱: 获取失败: %s", e)

    return set(sm_data.keys()) - KNOWN_EXCLUDE_ADDRESSES


def match_smart_money_in_transfers(token_address: str,
                                   smart_addresses: set[str],
                                   rpc_logs: list[dict]) -> dict:
    """
    在已有的 RPC Transfer 日志中匹配聪明钱地址
    返回 {has_buy, has_sell, buy_count, sell_count, details, bonus}
    """
    result = {"has_buy": False, "has_sell": False, "buy_count": 0,
              "sell_count": 0, "details": [], "bonus": 0}
    if not smart_addresses or not rpc_logs:
        return result

    buyers = set()
    sellers = set()

    for log_entry in rpc_logs:
        topics = log_entry.get("topics", [])
        if len(topics) < 3:
            continue
        from_addr = ("0x" + topics[1][26:]).lower()
        to_addr = ("0x" + topics[2][26:]).lower()

        # 聪明钱买入: 聪明钱是接收方, 来源是 DEX Router 或零地址
        if to_addr in smart_addresses:
            if from_addr in KNOWN_DEX_ROUTERS or from_addr == ZERO_ADDRESS:
                buyers.add(to_addr)

        # 聪明钱卖出: 聪明钱是发送方, 目标是 DEX Router
        if from_addr in smart_addresses:
            if to_addr in KNOWN_DEX_ROUTERS:
                sellers.add(from_addr)

    if buyers:
        result["has_buy"] = True
        result["buy_count"] = len(buyers)
        result["details"].append(f"聪明钱加仓 ({len(buyers)}个地址)")
        result["bonus"] = len(buyers)
    if sellers:
        result["has_sell"] = True
        result["sell_count"] = len(sellers)
        result["details"].append(f"聪明钱减仓 ({len(sellers)}个地址)")

    return result


# ===================================================================
#  钱包分析入口 — 批量分析开发者 + 币安信号 + 聪明钱
# ===================================================================
def batch_wallet_analysis(tokens: list[dict],
                          api_key: str,
                          binance_signals: dict[str, dict] | None = None,
                          smart_addresses: set[str] | None = None,
                          rpc_logs_map: dict[str, list] | None = None) -> dict[str, dict]:
    """
    批量分析钱包行为 (并发执行)
    返回 {address: {excluded, excludeReason, signals, bonus, details}}
    binance_signals: 币安聪明钱信号 {token_address: signal_data}
    """
    result_map = {}
    if not api_key:
        return result_map

    bn_signals = binance_signals or {}
    sm_addrs = smart_addresses or set()
    logs_map = rpc_logs_map or {}

    def _analyze_one(t: dict) -> tuple[str, dict]:
        addr = t.get("address", "")
        creator = t.get("creator", "")

        # 分析开发者行为
        dev = analyze_developer_behavior(addr, creator, api_key)

        # 合并结果
        all_details = list(dev["details"])
        total_bonus = dev["bonus"]
        signals = []
        if dev["has_buy"]:
            signals.append("开发者加仓")
        if dev["has_lp_add"]:
            signals.append("开发者加池子")

        # 合并币安聪明钱信号
        bn = bn_signals.get(addr.lower())
        if bn:
            direction = bn.get("direction", "")
            sm_count = bn.get("smartMoneyCount", 0)
            status = bn.get("status", "")
            max_gain = bn.get("maxGain", "0")
            exit_rate = bn.get("exitRate", 0)
            tag_events = bn.get("tagEvents", [])

            # 信号描述
            if direction == "buy":
                detail_str = f"币安聪明钱买入 ({sm_count}个地址"
                if status == "active":
                    detail_str += ", 活跃"
                if max_gain and float(max_gain) > 0:
                    detail_str += f", 最高涨{float(max_gain):.1f}%"
                detail_str += ")"
                all_details.append(detail_str)
                signals.append("币安聪明钱买入")
                total_bonus += min(sm_count, 3)  # 最多加 3 分
            elif direction == "sell":
                detail_str = f"币安聪明钱卖出 ({sm_count}个地址, 退出率{exit_rate}%)"
                all_details.append(detail_str)
                signals.append("币安聪明钱卖出")

            # tokenTag 敏感事件 (鲸鱼买卖等)
            for evt in tag_events:
                if evt not in all_details:
                    all_details.append(evt)
                    if "Add" in evt or "Buy" in evt:
                        total_bonus += 1
                    elif "Reduce" in evt or "Sell" in evt:
                        signals.append(evt)

        # 币安动态数据: 开发者持仓变化 + 聪明钱/KOL/专业投资者持仓
        dev_pct = t.get("devHoldPct", -1)
        prev_dev_pct = t.get("prevDevHoldPct", -1)
        sm_holders_bn = t.get("smartMoneyHolders", 0)
        kol_holders = t.get("kolHolders", 0)
        pro_holders = t.get("proHolders", 0)
        bn_dev_exclude = False
        bn_dev_exclude_reason = ""

        # 开发者持仓变化检测 (通过两轮扫描对比)
        if prev_dev_pct >= 0 and dev_pct >= 0:
            if prev_dev_pct > 0 and dev_pct == 0:
                all_details.append(f"开发者清仓 (持仓{prev_dev_pct:.2f}%→0%)")
                bn_dev_exclude = True
                bn_dev_exclude_reason = f"开发者清仓 (币安: {prev_dev_pct:.2f}%→0%)"
            elif dev_pct < prev_dev_pct * 0.5 and prev_dev_pct > 1:
                all_details.append(f"开发者大幅减仓 (持仓{prev_dev_pct:.2f}%→{dev_pct:.2f}%)")
            elif dev_pct > prev_dev_pct * 1.5 and dev_pct > 0.1:
                all_details.append(f"开发者加仓 (持仓{prev_dev_pct:.2f}%→{dev_pct:.2f}%)")
                if "开发者加仓" not in signals:
                    signals.append("开发者加仓")
                total_bonus += 1

        # 币安标注的聪明钱/KOL/专业投资者持仓
        if sm_holders_bn > 0:
            all_details.append(f"币安聪明钱持仓 ({sm_holders_bn}个地址)")
            if "币安聪明钱买入" not in signals:
                total_bonus += 1
        if kol_holders > 0:
            all_details.append(f"KOL持仓 ({kol_holders}个)")
            total_bonus += 1
        if pro_holders > 0:
            all_details.append(f"专业投资者持仓 ({pro_holders}个)")
            total_bonus += 1

        # GMGN 聪明钱链上匹配 (用淘汰阶段已有的 RPC Transfer 日志)
        if sm_addrs and logs_map:
            rpc_logs = logs_map.get(addr, [])
            sm = match_smart_money_in_transfers(addr, sm_addrs, rpc_logs)
            if sm["has_buy"]:
                all_details.extend(sm["details"])
                signals.append("聪明钱加仓")
                total_bonus += sm["bonus"]
            if sm["has_sell"]:
                all_details.extend(sm["details"])

        excluded = False
        exclude_reason = ""
        if dev["exclude"]:
            excluded = True
            exclude_reason = ", ".join(dev["details"])
        if bn_dev_exclude and not dev["exclude"]:
            # 币安数据检测到开发者清仓, 但链上分析没检测到 → 补充排除
            excluded = True
            if exclude_reason:
                exclude_reason += ", "
            exclude_reason += bn_dev_exclude_reason

        return addr, {
            "excluded": excluded,
            "excludeReason": exclude_reason,
            "signals": signals,
            "bonus": total_bonus,
            "details": all_details,
        }

    # 并发分析 (8 线程, BSCScan 限流 ~5 req/s)
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_analyze_one, t) for t in tokens]
        for f in as_completed(futures):
            try:
                addr, wa = f.result()
                result_map[addr] = wa
            except Exception:
                pass

    return result_map


# ===================================================================
#  DexScreener API (主要价格源, ~300 req/min)
# ===================================================================
def ds_get_pairs(token_address: str) -> list[dict] | None:
    _ensure_sessions()
    url = f"{DS_BASE}/tokens/v1/bsc/{token_address}"
    try:
        r = _gt_session.get(url, timeout=10, headers=DS_HEADERS)
        if r.status_code == 429:
            time.sleep(2)
            r = _gt_session.get(url, timeout=10, headers=DS_HEADERS)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        return data.get("pairs") or data.get("data") or []
    except Exception as e:
        log.debug("ds_get_pairs [%s]: %s", token_address[:20], e)
        return None


def ds_batch_prices(addresses: list[str]) -> dict[str, dict]:
    """DexScreener 批量查价格+流动性+涨跌幅+Boost (最多 10 个地址/请求, 带指数退避重试)"""
    _ensure_sessions()
    result = {}
    batch_size = 10
    max_retries = 3

    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i + batch_size]
        url = f"{DS_BASE}/tokens/v1/bsc/{','.join(batch)}"

        # 指数退避重试
        for attempt in range(max_retries):
            try:
                r = _gt_session.get(url, timeout=20, headers=DS_HEADERS)
                if r.status_code == 429:
                    wait = 3 * (attempt + 1)
                    log.warning("DS 429 限流, 等待 %ds (%d/%d)", wait, attempt + 1, max_retries)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                data = r.json()
                pairs = data if isinstance(data, list) else (data.get("pairs") or data.get("data") or [])
                for p in pairs:
                    if not p.get("baseToken"):
                        continue
                    addr = p["baseToken"]["address"].lower()
                    if addr in result:
                        continue
                    # 提取买卖笔数 (DexScreener txns 字段)
                    txns = p.get("txns") or {}
                    # 汇总多个时间段: 优先用 h1 (最近 1h), 回退 h24
                    txns_h1 = txns.get("h1") or {}
                    txns_h24 = txns.get("h24") or {}
                    buys_h1 = int(txns_h1.get("buys") or 0)
                    sells_h1 = int(txns_h1.get("sells") or 0)
                    buys_h24 = int(txns_h24.get("buys") or 0)
                    sells_h24 = int(txns_h24.get("sells") or 0)
                    # 提取价格变化 (DexScreener priceChange 字段, 百分比值)
                    pc = p.get("priceChange") or {}
                    # 提取 Boost 信息 (项目方付费推广, 正向信号)
                    boosts_info = p.get("boosts") or {}
                    boosts_active = int(boosts_info.get("active") or 0)
                    result[addr] = {
                        "price": float(p.get("priceUsd") or 0),
                        "liquidity": float((p.get("liquidity") or {}).get("usd") or 0),
                        "volume24h": float((p.get("volume") or {}).get("h24") or 0),
                        "volumeH1": float((p.get("volume") or {}).get("h1") or 0),
                        "buysH1": buys_h1,
                        "sellsH1": sells_h1,
                        "buysH24": buys_h24,
                        "sellsH24": sells_h24,
                        "priceChangeM5": float(pc.get("m5") or 0),
                        "priceChangeH1": float(pc.get("h1") or 0),
                        "priceChangeH6": float(pc.get("h6") or 0),
                        "priceChangeH24": float(pc.get("h24") or 0),
                        "boosts": boosts_active,
                        "name": p["baseToken"].get("name", ""),
                        "symbol": p["baseToken"].get("symbol", ""),
                        "pairAddress": (p.get("pairAddress") or "").lower(),
                    }
                break  # 成功则跳出重试循环
            except (ConnectionError, ConnectionResetError) as e:
                wait = 2 * (attempt + 1)
                log.warning("DS 连接被重置, 等待 %ds 重试 (%d/%d): %s", wait, attempt + 1, max_retries, e)
                time.sleep(wait)
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = 2 * (attempt + 1)
                    log.warning("DS 批量查价失败, 等待 %ds 重试 (%d/%d): %s", wait, attempt + 1, max_retries, e)
                    time.sleep(wait)
                else:
                    log.warning("DS 批量查价最终失败: %s", e)

        # 批次间隔加长, 避免触发限流
        if i + batch_size < len(addresses):
            time.sleep(0.5)

    return result


# ===================================================================
#  GeckoTerminal API (备选K线, ~30 req/min)
# ===================================================================
def _gt_request(url: str, max_retries: int = 3) -> dict | None:
    global _gt_rate_delay
    _ensure_sessions()
    for attempt in range(max_retries):
        try:
            r = _gt_session.get(url, timeout=15)
            if r.status_code == 429:
                wait = 5 * (attempt + 1)
                _gt_rate_delay = min(5.0, _gt_rate_delay + 1.0)
                log.warning("GT 429, 等待 %ds (%d/%d)", wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue
            r.raise_for_status()
            _gt_rate_delay = max(0.5, _gt_rate_delay - 0.2)
            return r.json()
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(3)
    return None


def gt_holder_counts(addresses: list[str]) -> dict[str, int]:
    """
    GeckoTerminal token info API 查持币地址数 (免费, 无需 key)
    仅用于已毕业代币 (bonding curve 结束后)
    限流: ~30 req/min, 需控制请求速率
    """
    result = {}
    if not addresses:
        return result
    for addr in addresses:
        url = f"{GT_BASE}/networks/bsc/tokens/{addr}/info"
        data = _gt_request(url)
        if data:
            holders = (data.get("data", {}).get("attributes", {})
                       .get("holders", {}).get("count"))
            if holders and holders > 0:
                result[addr] = holders
        time.sleep(0.4)  # ~2.5 req/s, 留余量避免 429
    log.info("GT 查到 %d/%d 个代币持币数", len(result), len(addresses))
    return result


def bscscan_scrape_holder_count(token_address: str) -> int | None:
    """
    爬取 BSCScan 网页获取持币地址数 (降级备选)
    BSCScan 网页端有持币数显示, 但 API 免费 key 不支持
    """
    _ensure_sessions()
    try:
        url = f"https://bscscan.com/token/{token_address}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html",
        }
        r = _bsc_session.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        # BSCScan 页面 meta 标签: "Holders: 369 | As at ..."
        m = re.search(r'Holders:\s*([\d,]+)', r.text)
        if m:
            return int(m.group(1).replace(",", ""))
    except Exception as e:
        log.debug("BSCScan 网页爬取失败 [%s]: %s", token_address[:16], e)
    return None


def graduated_holder_counts(addresses: list[str]) -> dict[str, int]:
    """
    BSCScan 网页爬取持币数 (并发, ~2s/个)
    用于: 已毕业代币 + detail API 返回 holders==0 的代币
    BSCScan 网页端有持币数, 免费 API 不支持, 直接爬网页
    GeckoTerminal 作为备选 (太慢, 仅在 BSCScan 失败时使用)
    """
    result = {}
    if not addresses:
        return result

    # 并发爬取 BSCScan 网页 (3 线程, 避免被限流)
    def _scrape_one(addr: str) -> tuple[str, int | None]:
        count = bscscan_scrape_holder_count(addr)
        return addr, count

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(_scrape_one, addr) for addr in addresses]
        for f in as_completed(futures):
            addr, count = f.result()
            if count is not None and count > 0:
                result[addr] = count

    log.info("BSCScan 网页查到 %d/%d 个代币持币数", len(result), len(addresses))

    # BSCScan 没查到的, 用 GT 补 (逐个查, 较慢, 仅补漏)
    missing = [a for a in addresses if a not in result]
    if missing and len(missing) <= 5:
        log.info("BSCScan 未覆盖 %d 个, 尝试 GT 补全...", len(missing))
        gt_result = gt_holder_counts(missing)
        result.update(gt_result)
        if gt_result:
            log.info("GT 补全 %d 个", len(gt_result))
    elif missing:
        log.info("BSCScan 未覆盖 %d 个, 跳过 GT (数量过多)", len(missing))

    return result


def gt_ohlcv_direct(token_address: str, limit: int = 72) -> list[list]:
    """直接用 tokenAddress 当 poolAddress 拿 1h K线"""
    url = f"{GT_BASE}/networks/bsc/pools/{token_address}/ohlcv/hour?aggregate=1&limit={limit}"
    data = _gt_request(url)
    if not data:
        return []
    return (data.get("data", {}).get("attributes", {}).get("ohlcv_list", []))


def gt_ohlcv_15min(token_address: str, limit: int = 48) -> list[list]:
    """拿 15 分钟 K线 (用于币龄<1h的底价计算)"""
    url = f"{GT_BASE}/networks/bsc/pools/{token_address}/ohlcv/minute?aggregate=15&limit={limit}"
    data = _gt_request(url)
    if not data:
        return []
    return (data.get("data", {}).get("attributes", {}).get("ohlcv_list", []))


def gt_ohlcv_1min(token_address: str, limit: int = 30) -> list[list]:
    """拿 1 分钟 K线 (用于假阳线死线检测, 需要细粒度数据)"""
    url = f"{GT_BASE}/networks/bsc/pools/{token_address}/ohlcv/minute?aggregate=1&limit={limit}"
    data = _gt_request(url)
    if not data:
        return []
    return (data.get("data", {}).get("attributes", {}).get("ohlcv_list", []))


def gt_batch_peak_prices(tokens: list[dict]) -> dict[str, dict]:
    """
    批量查询 GeckoTerminal 15 分钟 K线, 提取每个代币的最高价/最低价/振幅
    用于:
      1. 补充 peakPrice (扫描间隔内的价格冲高不会被 DexScreener 实时价捕获)
      2. 检测过山车行情 (klineHigh/klineLow 比值过大 = 已被炒过一轮)
      3. 检测单根K线暴跌 (maxCandleDrop > 40% = 过山车币, 暴涨后暴跌风险大)
    OHLCV 格式: [timestamp, open, high, low, close, volume]
      c[1] = open, c[2] = high, c[3] = low, c[4] = close

    K线数量策略:
      - 首次查询 (无 klineFixed 标记): 拉 24 根 (6h), 覆盖历史冲高
      - 后续轮次 (已有 klineFixed 标记): 拉 4 根 (1h), 只补上一轮间隔的遗漏
    限流策略: 3 线程并发 + 令牌桶限流 (~30 req/min), 遇 429 动态降速

    返回: {address: {"high": float, "low": float, "maxCandleDrop": float}}
        maxCandleDrop: 所有K线中单根最大跌幅 (close/open-1, 负值), 如 -0.45 表示某根K线跌45%
    """
    result = {}
    if not tokens:
        return result

    # 令牌桶限流: 控制全局请求速率, 避免触发 GT 30 req/min 限制
    import threading
    _lock = threading.Lock()
    _min_interval = 1.0  # 初始间隔 1.0s (~30 req/min, 踩线但有 429 退避兜底)
    _last_req_time = [0.0]  # 用列表以便闭包内修改
    _interval = [_min_interval]

    def _rate_wait():
        """等待直到可以发送下一个请求"""
        with _lock:
            now = time.time()
            elapsed = now - _last_req_time[0]
            if elapsed < _interval[0]:
                time.sleep(_interval[0] - elapsed)
            _last_req_time[0] = time.time()

    def _on_429():
        """遇到 429 时增大间隔"""
        with _lock:
            _interval[0] = min(5.0, _interval[0] + 0.5)

    def _on_success():
        """成功时缓慢恢复间隔"""
        with _lock:
            _interval[0] = max(_min_interval, _interval[0] - 0.1)

    def _query_one(t: dict) -> tuple[str, dict | None]:
        token_addr = t["address"]
        pool_addr = t.get("gtPoolAddress") or token_addr
        limit = 4 if t.get("klineFixed") else 24
        _rate_wait()
        try:
            candles = gt_ohlcv_15min(pool_addr, limit=limit)
            if candles:
                _on_success()
                high = max(float(c[2]) for c in candles)
                lows = [float(c[3]) for c in candles if float(c[3]) > 0]
                low = min(lows) if lows else 0
                drops = []
                for c in candles:
                    o = float(c[1])
                    cl = float(c[4])
                    if o > 0:
                        drops.append((cl - o) / o)
                max_candle_drop = min(drops) if drops else 0.0
                if high > 0:
                    last_candles = []
                    for c in candles[-3:]:
                        last_candles.append({
                            "o": float(c[1]),
                            "c": float(c[4]),
                            "v": float(c[5]),
                        })
                    return token_addr, {"high": high, "low": low, "maxCandleDrop": max_candle_drop,
                                  "klineCandles": last_candles}
            else:
                _on_success()
        except Exception as e:
            log.debug("GT K线查询失败 [%s]: %s", token_addr[:16], e)
        return token_addr, None

    # 3 线程并发查询, 令牌桶控制全局速率
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(_query_one, t) for t in tokens]
        for f in as_completed(futures):
            addr, data = f.result()
            if data:
                result[addr] = data

    return result


def detect_fake_candles(candles: list[list], candles_1m: list[list] | None = None) -> dict:
    """
    假K线检测: 识别价格被人为控制的代币
    特征 1: K线几乎全是实体柱, 没有上下影线 — 价格被精确控制
    特征 2: 几乎没有阴线 (收盘<开盘), 全是阳线 — 真实市场不可能只涨不跌
    特征 3: 只有头一两根大涨K线, 后面全是极小振幅的死线 — 脉冲拉盘后无人接盘
    这类币看似只涨不跌, 实际随时会一分钟内归零

    OHLCV 格式: [timestamp, open, high, low, close, volume]
    判定逻辑 (满足任一即判定为假K线):
      A. 无影线实体柱占比 ≥ 80% → 价格被精确控制
      B. 阳线占比 ≥ 90% (几乎没有阴线) → 只涨不跌不符合真实市场
      C. 头部脉冲 + 后续死线占比 ≥ 70% (1min K线) → 一根阳线拉盘后全是死线

    参数:
      candles: 15min K线 (用于特征 A/B)
      candles_1m: 1min K线 (用于特征 C, 可选)

    返回: {fake: bool, reason: str, ratio: float, total: int, no_wick: int,
           bullish_ratio: float, bullish: int,
           dead_line_ratio: float, dead_lines: int, spike_ratio: float}
    """
    base_result = {"fake": False, "reason": "", "ratio": 0, "total": 0,
                   "no_wick": 0, "bullish_ratio": 0, "bullish": 0,
                   "dead_line_ratio": 0, "dead_lines": 0, "spike_ratio": 0}

    if not candles or len(candles) < QUALITY_FAKE_CANDLE_MIN_COUNT:
        return base_result

    no_wick_count = 0
    bullish_count = 0  # 阳线数 (收盘 >= 开盘)
    valid_count = 0

    for c in candles:
        o, h, l, cl = float(c[1]), float(c[2]), float(c[3]), float(c[4])
        # 跳过无效K线 (价格为 0 或 high == low 即无交易)
        if h <= 0 or l <= 0 or h == l:
            continue
        valid_count += 1

        # 统计阳线 (收盘 >= 开盘)
        if cl >= o:
            bullish_count += 1

        body = abs(cl - o)
        # 实体为 0 (十字星) 不算无影线
        if body <= 0:
            continue
        upper_wick = h - max(o, cl)
        lower_wick = min(o, cl) - l
        total_wick = upper_wick + lower_wick
        wick_ratio = total_wick / body if body > 0 else 0
        if wick_ratio < QUALITY_FAKE_CANDLE_WICK_THRESHOLD:
            no_wick_count += 1

    if valid_count < QUALITY_FAKE_CANDLE_MIN_COUNT:
        return {**base_result, "total": valid_count, "no_wick": no_wick_count,
                "bullish": bullish_count}

    wick_ratio = no_wick_count / valid_count
    bullish_ratio = bullish_count / valid_count

    # 判定: 满足任一条件即为假K线
    fake = False
    reason = ""
    if wick_ratio >= QUALITY_FAKE_CANDLE_RATIO:
        fake = True
        reason = "无影线实体柱 {}/{} ({:.0f}%)".format(no_wick_count, valid_count, wick_ratio * 100)
    if bullish_ratio >= QUALITY_FAKE_CANDLE_BULLISH_RATIO:
        if fake:
            reason += " + 全阳线 {}/{} ({:.0f}%)".format(bullish_count, valid_count, bullish_ratio * 100)
        else:
            fake = True
            reason = "全阳线 {}/{} ({:.0f}%)".format(bullish_count, valid_count, bullish_ratio * 100)

    # 特征 C: 头部脉冲 + 后续死线检测 (使用 1min K线)
    dead_line_ratio = 0
    dead_lines = 0
    spike_ratio_val = 0
    if candles_1m and len(candles_1m) >= QUALITY_FAKE_CANDLE_DEAD_MIN_COUNT:
        # 按时间排序 (升序, 最早的在前)
        sorted_1m = sorted(candles_1m, key=lambda c: c[0])
        # 计算每根K线的振幅 (high - low) / low
        amplitudes = []
        for c in sorted_1m:
            h_1m, l_1m = float(c[2]), float(c[3])
            if l_1m > 0 and h_1m > 0:
                amp = (h_1m - l_1m) / l_1m
                amplitudes.append(amp)
            else:
                amplitudes.append(0)

        if len(amplitudes) >= QUALITY_FAKE_CANDLE_DEAD_MIN_COUNT:
            # 找头部最大振幅K线 (前 3 根中振幅最大的)
            head_count = min(3, len(amplitudes))
            head_max_amp = max(amplitudes[:head_count])
            head_max_idx = amplitudes[:head_count].index(head_max_amp)

            # 后续K线 = 头部脉冲之后的所有K线
            tail_start = head_max_idx + 1
            tail_amps = amplitudes[tail_start:]

            if len(tail_amps) >= 3 and head_max_amp > 0:
                # 统计死线数量 (振幅 < 0.5%)
                dead_count = sum(1 for a in tail_amps if a < QUALITY_FAKE_CANDLE_DEAD_THRESHOLD)
                dead_line_ratio = dead_count / len(tail_amps)
                dead_lines = dead_count

                # 计算头部脉冲与后续平均振幅的倍数
                tail_avg = sum(tail_amps) / len(tail_amps) if tail_amps else 0
                spike_ratio_val = head_max_amp / tail_avg if tail_avg > 0 else float('inf')

                # 判定: 头部脉冲振幅 ≥ 后续平均 × 10 且 后续死线占比 ≥ 70%
                if (spike_ratio_val >= QUALITY_FAKE_CANDLE_SPIKE_MULTIPLE
                        and dead_line_ratio >= QUALITY_FAKE_CANDLE_DEAD_RATIO):
                    dead_reason = "脉冲死线 头部振幅{:.1f}%是后续{:.1f}倍 + 死线{}/{} ({:.0f}%)".format(
                        head_max_amp * 100, spike_ratio_val,
                        dead_count, len(tail_amps), dead_line_ratio * 100)
                    if fake:
                        reason += " + " + dead_reason
                    else:
                        fake = True
                        reason = dead_reason

    return {
        "fake": fake,
        "reason": reason,
        "ratio": round(wick_ratio, 3),
        "total": valid_count,
        "no_wick": no_wick_count,
        "bullish_ratio": round(bullish_ratio, 3),
        "bullish": bullish_count,
        "dead_line_ratio": round(dead_line_ratio, 3),
        "dead_lines": dead_lines,
        "spike_ratio": round(spike_ratio_val, 1) if spike_ratio_val != float('inf') else 999,
    }


def calc_all_time_high(candles: list[list]) -> float | None:
    if not candles:
        return None
    return max(float(c[2]) for c in candles)


def calc_max_price_first_n_hours(candles: list[list], create_ts_sec: int,
                                 hours: int = 2) -> float | None:
    """计算前 N 小时的最高价"""
    if not candles:
        return None
    cutoff = create_ts_sec + hours * 3600
    max_high, found = 0.0, False
    for c in candles:
        ts = int(c[0])
        if ts > cutoff or ts < create_ts_sec - 3600:
            continue
        high = float(c[2])
        if high > max_high:
            max_high = high
        found = True
    return max_high if found else None


def calc_min_price_exclude_first(candles: list[list], create_ts_sec: int) -> float | None:
    """计算排除第1根K线后的最低价 (从第二根K线开始统计)"""
    if not candles or len(candles) < 2:
        return None
    sorted_c = sorted(candles, key=lambda c: int(c[0]))
    first_ts = int(sorted_c[0][0])
    min_low, found = float("inf"), False
    for c in sorted_c:
        if int(c[0]) == first_ts:
            continue
        low = float(c[3])
        if low > 0 and low < min_low:
            min_low = low
            found = True
    return min_low if found else None


def calc_min_price_all(candles: list[list]) -> float | None:
    """计算全部K线的最低价"""
    if not candles:
        return None
    min_low, found = float("inf"), False
    for c in candles:
        low = float(c[3])
        if low > 0 and low < min_low:
            min_low = low
            found = True
    return min_low if found else None


# ===================================================================
#  Step 2: 入场筛 — four.meme detail API
# ===================================================================
def admission_filter(new_tokens: list[dict], existing_addrs: set[str]) -> tuple[list[dict], list[dict]]:
    """
    入场筛: 对新发现的代币调 detail API (仅 four.meme) 或 DexScreener (flap), 淘汰无社交/总量≠10亿/币龄过大
    同时用 detail API 的 launchTime 修正链上解析可能不准的 createdAt
    返回: (admitted, rejected)
      admitted: [{"token": ..., "detail": ...}]
      rejected: [{"token": ..., "detail": ..., "reason": "..."}]
    """
    admitted = []
    rejected = []
    # 过滤已在队列或已淘汰的
    fresh = [t for t in new_tokens if t["address"] not in existing_addrs]
    if not fresh:
        return admitted, rejected

    # 按来源分组
    fm_tokens = [t for t in fresh if t.get("source") != "flap"]
    flap_tokens = [t for t in fresh if t.get("source") == "flap"]

    log.info("入场筛: %d 个新代币 (four.meme: %d, flap: %d)...",
             len(fresh), len(fm_tokens), len(flap_tokens))

    now_ms = int(time.time() * 1000)
    max_age_ms = MAX_AGE_HOURS * 3600 * 1000

    # --- four.meme 代币: 调 detail API ---
    detail_map = {}
    if fm_tokens:
        def _query_detail(addr: str) -> tuple[str, dict | None]:
            d = fm_detail(addr)
            if d is None:
                time.sleep(0.5)
                d = fm_detail(addr)  # 重试 1 次
            return addr, d

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(_query_detail, t["address"]) for t in fm_tokens]
            for f in as_completed(futures):
                addr, detail = f.result()
                if detail:
                    detail_map[addr] = detail

    # --- flap 代币: 用 DexScreener 批量查价获取基础数据 + 链上读取进度 + ERC-20 totalSupply + 社交媒体 ---
    flap_ds_data = {}
    flap_states = {}
    flap_supplies = {}
    flap_social = {}
    if flap_tokens:
        flap_addrs = [t["address"] for t in flap_tokens]
        flap_ds_data = ds_batch_prices(flap_addrs)
        flap_states = flap_get_token_states(flap_addrs)
        flap_social = flap_batch_details(flap_tokens)
        flap_supplies = erc20_total_supplies(flap_addrs)

    # 逐个判断入场条件
    for t in fresh:
        # 入场条件: 代币黑名单 (已确认的诈骗代币, 直接拒绝)
        if t["address"].lower() in TOKEN_BLACKLIST:
            rejected.append({"token": t, "detail": None,
                             "reason": "诈骗代币黑名单"})
            continue

        # 入场条件: 开发者黑名单 (已确认的诈骗开发者, 其所有代币一律拒绝)
        t_creator = (t.get("creator") or "").lower()
        if t_creator and t_creator in DEPLOYER_BLACKLIST:
            rejected.append({"token": t, "detail": None,
                             "reason": f"诈骗开发者黑名单 ({t_creator[:10]}...)"})
            continue

        # 入场条件: 蹭名币黑名单 (symbol 或 name 精确匹配知名币种, 直接拒绝)
        t_symbol = (t.get("symbol") or "").strip().lower()
        t_name = (t.get("name") or "").strip().lower()
        if t_symbol in FAKE_NAME_BLACKLIST or t_name in FAKE_NAME_BLACKLIST:
            rejected.append({"token": t, "detail": None,
                             "reason": f"蹭名币 ({t.get('symbol', '')})"})
            continue

        is_flap = t.get("source") == "flap"

        if is_flap:
            # flap 代币: 无 Detail API, 用链上事件数据 + DexScreener
            ds = flap_ds_data.get(t["address"], {})

            # 入场条件: 币龄不超过 MAX_AGE_HOURS
            token_age_ms = now_ms - t.get("createdAt", 0)
            if t.get("createdAt", 0) <= 0 or token_age_ms > max_age_ms:
                age_desc = f"币龄{token_age_ms / 3600000:.1f}h" if t.get("createdAt", 0) > 0 else "创建时间未知"
                rejected.append({"token": t, "detail": None, "reason": f"币龄过大 ({age_desc})"})
                continue

            # 入场条件: 总供应量 = 10亿 (通过 ERC-20 totalSupply() 链上读取)
            real_supply = flap_supplies.get(t["address"])
            if real_supply is None:
                rejected.append({"token": t, "detail": None, "reason": "totalSupply 读取失败"})
                continue
            if real_supply != TOTAL_SUPPLY:
                rejected.append({"token": t, "detail": None, "reason": f"总量≠10亿 ({real_supply})"})
                continue

            # flap 代币构造 detail 兼容结构 (用 Portal getTokenV5 链上数据 + DexScreener + flap.sh 社交)
            flap_state = flap_states.get(t["address"], {})
            flap_progress = flap_state.get("progress", 0)
            flap_graduated = flap_state.get("graduated", False)
            # 价格: DexScreener 优先 (已是 USD), 无则用 getTokenV5 链上价格 (根据 quote_token 换算)
            flap_price = ds.get("price", 0)
            if not flap_price and flap_state.get("price_native", 0) > 0:
                flap_price = flap_price_to_usd(
                    flap_state["price_native"],
                    flap_state.get("quote_token", ZERO_ADDRESS),
                )
            # 社交媒体: 从 flap.sh 页面 SSR 数据提取
            flap_meta = flap_social.get(t["address"])
            if flap_meta is not None:
                social_links = {}
                if flap_meta.get("twitter"):
                    social_links["twitter"] = flap_meta["twitter"]
                if flap_meta.get("telegram"):
                    social_links["telegram"] = flap_meta["telegram"]
                if flap_meta.get("website"):
                    social_links["website"] = flap_meta["website"]
                # 入场条件: 社交 ≥ 1 (与 four.meme 统一)
                if len(social_links) < MIN_SOCIAL_COUNT:
                    rejected.append({"token": t, "detail": None, "reason": "无社交媒体"})
                    continue
                social_count = len(social_links)
                social_desc = flap_meta.get("description", "")
            else:
                # flap.sh 页面抓取失败, pending 模式: 标记待补查, 给 2 轮缓冲时间
                # socialCount=0 会触发淘汰检查中的补查逻辑, 超过缓冲轮数仍无社交则淘汰
                log.warning("flap 社交抓取失败, pending 放行: %s", t["address"][:16])
                social_links = {}
                social_count = 0  # 标记为待补查, 不再假设有社交
                social_desc = ""
            flap_detail = {
                "holders": 0,
                "price": flap_price,
                "totalSupply": real_supply,
                "socialCount": social_count,
                "socialLinks": social_links,
                "descr": social_desc,
                "name": ds.get("name") or t.get("name", ""),
                "shortName": ds.get("symbol") or t.get("symbol", ""),
                "progress": 1.0 if flap_graduated else flap_progress,
                "day1Vol": ds.get("volume24h", 0),
                "liquidity": ds.get("liquidity", 0),
                "raisedAmount": flap_state.get("reserve", 0),
                "launchTime": 0,
                "_socialPendingRounds": 0 if social_count == 0 else -1,  # 0=pending待补查, -1=已有社交
            }
            admitted.append({"token": t, "detail": flap_detail})

        else:
            # four.meme 代币: 原有逻辑
            detail = detail_map.get(t["address"])
            if not detail:
                rejected.append({"token": t, "detail": None, "reason": "detail API 无数据"})
                continue

            # 用 detail API 的 launchTime 修正 createdAt (链上解析可能不准)
            if detail.get("launchTime") and detail["launchTime"] > 0:
                t["createdAt"] = detail["launchTime"]

            # 入场条件: 币龄不超过 MAX_AGE_HOURS
            token_age_ms = now_ms - t.get("createdAt", 0)
            if t.get("createdAt", 0) <= 0 or token_age_ms > max_age_ms:
                age_desc = f"币龄{token_age_ms / 3600000:.1f}h" if t.get("createdAt", 0) > 0 else "创建时间未知"
                rejected.append({"token": t, "detail": detail, "reason": f"币龄过大 ({age_desc})"})
                continue

            # 入场条件: 社交 ≥ 1, 总供应量 = 10亿
            reasons = []
            if detail["socialCount"] < MIN_SOCIAL_COUNT:
                reasons.append("无社交媒体")
            if detail["totalSupply"] != TOTAL_SUPPLY:
                reasons.append(f"总量≠10亿 ({detail['totalSupply']})")
            if reasons:
                rejected.append({"token": t, "detail": detail, "reason": ", ".join(reasons)})
                continue
            admitted.append({"token": t, "detail": detail})

    fm_admitted = sum(1 for a in admitted if a["token"].get("source") != "flap")
    flap_admitted = sum(1 for a in admitted if a["token"].get("source") == "flap")
    log.info("入场筛: 通过 %d/%d (four.meme: %d, flap: %d, 淘汰 %d)",
             len(admitted), len(fresh), fm_admitted, flap_admitted, len(rejected))
    return admitted, rejected


# ===================================================================
#  Step 3: 淘汰检查 — DexScreener + four.meme detail (仅 four.meme 代币) + BSCScan
# ===================================================================
def elimination_check(queue: list[dict], now_ms: int,
                      api_key: str) -> tuple[list[dict], list[dict], list[dict]]:
    """
    淘汰检查: 对队列中代币定期检查, 永久淘汰弃盘币
    持币数: BSCScan 网页爬取 (已毕业 + detail返回0的) > detail API (未毕业) > 缓存
    返回: (survivors, eliminated)
    """
    survivors = []
    eliminated = []

    if not queue:
        return survivors, eliminated

    # 1. 币龄淘汰 (无需 API)
    max_age_ms = MAX_AGE_HOURS * 3600 * 1000
    age_filtered = []
    over_age_count = 0
    for t in queue:
        age_ms = now_ms - t.get("createdAt", 0)
        if age_ms > max_age_ms:
            eliminated.append({**t, "eliminatedAt": now_ms,
                               "elimReason": f"币龄>{MAX_AGE_HOURS}h"})
            over_age_count += 1
        else:
            age_filtered.append(t)

    if over_age_count:
        log.info("淘汰: 币龄>%dh %d 个", MAX_AGE_HOURS, over_age_count)

    if not age_filtered:
        return survivors, eliminated

    # 1b. 本地预淘汰: 用已有数据快速剔除明显垃圾币 (零 API 调用)
    # 注意: peakHolders==0 但进度>0 说明有真实买盘但 API 持币数据不准, 跳过预淘汰
    pre_filtered = []
    for t in age_filtered:
        age_hours = (now_ms - t.get("createdAt", 0)) / 3600000
        elim_reason = None
        peak_h = t.get("peakHolders", 0)

        # 蹭名币: symbol 或 name 命中黑名单, 直接淘汰 (清理历史遗留)
        t_symbol = (t.get("symbol") or "").strip().lower()
        t_name = (t.get("name") or "").strip().lower()
        if t_symbol in FAKE_NAME_BLACKLIST or t_name in FAKE_NAME_BLACKLIST:
            elim_reason = f"蹭名币 ({t.get('symbol', '')})"
        # 进度>0 说明有资金进入, 但 holders==0 是 API 数据不准, 跳过让后续 API 更新
        elif peak_h == 0 and t.get("peakProgress", 0) > 0:
            pass
        elif age_hours > ELIM_EARLY_AGE_MIN and peak_h < ELIM_EARLY_PEAK_HOLDERS:
            elim_reason = f"币龄{_fmt_age(age_hours)} 最高持币仅{peak_h}"
        elif age_hours > ELIM_MID_AGE_HOURS and peak_h < ELIM_MID_PEAK_HOLDERS:
            elim_reason = f"币龄{_fmt_age(age_hours)} 最高持币仅{peak_h}"
        elif age_hours > ELIM_LATE_AGE_HOURS and peak_h < ELIM_LATE_PEAK_HOLDERS:
            elim_reason = f"币龄{_fmt_age(age_hours)} 最高持币仅{peak_h}"

        if elim_reason:
            eliminated.append({**t, "eliminatedAt": now_ms, "elimReason": elim_reason})
        else:
            pre_filtered.append(t)

    pre_elim_count = len(age_filtered) - len(pre_filtered)
    if pre_elim_count > 0:
        log.info("淘汰: 本地预淘汰 %d 个 (持币数不足)", pre_elim_count)

    if not pre_filtered:
        return survivors, eliminated

    # 2. DexScreener + four.meme detail + 已毕业代币持币数 并行查询
    addrs = [t["address"] for t in pre_filtered]

    # 新入队代币 (本轮刚加入) 不需要再查 detail, 入场筛已经查过
    # flap 代币不查 fm_detail (无此 API)
    need_detail = [t for t in pre_filtered
                   if now_ms - t.get("addedAt", 0) > 60000
                   and t.get("source") != "flap"]

    # 需要 BSCScan 爬取持币数的代币:
    # 1. 已毕业代币 (progress >= 1): detail API 毕业后返回 0, 必须用链上数据
    # 2. 缓存 holders==0 的代币: detail API 数据不准, 用 BSCScan 兜底拿真实持币数
    bscscan_addrs = list({t["address"] for t in pre_filtered
                          if t.get("progress", 0) >= 1
                          or t.get("holders", 0) == 0})

    # flap 代币: 通过链上合约读取 bonding curve 进度
    flap_addrs = [t["address"] for t in pre_filtered if t.get("source") == "flap"]

    # flap 代币社交数据补查: 入场时可能未获取社交信息, 需要补查
    flap_need_social = [t for t in pre_filtered
                        if t.get("source") == "flap" and t.get("socialCount", 0) == 0]

    log.info("淘汰检查: 并行查询 %d 个代币 (DS + detail(%d个) + BSCScan持币数(%d个) + flap进度(%d个) + flap社交(%d个))...",
             len(pre_filtered), len(need_detail), len(bscscan_addrs), len(flap_addrs), len(flap_need_social))

    def _fetch_all_details():
        """并发查询 fm_detail, 5 线程, 失败重试 1 次"""
        detail_map = {}
        def _query_one(addr: str) -> tuple[str, dict | None]:
            detail = fm_detail(addr)
            if detail is None:
                time.sleep(0.5)
                detail = fm_detail(addr)  # 重试 1 次
            return addr, detail
        with ThreadPoolExecutor(max_workers=5) as inner_pool:
            futures = [inner_pool.submit(_query_one, t["address"]) for t in need_detail]
            for f in as_completed(futures):
                addr, detail = f.result()
                if detail:
                    detail_map[addr] = detail
        return detail_map

    with ThreadPoolExecutor(max_workers=5) as pool:
        ds_future = pool.submit(ds_batch_prices, addrs)
        detail_future = pool.submit(_fetch_all_details)
        grad_future = pool.submit(graduated_holder_counts, bscscan_addrs)
        flap_future = pool.submit(flap_get_token_states, flap_addrs)
        flap_social_future = pool.submit(flap_batch_details, flap_need_social)
        ds_data = ds_future.result()
        detail_map = detail_future.result()
        grad_holders = grad_future.result()
        flap_states = flap_future.result()
        flap_social_map = flap_social_future.result()

    # 3. 逐个检查淘汰条件
    for t in pre_filtered:
        ds = ds_data.get(t["address"], {})
        detail = detail_map.get(t["address"])
        age_hours = (now_ms - t.get("createdAt", 0)) / 3600000

        # 更新动态数据 (DexScreener > detail/flap链上 > 队列缓存)
        # 注意: flap 代币不查 detail, 价格优先级为 DexScreener > flap链上换算 > 缓存
        is_flap = t.get("source") == "flap"
        ds_price = ds.get("price") or 0
        if is_flap:
            flap_state = flap_states.get(t["address"], {})
            flap_chain_price = 0
            if flap_state.get("price_native", 0) > 0:
                flap_chain_price = flap_price_to_usd(
                    flap_state["price_native"],
                    flap_state.get("quote_token", ZERO_ADDRESS),
                )
            current_price = ds_price or flap_chain_price or t.get("price", 0)
        else:
            current_price = (ds_price
                             or (detail["price"] if detail else 0)
                             or t.get("price", 0))
        # 持币数优先级:
        # 已毕业: BSCScan (链上索引) > 缓存 (detail 毕业后返回 0, 不用)
        # 未毕业: detail API > BSCScan 兜底 (detail 返回 0 时) > 缓存
        # 注意: 用 None 区分"没查到"和"查到了但值为 0", 避免错误回退缓存
        _grad_h = grad_holders.get(t["address"])  # None = 没查到
        _det_h = detail["holders"] if detail else None  # None = detail 没查到
        _cache_h = t.get("holders", 0)
        is_graduated = t.get("progress", 0) >= 1
        if is_graduated:
            current_holders = _grad_h if _grad_h is not None else _cache_h
        else:
            # 未毕业: detail 有值且 >0 用 detail, 否则用 BSCScan 兜底, 最后回退缓存
            if _det_h is not None and _det_h > 0:
                current_holders = _det_h
            elif _grad_h is not None:
                current_holders = _grad_h
            else:
                current_holders = _cache_h
        # 调试: 记录持币数来源 (仅对持币数有变化的代币)
        if current_holders != _cache_h:
            if is_graduated:
                _src = "BSCScan" if _grad_h is not None else "缓存"
            elif _det_h is not None and _det_h > 0:
                _src = "detail"
            elif _grad_h is not None:
                _src = "BSCScan兜底"
            else:
                _src = "缓存"
            log.info("  持币数更新 %s: %d→%d (来源:%s)",
                     t.get("name", t["address"][:16]),
                     _cache_h, current_holders, _src)
        current_liq = (ds.get("liquidity")
                       or t.get("liquidity", 0))
        # 进度: four.meme 用 detail API, flap 用 Portal getTokenV5
        if is_flap:
            flap_graduated = flap_state.get("graduated", False)
            current_progress = 1.0 if flap_graduated else flap_state.get("progress", t.get("progress", 0))
            # 更新 raisedAmount
            if flap_state.get("reserve", 0) > 0:
                t["raisedAmount"] = flap_state["reserve"]
        else:
            current_progress = (detail["progress"] if detail else 0) or t.get("progress", 0)

        t["price"] = current_price
        t["holders"] = current_holders
        # 记录持币数历史 (只保留最近 5 轮)
        hist = t.get("holdersHistory", [])
        hist.append(current_holders)
        t["holdersHistory"] = hist[-5:]
        t["liquidity"] = current_liq
        # 记录流动性历史 (只保留最近 5 轮, 精筛用)
        liq_hist = t.get("liquidityHistory", [])
        liq_hist.append(current_liq)
        t["liquidityHistory"] = liq_hist[-5:]
        # 记录成交额历史 (volume24h, 只保留最近 5 轮, 用于成交额异动检测)
        current_volume = t.get("volume24h", 0)
        vol_hist = t.get("volumeHistory", [])
        vol_hist.append(current_volume)
        t["volumeHistory"] = vol_hist[-5:]
        t["progress"] = current_progress
        if detail:
            t["socialCount"] = detail["socialCount"]
            t["socialLinks"] = detail["socialLinks"]
            t["day1Vol"] = detail.get("day1Vol") or t.get("day1Vol", 0)
            t["raisedAmount"] = detail.get("raisedAmount") or t.get("raisedAmount", 0)
            # 用 detail API 的 launchTime 修正 createdAt
            if detail.get("launchTime") and detail["launchTime"] > 0:
                t["createdAt"] = detail["launchTime"]
                age_hours = (now_ms - t["createdAt"]) / 3600000
        # flap 代币社交数据补查: 用 flap_batch_details 结果更新
        if is_flap and t.get("socialCount", 0) == 0:
            flap_meta = flap_social_map.get(t["address"])
            if flap_meta:
                social_links = {}
                if flap_meta.get("twitter"):
                    social_links["twitter"] = flap_meta["twitter"]
                if flap_meta.get("telegram"):
                    social_links["telegram"] = flap_meta["telegram"]
                if flap_meta.get("website"):
                    social_links["website"] = flap_meta["website"]
                if social_links:
                    t["socialCount"] = len(social_links)
                    t["socialLinks"] = social_links
                    t["_socialPendingRounds"] = -1  # 补查成功, 清除 pending 状态
                    log.info("  flap社交补查 %s: %d 个链接",
                             t.get("name") or t["address"][:16], len(social_links))
                else:
                    # 补查成功但确认无社交, 递增 pending 轮数
                    pending_rounds = t.get("_socialPendingRounds", 0)
                    t["_socialPendingRounds"] = pending_rounds + 1
            else:
                # 补查失败 (网络问题), 递增 pending 轮数
                pending_rounds = t.get("_socialPendingRounds", 0)
                t["_socialPendingRounds"] = pending_rounds + 1
        if ds:
            t["name"] = ds.get("name") or t.get("name", "")
            t["symbol"] = ds.get("symbol") or t.get("symbol", "")
            # 记录 GT 池子地址 (用于 GT K线查询, 部分代币 tokenAddress ≠ poolAddress)
            ds_pool = ds.get("pairAddress", "")
            if ds_pool:
                t["gtPoolAddress"] = ds_pool
            # 更新交易量和买卖笔数 (DexScreener)
            t["volume24h"] = ds.get("volume24h", 0)
            t["volumeH1"] = ds.get("volumeH1", 0)
            t["buysH1"] = ds.get("buysH1", 0)
            t["sellsH1"] = ds.get("sellsH1", 0)
            t["buysH24"] = ds.get("buysH24", 0)
            t["sellsH24"] = ds.get("sellsH24", 0)
            # 更新价格变化 (DexScreener priceChange, 百分比值, 零额外 API 调用)
            t["priceChangeM5"] = ds.get("priceChangeM5", 0)
            t["priceChangeH1"] = ds.get("priceChangeH1", 0)
            t["priceChangeH6"] = ds.get("priceChangeH6", 0)
            t["priceChangeH24"] = ds.get("priceChangeH24", 0)
            # 更新 Boost 信息 (项目方付费推广, 正向信号, 零额外 API 调用)
            t["boosts"] = ds.get("boosts", 0)

        # 更新峰值 (仅用 DexScreener 实时价)
        t["peakPrice"] = max(t.get("peakPrice", 0), current_price)
        t["peakHolders"] = max(t.get("peakHolders", 0), current_holders)
        t["peakLiquidity"] = max(t.get("peakLiquidity", 0), current_liq)
        t["peakProgress"] = max(t.get("peakProgress", 0), current_progress)

        # 记录价格历史 (只保留最近 5 轮, 与 holdersHistory 对齐)
        price_hist = t.get("priceHistory", [])
        price_hist.append(current_price)
        t["priceHistory"] = price_hist[-5:]

        # 记录进度历史 (只保留最近 5 轮, 与 priceHistory 对齐)
        prog_hist = t.get("progressHistory", [])
        prog_hist.append(current_progress)
        t["progressHistory"] = prog_hist[-5:]

        # 连续下跌计数
        last_price = t.get("lastPrice", 0)
        if last_price > 0 and current_price < last_price:
            t["consecDrops"] = t.get("consecDrops", 0) + 1
        else:
            t["consecDrops"] = 0
        t["prevPrice"] = t.get("lastPrice", 0)
        t["lastPrice"] = current_price

        # --- 淘汰条件 ---
        elim_reason = None

        # 1. 价格从峰值跌 90%+ (增加异常值保护: 价格<1e-7 视为 API 垃圾数据, 跳过)
        peak = t.get("peakPrice", 0)
        if (peak > 0 and current_price > 0
                and current_price >= ELIM_PRICE_DROP_MIN_PRICE
                and current_price < peak * (1 - ELIM_PRICE_DROP_PCT)):
            elim_reason = (f"价格跌{(1 - current_price / peak) * 100:.0f}% "
                           f"(峰:{peak:.2e} 现:{current_price:.2e})")

        # 1b. 单根K线暴跌 (过山车币): 任何一根15min K线 close/open 跌超阈值
        if not elim_reason:
            kmd = t.get("klineMaxDrop")
            if kmd is not None and kmd <= -ELIM_KLINESINGLE_DROP_PCT:
                elim_reason = f"单根K线跌{abs(kmd)*100:.0f}% (过山车币, >{ELIM_KLINESINGLE_DROP_PCT*100:.0f}%阈值)"

        # 2. 持币数从 30+ 跌破 10
        if not elim_reason:
            if (t.get("peakHolders", 0) >= ELIM_HOLDERS_PEAK_MIN
                    and current_holders < ELIM_HOLDERS_FLOOR):
                elim_reason = f"持币数 {t.get('peakHolders', 0)}→{current_holders}"

        # 3. 无社交媒体 (four.meme + flap 统一淘汰)
        if not elim_reason and detail and detail["socialCount"] < MIN_SOCIAL_COUNT:
            elim_reason = "无社交媒体"

        # 3b. flap 社交 pending 超时: 入场时社交抓取失败, 补查超过 2 轮仍无社交则淘汰
        if not elim_reason and is_flap:
            pending_rounds = t.get("_socialPendingRounds", -1)
            if pending_rounds >= FLAP_SOCIAL_PENDING_MAX_ROUNDS and t.get("socialCount", 0) < MIN_SOCIAL_COUNT:
                elim_reason = f"flap社交补查超时 ({pending_rounds}轮未获取到社交)"

        # 4. 流动性跌破 $1k (仅已毕业)
        if not elim_reason and is_graduated:
            if current_liq < ELIM_LIQ_FLOOR:
                elim_reason = f"流动性跌破 $1k (${current_liq:.0f})"

        # 5. 进度阶梯淘汰 (币龄越长, 容忍度越高)
        if not elim_reason:
            for age_h, min_prog in ELIM_PROGRESS_TIERS:
                if age_hours > age_h and current_progress < min_prog:
                    elim_reason = f"进度{current_progress * 100:.1f}% 币龄{_fmt_age(age_hours)} (阈值{min_prog*100:.0f}%)"
                    break

        # 6. 币龄>15min 最高持币数 < 3
        if not elim_reason:
            if age_hours > ELIM_EARLY_AGE_MIN and t.get("peakHolders", 0) < ELIM_EARLY_PEAK_HOLDERS:
                elim_reason = f"币龄{_fmt_age(age_hours)} 最高持币仅{t.get('peakHolders', 0)}"

        # 7. 币龄>1h 最高持币数 < 5
        if not elim_reason:
            if age_hours > ELIM_MID_AGE_HOURS and t.get("peakHolders", 0) < ELIM_MID_PEAK_HOLDERS:
                elim_reason = f"币龄{_fmt_age(age_hours)} 最高持币仅{t.get('peakHolders', 0)}"

        # 7b. 币龄>2h 最高持币数 < 8 (清理僵尸币, 数据验证: 峰值持币8~9偶有后续增长, 留缓冲)
        if not elim_reason:
            if age_hours > ELIM_LATE_AGE_HOURS and t.get("peakHolders", 0) < ELIM_LATE_PEAK_HOLDERS:
                elim_reason = f"币龄{_fmt_age(age_hours)} 最高持币仅{t.get('peakHolders', 0)}"

        # 8. 修正后币龄 > 48h
        if not elim_reason:
            if age_hours > MAX_AGE_HOURS:
                elim_reason = f"币龄>{MAX_AGE_HOURS}h (修正后{_fmt_age(age_hours)})"

        # 9. 持币数从峰值跌 70% (清理僵尸币, 避免占位)
        if not elim_reason:
            peak_h = t.get("peakHolders", 0)
            if (peak_h >= ELIM_HOLDERS_DROP_PEAK_MIN
                    and current_holders < peak_h * (1 - ELIM_HOLDERS_DROP_PCT)):
                elim_reason = (f"持币数跌{(1 - current_holders / peak_h) * 100:.0f}% "
                               f"({peak_h}→{current_holders})")

        # 10. 进度从峰值跌 40 个百分点 (加减法, 无币龄要求)
        if not elim_reason:
            peak_prog = t.get("peakProgress", 0)
            prog_diff = peak_prog - current_progress
            if prog_diff >= ELIM_PROGRESS_DROP_ABS:
                elim_reason = (f"进度跌{prog_diff * 100:.0f}个百分点 "
                               f"({peak_prog * 100:.1f}%→{current_progress * 100:.1f}%)")

        if elim_reason:
            eliminated.append({**t, "eliminatedAt": now_ms, "elimReason": elim_reason})
        else:
            survivors.append(t)

    elim_count = len(eliminated) - (len(queue) - len(pre_filtered))
    if elim_count > 0:
        log.info("淘汰: 条件淘汰 %d 个", elim_count)
        for e in eliminated[-elim_count:]:
            log.info("  ✗ %s — %s", e.get("name") or e["address"][:16], e["elimReason"])
    return survivors, eliminated


# ===================================================================
#  Step 5: 精筛 — 潜伏型筛选
# ===================================================================


# ===================================================================
#  Gas 大盘指数 — 多链 Gas 消耗反映整体市场活跃度
# ===================================================================
def _fetch_eth_gas_used_ratio() -> float | None:
    """
    通过 Ethereum RPC eth_feeHistory 获取最近 300 个区块的 gasUsedRatio。
    ETH ~12s/块, 300 块 ≈ 1 小时。eth_feeHistory 上限 1024 块, 一次搞定。
    返回平均值, 失败返回 None。
    """
    _ensure_sessions()
    try:
        r = _bsc_session.post(ETH_RPC, json={
            "jsonrpc": "2.0", "method": "eth_feeHistory",
            "params": ["0x12c", "latest", []],  # 0x12c = 300 块 ≈ 1h
            "id": 1,
        }, timeout=15, headers={"Content-Type": "application/json"})
        r.raise_for_status()
        data = r.json()
        ratios = data.get("result", {}).get("gasUsedRatio", [])
        if ratios:
            return sum(ratios) / len(ratios)
    except Exception as e:
        log.debug("ETH Gas 查询失败: %s", e)
    return None


def _fetch_bsc_gas_used_ratio() -> float | None:
    """
    通过 BSC RPC eth_feeHistory 获取最近 1200 个区块的 gasUsedRatio。
    BSC ~3s/块, 1200 块 ≈ 1 小时。eth_feeHistory 上限 1024 块, 需分两次。
    返回平均值, 失败返回 None。
    """
    _ensure_sessions()
    all_ratios = []
    try:
        # 第一次: 最近 1024 块
        r = _bsc_session.post(BSC_RPC, json={
            "jsonrpc": "2.0", "method": "eth_feeHistory",
            "params": ["0x400", "latest", []],  # 0x400 = 1024 块
            "id": 1,
        }, timeout=15, headers={"Content-Type": "application/json"})
        r.raise_for_status()
        data = r.json()
        result = data.get("result", {})
        ratios = result.get("gasUsedRatio", [])
        all_ratios.extend(ratios)

        # 第二次: 再往前 176 块 (1200 - 1024), 从第一次返回的最老区块开始
        oldest = result.get("oldestBlock")
        if oldest and len(ratios) >= 1024:
            oldest_num = int(oldest, 16) if isinstance(oldest, str) else oldest
            r2 = _bsc_session.post(BSC_RPC, json={
                "jsonrpc": "2.0", "method": "eth_feeHistory",
                "params": ["0xb0", hex(oldest_num - 1), []],  # 0xb0 = 176 块
                "id": 2,
            }, timeout=15, headers={"Content-Type": "application/json"})
            r2.raise_for_status()
            data2 = r2.json()
            ratios2 = data2.get("result", {}).get("gasUsedRatio", [])
            all_ratios = ratios2 + all_ratios  # 旧的在前, 新的在后

        if all_ratios:
            return sum(all_ratios) / len(all_ratios)
    except Exception as e:
        log.debug("BSC Gas 查询失败: %s", e)
        # 如果第二次失败但第一次有数据, 用第一次的
        if all_ratios:
            return sum(all_ratios) / len(all_ratios)
    return None


def _fetch_sol_tps() -> float | None:
    """
    通过 Solana RPC getRecentPerformanceSamples 获取最近 60 个采样的 TPS。
    每个采样 60 秒, 60 个 ≈ 1 小时。
    返回平均 TPS, 失败返回 None。
    """
    _ensure_sessions()
    try:
        r = _bsc_session.post(SOL_RPC, json={
            "jsonrpc": "2.0", "method": "getRecentPerformanceSamples",
            "params": [60],  # 最近 60 个采样 ≈ 1 小时
            "id": 1,
        }, timeout=15, headers={"Content-Type": "application/json"})
        r.raise_for_status()
        data = r.json()
        samples = data.get("result", [])
        if samples:
            total_tx = sum(s.get("numTransactions", 0) for s in samples)
            total_sec = sum(s.get("samplePeriodSecs", 60) for s in samples)
            if total_sec > 0:
                return total_tx / total_sec
    except Exception as e:
        log.debug("SOL TPS 查询失败: %s", e)
    return None


def fetch_gas_index() -> dict:
    """
    并行查询 ETH/BSC Gas 使用率 + SOL TPS, 计算加权 Gas 大盘指数。

    指标:
      - ETH gasUsedRatio (0~1): 以太坊区块 Gas 填充率
      - BSC gasUsedRatio (0~1): BSC 区块 Gas 填充率
      - SOL TPS 归一化 (0~1): Solana 交易吞吐量 / 基准 TPS

    加权公式: gas_index = ETH*0.4 + BSC*0.4 + SOL*0.2
    某条链查询失败时, 权重重新分配给成功的链。

    返回: {
        "eth_gas_ratio": float|None,
        "bsc_gas_ratio": float|None,
        "sol_tps": float|None,
        "sol_tps_normalized": float|None,
        "gas_index": float (0~1, 加权综合指数),
        "chains_ok": int (成功查询的链数),
    }
    """
    results = {"eth_gas_ratio": None, "bsc_gas_ratio": None,
               "sol_tps": None, "sol_tps_normalized": None,
               "gas_index": 0.0, "chains_ok": 0}

    # 并行查询三条链
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_eth = pool.submit(_fetch_eth_gas_used_ratio)
        f_bsc = pool.submit(_fetch_bsc_gas_used_ratio)
        f_sol = pool.submit(_fetch_sol_tps)

        eth_ratio = f_eth.result()
        bsc_ratio = f_bsc.result()
        sol_tps = f_sol.result()

    results["eth_gas_ratio"] = round(eth_ratio, 4) if eth_ratio is not None else None
    results["bsc_gas_ratio"] = round(bsc_ratio, 4) if bsc_ratio is not None else None
    results["sol_tps"] = round(sol_tps, 1) if sol_tps is not None else None

    # SOL TPS 归一化: TPS / 基准, 上限 1.0
    sol_normalized = None
    if sol_tps is not None:
        sol_normalized = min(sol_tps / SOL_TPS_NORMALIZE_BASE, 1.0)
        results["sol_tps_normalized"] = round(sol_normalized, 4)

    # 加权计算 Gas 指数 (某链失败时权重重新分配)
    weights = []
    values = []
    if eth_ratio is not None:
        weights.append(GAS_INDEX_ETH_WEIGHT)
        values.append(eth_ratio)
    if bsc_ratio is not None:
        weights.append(GAS_INDEX_BSC_WEIGHT)
        values.append(bsc_ratio)
    if sol_normalized is not None:
        weights.append(GAS_INDEX_SOL_WEIGHT)
        values.append(sol_normalized)

    results["chains_ok"] = len(weights)

    if weights:
        total_weight = sum(weights)
        gas_index = sum(w * v for w, v in zip(weights, values)) / total_weight
        results["gas_index"] = round(gas_index, 4)

    # 日志
    parts = []
    if eth_ratio is not None:
        parts.append(f"ETH={eth_ratio:.1%}")
    else:
        parts.append("ETH=N/A")
    if bsc_ratio is not None:
        parts.append(f"BSC={bsc_ratio:.1%}")
    else:
        parts.append("BSC=N/A")
    if sol_tps is not None:
        parts.append(f"SOL={sol_tps:.0f}tps({sol_normalized:.1%})")
    else:
        parts.append("SOL=N/A")
    log.info("Gas 大盘指数: %.1f%% (%s, %d/3链成功)",
             results["gas_index"] * 100, ", ".join(parts), results["chains_ok"])

    return results


def calc_market_sentiment(queue: list[dict], queue_state: dict,
                          scan_interval_min: int = 15) -> dict:
    """
    计算大盘情绪指标, Gas指数仍计算并记录用于数据分析, 不再作为精筛阻断条件

    判定逻辑: 纯 Gas 趋势
      - 查询 ETH/BSC/SOL 三链近 1 小时 Gas 指数 (加权综合值)
      - 对比 12h 前那一轮记录的 Gas 指数 (单点快照)
      - 当前 Gas 指数 > 12h 前 → 大盘向上 (链上活跃度在升温)
      - 当前 Gas 指数 ≤ 12h 前 → 大盘向下 (链上活跃度在降温)
      - 找不到 12h 前的记录时默认大盘向上, 不阻挡精筛
    """
    now_ms = int(time.time() * 1000)

    # 查询多链 Gas 指数 (近 1 小时平均)
    gas_data = fetch_gas_index()
    gas_index = gas_data.get("gas_index", 0.0)

    # 更新历史记录
    history = queue_state.get("marketSentimentHistory", [])
    history.append({
        "gas_index": round(gas_index, 4),
        "ts": now_ms,
    })
    # 只保留最近 200 轮 (~50h)
    if len(history) > 200:
        history = history[-200:]
    queue_state["marketSentimentHistory"] = history

    # 找 12h 前最近的那一轮记录
    hours_12_ms = 12 * 3600 * 1000
    target_ts = now_ms - hours_12_ms
    gas_12h_ago = None
    best_diff = float("inf")
    for h in history:
        if "gas_index" not in h:
            continue
        diff = abs(h["ts"] - target_ts)
        if diff < best_diff:
            best_diff = diff
            gas_12h_ago = h["gas_index"]

    # 判定: 当前 Gas > 12h 前 → 向上
    if gas_12h_ago is not None and best_diff < 2 * 3600 * 1000:
        # 找到的记录在 12h±2h 范围内才算有效
        is_bullish = gas_index > gas_12h_ago
    else:
        # 没有 12h 前的记录, 默认向上 (不阻挡精筛)
        gas_12h_ago = None
        is_bullish = True

    gas_trend = (gas_index - gas_12h_ago) if gas_12h_ago is not None else 0.0

    result = {
        "gas_index": round(gas_index, 4),
        "gas_detail": gas_data,
        "gas_12h_ago": round(gas_12h_ago, 4) if gas_12h_ago is not None else None,
        "gas_trend": round(gas_trend, 4),
        "is_bullish": is_bullish,
        "history_count": len(history),
    }

    direction = "↑ 向上" if is_bullish else "↓ 向下"
    if gas_12h_ago is not None:
        trend_str = f"+{gas_trend:.1%}" if gas_trend >= 0 else f"{gas_trend:.1%}"
        log.info("大盘情绪: %s (Gas指数=%.1f%%, 12h前=%.1f%%, 趋势%s)",
                 direction, gas_index * 100, gas_12h_ago * 100, trend_str)
    else:
        log.info("大盘情绪: %s (Gas指数=%.1f%%, 12h前=无数据, 默认向上)",
                 direction, gas_index * 100)

    return result


def _fmt_age(age_hours: float) -> str:
    """格式化币龄: <1h→分钟, 1h~24h→小时, ≥24h→天"""
    if age_hours < 1:
        return f"{age_hours * 60:.0f}m"
    elif age_hours < 24:
        return f"{age_hours:.1f}h"
    else:
        return f"{age_hours / 24:.1f}d"


def _age_tier_match(age_hours: float, tiers: list[tuple]) -> float:
    """
    币龄阶梯匹配: 找到 age_hours 满足的最高 tier 阈值。
    tiers: [(age_threshold, value), ...] 按 age 升序排列
    返回对应的 value; age 未达到任何 tier 时返回 0 (或第一个 tier 的值)
    """
    result = tiers[0][1] if tiers else 0
    for age_th, val in tiers:
        if age_hours >= age_th:
            result = val
    return result


def _age_tier_match_bonus(age_hours: float, tiers: list[tuple]) -> int:
    """
    仿盘加分阶梯: 找到满足条件的最高 bonus score
    tiers: [(age_threshold, count_threshold, bonus_score), ...]
    """
    best_score = 0
    for age_th, cnt_th, score in tiers:
        if age_hours >= age_th:
            best_score = max(best_score, score)
    return best_score


def _check_consolidation(price_hist: list[float], age_hours: float) -> float:
    """
    检测小涨跌不动(横盘整理): 最近 ≥3h 且 ≤8h 内:
      最低价→最高价涨幅 ≤ 3.5倍, 且最高价→最低价跌幅 ≤ 55%
    返回横盘持续时间(小时), 0 表示不满足

    每轮 15min, 3h=12轮, 8h=32轮
    """
    min_rounds = int(BONUS_CONSOLIDATION_MIN_HOURS * 4)   # 12
    max_rounds = int(BONUS_CONSOLIDATION_MAX_HOURS * 4)   # 32
    total_rounds = len(price_hist)
    if total_rounds < min_rounds:
        return 0.0

    actual_rounds = min(max_rounds, total_rounds)
    best_hours = 0.0

    for window_size in range(min_rounds, actual_rounds + 1):
        window = price_hist[-window_size:]
        valid_prices = [p for p in window if p > 0]
        if len(valid_prices) < 2:
            continue
        price_min = min(valid_prices)
        price_max = max(valid_prices)
        if price_min <= 0 or price_max <= 0:
            continue

        gain_ratio = price_max / price_min           # 最低→最高涨幅倍数
        drawdown = (price_max - price_min) / price_max  # 最高→最低跌幅

        if gain_ratio <= BONUS_CONSOLIDATION_MAX_GAIN and drawdown <= BONUS_CONSOLIDATION_MAX_DRAWDOWN:
            hours = window_size / 4.0
            if hours > best_hours:
                best_hours = hours

    return best_hours


def tag_filter(candidates: list[dict], now_ms: int,
               market_sentiment: dict | None = None) -> list[dict]:
    """
    标签制精筛 — v16: 加分标签(优先级排序)

    基础标签: 已注释, 全部放行

    加分标签 (只三项硬指标, 至少一项才开仓):
      - 小涨跌不动: 最低→最高涨幅≤3.5倍 且 最高→最低跌幅≤55%, 横盘≥3h且≤8h
      - 成交额异动: 阳线 + 成交额增量≥前根×7 + K线涨幅≤100% + 增量≥500u
      - 流动性异动: 仅已毕业, 阳线 + 流动性增量≥前根×1.2 + 价格涨幅<100% + 增量≥$15k

    返回按加分总分降序排列的精筛结果
    """
    results = []

    # 大盘是否向上
    is_bullish = True
    if market_sentiment:
        is_bullish = market_sentiment.get("is_bullish", True)

    for t in candidates:
        addr = t.get("address", "")
        current_price = t.get("price", 0)
        if current_price <= 0:
            continue

        holders = t.get("holders", 0)
        name = t.get("name") or addr[:16]
        progress = t.get("progress", 0)
        liquidity = t.get("liquidity", 0)
        is_graduated = progress >= 1.0
        source = t.get("source", "")
        is_flap = source == "flap"
        age_hours = (now_ms - t.get("createdAt", 0)) / 3600000
        peak_price = t.get("peakPrice", 0)

        # basic_pass = True
        # basic_fail_reasons = []

        # ==================== 基础标签: 全部已注释 (v16: 基础全过, 只看加分项) ====================
        # basic_pass = True
        # basic_fail_reasons = []
        #
        # # === 大盘向上 ===
        # if not is_bullish:
        #     basic_fail_reasons.append("大盘向下(近1hGAS<近12hGAS)")
        #     basic_pass = False
        #
        # # === 持币合格 ===
        # min_holders = _age_tier_match(age_hours, TAG_HOLDERS_TIERS)
        # if holders < min_holders:
        #     basic_fail_reasons.append(f"持币{holders}<{min_holders}(币龄{_fmt_age(age_hours)})")
        #     basic_pass = False
        #
        # # === 进度/流动性合格 ===
        # if is_graduated:
        #     min_liquidity = _age_tier_match(age_hours, TAG_LIQUIDITY_TIERS)
        #     if liquidity < min_liquidity:
        #         basic_fail_reasons.append(f"毕业流动性${liquidity:.0f}<${min_liquidity:.0f}(币龄{_fmt_age(age_hours)})")
        #         basic_pass = False
        # else:
        #     min_progress = _age_tier_match(age_hours, TAG_PROGRESS_TIERS)
        #     if progress < min_progress:
        #         basic_fail_reasons.append(f"进度{progress*100:.0f}%<{min_progress*100:.0f}%(币龄{_fmt_age(age_hours)})")
        #         basic_pass = False
        #
        # # === 1h买卖数合格 ===
        # buys_h1 = t.get("buysH1", 0)
        # sells_h1 = t.get("sellsH1", 0)
        # min_buys = _age_tier_match(age_hours, TAG_BUYS_TIERS)
        # if buys_h1 > 0 or sells_h1 > 0:
        #     if buys_h1 < min_buys:
        #         basic_fail_reasons.append(f"1h买入{buys_h1}<{min_buys}(币龄{_fmt_age(age_hours)})")
        #         basic_pass = False
        #     elif buys_h1 <= sells_h1:
        #         basic_fail_reasons.append(f"1h买入{buys_h1}≤卖出{sells_h1}")
        #         basic_pass = False
        #
        # # === 成交额合格 ===
        # volume_h1 = t.get("volumeH1", 0)
        # min_volume = _age_tier_match(age_hours, TAG_VOLUME_TIERS)
        # if volume_h1 > 0 and volume_h1 < min_volume:
        #     basic_fail_reasons.append(f"成交额${volume_h1:.0f}<${min_volume}(币龄{_fmt_age(age_hours)})")
        #     basic_pass = False
        #
        # # === 社交合格 ===
        # social_count = t.get("socialCount", 0)
        # if age_hours >= TAG_SOCIAL_MIN_AGE and social_count < TAG_SOCIAL_MIN_COUNT:
        #     flap_exempt = (is_flap
        #                    and holders >= TAG_FLAP_SOCIAL_EXEMPT_HOLDERS
        #                    and (is_graduated or progress >= TAG_FLAP_SOCIAL_EXEMPT_PROGRESS))
        #     if not flap_exempt:
        #         basic_fail_reasons.append(f"无社交媒体(币龄{_fmt_age(age_hours)})")
        #         basic_pass = False
        #
        # # === 创建者/代币合格 ===
        # creator = (t.get("creator") or "").lower()
        # if creator and creator in DEPLOYER_BLACKLIST:
        #     basic_fail_reasons.append(f"创建者在黑名单")
        #     basic_pass = False
        #
        # # === 币龄合格 ===
        # if age_hours > MAX_AGE_HOURS:
        #     basic_fail_reasons.append(f"币龄{_fmt_age(age_hours)}>{MAX_AGE_HOURS}h")
        #     basic_pass = False
        #
        # # === 未追高 (K线) ===
        # if peak_price > 0 and current_price > 0:
        #     price_change = current_price / peak_price - 1
        #     max_change = _age_tier_match(age_hours, TAG_KLINE_TIERS)
        #     if price_change > max_change:
        #         basic_fail_reasons.append(f"涨幅{price_change*100:.0f}%>{max_change*100:.0f}%(币龄{_fmt_age(age_hours)})")
        #         basic_pass = False
        #
        # # === 波动充足 (近3轮振幅≥20%) ===
        # price_hist = t.get("priceHistory", [])
        # if price_hist and len(price_hist) >= 2:
        #     window = price_hist[-TAG_AMPLITUDE_WINDOW:]
        #     has_amplitude = False
        #     for i in range(1, len(window)):
        #         prev = window[i - 1]
        #         curr = window[i]
        #         if prev > 0 and curr > 0:
        #             amp = abs(curr - prev) / prev
        #             if amp >= TAG_AMPLITUDE_MIN_RATIO:
        #                 has_amplitude = True
        #                 break
        #     if not has_amplitude:
        #         if len(price_hist) >= TAG_AMPLITUDE_WINDOW:
        #             basic_fail_reasons.append(f"波动不足(近{min(len(window),TAG_AMPLITUDE_WINDOW)}轮振幅<20%)")
        #             basic_pass = False
        #
        # # === 趋势向上 (当前价≥4轮前价) ===
        # if price_hist and len(price_hist) >= TAG_TREND_WINDOW:
        #     price_4_ago = price_hist[-TAG_TREND_WINDOW]
        #     if price_4_ago > 0 and current_price < price_4_ago:
        #         basic_fail_reasons.append(f"趋势向下(当前{current_price:.2e}<{TAG_TREND_WINDOW}轮前{price_4_ago:.2e})")
        #         basic_pass = False
        #
        # if not basic_pass:
        #     continue

        price_hist = t.get("priceHistory", [])
        creator = (t.get("creator") or "").lower()

        # ==================== 加分标签计算 (只三项硬指标) ====================
        bonus_score = 0
        bonus_tags = []

        # --- 加分: 小涨跌不动 (横盘整理 ≥3h 且 ≤8h) ---
        if price_hist and len(price_hist) >= 2:
            consolidation = _check_consolidation(price_hist, age_hours)
            if consolidation:
                bonus_score += BONUS_WEIGHT_CONSOLIDATION
                bonus_tags.append(f"小涨跌不动(横盘{consolidation:.1f}h)")

        # --- 加分: 成交额异动 (15min K线阳线 + 成交量爆炸 + 涨幅≤100%) ---
        vol_hist = t.get("volumeHistory", [])
        if len(vol_hist) >= 3 and len(price_hist) >= 2 and price_hist[-2] > 0:
            is_bullish_candle = price_hist[-1] > price_hist[-2]
            vol_cur = vol_hist[-1] - vol_hist[-2]
            vol_prev = vol_hist[-2] - vol_hist[-3]
            price_gain = (price_hist[-1] - price_hist[-2]) / price_hist[-2]
            if (is_bullish_candle and vol_prev > 0
                    and vol_cur >= vol_prev * BONUS_VOLUME_SURGE_RATIO
                    and vol_cur >= BONUS_VOLUME_SURGE_MIN_DELTA
                    and price_gain <= BONUS_VOLUME_SURGE_MAX_PRICE_GAIN):
                bonus_score += BONUS_WEIGHT_VOLUME_SURGE
                bonus_tags.append(f"成交额异动({vol_cur/vol_prev:.1f}x, 当前${vol_cur:.0f}/前根${vol_prev:.0f})")

        # --- 加分: 流动性异动 (仅已毕业, 15min K线阳线 + liq surge) ---
        if is_graduated:
            liq_hist = t.get("liquidityHistory", [])
            if len(liq_hist) >= 3 and len(price_hist) >= 2 and price_hist[-2] > 0:
                is_bullish_candle = price_hist[-1] > price_hist[-2]
                liq_cur = liq_hist[-1] - liq_hist[-2]
                liq_prev = liq_hist[-2] - liq_hist[-3]
                price_gain = (price_hist[-1] - price_hist[-2]) / price_hist[-2]
                if (is_bullish_candle and liq_prev > 0
                        and liq_cur >= liq_prev * BONUS_LIQ_SURGE_RATIO
                        and liq_cur >= BONUS_LIQ_SURGE_MIN_DELTA
                        and price_gain < BONUS_LIQ_SURGE_MAX_PRICE_GAIN):
                    bonus_score += BONUS_WEIGHT_LIQ_SURGE
                    bonus_tags.append(f"流动性异动({liq_cur/liq_prev:.1f}x)")

        t["_bonus_score"] = bonus_score
        t["_bonus_tags"] = bonus_tags
        t["_age_hours"] = age_hours
        t["_min_holders"] = _age_tier_match(age_hours, TAG_HOLDERS_TIERS)
        t["isGraduated"] = is_graduated

        # 至少一个加分项才能通过精筛
        if bonus_score <= 0:
            continue

        results.append(t)

        grad_str = "毕业" if is_graduated else f"进度{progress*100:.0f}%"
        log.info("标签精筛: ✓ %s — 币龄=%.1fh, 加分=%d, 标签=[%s]",
                 name, age_hours, bonus_score,
                 ", ".join(bonus_tags) if bonus_tags else "无")

    results.sort(key=lambda x: x.get("_bonus_score", 0), reverse=True)

    return results


def post_quality_defense(candidates: list[dict], api_key: str) -> list[dict]:
    """
    精筛后防线: 对精筛通过的少量代币做深度检查 (仅个位数, 不影响速度)
    1. Top10 持仓集中度: BSCScan Top Holders → 占比 > 85% 排除 (庄家控盘)
    2. 开发者行为: BSCScan Transfer → 清仓/撤池子 排除 (跑路信号)
    3. 假K线检测: GeckoTerminal 15min+1min K线 → 无影线≥80%/全阳线≥90%/脉冲死线 排除 (控盘刷量)
    """
    if not candidates or not api_key:
        return candidates

    log.info("精筛后防线: 检查 %d 个代币 (Top Holder + 开发者行为)...", len(candidates))

    results = []
    for t in candidates:
        addr = t.get("address", "")
        name = t.get("name") or addr[:16]
        creator = t.get("creator", "")
        total_supply = t.get("totalSupply", TOTAL_SUPPLY)
        exclude_reason = None

        # 1. Top10 持仓集中度检查
        try:
            top_holders = bscscan_top_holders(addr, api_key, offset=10)
            if top_holders:
                top10_total = 0
                for h in top_holders:
                    balance = int(h.get("TokenHolderQuantity", 0) or 0)
                    holder_addr = (h.get("TokenHolderAddress") or "").lower()
                    # 排除零地址/死地址/合约地址 (这些不算真实持仓)
                    if holder_addr in BURN_ADDRESSES:
                        continue
                    top10_total += balance
                if total_supply > 0:
                    concentration = top10_total / total_supply
                    t["top10Concentration"] = round(concentration, 4)
                    if concentration > TOP10_CONCENTRATION_MAX:
                        exclude_reason = "Top10持仓{:.0f}% (庄家控盘)".format(concentration * 100)
        except Exception as e:
            log.debug("Top Holder 查询失败 %s: %s", name, e)

        # 2. 开发者行为检查 (仅有 creator 地址时)
        if not exclude_reason and creator:
            try:
                dev = analyze_developer_behavior(addr, creator, api_key)
                if dev.get("exclude"):
                    exclude_reason = "开发者异常: " + ", ".join(dev.get("details", []))
                t["devBehavior"] = {
                    "hasSell": dev.get("has_sell", False),
                    "hasBuy": dev.get("has_buy", False),
                    "hasLpRemove": dev.get("has_lp_remove", False),
                    "sellPct": dev.get("sell_pct", 0),
                }
            except Exception as e:
                log.debug("开发者行为查询失败 %s: %s", name, e)

        # 3. 假K线检测 (GeckoTerminal 15min + 1min K线)
        # 特征 A/B: 15min K线全是实体柱无影线 / 全阳线 = 价格被控制
        # 特征 C: 1min K线头部脉冲 + 后续全是死线 = 拉盘后无人接盘
        if not exclude_reason:
            try:
                kline_query_addr = t.get("gtPoolAddress") or addr
                candles_15m = gt_ohlcv_15min(kline_query_addr, limit=12)  # 最近 3 小时的 15min K线
                candles_1m = gt_ohlcv_1min(kline_query_addr, limit=30)    # 最近 30 根 1min K线
                fake_result = detect_fake_candles(candles_15m, candles_1m)
                t["fakeCandle"] = fake_result
                if fake_result["fake"]:
                    exclude_reason = "假K线 ({} — 控盘刷量)".format(fake_result["reason"])
            except Exception as e:
                log.debug("假K线检测失败 %s: %s", name, e)

        if exclude_reason:
            log.info("  精筛后防线: ✗ %s — %s", name, exclude_reason)
            t["defenseExcludeReason"] = exclude_reason
        else:
            results.append(t)

    excluded = len(candidates) - len(results)
    if excluded > 0:
        log.info("精筛后防线: 排除 %d 个, 通过 %d 个", excluded, len(results))

    return results


# ===================================================================
#  消息格式化 & 推送
# ===================================================================
def format_message(results: list[dict]) -> str:
    """格式化精筛结果为钉钉 Markdown 格式"""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"## 🔍 BSC Token Scanner v6 极速报告", f"⏰ {now_str}", ""]

    for i, item in enumerate(results, 1):
        addr = item.get("address", "")
        name = item.get("name", "Unknown")
        symbol = item.get("symbol", "")
        price = item.get("price", 0)
        holders = item.get("holders", 0)
        age_min = (int(time.time() * 1000) - item.get("createdAt", 0)) / 60000

        lines.append(f"### #{i} {symbol} ({name})")
        if item.get("isRocket"):
            lines.append("🚀 **火箭通道** (快速起飞币)")
        if item.get("isGraduated"):
            lines.append("🎓 **毕业通道** (刚毕业强势币)")
        lines.append(f"📄 合约: `{addr}`")
        lines.append(f"💰 当前价: ${price:.10f}")
        lines.append(f"👥 持币: {holders} (峰值 {item.get('peakHolders', 0)})")
        progress = item.get("progress", 0)
        lines.append(f"📊 进度: {progress * 100:.1f}%")
        # 买卖比和交易量
        buys = item.get("buysH1", 0) or item.get("buysH24", 0)
        sells = item.get("sellsH1", 0) or item.get("sellsH24", 0)
        if buys > 0 or sells > 0:
            bs_ratio = buys / sells if sells > 0 else float('inf')
            lines.append(f"📈 买卖比: {bs_ratio:.1f} (买{buys}/卖{sells})")
        vol = item.get("volume24h", 0) or item.get("volumeH1", 0)
        if vol > 0:
            lines.append(f"💹 交易量: ${vol:,.0f}")
        # DexScreener 价格变化 (短期动量)
        pc_h1 = item.get("priceChangeH1", 0)
        pc_h24 = item.get("priceChangeH24", 0)
        if pc_h1 != 0 or pc_h24 != 0:
            lines.append(f"📉 涨跌: 1h {pc_h1:+.1f}% / 24h {pc_h24:+.1f}%")
        # DexScreener Boost (项目方付费推广)
        boosts = item.get("boosts", 0)
        if boosts > 0:
            lines.append(f"🔥 Boost: {boosts} 个活跃推广")
        # Top10 集中度
        top10 = item.get("top10Concentration")
        if top10 is not None:
            lines.append(f"🏦 Top10持仓: {top10 * 100:.0f}%")
        # 开发者行为
        dev = item.get("devBehavior")
        if dev:
            dev_tags = []
            if dev.get("hasBuy"):
                dev_tags.append("加仓")
            if dev.get("hasSell"):
                dev_tags.append(f"卖出{dev.get('sellPct', 0):.0f}%")
            if dev.get("hasLpRemove"):
                dev_tags.append("撤池子")
            if dev_tags:
                lines.append(f"👨‍💻 开发者: {', '.join(dev_tags)}")
            else:
                lines.append("👨‍💻 开发者: 无异常")
        lines.append(f"🔗 社交: {item.get('socialCount', 0)} 个")
        social = item.get("socialLinks", {})
        for stype, url in social.items():
            lines.append(f"  - [{stype}]({url})")
        lines.append(f"🕐 币龄: {age_min:.1f}min")
        cc = item.get("copycat")
        if cc and cc.get("isCopycat"):
            lines.append(f"🔥 仿盘: {cc['count']} 个同名代币")
        desc = (item.get("descr") or "").strip()
        if desc:
            lines.append(f"📝 {desc[:100]}{'...' if len(desc) > 100 else ''}")
        # 平台链接: 根据来源生成不同的链接
        is_flap = item.get("source") == "flap"
        if is_flap:
            platform_link = f"[Flap](https://flap.sh/bnb/{addr})"
        else:
            platform_link = f"[four.meme](https://four.meme/token/{addr})"
        lines.append(
            f"🌐 {platform_link}"
            f" | [BscScan](https://bscscan.com/token/{addr})"
            f" | [币安钱包](https://web3.binance.com/zh-CN/token/bsc/{addr})"
        )
        lines.append("")

    lines.append("—— BSC Token Scanner v6 ——")
    return "\n\n".join(lines)


def _dingtalk_sign(secret: str) -> tuple[str, str]:
    """钉钉加签: 返回 (timestamp, sign)"""
    import hmac
    import hashlib
    import base64
    from urllib.parse import quote_plus
    timestamp = str(round(time.time() * 1000))
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(secret.encode("utf-8"),
                         string_to_sign.encode("utf-8"),
                         digestmod=hashlib.sha256).digest()
    sign = quote_plus(base64.b64encode(hmac_code))
    return timestamp, sign


def send_dingtalk(webhook: str, secret: str, title: str, text: str) -> bool:
    """发送钉钉 Markdown 消息"""
    _ensure_sessions()
    try:
        url = webhook
        if secret:
            ts, sign = _dingtalk_sign(secret)
            url += f"&timestamp={ts}&sign={sign}"
        r = _fm_session.post(
            url,
            json={
                "msgtype": "markdown",
                "markdown": {"title": title, "text": text},
            },
            timeout=15,
        )
        r.raise_for_status()
        result = r.json()
        if result.get("errcode", -1) == 0:
            return True
        log.error("钉钉返回错误: %s", result)
        return False
    except Exception as e:
        log.error("钉钉: %s", e)
        return False


def print_console(msg: str) -> None:
    out = re.sub(r"<[^>]+>", "", msg)
    print("\n" + "=" * 60)
    print(out)
    print("=" * 60 + "\n")


# ===================================================================
#  从 token_scanner (GitHub Pages) 拉取精筛结果
# ===================================================================
SCANNER_LATEST_URL = "https://liuqinh2s.github.io/token_scanner/data/latest.json"

def fetch_scanner_quality_tokens() -> tuple[list[dict], str]:
    """
    从 token_scanner 的 GitHub Pages 拉取最新精筛结果
    返回 (代币列表, scanTime 字符串)
    失败返回 ([], "")
    """
    _ensure_sessions()
    try:
        resp = _gt_session.get(SCANNER_LATEST_URL, timeout=10)
        if resp.status_code != 200:
            log.info("拉取 scanner 精筛结果: HTTP %d, 跳过", resp.status_code)
            return [], ""
        data = resp.json()
        tokens = data.get("tokens", [])
        scan_time_str = data.get("scanTime", "")
        if not tokens:
            return [], scan_time_str
        # 检查数据新鲜度: scanTime 格式 "2026-04-14 21:01:34"
        if scan_time_str:
            try:
                from datetime import timedelta
                scan_dt = datetime.strptime(scan_time_str, "%Y-%m-%d %H:%M:%S")
                scan_dt = scan_dt.replace(tzinfo=timezone(timedelta(hours=8)))
                age_min = (datetime.now(timezone(timedelta(hours=8))) - scan_dt).total_seconds() / 60
                if age_min > 30:
                    log.info("拉取 scanner 精筛结果: 数据过旧 (%.0f 分钟前), 跳过", age_min)
                    return [], scan_time_str
            except Exception:
                pass
        # 转换字段名: scanner 输出格式 → trading 内部格式
        result = []
        for t in tokens:
            result.append({
                "address": t.get("address", ""),
                "name": t.get("name", ""),
                "symbol": t.get("symbol", ""),
                "holders": t.get("holders", 0),
                "peakHolders": t.get("peak_holders", 0),
                "createdAt": t.get("created_at", 0),
                "totalSupply": t.get("total_supply", 0),
                "price": t.get("price", 0),
                "peakPrice": t.get("max_price", 0),
                "socialCount": t.get("social_count", 0),
                "socialLinks": t.get("social_links", {}),
                "progress": t.get("progress", 0),
                "liquidity": t.get("liquidity", 0),
                "peakLiquidity": t.get("peak_liquidity", 0),
                "raisedAmount": t.get("raised_amount", 0),
                "volume24h": t.get("volume_24h", 0),
                "volumeH1": t.get("volume_h1", 0),
                "buysH1": t.get("buys_h1", 0),
                "sellsH1": t.get("sells_h1", 0),
                "buysH24": t.get("buys_h24", 0),
                "sellsH24": t.get("sells_h24", 0),
                "_bonus_score": t.get("bonus_score", 0),
                "_bonus_tags": t.get("bonus_tags", []),
                "_from_scanner": True,  # 标记来源
            })
        log.info("拉取 scanner 精筛结果: %d 个代币 (scanTime=%s)", len(result), scan_time_str)
        return result, scan_time_str
    except Exception as e:
        log.info("拉取 scanner 精筛结果失败: %s", e)
        return [], ""


# 上一轮 scanner 的 scanTime, 用于轮询时判断是否有新数据
_last_scanner_scan_time: str = ""

def poll_scanner_quality_tokens(poll_interval: int = 30, max_wait: int = 300) -> list[dict]:
    """
    轮询 token_scanner 的 GitHub Pages, 等待本轮新数据
    - poll_interval: 轮询间隔 (秒), 默认 30s
    - max_wait: 最大等待时间 (秒), 默认 300s (5 分钟)
    - 判断逻辑: scanTime 与上一轮不同 → 说明 scanner 已更新
    返回精筛代币列表, 超时返回空列表
    """
    global _last_scanner_scan_time
    waited = 0
    log.info("等待 scanner 新数据 (上次 scanTime=%s, 每 %ds 轮询, 最多等 %ds)...",
             _last_scanner_scan_time or "无", poll_interval, max_wait)
    while waited < max_wait:
        tokens, scan_time = fetch_scanner_quality_tokens()
        # 有数据且 scanTime 与上一轮不同 → 拿到新数据
        if scan_time and scan_time != _last_scanner_scan_time:
            log.info("scanner 新数据到达 (scanTime=%s, 等待 %ds)", scan_time, waited)
            _last_scanner_scan_time = scan_time
            return tokens
        # 首次运行 (无上一轮记录), 有数据就直接用
        if not _last_scanner_scan_time and tokens:
            _last_scanner_scan_time = scan_time
            return tokens
        time.sleep(poll_interval)
        waited += poll_interval
        log.info("轮询 scanner... (已等 %ds/%ds)", waited, max_wait)
    log.info("等待 scanner 新数据超时 (%ds), 本轮不合并 scanner 结果", max_wait)
    return []


# ===================================================================
#  主扫描流程
# ===================================================================
_scan_count = 0                    # 扫描轮次计数, 用于持仓同步降频
_SYNC_EVERY_N_SCANS = 1           # 每轮扫描同步一次持仓 (间隔已改为 15 分钟)

def scan_once(cfg: dict) -> None:
    global _scan_count
    _scan_count += 1
    log.info("=" * 50)
    log.info("开始扫描 (第 %d 轮)", _scan_count)
    log.info("=" * 50)
    max_push = cfg.get("max_push_count", 100)

    # 计时开始
    _t_start = time.time()

    # 行情
    global _bnb_usd_price
    ticker = fm_ticker_prices()
    bnb = ticker.get("BNB", 0)
    if bnb <= 0:
        log.warning("BNB 价格获取失败, 使用默认 600")
        ticker["BNB"] = 600.0
    _bnb_usd_price = ticker["BNB"]
    log.info("BNB=$%.2f", _bnb_usd_price)

    # 每 N 轮同步一次持仓 (监控线程已持续跟踪, 无需每轮全量同步)
    if _HAS_TRADER and cfg.get("trading", {}).get("enabled", False):
        if _scan_count % _SYNC_EVERY_N_SCANS == 1:
            try:
                bnb_usd = ticker.get("BNB", 600.0)
                _sync_positions_from_wallet(bnb_usd)
            except Exception as e:
                log.warning("扫描前持仓同步失败: %s", e)

    now_ms = int(time.time() * 1000)

    # 加载队列
    queue_state = load_queue()

    # 从持久化数据恢复开发者黑名单
    _bl = queue_state.get("deployerBlacklist", {})
    DEPLOYER_BLACKLIST.clear()
    for creator, info in _bl.items():
        if info.get("count", 0) >= DEPLOYER_SCAM_MIN_TOKENS:
            DEPLOYER_BLACKLIST.add(creator)
    if DEPLOYER_BLACKLIST:
        log.info("开发者黑名单: 加载 %d 个地址", len(DEPLOYER_BLACKLIST))

    # 从持久化数据恢复开发者白名单
    _wl = queue_state.get("deployerWhitelist", {})
    DEPLOYER_WHITELIST.clear()
    DEPLOYER_WHITELIST.update(_wl.keys())
    if DEPLOYER_WHITELIST:
        log.info("开发者白名单: 加载 %d 个地址", len(DEPLOYER_WHITELIST))

    # 迁移: 旧版 breakthrough 冻结快照 → 回归 tokens 列表 (一次性)
    old_bt = queue_state.pop("breakthrough", [])
    if old_bt:
        token_addrs = set(t["address"] for t in queue_state.get("tokens", []))
        migrated = 0
        for t in old_bt:
            if t["address"] not in token_addrs:
                t["isBreakthrough"] = True
                t["_bt_persisted"] = True
                queue_state["tokens"].append(t)
                migrated += 1
        if migrated:
            log.info("迁移旧突破代币: %d 个回归队列", migrated)
            save_queue(queue_state)

    existing_addrs = set(
        [t["address"] for t in queue_state.get("tokens", [])]
        + [t["address"] for t in queue_state.get("eliminated", [])]
    )

    # Step 1: 链上发现
    log.info("\n--- Step 1: 链上发现 ---")
    new_on_chain, latest_block = discover_on_chain(queue_state.get("lastBlock", 0))

    # Step 2: 入场筛
    log.info("\n--- Step 2: 入场筛 ---")
    admitted, rejected_at_entry = admission_filter(new_on_chain, existing_addrs)

    # 将通过入场筛的代币加入队列
    if admitted:
        for item in admitted:
            token = item["token"]
            detail = item["detail"]
            queue_state["tokens"].append({
                "address": token["address"],
                "creator": token.get("creator", ""),
                "block": token.get("block", 0),
                "source": token.get("source", "four.meme"),
                "name": detail.get("name") or token.get("name", ""),
                "symbol": detail.get("shortName") or token.get("symbol", ""),
                "createdAt": token["createdAt"],
                "addedAt": now_ms,
                "totalSupply": detail["totalSupply"],
                "socialCount": detail["socialCount"],
                "socialLinks": detail["socialLinks"],
                "descr": detail.get("descr", ""),
                "price": detail["price"],
                "addedPrice": detail["price"],
                "peakPrice": detail["price"],
                "holders": detail["holders"],
                "addedHolders": detail["holders"],
                "peakHolders": detail["holders"],
                "liquidity": detail.get("liquidity", 0),
                "peakLiquidity": detail.get("liquidity", 0),
                "raisedAmount": detail.get("raisedAmount", 0),
                "progress": detail.get("progress", 0),
                "peakProgress": detail.get("progress", 0),
                "addedProgress": detail.get("progress", 0),
                "day1Vol": detail.get("day1Vol", 0),
                "consecDrops": 0,
                "lastPrice": detail["price"],
                "priceHistory": [detail["price"]],
                "holdersHistory": [detail["holders"]],
                # flap 社交 pending: 入场时社交抓取失败, 标记待补查轮数 (0=pending, -1=已有社交)
                "_socialPendingRounds": detail.get("_socialPendingRounds", -1),
            })

    log.info("入队后: %d 个代币", len(queue_state["tokens"]))

    # Step 2b: 新入队代币 Top Holders 采集 (数据积累, 供后续分析)
    bscscan_key = cfg.get("bscscan_api_key", "")
    if admitted and bscscan_key:
        new_addrs = [item["token"]["address"] for item in admitted]
        log.info("Top Holders 采集: 查询 %d 个新入队代币...", len(new_addrs))
        top_holders_map = bscscan_top_holders_batch(new_addrs, bscscan_key, top_n=10)
        # 写入队列数据
        for t in queue_state["tokens"]:
            th = top_holders_map.get(t["address"])
            if th is not None:
                t["topHolders"] = th
        if top_holders_map:
            log.info("Top Holders 采集: %d/%d 个代币有数据", len(top_holders_map), len(new_addrs))

    # Step 3: 淘汰检查
    log.info("\n--- Step 3: 淘汰检查 ---")
    bscscan_key = cfg.get("bscscan_api_key", "")
    survivors, eliminated = elimination_check(queue_state["tokens"], now_ms, bscscan_key)
    queue_state["tokens"] = survivors
    queue_state["eliminated"].extend([{
        "address": e["address"], "name": e.get("name", ""),
        "symbol": e.get("symbol", ""),
        "elimReason": e["elimReason"], "eliminatedAt": e["eliminatedAt"],
        "createdAt": e.get("createdAt", 0),
    } for e in eliminated])

    # 入场被拒的代币也记入 eliminated (避免重复入场筛)
    queue_state["eliminated"].extend([{
        "address": r["token"].get("address", ""),
        "name": (r.get("detail") or {}).get("name") or r["token"].get("name", ""),
        "symbol": (r.get("detail") or {}).get("shortName") or r["token"].get("symbol", ""),
        "elimReason": f"入场拒绝: {r.get('reason', '')}",
        "eliminatedAt": now_ms,
        "createdAt": r["token"].get("createdAt", 0),
    } for r in rejected_at_entry if r["token"].get("address")])

    # Step 3b: 更新开发者画像 (诈骗/优质开发者自动收集)
    update_deployer_reputation(queue_state, survivors, eliminated)

    log.info("淘汰后: %d 个存活, %d 个淘汰", len(survivors), len(eliminated))

    # Step 4: 仿盘检测 + 精筛
    log.info("\n--- Step 4: 仿盘检测 + 精筛 ---")

    # 仿盘检测 (本地队列统计, 零 API 调用)
    rejected_as_known = []
    for r in rejected_at_entry:
        token = r.get("token", {})
        detail = r.get("detail") or {}
        rejected_as_known.append({
            "address": token.get("address", ""),
            "name": detail.get("name") or token.get("name", ""),
            "symbol": detail.get("shortName") or token.get("symbol", ""),
        })
    all_known = queue_state.get("tokens", []) + queue_state.get("eliminated", []) + rejected_as_known

    # 更新持久化名称索引
    update_name_index(queue_state, all_known)

    all_to_check = survivors + eliminated
    copycat_map = detect_copycats(all_to_check, all_known,
                                  queue_state.get("nameIndex"))
    for t in survivors:
        cc = copycat_map.get(t.get("address", "").lower())
        if cc:
            t["copycat"] = cc
    for t in eliminated:
        cc = copycat_map.get(t.get("address", "").lower())
        if cc:
            t["copycat"] = cc

    # 精筛 (标签制: 基础标签 + 加分项, 统一通道)
    # 先批量查询社交质量 (Twitter 粉丝数 + TG 成员数, 用于加分计算)
    batch_check_social_quality(survivors)
    # 计算大盘情绪
    market_sentiment = calc_market_sentiment(survivors, queue_state)
    scan_round = queue_state.get("scanRound", _scan_count - 1) + 1
    quality_results = tag_filter(survivors, now_ms, market_sentiment)

    # 精筛代币持币数刷新: 用 BSCScan 网页爬取真实持币数 (four.meme detail 对未毕业币不准)
    # 必须在再验证之前执行, 否则再验证用的是旧数据
    if quality_results:
        q_addrs = [t["address"] for t in quality_results]
        log.info("精筛持币数刷新: BSCScan 查询 %d 个代币...", len(q_addrs))
        real_holders = graduated_holder_counts(q_addrs)
        for t in quality_results:
            rh = real_holders.get(t["address"])
            if rh is not None and rh > 0:
                old_h = t.get("holders", 0)
                t["holders"] = rh
                t["peakHolders"] = max(t.get("peakHolders", 0), rh)
                # 同步更新队列中的对应代币
                for s in survivors:
                    if s["address"] == t["address"]:
                        s["holders"] = rh
                        s["peakHolders"] = max(s.get("peakHolders", 0), rh)
                        break
                if rh != old_h:
                    log.info("  持币数刷新 %s: %d→%d (BSCScan)",
                             t.get("name") or t["address"][:16], old_h, rh)

    # 精筛代币K线修正: 仅对精筛通过的少量代币拉 GT K线
    # 用 K线 high/low 补充 peakPrice, 并记录 klineHigh/klineLow 供过山车检测
    # 同时记录 klineMaxDrop (单根K线最大跌幅) 供过山车暴跌检测
    if quality_results:
        log.info("精筛K线修正: 查询 %d 个代币...", len(quality_results))
        kline_data = gt_batch_peak_prices(quality_results)
        for t in quality_results:
            kd = kline_data.get(t["address"])
            if kd:
                kline_high = kd["high"]
                kline_low = kd["low"]
                kline_max_drop = kd.get("maxCandleDrop", 0)
                old_peak = t.get("peakPrice", 0)
                if kline_high > old_peak:
                    t["peakPrice"] = kline_high
                    for s in survivors:
                        if s["address"] == t["address"]:
                            s["peakPrice"] = kline_high
                            break
                    log.info("  K线修正 %s: peakPrice %.2e → %.2e (+%.0f%%)",
                             t.get("name") or t["address"][:16],
                             old_peak, kline_high,
                             (kline_high / old_peak - 1) * 100 if old_peak > 0 else 0)
                t["klineHigh"] = kline_high
                t["klineLow"] = kline_low
                t["klineMaxDrop"] = kline_max_drop
                t["klineFixed"] = True
                kline_candles_data = kd.get("klineCandles", [])
                if kline_candles_data:
                    t["klineCandles"] = kline_candles_data
                for s in survivors:
                    if s["address"] == t["address"]:
                        s["klineHigh"] = kline_high
                        s["klineLow"] = kline_low
                        s["klineMaxDrop"] = kline_max_drop
                        s["klineFixed"] = True
                        if kline_candles_data:
                            s["klineCandles"] = kline_candles_data
                        break

    # 精筛再验证: K线更新后重新检查淘汰条件 + 精筛条件
    # - 不符合队列存活 → 移到本轮淘汰
    # - 符合队列存活但不符合精筛 → 留在队列 (从精筛列表移除)
    revalidated = []
    demoted_to_elim = 0
    for t in quality_results:
        # 检查是否满足淘汰条件 (复用 elimination_check 的条件逻辑)
        age_hours = (now_ms - t.get("createdAt", 0)) / 3600000
        current_price = t.get("price", 0)
        current_holders = t.get("holders", 0)
        current_liq = t.get("liquidity", 0)
        current_progress = t.get("progress", 0)
        peak_price = t.get("peakPrice", 0)
        is_graduated = current_progress >= 1
        elim_reason = None

        # 币龄淘汰
        if age_hours > MAX_AGE_HOURS:
            elim_reason = f"币龄>{MAX_AGE_HOURS}h"
        # 价格跌 90%
        if not elim_reason and peak_price > 0 and current_price > 0:
            if current_price < peak_price * (1 - ELIM_PRICE_DROP_PCT):
                elim_reason = (f"价格跌{(1 - current_price / peak_price) * 100:.0f}% "
                               f"(峰:{peak_price:.2e} 现:{current_price:.2e})")
        # 单根K线暴跌 (过山车币): 任何一根15min K线 close/open 跌超阈值
        if not elim_reason:
            kmd = t.get("klineMaxDrop")
            if kmd is not None and kmd <= -ELIM_KLINESINGLE_DROP_PCT:
                elim_reason = f"单根K线跌{abs(kmd)*100:.0f}% (过山车币, >{ELIM_KLINESINGLE_DROP_PCT*100:.0f}%阈值)"
        # 持币数从 30+ 跌破 10
        if not elim_reason:
            if (t.get("peakHolders", 0) >= ELIM_HOLDERS_PEAK_MIN
                    and current_holders < ELIM_HOLDERS_FLOOR):
                elim_reason = f"持币数 {t.get('peakHolders', 0)}→{current_holders}"
        # 无社交 (four.meme + flap 统一淘汰)
        if not elim_reason and t.get("socialCount", 0) < MIN_SOCIAL_COUNT:
            elim_reason = "无社交媒体"
        # 流动性枯竭 (仅已毕业)
        if not elim_reason and is_graduated:
            if (t.get("peakLiquidity", 0) >= ELIM_LIQ_PEAK_MIN
                    and current_liq < ELIM_LIQ_FLOOR):
                elim_reason = f"流动性 ${t.get('peakLiquidity', 0):.0f}→${current_liq:.0f}"
        # 进度阶梯淘汰
        if not elim_reason:
            for age_h, min_prog in ELIM_PROGRESS_TIERS:
                if age_hours > age_h and current_progress < min_prog:
                    elim_reason = f"进度{current_progress * 100:.1f}% 币龄{_fmt_age(age_hours)} (阈值{min_prog*100:.0f}%)"
                    break
        # 早期/中期持币数不足
        if not elim_reason:
            if age_hours > ELIM_EARLY_AGE_MIN and t.get("peakHolders", 0) < ELIM_EARLY_PEAK_HOLDERS:
                elim_reason = f"币龄{_fmt_age(age_hours)} 最高持币仅{t.get('peakHolders', 0)}"
        if not elim_reason:
            if age_hours > ELIM_MID_AGE_HOURS and t.get("peakHolders", 0) < ELIM_MID_PEAK_HOLDERS:
                elim_reason = f"币龄{_fmt_age(age_hours)} 最高持币仅{t.get('peakHolders', 0)}"

        if elim_reason:
            # 从队列存活中移除, 加入淘汰列表
            survivors[:] = [s for s in survivors if s["address"] != t["address"]]
            eliminated.append({**t, "eliminatedAt": now_ms, "elimReason": elim_reason})
            queue_state["eliminated"].append({
                "address": t["address"], "name": t.get("name", ""),
                "symbol": t.get("symbol", ""),
                "elimReason": elim_reason, "eliminatedAt": now_ms,
                "createdAt": t.get("createdAt", 0),
            })
            demoted_to_elim += 1
            log.info("精筛再验证: ✗ %s → 淘汰 (%s)",
                     t.get("name") or t["address"][:16], elim_reason)
        else:
            # K线成交额异动修正: 用 GT 15min K线真实成交量重新判定
            kline_candles = t.get("klineCandles", [])
            if len(kline_candles) >= 2:
                cur_candle = kline_candles[-1]
                prev_candle = kline_candles[-2]
                is_bullish_kline = cur_candle["c"] > cur_candle["o"]
                kline_vol_cur = cur_candle["v"]
                kline_vol_prev = prev_candle["v"]
                if prev_candle["c"] > 0:
                    kline_price_gain = (cur_candle["c"] - prev_candle["c"]) / prev_candle["c"]
                else:
                    kline_price_gain = 0

                old_tags = t.get("_bonus_tags", [])
                had_volume_surge = any("成交额异动" in tag for tag in old_tags)

                kline_surge = (is_bullish_kline and kline_vol_prev > 0
                               and kline_vol_cur >= kline_vol_prev * BONUS_VOLUME_SURGE_RATIO
                               and kline_vol_cur >= BONUS_VOLUME_SURGE_MIN_DELTA
                               and kline_price_gain <= BONUS_VOLUME_SURGE_MAX_PRICE_GAIN)

                if kline_surge:
                    if not had_volume_surge:
                        t["_bonus_tags"] = old_tags + [f"成交额异动({kline_vol_cur/kline_vol_prev:.1f}x, 当前${kline_vol_cur:.0f}/前根${kline_vol_prev:.0f})"]
                        t["_bonus_score"] = t.get("_bonus_score", 0) + BONUS_WEIGHT_VOLUME_SURGE
                        log.info("  K线成交额修正 %s: +成交额异动(%.1fx, 实K线 当前$%.0f/前根$%.0f)",
                                 t.get("name") or t["address"][:16],
                                 kline_vol_cur / kline_vol_prev, kline_vol_cur, kline_vol_prev)
                elif had_volume_surge:
                    t["_bonus_tags"] = [tag for tag in old_tags if "成交额异动" not in tag]
                    t["_bonus_score"] = max(0, t.get("_bonus_score", 0) - BONUS_WEIGHT_VOLUME_SURGE)
                    log.info("  K线成交额修正 %s: -成交额异动(假阳性, 实K线量 %.0f/%.0f)",
                             t.get("name") or t["address"][:16],
                             kline_vol_cur, kline_vol_prev)

                if t.get("_bonus_score", 0) <= 0:
                    elim_reason = "K线成交额修正后无加分项"
                    survivors[:] = [s for s in survivors if s["address"] != t["address"]]
                    eliminated.append({**t, "eliminatedAt": now_ms, "elimReason": elim_reason})
                    queue_state["eliminated"].append({
                        "address": t["address"], "name": t.get("name", ""),
                        "symbol": t.get("symbol", ""),
                        "elimReason": elim_reason, "eliminatedAt": now_ms,
                        "createdAt": t.get("createdAt", 0),
                    })
                    demoted_to_elim += 1
                    log.info("精筛再验证: ✗ %s → 淘汰 (%s)",
                             t.get("name") or t["address"][:16], elim_reason)
                else:
                    revalidated.append(t)
            else:
                old_tags = t.get("_bonus_tags", [])
                had_volume_surge = any("成交额异动" in tag for tag in old_tags)
                if had_volume_surge:
                    t["_bonus_tags"] = [tag for tag in old_tags if "成交额异动" not in tag]
                    t["_bonus_score"] = max(0, t.get("_bonus_score", 0) - BONUS_WEIGHT_VOLUME_SURGE)
                    log.info("  K线成交额修正 %s: -成交额异动(K线数据不足无法验证, 移除标签)",
                             t.get("name") or t["address"][:16])
                if t.get("_bonus_score", 0) <= 0:
                    elim_reason = "K线成交额修正后无加分项"
                    survivors[:] = [s for s in survivors if s["address"] != t["address"]]
                    eliminated.append({**t, "eliminatedAt": now_ms, "elimReason": elim_reason})
                    queue_state["eliminated"].append({
                        "address": t["address"], "name": t.get("name", ""),
                        "symbol": t.get("symbol", ""),
                        "elimReason": elim_reason, "eliminatedAt": now_ms,
                        "createdAt": t.get("createdAt", 0),
                    })
                    demoted_to_elim += 1
                    log.info("精筛再验证: ✗ %s → 淘汰 (%s)",
                             t.get("name") or t["address"][:16], elim_reason)
                else:
                    revalidated.append(t)

    # 再验证后的精筛结果替换原列表
    quality_results = revalidated

    # 精筛再验证后的结果 (标签制已统一通道, 无需单独毕业通道)

    if demoted_to_elim > 0:
        log.info("精筛再验证: %d 个淘汰", demoted_to_elim)

    # 按持币数排序
    quality_results.sort(key=lambda x: (x.get("holders", 0)), reverse=True)

    log.info("精筛通过: %d/%d", len(quality_results), len(survivors))

    # 去重: 过滤掉已推送过的代币 (一个币只推送一次, 保留最早那次)
    pushed_set = set(queue_state.get("pushed", []))
    new_quality = []
    repeat_count = 0
    for t in quality_results:
        addr = t.get("address", "")
        if addr in pushed_set:
            repeat_count += 1
            log.info("精筛去重: 跳过 %s (已推送过)", t.get("name") or addr[:16])
        else:
            new_quality.append(t)
    if repeat_count > 0:
        log.info("精筛去重: 过滤 %d 个重复代币, 剩余 %d 个新代币",
                 repeat_count, len(new_quality))
    quality_results = new_quality

    # 更新队列状态 (精筛再验证可能修改了 survivors)
    queue_state["tokens"] = survivors
    queue_state["lastBlock"] = latest_block
    queue_state["lastScanTime"] = now_ms
    queue_state["scanRound"] = scan_round
    save_queue(queue_state)

    # 合并 token_scanner 的精筛结果 (轮询等待新数据, 补充本地可能遗漏的代币)
    scanner_tokens = poll_scanner_quality_tokens(poll_interval=30, max_wait=300)
    if scanner_tokens:
        local_addrs = {t.get("address", "") for t in quality_results}
        merged_count = 0
        for st in scanner_tokens:
            addr = st.get("address", "")
            if addr and addr not in local_addrs and addr not in pushed_set:
                quality_results.append(st)
                local_addrs.add(addr)
                merged_count += 1
                log.info("合并 scanner 精筛: + %s (%s)", st.get("name") or addr[:16], addr[:16])
            elif addr in pushed_set and addr not in local_addrs:
                log.info("合并 scanner 精筛: 跳过 %s (已推送过)", st.get("name") or addr[:16])
        if merged_count > 0:
            log.info("合并 scanner 精筛: 新增 %d 个代币 (本地已有 %d 个重叠)",
                     merged_count, len(scanner_tokens) - merged_count)

    if not quality_results:
        log.info("本轮无推荐代币 (耗时 %.1f 秒)", time.time() - _t_start)
        return

    # 记录本轮精筛通过的代币到 pushed 集合 (去重用, 在推送/买入之前记录)
    pushed_list = queue_state.get("pushed", [])
    for t in quality_results:
        addr = t.get("address", "")
        if addr and addr not in pushed_set:
            pushed_list.append(addr)
            pushed_set.add(addr)
    # pushed 列表上限: 保留最近 500 个 (避免无限增长)
    if len(pushed_list) > 500:
        pushed_list = pushed_list[-500:]
    queue_state["pushed"] = pushed_list
    save_queue(queue_state)

    # 推送精筛结果 (代币详情, 不管是否开自动交易都推)
    filtered = quality_results[:max_push]
    msg = format_message(filtered)
    log.info("筛选通过 %d 个代币", len(filtered))

    bot_token = cfg.get("dingtalk_webhook", "")
    secret = cfg.get("dingtalk_secret", "")
    if not bot_token or "YOUR" in bot_token:
        log.warning("钉钉未配置, 仅打印:")
        print_console(msg)
    else:
        ok = send_dingtalk(bot_token, secret, "BSC Token Scanner v6", msg)
        log.info("钉钉推送%s", "成功" if ok else "失败")

    # 自动买入
    trading_enabled = _HAS_TRADER and cfg.get("trading", {}).get("enabled", False)
    if trading_enabled:
        bnb_usd = ticker.get("BNB", 600.0)
        to_buy = []
        for item in filtered:
            token_data = {
                "tokenAddress": item["address"],
                "name": item.get("name", ""),
                "shortName": item.get("symbol", ""),
                "channel": "graduated" if item.get("isGraduated") else "quality",
                "source": item.get("source", "four.meme"),
            }
            detail_data = {
                "holders": item.get("holders", 0),
                "price": item.get("price", 0),
                "socialCount": item.get("socialCount", 0),
                "socialLinks": item.get("socialLinks", {}),
                "_bonus_tags": item.get("_bonus_tags", []),
                "_bonus_score": item.get("_bonus_score", 0),
            }
            bonus_str = " | ".join(detail_data["_bonus_tags"]) if detail_data["_bonus_tags"] else "无"
            if detail_data["_bonus_score"] <= 0:
                log.info("  📦 %s [%s] 无加分项, 跳过买入 (bonus=%d)",
                         token_data["shortName"] or token_data["name"],
                         token_data["source"], detail_data["_bonus_score"])
                continue
            log.info("  📦 %s [%s] 加分(%d): %s",
                     token_data["shortName"] or token_data["name"],
                     token_data["source"], detail_data["_bonus_score"], bonus_str)
            to_buy.append((token_data, detail_data))
        log.info("自动买入: 准备买入 %d 个代币", len(to_buy))
        try:
            execute_buys(to_buy, cfg, bnb_usd)
        except Exception as e:
            log.error("自动买入异常: %s", e, exc_info=True)
    else:
        log.info("自动买入未执行 (_HAS_TRADER=%s, trading.enabled=%s)",
                 _HAS_TRADER, cfg.get("trading", {}).get("enabled", False))

    log.info("本轮扫描完成 (耗时 %.1f 秒)", time.time() - _t_start)


def main():
    global _fm_session, _gt_session, _bsc_session
    log.info("🚀 BSC Token Scanner v6 极速版启动")
    log.info("配置文件: %s", CONFIG_PATH)

    # 初始化交易模块
    try:
        cfg = load_config()
        if _HAS_TRADER and cfg.get("trading", {}).get("enabled", False):
            _ensure_sessions()
            ticker = fm_ticker_prices()
            bnb_usd = ticker.get("BNB", 600.0)
            if bnb_usd <= 0:
                bnb_usd = 600.0

            if init_trader(cfg, bnb_price_usd=bnb_usd):
                log.info("自动交易已启用, 启动持仓监控...")
                start_monitor(
                    cfg_loader=load_config,
                    bnb_price_func=lambda: fm_ticker_prices().get("BNB", 0),
                )
            else:
                log.warning("交易模块初始化失败, 仅运行扫描模式")
        elif not _HAS_TRADER:
            log.info("交易模块未安装 (缺少 web3), 仅运行扫描模式")
    except Exception as e:
        log.warning("交易模块加载异常: %s, 仅运行扫描模式", e)

    while True:
        try:
            cfg = load_config()
            _fm_session = _build_session(cfg.get("proxy"), FM_HEADERS)
            _gt_session = _build_session(cfg.get("proxy"), GT_HEADERS)
            _bsc_session = _build_session(cfg.get("proxy"), DS_HEADERS)
            scan_once(cfg)

            # 对齐整点时间: 计算到下一个整点间隔的等待秒数
            # 例如 interval=15 → 对齐到 :00, :15, :30, :45
            interval = cfg.get("scan_interval_minutes", SCAN_INTERVAL_MIN)
            now = time.time()
            interval_sec = interval * 60
            # 距离下一个整点间隔的秒数
            elapsed_in_slot = now % interval_sec
            wait_sec = interval_sec - elapsed_in_slot
            # 如果距离下一个整点不足 30 秒, 跳到再下一个 (避免刚扫完立刻再扫)
            if wait_sec < 30:
                wait_sec += interval_sec
            next_time = datetime.fromtimestamp(now + wait_sec).strftime("%H:%M:%S")
            log.info("下次扫描: %s (等待 %.0f 秒, 对齐 %d 分钟整点)",
                     next_time, wait_sec, interval)
            time.sleep(wait_sec)
        except KeyboardInterrupt:
            log.info("用户中断, 退出")
            if _HAS_TRADER:
                stop_monitor()
            break
        except Exception as e:
            log.error("扫描异常: %s", e, exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    main()

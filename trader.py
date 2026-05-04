"""
BSC 自动交易模块 - PancakeSwap V2 + four.meme Bonding Curve + flap Bonding Curve
买入:
  - four.meme bonding curve: 根据报价币 (quote) 自动选择 BNB 或 USDT 直接买入
  - flap bonding curve: 统一通过 Router 合约用 BNB 买入 (Portal 有 access control)
  - PancakeSwap: 自动检测流动性池配对, 优先 BNB→Token, 无 WBNB 池时回退 USDT→Token
  - 买入金额以 USDT 计价, BNB 报价时自动换算

  flap 平台交易架构:
    - Portal (0xe2cE6ab...): 状态查询 (getTokenV5) + 事件发射, 不可直接调用 buy/sell
    - Router (0x296B001...): 所有 flap 代币的交易入口 (买入/卖出), 内部调用 Portal
    - Portal 对外部直接调用 buy()/sell() 返回 0xac5f6092 (access control)
    - Router swap 函数 selector: 0x27772d13 (未公开 ABI, 通过逆向链上交易得到)
    - 不管 quote token 是什么 (BNB/USD1/USDT), Router 统一用 BNB 支付/收取

卖出:
  - four.meme bonding curve: 卖出收到报价币 (BNB 或 USDT)
  - flap bonding curve: 通过 Router 卖出, 收到 BNB (gas 较高, 需 700k)
  - PancakeSwap: 自动检测流动性池配对, 优先 Token→WBNB→BNB, 无 WBNB 池时回退 Token→USDT
  - 1 分钟扫描一次持仓代币价格, 预留 5% 执行缓冲空间

  止盈止损策略 (v16: 回撤止盈 + 中点止盈 + 固定止损 + 5%预留缓冲):
    1. 盈利 15% 后触发回撤止盈跟踪:
       - 15%~30% 区间: 回撤 15% 止盈 (实际 10% 预留缓冲, 从最高价回撤 10% 即卖出)
       - 30% 以上: 中点止盈法 (价格 ≤ (最高价 + 买入价) / 2 时卖出)
    2. 固定止损: 亏损 40% 止损 (实际 35% 预留缓冲)
    3. 动能衰竭止盈: 持币数/流动性/进度 多指标同时恶化时止盈
    4. 超期清仓: 持仓超过 12h 且亏损 → 卖出
"""

from __future__ import annotations

import os
import json
import time
import logging
import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

log = logging.getLogger(__name__)

try:
    from error_handler import send_error_manually as _send_error
except ImportError:
    _send_error = None

# ===================================================================
#  常量 & 合约地址 (BSC Mainnet)
# ===================================================================
BSC_CHAIN_ID = 56
BSC_RPC_ENDPOINTS = [
    "https://bsc-dataseed.bnbchain.org",
    "https://bsc-dataseed-public.bnbchain.org",
    "https://bsc-dataseed.nariox.org",
    "https://bsc-dataseed.defibit.io",
]

PANCAKE_ROUTER_V2 = Web3.to_checksum_address("0x10ED43C718714eb63d5aA57B78B54704E256024E")
WBNB = Web3.to_checksum_address("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
USDT = Web3.to_checksum_address("0x55d398326f99059fF775485246999027B3197955")  # BSC-USD (USDT)
USDT_DECIMALS = 18

# four.meme 合约
FM_TOKEN_MANAGER_V2 = Web3.to_checksum_address("0x5c952063c7fc8610FFDB798152D69F0B9550762b")
FM_HELPER_V3 = Web3.to_checksum_address("0xF251F83e40a78868FcfA3FA4599Dad6494E46034")

MAX_UINT256 = 2**256 - 1
DEFAULT_GAS_SWAP = 500_000
DEFAULT_GAS_APPROVE = 100_000

# ===================================================================
#  ABI (最小化)
# ===================================================================
ROUTER_ABI = json.loads("""[
  {
    "name":"swapExactETHForTokensSupportingFeeOnTransferTokens",
    "type":"function","stateMutability":"payable",
    "inputs":[
      {"name":"amountOutMin","type":"uint256"},
      {"name":"path","type":"address[]"},
      {"name":"to","type":"address"},
      {"name":"deadline","type":"uint256"}
    ],"outputs":[]
  },
  {
    "name":"swapExactTokensForETHSupportingFeeOnTransferTokens",
    "type":"function","stateMutability":"nonpayable",
    "inputs":[
      {"name":"amountIn","type":"uint256"},
      {"name":"amountOutMin","type":"uint256"},
      {"name":"path","type":"address[]"},
      {"name":"to","type":"address"},
      {"name":"deadline","type":"uint256"}
    ],"outputs":[]
  },
  {
    "name":"swapExactTokensForTokensSupportingFeeOnTransferTokens",
    "type":"function","stateMutability":"nonpayable",
    "inputs":[
      {"name":"amountIn","type":"uint256"},
      {"name":"amountOutMin","type":"uint256"},
      {"name":"path","type":"address[]"},
      {"name":"to","type":"address"},
      {"name":"deadline","type":"uint256"}
    ],"outputs":[]
  },
  {
    "name":"getAmountsOut",
    "type":"function","stateMutability":"view",
    "inputs":[
      {"name":"amountIn","type":"uint256"},
      {"name":"path","type":"address[]"}
    ],
    "outputs":[{"name":"amounts","type":"uint256[]"}]
  },
  {
    "name":"WETH",
    "type":"function","stateMutability":"view",
    "inputs":[],"outputs":[{"name":"","type":"address"}]
  }
]""")

ERC20_ABI = json.loads("""[
  {
    "name":"approve","type":"function","stateMutability":"nonpayable",
    "inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],
    "outputs":[{"name":"","type":"bool"}]
  },
  {
    "name":"balanceOf","type":"function","stateMutability":"view",
    "inputs":[{"name":"account","type":"address"}],
    "outputs":[{"name":"","type":"uint256"}]
  },
  {
    "name":"decimals","type":"function","stateMutability":"view",
    "inputs":[],"outputs":[{"name":"","type":"uint8"}]
  },
  {
    "name":"allowance","type":"function","stateMutability":"view",
    "inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],
    "outputs":[{"name":"","type":"uint256"}]
  }
]""")

# four.meme TokenManager2 ABI (最小化)
FM_MANAGER_ABI = json.loads("""[
  {
    "name":"buyTokenAMAP",
    "type":"function","stateMutability":"payable",
    "inputs":[
      {"name":"token","type":"address"},
      {"name":"funds","type":"uint256"},
      {"name":"minAmount","type":"uint256"}
    ],"outputs":[]
  },
  {
    "name":"sellToken",
    "type":"function","stateMutability":"nonpayable",
    "inputs":[
      {"name":"token","type":"address"},
      {"name":"amount","type":"uint256"}
    ],"outputs":[]
  }
]""")

# four.meme TokenManagerHelper3 ABI (最小化)
FM_HELPER_ABI = json.loads("""[
  {
    "name":"getTokenInfo",
    "type":"function","stateMutability":"view",
    "inputs":[{"name":"token","type":"address"}],
    "outputs":[
      {"name":"version","type":"uint256"},
      {"name":"tokenManager","type":"address"},
      {"name":"quote","type":"address"},
      {"name":"lastPrice","type":"uint256"},
      {"name":"tradingFeeRate","type":"uint256"},
      {"name":"minTradingFee","type":"uint256"},
      {"name":"launchTime","type":"uint256"},
      {"name":"offers","type":"uint256"},
      {"name":"maxOffers","type":"uint256"},
      {"name":"funds","type":"uint256"},
      {"name":"maxFunds","type":"uint256"},
      {"name":"liquidityAdded","type":"bool"}
    ]
  },
  {
    "name":"tryBuy",
    "type":"function","stateMutability":"view",
    "inputs":[
      {"name":"token","type":"address"},
      {"name":"amount","type":"uint256"},
      {"name":"funds","type":"uint256"}
    ],
    "outputs":[
      {"name":"tokenManager","type":"address"},
      {"name":"quote","type":"address"},
      {"name":"estimatedAmount","type":"uint256"},
      {"name":"estimatedCost","type":"uint256"},
      {"name":"estimatedFee","type":"uint256"},
      {"name":"amountMsgValue","type":"uint256"},
      {"name":"amountApproval","type":"uint256"},
      {"name":"amountFunds","type":"uint256"}
    ]
  },
  {
    "name":"trySell",
    "type":"function","stateMutability":"view",
    "inputs":[
      {"name":"token","type":"address"},
      {"name":"amount","type":"uint256"}
    ],
    "outputs":[
      {"name":"tokenManager","type":"address"},
      {"name":"quote","type":"address"},
      {"name":"funds","type":"uint256"},
      {"name":"fee","type":"uint256"}
    ]
  }
]""")

# flap 合约地址 (BSC 链上代币发射平台, 来源: docs.flap.sh)
# Portal: 事件发射 + 交易合约 (buy/sell/previewBuy/previewSell/getTokenV5)
FLAP_PORTAL = Web3.to_checksum_address("0xe2cE6ab80874Fa9Fa2aAE65D277Dd6B8e65C9De0")
# Router: 税币 (7777) 必须通过 Router 交易, Portal 对税币有 access control
FLAP_ROUTER = Web3.to_checksum_address("0x296B00198Dc7eC3410e12da814D9267bB8dF506A")
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
# 0xEeee...eee 代表原生代币 (BNB), Router 合约使用此地址标识 native token
FLAP_NATIVE_TOKEN = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

# flap quote token 分类 (用于价格换算和支付方式判断)
# 零地址 = 原生 BNB; 稳定币 = USD 计价
FLAP_STABLE_QUOTES = {
    "0x55d398326f99059ff775485246999027b3197955",  # USDT
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
    "0x61a10e8556bed032ea176330e7f17d6a12a10000",  # USD1/lisUSD
}

# flap Portal ABI (最小化: buy/sell/previewBuy/previewSell + getTokenV5)
FLAP_PORTAL_ABI = json.loads("""[
  {
    "name":"buy",
    "type":"function","stateMutability":"payable",
    "inputs":[
      {"name":"token","type":"address"},
      {"name":"recipient","type":"address"},
      {"name":"minAmount","type":"uint256"}
    ],
    "outputs":[{"name":"amount","type":"uint256"}]
  },
  {
    "name":"sell",
    "type":"function","stateMutability":"nonpayable",
    "inputs":[
      {"name":"token","type":"address"},
      {"name":"amount","type":"uint256"},
      {"name":"minEth","type":"uint256"}
    ],
    "outputs":[{"name":"eth","type":"uint256"}]
  },
  {
    "name":"previewBuy",
    "type":"function","stateMutability":"view",
    "inputs":[
      {"name":"token","type":"address"},
      {"name":"eth","type":"uint256"}
    ],
    "outputs":[{"name":"amount","type":"uint256"}]
  },
  {
    "name":"previewSell",
    "type":"function","stateMutability":"view",
    "inputs":[
      {"name":"token","type":"address"},
      {"name":"amount","type":"uint256"}
    ],
    "outputs":[{"name":"eth","type":"uint256"}]
  }
]""")

# ===================================================================
DB_PATH = Path(__file__).parent / "tokens.db"


def _init_positions_db(conn: sqlite3.Connection):
    """在已有的 tokens.db 中创建 positions 表"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address  TEXT NOT NULL,
            token_name     TEXT,
            token_decimals INTEGER DEFAULT 18,
            buy_price_usd  REAL,
            buy_amount     TEXT,
            buy_bnb        REAL,
            buy_tx         TEXT,
            buy_time       INTEGER,
            max_price_usd  REAL,
            current_price  REAL,
            status         TEXT DEFAULT 'OPEN',
            sell_price_usd REAL,
            sell_tx        TEXT,
            sell_time      INTEGER,
            sell_reason    TEXT,
            pnl_pct        REAL,
            venue          TEXT DEFAULT 'PANCAKE',
            UNIQUE(token_address, buy_time)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pos_status ON positions(status)
    """)
    # 迁移: 添加 channel 列 (潜伏型 'quality' / 毕业通道 'graduated')
    try:
        conn.execute("ALTER TABLE positions ADD COLUMN channel TEXT DEFAULT 'quality'")
    except Exception:
        pass  # 列已存在
    # 动能跟踪表: 记录每个持仓的持币数/流动性/进度历史
    conn.execute("""
        CREATE TABLE IF NOT EXISTS momentum (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id  INTEGER NOT NULL,
            ts           INTEGER NOT NULL,
            holders      INTEGER,
            liquidity    REAL,
            progress     REAL,
            price        REAL,
            FOREIGN KEY (position_id) REFERENCES positions(id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_momentum_pos ON momentum(position_id)
    """)
    conn.commit()


# ===================================================================
#  Web3 连接
# ===================================================================
_w3: Optional[Web3] = None
_wallet_address: Optional[str] = None
_private_key: Optional[str] = None


def _init_web3(rpc_url: str | None = None) -> Web3:
    """初始化 Web3 连接, 支持自定义 RPC"""
    global _w3
    if rpc_url:
        endpoints = [rpc_url]
    else:
        endpoints = BSC_RPC_ENDPOINTS

    for ep in endpoints:
        try:
            w3 = Web3(Web3.HTTPProvider(ep, request_kwargs={"timeout": 10}))
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            if w3.is_connected():
                log.info("Web3 已连接: %s (区块 %d)", ep, w3.eth.block_number)
                _w3 = w3
                return w3
        except Exception as e:
            log.warning("RPC 连接失败 [%s]: %s", ep, e)

    raise ConnectionError("无法连接到任何 BSC RPC 节点")


def _load_wallet() -> tuple[str, str]:
    """从环境变量或配置加载钱包"""
    global _wallet_address, _private_key

    pk = os.environ.get("BSC_PRIVATE_KEY", "")
    if not pk:
        # 尝试从配置读取
        cfg_path = Path(__file__).parent / "config.json"
        if cfg_path.exists():
            with open(cfg_path, "r") as f:
                cfg = json.load(f)
            pk = cfg.get("trading", {}).get("private_key", "")

    if not pk:
        raise ValueError(
            "未配置钱包私钥。请设置环境变量 BSC_PRIVATE_KEY 或在 config.json 中配置 trading.private_key"
        )

    if not pk.startswith("0x"):
        pk = "0x" + pk

    account = Web3().eth.account.from_key(pk)
    _wallet_address = account.address
    _private_key = pk
    log.info("钱包地址: %s", _wallet_address)
    return _wallet_address, pk


def get_bnb_balance() -> float:
    """获取钱包 BNB 余额 (单位: BNB)"""
    if not _w3 or not _wallet_address:
        return 0.0
    balance_wei = _w3.eth.get_balance(_wallet_address)
    return float(Web3.from_wei(balance_wei, "ether"))


def get_usdt_balance() -> float:
    """获取钱包 USDT 余额 (单位: USDT)"""
    if not _w3 or not _wallet_address:
        return 0.0
    try:
        usdt_contract = _w3.eth.contract(address=USDT, abi=ERC20_ABI)
        balance = usdt_contract.functions.balanceOf(_wallet_address).call()
        return balance / (10 ** USDT_DECIMALS)
    except Exception as e:
        log.debug("get_usdt_balance: %s", e)
        return 0.0


# ===================================================================
#  价格查询
# ===================================================================
def get_token_price_bnb(token_address: str, amount_in_bnb: float = 0.001) -> float | None:
    """
    通过 PancakeSwap getAmountsOut 查询代币价格
    返回: 1 个代币值多少 BNB (None 表示查询失败)
    """
    if not _w3:
        return None
    try:
        router = _w3.eth.contract(address=PANCAKE_ROUTER_V2, abi=ROUTER_ABI)
        token_cs = Web3.to_checksum_address(token_address)
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)
        decimals = token_contract.functions.decimals().call()

        # 用少量 BNB 询价, 得到能买多少代币
        amount_in_wei = Web3.to_wei(amount_in_bnb, "ether")
        amounts = router.functions.getAmountsOut(
            amount_in_wei, [WBNB, token_cs]
        ).call()
        tokens_out = amounts[1]
        if tokens_out <= 0:
            return None

        # 1 token = (amount_in_bnb / tokens_out) * 10^decimals BNB
        price_bnb = (amount_in_bnb * (10 ** decimals)) / tokens_out
        return price_bnb
    except Exception as e:
        log.debug("get_token_price_bnb [%s]: %s", token_address[:16], e)
        return None


def get_token_price_usd(token_address: str, bnb_price_usd: float) -> float | None:
    """获取代币的 USD 价格"""
    price_bnb = get_token_price_bnb(token_address)
    if price_bnb is None:
        return None
    return price_bnb * bnb_price_usd


# ===================================================================
#  four.meme 合约交互层
# ===================================================================
def fm_get_token_info(token_address: str) -> dict | None:
    """
    通过 Helper3 查询代币信息, 判断交易场所
    返回: {"version", "tokenManager", "quote", "liquidityAdded", "offers", ...} 或 None
    """
    if not _w3:
        return None
    try:
        helper = _w3.eth.contract(address=FM_HELPER_V3, abi=FM_HELPER_ABI)
        token_cs = Web3.to_checksum_address(token_address)
        info = helper.functions.getTokenInfo(token_cs).call()
        return {
            "version": info[0],
            "tokenManager": info[1],
            "quote": info[2],
            "lastPrice": info[3],
            "tradingFeeRate": info[4],
            "minTradingFee": info[5],
            "launchTime": info[6],
            "offers": info[7],
            "maxOffers": info[8],
            "funds": info[9],
            "maxFunds": info[10],
            "liquidityAdded": info[11],
        }
    except Exception as e:
        log.debug("fm_get_token_info [%s]: %s", token_address[:16], e)
        return None


def _check_pancake_liquidity(token_address: str) -> bool:
    """
    检查代币在 PancakeSwap 上是否有流动性 (通过 Router 询价)
    同时检查 WBNB 和 USDT 两个池子, 任一有流动性即返回 True
    """
    if not _w3:
        return False
    token_cs = Web3.to_checksum_address(token_address)
    router = _w3.eth.contract(address=PANCAKE_ROUTER_V2, abi=ROUTER_ABI)
    # 先查 WBNB 池
    try:
        test_amount = Web3.to_wei(0.0001, "ether")  # 0.0001 BNB
        amounts = router.functions.getAmountsOut(test_amount, [WBNB, token_cs]).call()
        if amounts[-1] > 0:
            return True
    except Exception:
        pass
    # 再查 USDT 池 (flap 毕业代币常见 Token/USDT 池)
    try:
        test_usdt = int(0.1 * (10 ** USDT_DECIMALS))  # 0.1 USDT
        amounts = router.functions.getAmountsOut(test_usdt, [USDT, token_cs]).call()
        if amounts[-1] > 0:
            return True
    except Exception:
        pass
    return False


def detect_venue(token_address: str, source_hint: str = "") -> str:
    """
    检测代币当前的交易场所
    返回: 'BONDING' (four.meme bonding curve) | 'FLAP' (flap bonding curve) |
          'PANCAKE' (已迁移到 PancakeSwap) | 'UNKNOWN'
    source_hint: 可选来源提示 ("flap" / "four.meme"), 优先查对应平台, 减少无效 RPC 调用

    容错机制:
    - Portal/Helper RPC 查询可能因网络抖动返回 None, 增加 1 次重试 (间隔 2s)
    - 重试后仍查不到, 但 source_hint + 地址后缀能确认平台 → 信任来源, 回退到对应通道
      (代币是从链上 TokenCreated 事件发现的, source 标记可靠)
    """
    addr_lower = token_address.lower()

    if source_hint == "flap":
        # flap 代币: 只查 flap Portal, 两个平台完全独立
        state = flap_get_token_state(token_address)
        if state is not None:
            if state["graduated"]:
                return "PANCAKE"
            if state["status"] in (1, 2):  # Tradable / InDuel
                return "FLAP"
            # status=3 (Killed) 等异常状态, 不重试, 直接 UNKNOWN
            log.warning("flap 代币 %s 状态异常 (status=%d)", token_address[:16], state["status"])
            return "UNKNOWN"
        # Portal 查不到 (state is None): 可能是网络抖动, 重试 1 次
        log.info("flap 代币 %s Portal 首次查询无结果, 2s 后重试...", token_address[:16])
        time.sleep(2)
        state = flap_get_token_state(token_address)
        if state is not None:
            if state["graduated"]:
                return "PANCAKE"
            if state["status"] in (1, 2):
                return "FLAP"
            log.warning("flap 代币 %s 重试后状态异常 (status=%d)", token_address[:16], state["status"])
            return "UNKNOWN"
        # 重试后仍查不到 → 检查 PancakeSwap 流动性
        if _check_pancake_liquidity(token_address):
            log.info("flap 代币 %s Portal 查不到, PancakeSwap 有流动性 → PANCAKE",
                     token_address[:16])
            return "PANCAKE"
        # 兜底: source_hint=flap + 地址后缀确认 → 信任来源, 回退到 FLAP 通道
        if addr_lower.endswith("7777") or addr_lower.endswith("8888"):
            log.warning("flap 代币 %s Portal 查不到, 但地址后缀确认是 flap → 回退 FLAP 通道",
                        token_address[:16])
            return "FLAP"
        return "UNKNOWN"

    if source_hint == "four.meme":
        # four.meme 代币: 只查 four.meme Helper
        info = fm_get_token_info(token_address)
        if info is not None:
            if info["liquidityAdded"]:
                return "PANCAKE"
            if info["offers"] > 0:
                return "BONDING"
            if _check_pancake_liquidity(token_address):
                log.info("four.meme 代币 %s 状态不明, PancakeSwap 有流动性 → PANCAKE",
                         token_address[:16])
                return "PANCAKE"
            return "UNKNOWN"
        # Helper 查不到: 重试 1 次
        log.info("four.meme 代币 %s Helper 首次查询无结果, 2s 后重试...", token_address[:16])
        time.sleep(2)
        info = fm_get_token_info(token_address)
        if info is not None:
            if info["liquidityAdded"]:
                return "PANCAKE"
            if info["offers"] > 0:
                return "BONDING"
            if _check_pancake_liquidity(token_address):
                return "PANCAKE"
            return "UNKNOWN"
        # 重试后仍查不到 → 检查 PancakeSwap
        if _check_pancake_liquidity(token_address):
            log.info("four.meme 代币 %s Helper 查不到, PancakeSwap 有流动性 → PANCAKE",
                     token_address[:16])
            return "PANCAKE"
        # 兜底: source_hint=four.meme + 地址后缀确认 → 信任来源, 回退到 BONDING 通道
        if addr_lower.endswith("4444") or addr_lower.endswith("ffff"):
            log.warning("four.meme 代币 %s Helper 查不到, 但地址后缀确认是 four.meme → 回退 BONDING 通道",
                        token_address[:16])
            return "BONDING"
        return "UNKNOWN"

    # 无 source_hint: 先查 four.meme, 查不到再查 flap
    info = fm_get_token_info(token_address)
    if info is not None:
        if info["liquidityAdded"]:
            return "PANCAKE"
        if info["offers"] > 0:
            return "BONDING"
        if _check_pancake_liquidity(token_address):
            log.info("four.meme 代币 %s 状态不明, PancakeSwap 有流动性 → PANCAKE",
                     token_address[:16])
            return "PANCAKE"
        return "UNKNOWN"

    # four.meme 查不到, 尝试 flap
    state = flap_get_token_state(token_address)
    if state is not None:
        if state["graduated"]:
            return "PANCAKE"
        if state["status"] in (1, 2):  # Tradable / InDuel
            return "FLAP"
        return "UNKNOWN"

    # 两个平台都查不到 → 最后兜底检查 PancakeSwap
    if _check_pancake_liquidity(token_address):
        log.info("代币 %s 平台未知, PancakeSwap 有流动性 → PANCAKE",
                 token_address[:16])
        return "PANCAKE"

    return "UNKNOWN"


def fm_buy_token(token_address: str, buy_usdt: float, slippage_pct: float = 12.0,
                 token_name: str = "", bnb_price_usd: float = 600.0) -> dict | None:
    """
    通过 four.meme bonding curve 买入代币
    自动检测报价币 (quote):
      - quote=WBNB: 用 BNB 直接买, 金额以 USDT 计价后换算为 BNB
      - quote=USDT: 直接用 USDT 买
      - 其他/未知: 默认用 BNB 买
    """
    if not _w3 or not _wallet_address or not _private_key:
        log.error("Web3/钱包未初始化")
        return None

    token_cs = Web3.to_checksum_address(token_address)

    try:
        # 获取代币信息, 判断报价币
        info = fm_get_token_info(token_address)
        if info is None:
            log.error("无法获取代币信息: %s", token_name)
            return None

        manager_addr = info["tokenManager"]
        if manager_addr == "0x" + "0" * 40:
            manager_addr = FM_TOKEN_MANAGER_V2

        quote_addr = (info.get("quote") or "").lower()
        is_usdt_quote = (quote_addr == USDT.lower())

        if is_usdt_quote:
            # ===== USDT 报价: 直接用 USDT 买 =====
            log.info("bonding curve [USDT报价] 买入 %s: $%.2f USDT", token_name, buy_usdt)

            # 检查 USDT 余额
            usdt_balance = get_usdt_balance()
            if usdt_balance < buy_usdt:
                log.error("USDT 余额不足: 需要 %.2f USDT, 当前 %.2f USDT",
                          buy_usdt, usdt_balance)
                return None

            amount_in_wei = int(buy_usdt * (10 ** USDT_DECIMALS))

            # Approve USDT 给 TokenManager
            usdt_contract = _w3.eth.contract(address=USDT, abi=ERC20_ABI)
            manager_cs = Web3.to_checksum_address(manager_addr)
            allowance = usdt_contract.functions.allowance(_wallet_address, manager_cs).call()
            if allowance < amount_in_wei:
                log.info("Approve USDT to TokenManager...")
                nonce = _w3.eth.get_transaction_count(_wallet_address)
                approve_tx = usdt_contract.functions.approve(
                    manager_cs, MAX_UINT256
                ).build_transaction({
                    "from": _wallet_address, "gas": DEFAULT_GAS_APPROVE,
                    "gasPrice": _w3.eth.gas_price, "nonce": nonce, "chainId": BSC_CHAIN_ID,
                })
                signed = _w3.eth.account.sign_transaction(approve_tx, _private_key)
                tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                if receipt["status"] != 1:
                    log.error("USDT Approve 失败")
                    return None
                time.sleep(2)

            # tryBuy 预估
            helper = _w3.eth.contract(address=FM_HELPER_V3, abi=FM_HELPER_ABI)
            try:
                est = helper.functions.tryBuy(token_cs, 0, amount_in_wei).call()
                estimated_amount = est[2]
                actual_msg_value = est[5]  # USDT 报价时 msg.value 应为 0
                actual_funds = est[7] if est[7] > 0 else amount_in_wei
                min_amount = int(estimated_amount * (1 - slippage_pct / 100))
                log.info("bonding curve 预估: 花费 %.2f USDT → %s 代币", buy_usdt, estimated_amount)
            except Exception as e:
                log.warning("tryBuy 预估失败: %s, 使用 minAmount=0", e)
                actual_msg_value = 0
                actual_funds = amount_in_wei
                min_amount = 0

            buy_bnb = 0.0  # USDT 报价不消耗 BNB (除 gas)

        else:
            # ===== BNB 报价 (默认): 用 BNB 直接买 =====
            buy_bnb = buy_usdt / bnb_price_usd
            log.info("bonding curve [BNB报价] 买入 %s: $%.2f → %.6f BNB (BNB价格 $%.2f)",
                     token_name, buy_usdt, buy_bnb, bnb_price_usd)

            # 检查 BNB 余额
            bnb_balance = get_bnb_balance()
            gas_reserve = 0.002
            if bnb_balance < buy_bnb + gas_reserve:
                log.error("BNB 余额不足: 需要 %.6f BNB (买入 %.6f + gas), 当前 %.6f BNB",
                          buy_bnb + gas_reserve, buy_bnb, bnb_balance)
                return None

            amount_in_wei = Web3.to_wei(buy_bnb, "ether")

            # tryBuy 预估
            helper = _w3.eth.contract(address=FM_HELPER_V3, abi=FM_HELPER_ABI)
            try:
                est = helper.functions.tryBuy(token_cs, 0, amount_in_wei).call()
                estimated_amount = est[2]
                actual_msg_value = est[5] if est[5] > 0 else amount_in_wei
                actual_funds = est[7] if est[7] > 0 else amount_in_wei
                min_amount = int(estimated_amount * (1 - slippage_pct / 100))
                log.info("bonding curve 预估: 花费 %.6f BNB → %s 代币", buy_bnb, estimated_amount)
            except Exception as e:
                log.warning("tryBuy 预估失败: %s, 使用 minAmount=0", e)
                actual_msg_value = amount_in_wei
                actual_funds = amount_in_wei
                min_amount = 0

        # ===== 执行买入 =====
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)
        manager = _w3.eth.contract(
            address=Web3.to_checksum_address(manager_addr), abi=FM_MANAGER_ABI,
        )

        balance_before = token_contract.functions.balanceOf(_wallet_address).call()

        nonce = _w3.eth.get_transaction_count(_wallet_address)
        tx = manager.functions.buyTokenAMAP(
            token_cs, actual_funds, min_amount,
        ).build_transaction({
            "from": _wallet_address, "value": actual_msg_value,
            "gas": DEFAULT_GAS_SWAP, "gasPrice": _w3.eth.gas_price,
            "nonce": nonce, "chainId": BSC_CHAIN_ID,
        })

        signed = _w3.eth.account.sign_transaction(tx, _private_key)
        tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("bonding curve 买入 TX: %s", tx_hash.hex())

        receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] != 1:
            log.error("bonding curve 买入失败: %s", tx_hash.hex())
            return None

        balance_after = token_contract.functions.balanceOf(_wallet_address).call()
        actual_received = balance_after - balance_before
        decimals = token_contract.functions.decimals().call()

        if actual_received <= 0:
            log.error("bonding curve 买入异常: TX 成功但未收到代币 %s (TX: %s)",
                      token_name or token_address[:16], tx_hash.hex())
            return None

        buy_price_usd = buy_usdt / (actual_received / 10**decimals)
        quote_label = "USDT" if is_usdt_quote else "BNB"

        log.info("bonding curve 买入成功 %s [%s报价]: $%.2f → %s 代币, 单价 $%.12f",
                 token_name or token_address[:16], quote_label, buy_usdt,
                 actual_received, buy_price_usd)

        return {
            "tx_hash": tx_hash.hex(),
            "token_amount": str(actual_received),
            "decimals": decimals,
            "buy_price_usd": buy_price_usd,
            "buy_bnb": buy_bnb,
            "venue": "BONDING",
        }

    except Exception as e:
        log.error("bonding curve 买入异常 [%s]: %s", token_name or token_address[:16], e)
        return None


def fm_sell_token(token_address: str, amount: int | None = None,
                  token_name: str = "", bnb_price_usd: float = 600.0) -> dict | None:
    """
    通过 four.meme bonding curve 卖出代币
    自动检测报价币 (quote):
      - quote=WBNB: 卖出收 BNB
      - quote=USDT: 卖出收 USDT
    """
    if not _w3 or not _wallet_address or not _private_key:
        log.error("Web3/钱包未初始化")
        return None

    token_cs = Web3.to_checksum_address(token_address)

    try:
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)

        if amount is None:
            amount = token_contract.functions.balanceOf(_wallet_address).call()
        if amount <= 0:
            log.warning("无代币可卖: %s", token_name or token_address[:16])
            return None

        # 获取 tokenManager 地址和报价币
        info = fm_get_token_info(token_address)
        if info is None:
            log.error("无法获取代币信息: %s", token_name)
            return None

        # 如果已迁移, 应该用 PancakeSwap 卖出
        if info["liquidityAdded"]:
            log.info("%s 已迁移到 PancakeSwap, 切换卖出方式", token_name)
            return sell_token(token_address, amount, token_name=token_name,
                              bnb_price_usd=bnb_price_usd)

        manager_addr = info["tokenManager"]
        if manager_addr == "0x" + "0" * 40:
            manager_addr = FM_TOKEN_MANAGER_V2
        manager_cs = Web3.to_checksum_address(manager_addr)

        quote_addr = (info.get("quote") or "").lower()
        is_usdt_quote = (quote_addr == USDT.lower())

        # Approve TokenManager
        allowance = token_contract.functions.allowance(
            _wallet_address, manager_cs
        ).call()
        if allowance < amount:
            log.info("Approve TokenManager: %s", token_name)
            nonce = _w3.eth.get_transaction_count(_wallet_address)
            approve_tx = token_contract.functions.approve(
                manager_cs, MAX_UINT256
            ).build_transaction({
                "from": _wallet_address,
                "gas": DEFAULT_GAS_APPROVE,
                "gasPrice": _w3.eth.gas_price,
                "nonce": nonce,
                "chainId": BSC_CHAIN_ID,
            })
            signed = _w3.eth.account.sign_transaction(approve_tx, _private_key)
            tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt["status"] != 1:
                log.error("Approve 失败: %s", tx_hash.hex())
                return None
            time.sleep(2)

        # 记录卖出前余额 (根据报价币类型)
        if is_usdt_quote:
            usdt_contract = _w3.eth.contract(address=USDT, abi=ERC20_ABI)
            balance_before = usdt_contract.functions.balanceOf(_wallet_address).call()
        else:
            balance_before = _w3.eth.get_balance(_wallet_address)

        manager = _w3.eth.contract(address=manager_cs, abi=FM_MANAGER_ABI)
        nonce = _w3.eth.get_transaction_count(_wallet_address)
        tx = manager.functions.sellToken(
            token_cs,
            amount,
        ).build_transaction({
            "from": _wallet_address,
            "gas": DEFAULT_GAS_SWAP,
            "gasPrice": _w3.eth.gas_price,
            "nonce": nonce,
            "chainId": BSC_CHAIN_ID,
        })

        signed = _w3.eth.account.sign_transaction(tx, _private_key)
        tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("bonding curve 卖出 TX: %s", tx_hash.hex())

        receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] != 1:
            log.error("bonding curve 卖出失败: %s", tx_hash.hex())
            return None

        # 计算收到的金额
        if is_usdt_quote:
            balance_after = usdt_contract.functions.balanceOf(_wallet_address).call()
            usdt_received = (balance_after - balance_before) / (10 ** USDT_DECIMALS)
            log.info("bonding curve 卖出成功 %s [USDT报价]: 收回 %.4f USDT",
                     token_name or token_address[:16], usdt_received)
            return {
                "tx_hash": tx_hash.hex(),
                "usdt_received": usdt_received,
            }
        else:
            balance_after = _w3.eth.get_balance(_wallet_address)
            gas_cost = receipt["gasUsed"] * receipt["effectiveGasPrice"]
            bnb_received = float(Web3.from_wei(balance_after - balance_before + gas_cost, "ether"))
            usdt_received = bnb_received * bnb_price_usd
            log.info("bonding curve 卖出成功 %s [BNB报价]: 收回 %.6f BNB ($%.4f)",
                     token_name or token_address[:16], bnb_received, usdt_received)
            return {
                "tx_hash": tx_hash.hex(),
                "usdt_received": usdt_received,
                "bnb_received": bnb_received,
            }

    except Exception as e:
        log.error("bonding curve 卖出异常 [%s]: %s", token_name or token_address[:16], e)
        return None


# ===================================================================
#  flap Bonding Curve 买卖
# ===================================================================
def flap_get_token_state(token_address: str) -> dict | None:
    """
    通过 RPC eth_call 调用 flap Portal 合约 getTokenV5(address) 读取代币状态
    返回: {"status", "reserve", "price_native", "quote_token", "graduated", "progress"} 或 None
    """
    if not _w3:
        return None
    try:
        # getTokenV5(address) selector: 0x5c4bc504
        token_cs = Web3.to_checksum_address(token_address)
        call_data = "0x5c4bc504" + token_cs[2:].lower().zfill(64)
        result = _w3.eth.call({
            "to": FLAP_PORTAL,
            "data": call_data,
        })
        raw = result.hex()
        if len(raw) < 576:
            return None
        words = [int(raw[i:i+64], 16) for i in range(0, min(len(raw), 768), 64)]

        status = words[0]       # 0=Invalid, 1=Tradable, 2=InDuel, 3=Killed, 4=DEX
        reserve_wei = words[1]
        price_wei = words[3]

        if status == 0:  # Invalid — 不是 flap 代币
            return None

        reserve_native = reserve_wei / 1e18
        price_native = price_wei / 1e18
        graduated = (status == 4)

        # 解析 quote token 地址 (word[9])
        quote_token = ZERO_ADDRESS
        if len(words) > 9 and len(raw) >= 640:
            qt_hex = raw[9 * 64 + 24: 9 * 64 + 64]
            quote_token = ("0x" + qt_hex).lower()

        # 计算进度
        progress = 0.0
        if graduated:
            progress = 1.0
        elif len(words) > 8 and words[5] > 0 and words[7] > 0 and words[8] > 0:
            r_virtual = words[5] / 1e18
            h_tokens = words[6] / 1e18
            K = words[7] / 1e18
            target_supply = words[8] / 1e18
            x_target = 1e9 - target_supply
            denominator = x_target + h_tokens
            if denominator > 0:
                target_reserve = K / denominator - r_virtual
                if target_reserve > 0:
                    progress = min(reserve_native / target_reserve, 1.0)

        return {
            "status": status,
            "reserve": reserve_native,
            "price_native": price_native,
            "quote_token": quote_token,
            "graduated": graduated,
            "progress": progress,
        }
    except Exception as e:
        log.debug("flap_get_token_state [%s]: %s", token_address[:16], e)
        return None


def _flap_router_buy(token_address: str, buy_usdt: float, slippage_pct: float = 12.0,
                     token_name: str = "", bnb_price_usd: float = 600.0) -> dict | None:
    """
    通过 flap Router 合约买入税币 (7777 后缀)
    Portal 对税币有 access control, 必须通过 Router 中转。
    Router 函数 selector: 0x27772d13 (未公开 ABI, 通过逆向链上交易得到)
    参数结构 (ABI-encoded struct):
      tokenIn: address (0xEeee...eee = native BNB)
      tokenOut: address (目标代币)
      amountIn: uint256 (BNB 金额 wei)
      minAmountOut: uint256 (最小输出, 不能为 0)
      ... routing info (WBNB, token, Portal 地址)
    """
    token_cs = Web3.to_checksum_address(token_address)
    try:
        # 查询代币状态
        state = flap_get_token_state(token_address)
        if state is None:
            log.error("flap router: 无法获取代币状态: %s", token_name)
            return None
        if state["graduated"]:
            log.info("flap router: %s 已毕业, 切换到 PancakeSwap 买入", token_name)
            return buy_token(token_address, buy_usdt, slippage_pct, token_name, bnb_price_usd)

        buy_bnb = buy_usdt / bnb_price_usd
        buy_wei = Web3.to_wei(buy_bnb, "ether")

        # 检查 BNB 余额
        bnb_balance = get_bnb_balance()
        gas_reserve = 0.002
        if bnb_balance < buy_bnb + gas_reserve:
            log.error("flap router: BNB 余额不足: 需要 %.6f BNB, 当前 %.6f BNB",
                      buy_bnb + gas_reserve, bnb_balance)
            return None

        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)

        # 构造 Router calldata (逆向自链上成功交易)
        # selector: 0x27772d13
        # struct 参数: offset(32) + struct fields
        selector = "27772d13"
        native = FLAP_NATIVE_TOKEN[2:].lower().zfill(64)
        token_hex = token_cs[2:].lower().zfill(64)
        amount_hex = hex(buy_wei)[2:].zfill(64)
        # minAmountOut = 1 (Router 不允许 0, 实际滑点由链上价格决定)
        min_out_hex = hex(1)[2:].zfill(64)
        wbnb_hex = WBNB[2:].lower().zfill(64)
        portal_hex = FLAP_PORTAL[2:].lower().zfill(64)
        recipient_hex = _wallet_address[2:].lower().zfill(64)
        zero_hex = "0" * 64

        # 完整 calldata 结构 (从链上成功交易逆向):
        # [0] offset to struct = 32
        # struct:
        #   [0] tokenIn (native)
        #   [1] tokenOut (target)
        #   [2] amountIn
        #   [3] minAmountOut
        #   [4] deadline (0)
        #   [5] 0x50 = 80 (offset to recipient within struct)
        #   [6] recipient
        #   [7] 0
        #   [8] 0
        #   [9] 0x180 = 384 (offset to route array)
        #   [10] 0x1a0 = 416 (offset)
        #   [11] 0x1c0 = 448 (offset)
        #   [12] 0
        #   [13] 0
        #   [14] 1 (route length)
        #   [15] 0x20 = 32 (offset to route[0])
        #   [16] 0
        #   [17] WBNB
        #   [18] tokenOut
        #   [19] Portal
        #   [20] 0
        #   [21] 0xc0 = 192 (offset)
        #   [22] 0
        calldata = selector
        calldata += hex(32)[2:].zfill(64)  # offset to struct
        calldata += native                  # [0] tokenIn
        calldata += token_hex               # [1] tokenOut
        calldata += amount_hex              # [2] amountIn
        calldata += min_out_hex             # [3] minAmountOut
        calldata += zero_hex                # [4] deadline = 0
        calldata += hex(80)[2:].zfill(64)   # [5] offset 0x50
        calldata += recipient_hex           # [6] recipient
        calldata += zero_hex                # [7] 0
        calldata += zero_hex                # [8] 0
        calldata += hex(384)[2:].zfill(64)  # [9] offset 0x180
        calldata += hex(416)[2:].zfill(64)  # [10] offset 0x1a0
        calldata += hex(448)[2:].zfill(64)  # [11] offset 0x1c0
        calldata += zero_hex                # [12] 0
        calldata += zero_hex                # [13] 0
        calldata += hex(1)[2:].zfill(64)    # [14] route length = 1
        calldata += hex(32)[2:].zfill(64)   # [15] offset to route[0]
        calldata += zero_hex                # [16] 0
        calldata += wbnb_hex                # [17] WBNB
        calldata += token_hex               # [18] tokenOut
        calldata += portal_hex              # [19] Portal
        calldata += zero_hex                # [20] 0
        calldata += hex(192)[2:].zfill(64)  # [21] offset 0xc0
        calldata += zero_hex                # [22] 0

        # 先模拟
        try:
            result = _w3.eth.call({
                "from": _wallet_address,
                "to": FLAP_ROUTER,
                "data": "0x" + calldata,
                "value": buy_wei,
            })
            estimated = int(result.hex(), 16)
            log.info("flap router 预估: %.6f BNB → %s 代币", buy_bnb, estimated)
        except Exception as e:
            log.warning("flap router 模拟失败: %s, 继续尝试发送", e)
            estimated = 0

        # 执行买入
        balance_before = token_contract.functions.balanceOf(_wallet_address).call()

        nonce = _w3.eth.get_transaction_count(_wallet_address)
        tx = {
            "from": _wallet_address,
            "to": FLAP_ROUTER,
            "data": "0x" + calldata,
            "value": buy_wei,
            "gas": 500_000,
            "gasPrice": _w3.eth.gas_price,
            "nonce": nonce,
            "chainId": BSC_CHAIN_ID,
        }

        signed = _w3.eth.account.sign_transaction(tx, _private_key)
        tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("flap router 买入 TX: %s", tx_hash.hex())

        receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] != 1:
            log.error("flap router 买入失败: %s", tx_hash.hex())
            return None

        balance_after = token_contract.functions.balanceOf(_wallet_address).call()
        actual_received = balance_after - balance_before
        decimals = token_contract.functions.decimals().call()

        if actual_received <= 0:
            log.error("flap router 买入异常: TX 成功但未收到代币 %s (TX: %s)",
                      token_name or token_address[:16], tx_hash.hex())
            return None

        buy_price_usd = buy_usdt / (actual_received / 10**decimals)

        log.info("flap router 买入成功 %s: $%.2f (%.6f BNB) → %s 代币, 单价 $%.12f",
                 token_name or token_address[:16], buy_usdt, buy_bnb,
                 actual_received, buy_price_usd)

        return {
            "tx_hash": tx_hash.hex(),
            "token_amount": str(actual_received),
            "decimals": decimals,
            "buy_price_usd": buy_price_usd,
            "buy_bnb": buy_bnb,
            "venue": "FLAP",
        }

    except Exception as e:
        log.error("flap router 买入异常 [%s]: %s", token_name or token_address[:16], e)
        return None


def _flap_router_sell(token_address: str, amount: int,
                      token_name: str = "", bnb_price_usd: float = 600.0) -> dict | None:
    """
    通过 flap Router 合约卖出税币 (7777 后缀)
    Router 函数 selector: 0x27772d13 (与买入相同的 swap 函数, tokenIn/tokenOut 互换)
    """
    token_cs = Web3.to_checksum_address(token_address)
    try:
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)
        decimals = token_contract.functions.decimals().call()

        # Approve Router 使用代币
        allowance = token_contract.functions.allowance(_wallet_address, FLAP_ROUTER).call()
        if allowance < amount:
            log.info("flap router: Approve 代币 to Router: %s", token_name)
            nonce = _w3.eth.get_transaction_count(_wallet_address)
            approve_tx = token_contract.functions.approve(
                FLAP_ROUTER, MAX_UINT256
            ).build_transaction({
                "from": _wallet_address, "gas": DEFAULT_GAS_APPROVE,
                "gasPrice": _w3.eth.gas_price, "nonce": nonce, "chainId": BSC_CHAIN_ID,
            })
            signed = _w3.eth.account.sign_transaction(approve_tx, _private_key)
            tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt["status"] != 1:
                log.error("flap router: 代币 Approve 失败")
                return None
            time.sleep(2)

        # 构造 Router sell calldata (与 buy 结构相同, tokenIn/tokenOut 互换)
        # 卖出: tokenIn = 代币, tokenOut = NATIVE (BNB)
        selector = "27772d13"
        token_hex = token_cs[2:].lower().zfill(64)
        native = FLAP_NATIVE_TOKEN[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        min_out_hex = hex(1)[2:].zfill(64)  # minAmountOut = 1
        wbnb_hex = WBNB[2:].lower().zfill(64)
        portal_hex = FLAP_PORTAL[2:].lower().zfill(64)
        recipient_hex = _wallet_address[2:].lower().zfill(64)
        zero_hex = "0" * 64

        calldata = selector
        calldata += hex(32)[2:].zfill(64)   # offset to struct
        calldata += token_hex               # [0] tokenIn = 代币
        calldata += native                  # [1] tokenOut = NATIVE (BNB)
        calldata += amount_hex              # [2] amountIn = 卖出数量
        calldata += min_out_hex             # [3] minAmountOut = 1
        calldata += zero_hex                # [4] deadline = 0
        calldata += hex(80)[2:].zfill(64)   # [5] offset 0x50
        calldata += recipient_hex           # [6] recipient
        calldata += zero_hex                # [7] 0
        calldata += zero_hex                # [8] 0
        calldata += hex(384)[2:].zfill(64)  # [9] offset 0x180
        calldata += hex(416)[2:].zfill(64)  # [10] offset 0x1a0
        calldata += hex(448)[2:].zfill(64)  # [11] offset 0x1c0
        calldata += zero_hex                # [12] 0
        calldata += zero_hex                # [13] 0
        calldata += hex(1)[2:].zfill(64)    # [14] route length = 1
        calldata += hex(32)[2:].zfill(64)   # [15] offset to route[0]
        calldata += zero_hex                # [16] 0
        calldata += token_hex               # [17] tokenIn (代币)
        calldata += wbnb_hex                # [18] WBNB
        calldata += portal_hex              # [19] Portal
        calldata += zero_hex                # [20] 0
        calldata += hex(192)[2:].zfill(64)  # [21] offset 0xc0
        calldata += zero_hex                # [22] 0

        # 模拟
        try:
            result = _w3.eth.call({
                "from": _wallet_address,
                "to": FLAP_ROUTER,
                "data": "0x" + calldata,
                "value": 0,
            })
            estimated_bnb = int(result.hex(), 16)
            log.info("flap router 预估卖出: %s 代币 → %s wei BNB",
                     amount, estimated_bnb)
        except Exception as e:
            log.warning("flap router 卖出模拟失败: %s", e)
            estimated_bnb = 0

        # 记录卖出前 BNB 余额
        balance_before = _w3.eth.get_balance(_wallet_address)

        # 执行卖出 (msg.value = 0, 卖代币收 BNB)
        nonce = _w3.eth.get_transaction_count(_wallet_address)
        tx = {
            "from": _wallet_address,
            "to": FLAP_ROUTER,
            "data": "0x" + calldata,
            "value": 0,
            "gas": 700_000,  # 卖出 gas 消耗较高 (~570k), 需要充足余量
            "gasPrice": _w3.eth.gas_price,
            "nonce": nonce,
            "chainId": BSC_CHAIN_ID,
        }

        signed = _w3.eth.account.sign_transaction(tx, _private_key)
        tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("flap router 卖出 TX: %s", tx_hash.hex())

        receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] != 1:
            log.error("flap router 卖出失败: %s", tx_hash.hex())
            return None

        # 计算收到的 BNB
        balance_after = _w3.eth.get_balance(_wallet_address)
        gas_cost = receipt["gasUsed"] * receipt["effectiveGasPrice"]
        bnb_received = float(Web3.from_wei(balance_after - balance_before + gas_cost, "ether"))
        usdt_received = bnb_received * bnb_price_usd

        log.info("flap router 卖出成功 %s: %d 代币 → %.6f BNB ($%.4f)",
                 token_name or token_address[:16], amount, bnb_received, usdt_received)

        return {
            "tx_hash": tx_hash.hex(),
            "usdt_received": usdt_received,
            "bnb_received": bnb_received,
        }

    except Exception as e:
        log.error("flap router 卖出异常 [%s]: %s", token_name or token_address[:16], e)
        return None


def flap_buy_token(token_address: str, buy_usdt: float, slippage_pct: float = 12.0,
                   token_name: str = "", bnb_price_usd: float = 600.0) -> dict | None:
    """
    通过 flap Router 合约买入代币 (所有 flap 代币统一走 Router)
    Portal 对外部直接调用有 access control (error 0xac5f6092), 必须通过 Router 中转。
    Router 内部调用 Portal 完成实际交易, TokenBought 事件仍从 Portal 发出。
    """
    if not _w3 or not _wallet_address or not _private_key:
        log.error("Web3/钱包未初始化")
        return None
    return _flap_router_buy(token_address, buy_usdt, slippage_pct,
                            token_name, bnb_price_usd)


def flap_sell_token(token_address: str, amount: int | None = None,
                    token_name: str = "", bnb_price_usd: float = 600.0) -> dict | None:
    """
    通过 flap Router 合约卖出代币 (所有 flap 代币统一走 Router)
    Portal 对外部直接调用有 access control (error 0xac5f6092), 必须通过 Router 中转。
    卖出 gas 消耗较高 (~570k), 需要设置 gas limit ≥ 700000。
    """
    if not _w3 or not _wallet_address or not _private_key:
        log.error("Web3/钱包未初始化")
        return None

    token_cs = Web3.to_checksum_address(token_address)

    try:
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)

        if amount is None:
            amount = token_contract.functions.balanceOf(_wallet_address).call()
        if amount <= 0:
            log.warning("flap: 无代币可卖: %s", token_name or token_address[:16])
            return None

        # 查询 flap 代币状态
        state = flap_get_token_state(token_address)
        if state is None:
            log.warning("flap: 无法获取代币状态, 尝试 PancakeSwap 卖出: %s", token_name)
            return sell_token(token_address, amount, token_name=token_name,
                              bnb_price_usd=bnb_price_usd)
        if state["graduated"]:
            log.info("flap: %s 已毕业, 切换到 PancakeSwap 卖出", token_name)
            return sell_token(token_address, amount, token_name=token_name,
                              bnb_price_usd=bnb_price_usd)

        # 所有 flap 代币统一走 Router
        return _flap_router_sell(token_address, amount, token_name, bnb_price_usd)

    except Exception as e:
        log.error("flap 卖出异常 [%s]: %s", token_name or token_address[:16], e)
        return None


def get_token_price_flap(token_address: str, bnb_price_usd: float = 600.0) -> float | None:
    """
    通过 flap Portal previewBuy 查询 bonding curve 上的代币价格
    返回: 1 个代币值多少 USD
    """
    if not _w3:
        return None
    try:
        state = flap_get_token_state(token_address)
        if state is None:
            return None
        # 直接用链上价格换算
        price_native = state["price_native"]
        if price_native <= 0:
            return None
        quote_token = state["quote_token"]
        qt = quote_token.lower()
        if qt == ZERO_ADDRESS or qt == ("0x" + "0" * 40):
            return price_native * bnb_price_usd
        if qt in FLAP_STABLE_QUOTES:
            return price_native  # 已是 USD 计价
        return None  # 非标 quote token, 无法换算
    except Exception as e:
        log.debug("get_token_price_flap [%s]: %s", token_address[:16], e)
        return None


def get_token_price_bnb_bonding(token_address: str,
                                 amount_in_bnb: float = 0.001) -> float | None:
    """
    通过 four.meme Helper3.tryBuy 查询 bonding curve 上的代币价格
    返回: 1 个代币值多少 BNB
    """
    if not _w3:
        return None
    try:
        helper = _w3.eth.contract(address=FM_HELPER_V3, abi=FM_HELPER_ABI)
        token_cs = Web3.to_checksum_address(token_address)
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)
        decimals = token_contract.functions.decimals().call()

        amount_in_wei = Web3.to_wei(amount_in_bnb, "ether")
        est = helper.functions.tryBuy(token_cs, 0, amount_in_wei).call()
        estimated_amount = est[2]
        if estimated_amount <= 0:
            return None
        price_bnb = (amount_in_bnb * (10 ** decimals)) / estimated_amount
        return price_bnb
    except Exception as e:
        log.debug("get_token_price_bnb_bonding [%s]: %s", token_address[:16], e)
        return None


def get_token_price_usd_auto(token_address: str, bnb_price_usd: float,
                              venue: str = "") -> float | None:
    """
    自动检测交易场所并获取 USD 价格
    venue 非空时优先查对应平台, 失败后再尝试其他平台
    """
    if venue == "PANCAKE":
        price = get_token_price_bnb(token_address)
        if price is not None:
            return price * bnb_price_usd
        # PancakeSwap 失败, 不再尝试其他 (已毕业的币不会在 bonding curve 上)
        return None

    if venue == "BONDING":
        price = get_token_price_bnb_bonding(token_address)
        if price is not None:
            return price * bnb_price_usd
        # bonding curve 失败, 尝试 PancakeSwap (可能刚毕业)
        price = get_token_price_bnb(token_address)
        if price is not None:
            return price * bnb_price_usd
        return None

    if venue == "FLAP":
        price = get_token_price_flap(token_address, bnb_price_usd)
        if price is not None:
            return price
        # flap bonding curve 失败, 尝试 PancakeSwap (可能刚毕业)
        price = get_token_price_bnb(token_address)
        if price is not None:
            return price * bnb_price_usd
        return None

    # 无 venue 提示: 先试 PancakeSwap, 再试 four.meme bonding curve, 最后试 flap
    price = get_token_price_bnb(token_address)
    if price is not None:
        return price * bnb_price_usd
    price = get_token_price_bnb_bonding(token_address)
    if price is not None:
        return price * bnb_price_usd
    price = get_token_price_flap(token_address, bnb_price_usd)
    if price is not None:
        return price
    return None


# ===================================================================
#  买入
# ===================================================================
def calculate_buy_amount(cfg: dict, bnb_price_usd: float,
                         pay_currency: str = "BNB") -> float:
    """
    计算本次买入金额 (以 USDT 计价)
    pay_currency: "BNB" 或 "USDT", 决定基于哪种余额计算
    规则: 可用余额折合 USD 的 buy_fraction, 但不小于 min_buy_usd, 不大于 max_buy_usd
    返回: USDT 数量 (0 表示余额不足)
    """
    trading_cfg = cfg.get("trading", {})
    min_usd = trading_cfg.get("min_buy_usd", 5)
    max_usd = trading_cfg.get("max_buy_usd", 100)
    fraction = trading_cfg.get("buy_fraction", 0.05)

    if pay_currency == "USDT":
        usdt_balance = get_usdt_balance()
        available_usd = usdt_balance
        reserve_usd = 1.0  # 保留 1 USDT 余量
        balance_label = f"USDT 余额 {usdt_balance:.2f}"
    else:
        bnb_balance = get_bnb_balance()
        available_usd = bnb_balance * bnb_price_usd
        reserve_usd = 2.0  # 保留 $2 等值 BNB 作为 gas
        balance_label = f"BNB 余额 {bnb_balance:.4f} (${available_usd:.2f})"

    buy_usdt = available_usd * fraction
    buy_usdt = max(min_usd, min(buy_usdt, max_usd))

    if buy_usdt > available_usd - reserve_usd:
        buy_usdt = available_usd - reserve_usd
        if buy_usdt <= 0:
            log.warning("余额不足: %s", balance_label)
            return 0.0

    log.info("买入计算: %s → 买入 $%.2f", balance_label, buy_usdt)
    return buy_usdt


def buy_token(token_address: str, buy_usdt: float, slippage_pct: float = 12.0,
              token_name: str = "", bnb_price_usd: float = 600.0) -> dict | None:
    """
    通过 PancakeSwap 买入代币
    自动检测流动性池配对:
      - 优先 BNB → WBNB → Token (swapExactETHForTokens)
      - WBNB 池无流动性时回退 USDT → Token (swapExactTokensForTokens)
    买入金额以 USDT 计价
    返回: {"tx_hash": ..., "token_amount": ..., "price_usd": ...} 或 None
    """
    if not _w3 or not _wallet_address or not _private_key:
        log.error("Web3/钱包未初始化")
        return None

    token_cs = Web3.to_checksum_address(token_address)

    try:
        router = _w3.eth.contract(address=PANCAKE_ROUTER_V2, abi=ROUTER_ABI)
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)
        deadline = int(time.time()) + 120

        # --- 自动检测流动性池: 先尝试 WBNB 路径, 失败则回退 USDT 路径 ---
        use_usdt_path = False
        buy_bnb = buy_usdt / bnb_price_usd
        buy_bnb_wei = Web3.to_wei(buy_bnb, "ether")
        path_wbnb = [WBNB, token_cs]

        try:
            amounts = router.functions.getAmountsOut(buy_bnb_wei, path_wbnb).call()
            expected_out = amounts[-1]
            if expected_out <= 0:
                raise ValueError("WBNB 池询价返回 0")
            amount_out_min = int(expected_out * (1 - slippage_pct / 100))
            log.info("PancakeSwap 买入 [WBNB路径]: $%.2f → %.6f BNB (BNB价格 $%.2f)",
                     buy_usdt, buy_bnb, bnb_price_usd)
        except Exception as e_wbnb:
            log.info("WBNB 路径询价失败 (%s), 尝试 USDT 路径: %s", e_wbnb, token_name)
            # 尝试 USDT → Token 路径
            path_usdt = [USDT, token_cs]
            buy_usdt_wei = int(buy_usdt * (10 ** USDT_DECIMALS))
            try:
                amounts = router.functions.getAmountsOut(buy_usdt_wei, path_usdt).call()
                expected_out = amounts[-1]
                if expected_out <= 0:
                    raise ValueError("USDT 池询价返回 0")
                amount_out_min = int(expected_out * (1 - slippage_pct / 100))
                use_usdt_path = True
                log.info("PancakeSwap 买入 [USDT路径]: $%.2f USDT", buy_usdt)
            except Exception as e_usdt:
                log.error("WBNB 和 USDT 路径均无流动性 [%s]: WBNB=%s, USDT=%s",
                          token_name, e_wbnb, e_usdt)
                return None

        if use_usdt_path:
            # === USDT → Token 路径 ===
            # 检查 USDT 余额
            usdt_contract = _w3.eth.contract(address=USDT, abi=ERC20_ABI)
            usdt_balance = usdt_contract.functions.balanceOf(_wallet_address).call()
            if usdt_balance < buy_usdt_wei:
                log.error("USDT 余额不足: 需要 %.2f USDT, 当前 %.2f USDT",
                          buy_usdt, usdt_balance / (10 ** USDT_DECIMALS))
                return None

            # Approve USDT 给 Router
            allowance = usdt_contract.functions.allowance(
                _wallet_address, PANCAKE_ROUTER_V2
            ).call()
            if allowance < buy_usdt_wei:
                log.info("Approve USDT to PancakeSwap Router...")
                nonce = _w3.eth.get_transaction_count(_wallet_address)
                approve_tx = usdt_contract.functions.approve(
                    PANCAKE_ROUTER_V2, MAX_UINT256
                ).build_transaction({
                    "from": _wallet_address, "gas": DEFAULT_GAS_APPROVE,
                    "gasPrice": _w3.eth.gas_price, "nonce": nonce, "chainId": BSC_CHAIN_ID,
                })
                signed = _w3.eth.account.sign_transaction(approve_tx, _private_key)
                tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                if receipt["status"] != 1:
                    log.error("USDT Approve 失败")
                    return None
                log.info("USDT Approve 成功")
                time.sleep(2)

            # 获取买入前余额
            balance_before = token_contract.functions.balanceOf(_wallet_address).call()

            nonce = _w3.eth.get_transaction_count(_wallet_address)
            tx = router.functions.swapExactTokensForTokensSupportingFeeOnTransferTokens(
                buy_usdt_wei, amount_out_min, path_usdt, _wallet_address, deadline,
            ).build_transaction({
                "from": _wallet_address, "value": 0,
                "gas": DEFAULT_GAS_SWAP, "gasPrice": _w3.eth.gas_price,
                "nonce": nonce, "chainId": BSC_CHAIN_ID,
            })

            signed = _w3.eth.account.sign_transaction(tx, _private_key)
            tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
            log.info("买入 TX [USDT路径] 已发送: %s", tx_hash.hex())

            receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt["status"] != 1:
                log.error("买入交易失败 [USDT路径]: %s", tx_hash.hex())
                return None

            balance_after = token_contract.functions.balanceOf(_wallet_address).call()
            actual_received = balance_after - balance_before
            decimals = token_contract.functions.decimals().call()

            if actual_received <= 0:
                log.error("买入异常: TX 成功但未收到代币 %s (TX: %s)",
                          token_name or token_address[:16], tx_hash.hex())
                return None

            buy_price_usd = buy_usdt / (actual_received / 10**decimals)

            log.info("买入成功 %s [USDT路径]: $%.2f USDT → %s 代币, 单价 $%.12f",
                     token_name or token_address[:16], buy_usdt, actual_received, buy_price_usd)

            return {
                "tx_hash": tx_hash.hex(),
                "token_amount": str(actual_received),
                "decimals": decimals,
                "buy_price_usd": buy_price_usd,
                "buy_bnb": 0.0,
                "venue": "PANCAKE",
            }
        else:
            # === BNB → WBNB → Token 路径 (原逻辑) ===
            # 检查 BNB 余额
            bnb_balance = get_bnb_balance()
            gas_reserve = 0.002
            if bnb_balance < buy_bnb + gas_reserve:
                log.error("BNB 余额不足: 需要 %.6f BNB (买入 %.6f + gas), 当前 %.6f BNB",
                          buy_bnb + gas_reserve, buy_bnb, bnb_balance)
                return None

            # 获取买入前余额
            balance_before = token_contract.functions.balanceOf(_wallet_address).call()

            nonce = _w3.eth.get_transaction_count(_wallet_address)
            tx = router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
                amount_out_min, path_wbnb, _wallet_address, deadline,
            ).build_transaction({
                "from": _wallet_address, "value": buy_bnb_wei,
                "gas": DEFAULT_GAS_SWAP, "gasPrice": _w3.eth.gas_price,
                "nonce": nonce, "chainId": BSC_CHAIN_ID,
            })

            signed = _w3.eth.account.sign_transaction(tx, _private_key)
            tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
            log.info("买入 TX 已发送: %s", tx_hash.hex())

            receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt["status"] != 1:
                log.error("买入交易失败: %s", tx_hash.hex())
                return None

            balance_after = token_contract.functions.balanceOf(_wallet_address).call()
            actual_received = balance_after - balance_before
            decimals = token_contract.functions.decimals().call()

            if actual_received <= 0:
                log.error("买入异常: TX 成功但未收到代币 %s (TX: %s)",
                          token_name or token_address[:16], tx_hash.hex())
                return None

            buy_price_usd = buy_usdt / (actual_received / 10**decimals)

            log.info("买入成功 %s: $%.2f (%.6f BNB) → %s 代币, 单价 $%.12f",
                     token_name or token_address[:16], buy_usdt, buy_bnb, actual_received, buy_price_usd)

            return {
                "tx_hash": tx_hash.hex(),
                "token_amount": str(actual_received),
                "decimals": decimals,
                "buy_price_usd": buy_price_usd,
                "buy_bnb": buy_bnb,
                "venue": "PANCAKE",
            }

    except Exception as e:
        log.error("买入异常 [%s]: %s", token_name or token_address[:16], e)
        return None


# ===================================================================
#  卖出
# ===================================================================
def sell_token(token_address: str, amount: int | None = None,
               slippage_pct: float = 15.0, token_name: str = "",
               bnb_price_usd: float = 600.0) -> dict | None:
    """
    通过 PancakeSwap 卖出代币
    自动检测流动性池配对:
      - 优先 Token → WBNB → BNB (swapExactTokensForETH)
      - WBNB 池无流动性时回退 Token → USDT (swapExactTokensForTokens)
    amount=None 表示卖出全部持仓
    返回: {"tx_hash": ..., "usdt_received": ...} 或 None
    """
    if not _w3 or not _wallet_address or not _private_key:
        log.error("Web3/钱包未初始化")
        return None

    token_cs = Web3.to_checksum_address(token_address)

    try:
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)
        router = _w3.eth.contract(address=PANCAKE_ROUTER_V2, abi=ROUTER_ABI)

        if amount is None:
            amount = token_contract.functions.balanceOf(_wallet_address).call()
        if amount <= 0:
            log.warning("无代币可卖: %s", token_name or token_address[:16])
            return None

        # 检查 & 执行 approve
        allowance = token_contract.functions.allowance(
            _wallet_address, PANCAKE_ROUTER_V2
        ).call()
        if allowance < amount:
            log.info("执行 approve: %s", token_name or token_address[:16])
            nonce = _w3.eth.get_transaction_count(_wallet_address)
            approve_tx = token_contract.functions.approve(
                PANCAKE_ROUTER_V2, MAX_UINT256
            ).build_transaction({
                "from": _wallet_address, "gas": DEFAULT_GAS_APPROVE,
                "gasPrice": _w3.eth.gas_price, "nonce": nonce, "chainId": BSC_CHAIN_ID,
            })
            signed = _w3.eth.account.sign_transaction(approve_tx, _private_key)
            tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt["status"] != 1:
                log.error("Approve 失败: %s", tx_hash.hex())
                return None
            log.info("Approve 成功")
            time.sleep(2)

        deadline = int(time.time()) + 120

        # --- 自动检测流动性池: 先尝试 WBNB 路径, 失败则回退 USDT 路径 ---
        use_usdt_path = False
        path_wbnb = [token_cs, WBNB]
        try:
            amounts = router.functions.getAmountsOut(amount, path_wbnb).call()
            expected_bnb = amounts[-1]
            if expected_bnb <= 0:
                raise ValueError("WBNB 池询价返回 0")
            amount_out_min = int(expected_bnb * (1 - slippage_pct / 100))
            log.info("卖出询价 [WBNB路径]: %s → %.6f BNB",
                     token_name or token_address[:16],
                     float(Web3.from_wei(expected_bnb, "ether")))
        except Exception as e_wbnb:
            log.info("WBNB 路径询价失败 (%s), 尝试 USDT 路径: %s", e_wbnb, token_name)
            # 尝试 Token → USDT 路径
            path_usdt = [token_cs, USDT]
            try:
                amounts = router.functions.getAmountsOut(amount, path_usdt).call()
                expected_usdt = amounts[-1]
                if expected_usdt <= 0:
                    raise ValueError("USDT 池询价返回 0")
                amount_out_min = int(expected_usdt * (1 - slippage_pct / 100))
                use_usdt_path = True
                log.info("卖出询价 [USDT路径]: %s → %.4f USDT",
                         token_name or token_address[:16],
                         expected_usdt / (10 ** USDT_DECIMALS))
            except Exception as e_usdt:
                log.error("WBNB 和 USDT 路径均无流动性 [%s]: WBNB=%s, USDT=%s",
                          token_name, e_wbnb, e_usdt)
                return None

        if use_usdt_path:
            # === Token → USDT 路径 ===
            usdt_contract = _w3.eth.contract(address=USDT, abi=ERC20_ABI)
            usdt_before = usdt_contract.functions.balanceOf(_wallet_address).call()

            nonce = _w3.eth.get_transaction_count(_wallet_address)
            tx = router.functions.swapExactTokensForTokensSupportingFeeOnTransferTokens(
                amount, amount_out_min, path_usdt, _wallet_address, deadline,
            ).build_transaction({
                "from": _wallet_address, "value": 0,
                "gas": DEFAULT_GAS_SWAP, "gasPrice": _w3.eth.gas_price,
                "nonce": nonce, "chainId": BSC_CHAIN_ID,
            })

            signed = _w3.eth.account.sign_transaction(tx, _private_key)
            tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
            log.info("卖出 TX [USDT路径] 已发送: %s", tx_hash.hex())

            receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt["status"] != 1:
                log.error("卖出交易失败 [USDT路径]: %s", tx_hash.hex())
                return None

            usdt_after = usdt_contract.functions.balanceOf(_wallet_address).call()
            usdt_received = (usdt_after - usdt_before) / (10 ** USDT_DECIMALS)

            log.info("卖出成功 %s [USDT路径]: 收回 %.4f USDT",
                     token_name or token_address[:16], usdt_received)

            return {
                "tx_hash": tx_hash.hex(),
                "usdt_received": usdt_received,
            }
        else:
            # === Token → WBNB → BNB 路径 (原逻辑) ===
            bnb_before = _w3.eth.get_balance(_wallet_address)

            nonce = _w3.eth.get_transaction_count(_wallet_address)
            tx = router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
                amount, amount_out_min, path_wbnb, _wallet_address, deadline,
            ).build_transaction({
                "from": _wallet_address, "value": 0,
                "gas": DEFAULT_GAS_SWAP, "gasPrice": _w3.eth.gas_price,
                "nonce": nonce, "chainId": BSC_CHAIN_ID,
            })

            signed = _w3.eth.account.sign_transaction(tx, _private_key)
            tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
            log.info("卖出 TX 已发送: %s", tx_hash.hex())

            receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt["status"] != 1:
                log.error("卖出交易失败: %s", tx_hash.hex())
                return None

            bnb_after = _w3.eth.get_balance(_wallet_address)
            gas_cost = receipt["gasUsed"] * receipt["effectiveGasPrice"]
            bnb_received = float(Web3.from_wei(bnb_after - bnb_before + gas_cost, "ether"))
            usdt_received = bnb_received * bnb_price_usd

            log.info("卖出成功 %s: 收回 %.6f BNB ($%.4f)",
                     token_name or token_address[:16], bnb_received, usdt_received)

            return {
                "tx_hash": tx_hash.hex(),
                "usdt_received": usdt_received,
                "bnb_received": bnb_received,
            }

    except Exception as e:
        log.error("卖出异常 [%s]: %s", token_name or token_address[:16], e)
        return None


# ===================================================================
#  持仓管理
# ===================================================================
def record_buy(conn: sqlite3.Connection, token_address: str, token_name: str,
               decimals: int, buy_result: dict, channel: str = "quality"):
    """记录买入持仓"""
    now_ms = int(time.time() * 1000)
    conn.execute("""
        INSERT INTO positions
            (token_address, token_name, token_decimals, buy_price_usd, buy_amount,
             buy_bnb, buy_tx, buy_time, max_price_usd, current_price, status, venue, channel)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?)
    """, (
        token_address.lower(),
        token_name,
        decimals,
        buy_result["buy_price_usd"],
        buy_result["token_amount"],
        buy_result["buy_bnb"],
        buy_result["tx_hash"],
        now_ms,
        buy_result["buy_price_usd"],  # max_price 初始 = buy_price
        buy_result["buy_price_usd"],
        buy_result.get("venue", "PANCAKE"),
        channel,
    ))
    conn.commit()


def get_open_positions(conn: sqlite3.Connection) -> list[dict]:
    """获取所有未平仓持仓"""
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM positions WHERE status = 'OPEN'
    """).fetchall()
    conn.row_factory = None
    return [dict(r) for r in rows]


def has_open_position(conn: sqlite3.Connection, token_address: str) -> bool:
    """检查是否已有该代币的持仓"""
    row = conn.execute(
        "SELECT 1 FROM positions WHERE LOWER(token_address) = ? AND status = 'OPEN' LIMIT 1",
        (token_address.lower(),)
    ).fetchone()
    return row is not None


def is_in_cooldown(conn: sqlite3.Connection, token_address: str,
                   trading_cfg: dict) -> tuple[bool, str]:
    """
    检查该代币是否在平仓冷却期内。
    盈利平仓: 冷却 rebuy_cooldown_profit_hours (默认12h)
    亏损平仓: 冷却 rebuy_cooldown_loss_hours (默认24h)
    返回: (是否冷却中, 原因描述)
    """
    profit_cd_h = trading_cfg.get("rebuy_cooldown_profit_hours", 12)
    loss_cd_h = trading_cfg.get("rebuy_cooldown_loss_hours", 24)
    now_ms = int(time.time() * 1000)

    # 查询该代币最近一次已平仓记录
    row = conn.execute(
        """SELECT sell_time, pnl_pct FROM positions
           WHERE LOWER(token_address) = ? AND status = 'CLOSED'
           ORDER BY sell_time DESC LIMIT 1""",
        (token_address.lower(),)
    ).fetchone()
    if not row:
        return False, ""

    sell_time, pnl_pct = row
    if sell_time is None:
        return False, ""

    elapsed_h = (now_ms - sell_time) / (3600 * 1000)
    if pnl_pct is not None and pnl_pct > 0:
        # 盈利平仓
        if elapsed_h < profit_cd_h:
            return True, f"盈利平仓后冷却中({elapsed_h:.1f}h/{profit_cd_h}h)"
    else:
        # 亏损平仓
        if elapsed_h < loss_cd_h:
            return True, f"亏损平仓后冷却中({elapsed_h:.1f}h/{loss_cd_h}h)"

    return False, ""


def close_position(conn: sqlite3.Connection, position_id: int,
                   sell_price_usd: float, sell_tx: str, sell_reason: str,
                   buy_price_usd: float):
    """关闭持仓"""
    now_ms = int(time.time() * 1000)
    pnl_pct = ((sell_price_usd - buy_price_usd) / buy_price_usd * 100) if buy_price_usd > 0 else 0
    conn.execute("""
        UPDATE positions SET
            status = 'CLOSED',
            sell_price_usd = ?,
            sell_tx = ?,
            sell_time = ?,
            sell_reason = ?,
            pnl_pct = ?
        WHERE id = ?
    """, (sell_price_usd, sell_tx, now_ms, sell_reason, pnl_pct, position_id))
    conn.commit()


def update_position_price(conn: sqlite3.Connection, position_id: int,
                          current_price: float, max_price: float):
    """更新持仓的当前价格和最高价"""
    conn.execute("""
        UPDATE positions SET current_price = ?, max_price_usd = ? WHERE id = ?
    """, (current_price, max_price, position_id))
    conn.commit()


def record_momentum(conn: sqlite3.Connection, position_id: int,
                    holders: int | None, liquidity: float | None,
                    progress: float | None, price: float | None):
    """记录一条动能快照 (每轮监控调用一次)"""
    conn.execute("""
        INSERT INTO momentum (position_id, ts, holders, liquidity, progress, price)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (position_id, int(time.time() * 1000), holders, liquidity, progress, price))
    conn.commit()


def get_momentum_history(conn: sqlite3.Connection, position_id: int,
                         limit: int = 30) -> list[dict]:
    """获取持仓的动能历史 (最近 N 条, 按时间升序)"""
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM momentum WHERE position_id = ?
        ORDER BY ts DESC LIMIT ?
    """, (position_id, limit)).fetchall()
    conn.row_factory = None
    return [dict(r) for r in reversed(rows)]  # 升序返回


def calc_momentum_signals(history: list[dict]) -> dict:
    """
    根据动能历史计算衰竭信号
    返回: {
        "holders_peak": int, "holders_last": int, "holders_drawdown_pct": float,
        "liquidity_peak": float, "liquidity_last": float, "liquidity_drawdown_pct": float,
        "progress_peak": float, "progress_last": float, "progress_drop_pp": float,
        "bad_count": int, "bad_signals": list[str],
        "is_graduated": bool,
    }
    """
    if len(history) < 2:
        return {"bad_count": 0, "bad_signals": []}

    # 提取有效序列
    h_seq = [r["holders"] for r in history if r["holders"] is not None]
    l_seq = [r["liquidity"] for r in history if r["liquidity"] is not None]
    p_seq = [r["progress"] for r in history if r["progress"] is not None]

    result = {
        "holders_peak": 0, "holders_last": 0, "holders_drawdown_pct": 0,
        "liquidity_peak": 0, "liquidity_last": 0, "liquidity_drawdown_pct": 0,
        "progress_peak": 0, "progress_last": 0, "progress_drop_pp": 0,
        "bad_count": 0, "bad_signals": [],
        "is_graduated": False,
    }

    # 持币数回撤 (相对值)
    if h_seq:
        peak_h = max(h_seq)
        last_h = h_seq[-1]
        result["holders_peak"] = peak_h
        result["holders_last"] = last_h
        if peak_h > 0:
            result["holders_drawdown_pct"] = (peak_h - last_h) / peak_h * 100

    # 流动性回撤 (相对值, 仅已毕业有意义)
    if l_seq:
        peak_l = max(l_seq)
        last_l = l_seq[-1]
        result["liquidity_peak"] = peak_l
        result["liquidity_last"] = last_l
        if peak_l > 0:
            result["liquidity_drawdown_pct"] = (peak_l - last_l) / peak_l * 100

    # 进度回撤 (绝对值百分点, 仅未毕业有意义)
    if p_seq:
        peak_p = max(p_seq)
        last_p = p_seq[-1]
        result["progress_peak"] = peak_p
        result["progress_last"] = last_p
        result["progress_drop_pp"] = (peak_p - last_p) * 100  # 百分点
        result["is_graduated"] = last_p >= 1.0

    # 判断恶化信号
    bad_signals = []

    # 持币数: 从峰值跌 >30%
    if result["holders_peak"] > 0 and result["holders_drawdown_pct"] > 30:
        bad_signals.append(
            f"持币跌{result['holders_drawdown_pct']:.0f}% "
            f"({result['holders_peak']}→{result['holders_last']})")

    # 流动性: 从峰值跌 >30% (仅已毕业, 流动性 >$100 才有意义)
    if result["is_graduated"] and result["liquidity_peak"] > 100:
        if result["liquidity_drawdown_pct"] > 30:
            bad_signals.append(
                f"流动性跌{result['liquidity_drawdown_pct']:.0f}% "
                f"(${result['liquidity_peak']:.0f}→${result['liquidity_last']:.0f})")

    # 进度: 从峰值跌 >15 个百分点 (仅未毕业, 峰值进度 >5% 才有意义)
    if not result["is_graduated"] and result["progress_peak"] > 0.05:
        if result["progress_drop_pp"] > 15:
            bad_signals.append(
                f"进度跌{result['progress_drop_pp']:.0f}pp "
                f"({result['progress_peak']*100:.1f}%→{result['progress_last']*100:.1f}%)")

    result["bad_count"] = len(bad_signals)
    result["bad_signals"] = bad_signals
    return result


# ===================================================================
#  卖出策略
# ===================================================================
def check_sell_conditions(pos: dict, current_price: float,
                          cfg: dict,
                          momentum: dict | None = None) -> tuple[bool, str]:
    """
    检查是否满足卖出条件 (v16: 回撤止盈 + 中点止盈 + 固定止损 + 5%预留缓冲)
    返回: (是否应该卖出, 卖出原因)

    v16 策略:
      1. 回撤止盈: 盈利达到 tp_trigger_pct 后触发跟踪
         - 触发~tp_midpoint_pct 区间: 从最高价回撤 (tp_drawdown_pct - 5%) 即卖出 (5%缓冲提前触发)
         - tp_midpoint_pct 以上: 中点止盈法, 价格 ≤ (最高价 + 买入价) / 2
      2. 固定止损: 亏损 40% 止损 (5%缓冲: 实际 35% 止损)
      3. 动能衰竭止盈: ≥2 个指标同时恶化且当前盈利 → 止盈
      4. 超期清仓: 持仓超过 12h 且仍亏损 → 卖出

    momentum: calc_momentum_signals() 的返回值
    """
    trading_cfg = cfg.get("trading", {})
    buy_price = pos["buy_price_usd"]
    max_price = pos["max_price_usd"] or 0

    if buy_price <= 0:
        return False, ""

    profit_pct = (current_price - buy_price) / buy_price * 100
    max_profit_pct = (max_price - buy_price) / buy_price * 100 if max_price > 0 else 0

    # 5% 预留缓冲 (链上代币价格变动太快, 1 分钟扫描不够, 提前触发)
    BUFFER_PCT = 5

    # ==================== 策略1: 回撤止盈 ====================
    tp_trigger_pct = trading_cfg.get("tp_trigger_pct", 15)
    tp_midpoint_pct = trading_cfg.get("tp_midpoint_pct", 30)
    tp_drawdown_pct = trading_cfg.get("tp_drawdown_pct", 15)

    # 触发跟踪 (触发阈值不受缓冲影响, 缓冲仅用于回撤卖出线)
    if max_profit_pct >= tp_trigger_pct:
        if max_profit_pct >= tp_midpoint_pct:
            # 30% 以上: 中点止盈法 (价格 ≤ (最高价 + 买入价) / 2)
            midpoint = (max_price + buy_price) / 2
            if current_price <= midpoint:
                mid_pct = (midpoint - buy_price) / buy_price * 100
                return True, (f"MIDPOINT_TP (中点止盈: 最高盈利 {max_profit_pct:.0f}%, "
                              f"中点 ${midpoint:.12f} ({mid_pct:.0f}%), "
                              f"当前 ${current_price:.12f} ({profit_pct:.0f}%))")
        else:
            # 15%~30%: 回撤止盈 (带缓冲: 回撤 10% 即卖出)
            drawdown_sell_pct = tp_drawdown_pct - BUFFER_PCT
            drawdown_price = max_price * (1 - drawdown_sell_pct / 100)
            if current_price <= drawdown_price:
                sell_at_pct = (drawdown_price - buy_price) / buy_price * 100
                return True, (f"TRAILING_TP (回撤止盈: 最高盈利 {max_profit_pct:.0f}%, "
                              f"回撤{drawdown_sell_pct}%线 ${drawdown_price:.12f} ({sell_at_pct:.0f}%), "
                              f"当前 ${current_price:.12f} ({profit_pct:.0f}%))")

    # ==================== 策略2: 动能衰竭止盈 ====================
    if momentum and trading_cfg.get("momentum_tp_enabled", True):
        min_bad = trading_cfg.get("momentum_min_bad_signals", 2)
        if momentum.get("bad_count", 0) >= min_bad and profit_pct > 0:
            signals_str = " + ".join(momentum["bad_signals"])
            return True, (f"MOMENTUM_TP (盈利 {profit_pct:.0f}%, "
                          f"{momentum['bad_count']}个动能衰竭: {signals_str})")

    # ==================== 策略3: 超期清仓 ====================
    hold_ms = int(time.time() * 1000) - pos["buy_time"]
    hold_hours = hold_ms / (3600 * 1000)

    expire_loss_hours = trading_cfg.get("expire_loss_hours", 12)
    if hold_hours >= expire_loss_hours and profit_pct < 0:
        return True, f"EXPIRE_LOSS (持仓 {hold_hours:.0f}h, 亏损 {profit_pct:.0f}%)"

    # ==================== 策略4: 固定止损 ====================
    stop_loss_original = trading_cfg.get("stop_loss_pct", -40)
    stop_loss_buffered = stop_loss_original + BUFFER_PCT  # -40 + 5 = -35

    if profit_pct <= stop_loss_buffered:
        return True, (f"STOP_LOSS (亏损 {profit_pct:.0f}%, "
                      f"阈值 {stop_loss_buffered}%, 原始 {stop_loss_original}%)")

    return False, ""


# ===================================================================
#  钉钉通知 (交易相关)
# ===================================================================
def _dingtalk_trade_sign(secret: str) -> tuple[str, str]:
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


def _send_trade_notify(cfg: dict, title: str, text: str):
    """发送交易通知到钉钉 (Markdown 格式)"""
    import requests as req
    webhook = cfg.get("dingtalk_webhook", "")
    secret = cfg.get("dingtalk_secret", "")
    if not webhook or "YOUR" in webhook:
        log.info("[交易通知] %s", text)
        return
    try:
        url = webhook
        if secret:
            ts, sign = _dingtalk_trade_sign(secret)
            url += f"&timestamp={ts}&sign={sign}"
        req.post(
            url,
            json={
                "msgtype": "markdown",
                "markdown": {"title": title, "text": text},
            },
            timeout=10,
        )
    except Exception as e:
        log.warning("交易通知发送失败: %s", e)


def _trade_time_str() -> str:
    """返回当前时间字符串, 用于交易通知末尾"""
    from datetime import datetime, timezone, timedelta
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def notify_buy(cfg: dict, token_name: str, token_address: str,
               buy_usdt: float, buy_price_usd: float, tx_hash: str,
               bonus_tags: list | None = None):
    """买入成功通知"""
    bonus_str = ""
    if bonus_tags:
        bonus_str = f"\n\n加分项: {' | '.join(bonus_tags)}"
    text = (
        f"## 🟢 买入成功\n\n"
        f"代币: {token_name}\n\n"
        f"合约: `{token_address}`\n\n"
        f"花费: {buy_usdt:.2f} USDT\n\n"
        f"单价: ${buy_price_usd:.12f}\n\n"
        f"TX: [查看](https://bscscan.com/tx/{tx_hash})"
        f"{bonus_str}\n\n"
        f"时间: {_trade_time_str()}"
    )
    _send_trade_notify(cfg, "买入成功", text)


def notify_buy_failed(cfg: dict, token_name: str, token_address: str,
                      reason: str):
    """买入失败记录 (仅写日志, 不推送钉钉; 精筛报告已推送代币详情, 没有买入成功通知即表示失败)"""
    log.info("[买入跳过] %s (%s): %s", token_name, token_address[:16], reason)


def notify_sell(cfg: dict, token_name: str, token_address: str,
                sell_reason: str, pnl_pct: float, tx_hash: str,
                buy_price: float = 0, max_price: float = 0,
                sell_price: float = 0):
    """卖出成功通知"""
    emoji = "🔴" if pnl_pct < 0 else "🟡" if pnl_pct < 50 else "🟢"
    max_pnl_pct = ((max_price - buy_price) / buy_price * 100) if (buy_price > 0 and max_price > 0) else 0
    text = (
        f"## {emoji} 卖出成功\n\n"
        f"代币: {token_name}\n\n"
        f"合约: `{token_address}`\n\n"
        f"原因: {sell_reason}\n\n"
        f"盈亏: {pnl_pct:+.1f}% | 最高盈利: {max_pnl_pct:+.1f}%\n\n"
        f"买入: ${buy_price:.12f}\n\n"
        f"最高: ${max_price:.12f}\n\n"
        f"卖出: ${sell_price:.12f}\n\n"
        f"TX: [查看](https://bscscan.com/tx/{tx_hash})\n\n"
        f"时间: {_trade_time_str()}"
    )
    _send_trade_notify(cfg, "卖出成功", text)


def notify_sell_failed(cfg: dict, token_name: str, token_address: str,
                       reason: str):
    """卖出失败通知"""
    text = (
        f"## 🔴 卖出失败\n\n"
        f"代币: {token_name}\n\n"
        f"合约: `{token_address}`\n\n"
        f"原因: {reason}\n\n"
        f"时间: {_trade_time_str()}"
    )
    _send_trade_notify(cfg, "卖出失败", text)


# ===================================================================
#  BNB 自动补充 (gas 费)
# ===================================================================
def _ensure_bnb_for_gas(bnb_price_usd: float, slippage_pct: float = 12.0):
    """检查 BNB 余额是否足够支付 gas (现在 BNB 是主要交易资产, 仅做余额检查)"""
    if not _w3 or not _wallet_address:
        return
    bnb_balance = get_bnb_balance()
    bnb_value_usd = bnb_balance * bnb_price_usd
    if bnb_value_usd < 1.0:
        log.warning("BNB 余额过低: %.4f BNB ($%.2f), 可能不足以支付 gas",
                     bnb_balance, bnb_value_usd)


# ===================================================================
#  执行买入 (供 scanner 调用)
# ===================================================================
def execute_buys(tokens: list[tuple[dict, dict]], cfg: dict,
                 bnb_price_usd: float):
    """
    对筛选通过的代币执行自动买入
    tokens: [(token_dict, detail_dict), ...]
    自动检测交易场所: bonding curve (PUBLISH) 或 PancakeSwap (TRADE)
    bonding curve 根据报价币 (quote) 自动选择 BNB 或 USDT 支付
    买入前检查实时价格, 偏离精筛价格过大则放弃

    仓位管理:
    - 最多同时持有 max_positions (默认 10) 个仓位, 仓位满则不开新仓
    - 代币按加分项数量降序排序, 加分项多的优先买入
    """
    trading_cfg = cfg.get("trading", {})
    if not trading_cfg.get("enabled", False):
        return

    log.info("执行自动买入: %d 个候选代币, BNB=$%.2f", len(tokens), bnb_price_usd)

    slippage = trading_cfg.get("slippage_pct", 12.0)
    # 价格保护: 实时价格偏离精筛价格的最大倍数 (默认 3 倍)
    max_price_deviation = trading_cfg.get("max_price_deviation", 3.0)
    # 仓位上限: 最多同时持有的仓位数 (默认 10)
    max_positions = trading_cfg.get("max_positions", 10)
    conn = sqlite3.connect(str(DB_PATH))
    _init_positions_db(conn)

    # 检查当前持仓数量
    open_positions = get_open_positions(conn)
    current_count = len(open_positions)
    if current_count >= max_positions:
        log.info("仓位已满 (%d/%d), 本轮不开新仓", current_count, max_positions)
        conn.close()
        return
    available_slots = max_positions - current_count
    log.info("当前持仓 %d/%d, 可开 %d 个新仓", current_count, max_positions, available_slots)

    # 按加分项数量降序排序: 加分项多的优先买入
    def _bonus_sort_key(item):
        tk, detail = item
        bonus_tags = detail.get("_bonus_tags") or []
        return len(bonus_tags)
    tokens = sorted(tokens, key=_bonus_sort_key, reverse=True)

    # BNB 余额检查
    _ensure_bnb_for_gas(bnb_price_usd, slippage)

    bought_count = 0
    for tk, detail in tokens:
        # 仓位上限检查
        if bought_count >= available_slots:
            log.info("已达本轮可开仓上限 (%d), 剩余候选跳过", available_slots)
            break

        addr = tk.get("tokenAddress", "")
        name = tk.get("shortName") or tk.get("name") or addr[:16]
        channel = tk.get("channel", "quality")  # 通道: quality / graduated
        source = tk.get("source", "")  # 来源平台: "flap" / "four.meme"

        # 检查是否已有持仓 (仅写日志)
        if has_open_position(conn, addr):
            log.info("跳过 %s: 已有持仓", name)
            notify_buy_failed(cfg, name, addr, "已有持仓, 跳过重复买入")
            continue

        # 检查平仓冷却期 (仅写日志)
        in_cd, cd_reason = is_in_cooldown(conn, addr, trading_cfg)
        if in_cd:
            log.info("跳过 %s: %s", name, cd_reason)
            notify_buy_failed(cfg, name, addr, cd_reason)
            continue

        # 价格保护: 买入前查实时价格, 和精筛价格对比
        scan_price = detail.get("price", 0)
        if scan_price > 0 and max_price_deviation > 0:
            # 根据来源提示优先查对应平台价格, 减少无效 API 调用
            venue_hint = "FLAP" if source == "flap" else ""
            realtime_price = get_token_price_usd_auto(addr, bnb_price_usd, venue=venue_hint)
            if realtime_price and realtime_price > 0:
                deviation = realtime_price / scan_price
                if deviation > max_price_deviation:
                    reason = (f"价格偏离过大 (精筛 ${scan_price:.2e} → "
                              f"实时 ${realtime_price:.2e}, {deviation:.1f}倍, "
                              f"上限 {max_price_deviation:.1f}倍)")
                    log.warning("跳过 %s: %s", name, reason)
                    notify_buy_failed(cfg, name, addr, reason)
                    continue
                log.info("价格检查 %s: 精筛 $%.2e → 实时 $%.2e (%.1f倍, OK)",
                         name, scan_price, realtime_price, deviation)
            else:
                reason = "无法获取实时价格, 放弃买入"
                log.warning("跳过 %s: %s", name, reason)
                notify_buy_failed(cfg, name, addr, reason)
                continue

        # 检测交易场所 (传入来源提示, 减少无效 RPC 调用)
        venue = detect_venue(addr, source_hint=source)
        log.info("代币 %s 交易场所: %s", name, venue)

        # 根据交易场所和报价币确定支付币种
        pay_currency = "BNB"  # 默认用 BNB
        if venue == "BONDING":
            info = fm_get_token_info(addr)
            if info:
                quote_addr = (info.get("quote") or "").lower()
                if quote_addr == USDT.lower():
                    pay_currency = "USDT"
                    log.info("代币 %s 报价币: USDT", name)
                else:
                    log.info("代币 %s 报价币: BNB", name)
        elif venue == "FLAP":
            # flap: Router 统一用 BNB 支付 (不管 quote token 是什么)
            log.info("代币 %s (flap, Router BNB)", name)

        # 计算买入金额 (根据支付币种选择对应余额)
        buy_usdt = calculate_buy_amount(cfg, bnb_price_usd, pay_currency)
        if buy_usdt <= 0:
            reason = f"余额不足 ({pay_currency})"
            log.warning("%s, 跳过 %s", reason, name)
            notify_buy_failed(cfg, name, addr, reason)
            continue

        result = None
        try:
            if venue == "BONDING":
                result = fm_buy_token(addr, buy_usdt, slippage, name, bnb_price_usd)
            elif venue == "FLAP":
                result = flap_buy_token(addr, buy_usdt, slippage, name, bnb_price_usd)
            elif venue == "PANCAKE":
                result = buy_token(addr, buy_usdt, slippage, name, bnb_price_usd)
            else:
                reason = f"无法确定交易场所 (venue={venue})"
                log.warning("跳过 %s: %s", name, reason)
                notify_buy_failed(cfg, name, addr, reason)
                continue
        except Exception as e:
            reason = f"买入异常: {e}"
            log.error("买入异常 %s [%s]: %s", name, venue, e, exc_info=True)
            notify_buy_failed(cfg, name, addr, reason)
            continue

        if result:
            record_buy(conn, addr, name, result["decimals"], result, channel)
            notify_buy(cfg, name, addr, buy_usdt,
                       result["buy_price_usd"], result["tx_hash"],
                       bonus_tags=detail.get("_bonus_tags"))
            bought_count += 1
        else:
            reason = f"交易执行失败 (venue={venue}, 金额=${buy_usdt:.2f})"
            log.error("买入失败 %s: %s", name, reason)
            notify_buy_failed(cfg, name, addr, reason)

        time.sleep(2)  # 避免 nonce 冲突

    conn.close()


# ===================================================================
#  持仓监控循环 (后台线程)
# ===================================================================
_monitor_stop = threading.Event()


def monitor_positions(cfg_loader, bnb_price_func):
    """
    持仓监控主循环, 每分钟检查所有持仓
    cfg_loader: callable, 返回最新 config dict
    bnb_price_func: callable, 返回 BNB 的 USD 价格
    """
    log.info("持仓监控线程启动")

    # 延迟导入 scanner 模块的数据查询函数 (避免循环依赖)
    _fm_detail = None
    _ds_batch = None
    try:
        from scanner import fm_detail as _fm_detail_fn, ds_batch_prices as _ds_batch_fn
        _fm_detail = _fm_detail_fn
        _ds_batch = _ds_batch_fn
        log.info("监控: 动能跟踪已启用 (fm_detail + DexScreener)")
    except ImportError:
        log.warning("监控: 无法导入 scanner 数据函数, 动能跟踪禁用")

    while not _monitor_stop.is_set():
        try:
            cfg = cfg_loader()
            trading_cfg = cfg.get("trading", {})
            if not trading_cfg.get("enabled", False):
                _monitor_stop.wait(60)
                continue

            bnb_price = bnb_price_func()
            if bnb_price <= 0:
                log.warning("监控: BNB 价格无效, 跳过本轮")
                _monitor_stop.wait(60)
                continue

            conn = sqlite3.connect(str(DB_PATH))
            _init_positions_db(conn)
            positions = get_open_positions(conn)

            if not positions:
                conn.close()
                _monitor_stop.wait(60)
                continue

            log.info("监控: %d 个持仓", len(positions))
            slippage = trading_cfg.get("slippage_pct", 15.0)

            # 批量查询 DexScreener 数据 (价格 + 流动性, 一次请求)
            ds_data = {}
            if _ds_batch:
                try:
                    addrs = [pos["token_address"] for pos in positions]
                    ds_data = _ds_batch(addrs)
                except Exception as e:
                    log.debug("监控: DexScreener 批量查询失败: %s", e)

            for pos in positions:
                if _monitor_stop.is_set():
                    break

                addr = pos["token_address"]
                name = pos["token_name"] or addr[:16]

                # --- 链上余额检查: 检测手动卖出 ---
                try:
                    token_cs = Web3.to_checksum_address(addr)
                    token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)
                    on_chain_balance = token_contract.functions.balanceOf(_wallet_address).call()
                    if on_chain_balance == 0:
                        log.info("监控: %s 链上余额为 0, 判定为手动卖出, 自动关闭持仓", name)
                        # 尝试获取当前价格用于记录盈亏
                        venue = pos.get("venue", "PANCAKE")
                        last_price = get_token_price_usd_auto(addr, bnb_price, venue)
                        if last_price is None:
                            last_price = pos.get("current_price", 0) or 0
                        buy_price = pos["buy_price_usd"]
                        close_position(conn, pos["id"], last_price, "", "MANUAL_SELL", buy_price)
                        pnl = ((last_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                        notify_sell(cfg, name, addr, "手动卖出 (链上余额归零)", pnl, "",
                                    buy_price=buy_price,
                                    max_price=pos.get("max_price_usd", 0),
                                    sell_price=last_price)
                        continue
                except Exception as e_bal:
                    log.debug("监控: 链上余额查询失败 %s: %s", name, e_bal)

                # 获取当前价格 (自动检测交易场所)
                venue = pos.get("venue", "PANCAKE")
                current_price = get_token_price_usd_auto(addr, bnb_price, venue)
                if current_price is None:
                    log.debug("监控: 无法获取 %s 价格", name)
                    continue

                # 计算持仓价值, 低于 $0.1 跳过监控
                try:
                    decimals = pos.get("token_decimals", 18)
                    token_amount = int(pos.get("buy_amount", 0)) / (10 ** decimals)
                    position_value = token_amount * current_price
                    if position_value < 0.1:
                        log.debug("监控: 跳过 %s (价值 $%.4f < $0.1)", name, position_value)
                        continue
                except Exception:
                    pass  # 无法计算价值时继续监控

                # 更新最高价
                max_price = max(pos["max_price_usd"] or 0, current_price)
                update_position_price(conn, pos["id"], current_price, max_price)

                buy_price = pos["buy_price_usd"]
                profit_pct = ((current_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                hold_ms = int(time.time() * 1000) - pos["buy_time"]
                hold_hours = hold_ms / (3600 * 1000)
                hold_days = hold_hours / 24
                expire_loss_hours = trading_cfg.get("expire_loss_hours", 48)
                expire_underperform_hours = trading_cfg.get("expire_underperform_hours", 72)
                expire_min_profit_pct = trading_cfg.get("expire_min_profit_pct", 500)
                expire_tag = ""
                if hold_hours >= expire_loss_hours and profit_pct < 0:
                    expire_tag = f" ⚠️超期{expire_loss_hours:.0f}h仍亏损"
                elif hold_hours >= expire_underperform_hours and profit_pct < expire_min_profit_pct:
                    expire_tag = f" ⚠️超期{expire_underperform_hours:.0f}h未达{expire_min_profit_pct:.0f}%"
                log.info("  %s [%s]: 当前 $%.12f | 买入 $%.12f | 最高 $%.12f | 盈亏 %+.1f%% | 持仓 %.1f天(%.0fh)%s",
                         name, venue, current_price, buy_price, max_price, profit_pct,
                         hold_days, hold_hours, expire_tag)

                # --- 动能数据采集 ---
                cur_holders = None
                cur_liquidity = None
                cur_progress = None

                # DexScreener: 流动性 (已在批量查询中获取)
                ds = ds_data.get(addr, {})
                if ds:
                    cur_liquidity = ds.get("liquidity", 0)

                # fm_detail: 持币数 + 进度 (仅 four.meme 代币)
                if _fm_detail:
                    try:
                        detail = _fm_detail(addr)
                        if detail:
                            cur_holders = detail.get("holders", 0)
                            cur_progress = detail.get("progress", 0)
                    except Exception as e:
                        log.debug("监控: fm_detail 查询失败 %s: %s", name, e)

                # 记录动能快照
                record_momentum(conn, pos["id"],
                                cur_holders, cur_liquidity, cur_progress, current_price)

                # 计算动能信号
                momentum_signals = None
                if trading_cfg.get("momentum_tp_enabled", True):
                    history = get_momentum_history(conn, pos["id"])
                    if len(history) >= 2:
                        momentum_signals = calc_momentum_signals(history)
                        if momentum_signals["bad_count"] > 0:
                            log.info("  %s 动能信号: %s",
                                     name, " + ".join(momentum_signals["bad_signals"]))

                # 检查卖出条件
                pos_updated = {**pos, "max_price_usd": max_price}
                should_sell, reason = check_sell_conditions(
                    pos_updated, current_price, cfg, momentum=momentum_signals)

                if should_sell:
                    log.info("触发卖出 %s: %s", name, reason)
                    # 检测当前实际交易场所 (可能已从 bonding curve 迁移到 PancakeSwap)
                    current_venue = detect_venue(addr, source_hint=pos.get("source", ""))
                    # UNKNOWN 时回退到持仓记录的 venue (买入时确定的通道更可靠)
                    if current_venue == "UNKNOWN":
                        fallback_venue = pos.get("venue", "PANCAKE")
                        log.warning("detect_venue 返回 UNKNOWN, 回退到持仓记录 venue=%s: %s",
                                    fallback_venue, name)
                        current_venue = fallback_venue
                    if current_venue == "BONDING":
                        sell_result = fm_sell_token(addr, token_name=name,
                                                    bnb_price_usd=bnb_price)
                    elif current_venue == "FLAP":
                        sell_result = flap_sell_token(addr, token_name=name,
                                                      bnb_price_usd=bnb_price)
                    else:
                        sell_result = sell_token(addr, slippage_pct=slippage,
                                                 token_name=name,
                                                 bnb_price_usd=bnb_price)
                    if sell_result:
                        # 验证链上余额确认代币确实被卖出
                        try:
                            token_cs = Web3.to_checksum_address(addr)
                            token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)
                            remaining = token_contract.functions.balanceOf(_wallet_address).call()
                            if remaining > 0:
                                decimals = token_contract.functions.decimals().call()
                                remaining_amount = remaining / (10 ** decimals)
                                remaining_value = remaining_amount * current_price
                                if remaining_value > 0.5:
                                    log.warning("卖出后仍有余额 %s: %.4f 个 (价值 $%.2f), 不关闭持仓",
                                                name, remaining_amount, remaining_value)
                                    notify_sell_failed(cfg, name, addr,
                                                       f"卖出后仍有余额 {remaining_amount:.4f} 个 (价值 ${remaining_value:.2f}), 未完全卖出")
                                    continue
                        except Exception as e_check:
                            log.debug("卖出后余额检查异常 %s: %s", name, e_check)

                        close_position(conn, pos["id"], current_price,
                                       sell_result["tx_hash"], reason, buy_price)
                        pnl = ((current_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                        notify_sell(cfg, name, addr, reason, pnl, sell_result["tx_hash"],
                                    buy_price=buy_price, max_price=max_price,
                                    sell_price=current_price)
                        # 诈骗开发者记录: 亏损 80%+ 的代币, 记录 creator 到黑名单
                        if pnl <= -80:
                            try:
                                from scanner import load_queue, save_queue, ZERO_ADDRESS, DEPLOYER_BLACKLIST, DEPLOYER_SCAM_MIN_TOKENS
                                _qs = load_queue()
                                _bl = _qs.setdefault("deployerBlacklist", {})
                                # 从队列中找到该代币的 creator
                                _creator = ""
                                for _t in _qs.get("tokens", []) + _qs.get("eliminated", []):
                                    if isinstance(_t, dict) and _t.get("address", "").lower() == addr.lower():
                                        _creator = (_t.get("creator") or "").lower()
                                        break
                                if _creator and _creator != ZERO_ADDRESS:
                                    if _creator not in _bl:
                                        _bl[_creator] = {"count": 0, "tokens": [], "firstSeen": int(time.time() * 1000), "lastSeen": int(time.time() * 1000)}
                                    _entry = _bl[_creator]
                                    if addr.lower() not in _entry["tokens"]:
                                        _entry["tokens"].append(addr.lower())
                                        _entry["count"] = len(_entry["tokens"])
                                        _entry["lastSeen"] = int(time.time() * 1000)
                                        save_queue(_qs)
                                        # 更新运行时黑名单
                                        if _entry["count"] >= DEPLOYER_SCAM_MIN_TOKENS:
                                            DEPLOYER_BLACKLIST.add(_creator)
                                        log.info("🚨 诈骗开发者记录 (交易亏损%.0f%%): %s — %s (累计%d个诈骗币)",
                                                 pnl, _creator[:16], name, _entry["count"])
                            except Exception as _e:
                                log.debug("诈骗开发者记录失败: %s", _e)
                    else:
                        log.error("卖出失败: %s", name)
                        notify_sell_failed(cfg, name, addr,
                                           f"交易执行失败 (venue={current_venue}, 触发原因: {reason})")

                time.sleep(1)  # 避免 RPC 限流

            conn.close()

        except Exception as e:
            log.error("监控异常: %s", e, exc_info=True)
            if _send_error:
                _send_error(f"持仓监控异常: {e}", exc_info=True)

        _monitor_stop.wait(trading_cfg.get("monitor_interval_sec", 60) if 'trading_cfg' in dir() else 60)

    log.info("持仓监控线程停止")


def start_monitor(cfg_loader, bnb_price_func) -> threading.Thread:
    """启动持仓监控后台线程"""
    _monitor_stop.clear()
    t = threading.Thread(
        target=monitor_positions,
        args=(cfg_loader, bnb_price_func),
        daemon=True,
        name="position-monitor",
    )
    t.start()
    return t


def stop_monitor():
    """停止持仓监控"""
    _monitor_stop.set()


# ===================================================================
#  启动时清理低价值历史记录
# ===================================================================
CLEANUP_VALUE_THRESHOLD = 0.1  # 低于 $0.1 的已平仓记录直接删除


def _cleanup_low_value_positions(bnb_price_usd: float):
    """
    清理数据库中价值极低的已平仓记录, 减少启动时链上扫描量
    删除条件: status=CLOSED 且持仓价值低于阈值 且已过冷却期
    不会删除 OPEN 持仓, 不调用任何 API
    注意: 冷却期内的 CLOSED 记录绝不删除, 否则 is_in_cooldown 会失效
    """
    conn = sqlite3.connect(str(DB_PATH))
    _init_positions_db(conn)

    # 查询所有 CLOSED 记录, 用数据库中已有的价格数据判断
    rows = conn.execute("""
        SELECT id, token_address, token_name, buy_amount, token_decimals,
               sell_price_usd, current_price, sell_time
        FROM positions
        WHERE status = 'CLOSED'
    """).fetchall()

    if not rows:
        conn.close()
        return

    now_ms = int(time.time() * 1000)
    # 冷却保护: 平仓后 48h 内的记录不删除 (取盈利/亏损冷却期的较大值再加缓冲)
    COOLDOWN_PROTECT_MS = 48 * 3600 * 1000

    delete_ids = []
    for row in rows:
        pos_id, addr, name, buy_amount_str, decimals, sell_price, current_price, sell_time = row

        # 冷却期保护: 平仓时间在保护期内的记录绝不删除
        if sell_time and (now_ms - sell_time) < COOLDOWN_PROTECT_MS:
            continue

        # 用 buy_amount * best_price 估算持仓价值 (不能用单价和阈值比较, meme 币单价极低)
        best_price = max(sell_price or 0, current_price or 0)
        try:
            amount = int(buy_amount_str or "0")
            dec = decimals or 18
            token_amount = amount / (10 ** dec)
            value = token_amount * best_price
            if value < CLEANUP_VALUE_THRESHOLD:
                delete_ids.append((pos_id, name or addr[:16]))
        except (ValueError, TypeError, OverflowError):
            # 无法计算价值时, 用买入金额兜底 (buy_bnb * bnb_price)
            pass

    if delete_ids:
        conn.execute(
            f"DELETE FROM positions WHERE id IN ({','.join(str(d[0]) for d in delete_ids)})"
        )
        conn.commit()
        log.info("数据库清理: 删除 %d 条低价值已平仓记录 (价值<$%.2f, 已过冷却保护期)",
                 len(delete_ids), CLEANUP_VALUE_THRESHOLD)

    remaining = conn.execute("SELECT COUNT(DISTINCT token_address) FROM positions").fetchone()[0]
    log.info("数据库清理: 剩余 %d 个不同代币地址", remaining)
    conn.close()


# ===================================================================
#  启动时钱包扫描 — 从链上同步真实持仓
# ===================================================================
SKIP_TOKENS = {
    WBNB.lower(),
    USDT.lower(),
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
    "0x55d398326f99059ff775485246999027b3197955",  # USDT
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
}


def _scan_wallet_tokens(bnb_price_usd: float) -> list[dict]:
    """
    扫描钱包中所有 BEP-20 代币, 返回价值 > $0.1 的持仓列表 (排除 BNB/USDT 等稳定币)

    代币地址来源 (多源合并):
      1. 数据库历史持仓记录 (OPEN + CLOSED)
      2. BSCScan tokentx API (有 key 时)
    然后逐个查链上余额 + 询价, 筛选价值 > $0.1 的
    """
    if not _w3 or not _wallet_address:
        return []

    import requests as req

    token_addrs: set[str] = set()

    # 来源 1: 数据库历史持仓
    try:
        conn = sqlite3.connect(str(DB_PATH))
        _init_positions_db(conn)
        rows = conn.execute("SELECT DISTINCT token_address FROM positions").fetchall()
        for (addr,) in rows:
            addr_lower = (addr or "").lower()
            if addr_lower and addr_lower not in SKIP_TOKENS:
                token_addrs.add(addr_lower)
        conn.close()
        if token_addrs:
            log.info("持仓同步: 从数据库获取 %d 个历史代币地址", len(token_addrs))
    except Exception as e:
        log.debug("读取数据库历史持仓失败: %s", e)

    # 来源 2+3: BSCScan tokentx + addresstokenbalance 并行查询
    cfg_path = Path(__file__).parent / "config.json"
    api_key = ""
    if cfg_path.exists():
        try:
            with open(cfg_path, "r") as f:
                api_key = json.load(f).get("bscscan_api_key", "")
        except Exception:
            pass

    def _fetch_tokentx() -> set[str]:
        """BSCScan tokentx API"""
        addrs = set()
        try:
            params = {
                "module": "account", "action": "tokentx",
                "address": _wallet_address, "startblock": 0, "endblock": 99999999,
                "page": 1, "offset": 1000, "sort": "desc",
            }
            if api_key:
                params["apikey"] = api_key
            r = req.get("https://api.bscscan.com/api", params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "1":
                for tx in data.get("result", []):
                    a = (tx.get("contractAddress") or "").lower()
                    if a and a not in SKIP_TOKENS:
                        addrs.add(a)
            if addrs:
                log.info("持仓同步: 从 BSCScan tokentx 补充 %d 个代币地址", len(addrs))
        except Exception as e:
            log.debug("BSCScan tokentx 查询失败: %s", e)
        return addrs

    def _fetch_tokenbalance() -> set[str]:
        """BSCScan addresstokenbalance API"""
        addrs = set()
        if not api_key:
            return addrs
        try:
            params3 = {
                "module": "account", "action": "addresstokenbalance",
                "address": _wallet_address, "page": 1, "offset": 100,
                "apikey": api_key,
            }
            r3 = req.get("https://api.bscscan.com/api", params=params3, timeout=15)
            r3.raise_for_status()
            data3 = r3.json()
            if data3.get("status") == "1":
                for item in data3.get("result", []):
                    a = (item.get("TokenAddress") or "").lower()
                    if a and a not in SKIP_TOKENS:
                        addrs.add(a)
            if addrs:
                log.info("持仓同步: 从 BSCScan addresstokenbalance 补充 %d 个代币地址", len(addrs))
        except Exception as e:
            log.debug("BSCScan addresstokenbalance 查询失败: %s", e)
        return addrs

    from concurrent.futures import ThreadPoolExecutor as _TPE
    with _TPE(max_workers=2) as pool:
        f1 = pool.submit(_fetch_tokentx)
        f2 = pool.submit(_fetch_tokenbalance)
        token_addrs.update(f1.result())
        token_addrs.update(f2.result())

    if not token_addrs:
        log.info("持仓同步: 无历史代币记录, 跳过")
        return []

    log.info("持仓同步: 共 %d 个候选代币, 逐个检查链上余额...", len(token_addrs))

    # 逐个查链上余额和价格
    holdings = []
    for addr in token_addrs:
        try:
            token_cs = Web3.to_checksum_address(addr)
            contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)
            balance = contract.functions.balanceOf(_wallet_address).call()
            if balance <= 0:
                continue
            decimals = contract.functions.decimals().call()
            token_amount = balance / (10 ** decimals)

            # 获取 USD 价格
            price_usd = get_token_price_usd_auto(addr, bnb_price_usd)
            if price_usd is None or price_usd <= 0:
                continue

            value_usd = token_amount * price_usd
            if value_usd < 0.1:
                continue

            # 获取代币名称和代码
            try:
                name_abi = [{"name": "name", "type": "function", "stateMutability": "view",
                             "inputs": [], "outputs": [{"name": "", "type": "string"}]}]
                sym_abi = [{"name": "symbol", "type": "function", "stateMutability": "view",
                            "inputs": [], "outputs": [{"name": "", "type": "string"}]}]
                name_contract = _w3.eth.contract(address=token_cs, abi=name_abi)
                sym_contract = _w3.eth.contract(address=token_cs, abi=sym_abi)
                token_name = name_contract.functions.name().call()
                token_symbol = sym_contract.functions.symbol().call()
            except Exception:
                token_name = addr[:16]
                token_symbol = addr[:10]

            venue = detect_venue(addr)

            holdings.append({
                "address": addr,
                "name": token_name,
                "symbol": token_symbol,
                "decimals": decimals,
                "balance": str(balance),
                "price_usd": price_usd,
                "value_usd": value_usd,
                "venue": venue if venue != "UNKNOWN" else "PANCAKE",
            })
            log.info("  发现持仓 %s: %.2f 个, $%.10f/个, 价值 $%.2f [%s]",
                     token_symbol, token_amount, price_usd, value_usd, venue)

        except Exception as e:
            log.debug("检查代币余额 [%s]: %s", addr[:16], e)

        time.sleep(0.3)

    log.info("持仓同步: 发现 %d 个价值 > $0.1 的链上持仓", len(holdings))
    return holdings


def _sync_positions_from_wallet(bnb_price_usd: float):
    """
    启动时同步: 以链上钱包实际持仓为准
    1. 扫描钱包中价值 > $0.1 的代币 (排除 BNB/USDT 等)
    2. 数据库中没有 OPEN 记录的 → 新建 (用当前价作为买入价)
    3. 数据库中有 OPEN 记录但链上余额为 0 的 → 关闭
    """
    log.info("========== 启动持仓同步 ==========")
    conn = sqlite3.connect(str(DB_PATH))
    _init_positions_db(conn)

    old_positions = get_open_positions(conn)
    wallet_tokens = _scan_wallet_tokens(bnb_price_usd)
    wallet_addrs = {h["address"].lower() for h in wallet_tokens}

    # 关闭链上已无余额的持仓
    closed = 0
    for pos in old_positions:
        if pos["token_address"].lower() not in wallet_addrs:
            log.info("同步: 关闭无余额持仓 %s (链上已无价值>$0.1的余额)",
                     pos["token_name"] or pos["token_address"][:16])
            close_position(conn, pos["id"], 0, "", "SYNC_NO_BALANCE", pos["buy_price_usd"])
            closed += 1

    # 为链上有余额但数据库无记录的代币创建持仓
    created = 0
    for h in wallet_tokens:
        addr = h["address"]
        if not has_open_position(conn, addr):
            now_ms = int(time.time() * 1000)
            conn.execute("""
                INSERT INTO positions
                    (token_address, token_name, token_decimals, buy_price_usd, buy_amount,
                     buy_bnb, buy_tx, buy_time, max_price_usd, current_price, status, venue)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
            """, (
                addr,
                h["name"],
                h["decimals"],
                h["price_usd"],       # 用当前价作为买入价
                h["balance"],
                0,                     # buy_bnb 未知
                "wallet_sync",         # 标记为钱包同步
                now_ms,
                h["price_usd"],       # max_price = 当前价
                h["price_usd"],
                h["venue"],
            ))
            log.info("同步: 新建持仓 %s, 当前价 $%.10f, 价值 $%.2f",
                     h["name"], h["price_usd"], h["value_usd"])
            created += 1
        else:
            log.info("同步: 已有持仓记录 %s, 保留", h["name"])

    conn.commit()
    conn.close()
    log.info("持仓同步完成: %d 个活跃持仓 (新建 %d, 关闭 %d)",
             len(wallet_tokens), created, closed)


# ===================================================================
#  初始化
# ===================================================================
def init_trader(cfg: dict, bnb_price_usd: float = 0):
    """初始化交易模块 (Web3 连接 + 钱包加载 + 链上持仓同步)"""
    trading_cfg = cfg.get("trading", {})
    if not trading_cfg.get("enabled", False):
        log.info("自动交易未启用")
        return False

    rpc_url = trading_cfg.get("rpc_url")
    try:
        _init_web3(rpc_url)
        _load_wallet()
        balance_bnb = get_bnb_balance()
        balance_usdt = get_usdt_balance()
        log.info("钱包余额: %.4f BNB, %.2f USDT", balance_bnb, balance_usdt)

        # 清理低价值历史记录, 减少链上扫描量
        if bnb_price_usd > 0:
            _cleanup_low_value_positions(bnb_price_usd)

        # 从链上同步真实持仓
        if bnb_price_usd > 0:
            _sync_positions_from_wallet(bnb_price_usd)

        return True
    except Exception as e:
        log.error("交易模块初始化失败: %s", e)
        return False

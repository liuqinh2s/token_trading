"""
BSC 自动交易模块 - PancakeSwap V2
买入: 扫描筛选通过后自动买入
卖出:
  1. 回撤止盈: 盈利超过100%, 当价格回撤到 (买入价+最高价)/2 时卖出
  2. 超期清仓: 持仓超过3天且未盈利时卖出
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


def detect_venue(token_address: str) -> str:
    """
    检测代币当前的交易场所
    返回: 'BONDING' (bonding curve) | 'PANCAKE' (已迁移到 PancakeSwap) | 'UNKNOWN'
    """
    info = fm_get_token_info(token_address)
    if info is None:
        return "UNKNOWN"
    if info["liquidityAdded"]:
        return "PANCAKE"
    if info["offers"] > 0:
        return "BONDING"
    return "UNKNOWN"


def fm_buy_token(token_address: str, buy_bnb: float, slippage_pct: float = 12.0,
                 token_name: str = "", bnb_price_usd: float = 600.0) -> dict | None:
    """
    通过 four.meme bonding curve 买入代币
    使用 TokenManager2.buyTokenAMAP (花费固定 BNB, 获取尽可能多的代币)
    """
    if not _w3 or not _wallet_address or not _private_key:
        log.error("Web3/钱包未初始化")
        return None

    token_cs = Web3.to_checksum_address(token_address)

    try:
        helper = _w3.eth.contract(address=FM_HELPER_V3, abi=FM_HELPER_ABI)
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)
        amount_in_wei = Web3.to_wei(buy_bnb, "ether")

        # 1) 通过 Helper3.tryBuy 预估买入数量
        try:
            est = helper.functions.tryBuy(token_cs, 0, amount_in_wei).call()
            estimated_amount = est[2]
            actual_msg_value = est[5] if est[5] > 0 else amount_in_wei
            actual_funds = est[7] if est[7] > 0 else amount_in_wei
            min_amount = int(estimated_amount * (1 - slippage_pct / 100))
            log.info("bonding curve 预估: 花费 %.4f BNB → %s 代币",
                     buy_bnb, estimated_amount)
        except Exception as e:
            log.warning("tryBuy 预估失败: %s, 使用 minAmount=0", e)
            actual_msg_value = amount_in_wei
            actual_funds = amount_in_wei
            min_amount = 0

        # 2) 获取 tokenManager 地址
        info = fm_get_token_info(token_address)
        if info is None:
            log.error("无法获取代币信息: %s", token_name)
            return None
        manager_addr = info["tokenManager"]
        if manager_addr == "0x" + "0" * 40:
            manager_addr = FM_TOKEN_MANAGER_V2

        manager = _w3.eth.contract(
            address=Web3.to_checksum_address(manager_addr),
            abi=FM_MANAGER_ABI,
        )

        # 3) 获取买入前余额
        balance_before = token_contract.functions.balanceOf(_wallet_address).call()

        # 4) 构建买入交易
        nonce = _w3.eth.get_transaction_count(_wallet_address)
        tx = manager.functions.buyTokenAMAP(
            token_cs,
            actual_funds,
            min_amount,
        ).build_transaction({
            "from": _wallet_address,
            "value": actual_msg_value,
            "gas": DEFAULT_GAS_SWAP,
            "gasPrice": _w3.eth.gas_price,
            "nonce": nonce,
            "chainId": BSC_CHAIN_ID,
        })

        # 5) 签名 & 发送
        signed = _w3.eth.account.sign_transaction(tx, _private_key)
        tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("bonding curve 买入 TX: %s", tx_hash.hex())

        receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] != 1:
            log.error("bonding curve 买入失败: %s", tx_hash.hex())
            return None

        # 6) 获取实际买到的数量
        balance_after = token_contract.functions.balanceOf(_wallet_address).call()
        actual_received = balance_after - balance_before
        decimals = token_contract.functions.decimals().call()

        buy_price_usd = (buy_bnb * bnb_price_usd) / (actual_received / 10**decimals) if actual_received > 0 else 0

        log.info("bonding curve 买入成功 %s: %.4f BNB → %s 代币, 单价 $%.12f",
                 token_name or token_address[:16], buy_bnb, actual_received, buy_price_usd)

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
                  token_name: str = "") -> dict | None:
    """
    通过 four.meme bonding curve 卖出代币
    需要先 approve TokenManager 合约
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

        # 获取 tokenManager 地址
        info = fm_get_token_info(token_address)
        if info is None:
            log.error("无法获取代币信息: %s", token_name)
            return None

        # 如果已迁移, 应该用 PancakeSwap 卖出
        if info["liquidityAdded"]:
            log.info("%s 已迁移到 PancakeSwap, 切换卖出方式", token_name)
            return sell_token(token_address, amount, token_name=token_name)

        manager_addr = info["tokenManager"]
        if manager_addr == "0x" + "0" * 40:
            manager_addr = FM_TOKEN_MANAGER_V2
        manager_cs = Web3.to_checksum_address(manager_addr)

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

        # 获取卖出前 BNB 余额
        bnb_before = _w3.eth.get_balance(_wallet_address)

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

        bnb_after = _w3.eth.get_balance(_wallet_address)
        gas_cost = receipt["gasUsed"] * receipt["effectiveGasPrice"]
        bnb_received = float(Web3.from_wei(bnb_after - bnb_before + gas_cost, "ether"))

        log.info("bonding curve 卖出成功 %s: 收回 %.6f BNB",
                 token_name or token_address[:16], bnb_received)

        return {
            "tx_hash": tx_hash.hex(),
            "bnb_received": bnb_received,
        }

    except Exception as e:
        log.error("bonding curve 卖出异常 [%s]: %s", token_name or token_address[:16], e)
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
    优先尝试 PancakeSwap, 失败则尝试 bonding curve
    """
    if venue == "PANCAKE":
        price = get_token_price_bnb(token_address)
        if price is not None:
            return price * bnb_price_usd

    if venue == "BONDING":
        price = get_token_price_bnb_bonding(token_address)
        if price is not None:
            return price * bnb_price_usd

    # 自动检测: 先试 PancakeSwap, 再试 bonding curve
    price = get_token_price_bnb(token_address)
    if price is not None:
        return price * bnb_price_usd
    price = get_token_price_bnb_bonding(token_address)
    if price is not None:
        return price * bnb_price_usd
    return None


# ===================================================================
#  买入
# ===================================================================
def calculate_buy_amount(cfg: dict, bnb_price_usd: float) -> float:
    """
    计算本次买入的 BNB 数量
    规则: 总资金的 1/10, 但不小于 $10, 不大于 $100
    """
    trading_cfg = cfg.get("trading", {})
    min_usd = trading_cfg.get("min_buy_usd", 10)
    max_usd = trading_cfg.get("max_buy_usd", 100)
    fraction = trading_cfg.get("buy_fraction", 0.1)

    balance_bnb = get_bnb_balance()
    balance_usd = balance_bnb * bnb_price_usd

    buy_usd = balance_usd * fraction
    buy_usd = max(min_usd, min(buy_usd, max_usd))

    buy_bnb = buy_usd / bnb_price_usd

    # 保留至少 0.005 BNB 作为 gas
    if buy_bnb > balance_bnb - 0.005:
        buy_bnb = balance_bnb - 0.005
        if buy_bnb <= 0:
            log.warning("BNB 余额不足: %.4f BNB", balance_bnb)
            return 0.0

    log.info("买入计算: 余额 %.4f BNB ($%.2f) → 买入 %.4f BNB ($%.2f)",
             balance_bnb, balance_usd, buy_bnb, buy_bnb * bnb_price_usd)
    return buy_bnb


def buy_token(token_address: str, buy_bnb: float, slippage_pct: float = 12.0,
              token_name: str = "", bnb_price_usd: float = 600.0) -> dict | None:
    """
    通过 PancakeSwap 买入代币
    返回: {"tx_hash": ..., "token_amount": ..., "price_usd": ...} 或 None
    """
    if not _w3 or not _wallet_address or not _private_key:
        log.error("Web3/钱包未初始化")
        return None

    token_cs = Web3.to_checksum_address(token_address)

    try:
        router = _w3.eth.contract(address=PANCAKE_ROUTER_V2, abi=ROUTER_ABI)
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)

        amount_in_wei = Web3.to_wei(buy_bnb, "ether")
        deadline = int(time.time()) + 120

        # 询价
        try:
            amounts = router.functions.getAmountsOut(
                amount_in_wei, [WBNB, token_cs]
            ).call()
            expected_out = amounts[1]
            amount_out_min = int(expected_out * (1 - slippage_pct / 100))
        except Exception:
            # 询价失败, 设 amountOutMin = 0 (接受任意数量)
            amount_out_min = 0
            log.warning("询价失败, 使用 amountOutMin=0")

        # 获取买入前余额
        balance_before = token_contract.functions.balanceOf(_wallet_address).call()

        # 构建交易
        nonce = _w3.eth.get_transaction_count(_wallet_address)
        tx = router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
            amount_out_min,
            [WBNB, token_cs],
            _wallet_address,
            deadline,
        ).build_transaction({
            "from": _wallet_address,
            "value": amount_in_wei,
            "gas": DEFAULT_GAS_SWAP,
            "gasPrice": _w3.eth.gas_price,
            "nonce": nonce,
            "chainId": BSC_CHAIN_ID,
        })

        # 签名 & 发送
        signed = _w3.eth.account.sign_transaction(tx, _private_key)
        tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("买入 TX 已发送: %s", tx_hash.hex())

        # 等待确认
        receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] != 1:
            log.error("买入交易失败: %s", tx_hash.hex())
            return None

        # 获取实际买到的数量
        balance_after = token_contract.functions.balanceOf(_wallet_address).call()
        actual_received = balance_after - balance_before
        decimals = token_contract.functions.decimals().call()

        buy_price_usd = (buy_bnb * bnb_price_usd) / (actual_received / 10**decimals) if actual_received > 0 else 0

        log.info("买入成功 %s: %.4f BNB → %s 代币, 单价 $%.12f",
                 token_name or token_address[:16], buy_bnb,
                 actual_received, buy_price_usd)

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
               slippage_pct: float = 15.0, token_name: str = "") -> dict | None:
    """
    通过 PancakeSwap 卖出代币
    amount=None 表示卖出全部持仓
    返回: {"tx_hash": ..., "bnb_received": ...} 或 None
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
            log.info("Approve 成功")
            time.sleep(2)

        # 询价
        deadline = int(time.time()) + 120
        try:
            amounts = router.functions.getAmountsOut(
                amount, [token_cs, WBNB]
            ).call()
            expected_bnb = amounts[1]
            amount_out_min = int(expected_bnb * (1 - slippage_pct / 100))
        except Exception:
            amount_out_min = 0
            log.warning("卖出询价失败, 使用 amountOutMin=0")

        # 获取卖出前 BNB 余额
        bnb_before = _w3.eth.get_balance(_wallet_address)

        # 构建卖出交易
        nonce = _w3.eth.get_transaction_count(_wallet_address)
        tx = router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
            amount,
            amount_out_min,
            [token_cs, WBNB],
            _wallet_address,
            deadline,
        ).build_transaction({
            "from": _wallet_address,
            "gas": DEFAULT_GAS_SWAP,
            "gasPrice": _w3.eth.gas_price,
            "nonce": nonce,
            "chainId": BSC_CHAIN_ID,
        })

        signed = _w3.eth.account.sign_transaction(tx, _private_key)
        tx_hash = _w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("卖出 TX 已发送: %s", tx_hash.hex())

        receipt = _w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] != 1:
            log.error("卖出交易失败: %s", tx_hash.hex())
            return None

        bnb_after = _w3.eth.get_balance(_wallet_address)
        # 扣除 gas 后的净收入
        gas_cost = receipt["gasUsed"] * receipt["effectiveGasPrice"]
        bnb_received = float(Web3.from_wei(bnb_after - bnb_before + gas_cost, "ether"))

        log.info("卖出成功 %s: 收回 %.6f BNB", token_name or token_address[:16], bnb_received)

        return {
            "tx_hash": tx_hash.hex(),
            "bnb_received": bnb_received,
        }

    except Exception as e:
        log.error("卖出异常 [%s]: %s", token_name or token_address[:16], e)
        return None


# ===================================================================
#  持仓管理
# ===================================================================
def record_buy(conn: sqlite3.Connection, token_address: str, token_name: str,
               decimals: int, buy_result: dict):
    """记录买入持仓"""
    now_ms = int(time.time() * 1000)
    conn.execute("""
        INSERT INTO positions
            (token_address, token_name, token_decimals, buy_price_usd, buy_amount,
             buy_bnb, buy_tx, buy_time, max_price_usd, current_price, status, venue)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
    """, (
        token_address,
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
        "SELECT 1 FROM positions WHERE token_address = ? AND status = 'OPEN' LIMIT 1",
        (token_address,)
    ).fetchone()
    return row is not None


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


# ===================================================================
#  卖出策略
# ===================================================================
def check_sell_conditions(pos: dict, current_price: float,
                          cfg: dict) -> tuple[bool, str]:
    """
    检查是否满足卖出条件
    返回: (是否应该卖出, 卖出原因)

    策略:
      1. 回撤止盈: 盈利超过100%, 价格回撤到 (buy_price + max_price) / 2
      2. 超期清仓: 持仓超过3天且未盈利
    """
    trading_cfg = cfg.get("trading", {})
    buy_price = pos["buy_price_usd"]
    max_price = pos["max_price_usd"]

    if buy_price <= 0:
        return False, ""

    profit_pct = (current_price - buy_price) / buy_price * 100

    # 策略1: 回撤止盈
    tp_trigger_pct = trading_cfg.get("tp_trigger_pct", 100)  # 触发止盈的盈利百分比
    max_profit_pct = (max_price - buy_price) / buy_price * 100 if buy_price > 0 else 0

    if max_profit_pct >= tp_trigger_pct:
        # 已触发止盈条件, 检查回撤
        sell_threshold = (buy_price + max_price) / 2
        if current_price <= sell_threshold:
            return True, f"TRAILING_TP (最高盈利 {max_profit_pct:.0f}%, 当前 {profit_pct:.0f}%)"

    # 策略2: 超期清仓
    expire_hours = trading_cfg.get("expire_hours", 72)  # 默认3天
    hold_ms = int(time.time() * 1000) - pos["buy_time"]
    hold_hours = hold_ms / (3600 * 1000)

    if hold_hours >= expire_hours and current_price <= buy_price:
        return True, f"EXPIRE (持仓 {hold_hours:.1f}h, 盈亏 {profit_pct:.1f}%)"

    return False, ""


# ===================================================================
#  Telegram 通知 (交易相关)
# ===================================================================
def _send_trade_notify(cfg: dict, text: str):
    """发送交易通知到 Telegram"""
    import requests as req
    bot_token = cfg.get("telegram_bot_token", "")
    chat_id = cfg.get("telegram_chat_id", "")
    if not bot_token or not chat_id or "YOUR" in bot_token:
        log.info("[交易通知] %s", text)
        return
    try:
        req.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception as e:
        log.warning("交易通知发送失败: %s", e)


def notify_buy(cfg: dict, token_name: str, token_address: str,
               buy_bnb: float, buy_price_usd: float, tx_hash: str):
    """买入通知"""
    text = (
        f"🟢 <b>买入成功</b>\n"
        f"代币: {token_name}\n"
        f"合约: <code>{token_address}</code>\n"
        f"花费: {buy_bnb:.4f} BNB\n"
        f"单价: ${buy_price_usd:.12f}\n"
        f"TX: <a href='https://bscscan.com/tx/{tx_hash}'>查看</a>"
    )
    _send_trade_notify(cfg, text)


def notify_sell(cfg: dict, token_name: str, token_address: str,
                sell_reason: str, pnl_pct: float, tx_hash: str):
    """卖出通知"""
    emoji = "🔴" if pnl_pct < 0 else "🟡" if pnl_pct < 50 else "🟢"
    text = (
        f"{emoji} <b>卖出</b>\n"
        f"代币: {token_name}\n"
        f"合约: <code>{token_address}</code>\n"
        f"原因: {sell_reason}\n"
        f"盈亏: {pnl_pct:+.1f}%\n"
        f"TX: <a href='https://bscscan.com/tx/{tx_hash}'>查看</a>"
    )
    _send_trade_notify(cfg, text)


# ===================================================================
#  执行买入 (供 scanner 调用)
# ===================================================================
def execute_buys(tokens: list[tuple[dict, dict]], cfg: dict,
                 bnb_price_usd: float):
    """
    对筛选通过的代币执行自动买入
    tokens: [(token_dict, detail_dict), ...]
    自动检测交易场所: bonding curve (PUBLISH) 或 PancakeSwap (TRADE)
    """
    trading_cfg = cfg.get("trading", {})
    if not trading_cfg.get("enabled", False):
        return

    slippage = trading_cfg.get("slippage_pct", 12.0)
    conn = sqlite3.connect(str(DB_PATH))
    _init_positions_db(conn)

    for tk, detail in tokens:
        addr = tk.get("tokenAddress", "")
        name = tk.get("name", addr[:16])

        # 检查是否已有持仓
        if has_open_position(conn, addr):
            log.info("跳过 %s: 已有持仓", name)
            continue

        # 计算买入金额
        buy_bnb = calculate_buy_amount(cfg, bnb_price_usd)
        if buy_bnb <= 0:
            log.warning("余额不足, 停止买入")
            break

        # 检测交易场所
        venue = detect_venue(addr)
        log.info("代币 %s 交易场所: %s", name, venue)

        result = None
        if venue == "BONDING":
            result = fm_buy_token(addr, buy_bnb, slippage, name, bnb_price_usd)
        elif venue == "PANCAKE":
            result = buy_token(addr, buy_bnb, slippage, name, bnb_price_usd)
        else:
            log.warning("跳过 %s: 无法确定交易场所", name)
            continue

        if result:
            record_buy(conn, addr, name, result["decimals"], result)
            notify_buy(cfg, name, addr, buy_bnb,
                       result["buy_price_usd"], result["tx_hash"])

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

            for pos in positions:
                if _monitor_stop.is_set():
                    break

                addr = pos["token_address"]
                name = pos["token_name"] or addr[:16]

                # 获取当前价格 (自动检测交易场所)
                venue = pos.get("venue", "PANCAKE")
                current_price = get_token_price_usd_auto(addr, bnb_price, venue)
                if current_price is None:
                    log.debug("监控: 无法获取 %s 价格", name)
                    continue

                # 更新最高价
                max_price = max(pos["max_price_usd"] or 0, current_price)
                update_position_price(conn, pos["id"], current_price, max_price)

                buy_price = pos["buy_price_usd"]
                profit_pct = ((current_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                log.info("  %s [%s]: 当前 $%.12f | 最高 $%.12f | 盈亏 %+.1f%%",
                         name, venue, current_price, max_price, profit_pct)

                # 检查卖出条件
                pos_updated = {**pos, "max_price_usd": max_price}
                should_sell, reason = check_sell_conditions(pos_updated, current_price, cfg)

                if should_sell:
                    log.info("触发卖出 %s: %s", name, reason)
                    # 检测当前实际交易场所 (可能已从 bonding curve 迁移到 PancakeSwap)
                    current_venue = detect_venue(addr)
                    if current_venue == "BONDING":
                        sell_result = fm_sell_token(addr, token_name=name)
                    else:
                        sell_result = sell_token(addr, slippage_pct=slippage, token_name=name)
                    if sell_result:
                        close_position(conn, pos["id"], current_price,
                                       sell_result["tx_hash"], reason, buy_price)
                        pnl = ((current_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                        notify_sell(cfg, name, addr, reason, pnl, sell_result["tx_hash"])
                    else:
                        log.error("卖出失败: %s", name)

                time.sleep(1)  # 避免 RPC 限流

            conn.close()

        except Exception as e:
            log.error("监控异常: %s", e, exc_info=True)

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
#  初始化
# ===================================================================
def init_trader(cfg: dict):
    """初始化交易模块 (Web3 连接 + 钱包加载)"""
    trading_cfg = cfg.get("trading", {})
    if not trading_cfg.get("enabled", False):
        log.info("自动交易未启用")
        return False

    rpc_url = trading_cfg.get("rpc_url")
    try:
        _init_web3(rpc_url)
        _load_wallet()
        balance = get_bnb_balance()
        log.info("钱包余额: %.4f BNB", balance)
        return True
    except Exception as e:
        log.error("交易模块初始化失败: %s", e)
        return False

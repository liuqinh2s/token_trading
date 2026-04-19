"""
BSC 自动交易模块 - PancakeSwap V2 + four.meme Bonding Curve
买入:
  - bonding curve: 根据报价币 (quote) 自动选择 BNB 或 USDT 直接买入
  - PancakeSwap: 用 BNB 直接买入 (BNB → Token)
  - 买入金额以 USDT 计价, BNB 报价时自动换算
卖出:
  - bonding curve: 卖出收到报价币 (BNB 或 USDT)
  - PancakeSwap: 卖出收到 BNB (Token → BNB)
  1. 回撤止盈: 盈利超过20%, 当价格回撤到 (买入价+最高价)/2 时卖出
  2. 超期清仓 (阶梯式):
     - 持仓超过24小时且未盈利 → 卖出
     - 持仓超过48小时且盈利未达 tp_trigger_pct → 卖出
  3. 重买冷却: 盈利平仓后12h内不再买同一币, 亏损平仓后48h内不再买
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
    通过 PancakeSwap 买入代币 (BNB → WBNB → Token)
    买入金额以 USDT 计价, 换算为 BNB 后直接用 BNB 支付
    返回: {"tx_hash": ..., "token_amount": ..., "price_usd": ...} 或 None
    """
    if not _w3 or not _wallet_address or not _private_key:
        log.error("Web3/钱包未初始化")
        return None

    token_cs = Web3.to_checksum_address(token_address)

    try:
        # 将 USDT 计价金额换算为 BNB
        buy_bnb = buy_usdt / bnb_price_usd
        buy_bnb_wei = Web3.to_wei(buy_bnb, "ether")
        log.info("PancakeSwap 买入: $%.2f → %.6f BNB (BNB价格 $%.2f)",
                 buy_usdt, buy_bnb, bnb_price_usd)

        # 检查 BNB 余额是否足够 (买入金额 + gas 预留)
        bnb_balance = get_bnb_balance()
        gas_reserve = 0.002  # 预留 gas
        if bnb_balance < buy_bnb + gas_reserve:
            log.error("BNB 余额不足: 需要 %.6f BNB (买入 %.6f + gas), 当前 %.6f BNB",
                      buy_bnb + gas_reserve, buy_bnb, bnb_balance)
            return None

        router = _w3.eth.contract(address=PANCAKE_ROUTER_V2, abi=ROUTER_ABI)
        token_contract = _w3.eth.contract(address=token_cs, abi=ERC20_ABI)

        deadline = int(time.time()) + 120
        path = [WBNB, token_cs]

        # 询价: BNB → Token
        try:
            amounts = router.functions.getAmountsOut(buy_bnb_wei, path).call()
            expected_out = amounts[-1]
            amount_out_min = int(expected_out * (1 - slippage_pct / 100))
        except Exception:
            amount_out_min = 0
            log.warning("询价失败, 使用 amountOutMin=0")

        # 获取买入前余额
        balance_before = token_contract.functions.balanceOf(_wallet_address).call()

        # 构建交易: BNB → Token (通过 WBNB)
        nonce = _w3.eth.get_transaction_count(_wallet_address)
        tx = router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
            amount_out_min, path, _wallet_address, deadline,
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
    通过 PancakeSwap 卖出代币 (Token → WBNB → BNB)
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

        # 询价: Token → WBNB (→ BNB)
        path = [token_cs, WBNB]
        deadline = int(time.time()) + 120
        try:
            amounts = router.functions.getAmountsOut(amount, path).call()
            expected_bnb = amounts[-1]
            amount_out_min = int(expected_bnb * (1 - slippage_pct / 100))
        except Exception:
            amount_out_min = 0
            log.warning("卖出询价失败, 使用 amountOutMin=0")

        # 获取卖出前 BNB 余额
        bnb_before = _w3.eth.get_balance(_wallet_address)

        # 构建卖出交易: Token → WBNB → BNB
        nonce = _w3.eth.get_transaction_count(_wallet_address)
        tx = router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
            amount, amount_out_min, path, _wallet_address, deadline,
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
    亏损平仓: 冷却 rebuy_cooldown_loss_hours (默认48h)
    返回: (是否冷却中, 原因描述)
    """
    profit_cd_h = trading_cfg.get("rebuy_cooldown_profit_hours", 12)
    loss_cd_h = trading_cfg.get("rebuy_cooldown_loss_hours", 48)
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


# ===================================================================
#  卖出策略
# ===================================================================
def check_sell_conditions(pos: dict, current_price: float,
                          cfg: dict) -> tuple[bool, str]:
    """
    检查是否满足卖出条件
    返回: (是否应该卖出, 卖出原因)

    策略 (潜伏型配套, 回测最优解):
      1. 回撤止盈: 最高盈利超过 tp_trigger_pct 后, 价格回撤到 (buy_price + max_price) / 2 卖出
         - 如果回撤时当前价格低于买入价 (不盈利), 不卖出, 返回 RESET 信号重置峰值
      2. 兜底止损: 亏损超过 stop_loss_pct 卖出 (默认 -60%, 毕业通道 -30%)
      3. 无超期清仓 (潜伏型策略买入的是蓄势待发的币, 可能需要很长时间才起飞)
    """
    trading_cfg = cfg.get("trading", {})
    buy_price = pos["buy_price_usd"]
    max_price = pos["max_price_usd"]
    channel = pos.get("channel", "quality")

    if buy_price <= 0:
        return False, ""

    profit_pct = (current_price - buy_price) / buy_price * 100

    # 策略1: 回撤止盈
    tp_trigger_pct = trading_cfg.get("tp_trigger_pct", 30)  # 触发止盈的盈利百分比
    max_profit_pct = (max_price - buy_price) / buy_price * 100 if buy_price > 0 else 0

    if max_profit_pct >= tp_trigger_pct:
        # 已触发止盈条件, 检查回撤
        sell_threshold = (buy_price + max_price) / 2
        if current_price <= sell_threshold:
            # 回撤到阈值, 但如果当前不盈利则不卖出, 重置止盈状态
            if current_price <= buy_price:
                return False, "RESET_TP"
            return True, f"TRAILING_TP (最高盈利 {max_profit_pct:.0f}%, 当前 {profit_pct:.0f}%)"

    # 策略2: 兜底止损 (毕业通道用更紧的止损)
    default_sl = trading_cfg.get("stop_loss_pct", -60)
    if channel == "graduated":
        stop_loss_pct = trading_cfg.get("grad_stop_loss_pct", -30)
    else:
        stop_loss_pct = default_sl
    if stop_loss_pct and profit_pct <= stop_loss_pct:
        return True, f"STOP_LOSS (亏损 {profit_pct:.0f}%, 阈值 {stop_loss_pct}%)"

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
               buy_usdt: float, buy_price_usd: float, tx_hash: str):
    """买入通知"""
    text = (
        f"🟢 <b>买入成功</b>\n"
        f"代币: {token_name}\n"
        f"合约: <code>{token_address}</code>\n"
        f"花费: {buy_usdt:.2f} USDT\n"
        f"单价: ${buy_price_usd:.12f}\n"
        f"TX: <a href='https://bscscan.com/tx/{tx_hash}'>查看</a>"
    )
    _send_trade_notify(cfg, text)


def notify_sell(cfg: dict, token_name: str, token_address: str,
                sell_reason: str, pnl_pct: float, tx_hash: str,
                buy_price: float = 0, max_price: float = 0,
                sell_price: float = 0):
    """卖出通知"""
    emoji = "🔴" if pnl_pct < 0 else "🟡" if pnl_pct < 50 else "🟢"
    max_pnl_pct = ((max_price - buy_price) / buy_price * 100) if (buy_price > 0 and max_price > 0) else 0
    text = (
        f"{emoji} <b>卖出</b>\n"
        f"代币: {token_name}\n"
        f"合约: <code>{token_address}</code>\n"
        f"原因: {sell_reason}\n"
        f"盈亏: {pnl_pct:+.1f}% | 最高盈利: {max_pnl_pct:+.1f}%\n"
        f"买入: ${buy_price:.12f}\n"
        f"最高: ${max_price:.12f}\n"
        f"卖出: ${sell_price:.12f}\n"
        f"TX: <a href='https://bscscan.com/tx/{tx_hash}'>查看</a>"
    )
    _send_trade_notify(cfg, text)


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
    """
    trading_cfg = cfg.get("trading", {})
    if not trading_cfg.get("enabled", False):
        return

    slippage = trading_cfg.get("slippage_pct", 12.0)
    # 价格保护: 实时价格偏离精筛价格的最大倍数 (默认 3 倍)
    max_price_deviation = trading_cfg.get("max_price_deviation", 3.0)
    conn = sqlite3.connect(str(DB_PATH))
    _init_positions_db(conn)

    # BNB 余额检查
    _ensure_bnb_for_gas(bnb_price_usd, slippage)

    for tk, detail in tokens:
        addr = tk.get("tokenAddress", "")
        name = tk.get("name", addr[:16])
        channel = tk.get("channel", "quality")  # 通道: quality / graduated

        # 检查是否已有持仓
        if has_open_position(conn, addr):
            log.info("跳过 %s: 已有持仓", name)
            continue

        # 检查平仓冷却期
        in_cd, cd_reason = is_in_cooldown(conn, addr, trading_cfg)
        if in_cd:
            log.info("跳过 %s: %s", name, cd_reason)
            continue

        # 价格保护: 买入前查实时价格, 和精筛价格对比
        scan_price = detail.get("price", 0)
        if scan_price > 0 and max_price_deviation > 0:
            realtime_price = get_token_price_usd_auto(addr, bnb_price_usd)
            if realtime_price and realtime_price > 0:
                deviation = realtime_price / scan_price
                if deviation > max_price_deviation:
                    log.warning("跳过 %s: 价格偏离过大 (精筛 $%.2e → 实时 $%.2e, %.1f倍, 上限 %.1f倍)",
                                name, scan_price, realtime_price, deviation, max_price_deviation)
                    continue
                log.info("价格检查 %s: 精筛 $%.2e → 实时 $%.2e (%.1f倍, OK)",
                         name, scan_price, realtime_price, deviation)
            else:
                log.warning("跳过 %s: 无法获取实时价格, 放弃买入", name)
                continue

        # 检测交易场所
        venue = detect_venue(addr)
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

        # 计算买入金额 (根据支付币种选择对应余额)
        buy_usdt = calculate_buy_amount(cfg, bnb_price_usd, pay_currency)
        if buy_usdt <= 0:
            log.warning("余额不足, 跳过 %s", name)
            continue

        result = None
        if venue == "BONDING":
            result = fm_buy_token(addr, buy_usdt, slippage, name, bnb_price_usd)
        elif venue == "PANCAKE":
            result = buy_token(addr, buy_usdt, slippage, name, bnb_price_usd)
        else:
            log.warning("跳过 %s: 无法确定交易场所", name)
            continue

        if result:
            record_buy(conn, addr, name, result["decimals"], result, channel)
            notify_buy(cfg, name, addr, buy_usdt,
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
                expire_hours_1 = trading_cfg.get("expire_hours_1", 24)
                expire_hours_2 = trading_cfg.get("expire_hours_2", 48)
                tp_trigger_pct = trading_cfg.get("tp_trigger_pct", 100)
                expire_tag = ""
                if hold_hours >= expire_hours_1 and profit_pct <= 0:
                    expire_tag = " ⚠️超期24h未盈利"
                elif hold_hours >= expire_hours_2 and profit_pct < tp_trigger_pct:
                    expire_tag = f" ⚠️超期48h未达{tp_trigger_pct}%"
                log.info("  %s [%s]: 当前 $%.12f | 买入 $%.12f | 最高 $%.12f | 盈亏 %+.1f%% | 持仓 %.1f天(%.0fh)%s",
                         name, venue, current_price, buy_price, max_price, profit_pct,
                         hold_days, hold_hours, expire_tag)

                # 检查卖出条件
                pos_updated = {**pos, "max_price_usd": max_price}
                should_sell, reason = check_sell_conditions(pos_updated, current_price, cfg)

                # 止盈回撤但不盈利: 重置止盈状态, 等下一个触发点
                if not should_sell and reason == "RESET_TP":
                    log.info("  %s: 止盈回撤但未盈利, 重置止盈状态 (最高 $%.12f → 当前 $%.12f)",
                             name, max_price, current_price)
                    update_position_price(conn, pos["id"], current_price, current_price)
                    continue

                if should_sell:
                    log.info("触发卖出 %s: %s", name, reason)
                    # 检测当前实际交易场所 (可能已从 bonding curve 迁移到 PancakeSwap)
                    current_venue = detect_venue(addr)
                    if current_venue == "BONDING":
                        sell_result = fm_sell_token(addr, token_name=name,
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
                                    continue
                        except Exception as e_check:
                            log.debug("卖出后余额检查异常 %s: %s", name, e_check)

                        close_position(conn, pos["id"], current_price,
                                       sell_result["tx_hash"], reason, buy_price)
                        pnl = ((current_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                        notify_sell(cfg, name, addr, reason, pnl, sell_result["tx_hash"],
                                    buy_price=buy_price, max_price=max_price,
                                    sell_price=current_price)
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
#  启动时清理低价值历史记录
# ===================================================================
CLEANUP_VALUE_THRESHOLD = 0.1  # 低于 $0.1 的已平仓记录直接删除


def _cleanup_low_value_positions(bnb_price_usd: float):
    """
    清理数据库中价值极低的已平仓记录, 减少启动时链上扫描量
    删除条件: status=CLOSED 且 (sell_price 和 current_price 都低于阈值)
    不会删除 OPEN 持仓, 不调用任何 API
    """
    conn = sqlite3.connect(str(DB_PATH))
    _init_positions_db(conn)

    # 查询所有 CLOSED 记录, 用数据库中已有的价格数据判断
    rows = conn.execute("""
        SELECT id, token_address, token_name, buy_amount, token_decimals,
               sell_price_usd, current_price
        FROM positions
        WHERE status = 'CLOSED'
    """).fetchall()

    if not rows:
        conn.close()
        return

    delete_ids = []
    for row in rows:
        pos_id, addr, name, buy_amount_str, decimals, sell_price, current_price = row
        # 用 sell_price 和 current_price 中较大的估算残余价值
        best_price = max(sell_price or 0, current_price or 0)
        if best_price < CLEANUP_VALUE_THRESHOLD:
            # 价格本身就低于阈值, 直接标记删除
            delete_ids.append((pos_id, name or addr[:16]))
            continue
        # 如果有 buy_amount, 估算持仓价值
        try:
            amount = int(buy_amount_str or "0")
            dec = decimals or 18
            token_amount = amount / (10 ** dec)
            value = token_amount * best_price
            if value < CLEANUP_VALUE_THRESHOLD:
                delete_ids.append((pos_id, name or addr[:16]))
        except (ValueError, TypeError, OverflowError):
            pass

    if delete_ids:
        conn.execute(
            f"DELETE FROM positions WHERE id IN ({','.join(str(d[0]) for d in delete_ids)})"
        )
        conn.commit()
        log.info("数据库清理: 删除 %d 条低价值已平仓记录 (价值<$%.2f)",
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
    然后逐个查链上余额 + 询价, 筛选价值 > $1 的
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

            # 获取代币名称
            try:
                name_abi = [{"name": "name", "type": "function", "stateMutability": "view",
                             "inputs": [], "outputs": [{"name": "", "type": "string"}]}]
                name_contract = _w3.eth.contract(address=token_cs, abi=name_abi)
                token_name = name_contract.functions.name().call()
            except Exception:
                token_name = addr[:16]

            venue = detect_venue(addr)

            holdings.append({
                "address": addr,
                "name": token_name,
                "decimals": decimals,
                "balance": str(balance),
                "price_usd": price_usd,
                "value_usd": value_usd,
                "venue": venue if venue != "UNKNOWN" else "PANCAKE",
            })
            log.info("  发现持仓 %s: %.2f 个, $%.10f/个, 价值 $%.2f [%s]",
                     token_name, token_amount, price_usd, value_usd, venue)

        except Exception as e:
            log.debug("检查代币余额 [%s]: %s", addr[:16], e)

        time.sleep(0.3)

    log.info("持仓同步: 发现 %d 个价值 > $0.1 的链上持仓", len(holdings))
    return holdings


def _sync_positions_from_wallet(bnb_price_usd: float):
    """
    启动时同步: 以链上钱包实际持仓为准
    1. 扫描钱包中价值 > $1 的代币 (排除 BNB/USDT 等)
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
            log.info("同步: 关闭无余额持仓 %s (链上已无价值>$1的余额)",
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

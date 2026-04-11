"""
BSC Token Scanner v5 — 链上发现 + 队列淘汰制
数据源: BSC RPC (链上事件) + four.meme API (详情) + DexScreener (价格) + GeckoTerminal (K线) + BSCScan (链上行为)

v2 架构: 链上发现 + 队列淘汰制
每 15 分钟执行一次:
  1. 链上发现 (~1s): BSC RPC eth_getLogs → four.meme TokenCreated 事件 → 新代币地址
  2. 入场筛 (~35s): four.meme Detail API → 淘汰无社交 / 总量≠10亿
  3. 淘汰检查 (~15s): DexScreener 批量查价 + Detail API 查持币数 → 永久淘汰弃盘币
  4. 钱包行为分析 (~20s): BscScan tokentx → 开发者行为 + 聪明钱自动发现 + 聪明钱行为追踪
  5. 精筛 (~10s): K线/价格比/底价区间 + 钱包行为排除/加分 → 输出推荐

淘汰条件 (永久剔除):
  - 价格从峰值跌 90%+
  - 持币地址从 30+ 跌破 10
  - 无社交媒体
  - 流动性从 >$1k 跌破 $100
  - 进度 < 1% 且币龄 > 2h
  - 进度 < 5% 且币龄 > 4h
  - 币龄 > 5min 且最高持币数 < 3
  - 币龄 > 15min 且最高持币数 < 5
  - 币龄 > 1h 且最高持币数 < 10
  - 币龄 > 72h

精筛排除 (钱包行为):
  - 开发者减仓/清仓/撤流动性
  - 聪明钱减仓/清仓

精筛加分 (钱包行为):
  - 开发者加仓/加流动性
  - 聪明钱加仓
"""

from __future__ import annotations

import json
import time
import logging
import re
import sys
import sqlite3
import xml.etree.ElementTree as ET
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
except ImportError:
    _HAS_TRADER = False

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

# four.meme 合约地址 (用于链上事件发现)
FOUR_MEME_CONTRACT = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
TOKEN_CREATE_TOPIC = "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

FM_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Origin": "https://four.meme",
    "Referer": "https://four.meme/",
}
GT_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
DS_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

# 精筛阈值
MAX_AGE_HOURS = 72
SCAN_INTERVAL_MIN = 15
TOTAL_SUPPLY = 1_000_000_000           # 10亿
MAX_CURRENT_PRICE_OLD = 0.000023       # 币龄 > 1h 当前价格上限 (USD)
MAX_CURRENT_PRICE_YOUNG = 0.0000045    # 币龄 ≤ 1h 当前价格上限 (USD)
MAX_HIGH_PRICE = 0.00004               # 历史最高价上限 (USD)
MAX_EARLY_HIGH_PRICE = 0.00002         # 前2小时最高价上限 (USD, 币龄>1h时检查)
MAX_EARLY_HIGH_PRICE_RELAXED = 0.000023  # 前2h最高价放宽上限 (币龄<4h且价>$0.00001)
MAX_CURRENT_PRICE_YOUNG_RELAXED = 0.0000045  # 年轻代币放宽价格上限
PRICE_RATIO_LOW = 0.4                  # 当前价 ≥ 最高价 * 40%
PRICE_RATIO_HIGH = 0.9                 # 当前价 ≤ 最高价 * 90%
FLOOR_RATIO_LOW = 0.1                  # 现价比底价高 ≥ 10%
FLOOR_RATIO_HIGH = 1.0                 # 现价比底价高 ≤ 100%
HOLDERS_THRESHOLD_OLD = 60             # 币龄 > 1h 时持币地址数阈值
HOLDERS_THRESHOLD_YOUNG = 30           # 币龄 ≤ 1h 时持币地址数阈值
MIN_SOCIAL_COUNT = 1                   # 最少关联社交媒体数

# 淘汰阈值
ELIM_PRICE_DROP_PCT = 0.90             # 价格从峰值跌 90%
ELIM_HOLDERS_FLOOR = 10               # 持币数跌破 10
ELIM_HOLDERS_PEAK_MIN = 30            # 持币数曾达到 30 才触发跌破淘汰
ELIM_LIQ_FLOOR = 100                  # 流动性跌破 $100
ELIM_LIQ_PEAK_MIN = 1000              # 流动性曾达到 $1000 才触发跌破淘汰
ELIM_PROGRESS_MIN = 0.01              # 进度 < 1%
ELIM_PROGRESS_AGE_HOURS = 2           # 进度<1%淘汰的币龄门槛
ELIM_PROGRESS_MIN_MID = 0.05          # 进度 < 5%
ELIM_PROGRESS_AGE_HOURS_MID = 4       # 进度<5%淘汰的币龄门槛
ELIM_EARLY_PEAK_HOLDERS = 5           # 币龄>15min 最高持币数 < 5 淘汰
ELIM_EARLY_AGE_MIN = 0.25             # 15 分钟 = 0.25h
ELIM_TINY_PEAK_HOLDERS = 3            # 币龄>5min 最高持币数 < 3 淘汰
ELIM_TINY_AGE_MIN = 5 / 60            # 5 分钟
ELIM_MID_PEAK_HOLDERS = 10            # 币龄>1h 最高持币数 < 10 淘汰
ELIM_MID_AGE_HOURS = 1                # 1 小时

# GeckoTerminal 动态速率控制
_gt_rate_delay: float = 2.0

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
}

# 聪明钱地址缓存
_smart_money_cache: dict = {"ts": 0, "addresses": set()}
SMART_MONEY_CACHE_TTL = 3600  # 1 小时


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
_gt_session: requests.Session = None  # type: ignore
_bsc_session: requests.Session = None  # type: ignore


def _ensure_sessions():
    global _fm_session, _gt_session, _bsc_session
    if _fm_session is None:
        try:
            cfg = load_config()
            proxy = cfg.get("proxy")
        except Exception:
            proxy = None
        _fm_session = _build_session(proxy, FM_HEADERS)
        _gt_session = _build_session(proxy, GT_HEADERS)
        _bsc_session = _build_session(proxy, DS_HEADERS)


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
    return {"tokens": [], "eliminated": [], "lastBlock": 0, "lastScanTime": 0}


def save_queue(queue: dict):
    """保存队列状态"""
    # 只保留最近 1000 条淘汰记录
    if len(queue.get("eliminated", [])) > 1000:
        queue["eliminated"] = queue["eliminated"][-1000:]
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(queue, f, ensure_ascii=False, indent=2)


# ===================================================================
#  热点数据层
# ===================================================================
_hotspot_cache: dict = {"ts": 0, "keywords": []}
HOTSPOT_CACHE_TTL = 900  # 15 分钟


def _normalize(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[_\-./·・\s]+", " ", text)
    return text


def fetch_weibo_hot() -> list[dict]:
    _ensure_sessions()
    try:
        r = _fm_session.get("https://weibo.com/ajax/side/hotSearch", headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://weibo.com/",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json",
        }, timeout=10)
        r.raise_for_status()
        items = r.json().get("data", {}).get("realtime", [])
        results = [{"word": item["word"].strip(), "rank": i, "source": "weibo"}
                   for i, item in enumerate(items)
                   if (item.get("word") or "").strip() and len(item["word"].strip()) >= 2]
        log.info("微博热搜: %d 个关键词", len(results))
        return results
    except Exception as e:
        log.warning("微博热搜获取失败: %s", e)
        return []


def fetch_google_trends(geos: tuple[str, ...] = ("US", "CN")) -> list[dict]:
    _ensure_sessions()
    results, seen = [], set()
    for geo in geos:
        try:
            r = _gt_session.get(f"https://trends.google.com/trending/rss?geo={geo}", timeout=10)
            r.raise_for_status()
            root = ET.fromstring(r.text)
            for i, item in enumerate(root.findall(".//item")):
                title_el = item.find("title")
                if title_el is not None and title_el.text:
                    word = title_el.text.strip()
                    if word and word.lower() not in seen:
                        seen.add(word.lower())
                        results.append({"word": word, "rank": i, "source": f"google/{geo}"})
        except Exception as e:
            log.warning("Google Trends [%s] 获取失败: %s", geo, e)
        time.sleep(0.3)
    log.info("Google Trends: %d 个关键词", len(results))
    return results


def fetch_twitter_trending() -> list[dict]:
    _ensure_sessions()
    try:
        r = _gt_session.get("https://getdaytrends.com/united-states/", timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html",
        })
        r.raise_for_status()
        matches = re.findall(r'href="/united-states/trend/[^"]*">([^<]+)</a>', r.text)
        results, seen = [], set()
        for i, word in enumerate(matches):
            word = word.strip().lstrip("#")
            if word and len(word) >= 2 and word.lower() not in seen:
                seen.add(word.lower())
                results.append({"word": word, "rank": i, "source": "twitter"})
        log.info("Twitter Trending: %d 个关键词", len(results))
        return results
    except Exception as e:
        log.warning("Twitter Trending 获取失败: %s", e)
        return []


def fetch_all_hotspots(cfg: dict) -> list[dict]:
    global _hotspot_cache
    now = time.time()
    if now - _hotspot_cache["ts"] < HOTSPOT_CACHE_TTL and _hotspot_cache["keywords"]:
        log.info("使用缓存热点: %d 个关键词", len(_hotspot_cache["keywords"]))
        return _hotspot_cache["keywords"]

    hotspot_cfg = cfg.get("hotspot", {})
    if not hotspot_cfg.get("enabled", True):
        return []

    all_kw: list[dict] = []
    if hotspot_cfg.get("weibo", True):
        all_kw.extend(fetch_weibo_hot())
    if hotspot_cfg.get("google", True):
        geos = tuple(hotspot_cfg.get("google_geos", ["US", "CN"]))
        all_kw.extend(fetch_google_trends(geos))
    if hotspot_cfg.get("twitter", True):
        all_kw.extend(fetch_twitter_trending())

    log.info("热点汇总: %d 个关键词", len(all_kw))
    _hotspot_cache = {"ts": now, "keywords": all_kw}
    return all_kw


def hotspot_match(token: dict, hotspots: list[dict],
                  descr: str = "") -> tuple[float, list[str], bool]:
    """返回 (score, matched_keywords, is_hot)"""
    name = _normalize(token.get("name", ""))
    short = _normalize(token.get("shortName", "") or token.get("symbol", ""))
    desc = _normalize(descr)
    score, matched, seen_words = 0.0, [], set()

    for h in hotspots:
        wl = _normalize(h["word"])
        if wl in seen_words:
            continue
        if len(wl) <= 3:
            if wl != name and wl != short:
                continue
        else:
            found = wl in name or wl in short or (desc and wl in desc)
            if not found and len(name) >= 2:
                found = name in wl or short in wl
            if not found:
                continue
        seen_words.add(wl)
        rank_w = max(0.5, 1.0 - h["rank"] * 0.01)
        src_w = {"weibo": 1.2, "twitter": 1.0}.get(h["source"].split("/")[0], 0.9)
        score += rank_w * src_w
        matched.append(f"{h['word']}({h['source']})")

    return score, matched, len(matched) > 0


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
        return {
            "holders": int(tp.get("holderCount", 0) or 0),
            "price": float(tp.get("price", 0) or 0),
            "totalSupply": int(raw.get("totalAmount", 0) or 0),
            "socialCount": len(social_links),
            "socialLinks": social_links,
            "descr": raw.get("descr", ""),
            "name": raw.get("name", ""),
            "shortName": raw.get("shortName", ""),
            "progress": float(raw.get("progress", 0) or 0),
            "day1Vol": float(tp.get("day1Vol", 0) or raw.get("day1Vol", 0) or 0),
            "liquidity": float(tp.get("liquidity", 0) or 0),
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


def discover_on_chain(from_block: int) -> tuple[list[dict], int]:
    """
    链上发现: 通过 BSC RPC eth_getLogs 查询 four.meme TokenCreated 事件
    返回: (新代币列表, 最新区块号)
    """
    # 获取最新区块号
    block_res = _rpc_call("eth_blockNumber", [])
    if not block_res or not block_res.get("result"):
        log.warning("获取最新区块号失败")
        return [], from_block
    latest_block = int(block_res["result"], 16)

    if from_block <= 0:
        # 首次运行: 只扫最近 15 分钟 (~2000 blocks)
        from_block = latest_block - 2000

    # 安全上限: 不超过 10000 blocks
    if latest_block - from_block > 10000:
        log.warning("区块跨度过大 (%d), 截断到最近 10000 blocks", latest_block - from_block)
        from_block = latest_block - 10000

    log.info("链上扫描区块 %d ~ %d (%d blocks)", from_block, latest_block, latest_block - from_block)

    tokens = []
    chunk = 10000
    current = from_block

    while current <= latest_block:
        end = min(current + chunk - 1, latest_block)
        try:
            res = _rpc_call("eth_getLogs", [{
                "address": FOUR_MEME_CONTRACT,
                "fromBlock": hex(current),
                "toBlock": hex(end),
                "topics": [TOKEN_CREATE_TOPIC],
            }])

            if not res:
                current = end + 1
                continue

            if res.get("error"):
                err_msg = res["error"].get("message", "")
                if "pruned" in err_msg:
                    current = end + 50000
                    continue
                log.warning("RPC error: %s", err_msg)
                current = end + 1
                continue

            for log_entry in (res.get("result") or []):
                data = log_entry["data"][2:]  # 去掉 0x 前缀
                token_addr = ("0x" + data[88:128]).lower()
                # 只保留 four.meme 代币 (后缀 4444 或 ffff)
                if not token_addr.endswith("4444") and not token_addr.endswith("ffff"):
                    continue

                creator_addr = ("0x" + data[24:64]).lower()
                create_ts = int(data[384:448], 16)  # word[6]

                # 解码名称和符号
                name, symbol = "", ""
                try:
                    name_len = int(data[512:576], 16)  # word[8]
                    if 0 < name_len < 200:
                        name = bytes.fromhex(data[576:576 + name_len * 2]).decode("utf-8", errors="replace")
                    # symbol 位置取决于 name 长度 (动态编码)
                    name_words = max(1, (name_len + 31) // 32)
                    sym_len_offset = (9 + name_words) * 64
                    if sym_len_offset + 64 <= len(data):
                        sym_len = int(data[sym_len_offset:sym_len_offset + 64], 16)
                        if 0 < sym_len < 100:
                            symbol = bytes.fromhex(
                                data[sym_len_offset + 64:sym_len_offset + 64 + sym_len * 2]
                            ).decode("utf-8", errors="replace")
                except Exception:
                    pass  # 解码失败, 后续从 detail API 获取

                tokens.append({
                    "address": token_addr,
                    "creator": creator_addr,
                    "createdAt": create_ts * 1000,  # 毫秒
                    "name": name,
                    "symbol": symbol,
                    "block": int(log_entry["blockNumber"], 16),
                })

        except Exception as e:
            log.warning("链上扫描异常: %s", e)
            time.sleep(1)

        current = end + 1
        time.sleep(0.1)

    log.info("链上发现 %d 个新代币", len(tokens))
    return tokens, latest_block


# ===================================================================
#  RPC 查持币地址数 — 通过 eth_getLogs 查 ERC-20 Transfer 事件
#  作为 BSCScan tokenholdercount (PRO 端点) 的免费替代方案
#  通过追踪每个地址的净余额 (转入-转出), 只统计余额>0的地址
# ===================================================================
def rpc_holder_counts(token_infos: list[dict]) -> dict[str, int]:
    """
    RPC 查持币地址数: 通过 eth_getLogs 查 ERC-20 Transfer 事件
    追踪每个地址的净余额 (转入金额 - 转出金额), 只统计余额 > 0 的地址
    token_infos: [{"address": ..., "block": ..., "createdAt": ...}]
    返回: {address: holder_count}
    """
    result = {}
    if not token_infos:
        return result

    # 获取当前区块号
    latest_block = 0
    try:
        block_res = _rpc_call("eth_blockNumber", [])
        if block_res and block_res.get("result"):
            latest_block = int(block_res["result"], 16)
    except Exception:
        pass

    def _query_one(info: dict) -> tuple[str, int | None]:
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
                return addr, None

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
            return addr, holder_count if holder_count > 0 else None

        except Exception:
            return addr, None

    # 并发查询 (10 线程)
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(_query_one, info) for info in token_infos]
        for f in as_completed(futures):
            addr, count = f.result()
            if count is not None:
                result[addr] = count

    log.info("RPC 查到 %d/%d 个代币持币数", len(result), len(token_infos))
    return result


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


def bscscan_get_token_transfers_all(token_address: str,
                                    api_key: str, offset: int = 200) -> list[dict]:
    """通过 BSCScan API 获取代币全量转账记录 (用于聪明钱分析)"""
    d = _bscscan_get({
        "module": "account", "action": "tokentx",
        "contractaddress": token_address,
        "page": 1, "offset": offset,
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
#  聪明钱自动发现 — Top Holders 交叉分析
# ===================================================================
def _discover_smart_money_from_top_holders(cfg: dict, api_key: str) -> set[str]:
    """
    从 GeckoTerminal 热门代币 + RPC Top Holders 交叉分析发现聪明钱地址
    逻辑: GeckoTerminal trending pools → 筛选涨幅>10%且交易量>$50k 的代币
          → RPC eth_getLogs 查每个代币 Top 50 Holders
          → 在 ≥2 个代币中都是大户的地址自动识别为聪明钱
    """
    addresses = set()
    addr_freq: dict[str, int] = {}

    try:
        hot_tokens = _fetch_smart_money_source_tokens(10)
        if not hot_tokens:
            return addresses

        log.info("聪明钱发现: 分析 %d 个热门代币的 Top Holders (RPC)", len(hot_tokens))

        for token_addr in hot_tokens:
            holders = _rpc_top_holders(token_addr, 50)
            for addr in holders:
                if addr and addr not in KNOWN_EXCLUDE_ADDRESSES:
                    addr_freq[addr] = addr_freq.get(addr, 0) + 1
            time.sleep(0.2)

        min_freq = cfg.get("smart_money", {}).get("min_cross_freq", 2)
        for addr, freq in addr_freq.items():
            if freq >= min_freq:
                addresses.add(addr)

        if addresses:
            log.info("聪明钱发现: Top Holders 交叉分析发现 %d 个地址 (出现≥%d次)",
                     len(addresses), min_freq)

    except Exception as e:
        log.debug("聪明钱发现失败: %s", e)

    return addresses


def _fetch_smart_money_source_tokens(limit: int = 10) -> list[str]:
    """
    从 GeckoTerminal trending pools 获取热门代币地址
    筛选: 24h 涨幅 > 10% 且交易量 > $50k
    """
    try:
        data = _gt_request(f"{GT_BASE}/networks/bsc/trending_pools")
        if not data or not data.get("data"):
            return []

        pools = data["data"]
        seen = set()
        tokens = []
        for pool in pools:
            attrs = pool.get("attributes", {})
            h24_change = float((attrs.get("price_change_percentage") or {}).get("h24") or 0)
            h24_vol = float((attrs.get("volume_usd") or {}).get("h24") or 0)
            if h24_change < 10 or h24_vol < 50000:
                continue
            # 提取 base token 地址
            base_token_id = (pool.get("relationships", {})
                             .get("base_token", {})
                             .get("data", {})
                             .get("id", ""))
            addr = base_token_id.replace("bsc_", "").lower()
            if not addr or len(addr) != 42 or addr in seen:
                continue
            if addr in KNOWN_EXCLUDE_ADDRESSES:
                continue
            seen.add(addr)
            tokens.append(addr)
            if len(tokens) >= limit:
                break

        if tokens:
            log.info("聪明钱: GeckoTerminal trending: %d 个涨幅代币", len(tokens))
        return tokens
    except Exception as e:
        log.warning("GeckoTerminal trending 获取失败: %s", e)
        return []


def _rpc_top_holders(token_address: str, top_n: int = 50,
                     from_block: int = 0) -> list[str]:
    """
    RPC 查 Top Holders: 通过 eth_getLogs 查 ERC-20 Transfer 事件
    计算每个地址的净余额, 返回余额最高的 top_n 个地址
    """
    try:
        if from_block > 0:
            fb = hex(max(0, from_block - 1))
        else:
            # 查最近 100000 块 (~3.5天)
            block_res = _rpc_call("eth_blockNumber", [])
            if not block_res or not block_res.get("result"):
                return []
            latest = int(block_res["result"], 16)
            fb = hex(max(0, latest - 100000))

        res = _rpc_call("eth_getLogs", [{
            "address": token_address,
            "fromBlock": fb,
            "toBlock": "latest",
            "topics": [ERC20_TRANSFER_TOPIC],
        }])

        if not res or res.get("error"):
            return []

        logs = res.get("result") or []
        balances: dict[str, int] = {}  # addr -> net balance
        for log_entry in logs:
            topics = log_entry.get("topics", [])
            if len(topics) < 3:
                continue
            from_addr = ("0x" + topics[1][26:]).lower()
            to_addr = ("0x" + topics[2][26:]).lower()
            # 用转账金额计算净余额
            data = log_entry.get("data", "0x0")
            value = int(data, 16) if data and data != "0x" else 0

            if from_addr not in BURN_ADDRESSES and from_addr not in KNOWN_EXCLUDE_ADDRESSES:
                balances[from_addr] = balances.get(from_addr, 0) - value
            if to_addr not in BURN_ADDRESSES and to_addr not in KNOWN_EXCLUDE_ADDRESSES:
                balances[to_addr] = balances.get(to_addr, 0) + value

        # 排序取 Top N (余额 > 0 的)
        positive = [(addr, bal) for addr, bal in balances.items() if bal > 0]
        positive.sort(key=lambda x: x[1], reverse=True)
        return [addr for addr, _ in positive[:top_n]]

    except Exception:
        return []


def load_smart_money_addresses(cfg: dict) -> set[str]:
    """加载聪明钱地址 (带缓存, 多源合并): 手动配置 + Top Holders 交叉分析自动发现"""
    global _smart_money_cache
    now = time.time()
    if now - _smart_money_cache["ts"] < SMART_MONEY_CACHE_TTL and _smart_money_cache["addresses"]:
        return _smart_money_cache["addresses"]

    addresses = set()

    # 来源 1: 手动配置
    sm_cfg = cfg.get("smart_money", {})
    for addr in sm_cfg.get("addresses", []):
        addr_lower = (addr or "").strip().lower()
        if addr_lower and len(addr_lower) == 42 and addr_lower not in KNOWN_EXCLUDE_ADDRESSES:
            addresses.add(addr_lower)
    manual_count = len(addresses)

    # 来源 2: Top Holders 交叉分析自动发现
    discovered = _discover_smart_money_from_top_holders(cfg, "")
    addresses.update(discovered)

    # 排除已知非聪明钱地址
    addresses -= KNOWN_EXCLUDE_ADDRESSES

    log.info("聪明钱地址: %d 个 (手动 %d, 自动发现 %d)",
             len(addresses), manual_count, len(discovered))
    _smart_money_cache = {"ts": now, "addresses": addresses}
    return addresses


# ===================================================================
#  聪明钱行为分析
# ===================================================================
def analyze_smart_money_behavior(token_address: str, smart_addresses: set[str],
                                 api_key: str) -> dict:
    """
    分析聪明钱对该代币的链上行为
    改进: DEX Router 精确匹配买卖方向, 按地址数计数
    """
    result = {
        "has_buy": False, "has_sell": False,
        "buy_count": 0, "sell_count": 0,
        "details": [], "bonus": 0, "exclude": False,
    }
    if not smart_addresses or not api_key:
        return result

    all_transfers = bscscan_get_token_transfers_all(token_address, api_key)
    if not all_transfers:
        return result

    buyers = set()
    sellers = set()

    for tx in all_transfers:
        from_addr = (tx.get("from") or "").lower()
        to_addr = (tx.get("to") or "").lower()
        value = int(tx.get("value", 0) or 0)
        if value <= 0:
            continue

        # 聪明钱买入: 聪明钱是接收方, 来源是 DEX Router 或零地址
        if to_addr in smart_addresses:
            if from_addr in KNOWN_DEX_ROUTERS or from_addr == ZERO_ADDRESS:
                buyers.add(to_addr)

        # 聪明钱卖出: 聪明钱是发送方, 目标是 DEX Router
        if from_addr in smart_addresses:
            if to_addr in KNOWN_DEX_ROUTERS:
                sellers.add(from_addr)

    result["buy_count"] = len(buyers)
    result["sell_count"] = len(sellers)

    if buyers:
        result["has_buy"] = True
        result["details"].append(f"聪明钱加仓 ({len(buyers)}个地址)")
        result["bonus"] += len(buyers)

    if sellers:
        result["has_sell"] = True
        result["details"].append(f"聪明钱减仓 ({len(sellers)}个地址)")
        result["exclude"] = True

    return result


# ===================================================================
#  钱包分析入口 — 批量分析开发者+聪明钱
# ===================================================================
def batch_wallet_analysis(tokens: list[dict], smart_addresses: set[str],
                          api_key: str) -> dict[str, dict]:
    """
    批量分析钱包行为 (并发执行)
    返回 {address: {excluded, excludeReason, signals, bonus, details}}
    """
    result_map = {}
    if not api_key:
        return result_map

    has_smart_money = bool(smart_addresses)

    def _analyze_one(t: dict) -> tuple[str, dict]:
        addr = t.get("address", "")
        creator = t.get("creator", "")

        # 分析开发者行为
        dev = analyze_developer_behavior(addr, creator, api_key)

        # 分析聪明钱行为 (无聪明钱地址时跳过, 省一次 API 调用)
        if has_smart_money:
            sm = analyze_smart_money_behavior(addr, smart_addresses, api_key)
        else:
            sm = {"has_buy": False, "has_sell": False, "buy_count": 0,
                  "sell_count": 0, "details": [], "bonus": 0, "exclude": False}

        # 合并结果
        all_details = dev["details"] + sm["details"]
        total_bonus = dev["bonus"] + sm["bonus"]
        signals = []
        if dev["has_buy"]:
            signals.append("开发者加仓")
        if dev["has_lp_add"]:
            signals.append("开发者加池子")
        if sm["has_buy"]:
            signals.append("聪明钱加仓")

        excluded = False
        exclude_reason = ""
        if dev["exclude"]:
            excluded = True
            exclude_reason = ", ".join(dev["details"])
        if sm["exclude"]:
            excluded = True
            if exclude_reason:
                exclude_reason += ", "
            exclude_reason += ", ".join(sm["details"])

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
    """DexScreener 批量查价格+流动性 (最多 30 个地址/请求)"""
    _ensure_sessions()
    result = {}
    batch_size = 30

    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i + batch_size]
        try:
            url = f"{DS_BASE}/tokens/v1/bsc/{','.join(batch)}"
            r = _gt_session.get(url, timeout=15, headers=DS_HEADERS)
            if r.status_code == 429:
                time.sleep(2)
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
                result[addr] = {
                    "price": float(p.get("priceUsd") or 0),
                    "liquidity": float((p.get("liquidity") or {}).get("usd") or 0),
                    "volume24h": float((p.get("volume") or {}).get("h24") or 0),
                    "name": p["baseToken"].get("name", ""),
                    "symbol": p["baseToken"].get("symbol", ""),
                }
        except Exception as e:
            log.warning("DS 批量查价失败: %s", e)
        if i + batch_size < len(addresses):
            time.sleep(0.3)

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


def gt_ohlcv_direct(token_address: str, limit: int = 72) -> list[list]:
    """直接用 tokenAddress 当 poolAddress 拿 K线"""
    url = f"{GT_BASE}/networks/bsc/pools/{token_address}/ohlcv/hour?aggregate=1&limit={limit}"
    data = _gt_request(url)
    if not data:
        return []
    return (data.get("data", {}).get("attributes", {}).get("ohlcv_list", []))


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
    """计算排除第1根K线后的最低价 (币龄>1h时使用)"""
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
    """计算全部K线的最低价 (币龄≤1h时使用)"""
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
def admission_filter(new_tokens: list[dict], existing_addrs: set[str]) -> list[dict]:
    """
    入场筛: 对新发现的代币调 detail API, 淘汰无社交/总量≠10亿
    返回: [{"token": ..., "detail": ...}, ...]
    """
    admitted = []
    # 过滤已在队列或已淘汰的
    fresh = [t for t in new_tokens if t["address"] not in existing_addrs]
    if not fresh:
        return admitted

    log.info("入场筛: 对 %d 个新代币调 detail API...", len(fresh))

    batch_size = 5
    for i in range(0, len(fresh), batch_size):
        batch = fresh[i:i + batch_size]
        for t in batch:
            detail = fm_detail(t["address"])
            if not detail:
                continue
            # 入场条件: 社交 ≥ 1, 总供应量 = 10亿
            if detail["socialCount"] < MIN_SOCIAL_COUNT:
                continue
            if detail["totalSupply"] != TOTAL_SUPPLY:
                continue
            admitted.append({"token": t, "detail": detail})
            time.sleep(0.2)

    log.info("入场筛: 通过 %d/%d (淘汰 %d: 无社交/总量不符)",
             len(admitted), len(fresh), len(fresh) - len(admitted))
    return admitted


# ===================================================================
#  Step 3: 淘汰检查 — DexScreener + four.meme detail + BSCScan
# ===================================================================
def elimination_check(queue: list[dict], now_ms: int,
                      api_key: str) -> tuple[list[dict], list[dict]]:
    """
    淘汰检查: 对队列中代币定期检查, 永久淘汰弃盘币
    返回: (survivors, eliminated)
    """
    survivors = []
    eliminated = []

    if not queue:
        return survivors, eliminated

    # 1. 币龄淘汰 (无需 API)
    max_age_ms = MAX_AGE_HOURS * 3600 * 1000
    age_filtered = []
    for t in queue:
        if now_ms - t.get("createdAt", 0) > max_age_ms:
            eliminated.append({**t, "eliminatedAt": now_ms,
                               "elimReason": f"币龄>{MAX_AGE_HOURS}h"})
        else:
            age_filtered.append(t)

    if eliminated:
        log.info("淘汰: 币龄超限 %d 个", len(eliminated))

    if not age_filtered:
        return survivors, eliminated

    # 2. DexScreener + RPC 持币数 + four.meme detail 三者并行
    addrs = [t["address"] for t in age_filtered]
    token_infos = [{"address": t["address"], "block": t.get("block", 0),
                    "createdAt": t.get("createdAt", 0)} for t in age_filtered]

    log.info("淘汰检查: 并行查询 %d 个代币 (DexScreener + RPC持币数 + detail)...",
             len(age_filtered))

    def _fetch_all_details():
        detail_map = {}
        for t in age_filtered:
            detail = fm_detail(t["address"])
            if detail:
                detail_map[t["address"]] = detail
            time.sleep(0.2)
        return detail_map

    with ThreadPoolExecutor(max_workers=3) as pool:
        ds_future = pool.submit(ds_batch_prices, addrs)
        rpc_future = pool.submit(rpc_holder_counts, token_infos)
        detail_future = pool.submit(_fetch_all_details)
        ds_data = ds_future.result()
        rpc_holders = rpc_future.result()
        detail_map = detail_future.result()

    # 4. 逐个检查淘汰条件
    for t in age_filtered:
        ds = ds_data.get(t["address"], {})
        detail = detail_map.get(t["address"])
        age_hours = (now_ms - t.get("createdAt", 0)) / 3600000

        # 更新动态数据
        current_price = ds.get("price") or (detail["price"] if detail else 0) or t.get("price", 0)
        rpc_h = rpc_holders.get(t["address"])
        current_holders = rpc_h if rpc_h is not None else (
            detail["holders"] if detail else t.get("holders", 0))
        current_liq = ds.get("liquidity") or t.get("liquidity", 0)
        current_progress = (detail["progress"] if detail else 0) or t.get("progress", 0)

        t["price"] = current_price
        t["holders"] = current_holders
        t["liquidity"] = current_liq
        t["progress"] = current_progress
        if detail:
            t["socialCount"] = detail["socialCount"]
            t["socialLinks"] = detail["socialLinks"]
            t["day1Vol"] = detail.get("day1Vol") or t.get("day1Vol", 0)
        if ds:
            t["name"] = ds.get("name") or t.get("name", "")
            t["symbol"] = ds.get("symbol") or t.get("symbol", "")

        # 更新峰值
        t["peakPrice"] = max(t.get("peakPrice", 0), current_price)
        t["peakHolders"] = max(t.get("peakHolders", 0), current_holders)
        t["peakLiquidity"] = max(t.get("peakLiquidity", 0), current_liq)

        # 连续下跌计数
        last_price = t.get("lastPrice", 0)
        if last_price > 0 and current_price < last_price:
            t["consecDrops"] = t.get("consecDrops", 0) + 1
        else:
            t["consecDrops"] = 0
        t["lastPrice"] = current_price

        # --- 淘汰条件 ---
        elim_reason = None

        # 1. 价格从峰值跌 90%+
        peak = t.get("peakPrice", 0)
        if peak > 0 and current_price > 0 and current_price < peak * (1 - ELIM_PRICE_DROP_PCT):
            elim_reason = (f"价格跌{(1 - current_price / peak) * 100:.0f}% "
                           f"(峰:{peak:.2e} 现:{current_price:.2e})")

        # 2. 持币数从 30+ 跌破 10
        if not elim_reason:
            if (t.get("peakHolders", 0) >= ELIM_HOLDERS_PEAK_MIN
                    and current_holders < ELIM_HOLDERS_FLOOR):
                elim_reason = f"持币数 {t.get('peakHolders', 0)}→{current_holders}"

        # 3. 无社交媒体
        if not elim_reason and detail and detail["socialCount"] < MIN_SOCIAL_COUNT:
            elim_reason = "无社交媒体"

        # 4. 流动性从 >$1k 跌破 $100
        if not elim_reason:
            if (t.get("peakLiquidity", 0) >= ELIM_LIQ_PEAK_MIN
                    and current_liq < ELIM_LIQ_FLOOR):
                elim_reason = f"流动性 ${t.get('peakLiquidity', 0):.0f}→${current_liq:.0f}"

        # 5. 进度 < 1% 且币龄 > 2h
        if not elim_reason:
            if age_hours > ELIM_PROGRESS_AGE_HOURS and current_progress < ELIM_PROGRESS_MIN:
                elim_reason = f"进度{current_progress * 100:.2f}% 币龄{age_hours:.1f}h"

        # 5b. 进度 < 5% 且币龄 > 4h
        if not elim_reason:
            if age_hours > ELIM_PROGRESS_AGE_HOURS_MID and current_progress < ELIM_PROGRESS_MIN_MID:
                elim_reason = f"进度{current_progress * 100:.2f}% 币龄{age_hours:.1f}h"

        # 6. 币龄>5min 最高持币数 < 3
        if not elim_reason:
            if age_hours > ELIM_TINY_AGE_MIN and t.get("peakHolders", 0) < ELIM_TINY_PEAK_HOLDERS:
                elim_reason = f"币龄{age_hours:.1f}h 最高持币仅{t.get('peakHolders', 0)}"

        # 7. 币龄>15min 最高持币数 < 5
        if not elim_reason:
            if age_hours > ELIM_EARLY_AGE_MIN and t.get("peakHolders", 0) < ELIM_EARLY_PEAK_HOLDERS:
                elim_reason = f"币龄{age_hours:.1f}h 最高持币仅{t.get('peakHolders', 0)}"

        # 8. 币龄>1h 最高持币数 < 10
        if not elim_reason:
            if age_hours > ELIM_MID_AGE_HOURS and t.get("peakHolders", 0) < ELIM_MID_PEAK_HOLDERS:
                elim_reason = f"币龄{age_hours:.1f}h 最高持币仅{t.get('peakHolders', 0)}"

        if elim_reason:
            eliminated.append({**t, "eliminatedAt": now_ms, "elimReason": elim_reason})
        else:
            survivors.append(t)

    elim_count = len(eliminated) - (len(queue) - len(age_filtered))
    if elim_count > 0:
        log.info("淘汰: 条件淘汰 %d 个", elim_count)
        for e in eliminated[-elim_count:]:
            log.info("  ✗ %s — %s", e.get("name") or e["address"][:16], e["elimReason"])

    return survivors, eliminated


# ===================================================================
#  Step 5: 精筛 — K线 + 价格比 + 钱包行为排除/加分
# ===================================================================
def quality_filter(candidates: list[dict], now_ms: int,
                   wallet_map: dict[str, dict]) -> list[dict]:
    """
    精筛: 对存活代币执行 K线条件筛选 + 钱包行为排除/加分
    条件:
      - 持币地址数: 币龄>1h ≥60, ≤1h ≥30
      - 当前价: ≤1h ≤$0.0000045, >1h ≤$0.000023 (币龄<4h且价>$0.00001时放宽)
      - 历史最高价 ≤ $0.00004
      - 前2h最高价 ≤ $0.00002 (币龄>1h, 放宽时 ≤$0.000023)
      - 当前价在最高价 40%~90%
      - 现价比底价高 10%~100%
      - 钱包行为排除/加分
    """
    global _gt_rate_delay
    results = []

    for i, t in enumerate(candidates):
        age_hours = (now_ms - t.get("createdAt", 0)) / 3600000
        create_ts_sec = t.get("createdAt", 0) // 1000
        current_price = t.get("price", 0)
        addr = t.get("address", "")
        name = t.get("name") or addr[:16]

        # 钱包行为排除检查
        wa = wallet_map.get(addr)
        if wa and wa["excluded"]:
            log.info("精筛: ✗ %s — 钱包排除: %s", name, wa["excludeReason"])
            continue

        # 价格初筛
        is_relaxed = age_hours < 4 and current_price > 0.00001
        young_limit = MAX_CURRENT_PRICE_YOUNG_RELAXED if is_relaxed else MAX_CURRENT_PRICE_YOUNG
        max_price = MAX_CURRENT_PRICE_OLD if age_hours > 1 else young_limit
        if current_price > max_price:
            continue

        # 持币数
        holders = t.get("holders", 0)
        if age_hours > 1 and holders < HOLDERS_THRESHOLD_OLD:
            continue
        if age_hours <= 1 and holders < HOLDERS_THRESHOLD_YOUNG:
            continue

        # K线 (GeckoTerminal)
        if i > 0:
            time.sleep(_gt_rate_delay)
        candles = gt_ohlcv_direct(addr, 72)

        ath, high2h = None, None
        if candles:
            high2h = calc_max_price_first_n_hours(candles, create_ts_sec, 2)
            ath = calc_all_time_high(candles)

        # 用 K线 ATH 修正队列中的 peakPrice (解决15分钟快照遗漏峰值的问题)
        if ath is not None:
            t["peakPrice"] = max(t.get("peakPrice", 0), ath)

        if ath is None and high2h is None:
            continue
        if ath is None:
            ath = high2h

        if ath > MAX_HIGH_PRICE:
            continue

        # 币龄≤1h 时 ATH 也不能超过 YOUNG 阈值
        if age_hours <= 1 and ath > MAX_CURRENT_PRICE_YOUNG:
            continue

        # 前2h最高价 (币龄>1h时检查)
        early_high_limit = MAX_EARLY_HIGH_PRICE_RELAXED if is_relaxed else MAX_EARLY_HIGH_PRICE
        if age_hours > 1 and high2h is not None and high2h > early_high_limit:
            continue

        # 现价/最高价比
        if ath > 0 and current_price:
            ratio = current_price / ath
            if ratio < PRICE_RATIO_LOW or ratio > PRICE_RATIO_HIGH:
                continue

        # 底价检查
        if current_price and candles and len(candles) >= 1:
            min_price = (calc_min_price_exclude_first(candles, create_ts_sec)
                         if age_hours > 1
                         else calc_min_price_all(candles))
            if min_price and min_price > 0:
                above_min_ratio = current_price / min_price - 1
                if above_min_ratio < FLOOR_RATIO_LOW or above_min_ratio > FLOOR_RATIO_HIGH:
                    continue

        results.append({
            **t,
            "ath": ath,
            "high2h": high2h,
            "walletSignals": wa["signals"] if wa else [],
            "walletAnalysis": wa,
        })
        sig_str = f" 💰 {', '.join(wa['signals'])}" if wa and wa["signals"] else ""
        log.info("精筛: ✓ %s — ATH %.3e, 现价 %.3e, 持币 %d%s",
                 name, ath, current_price, holders, sig_str)

    return results


# ===================================================================
#  消息格式化 & 推送
# ===================================================================
def format_message(results: list[dict]) -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"🔍 <b>BSC Token Scanner 报告</b>", f"⏰ {now_str}", ""]

    for i, item in enumerate(results, 1):
        addr = item.get("address", "")
        name = item.get("name", "Unknown")
        symbol = item.get("symbol", "")
        price = item.get("price", 0)
        holders = item.get("holders", 0)
        age_hours = (int(time.time() * 1000) - item.get("createdAt", 0)) / 3600000

        lines.append(f"<b>#{i} {name} ({symbol})</b>")
        lines.append(f"📄 合约: <code>{addr}</code>")
        lines.append(f"💰 当前价: ${price:.10f}")
        if item.get("ath"):
            lines.append(f"📈 历史最高: ${item['ath']:.10f}")
        if item.get("high2h"):
            lines.append(f"📊 前2h最高: ${item['high2h']:.10f}")
        if item.get("ath") and price:
            lines.append(f"📉 现/高: {price / item['ath'] * 100:.1f}%")
        lines.append(f"👥 持币: {holders}")
        lines.append(f"🔗 社交: {item.get('socialCount', 0)} 个")
        social = item.get("socialLinks", {})
        for stype, url in social.items():
            lines.append(f"  • <a href='{url}'>{stype}</a>")
        lines.append(f"🕐 币龄: {age_hours:.1f}h")
        if item.get("hotNews", {}).get("isHot"):
            lines.append(f"🔥 热点: {', '.join(item['hotNews'].get('matched', []))}")
        wa = item.get("walletAnalysis")
        if wa and wa.get("details"):
            lines.append(f"🔗 链上: {', '.join(wa['details'])}")
        if wa and wa.get("bonus", 0) > 0:
            lines.append(f"⭐ 加分: +{wa['bonus']}")
        desc = (item.get("descr") or "").strip()
        if desc:
            lines.append(f"📝 {desc[:100]}{'...' if len(desc) > 100 else ''}")
        lines.append(
            f"🌐 <a href='https://four.meme/token/{addr}'>four.meme</a>"
            f" | <a href='https://bscscan.com/token/{addr}'>BscScan</a>"
            f" | <a href='https://web3.binance.com/zh-CN/token/bsc/{addr}'>币安钱包</a>"
        )
        lines.append("")

    lines.append("—— BSC Token Scanner v5 ——")
    return "\n".join(lines)


def send_telegram(bot_token: str, chat_id: str, text: str) -> bool:
    _ensure_sessions()
    try:
        r = _fm_session.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("ok", False)
    except Exception as e:
        log.error("Telegram: %s", e)
        return False


def print_console(msg: str) -> None:
    out = re.sub(r"<[^>]+>", "", msg)
    print("\n" + "=" * 60)
    print(out)
    print("=" * 60 + "\n")


# ===================================================================
#  主扫描流程
# ===================================================================
def scan_once(cfg: dict) -> None:
    log.info("=" * 50)
    log.info("开始扫描")
    log.info("=" * 50)
    max_push = cfg.get("max_push_count", 100)
    bscscan_key = cfg.get("bscscan_api_key", "")

    # 行情
    ticker = fm_ticker_prices()
    bnb = ticker.get("BNB", 0)
    if bnb <= 0:
        log.warning("BNB 价格获取失败, 使用默认 600")
        ticker["BNB"] = 600.0
    log.info("BNB=$%.2f", ticker.get("BNB", 0))

    # 每次扫描前重新同步持仓
    if _HAS_TRADER and cfg.get("trading", {}).get("enabled", False):
        try:
            bnb_usd = ticker.get("BNB", 600.0)
            _sync_positions_from_wallet(bnb_usd)
        except Exception as e:
            log.warning("扫描前持仓同步失败: %s", e)

    now_ms = int(time.time() * 1000)

    # 加载队列
    queue_state = load_queue()
    existing_addrs = set(
        [t["address"] for t in queue_state.get("tokens", [])]
        + [t["address"] for t in queue_state.get("eliminated", [])]
    )

    # Step 1: 链上发现
    log.info("\n--- Step 1: 链上发现 ---")
    new_on_chain, latest_block = discover_on_chain(queue_state.get("lastBlock", 0))

    # Step 2: 入场筛
    log.info("\n--- Step 2: 入场筛 ---")
    admitted = admission_filter(new_on_chain, existing_addrs)

    # 将通过入场筛的代币加入队列 (新代币刚创建, 用 detail API 的 holders 即可)
    if admitted:
        for item in admitted:
            token = item["token"]
            detail = item["detail"]
            queue_state["tokens"].append({
                "address": token["address"],
                "creator": token.get("creator", ""),
                "block": token.get("block", 0),
                "name": detail.get("name") or token.get("name", ""),
                "symbol": detail.get("shortName") or token.get("symbol", ""),
                "createdAt": token["createdAt"],
                "addedAt": now_ms,
                "totalSupply": detail["totalSupply"],
                "socialCount": detail["socialCount"],
                "socialLinks": detail["socialLinks"],
                "descr": detail.get("descr", ""),
                "price": detail["price"],
                "peakPrice": detail["price"],
                "holders": detail["holders"],
                "peakHolders": detail["holders"],
                "liquidity": detail.get("liquidity", 0),
                "peakLiquidity": detail.get("liquidity", 0),
                "progress": detail.get("progress", 0),
                "day1Vol": detail.get("day1Vol", 0),
                "consecDrops": 0,
                "lastPrice": detail["price"],
            })

    log.info("入队后: %d 个代币", len(queue_state["tokens"]))

    # Step 3: 淘汰检查
    log.info("\n--- Step 3: 淘汰检查 ---")
    survivors, eliminated = elimination_check(queue_state["tokens"], now_ms, bscscan_key)
    queue_state["tokens"] = survivors
    queue_state["eliminated"].extend([{
        "address": e["address"], "name": e.get("name", ""),
        "elimReason": e["elimReason"], "eliminatedAt": e["eliminatedAt"],
        "createdAt": e.get("createdAt", 0),
    } for e in eliminated])

    log.info("淘汰后: %d 个存活, %d 个淘汰", len(survivors), len(eliminated))

    # Step 4: 钱包分析 + 精筛 + 热点匹配
    log.info("\n--- Step 4: 钱包分析 + 精筛 ---")

    # 钱包行为分析 (开发者+聪明钱)
    wallet_map = {}
    if bscscan_key and survivors:
        smart_addresses = load_smart_money_addresses(cfg)
        log.info("分析 %d 个代币的开发者/聪明钱行为...", len(survivors))
        wallet_map = batch_wallet_analysis(survivors, smart_addresses, bscscan_key)
        excluded_count = sum(1 for w in wallet_map.values() if w["excluded"])
        signal_count = sum(1 for w in wallet_map.values() if w["signals"])
        log.info("钱包分析: 排除 %d, 有加分信号 %d", excluded_count, signal_count)

    # 精筛
    quality_results = quality_filter(survivors, now_ms, wallet_map)

    # 热点匹配
    hotspots = fetch_all_hotspots(cfg)
    for t in quality_results:
        score, matched, is_hot = hotspot_match(t, hotspots, t.get("descr", ""))
        t["hotNews"] = {"score": score, "matched": matched, "isHot": is_hot}

    # 按持币数排序
    quality_results.sort(key=lambda x: (x.get("holders", 0)), reverse=True)

    log.info("精筛通过: %d/%d", len(quality_results), len(survivors))

    # 更新队列状态
    queue_state["lastBlock"] = latest_block
    queue_state["lastScanTime"] = now_ms
    save_queue(queue_state)

    if not quality_results:
        log.info("本轮无推荐代币")
        return

    # 推送
    filtered = quality_results[:max_push]
    msg = format_message(filtered)
    log.info("筛选通过 %d 个代币", len(filtered))

    bot_token = cfg.get("telegram_bot_token", "")
    chat_id = cfg.get("telegram_chat_id", "")
    if not bot_token or not chat_id or "YOUR" in bot_token:
        log.warning("Telegram 未配置, 仅打印:")
        print_console(msg)
    else:
        ok = send_telegram(bot_token, chat_id, msg)
        log.info("Telegram 推送%s", "成功" if ok else "失败")

    # 自动买入
    if _HAS_TRADER and cfg.get("trading", {}).get("enabled", False):
        bnb_usd = ticker.get("BNB", 600.0)
        # 构造兼容 trader 模块的数据格式
        to_buy = []
        for item in filtered:
            token_data = {
                "tokenAddress": item["address"],
                "name": item.get("name", ""),
                "shortName": item.get("symbol", ""),
            }
            detail_data = {
                "holders": item.get("holders", 0),
                "price": item.get("price", 0),
                "socialCount": item.get("socialCount", 0),
                "socialLinks": item.get("socialLinks", {}),
            }
            to_buy.append((token_data, detail_data))
        execute_buys(to_buy, cfg, bnb_usd)


def main():
    global _fm_session, _gt_session, _bsc_session
    log.info("🚀 BSC Token Scanner v5 启动")
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
            interval = cfg.get("scan_interval_minutes", 15)
            log.info("下次扫描: %d 分钟后", interval)
            time.sleep(interval * 60)
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

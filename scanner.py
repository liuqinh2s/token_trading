"""
BSC Token Scanner - four.meme 新币扫描器 v3
数据源: four.meme API (代币发现/详情) + DexScreener (主要价格) + GeckoTerminal (备选K线)

三级筛选管线:
  Stage 1 (初筛): 币龄≤3天、当前价≤$0.00002、持币地址粗筛 — 仅用 search API 批量数据
  Stage 2 (详情筛): 社交媒体≥1、持币(>1h:≥60,≤1h:≥30)、总量=10亿、当前价分段 — detail API
  Stage 3 (K线筛): 历史最高价≤$0.00004、前2h最高价≤$0.00002(>1h)、价在最高价40%~90%、现价比底价高10%~50%(>1h,排除首根K线) — DexScreener+GT
"""

from __future__ import annotations

import json
import time
import logging
import re
import sys
import xml.etree.ElementTree as ET
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
BSCSCAN_API = "https://api.bscscan.com/api"

FM_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Origin": "https://four.meme",
    "Referer": "https://four.meme/",
}
GT_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
DS_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

# 筛选阈值 (与新项目同步)
MAX_AGE_HOURS = 72
TOTAL_SUPPLY = 1_000_000_000           # 10亿
MAX_CURRENT_PRICE_OLD = 0.00002        # 币龄 > 1h 当前价格上限 (USD)
MAX_CURRENT_PRICE_YOUNG = 0.000004     # 币龄 ≤ 1h 当前价格上限 (USD)
MAX_HIGH_PRICE = 0.00004               # 历史最高价上限 (USD)
MAX_EARLY_HIGH_PRICE = 0.00002         # 前2小时最高价上限 (USD, 币龄>1h时检查)
PRICE_RATIO_LOW = 0.4                  # 当前价 ≥ 最高价 * 40%
PRICE_RATIO_HIGH = 0.9                 # 当前价 ≤ 最高价 * 90%
FLOOR_RATIO_LOW = 0.1                  # 现价比底价高 ≥ 10% (币龄>1h, 排除首根K线)
FLOOR_RATIO_HIGH = 0.5                 # 现价比底价高 ≤ 50% (币龄>1h, 排除首根K线)
HOLDERS_THRESHOLD_OLD = 60             # 币龄 > 1h 时持币地址数阈值
HOLDERS_THRESHOLD_YOUNG = 30           # 币龄 ≤ 1h 时持币地址数阈值
MIN_SOCIAL_COUNT = 1                   # 最少关联社交媒体数

pushed_tokens: set[str] = set()
MAX_CACHE = 5000

# GeckoTerminal 动态速率控制
_gt_rate_delay: float = 2.0


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


def _ensure_sessions():
    global _fm_session, _gt_session
    if _fm_session is None:
        try:
            cfg = load_config()
            proxy = cfg.get("proxy")
        except Exception:
            proxy = None
        _fm_session = _build_session(proxy, FM_HEADERS)
        _gt_session = _build_session(proxy, GT_HEADERS)


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
    short = _normalize(token.get("shortName", ""))
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
def fm_search_tokens(cfg: dict) -> list[dict]:
    """
    全量扫描 four.meme 代币, 多维度查询组合最大化覆盖。
    维度: symbol × sort × status × type
    """
    _ensure_sessions()
    max_age_ms = cfg.get("max_age_hours", MAX_AGE_HOURS) * 3600 * 1000
    now_ms = int(time.time() * 1000)
    seen: dict[str, dict] = {}
    page_size = 100
    max_pages = 10

    def _fetch_pages(query: dict, label: str):
        for page in range(1, max_pages + 1):
            payload = {"pageIndex": page, "pageSize": page_size, **query}
            try:
                r = _fm_session.post(FM_SEARCH, json=payload, timeout=15)
                r.raise_for_status()
                d = r.json()
                if d.get("code") != 0:
                    break
                items = d.get("data", [])
                if not items:
                    break
                for t in items:
                    addr = (t.get("tokenAddress") or "").lower()
                    if addr and addr not in seen:
                        seen[addr] = t
                if query.get("type") == "NEW":
                    oldest_ts = min(int(t.get("createDate", 0)) for t in items)
                    if oldest_ts > 0 and (now_ms - oldest_ts) > max_age_ms:
                        break
                if len(items) < page_size:
                    break
            except Exception as e:
                log.error("fm_search [%s] p%d: %s", label, page, e)
                break
            time.sleep(0.3)

    symbols = ("BNB", "USD1", "USDT", "CAKE")

    # NEW × DESC/ASC × PUBLISH/TRADE × 4 symbols
    for sym in symbols:
        _fetch_pages({"type": "NEW", "listType": "NOR", "sort": "DESC",
                      "status": "PUBLISH", "symbol": sym}, f"NEW/DESC/PUB/{sym}")
    for sym in symbols:
        _fetch_pages({"type": "NEW", "listType": "NOR", "sort": "ASC",
                      "status": "PUBLISH", "symbol": sym}, f"NEW/ASC/PUB/{sym}")
    for sym in symbols:
        _fetch_pages({"type": "NEW", "listType": "NOR_DEX", "sort": "DESC",
                      "status": "TRADE", "symbol": sym}, f"NEW/DESC/TRADE/{sym}")
    for sym in symbols:
        _fetch_pages({"type": "NEW", "listType": "NOR_DEX", "sort": "ASC",
                      "status": "TRADE", "symbol": sym}, f"NEW/ASC/TRADE/{sym}")

    # HOT/VOL/PROGRESS × 双状态
    for sort_type, list_type in [("HOT", "ADV"), ("VOL", "NOR"), ("PROGRESS", "NOR")]:
        for status, lt in [("PUBLISH", list_type), ("TRADE", "NOR_DEX")]:
            _fetch_pages({"type": sort_type, "listType": lt, "status": status},
                         f"{sort_type}/{status}")

    log.info("fm_search: 共获取 %d 个代币 (去重后)", len(seen))
    return list(seen.values())


def fm_detail(address: str) -> dict | None:
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
#  BSCScan API — 链上真实持仓地址数
# ===================================================================
def bscscan_holder_count(token_address: str, api_key: str) -> int | None:
    """通过 BSCScan API 获取链上真实持仓地址数"""
    if not api_key:
        return None
    _ensure_sessions()
    try:
        r = _gt_session.get(BSCSCAN_API, params={
            "module": "token", "action": "tokenholdercount",
            "contractaddress": token_address, "apikey": api_key,
        }, timeout=8)
        r.raise_for_status()
        d = r.json()
        if d.get("status") == "1" and d.get("result"):
            return int(d["result"])
    except Exception:
        pass
    return None


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


def ds_extract_prices(pairs: list[dict]) -> dict | None:
    """从 DexScreener pair 数据提取价格信息, 用 priceChange 反推历史高点"""
    if not pairs:
        return None
    for pair in pairs:
        if pair.get("chainId") and pair["chainId"] != "bsc":
            continue
        price_usd = float(pair.get("priceUsd") or 0)
        if not price_usd:
            continue
        max_price = price_usd
        pc = pair.get("priceChange", {})
        for key in ("m5", "h1", "h6", "h24"):
            if pc.get(key) is not None:
                try:
                    pct = float(pc[key])
                    if pct < 0:
                        max_price = max(max_price, price_usd / (1 + pct / 100))
                except (ValueError, TypeError):
                    pass
        return {"ath": max_price, "high2h": max_price, "currentPrice": price_usd,
                "name": (pair.get("baseToken") or {}).get("name"),
                "symbol": (pair.get("baseToken") or {}).get("symbol")}
    return None


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


def calc_first_2h_max(candles: list[list], create_ts_sec: int) -> float | None:
    if not candles:
        return None
    cutoff = create_ts_sec + 2 * 3600
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


def calc_floor_price(candles: list[list], create_ts_sec: int) -> float | None:
    """
    计算底价: 排除第1根1小时K线后, 所有K线的最低价中的最小值。
    用于币龄>1h的代币, 判断现价是否在底价之上10%~50%。
    """
    if not candles:
        return None
    first_hour_cutoff = create_ts_sec + 3600
    min_low, found = float("inf"), False
    for c in candles:
        ts = int(c[0])
        if ts < first_hour_cutoff:
            continue  # 排除首根1小时K线
        low = float(c[3])  # OHLCV: [ts, open, high, low, close, volume]
        if low < min_low:
            min_low = low
        found = True
    return min_low if found else None


# ===================================================================
#  三级筛选管线
# ===================================================================
def stage1_prefilter(tokens: list[dict], now_ms: int) -> list[dict]:
    """
    Stage 1 初筛 — 仅用 search API 批量数据, 0 额外请求
    条件: 币龄≤72h, 当前价≤MAX_CURRENT_PRICE_OLD, 持币地址粗筛(半阈值)
    """
    max_age_ms = MAX_AGE_HOURS * 3600 * 1000
    results = []
    for t in tokens:
        create_date = int(t.get("createDate", 0) or 0)
        if create_date <= 0:
            continue
        age_ms = now_ms - create_date
        if age_ms <= 0 or age_ms > max_age_ms:
            continue
        price = float(t.get("price", 0) or 0)
        if price > MAX_CURRENT_PRICE_OLD:
            continue
        hold = int(t.get("hold", 0) or 0)
        age_hours = age_ms / (3600 * 1000)
        min_hold = (HOLDERS_THRESHOLD_OLD * 0.5) if age_hours > 1 else (HOLDERS_THRESHOLD_YOUNG * 0.5)
        if hold < min_hold:
            continue
        addr = (t.get("tokenAddress") or "").lower()
        if addr in pushed_tokens:
            continue
        results.append(t)
    log.info("Stage1 初筛: %d/%d 通过", len(results), len(tokens))
    return results


def stage2_detail(candidates: list[dict], now_ms: int,
                  bscscan_key: str = "") -> list[dict]:
    """
    Stage 2 详情筛 — four.meme detail API, 每候选 1 请求
    条件: 社交媒体≥1, 持币(>1h:≥60,≤1h:≥30), 总量=10亿, 当前价分段
    """
    results = []
    for i, t in enumerate(candidates):
        addr = t.get("tokenAddress", "")
        if i > 0:
            time.sleep(0.3)
        detail = fm_detail(addr)
        if not detail:
            continue

        # BSCScan 链上持仓覆盖 (更准确)
        onchain_holders = bscscan_holder_count(addr, bscscan_key)
        if onchain_holders is not None and onchain_holders > 0:
            detail["holders"] = onchain_holders

        create_date = int(t.get("createDate", 0) or 0)
        age_hours = (now_ms - create_date) / (3600 * 1000)

        # 社交媒体 ≥ 1
        if detail["socialCount"] < MIN_SOCIAL_COUNT:
            continue
        # 持币地址数 (按币龄区分)
        if age_hours > 1 and detail["holders"] < HOLDERS_THRESHOLD_OLD:
            continue
        if age_hours <= 1 and detail["holders"] < HOLDERS_THRESHOLD_YOUNG:
            continue
        # 总量 = 10亿
        if detail["totalSupply"] != TOTAL_SUPPLY:
            continue
        # 当前价: 币龄≤1h → ≤0.000004, 币龄>1h → ≤0.00002
        max_price = MAX_CURRENT_PRICE_OLD if age_hours > 1 else MAX_CURRENT_PRICE_YOUNG
        if detail["price"] > max_price:
            continue

        results.append({"token": t, "detail": detail, "ageHours": age_hours})

        if i % 10 == 9:
            log.info("  Stage2: %d/%d 已检查, 通过 %d", i + 1, len(candidates), len(results))

    log.info("Stage2 详情筛: %d/%d 通过", len(results), len(candidates))
    return results


def stage3_kline(candidates: list[dict], hotspots: list[dict]) -> list[dict]:
    """
    Stage 3 K线筛 — 两阶段: DS快筛(现价) → GT精筛(K线)
    条件: ATH≤$0.00004, 前2h最高(>1h)≤$0.00002, 现价/ATH在40%~90%,
          现价比底价高10%~50%(币龄>1h,排除首根K线)
    """
    global _gt_rate_delay

    # Phase A: DexScreener 批量获取现价, 快速淘汰
    ds_data: dict[str, dict] = {}
    for cand in candidates:
        addr = cand["token"]["tokenAddress"]
        pairs = ds_get_pairs(addr)
        ds_info = ds_extract_prices(pairs) if pairs else None
        ds_data[addr] = ds_info or {}
        time.sleep(0.2)

    ds_filtered = []
    for cand in candidates:
        addr = cand["token"]["tokenAddress"]
        name = cand["token"].get("name", addr[:16])
        ds = ds_data.get(addr, {})
        ds_price = ds.get("currentPrice")
        if ds_price:
            max_cur = MAX_CURRENT_PRICE_OLD if cand["ageHours"] > 1 else MAX_CURRENT_PRICE_YOUNG
            if ds_price > max_cur:
                log.info("  Stage3-A: %s — DS现价 %.2e > %.2e, 淘汰", name, ds_price, max_cur)
                continue
        ds_filtered.append(cand)

    log.info("Stage3-A: DS快筛 %d/%d 通过", len(ds_filtered), len(candidates))

    # Phase B: GeckoTerminal K线精筛
    results = []
    for i, cand in enumerate(ds_filtered):
        t = cand["token"]
        detail = cand["detail"]
        age_hours = cand["ageHours"]
        addr = t["tokenAddress"]
        name = t.get("name", addr[:16])
        create_ts_sec = int(t.get("createDate", 0)) // 1000
        ds = ds_data.get(addr, {})
        ds_current = ds.get("currentPrice")
        ds_name = ds.get("name")
        ds_symbol = ds.get("symbol")

        # GT K线
        if i > 0:
            time.sleep(_gt_rate_delay)
        candles = gt_ohlcv_direct(addr, 72)

        ath, high2h, gt_current = None, None, None
        if candles:
            high2h = calc_first_2h_max(candles, create_ts_sec)
            ath = calc_all_time_high(candles)
            latest = max(candles, key=lambda c: int(c[0]))
            gt_current = float(latest[4])

        current_price = ds_current or gt_current
        floor_price = calc_floor_price(candles, create_ts_sec) if candles else None

        if ath is None and high2h is None:
            log.info("  Stage3-B: %s — 无K线数据, 跳过", name)
            continue
        if ath is None:
            ath = high2h

        log.info("  Stage3-B: %s — ATH %.2e, 2h高 %.2e, 底价 %.2e, 现价 %.2e",
                 name, ath or 0, high2h or 0, floor_price or 0, current_price or 0)

        # ATH 检查
        if ath > MAX_HIGH_PRICE:
            log.info("  Stage3-B: %s — ATH %.2e > %.2e, 跳过", name, ath, MAX_HIGH_PRICE)
            continue

        # 币龄≤1h 时 ATH 也不能超过 YOUNG 阈值
        if age_hours <= 1 and ath > MAX_CURRENT_PRICE_YOUNG:
            log.info("  Stage3-B: %s — 币龄≤1h, ATH %.2e > %.2e, 跳过",
                     name, ath, MAX_CURRENT_PRICE_YOUNG)
            continue

        # 现价检查
        if current_price:
            max_cur = MAX_CURRENT_PRICE_OLD if age_hours > 1 else MAX_CURRENT_PRICE_YOUNG
            if current_price > max_cur:
                log.info("  Stage3-B: %s — 现价 %.2e > %.2e, 跳过", name, current_price, max_cur)
                continue

        # 前2h最高价 (币龄>1h时检查)
        if age_hours > 1 and high2h is not None and high2h > MAX_EARLY_HIGH_PRICE:
            log.info("  Stage3-B: %s — 前2h最高 %.2e > %.2e, 跳过",
                     name, high2h, MAX_EARLY_HIGH_PRICE)
            continue

        # 价格区间: 现价/ATH 在 40%~90% (币龄≥1h时检查)
        if age_hours >= 1 and ath > 0 and current_price:
            ratio = current_price / ath
            if ratio < PRICE_RATIO_LOW or ratio > PRICE_RATIO_HIGH:
                log.info("  Stage3-B: %s — 现/高 %.1f%% 不在 40%%~90%%, 跳过",
                         name, ratio * 100)
                continue

        # 底价检查: 现价比底价高10%~50% (币龄>1h, 排除首根K线)
        if age_hours > 1 and floor_price and floor_price > 0 and current_price:
            floor_ratio = (current_price - floor_price) / floor_price
            if floor_ratio < FLOOR_RATIO_LOW or floor_ratio > FLOOR_RATIO_HIGH:
                log.info("  Stage3-B: %s — 现价比底价高 %.1f%%, 不在 10%%~50%%, 跳过",
                         name, floor_ratio * 100)
                continue

        # 热点匹配
        score, matched, is_hot = hotspot_match(t, hotspots, detail.get("descr", ""))

        results.append({
            "token": t, "detail": detail, "ageHours": age_hours,
            "ath": ath, "high2h": high2h, "currentPrice": current_price,
            "floorPrice": floor_price,
            "hotScore": score, "hotMatched": matched, "isHot": is_hot,
            "dsName": ds_name, "dsSymbol": ds_symbol,
        })
        hot_tag = f" 🔥{','.join(matched)}" if is_hot else ""
        floor_tag = f", 底+{((current_price - floor_price) / floor_price * 100):.1f}%" if (
            age_hours > 1 and floor_price and floor_price > 0 and current_price) else ""
        log.info("  Stage3-B: ✓ %s — ATH %.2e, 2h高 %.2e, 现/高 %.1f%%%s%s",
                 name, ath, high2h or 0,
                 (current_price / ath * 100) if ath > 0 and current_price else 0,
                 floor_tag, hot_tag)

    log.info("Stage3 K线筛: %d/%d 通过", len(results), len(ds_filtered))
    return results


# ===================================================================
#  消息格式化 & 推送
# ===================================================================
def format_message(results: list[dict]) -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"🔍 <b>BSC Token Scanner 报告</b>", f"⏰ {now_str}", ""]

    for i, item in enumerate(results, 1):
        t = item["token"]
        detail = item["detail"]
        addr = t.get("tokenAddress", "")
        name = t.get("name", "Unknown")
        short = t.get("shortName", "")
        price = item.get("currentPrice") or detail.get("price", 0)
        holders = detail.get("holders", 0)

        lines.append(f"<b>#{i} {name} ({short})</b>")
        lines.append(f"📄 合约: <code>{addr}</code>")
        lines.append(f"💰 当前价: ${price:.10f}")
        if item.get("ath"):
            lines.append(f"📈 历史最高: ${item['ath']:.10f}")
        if item.get("high2h"):
            lines.append(f"📊 前2h最高: ${item['high2h']:.10f}")
        if item.get("floorPrice"):
            lines.append(f"📉 底价: ${item['floorPrice']:.10f}")
        if item.get("ath") and price:
            lines.append(f"📉 现/高: {price / item['ath'] * 100:.1f}%")
        if item.get("floorPrice") and item["floorPrice"] > 0 and price:
            lines.append(f"📊 现/底: +{(price - item['floorPrice']) / item['floorPrice'] * 100:.1f}%")
        lines.append(f"👥 持币: {holders}")
        lines.append(f"🔗 社交: {detail.get('socialCount', 0)} 个")
        social = detail.get("socialLinks", {})
        for stype, url in social.items():
            lines.append(f"  • <a href='{url}'>{stype}</a>")
        lines.append(f"🕐 币龄: {item['ageHours']:.1f}h")
        if item.get("isHot"):
            lines.append(f"🔥 热点: {', '.join(item['hotMatched'])}")
        desc = (detail.get("descr") or "").strip()
        if desc:
            lines.append(f"📝 {desc[:100]}{'...' if len(desc) > 100 else ''}")
        lines.append(
            f"🌐 <a href='https://four.meme/token/{addr}'>four.meme</a>"
            f" | <a href='https://bscscan.com/token/{addr}'>BscScan</a>"
            f" | <a href='https://dexscreener.com/bsc/{addr}'>DexScreener</a>"
        )
        lines.append("")

    lines.append("—— BSC Token Scanner v3 ——")
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
    global pushed_tokens
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

    # 每次扫描前重新同步持仓 (处理手动平仓等情况)
    if _HAS_TRADER and cfg.get("trading", {}).get("enabled", False):
        try:
            bnb_usd = ticker.get("BNB", 600.0)
            _sync_positions_from_wallet(bnb_usd)
        except Exception as e:
            log.warning("扫描前持仓同步失败: %s", e)

    now_ms = int(time.time() * 1000)

    # Step 1: 获取代币列表
    log.info("从 four.meme API 获取代币列表...")
    api_tokens = fm_search_tokens(cfg)
    log.info("获取 %d 个代币", len(api_tokens))

    # Stage 1: 初筛
    log.info("Stage1 条件: 币龄≤%dh, 当前价≤$%s, 持币≥(>1h:%d, ≤1h:%d)×0.5",
             MAX_AGE_HOURS, MAX_CURRENT_PRICE_OLD,
             HOLDERS_THRESHOLD_OLD, HOLDERS_THRESHOLD_YOUNG)
    s1 = stage1_prefilter(api_tokens, now_ms)
    if not s1:
        log.info("初筛无结果")
        return

    # Stage 2 + 热点并行
    log.info("Stage2 条件: 社交≥%d, 持币(>1h:≥%d, ≤1h:≥%d), 总量=%s, 当前价分段",
             MIN_SOCIAL_COUNT, HOLDERS_THRESHOLD_OLD, HOLDERS_THRESHOLD_YOUNG, TOTAL_SUPPLY)
    hotspots = fetch_all_hotspots(cfg)
    s2 = stage2_detail(s1, now_ms, bscscan_key)
    if not s2:
        log.info("详情筛无结果")
        return

    # Stage 3: K线筛
    log.info("Stage3 条件: ATH≤$%s, 前2h最高(>1h)≤$%s, 现/高在%.0f%%~%.0f%%, 现价比底价高%.0f%%~%.0f%%(>1h)",
             MAX_HIGH_PRICE, MAX_EARLY_HIGH_PRICE,
             PRICE_RATIO_LOW * 100, PRICE_RATIO_HIGH * 100,
             FLOOR_RATIO_LOW * 100, FLOOR_RATIO_HIGH * 100)
    s3 = stage3_kline(s2, hotspots)
    if not s3:
        log.info("K线筛无结果")
        return

    # 按持币数降序排列
    s3.sort(key=lambda x: x["detail"]["holders"], reverse=True)
    filtered = s3[:max_push]

    # 推送
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

    for item in filtered:
        pushed_tokens.add(item["token"]["tokenAddress"].lower())
    if len(pushed_tokens) > MAX_CACHE:
        pushed_tokens = set(list(pushed_tokens)[-MAX_CACHE // 2:])

    # 自动买入
    if _HAS_TRADER and cfg.get("trading", {}).get("enabled", False):
        bnb_usd = ticker.get("BNB", 600.0)
        to_buy = [(item["token"], item["detail"]) for item in filtered]
        execute_buys(to_buy, cfg, bnb_usd)


def main():
    global _fm_session, _gt_session
    log.info("🚀 BSC Token Scanner v3 启动")
    log.info("配置文件: %s", CONFIG_PATH)

    # 初始化交易模块
    try:
        cfg = load_config()
        if _HAS_TRADER and cfg.get("trading", {}).get("enabled", False):
            # 先获取 BNB 价格, 用于钱包扫描
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

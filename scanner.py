"""
BSC 土狗扫盘程序 - four.meme 新币扫描器 v2
数据源: four.meme API (代币发现/详情) + GeckoTerminal API (K线数据)

筛选条件：
1. 代币总量 = 10亿, 发行时间 > 4小时 且 < 3天
2. 历史最高价 ≤ 0.00014 USD, 前2小时最高价 ≤ 0.00004 USD, 当前价 ≤ 0.00002 USD
3. 持币地址数 ≥ 150
4. 关联社交媒体 ≥ 1
5. 有对应热点新闻
"""

from __future__ import annotations

import json
import time
import logging
import re
import sys
import sqlite3
import xml.etree.ElementTree as ET
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone
from pathlib import Path

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

FM_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Origin": "https://four.meme",
    "Referer": "https://four.meme/",
}
GT_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

pushed_tokens: set[str] = set()
MAX_CACHE = 5000

DB_PATH = Path(__file__).parent / "tokens.db"


# ===================================================================
#  SQLite 本地代币缓存
#  跨轮次累积代币, 解决 API 分页上限 (10页×100) 无法覆盖全时间窗口的问题
#  每 15 分钟扫一次, 每次覆盖最新 ~5h, 运行数小时后即实现全量覆盖
# ===================================================================
def _init_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tokens (
            address      TEXT PRIMARY KEY,
            name         TEXT,
            short_name   TEXT,
            symbol       TEXT,
            status       TEXT,
            create_date  INTEGER,
            price        REAL,
            hold         INTEGER,
            day1_vol     REAL,
            progress     REAL,
            raw_json     TEXT,
            first_seen   INTEGER,
            last_updated INTEGER
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tokens_create ON tokens(create_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tokens_hold ON tokens(hold)
    """)
    conn.commit()
    return conn


def db_upsert_tokens(conn: sqlite3.Connection, tokens: list[dict]):
    """批量插入或更新代币 (价格/持币人数/交易量等实时字段始终更新)"""
    now_ms = int(time.time() * 1000)
    rows = []
    for t in tokens:
        addr = t.get("tokenAddress", "")
        if not addr:
            continue
        rows.append((
            addr,
            t.get("name", ""),
            t.get("shortName", ""),
            t.get("symbol", "BNB"),
            t.get("status", ""),
            int(t.get("createDate", 0)),
            float(t.get("price", 0)),
            int(t.get("hold", 0) or 0),
            float(t.get("day1Vol", 0) or 0),
            float(t.get("progress", 0) or 0),
            json.dumps(t, ensure_ascii=False),
            now_ms,
            now_ms,
        ))
    conn.executemany("""
        INSERT INTO tokens (address, name, short_name, symbol, status,
                            create_date, price, hold, day1_vol, progress,
                            raw_json, first_seen, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            price        = excluded.price,
            hold         = excluded.hold,
            day1_vol     = excluded.day1_vol,
            progress     = excluded.progress,
            status       = excluded.status,
            raw_json     = excluded.raw_json,
            last_updated = excluded.last_updated
    """, rows)
    conn.commit()


def db_query_candidates(conn: sqlite3.Connection, cfg: dict,
                        ticker: dict[str, float]) -> list[dict]:
    """
    从本地数据库中查询符合初筛条件的代币:
      - 发行时间在 [min_age, max_age] 范围内
      - 持币人数 ≥ min_holders
    价格过滤在 Python 层做 (需要汇率换算)
    """
    now_ms = int(time.time() * 1000)
    min_age_ms = cfg.get("min_age_hours", 4) * 3600 * 1000
    max_age_ms = cfg.get("max_age_hours", 72) * 3600 * 1000
    min_holders = cfg.get("min_holders", 150)

    oldest = now_ms - max_age_ms
    newest = now_ms - min_age_ms

    rows = conn.execute("""
        SELECT raw_json FROM tokens
        WHERE create_date BETWEEN ? AND ?
          AND hold >= ?
        ORDER BY day1_vol DESC
    """, (oldest, newest, min_holders)).fetchall()

    # 解析 JSON, 应用价格过滤
    max_price = cfg.get("max_price_current", 0.00002)
    bnb_price = ticker.get("BNB", 600.0)
    results = []

    for (raw,) in rows:
        tk = json.loads(raw)
        addr = tk.get("tokenAddress", "")
        if addr in pushed_tokens:
            continue

        raw_price = float(tk.get("price", 0))
        base = tk.get("symbol", "BNB").upper()
        if base == "USDT":
            price_usd = raw_price
        else:
            price_usd = raw_price * ticker.get(base, bnb_price)
        if price_usd <= 0 or price_usd > max_price:
            continue

        tk["_price_usd"] = price_usd
        tk["_holders"] = int(tk.get("hold", 0) or 0)
        results.append(tk)

    results.sort(key=lambda x: float(x.get("day1Vol", 0) or 0), reverse=True)
    return results


def db_cleanup(conn: sqlite3.Connection, max_age_hours: int = 168):
    """清理超过 max_age_hours 的旧记录 (默认 7 天)"""
    cutoff = int(time.time() * 1000) - max_age_hours * 3600 * 1000
    deleted = conn.execute("DELETE FROM tokens WHERE create_date < ?", (cutoff,)).rowcount
    conn.commit()
    if deleted > 0:
        log.info("db_cleanup: 清理 %d 条过期记录", deleted)


# ===================================================================
#  热点数据层
#  从微博热搜 / Google Trends / Twitter(X) 抓取实时热点关键词,
#  用于与代币名称/描述做交叉匹配 (加分项, 匹配的代币优先推送)
# ===================================================================
_hotspot_cache: dict = {"ts": 0, "keywords": []}
HOTSPOT_CACHE_TTL = 900  # 缓存 15 分钟


def fetch_weibo_hot() -> list[dict]:
    """
    获取微博实时热搜 Top50
    返回: [{"word": "关键词", "rank": 0, "source": "weibo"}, ...]
    """
    url = "https://weibo.com/ajax/side/hotSearch"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://weibo.com/",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json",
    }
    try:
        _ensure_sessions()
        r = _fm_session.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        d = r.json()
        items = d.get("data", {}).get("realtime", [])
        results = []
        for i, item in enumerate(items):
            word = (item.get("word") or "").strip()
            if word and len(word) >= 2:
                results.append({"word": word, "rank": i, "source": "weibo"})
        log.info("微博热搜: 获取 %d 个关键词", len(results))
        return results
    except Exception as e:
        log.warning("微博热搜获取失败: %s", e)
        return []


def fetch_google_trends(geos: tuple[str, ...] = ("US", "CN")) -> list[dict]:
    """
    获取 Google Trends 每日热门搜索 (多地区)
    返回: [{"word": "关键词", "rank": 0, "source": "google"}, ...]
    """
    results = []
    seen: set[str] = set()
    for geo in geos:
        url = f"https://trends.google.com/trending/rss?geo={geo}"
        try:
            _ensure_sessions()
            r = _gt_session.get(url, timeout=10)
            r.raise_for_status()
            root = ET.fromstring(r.text)
            for i, item in enumerate(root.findall(".//item")):
                title_el = item.find("title")
                if title_el is not None and title_el.text:
                    word = title_el.text.strip()
                    if word and word.lower() not in seen:
                        seen.add(word.lower())
                        results.append({
                            "word": word,
                            "rank": i,
                            "source": f"google/{geo}",
                        })
        except Exception as e:
            log.warning("Google Trends [%s] 获取失败: %s", geo, e)
        time.sleep(0.3)
    log.info("Google Trends: 获取 %d 个关键词", len(results))
    return results


def fetch_twitter_trending() -> list[dict]:
    """
    通过 getdaytrends.com 抓取 Twitter/X 当日热门话题
    返回: [{"word": "关键词", "rank": 0, "source": "twitter"}, ...]
    """
    url = "https://getdaytrends.com/united-states/"
    try:
        _ensure_sessions()
        r = _gt_session.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html",
        })
        r.raise_for_status()
        # 提取趋势链接中的话题文本
        matches = re.findall(
            r'href="/united-states/trend/[^"]*">([^<]+)</a>', r.text
        )
        results = []
        seen: set[str] = set()
        for i, word in enumerate(matches):
            word = word.strip().lstrip("#")
            if word and len(word) >= 2 and word.lower() not in seen:
                seen.add(word.lower())
                results.append({"word": word, "rank": i, "source": "twitter"})
        log.info("Twitter Trending: 获取 %d 个关键词", len(results))
        return results
    except Exception as e:
        log.warning("Twitter Trending 获取失败: %s", e)
        return []


def fetch_all_hotspots(cfg: dict) -> list[dict]:
    """
    汇总所有热点关键词, 带 15 分钟缓存
    返回: [{"word": ..., "rank": ..., "source": ...}, ...]
    """
    global _hotspot_cache
    now = time.time()
    if now - _hotspot_cache["ts"] < HOTSPOT_CACHE_TTL and _hotspot_cache["keywords"]:
        log.info("使用缓存热点: %d 个关键词", len(_hotspot_cache["keywords"]))
        return _hotspot_cache["keywords"]

    hotspot_cfg = cfg.get("hotspot", {})
    if not hotspot_cfg.get("enabled", True):
        return []

    all_kw: list[dict] = []

    # 微博热搜
    if hotspot_cfg.get("weibo", True):
        all_kw.extend(fetch_weibo_hot())

    # Google Trends
    if hotspot_cfg.get("google", True):
        geos = tuple(hotspot_cfg.get("google_geos", ["US", "CN"]))
        all_kw.extend(fetch_google_trends(geos))

    # Twitter/X
    if hotspot_cfg.get("twitter", True):
        all_kw.extend(fetch_twitter_trending())

    log.info("热点汇总: 共 %d 个关键词 (微博/Google/Twitter)", len(all_kw))
    _hotspot_cache = {"ts": now, "keywords": all_kw}
    return all_kw


def _normalize(text: str) -> str:
    """统一小写, 去除多余空格和特殊符号, 用于模糊匹配"""
    text = text.lower().strip()
    text = re.sub(r"[_\-./·・\s]+", " ", text)
    return text


def hotspot_match(token: dict, hotspots: list[dict],
                  detail: dict | None = None) -> tuple[float, list[str]]:
    """
    计算代币与热点关键词的匹配分数

    匹配字段: name, shortName, descr(detail)
    匹配逻辑:
      - 完全包含 (热点关键词 ⊂ 代币字段): 权重高
      - 越短的热点词要求越精确 (≤3字符需完全匹配 name/shortName)
      - 热点排名靠前的权重更高

    返回: (总分, [匹配到的关键词列表])
    """
    # 构建待匹配文本
    name = _normalize(token.get("name", ""))
    short = _normalize(token.get("shortName", ""))
    desc = ""
    if detail:
        desc = _normalize(detail.get("descr", "") or "")

    score = 0.0
    matched: list[str] = []
    seen_words: set[str] = set()

    for h in hotspots:
        word = h["word"]
        word_lower = _normalize(word)
        if word_lower in seen_words:
            continue

        # 短关键词 (≤3字符) 要求精确匹配 name 或 shortName
        if len(word_lower) <= 3:
            if word_lower != name and word_lower != short:
                continue
        else:
            # 长关键词: 检查子串包含
            found = False
            if word_lower in name or word_lower in short:
                found = True
            elif desc and word_lower in desc:
                found = True
            # 反向匹配: name/short 包含在热点词中 (如代币名 "张雪" 匹配热点 "张雪机车")
            if not found and len(name) >= 2:
                if name in word_lower or short in word_lower:
                    found = True
            if not found:
                continue

        seen_words.add(word_lower)
        # 排名权重: rank=0 → 1.0, rank=49 → 0.5
        rank_weight = max(0.5, 1.0 - h["rank"] * 0.01)
        # 来源权重: 微博略高 (中文 meme 币与中文热点相关性更强)
        source_weight = {"weibo": 1.2, "twitter": 1.0}.get(
            h["source"].split("/")[0], 0.9
        )
        score += rank_weight * source_weight
        matched.append(f"{word}({h['source']})")

    return score, matched
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


def _init_sessions(proxy_cfg: dict | None = None):
    """初始化 four.meme 和 GeckoTerminal 两个 session"""
    fm = _build_session(proxy_cfg, FM_HEADERS)
    gt = _build_session(proxy_cfg, GT_HEADERS)
    return fm, gt


# 全局 Session
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
        _fm_session, _gt_session = _init_sessions(proxy)


# ===================================================================
#  four.meme API 层
# ===================================================================
def fm_search_tokens(cfg: dict) -> list[dict]:
    """
    全量扫描 four.meme 代币, 尽可能覆盖完整时间窗口。

    策略:
      - 按 symbol 分片查询 NEW 排序 (BNB/USD1/USDT/CAKE), 每片最多 10 页 × 100
        BNB ~10h, USD1 ~61h, USDT/CAKE >72h  →  合计覆盖绝大多数代币
      - 补充 HOT / VOL / PROGRESS 排序, 各 1 页, 捕获老但活跃的代币
      - 同时拉取 PUBLISH + TRADE 状态
      - 当某页最旧代币超过 max_age 时提前停止翻页
    """
    _ensure_sessions()
    max_age_ms = cfg.get("max_age_hours", 72) * 3600 * 1000
    now_ms = int(time.time() * 1000)
    seen: dict[str, dict] = {}
    page_size = 100  # API 最大支持 100
    max_pages = 10   # API 最大支持 10 页

    def _add_tokens(items: list[dict]):
        for t in items:
            addr = t.get("tokenAddress", "")
            if addr and addr not in seen:
                seen[addr] = t

    def _fetch_pages(query: dict, label: str):
        """逐页拉取, 超出时间窗口时提前停止"""
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
                _add_tokens(items)

                # 检查最旧代币是否超出时间窗口
                oldest_ts = min(int(t.get("createDate", 0)) for t in items)
                if oldest_ts > 0 and (now_ms - oldest_ts) > max_age_ms:
                    log.debug("  %s p%d: 已超出 %dh 时间窗口, 停止翻页",
                              label, page, cfg.get("max_age_hours", 72))
                    break
                if len(items) < page_size:
                    break
            except Exception as e:
                log.error("fm_search [%s] p%d: %s", label, page, e)
                break
            time.sleep(0.3)

    # ── 1. 按 symbol 分片查询 NEW 排序 (PUBLISH) ──
    for sym in ("BNB", "USD1", "USDT", "CAKE"):
        _fetch_pages(
            {"type": "NEW", "listType": "NOR", "sort": "DESC",
             "status": "PUBLISH", "symbol": sym},
            label=f"NEW/PUBLISH/{sym}",
        )

    # ── 2. 补充排序: HOT / VOL / PROGRESS (PUBLISH), 各 1 页 ──
    for sort_type, list_type in [("HOT", "ADV"), ("VOL", "NOR"), ("PROGRESS", "NOR")]:
        payload = {"pageIndex": 1, "pageSize": page_size,
                   "type": sort_type, "listType": list_type, "status": "PUBLISH"}
        try:
            r = _fm_session.post(FM_SEARCH, json=payload, timeout=15)
            r.raise_for_status()
            d = r.json()
            if d.get("code") == 0:
                _add_tokens(d.get("data", []))
        except Exception as e:
            log.error("fm_search [%s]: %s", sort_type, e)
        time.sleep(0.3)

    # ── 3. 已迁移代币 (TRADE) ──
    for sym in ("BNB", "USD1", "USDT"):
        _fetch_pages(
            {"type": "NEW", "listType": "NOR_DEX", "sort": "DESC",
             "status": "TRADE", "symbol": sym},
            label=f"NEW/TRADE/{sym}",
        )

    log.info("fm_search: 共获取 %d 个代币", len(seen))
    return list(seen.values())


def fm_detail(address: str) -> dict | None:
    _ensure_sessions()
    try:
        r = _fm_session.get(FM_DETAIL, params={"address": address}, timeout=20)
        r.raise_for_status()
        d = r.json()
        return d.get("data") if d.get("code") == 0 else None
    except Exception as e:
        log.error("fm_detail [%s]: %s", address[:20], e)
        return None


def fm_ticker_prices() -> dict[str, float]:
    """返回 {BASE_SYMBOL: usdt_price}"""
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
#  GeckoTerminal API 层
# ===================================================================
def _gt_request(url: str, params: dict | None = None,
                max_retries: int = 3) -> dict | None:
    """带退避重试的 GeckoTerminal 请求"""
    _ensure_sessions()
    for attempt in range(max_retries):
        try:
            r = _gt_session.get(url, params=params, timeout=15)
            if r.status_code == 429:
                wait = 5 * (attempt + 1)
                log.warning("GeckoTerminal 429, 等待 %ds (%d/%d)",
                            wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError:
            if attempt < max_retries - 1:
                time.sleep(3)
            else:
                raise
    return None


def gt_get_pool(token_address: str) -> str | None:
    """查询代币在 BSC 上的首个交易池地址"""
    try:
        data = _gt_request(f"{GT_BASE}/networks/bsc/tokens/{token_address}")
        if not data:
            return None
        pools = (data.get("data", {})
                 .get("relationships", {})
                 .get("top_pools", {})
                 .get("data", []))
        if pools:
            return pools[0]["id"].replace("bsc_", "")
        return None
    except Exception as e:
        log.error("gt_get_pool [%s]: %s", token_address[:20], e)
        return None


def gt_ohlcv_hourly(pool_address: str, limit: int = 72) -> list[list]:
    """获取小时级 OHLCV: [[ts, o, h, l, c, vol], ...] 最新在前"""
    try:
        data = _gt_request(
            f"{GT_BASE}/networks/bsc/pools/{pool_address}/ohlcv/hour",
            params={"aggregate": 1, "limit": limit},
        )
        if not data:
            return []
        return (data.get("data", {})
                .get("attributes", {})
                .get("ohlcv_list", []))
    except Exception as e:
        log.error("gt_ohlcv [%s]: %s", pool_address[:20], e)
        return []


def calc_all_time_high(candles: list[list]) -> float | None:
    """从 OHLCV 中提取历史最高价 (USD)
    candles: [[ts, o, h, l, c, vol], ...] 最新在前
    """
    if not candles:
        return None
    max_high = 0.0
    for c in candles:
        high = float(c[2])
        if high > max_high:
            max_high = high
    return max_high if max_high > 0 else None


def calc_max_price_first_n_hours(candles: list[list], hours: int = 2) -> float | None:
    """从 OHLCV 中提取前 N 小时内的最高价 (USD)
    candles: [[ts, o, h, l, c, vol], ...] 最新在前
    利用最旧蜡烛的时间戳作为起始时间
    """
    if not candles:
        return None
    # 找到最早的时间戳（即代币上线附近的时间）
    oldest_ts = min(int(c[0]) for c in candles)
    cutoff_ts = oldest_ts + hours * 3600  # 前 N 小时的截止时间戳
    max_high = 0.0
    for c in candles:
        ts = int(c[0])
        if ts <= cutoff_ts:
            high = float(c[2])
            if high > max_high:
                max_high = high
    return max_high if max_high > 0 else None


# ===================================================================
#  三级筛选管线
# ===================================================================
def stage1_initial(tokens: list[dict], cfg: dict,
                   ticker: dict[str, float]) -> list[dict]:
    """
    初筛（列表数据, 零额外请求）:
      - 发行时间: min_age_hours < age < max_age_hours
      - 当前价 ≤ max_price_current
      - 持币地址 ≥ min_holders
      - 未推送过
    """
    now_ms = int(time.time() * 1000)
    min_age_ms = cfg.get("min_age_hours", 4) * 3600 * 1000
    max_age_ms = cfg.get("max_age_hours", 72) * 3600 * 1000
    max_price = cfg.get("max_price_current", 0.00002)
    min_holders = cfg.get("min_holders", 150)
    bnb_price = ticker.get("BNB", 600.0)
    results = []

    skip = {"pushed": 0, "age": 0, "price": 0, "holders": 0}

    for tk in tokens:
        addr = tk.get("tokenAddress", "")
        if addr in pushed_tokens:
            skip["pushed"] += 1; continue

        create_ts = int(tk.get("createDate", 0))
        age = now_ms - create_ts
        if create_ts <= 0 or age < min_age_ms or age > max_age_ms:
            skip["age"] += 1; continue

        raw_price = float(tk.get("price", 0))
        base = tk.get("symbol", "BNB").upper()
        if base == "USDT":
            price_usd = raw_price
        else:
            price_usd = raw_price * ticker.get(base, bnb_price)
        if price_usd <= 0 or price_usd > max_price:
            skip["price"] += 1; continue

        holders = int(tk.get("hold", 0) or 0)
        if holders < min_holders:
            skip["holders"] += 1; continue

        tk["_price_usd"] = price_usd
        tk["_holders"] = holders
        results.append(tk)

    results.sort(key=lambda x: float(x.get("day1Vol", 0) or 0), reverse=True)
    log.info("初筛: 通过=%d | 跳过: 已推=%d 年龄=%d 价格=%d 持币=%d",
             len(results), skip["pushed"], skip["age"], skip["price"], skip["holders"])
    return results


def stage2_detail(candidates: list[dict], cfg: dict,
                  max_check: int = 30) -> list[tuple[dict, dict]]:
    """
    详情筛（four.meme 详情请求）:
      - 代币总量 = required_total_supply (10亿)
      - 关联社交媒体 ≥ min_social_links
    """
    required_supply = cfg.get("required_total_supply", 1_000_000_000)
    min_links = cfg.get("min_social_links", 1)
    results: list[tuple[dict, dict]] = []

    for i, tk in enumerate(candidates):
        if i >= max_check:
            break
        addr = tk.get("tokenAddress", "")
        if i > 0:
            time.sleep(0.5)
        detail = fm_detail(addr)
        if not detail:
            continue

        # 总量
        try:
            supply = int(float(detail.get("totalAmount", 0)))
        except (ValueError, TypeError):
            supply = 0
        if supply != required_supply:
            log.debug("跳过 %s: 总量 %s != %s",
                      detail.get("name", addr), supply, required_supply)
            continue

        # 社交媒体
        link_count = sum(
            1 for k in ("twitterUrl", "telegramUrl", "webUrl")
            if (detail.get(k) or "").strip()
        )
        if link_count < min_links:
            log.debug("跳过 %s: 社交媒体 %d < %d",
                      detail.get("name", addr), link_count, min_links)
            continue

        tk["_social_count"] = link_count
        results.append((tk, detail))

    log.info("详情筛: 检查 %d, 通过 %d", min(len(candidates), max_check), len(results))
    return results


def stage3_kline(candidates: list[tuple[dict, dict]], cfg: dict,
                 max_check: int = 15) -> list[tuple[dict, dict]]:
    """
    K线筛（GeckoTerminal OHLCV）:
      - 历史最高价 ≤ max_price_ath
      - 前2小时最高价 ≤ max_price_2h
    """
    max_ath = cfg.get("max_price_ath", 0.00014)
    max_2h = cfg.get("max_price_2h", 0.00004)
    results: list[tuple[dict, dict]] = []

    for i, (tk, detail) in enumerate(candidates):
        if i >= max_check:
            break
        addr = tk.get("tokenAddress", "")
        name = tk.get("name", addr[:16])

        if i > 0:
            time.sleep(3)

        pool = gt_get_pool(addr)
        if not pool:
            log.debug("跳过 %s: 无 GeckoTerminal 交易池", name)
            continue

        time.sleep(3)

        candles = gt_ohlcv_hourly(pool, limit=72)
        if not candles:
            log.debug("跳过 %s: 无 K 线数据", name)
            continue

        high_ath = calc_all_time_high(candles)
        if high_ath is None:
            log.debug("跳过 %s: 无法确定历史最高价", name)
            continue

        log.info("  %s: 历史最高 $%.10f (阈值 $%.10f)", name, high_ath, max_ath)
        if high_ath > max_ath:
            continue

        # 前2小时最高价检查
        high_2h = calc_max_price_first_n_hours(candles, hours=2)
        if high_2h is not None and high_2h > max_2h:
            log.info("  %s: 前2h最高 $%.10f > 阈值 $%.10f, 跳过",
                     name, high_2h, max_2h)
            continue

        tk["_high_ath"] = high_ath
        tk["_high_2h"] = high_2h
        results.append((tk, detail))

    log.info("K线筛: 检查 %d, 通过 %d", min(len(candidates), max_check), len(results))
    return results


# ===================================================================
#  社交链接 & 描述
# ===================================================================
def extract_social_links(detail: dict) -> list[dict]:
    links: list[dict] = []
    seen: set[str] = set()
    for key, label in [("twitterUrl", "Twitter"),
                       ("telegramUrl", "Telegram"),
                       ("webUrl", "Website")]:
        url = (detail.get(key) or "").strip()
        if url and url not in seen:
            links.append({"type": label, "url": url})
            seen.add(url)
    return links


def truncate_desc(detail: dict, limit: int = 100) -> str:
    desc = (detail.get("descr") or "").strip()
    if not desc:
        return "暂无介绍"
    return desc[:limit - 3] + "..." if len(desc) > limit else desc


# ===================================================================
#  消息格式化 & 推送
# ===================================================================
def format_social_html(links: list[dict]) -> str:
    return "\n".join(f"  • <a href='{l['url']}'>{l['type']}</a>" for l in links)


def format_message(infos: list[dict]) -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"🔍 <b>BSC 土狗扫描报告</b>", f"⏰ {now_str}", ""]

    for i, info in enumerate(infos, 1):
        lines.append(f"<b>#{i} {info['name']} ({info['short']})</b>")
        lines.append(f"📄 合约: <code>{info['address']}</code>")
        lines.append(f"💰 当前价: ${info['price_usd']:.10f}")
        lines.append(f"📈 历史最高: ${info['high_ath']:.10f}")
        lines.append(f"👥 持币人数: {info['holders']}")
        lines.append(f"🔗 社交媒体: {info['social_count']} 个")
        social_links = info.get("social_links", [])
        if social_links:
            lines.append(format_social_html(social_links))
        lines.append(f"🕐 创建: {info['create_time']}")
        hotspot_matched = info.get("hotspot_matched", [])
        if hotspot_matched:
            lines.append(f"🔥 热点匹配: {', '.join(hotspot_matched)}")
        lines.append(f"📝 {info['desc']}")
        a = info["address"]
        lines.append(
            f"🌐 <a href='https://four.meme/token/{a}'>four.meme</a>"
            f" | <a href='https://bscscan.com/token/{a}'>BscScan</a>"
        )
        lines.append("")

    lines.append("—— four.meme 土狗扫描器 v2 ——")
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
        result = r.json()
        if result.get("ok"):
            return True
        log.warning("Telegram: %s", result.get("description"))
        return False
    except Exception as e:
        log.error("Telegram: %s", e)
        return False


def print_console(msg: str) -> None:
    out = msg.replace("<b>", "").replace("</b>", "")
    out = out.replace("<code>", "").replace("</code>", "")
    out = re.sub(r"<a href='[^']*'>", "", out).replace("</a>", "")
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
    max_push = cfg.get("max_push_count", 3)

    # 0) 初始化数据库 & 行情
    conn = _init_db()
    ticker = fm_ticker_prices()
    bnb = ticker.get("BNB", 0)
    if bnb <= 0:
        log.warning("BNB 价格获取失败, 使用默认 600")
        ticker["BNB"] = 600.0
    log.info("BNB=$%.2f", ticker.get("BNB", 0))

    # 1) 拉取代币 (全量扫描) → 存入 SQLite
    tokens = fm_search_tokens(cfg)
    if tokens:
        db_upsert_tokens(conn, tokens)
        log.info("已入库 %d 个代币", len(tokens))
    else:
        log.warning("本轮未获取到新代币, 继续使用数据库累积数据")

    # 统计数据库总量
    total_db = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
    log.info("数据库累积代币总数: %d", total_db)

    # 2) 从数据库查询初筛候选 (跨轮次累积, 覆盖全时间窗口)
    s1 = db_query_candidates(conn, cfg, ticker)
    if not s1:
        log.info("初筛无结果")
        db_cleanup(conn)
        conn.close()
        return
    log.info("初筛: 通过 %d 个候选", len(s1))

    # 2.5) 获取热点关键词 (用于后续加分排序)
    hotspots = fetch_all_hotspots(cfg)

    # 3) 详情筛
    s2 = stage2_detail(s1, cfg, max_check=max_push * 8)
    if not s2:
        log.info("详情筛无结果")
        db_cleanup(conn)
        conn.close()
        return

    # 4) K线筛
    s3 = stage3_kline(s2, cfg, max_check=max_push * 5)
    if not s3:
        log.info("K线筛无结果")
        db_cleanup(conn)
        conn.close()
        return

    # 4.5) 热点加分排序: 匹配热点的代币优先推送
    if hotspots:
        for tk, detail in s3:
            score, matched = hotspot_match(tk, hotspots, detail)
            tk["_hotspot_score"] = score
            tk["_hotspot_matched"] = matched
            if matched:
                log.info("  热点匹配 %s: %.2f 分, 关键词: %s",
                         tk.get("name", ""), score, ", ".join(matched))
        # 热点分高的排前面, 同分按原顺序
        s3.sort(key=lambda x: x[0].get("_hotspot_score", 0), reverse=True)

    # 5) 组装推送
    to_push = s3[:max_push]
    infos = []
    for tk, detail in to_push:
        addr = tk["tokenAddress"]
        create_ts = int(tk.get("createDate", 0))
        create_dt = datetime.fromtimestamp(create_ts / 1000, tz=timezone.utc)
        infos.append({
            "name": tk.get("name", "Unknown"),
            "short": tk.get("shortName", ""),
            "address": addr,
            "price_usd": tk.get("_price_usd", 0),
            "high_ath": tk.get("_high_ath", 0),
            "holders": tk.get("_holders", 0),
            "social_count": tk.get("_social_count", 0),
            "social_links": extract_social_links(detail),
            "create_time": create_dt.strftime("%Y-%m-%d %H:%M UTC"),
            "desc": truncate_desc(detail),
            "hotspot_score": tk.get("_hotspot_score", 0),
            "hotspot_matched": tk.get("_hotspot_matched", []),
        })

    msg = format_message(infos)
    log.info("推送 %d 个代币", len(infos))

    bot_token = cfg.get("telegram_bot_token", "")
    chat_id = cfg.get("telegram_chat_id", "")
    if not bot_token or not chat_id or "YOUR" in bot_token:
        log.warning("Telegram 未配置, 仅打印:")
        print_console(msg)
    else:
        ok = send_telegram(bot_token, chat_id, msg)
        log.info("Telegram 推送%s", "成功" if ok else "失败")

    for info in infos:
        pushed_tokens.add(info["address"])
    if len(pushed_tokens) > MAX_CACHE:
        pushed_tokens = set(list(pushed_tokens)[-MAX_CACHE // 2:])

    # 6) 清理过期数据 & 关闭连接
    db_cleanup(conn)
    conn.close()


def main():
    global _fm_session, _gt_session
    log.info("🚀 BSC 土狗扫描器 v2 启动")
    log.info("配置文件: %s", CONFIG_PATH)

    while True:
        try:
            cfg = load_config()
            # 热更新 session (代理等)
            _fm_session, _gt_session = _init_sessions(cfg.get("proxy"))
            log.info(
                "筛选: 年龄 %d~%dh | 当前价<$%s | 历史最高<$%s | 前2h最高<$%s | 总量=%s | 持币>%d | 社交>%d",
                cfg.get("min_age_hours", 4),
                cfg.get("max_age_hours", 72),
                cfg.get("max_price_current", 0.00002),
                cfg.get("max_price_ath", 0.00014),
                cfg.get("max_price_2h", 0.00004),
                cfg.get("required_total_supply", 1_000_000_000),
                cfg.get("min_holders", 150),
                cfg.get("min_social_links", 1),
            )
            scan_once(cfg)
            interval = cfg.get("scan_interval_minutes", 15)
            log.info("下次扫描: %d 分钟后", interval)
            time.sleep(interval * 60)
        except KeyboardInterrupt:
            log.info("用户中断, 退出"); break
        except Exception as e:
            log.error("扫描异常: %s", e, exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    main()

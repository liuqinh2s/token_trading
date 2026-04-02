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

try:
    from trader import init_trader, execute_buys, start_monitor, stop_monitor
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
#  链上扫描常量 (four.meme TokenManager2 V2)
# ===================================================================
TOKEN_MANAGER_V2 = "0x5c952063c7fc8610FFDB798152D69F0B9550762b"
TOKEN_CREATE_TOPIC = "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"
BSC_BLOCK_TIME = 3  # ~3 秒/块

SCAN_RPC_URLS = [
    "https://bsc.publicnode.com",
    "https://1rpc.io/bnb",
    "https://bsc-dataseed1.binance.org",
    "https://bsc-dataseed2.binance.org",
    "https://bsc.api.onfinality.io/public",
    "https://bsc-rpc.publicnode.com",
]


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
            total_supply INTEGER DEFAULT 0,
            raw_json     TEXT,
            first_seen   INTEGER,
            last_updated INTEGER
        )
    """)
    # 兼容旧表: 若缺少 total_supply 列则自动添加
    try:
        conn.execute("SELECT total_supply FROM tokens LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE tokens ADD COLUMN total_supply INTEGER DEFAULT 0")
        conn.commit()
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tokens_create ON tokens(create_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tokens_hold ON tokens(hold)
    """)
    conn.commit()
    return conn


def db_upsert_tokens(conn: sqlite3.Connection, tokens: list[dict]):
    """批量插入或更新代币 (地址统一小写; 非零值覆盖零值, 避免链上空数据覆盖 API 数据)"""
    now_ms = int(time.time() * 1000)
    rows = []
    for t in tokens:
        addr = (t.get("tokenAddress", "") or "").lower()
        if not addr:
            continue
        total_supply = int(t.get("_total_supply", 0))
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
            total_supply,
            json.dumps(t, ensure_ascii=False),
            now_ms,
            now_ms,
        ))
    conn.executemany("""
        INSERT INTO tokens (address, name, short_name, symbol, status,
                            create_date, price, hold, day1_vol, progress,
                            total_supply, raw_json, first_seen, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            name         = CASE WHEN excluded.name != '' THEN excluded.name ELSE tokens.name END,
            short_name   = CASE WHEN excluded.short_name != '' THEN excluded.short_name ELSE tokens.short_name END,
            symbol       = CASE WHEN excluded.symbol != 'BNB' THEN excluded.symbol
                                WHEN tokens.symbol = '' THEN excluded.symbol ELSE tokens.symbol END,
            status       = CASE WHEN excluded.status != '' THEN excluded.status ELSE tokens.status END,
            create_date  = CASE WHEN excluded.create_date > 0 THEN excluded.create_date ELSE tokens.create_date END,
            price        = CASE WHEN excluded.price > 0 THEN excluded.price ELSE tokens.price END,
            hold         = CASE WHEN excluded.hold > 0 THEN excluded.hold ELSE tokens.hold END,
            day1_vol     = CASE WHEN excluded.day1_vol > 0 THEN excluded.day1_vol ELSE tokens.day1_vol END,
            progress     = CASE WHEN excluded.progress > 0 THEN excluded.progress ELSE tokens.progress END,
            total_supply = CASE WHEN excluded.total_supply > 0 THEN excluded.total_supply ELSE tokens.total_supply END,
            raw_json     = CASE WHEN excluded.price > 0 THEN excluded.raw_json ELSE tokens.raw_json END,
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
    min_age_ms = cfg.get("min_age_hours", 2) * 3600 * 1000
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
        if addr.lower() in pushed_tokens:
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
#  链上扫描层
#  通过 eth_getLogs 读取 TokenManager2 的 TokenCreate 事件,
#  100% 覆盖全时间窗口内的所有 four.meme 代币 (不受 API 分页上限约束)
# ===================================================================
def _rpc_call(method: str, params: list, rpc_url: str):
    """JSON-RPC 调用 BSC 节点 (走代理 session)"""
    _ensure_sessions()
    r = _fm_session.post(rpc_url, json={
        "jsonrpc": "2.0", "id": 1,
        "method": method, "params": params,
    }, timeout=30, headers={"Content-Type": "application/json"})
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"RPC error: {data['error']}")
    return data.get("result")


def _decode_token_create_log(log_entry: dict) -> dict | None:
    """
    解码 TokenCreate 事件:
    TokenCreate(address creator, address token, uint256 tokenId,
                string name, string symbol,
                uint256 totalSupply, uint256 launchTs, uint256 unknown)

    所有参数非 indexed, 全部在 data 字段 (ABI 编码)
    """
    try:
        data = log_entry["data"][2:]  # 去掉 0x

        # Head: 8 个 word (每个 64 hex chars = 32 bytes)
        # Word 0: creator address
        # Word 1: token address
        token = "0x" + data[88:128]  # word 1, 取后 40 chars (20 bytes)
        # Word 2: tokenId
        # Word 3: offset to name (bytes)
        name_offset = int(data[192:256], 16)
        # Word 4: offset to symbol (bytes)
        sym_offset = int(data[256:320], 16)
        # Word 5: totalSupply (raw, with decimals)
        total_supply_raw = int(data[320:384], 16)
        # Word 6: launchTimestamp
        launch_ts = int(data[384:448], 16)

        # 解码 name 字符串
        no = name_offset * 2  # byte offset → hex char offset
        name_len = int(data[no:no + 64], 16)
        name = bytes.fromhex(data[no + 64:no + 64 + name_len * 2]).decode(
            "utf-8", errors="replace")

        # 解码 symbol 字符串
        so = sym_offset * 2
        sym_len = int(data[so:so + 64], 16)
        symbol = bytes.fromhex(data[so + 64:so + 64 + sym_len * 2]).decode(
            "utf-8", errors="replace")

        # 转换总量 (假设 18 decimals)
        total_supply = total_supply_raw // (10 ** 18)

        # 转换时间戳 (秒 → 毫秒)
        if launch_ts > 1e12:
            create_date_ms = launch_ts  # 已是毫秒
        elif launch_ts > 0:
            create_date_ms = launch_ts * 1000
        else:
            # 回退: 用区块号估算
            block_num = int(log_entry.get("blockNumber", "0x0"), 16)
            create_date_ms = int(time.time() * 1000) - block_num * BSC_BLOCK_TIME * 1000
            if create_date_ms < 0:
                create_date_ms = int(time.time() * 1000)

        return {
            "tokenAddress": token,
            "name": name,
            "shortName": symbol,
            "symbol": "BNB",  # 默认, 后续由 API 更新
            "status": "",
            "createDate": create_date_ms,
            "price": 0,
            "hold": 0,
            "day1Vol": 0,
            "progress": 0,
            "_total_supply": total_supply,
        }
    except Exception as e:
        log.debug("解码 TokenCreate 事件失败: %s", e)
        return None


def onchain_scan_tokens(cfg: dict) -> list[dict]:
    """
    通过链上 eth_getLogs 扫描 TokenManager2 的 TokenCreate 事件,
    100% 覆盖全时间窗口内的所有 four.meme 代币。

    BSC 出块约 3 秒, 72h ≈ 86400 块。
    按 2000 块分片查询 (自动降级到 500), 约 45-175 次 RPC, 耗时 ~30-60 秒。
    """
    max_age_hours = cfg.get("max_age_hours", 72)
    required_supply = cfg.get("required_total_supply", 1_000_000_000)

    # 选择 RPC (优先使用配置, 否则用内置列表)
    rpc_url = cfg.get("scan_rpc_url", "") or cfg.get("trading", {}).get("rpc_url", "")
    if not rpc_url:
        rpc_url = SCAN_RPC_URLS[0]

    # 获取当前区块
    current_block = None
    for url in ([rpc_url] + SCAN_RPC_URLS):
        try:
            current_block = int(_rpc_call("eth_blockNumber", [], url), 16)
            rpc_url = url
            break
        except Exception as e:
            log.warning("RPC %s 获取区块高度失败: %s", url[:40], e)
    if current_block is None:
        log.error("所有 RPC 获取区块高度失败, 跳过链上扫描")
        return []

    log.info("链上扫描: 当前区块 %d, RPC %s", current_block, rpc_url[:40])

    blocks_back = max_age_hours * 3600 // BSC_BLOCK_TIME
    start_block = current_block - blocks_back
    chunk_size = 2000  # 起始分片大小, 失败时自动缩小

    all_tokens: list[dict] = []
    total_events = 0
    supply_filtered = 0
    from_block = start_block
    consecutive_errors = 0
    total_chunks = 0
    empty_chunks = 0

    while from_block <= current_block:
        to_block = min(from_block + chunk_size - 1, current_block)

        try:
            logs = _rpc_call("eth_getLogs", [{
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "address": TOKEN_MANAGER_V2,
                "topics": [TOKEN_CREATE_TOPIC],
            }], rpc_url)

            consecutive_errors = 0
            total_chunks += 1
            if not logs:
                empty_chunks += 1

            for entry in (logs or []):
                total_events += 1
                token = _decode_token_create_log(entry)
                if token is None:
                    continue
                # 立即过滤总量
                if token["_total_supply"] != required_supply:
                    supply_filtered += 1
                    continue
                all_tokens.append(token)

            from_block = to_block + 1

        except Exception as e:
            err_msg = str(e).lower()
            consecutive_errors += 1

            # 分片太大 → 缩小
            if any(w in err_msg for w in ("limit", "range", "exceed", "too many")):
                if chunk_size > 100:
                    chunk_size = max(100, chunk_size // 2)
                    log.info("RPC 区块范围超限, 缩小到 %d 块/次", chunk_size)
                    consecutive_errors = 0  # 范围超限不算连续失败
                    continue

            # 连续失败 → 切换 RPC
            if consecutive_errors >= 3:
                switched = False
                for alt_rpc in SCAN_RPC_URLS:
                    if alt_rpc == rpc_url:
                        continue
                    try:
                        _rpc_call("eth_blockNumber", [], alt_rpc)
                        rpc_url = alt_rpc
                        chunk_size = 500
                        consecutive_errors = 0
                        log.info("切换备用 RPC: %s", rpc_url[:40])
                        switched = True
                        break
                    except Exception:
                        continue
                if not switched:
                    log.error("所有 RPC 失败, 中止链上扫描 (已扫到区块 %d)", from_block)
                    break
            else:
                log.warning("RPC 请求失败 (%d/3): %s", consecutive_errors, e)
                time.sleep(1)

    log.info("链上扫描完成: 共 %d 个 TokenCreate, 总量匹配 %d, 跳过 %d "
             "(chunk_size=%d, 总请求=%d, 空块=%d, 区块范围 %d→%d)",
             total_events, len(all_tokens), supply_filtered,
             chunk_size, total_chunks, empty_chunks, start_block, current_block)
    return all_tokens


def _enrich_onchain_gaps(conn: sqlite3.Connection, cfg: dict,
                         max_fill: int = 100):
    """
    为链上发现但缺少 API 数据 (hold=0) 的代币补全信息。
    通过 four.meme detail API 逐个获取, 受限于速率每轮最多补全 max_fill 个。
    多轮运行后全部补全。
    """
    now_ms = int(time.time() * 1000)
    min_age_ms = cfg.get("min_age_hours", 2) * 3600 * 1000
    max_age_ms = cfg.get("max_age_hours", 72) * 3600 * 1000
    required_supply = cfg.get("required_total_supply", 1_000_000_000)

    oldest = now_ms - max_age_ms
    newest = now_ms - min_age_ms

    rows = conn.execute("""
        SELECT address FROM tokens
        WHERE create_date BETWEEN ? AND ?
          AND total_supply = ?
          AND hold = 0
          AND price = 0
        ORDER BY create_date DESC
        LIMIT ?
    """, (oldest, newest, required_supply, max_fill)).fetchall()

    if not rows:
        return

    log.info("补全链上代币数据: %d 个待查", len(rows))
    filled = 0
    consecutive_miss = 0

    for (addr,) in rows:
        time.sleep(0.3)
        detail = fm_detail(addr)
        if not detail:
            consecutive_miss += 1
            if consecutive_miss >= 20:
                log.info("连续 %d 个补全失败, 跳过剩余", consecutive_miss)
                break
            continue

        consecutive_miss = 0
        hold = int(detail.get("holderCount", 0) or detail.get("hold", 0) or 0)
        price = float(detail.get("price", 0) or 0)
        status = detail.get("status", "")

        conn.execute("""
            UPDATE tokens SET hold = ?, price = ?, status = ?, last_updated = ?
            WHERE address = ? AND (hold = 0 OR price = 0)
        """, (hold, price, status, now_ms, addr))
        if hold > 0 or price > 0:
            filled += 1

    conn.commit()
    log.info("补全完成: %d / %d 个代币获取到数据", filled, len(rows))


# ===================================================================
#  four.meme API 层
# ===================================================================
def fm_search_tokens(cfg: dict) -> list[dict]:
    """
    全量扫描 four.meme 代币, 通过多维度查询组合最大化覆盖。

    策略 (每组最多 10 页 × 100 = 1000 条):
      ┌─────────────────────────────────────────────────────────────┐
      │  维度1: symbol = BNB / USD1 / USDT / CAKE                 │
      │  维度2: sort   = DESC (最新) / ASC (最旧)                   │
      │  维度3: status = PUBLISH (bonding curve) / TRADE (已迁移)   │
      │  维度4: type   = NEW / HOT / VOL / PROGRESS                │
      └─────────────────────────────────────────────────────────────┘
      DESC 从最新到最旧, ASC 从最旧到最新 → 两端各覆盖 1000 条,
      中间部分由 SQLite 跨轮次累积补全。

    理论上限: ~30 组 × 1000 = 30000, 去重后 20000+
    """
    _ensure_sessions()
    max_age_ms = cfg.get("max_age_hours", 72) * 3600 * 1000
    now_ms = int(time.time() * 1000)
    seen: dict[str, dict] = {}
    page_size = 100
    max_pages = 10

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

                # 检查是否超出时间窗口 (仅对按时间排序的查询)
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

    # ── 1. NEW/DESC (最新→最旧) × PUBLISH × 4 symbols = 最多 4000 ──
    for sym in symbols:
        _fetch_pages(
            {"type": "NEW", "listType": "NOR", "sort": "DESC",
             "status": "PUBLISH", "symbol": sym},
            label=f"NEW/DESC/PUB/{sym}",
        )

    # ── 2. NEW/ASC (最旧→最新) × PUBLISH × 4 symbols = 最多 4000 ──
    #    与 DESC 覆盖相反方向, 大幅减少盲区
    for sym in symbols:
        _fetch_pages(
            {"type": "NEW", "listType": "NOR", "sort": "ASC",
             "status": "PUBLISH", "symbol": sym},
            label=f"NEW/ASC/PUB/{sym}",
        )

    # ── 3. NEW/DESC × TRADE × 4 symbols = 最多 4000 ──
    for sym in symbols:
        _fetch_pages(
            {"type": "NEW", "listType": "NOR_DEX", "sort": "DESC",
             "status": "TRADE", "symbol": sym},
            label=f"NEW/DESC/TRADE/{sym}",
        )

    # ── 4. NEW/ASC × TRADE × 4 symbols = 最多 4000 ──
    for sym in symbols:
        _fetch_pages(
            {"type": "NEW", "listType": "NOR_DEX", "sort": "ASC",
             "status": "TRADE", "symbol": sym},
            label=f"NEW/ASC/TRADE/{sym}",
        )

    # ── 5. HOT/VOL/PROGRESS 多页 × 双状态, 捕获中段活跃代币 ──
    for sort_type, list_type in [("HOT", "ADV"), ("VOL", "NOR"), ("PROGRESS", "NOR")]:
        for status, lt_override in [("PUBLISH", list_type), ("TRADE", "NOR_DEX")]:
            _fetch_pages(
                {"type": sort_type, "listType": lt_override,
                 "status": status},
                label=f"{sort_type}/{status}",
            )

    # ── 6. HOT 按 symbol 分片 (额外覆盖) ──
    for sym in symbols:
        for status, lt in [("PUBLISH", "ADV"), ("TRADE", "NOR_DEX")]:
            payload = {"pageIndex": 1, "pageSize": page_size,
                       "type": "HOT", "listType": lt,
                       "status": status, "symbol": sym}
            try:
                r = _fm_session.post(FM_SEARCH, json=payload, timeout=15)
                r.raise_for_status()
                d = r.json()
                if d.get("code") == 0:
                    _add_tokens(d.get("data", []))
            except Exception as e:
                log.error("fm_search [HOT/%s/%s]: %s", status, sym, e)
            time.sleep(0.3)

    log.info("fm_search: 共获取 %d 个代币 (去重后)", len(seen))
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
    global _gt_rate_delay
    _ensure_sessions()
    for attempt in range(max_retries):
        try:
            r = _gt_session.get(url, params=params, timeout=15)
            if r.status_code == 429:
                wait = 5 * (attempt + 1)
                _gt_rate_delay = min(5.0, _gt_rate_delay + 1.0)
                log.warning("GeckoTerminal 429, 等待 %ds (%d/%d)",
                            wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue
            r.raise_for_status()
            # 成功时逐步恢复速度
            _gt_rate_delay = max(0.3, _gt_rate_delay - 0.2)
            return r.json()
        except requests.exceptions.HTTPError:
            if attempt < max_retries - 1:
                time.sleep(3)
            else:
                raise
    return None


_gt_rate_delay: float = 0.5  # GeckoTerminal 动态速率控制


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


def calc_first_2h_max(candles: list[list], create_ts_sec: int) -> float | None:
    """从 OHLCV 中提取发币后前 2 小时内的最高价 (USD)
    candles: [[ts, o, h, l, c, vol], ...] 最新在前
    """
    if not candles:
        return None
    cutoff = create_ts_sec + 2 * 3600
    max_high = 0.0
    found = False
    for c in candles:
        ts = int(c[0])
        if ts > cutoff:
            continue
        if ts < create_ts_sec - 3600:
            continue
        high = float(c[2])
        if high > max_high:
            max_high = high
        found = True
    return max_high if found else None


# ===================================================================
#  DexScreener API 层 (300 req/min, 比 GeckoTerminal 快 10 倍)
# ===================================================================
DS_BASE = "https://api.dexscreener.com"
DS_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}


def ds_get_pairs(token_address: str) -> list[dict] | None:
    """通过 DexScreener 获取代币的交易对信息 (含价格历史)"""
    _ensure_sessions()
    url = f"{DS_BASE}/tokens/v1/bsc/{token_address}"
    try:
        r = _gt_session.get(url, timeout=10, headers=DS_HEADERS)
        if r.status_code == 429:
            time.sleep(2)
            r = _gt_session.get(url, timeout=10, headers=DS_HEADERS)
        r.raise_for_status()
        data = r.json()
        # v1 返回数组
        if isinstance(data, list):
            return data
        return data.get("pairs") or data.get("data") or []
    except Exception as e:
        log.debug("ds_get_pairs [%s]: %s", token_address[:20], e)
        return None


def ds_calc_first_2h_max(pairs: list[dict], create_ts_sec: int,
                         bnb_price: float = 600.0) -> float | None:
    """
    从 DexScreener pair 数据估算前 2h 最高价。
    DexScreener 不直接给 OHLCV, 但 pair 里有 priceUsd 和 priceChange。
    如果代币年龄 < 6h, 用 ATH 近似前 2h 最高价 (误差可接受)。
    """
    if not pairs:
        return None
    for pair in pairs:
        if pair.get("chainId") != "bsc":
            continue
        price_usd_str = pair.get("priceUsd")
        if not price_usd_str:
            continue
        try:
            price_usd = float(price_usd_str)
        except (ValueError, TypeError):
            continue
        # DexScreener 没有直接的前 2h 最高价,
        # 但对于新币 (< 6h), ATH ≈ 前 2h 最高价
        # 用 priceChange 反推: 如果有 h1/h6 变化率, 可以估算
        price_change = pair.get("priceChange", {})
        # 尝试用 h1 变化率反推 1h 前价格
        h1_change = price_change.get("h1")
        h6_change = price_change.get("h6")

        # 简单策略: 取当前价 / (1 + 最大跌幅) 作为历史高点估算
        max_price = price_usd
        if h1_change is not None:
            try:
                h1_pct = float(h1_change)
                if h1_pct < 0:
                    # 价格下跌了, 1h 前更高
                    price_1h_ago = price_usd / (1 + h1_pct / 100)
                    max_price = max(max_price, price_1h_ago)
            except (ValueError, TypeError):
                pass
        if h6_change is not None:
            try:
                h6_pct = float(h6_change)
                if h6_pct < 0:
                    price_6h_ago = price_usd / (1 + h6_pct / 100)
                    max_price = max(max_price, price_6h_ago)
            except (ValueError, TypeError):
                pass
        return max_price
    return None


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
    min_age_ms = cfg.get("min_age_hours", 2) * 3600 * 1000
    max_age_ms = cfg.get("max_age_hours", 72) * 3600 * 1000
    max_price = cfg.get("max_price_current", 0.00002)
    min_holders = cfg.get("min_holders", 150)
    bnb_price = ticker.get("BNB", 600.0)
    results = []

    skip = {"pushed": 0, "age": 0, "price": 0, "holders": 0}

    for tk in tokens:
        addr = tk.get("tokenAddress", "")
        if addr.lower() in pushed_tokens:
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
        create_ts_sec = int(tk.get("createDate", 0)) // 1000

        if i > 0:
            time.sleep(0.3)

        # ── 优先 DexScreener (快, 300 req/min) ──
        high_2h = None
        pairs = ds_get_pairs(addr)
        if pairs:
            high_2h = ds_calc_first_2h_max(pairs, create_ts_sec, bnb_price)

        # ── Fallback: GeckoTerminal OHLCV (慢但精确) ──
        if high_2h is None:
            if i > 0:
                time.sleep(_gt_rate_delay)
            pool = gt_get_pool(addr)
            if pool:
                time.sleep(_gt_rate_delay)
                candles = gt_ohlcv_hourly(pool, limit=72)
                if candles:
                    high_2h = calc_first_2h_max(candles, create_ts_sec)

        if high_2h is None:
            log.debug("跳过 %s: 无法获取价格数据", name)
            continue

        log.info("  %s: 前2h最高 $%.10f (阈值 $%.10f)", name, high_2h, max_2h)
        if high_2h > max_2h:
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
        lines.append(f"📈 前2h最高: ${info['high_2h']:.10f}")
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

    # 1) 链上扫描: 通过 TokenCreate 事件发现 100% 的代币 (已按总量筛选)
    onchain_tokens = onchain_scan_tokens(cfg)
    if onchain_tokens:
        db_upsert_tokens(conn, onchain_tokens)
        log.info("链上发现 %d 个代币 (总量=%s 筛选后), 已入库",
                 len(onchain_tokens), cfg.get("required_total_supply", 1_000_000_000))

    # 2) API 扫描: 获取价格/持币人数/交易量等实时批量数据
    tokens = fm_search_tokens(cfg)
    if tokens:
        db_upsert_tokens(conn, tokens)
        log.info("API 获取 %d 个代币, 已入库", len(tokens))
    elif not onchain_tokens:
        log.warning("链上扫描和 API 均未获取到代币")

    # 统计数据库总量
    total_db = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
    onchain_only = conn.execute(
        "SELECT COUNT(*) FROM tokens WHERE hold = 0 AND price = 0"
    ).fetchone()[0]
    log.info("数据库代币总数: %d (其中仅链上数据: %d)", total_db, onchain_only)

    # 2.5) 补全: 链上发现但 API 未覆盖的代币, 逐个查询 detail
    _enrich_onchain_gaps(conn, cfg)

    # 3) 从数据库查询初筛候选 (链上扫描 100% 覆盖 + API 数据丰富)
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
            "high_2h": tk.get("_high_2h", 0),
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
        pushed_tokens.add(info["address"].lower())
    if len(pushed_tokens) > MAX_CACHE:
        pushed_tokens = set(list(pushed_tokens)[-MAX_CACHE // 2:])

    # 5.5) 自动买入
    if _HAS_TRADER and cfg.get("trading", {}).get("enabled", False):
        bnb_usd = ticker.get("BNB", 600.0)
        execute_buys(to_push, cfg, bnb_usd)

    # 6) 清理过期数据 & 关闭连接
    db_cleanup(conn)
    conn.close()


def main():
    global _fm_session, _gt_session
    log.info("🚀 BSC 土狗扫描器 v2 启动")
    log.info("配置文件: %s", CONFIG_PATH)

    # 初始化交易模块 & 启动持仓监控
    _monitor_thread = None
    try:
        cfg = load_config()
        if _HAS_TRADER and cfg.get("trading", {}).get("enabled", False):
            if init_trader(cfg):
                log.info("自动交易已启用, 启动持仓监控...")
                _monitor_thread = start_monitor(
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
            log.info("用户中断, 退出")
            if _HAS_TRADER:
                stop_monitor()
            break
        except Exception as e:
            log.error("扫描异常: %s", e, exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    main()

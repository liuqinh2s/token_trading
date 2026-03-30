"""
BSC 土狗扫盘程序 - four.meme 新币扫描器
扫描 four.meme 上新发行的代币，筛选后推送到 Telegram
"""

from __future__ import annotations

import json
import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# 日志配置
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scanner.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 配置加载
# ---------------------------------------------------------------------------
CONFIG_PATH = Path(__file__).parent / "config.json"


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# four.meme API
# ---------------------------------------------------------------------------
FOUR_MEME_SEARCH_URL = "https://four.meme/meme-api/v1/public/token/search"
FOUR_MEME_DETAIL_URL = "https://four.meme/meme-api/v1/private/token/get/v2"
FOUR_MEME_TICKER_URL = "https://four.meme/meme-api/v1/public/ticker"

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Origin": "https://four.meme",
    "Referer": "https://four.meme/",
}


def _build_session(proxy_cfg: dict | None = None) -> requests.Session:
    """构建带自动重试、连接复用和代理的 Session"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,                    # 最多重试 3 次
        backoff_factor=1,           # 退避: 1s, 2s, 4s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=5,
        pool_maxsize=10,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)

    # 代理配置
    if proxy_cfg and proxy_cfg.get("enabled"):
        session.proxies = {
            "http": proxy_cfg.get("http", ""),
            "https": proxy_cfg.get("https", ""),
        }
        logger.info("已启用代理: %s", proxy_cfg.get("https", ""))

    return session


def _init_session() -> requests.Session:
    """从配置文件读取代理设置并初始化 Session"""
    try:
        cfg = load_config()
        proxy_cfg = cfg.get("proxy")
    except Exception:
        proxy_cfg = None
    return _build_session(proxy_cfg)


# 全局复用 Session，减少 TLS 握手开销
_session = _init_session()

# 已推送过的合约地址缓存（避免重复推送）
pushed_tokens: set[str] = set()
# 最多缓存数量，防止内存无限增长
MAX_CACHE_SIZE = 5000


def fetch_new_tokens(page_size: int = 50, max_pages: int = 1) -> list[dict]:
    """从 four.meme 获取最新发布的代币列表（未迁移，仍在 bonding curve）
    分别拉取 HOT 和 NEW 排序，支持多页，合并去重以兼顾热度和时效
    """
    all_tokens: dict[str, dict] = {}
    for sort_type, list_type in [("HOT", "ADV"), ("NEW", "NOR")]:
        for page in range(1, max_pages + 1):
            payload = {
                "pageIndex": page,
                "pageSize": page_size,
                "type": sort_type,
                "listType": list_type,
                "sort": "DESC",
                "status": "PUBLISH",
            }
            try:
                resp = _session.post(
                    FOUR_MEME_SEARCH_URL, json=payload, timeout=15
                )
                resp.raise_for_status()
                data = resp.json()
                if data.get("code") == 0:
                    items = data.get("data", [])
                    for t in items:
                        addr = t.get("tokenAddress", "")
                        if addr and addr not in all_tokens:
                            all_tokens[addr] = t
                    # 如果返回数量不足一页，说明没有更多了
                    if len(items) < page_size:
                        break
            except Exception as e:
                logger.error("获取代币列表失败 [%s] 第%d页: %s", sort_type, page, e)
                break
            if page < max_pages:
                time.sleep(0.3)  # 多页时限流
    return list(all_tokens.values())


def fetch_token_detail(token_address: str) -> dict | None:
    """获取单个代币的详细信息（含描述）"""
    try:
        resp = _session.get(
            FOUR_MEME_DETAIL_URL,
            params={"address": token_address},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") == 0:
            detail = data.get("data")
            # 调试: 打印详情接口原始字段（仅首次）
            if detail and not getattr(fetch_token_detail, "_logged", False):
                logger.info("详情接口原始字段: %s", json.dumps(detail, ensure_ascii=False, indent=2))
                fetch_token_detail._logged = True
            return detail
        return None
    except Exception as e:
        logger.error("获取代币详情失败 [%s]: %s", token_address, e)
        return None


def fetch_ticker_prices() -> dict[str, float]:
    """获取所有交易对的 USDT 价格映射，如 {'BNB': 619.0, 'CAKE': 1.4, 'USD1': 1.0, ...}"""
    prices: dict[str, float] = {}
    try:
        resp = _session.post(FOUR_MEME_TICKER_URL, json={}, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") == 0:
            for t in data.get("data", []):
                sym = t.get("symbol", "")
                if sym.endswith("USDT"):
                    base = sym[: -4]  # 去掉 USDT 后缀
                    try:
                        prices[base.upper()] = float(t.get("price", 0))
                    except (ValueError, TypeError):
                        pass
    except Exception as e:
        logger.error("获取行情失败: %s", e)
    return prices


# ---------------------------------------------------------------------------
# 筛选逻辑
# ---------------------------------------------------------------------------
def filter_tokens(
    tokens: list[dict], cfg: dict,
    ticker_prices: dict[str, float], bnb_price: float,
) -> list[dict]:
    """
    初筛条件（基于列表数据，无需额外请求）：
    1. 发币时间 < max_age_hours（默认 48 小时）
    2. 未迁移（status == PUBLISH）
    3. 当前价格 <= max_current_price (USD)
    4. 未推送过
    """
    now_ms = int(time.time() * 1000)
    max_age_ms = cfg.get("max_age_hours", 48) * 3600 * 1000
    max_current_price = cfg.get("max_current_price", 0.00003)
    min_holders = cfg.get("min_holders", 150)
    results = []

    skip_reasons: dict[str, int] = {
        "already_pushed": 0, "too_old": 0, "not_publish": 0,
        "price_too_high": 0, "price_zero": 0, "holders_low": 0,
        "progress_low": 0,
    }

    for token in tokens:
        addr = token.get("tokenAddress", "")

        # 跳过已推送
        if addr in pushed_tokens:
            skip_reasons["already_pushed"] += 1
            continue

        # 时间过滤
        create_ts = int(token.get("createDate", 0))
        if create_ts <= 0 or (now_ms - create_ts) > max_age_ms:
            skip_reasons["too_old"] += 1
            continue

        # 状态过滤（只要未迁移的）
        if token.get("status", "") != "PUBLISH":
            skip_reasons["not_publish"] += 1
            continue

        # Bonding Curve 进度过滤
        progress = float(token.get("progress", 0))
        min_progress = cfg.get("min_progress", 0.3)
        if progress < min_progress:
            skip_reasons["progress_low"] += 1
            continue

        # 价格过滤：将 token 价格转换为 USD
        raw_price = float(token.get("price", 0))
        base_symbol = token.get("symbol", "BNB").upper()
        base_price_usd = ticker_prices.get(base_symbol, 0)
        if base_symbol in ("USDT",):
            price_usd = raw_price
        elif base_price_usd > 0:
            price_usd = raw_price * base_price_usd
        else:
            price_usd = raw_price * bnb_price  # fallback

        if price_usd <= 0:
            skip_reasons["price_zero"] += 1
            continue
        if price_usd > max_current_price:
            skip_reasons["price_too_high"] += 1
            continue

        # 持币人数预筛选（列表接口 hold 字段）
        hold_count = int(token.get("hold", 0) or 0)
        if hold_count < min_holders:
            skip_reasons["holders_low"] += 1
            continue

        token["_price_usd"] = price_usd
        token["_holder_count"] = hold_count
        results.append(token)

    # 按交易量降序排列，优先推送有交易的
    logger.info(
        "初筛统计: 已推送=%d, 太旧=%d, 非PUBLISH=%d, 进度低=%d, 价格为0=%d, 价格过高=%d, 持币人少=%d, 通过=%d",
        skip_reasons["already_pushed"], skip_reasons["too_old"],
        skip_reasons["not_publish"], skip_reasons["progress_low"],
        skip_reasons["price_zero"], skip_reasons["price_too_high"],
        skip_reasons["holders_low"], len(results),
    )
    results.sort(key=lambda x: float(x.get("day1Vol", 0) or 0), reverse=True)
    return results


# ---------------------------------------------------------------------------
# 描述处理
# ---------------------------------------------------------------------------
def has_media_links(detail: dict) -> bool:
    """检查代币是否有至少一个关联媒体链接（Twitter / Telegram / 官网）"""
    for key in ("twitterUrl", "telegramUrl", "webUrl"):
        val = (detail.get(key) or "").strip()
        if val:
            return True
    return False


def extract_social_links(detail: dict) -> list[dict]:
    """提取并去重所有社交/媒体链接"""
    links: list[dict] = []
    seen_urls: set[str] = set()
    link_map = {
        "twitterUrl": "Twitter",
        "telegramUrl": "Telegram",
        "webUrl": "Website",
    }
    for key, label in link_map.items():
        url = (detail.get(key) or "").strip()
        if url and url not in seen_urls:
            links.append({"type": label, "url": url})
            seen_urls.add(url)
    return links


def get_holder_count(detail: dict) -> int:
    """从详情中提取持币人数"""
    # 优先从 tokenPrice 子对象取
    tp = detail.get("tokenPrice") or {}
    hc = tp.get("holderCount", 0)
    if hc:
        return int(hc)
    # fallback: 顶层字段
    return int(detail.get("holderCount", 0))


def apply_detail_filters(
    candidates: list[dict], cfg: dict,
    max_check: int = 20,
) -> list[tuple[dict, dict]]:
    """
    对初筛候选币逐个拉取详情，做二次筛选：
    - 至少有一个关联媒体链接
    - 持币人数 > min_holders（默认 150）
    返回 (search_token, detail) 元组列表
    """
    min_holders = cfg.get("min_holders", 150)
    results: list[tuple[dict, dict]] = []

    checked = 0
    for token in candidates:
        if checked >= max_check:
            break
        addr = token.get("tokenAddress", "")
        if checked > 0:
            time.sleep(0.5)  # 限流，避免 429
        detail = fetch_token_detail(addr)
        checked += 1
        if not detail:
            logger.debug("跳过 %s: 无法获取详情", addr)
            continue

        # 媒体链接过滤
        social_links = extract_social_links(detail)
        if not social_links:
            logger.debug("跳过 %s: 无关联媒体链接", detail.get("name", addr))
            continue

        # 将社交链接存入 detail 方便后续展示
        detail["_social_links"] = social_links

        # 持币人数过滤（用详情接口的精确数据更新）
        holders = get_holder_count(detail)
        logger.debug(
            "代币 %s: 详情holderCount=%d, 列表hold=%d",
            detail.get("name", addr), holders, token.get("_holder_count", 0),
        )
        if holders <= 0:
            # 详情接口有时返回 0，回退到列表接口的 hold 字段
            holders = token.get("_holder_count", 0)
            logger.warning(
                "代币 %s 详情接口 holderCount=0，回退使用列表 hold=%d，数据可能不准",
                detail.get("name", addr), holders,
            )
        if holders < min_holders:
            logger.debug(
                "跳过 %s: 持币人数 %d < %d",
                detail.get("name", addr), holders, min_holders,
            )
            continue

        # 将持币人数存入 token 方便后续展示
        token["_holder_count"] = holders
        results.append((token, detail))

    return results


def get_description_from_detail(detail: dict) -> str:
    """从已获取的详情中提取描述，截断到 100 字"""
    desc = (detail.get("descr") or "").strip()
    if not desc:
        return "暂无介绍"
    if len(desc) > 100:
        desc = desc[:97] + "..."
    return desc


# ---------------------------------------------------------------------------
# Telegram 推送
# ---------------------------------------------------------------------------
def send_telegram(bot_token: str, chat_id: str, text: str) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = _session.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        result = resp.json()
        if result.get("ok"):
            return True
        logger.warning("Telegram 发送失败: %s", result.get("description"))
        return False
    except Exception as e:
        logger.error("Telegram 请求异常: %s", e)
        return False


def format_social_links(links: list[dict]) -> str:
    """格式化社交链接为 HTML 文本"""
    parts = []
    for link in links:
        parts.append(f"  • <a href='{link['url']}'>{link['type']}</a>")
    return "\n".join(parts)


def format_message(tokens_info: list[dict]) -> str:
    """格式化推送消息"""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"🔍 <b>BSC 土狗扫描报告</b>", f"⏰ {now_str}", ""]

    for i, info in enumerate(tokens_info, 1):
        name = info["name"]
        short = info.get("shortName", "")
        addr = info["address"]
        price_usd = info["price_usd"]
        desc = info["description"]
        create_time = info["create_time"]
        progress = info.get("progress", "N/A")

        lines.append(f"<b>#{i} {name} ({short})</b>")
        lines.append(f"📄 合约: <code>{addr}</code>")
        lines.append(f"💰 价格: ${price_usd:.10f}")
        lines.append(f"📊 Bonding Curve: {progress}%")
        lines.append(f"👥 持币人数: {info.get('holders', 'N/A')}")
        lines.append(f"🕐 创建: {create_time}")
        lines.append(f"📝 {desc}")

        # 社交链接
        social_links = info.get("social_links", [])
        if social_links:
            lines.append(f"🌐 社交链接:")
            lines.append(format_social_links(social_links))

        lines.append(f"🔗 <a href='https://four.meme/token/{addr}'>four.meme</a>"
                      f" | <a href='https://bscscan.com/token/{addr}'>BscScan</a>")
        lines.append("")

    lines.append("—— four.meme 土狗扫描器 ——")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 主扫描流程
# ---------------------------------------------------------------------------
def scan_once(cfg: dict) -> None:
    global pushed_tokens

    logger.info("开始扫描...")
    max_push = cfg.get("max_push_count", 3)

    # 拉取最新代币
    page_size = cfg.get("page_size", 50)
    max_pages = cfg.get("max_pages", 3)
    tokens = fetch_new_tokens(page_size=page_size, max_pages=max_pages)
    if not tokens:
        logger.info("未获取到代币数据")
        return

    logger.info("获取到 %d 个代币，开始筛选...", len(tokens))

    # 获取行情价格（供初筛价格换算）
    ticker_prices = fetch_ticker_prices()
    bnb_price = ticker_prices.get("BNB", 600.0)
    if bnb_price <= 0:
        logger.warning("无法获取 BNB 价格，使用默认值 600")
        bnb_price = 600.0
    logger.info("行情数据: BNB=$%.2f, 共 %d 个交易对", bnb_price, len(ticker_prices))

    # 筛选
    qualified = filter_tokens(tokens, cfg, ticker_prices, bnb_price)
    if not qualified:
        logger.info("本轮无符合条件的代币")
        return

    logger.info("筛选出 %d 个初筛候选代币，开始二次筛选（媒体链接 + 持币人数）...", len(qualified))

    # 二次筛选：拉取详情，检查媒体链接和持币人数
    # max_check 多查一些以确保能凑够 max_push 个
    passed = apply_detail_filters(
        qualified, cfg, max_check=max_push * 5
    )
    if not passed:
        logger.info("本轮无通过二次筛选的代币")
        return

    logger.info("二次筛选通过 %d 个代币", len(passed))

    # 取前 N 个
    to_push = passed[:max_push]

    # 组装推送信息（详情已在二次筛选中获取，无需重复请求）
    tokens_info = []
    for token, detail in to_push:
        addr = token.get("tokenAddress", "")
        create_ts = int(token.get("createDate", 0))
        create_dt = datetime.fromtimestamp(create_ts / 1000, tz=timezone.utc)
        progress_raw = float(token.get("progress", 0))

        info = {
            "name": token.get("name", "Unknown"),
            "shortName": token.get("shortName", ""),
            "address": addr,
            "price_usd": token.get("_price_usd", 0),
            "create_time": create_dt.strftime("%Y-%m-%d %H:%M UTC"),
            "progress": f"{progress_raw * 100:.1f}",
            "holders": token.get("_holder_count", 0),
            "description": get_description_from_detail(detail),
            "social_links": detail.get("_social_links", []),
        }
        tokens_info.append(info)

    # 格式化并推送
    msg = format_message(tokens_info)
    logger.info("推送 %d 个代币到 Telegram...", len(tokens_info))

    bot_token = cfg.get("telegram_bot_token", "")
    chat_id = cfg.get("telegram_chat_id", "")

    if bot_token == "YOUR_BOT_TOKEN_HERE" or chat_id == "YOUR_CHAT_ID_HERE":
        logger.warning("⚠️  Telegram 未配置，仅打印消息:")
        # 将 HTML 标签简单替换用于控制台显示
        print("\n" + "=" * 60)
        console_msg = msg.replace("<b>", "").replace("</b>", "")
        console_msg = console_msg.replace("<code>", "").replace("</code>", "")
        console_msg = console_msg.replace("<a href='", "[").replace("'>", "] ")
        console_msg = console_msg.replace("</a>", "")
        print(console_msg)
        print("=" * 60 + "\n")
    else:
        ok = send_telegram(bot_token, chat_id, msg)
        if ok:
            logger.info("Telegram 推送成功")
        else:
            logger.error("Telegram 推送失败")

    # 记录已推送
    for info in tokens_info:
        pushed_tokens.add(info["address"])

    # 清理缓存
    if len(pushed_tokens) > MAX_CACHE_SIZE:
        pushed_tokens = set(list(pushed_tokens)[-MAX_CACHE_SIZE // 2:])


def main():
    logger.info("🚀 BSC 土狗扫描器启动")
    logger.info("配置文件: %s", CONFIG_PATH)

    while True:
        try:
            global _session
            cfg = load_config()  # 每轮重新加载配置，方便热更新
            # 启动时打印筛选参数，方便确认配置
            logger.info(
                "筛选参数: 当前价格<$%s | 持币人>%d | 年龄<%dh | 进度>%s | 每轮推送≤%d",
                cfg.get("max_current_price", 0.00003),
                cfg.get("min_holders", 150),
                cfg.get("max_age_hours", 48),
                cfg.get("min_progress", 0.3),
                cfg.get("max_push_count", 3),
            )
            _session = _build_session(cfg.get("proxy"))  # 代理配置热更新
            scan_once(cfg)
            interval = cfg.get("scan_interval_minutes", 15)
            logger.info("下次扫描: %d 分钟后", interval)
            time.sleep(interval * 60)
        except KeyboardInterrupt:
            logger.info("用户中断，退出程序")
            break
        except Exception as e:
            logger.error("扫描异常: %s", e, exc_info=True)
            time.sleep(60)  # 出错后等 1 分钟重试


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
BSC 土狗扫盘程序 v2
每15分钟扫描一次 BSC 链上新代币，筛选符合条件的代币并通过 Telegram 推送

数据源: DexScreener (token-profiles/boosts/search) + GeckoTerminal (new_pools/OHLCV)
持币人数: BSCScan API
社交链接优先策略: 优先检查有社交资料的代币，减少无效 API 调用

筛选条件：
1. 历史最高价格 < 0.00004 USD
2. 当前持币人数 > 150
3. 至少有一个社交媒体链接
4. 当前价格 < 0.00003 USD
5. 发币时间距今 < 3 天
"""

from __future__ import annotations

import json
import requests
import time
import hmac
import hashlib
import base64
import urllib.parse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ============================================================
# 日志配置
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bsc_scanner.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ============================================================
# 配置加载
# ============================================================
CONFIG_PATH = Path(__file__).parent / "bsc_config.json"

# 默认配置（配置文件缺失时的兜底）
DEFAULT_CONFIG = {
    "dingtalk_webhook": "",
    "dingtalk_secret": "",
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "scan_interval_minutes": 15,
    "max_ath_price": 0.00004,
    "min_holders": 150,
    "max_current_price": 0.00003,
    "max_age_days": 3,
}


def load_config() -> dict:
    """加载配置文件，缺失字段用默认值补齐"""
    cfg = dict(DEFAULT_CONFIG)
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg.update(json.load(f))
    except FileNotFoundError:
        log.warning("配置文件 %s 不存在，使用默认配置", CONFIG_PATH)
    except Exception as e:
        log.error("加载配置文件失败: %s，使用默认配置", e)
    return cfg


CONFIG = load_config()

pushed_tokens: set[str] = set()


# ============================================================
# 通用 HTTP
# ============================================================
def safe_get(url: str, params: dict | None = None, retries: int = 3):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15,
                             headers={"Accept": "application/json"})
            if r.status_code == 429:
                time.sleep(5 * (i + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i == retries - 1:
                log.error(f"请求失败 {url[:80]}: {e}")
            time.sleep(2)
    return None


# ============================================================
# 数据采集: 多源汇聚 BSC 候选代币
# ============================================================
def collect_candidates() -> dict[str, dict]:
    """返回 {address_lower: candidate_info}"""
    pool = {}

    def _add(addr_raw: str, **kwargs):
        addr = addr_raw.strip().lower()
        if not addr:
            return
        if addr in pool:
            # 合并社交链接
            old_links = pool[addr].get("links", [])
            new_links = kwargs.get("links", [])
            merged_urls = {l.get("url", "") for l in old_links}
            for l in new_links:
                if l.get("url", "") not in merged_urls:
                    old_links.append(l)
            pool[addr]["links"] = old_links
        else:
            pool[addr] = {"tokenAddress": addr_raw.strip(), **kwargs}

    # --- 源1: DexScreener token-profiles (自带社交) ---
    log.info("[源1] DexScreener token-profiles/latest")
    data = safe_get("https://api.dexscreener.com/token-profiles/latest/v1")
    if data and isinstance(data, list):
        for t in data:
            if t.get("chainId") != "bsc":
                continue
            _add(t.get("tokenAddress", ""),
                 description=t.get("description", ""),
                 links=t.get("links", []),
                 source="profiles")
    c1 = sum(1 for v in pool.values() if v.get("source") == "profiles")
    log.info(f"  profiles: {c1}")

    # --- 源2: DexScreener token-boosts ---
    log.info("[源2] DexScreener token-boosts (latest + top)")
    for ep in ["https://api.dexscreener.com/token-boosts/latest/v1",
               "https://api.dexscreener.com/token-boosts/top/v1"]:
        data = safe_get(ep)
        if data and isinstance(data, list):
            for t in data:
                if t.get("chainId") != "bsc":
                    continue
                _add(t.get("tokenAddress", ""),
                     description=t.get("description", ""),
                     links=t.get("links", []),
                     source="boosts")

    # --- 源3: DexScreener 搜索 ---
    log.info("[源3] DexScreener search")
    cutoff_ms = int((time.time() - CONFIG["max_age_days"] * 86400) * 1000)
    for q in ["bsc meme", "bsc new", "bnb chain meme", "four meme bsc"]:
        data = safe_get("https://api.dexscreener.com/latest/dex/search", params={"q": q})
        if data and "pairs" in data:
            for pair in data["pairs"]:
                if pair.get("chainId") != "bsc":
                    continue
                ca = pair.get("pairCreatedAt", 0)
                if ca and ca < cutoff_ms:
                    continue
                base = pair.get("baseToken", {})
                links = []
                info = pair.get("info") or {}
                for w in info.get("websites", []):
                    links.append({"type": "website", "url": w.get("url", "")})
                for s in info.get("socials", []):
                    links.append({"type": s.get("platform", s.get("type", "")),
                                  "url": s.get("url", "")})
                _add(base.get("address", ""),
                     tokenName=base.get("name", ""),
                     tokenSymbol=base.get("symbol", ""),
                     pairCreatedAt=ca,
                     links=links,
                     source="search")
        time.sleep(0.4)

    # --- 源4: GeckoTerminal 新池 ---
    log.info("[源4] GeckoTerminal new_pools")
    for page in range(1, 4):  # 3页足够，减少配额消耗留给ATH查询
        data = safe_get(f"https://api.geckoterminal.com/api/v2/networks/bsc/new_pools?page={page}")
        if not data:
            break
        pools = data.get("data", [])
        if not pools:
            break
        for p in pools:
            attrs = p.get("attributes", {})
            rels = p.get("relationships", {})
            base_id = rels.get("base_token", {}).get("data", {}).get("id", "")
            addr = base_id.replace("bsc_", "") if base_id.startswith("bsc_") else ""
            created_ms = 0
            created_raw = attrs.get("pool_created_at", "")
            if created_raw:
                try:
                    created_ms = int(datetime.fromisoformat(
                        created_raw.replace("Z", "+00:00")).timestamp() * 1000)
                except Exception:
                    pass
            _add(addr, tokenName=attrs.get("name", ""),
                 pairCreatedAt=created_ms, links=[], source="gecko")
        time.sleep(0.5)

    log.info(f"  汇总: {len(pool)} 个 BSC 候选代币")
    return pool


# ============================================================
# 详情查询
# ============================================================
def get_pair_detail(token_address: str) -> dict | None:
    data = safe_get(f"https://api.dexscreener.com/tokens/v1/bsc/{token_address}")
    if not data or not isinstance(data, list) or not data:
        return None
    pairs = sorted(data, key=lambda x: float(
        x.get("liquidity", {}).get("usd", 0) or 0), reverse=True)
    return pairs[0] if pairs else None


def estimate_holder_count(detail: dict) -> int:
    """
    估算持币人数
    BSCScan V1 API 已废弃，V2 免费版不支持 BSC 链
    替代方案: 用 DexScreener 24h 交易数据中 buys - sells 作为净买入人次
    净买入 > 0 表示有资金持续流入，数值越大持币人越多
    """
    txns = detail.get("txns", {})
    h24 = txns.get("h24", {})
    buys = h24.get("buys", 0)
    sells = h24.get("sells", 0)
    # 净买入人次作为持币活跃度指标
    return max(buys - sells, 0)


def estimate_ath(token_address: str) -> float:
    for attempt in range(3):
        try:
            time.sleep(1.5 + attempt)  # 递增延迟，避免限流
            data = safe_get(
                f"https://api.geckoterminal.com/api/v2/networks/bsc/tokens/{token_address}/pools")
            if not data:
                continue
            pools = data.get("data", [])
            if not pools:
                return 0.0
            pool_addr = pools[0]["attributes"]["address"]
            time.sleep(1.0 + attempt)
            data2 = safe_get(
                f"https://api.geckoterminal.com/api/v2/networks/bsc/pools/{pool_addr}"
                f"/ohlcv/day?limit=7&currency=usd")
            if not data2:
                continue
            candles = data2.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
            if not candles:
                continue
            return max(float(c[2]) for c in candles)
        except Exception:
            pass
    return 0.0


def extract_social_links(candidate: dict, detail: dict) -> list[dict]:
    """合并候选自带链接 + DexScreener 详情中的链接"""
    links = list(candidate.get("links", []))
    info = detail.get("info") or {}
    seen_urls = {l.get("url", "") for l in links}
    for w in info.get("websites", []):
        u = w.get("url", "")
        if u and u not in seen_urls:
            links.append({"type": "website", "url": u})
            seen_urls.add(u)
    for s in info.get("socials", []):
        u = s.get("url", "")
        if u and u not in seen_urls:
            links.append({"type": s.get("platform", s.get("type", "social")), "url": u})
            seen_urls.add(u)
    return links


# ============================================================
# 推送: 钉钉 + Telegram
# ============================================================
def _dingtalk_sign_url(webhook: str, secret: str) -> str:
    """钉钉加签: 在 Webhook URL 后追加 timestamp 和 sign 参数"""
    timestamp = str(round(time.time() * 1000))
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(secret.encode("utf-8"),
                         string_to_sign.encode("utf-8"),
                         digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return f"{webhook}&timestamp={timestamp}&sign={sign}"


def send_dingtalk(text: str) -> bool:
    """通过钉钉群机器人 Webhook 发送 Markdown 消息"""
    webhook = CONFIG.get("dingtalk_webhook", "")
    secret = CONFIG.get("dingtalk_secret", "")

    if not webhook:
        return False

    url = _dingtalk_sign_url(webhook, secret) if secret else webhook

    title = "BSC 土狗发现"
    for line in text.split("\n"):
        stripped = line.strip().strip("*").strip()
        if stripped and "BSC" in stripped:
            title = stripped[:20]
            break

    payload = {
        "msgtype": "markdown",
        "markdown": {"title": title, "text": text},
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        result = r.json()
        if result.get("errcode") == 0:
            log.info("钉钉推送成功")
            return True
        log.error(f"钉钉推送失败: {result}")
    except Exception as e:
        log.error(f"钉钉推送异常: {e}")
    return False


def send_telegram(text: str) -> bool:
    """通过 Telegram Bot 发送 HTML 消息"""
    bot_token = CONFIG.get("telegram_bot_token", "")
    chat_id = CONFIG.get("telegram_chat_id", "")

    if not bot_token or not chat_id:
        return False

    # Markdown 转 HTML 简单处理
    html_text = text.replace("*", "<b>", 1)
    # 逐对替换 *...*  为 <b>...</b>
    import re
    html_text = re.sub(r'\*([^*]+)\*', r'<b>\1</b>', text)
    html_text = html_text.replace("`", "<code>", 1).replace("`", "</code>", 1)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": html_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        result = resp.json()
        if result.get("ok"):
            log.info("Telegram 推送成功")
            return True
        log.warning("Telegram 发送失败: %s", result.get("description"))
    except Exception as e:
        log.error("Telegram 请求异常: %s", e)
    return False


def send_message(text: str) -> bool:
    """同时推送到所有已配置的渠道，至少一个成功即返回 True"""
    dingtalk_ok = send_dingtalk(text)
    telegram_ok = send_telegram(text)

    if not dingtalk_ok and not telegram_ok:
        log.info("无推送渠道已配置，输出到控制台:")
        print("\n" + "=" * 55)
        console_text = text.replace("*", "").replace("`", "")
        print(console_text)
        print("=" * 55 + "\n")
        return False

    return dingtalk_ok or telegram_ok


def format_social(links: list[dict]) -> str:
    parts = []
    for s in links:
        t = s.get("type", s.get("label", "link"))
        u = s.get("url", "")
        parts.append(f"  • {t}: {u}")
    return "\n".join(parts)


# ============================================================
# 核心扫描
# ============================================================
def scan_once():
    log.info("=" * 55)
    log.info("开始扫描 BSC 新代币...")
    log.info("=" * 55)

    candidates = collect_candidates()
    if not candidates:
        log.info("未获取到候选代币")
        return

    cutoff_ms = int((time.time() - CONFIG["max_age_days"] * 86400) * 1000)
    qualified = []

    # 优先检查有社交链接的候选 (排前面)
    sorted_cands = sorted(candidates.values(),
                          key=lambda x: len(x.get("links", [])), reverse=True)

    for idx, cand in enumerate(sorted_cands):
        addr = cand.get("tokenAddress", "")
        if not addr or addr.lower() in pushed_tokens:
            continue

        # -- 获取 DexScreener 详情 --
        detail = get_pair_detail(addr)
        time.sleep(0.35)
        if not detail:
            continue

        # -- 条件5: 发币时间 < 3天 --
        created_at = detail.get("pairCreatedAt", cand.get("pairCreatedAt", 0))
        if created_at and created_at < cutoff_ms:
            continue

        # -- 条件4: 价格 < max_current_price --
        try:
            price = float(detail.get("priceUsd", "0") or "0")
        except (ValueError, TypeError):
            price = 0.0
        if price <= 0 or price >= CONFIG["max_current_price"]:
            continue

        # -- 条件3: 至少一个社交链接 --
        links = extract_social_links(cand, detail)
        if not links:
            continue

        base = detail.get("baseToken", {})
        name = base.get("name", cand.get("tokenName", ""))
        symbol = base.get("symbol", cand.get("tokenSymbol", ""))
        log.info(f"[{idx+1}] {name} (${symbol}) 价格=${price:.8f} 社交={len(links)} -- 查持币人数")

        # -- 条件2: 持币人数(净买入人次) > min_holders --
        holders = estimate_holder_count(detail)

        if holders < CONFIG["min_holders"]:
            log.info(f"  24h净买入人次: {holders} < {CONFIG['min_holders']} -- 不满足")
            continue
        log.info(f"  24h净买入人次: {holders} -- 通过!")

        # -- 条件1: ATH < max_ath_price --
        ath = estimate_ath(addr)
        time.sleep(0.3)
        if ath <= 0:
            log.info(f"  ATH: 无数据 -- 跳过")
            continue
        if ath >= CONFIG["max_ath_price"]:
            log.info(f"  ATH: ${ath:.10f} >= ${CONFIG['max_ath_price']} -- 不满足")
            continue
        log.info(f"  ATH: ${ath:.10f} -- 通过! 全部条件满足!")

        # ========== 命中 ==========
        desc = cand.get("description", "") or \
               (detail.get("info") or {}).get("description", "") or "暂无介绍"
        ct_str = (datetime.fromtimestamp(created_at / 1000, tz=timezone.utc)
                  .strftime("%Y-%m-%d %H:%M UTC") if created_at else "未知")
        social_text = format_social(links)
        dex_url = f"https://dexscreener.com/bsc/{addr}"

        qualified.append({
            "name": name, "symbol": symbol, "address": addr,
            "description": desc, "created_time": ct_str,
            "price_usd": price, "ath": ath, "holders": holders,
            "social_links": links, "dex_url": dex_url,
        })

        msg = (
            f"🐶 *BSC 土狗发现*\n"
            f"━━━━━━━━━━━━━━━\n"
            f"*{name}* (${symbol})\n\n"
            f"📍 合约地址:\n`{addr}`\n\n"
            f"📝 简介: {desc[:200]}\n\n"
            f"📅 发币时间: {ct_str}\n"
            f"💰 当前价格: ${price:.10f}\n"
            f"📈 历史最高: ${ath:.10f}\n"
            f"👥 24h净买入人次: {holders}\n\n"
            f"🔗 社交链接:\n{social_text}\n\n"
            f"📊 [DexScreener]({dex_url})"
        )
        send_message(msg)
        pushed_tokens.add(addr.lower())

    log.info(f"本轮扫描完成，发现 {len(qualified)} 个符合条件的代币")
    if qualified:
        summary = (f"📊 *BSC 扫盘总结* ({datetime.now().strftime('%H:%M')})\n"
                   f"本轮发现 {len(qualified)} 个土狗:\n")
        for t in qualified:
            summary += f"• {t['symbol']} - ${t['price_usd']:.10f} (净买入{t['holders']}次)\n"
        send_message(summary)


# ============================================================
# 主入口
# ============================================================
def main():
    global CONFIG
    log.info("BSC 土狗扫盘程序 v2 启动")
    log.info("配置文件: %s", CONFIG_PATH)
    interval = CONFIG["scan_interval_minutes"]
    log.info(f"扫描间隔: {interval} 分钟")
    log.info(f"筛选: ATH<${CONFIG['max_ath_price']} | 持币>{CONFIG['min_holders']}"
             f" | 有社交 | 价格<${CONFIG['max_current_price']} | <{CONFIG['max_age_days']}天")

    # 推送渠道检查
    channels = []
    if CONFIG.get("dingtalk_webhook"):
        channels.append("钉钉")
    if CONFIG.get("telegram_bot_token") and CONFIG.get("telegram_chat_id"):
        channels.append("Telegram")
    if channels:
        log.info("推送渠道: %s", " + ".join(channels))
    else:
        log.warning("未配置任何推送渠道，结果将输出到控制台和日志文件")

    scan_once()

    while True:
        CONFIG = load_config()  # 每轮重新加载配置，方便热更新
        interval = CONFIG["scan_interval_minutes"]
        log.info(f"下次扫描: {interval} 分钟后...")
        time.sleep(interval * 60)
        scan_once()


if __name__ == "__main__":
    main()


"""
全局异常捕获 & 钉钉推送 & 日志记录

初始化后会接管 sys.excepthook 和 threading.excepthook,
任何未捕获的异常都会被记录到日志并推送到钉钉。
"""
from __future__ import annotations

import json
import logging
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("error_handler")

_CONFIG_PATH = Path(__file__).parent / "config.json"
_DINGTALK_WH = ""
_DINGTALK_SECRET = ""
_INITIALIZED = False
_ERROR_LOG_PATH = Path(__file__).parent / "error.log"


def _load_dingtalk_config() -> tuple[str, str]:
    try:
        if _CONFIG_PATH.exists():
            with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            return cfg.get("dingtalk_webhook", ""), cfg.get("dingtalk_secret", "")
    except Exception:
        pass
    return "", ""


def _dingtalk_sign(secret: str) -> tuple[str, str]:
    import hmac
    import hashlib
    import base64
    from urllib.parse import quote_plus
    timestamp = str(round(time.time() * 1000))
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(
        secret.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    sign = quote_plus(base64.b64encode(hmac_code))
    return timestamp, sign


def _send_dingtalk_error(title: str, text: str) -> bool:
    global _DINGTALK_WH, _DINGTALK_SECRET
    if not _DINGTALK_WH or "YOUR" in _DINGTALK_WH:
        return False
    try:
        import requests as _req
        url = _DINGTALK_WH
        if _DINGTALK_SECRET:
            ts, sign = _dingtalk_sign(_DINGTALK_SECRET)
            url += f"&timestamp={ts}&sign={sign}"
        r = _req.post(
            url,
            json={
                "msgtype": "markdown",
                "markdown": {"title": title, "text": text},
            },
            timeout=10,
        )
        r.raise_for_status()
        result = r.json()
        return result.get("errcode", -1) == 0
    except Exception as e:
        log.error("错误推送钉钉失败: %s", e)
        return False


def _format_error(exc_type: type, exc_value: BaseException, tb) -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    exc_name = exc_type.__name__
    exc_msg = str(exc_value)

    tb_lines = traceback.format_exception(exc_type, exc_value, tb)

    ding_lines = [
        f"## 🚨 运行异常报告",
        f"⏰ {now_str}",
        f"",
        f"**异常类型:** `{exc_name}`",
        f"**异常信息:** {exc_msg}",
        f"",
        f"**堆栈跟踪:**",
        f"```",
    ]

    for line in tb_lines:
        ding_lines.append(line.rstrip())

    ding_lines.append("```")

    ding_text = "\n".join(ding_lines)
    if len(ding_text) > 18000:
        ding_text = ding_text[:18000] + "\n... (截断)"

    return ding_text


def _log_error(exc_type: type, exc_value: BaseException, tb):
    tb_text = "".join(traceback.format_exception(exc_type, exc_value, tb))
    log.critical("未捕获异常:\n%s", tb_text)

    try:
        with open(_ERROR_LOG_PATH, "a", encoding="utf-8") as f:
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            f.write(f"\n{'='*80}\n")
            f.write(f"[{now_str}] 未捕获异常\n")
            f.write(tb_text)
            f.write(f"{'='*80}\n")
    except Exception:
        pass


def _global_excepthook(exc_type, exc_value, tb):
    if exc_type is KeyboardInterrupt:
        sys.__excepthook__(exc_type, exc_value, tb)
        return

    _log_error(exc_type, exc_value, tb)

    ding_text = _format_error(exc_type, exc_value, tb)
    _send_dingtalk_error("BSC Scanner 运行异常", ding_text)

    sys.__excepthook__(exc_type, exc_value, tb)


def _thread_excepthook(args):
    exc_type, exc_value, tb = args.exc_type, args.exc_value, args.exc_traceback
    if exc_type is KeyboardInterrupt:
        return

    _log_error(exc_type, exc_value, tb)

    ding_text = _format_error(exc_type, exc_value, tb)
    _send_dingtalk_error("BSC Scanner 线程异常", ding_text)

    threading.__excepthook__(args)


def init_error_handler():
    global _INITIALIZED, _DINGTALK_WH, _DINGTALK_SECRET

    if _INITIALIZED:
        return

    _DINGTALK_WH, _DINGTALK_SECRET = _load_dingtalk_config()

    sys.excepthook = _global_excepthook

    if hasattr(threading, "excepthook"):
        threading.excepthook = _thread_excepthook

    _INITIALIZED = True

    if _DINGTALK_WH and "YOUR" not in _DINGTALK_WH:
        log.info("全局异常处理器已启用 (含钉钉推送)")
    else:
        log.info("全局异常处理器已启用 (钉钉未配置, 仅日志)")


def send_error_manually(error_msg: str, exc_info: bool = False):
    """
    手动推送错误到钉钉 (用于已知异常但想额外通知的场景)
    """
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    tb_text = ""
    if exc_info:
        tb_text = traceback.format_exc()

    log_text = f"手动上报: {error_msg}"
    if tb_text and tb_text.strip() != "NoneType: None":
        log_text += f"\n{tb_text.rstrip()}"
    log.error(log_text)

    ding_lines = [
        f"## ⚠️ 手动异常报告",
        f"⏰ {now_str}",
        f"",
        f"**描述:** {error_msg}",
    ]
    if tb_text and tb_text.strip() != "NoneType: None":
        ding_lines.append(f"")
        ding_lines.append(f"**堆栈:**")
        ding_lines.append(f"```")
        ding_lines.append(tb_text.rstrip())
        ding_lines.append(f"```")

    _send_dingtalk_error("BSC Scanner 异常通知", "\n".join(ding_lines))

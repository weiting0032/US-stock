import functools
import json
import math
import os
import threading
import time
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import gspread
import numpy as np
import pandas as pd
import pytz
import requests
import yfinance as yf

try:
    import streamlit as st
except Exception:
    st = None


def ttl_cache(ttl_seconds: float, maxsize: int = 1024):
    """執行緒安全的 TTL 快取（提供 .cache_clear()，與 lru_cache 介面相容）。

    用於即時行情類函式：lru_cache 在 Streamlit 常駐進程中永不過期，會讓盤中報價
    凍結整個 session（NAV／訊號全是進程啟動當下的舊價）。改用本快取後，超過 TTL
    會自動重抓；且不依賴 Streamlit ScriptRunContext，在 ThreadPoolExecutor 掃描
    與 GitHub Actions 短進程中皆正常運作。
    """
    def decorator(fn):
        store: Dict[tuple, tuple] = {}
        lock = threading.Lock()

        @functools.wraps(fn)
        def wrapper(*args):
            now = time.time()
            with lock:
                hit = store.get(args)
                if hit is not None and now - hit[1] < ttl_seconds:
                    return hit[0]
            value = fn(*args)
            with lock:
                if len(store) >= maxsize:
                    store.clear()
                store[args] = (value, now)
            return value

        def cache_clear():
            with lock:
                store.clear()

        wrapper.cache_clear = cache_clear
        return wrapper

    return decorator


# ===============================
# Env helpers
# ===============================
_secrets_probe_ok = None  # None=未探測；True/False=st.secrets 是否可用（探測一次後記住）


def _config_raw(name: str) -> Optional[str]:
    """讀取設定原始值：優先「環境變數」，其次「Streamlit secrets」，皆無回 None。

    讓策略參數（EXIT_* / ENTRY_* / SCORE_* …）有單一套用管道：
      • GitHub Actions 跑 scanner.py → 由 workflow env 帶入環境變數
      • Streamlit App 部署          → 由 App 的 Secrets（st.secrets）帶入
    st.secrets 只探測一次；在非 Streamlit（如 Actions）環境探測失敗即記住並不再嘗試，
    避免每個參數重複觸發例外與日誌噪音。
    """
    val = os.getenv(name)
    if val is not None and str(val).strip() != "":
        return str(val).strip()

    global _secrets_probe_ok
    if st is not None and _secrets_probe_ok is not False:
        try:
            has = name in st.secrets
            _secrets_probe_ok = True
            if has:
                sv = str(st.secrets[name]).strip()
                if sv != "":
                    return sv
        except Exception:
            _secrets_probe_ok = False
    return None


def get_env_str(name: str, default: str = "") -> str:
    val = _config_raw(name)
    return val if val is not None else default


def get_env_float(name: str, default: float) -> float:
    val = _config_raw(name)
    if val is None:
        return float(default)
    try:
        return float(val)
    except Exception:
        return float(default)


def get_env_int(name: str, default: int) -> int:
    val = _config_raw(name)
    if val is None:
        return int(default)
    try:
        return int(val)
    except Exception:
        return int(default)


PORTFOLIO_SHEET_TITLE = get_env_str("PORTFOLIO_SHEET_TITLE", "US Stock")

TG_TOKEN = get_env_str("TG_TOKEN", "")
TG_CHAT_ID = get_env_str("TG_CHAT_ID", "")

DEFAULT_INITIAL_CAPITAL = get_env_float("INITIAL_CAPITAL", 32000)
MAX_POSITION_WEIGHT = get_env_float("MAX_POSITION_WEIGHT", 0.30)
RISK_PER_TRADE_PCT = get_env_float("RISK_PER_TRADE_PCT", 0.01)
CASH_RESERVE_PCT = get_env_float("CASH_RESERVE_PCT", 0.10)
COOLDOWN_DAYS = get_env_int("COOLDOWN_DAYS", 3)

PRE_ALERT_PCT = get_env_float("PRE_ALERT_PCT", 0.01)
ALERT_MIN_MINUTES = get_env_int("ALERT_MIN_MINUTES", 30)
ALERT_MIN_PRICE_CHANGE = get_env_float("ALERT_MIN_PRICE_CHANGE", 1.0)
ALERT_MIN_SCORE_CHANGE = get_env_float("ALERT_MIN_SCORE_CHANGE", 0.8)

MARKET_CACHE_TTL = get_env_int("MARKET_CACHE_TTL", 300)  # 行情快取存活秒數（盤中報價刷新節奏）
AUTO_SPLIT_ADJUST = get_env_int("AUTO_SPLIT_ADJUST", 1)   # 1=自動把進場後分割對齊到還原權值價（若你已手動改交易股數，設 0）

MIN_AVG_DOLLAR_VOLUME = get_env_float("MIN_AVG_DOLLAR_VOLUME", 20000000)
MIN_PRICE = get_env_float("MIN_PRICE", 10)
EARNINGS_BLOCK_DAYS = get_env_int("EARNINGS_BLOCK_DAYS", 2)
DEFAULT_COMMISSION = get_env_float("DEFAULT_COMMISSION", 0)
DEFAULT_SLIPPAGE_PCT = get_env_float("DEFAULT_SLIPPAGE_PCT", 0.001)

BREAKOUT_ADX_MIN = get_env_float("BREAKOUT_ADX_MIN", 18)
RS_LOOKBACK_DAYS = get_env_int("RS_LOOKBACK_DAYS", 20)
NEAR_52W_HIGH_PCT = get_env_float("NEAR_52W_HIGH_PCT", 0.10)
PORTFOLIO_HEAT_LIMIT_PCT = get_env_float("PORTFOLIO_HEAT_LIMIT_PCT", 0.05)

LARGE_CAP_MAX_WEIGHT = get_env_float("LARGE_CAP_MAX_WEIGHT", 0.25)
SMALL_CAP_MAX_WEIGHT = get_env_float("SMALL_CAP_MAX_WEIGHT", 0.12)
LARGE_CAP_RISK_PER_TRADE_PCT = get_env_float("LARGE_CAP_RISK_PER_TRADE_PCT", 0.01)
SMALL_CAP_RISK_PER_TRADE_PCT = get_env_float("SMALL_CAP_RISK_PER_TRADE_PCT", 0.005)

SIGNAL_LOG_LOOKAHEAD_DAYS = get_env_int("SIGNAL_LOG_LOOKAHEAD_DAYS", 20)

SCORE_BUY_NOW_THRESHOLD = get_env_float("SCORE_BUY_NOW_THRESHOLD", 3.5)
SCORE_BUY_ADD_THRESHOLD = get_env_float("SCORE_BUY_ADD_THRESHOLD", 4.5)
SCALE_IN_MAX_WEIGHT_RATIO = get_env_float("SCALE_IN_MAX_WEIGHT_RATIO", 0.75)
# 出場策略（以進場成本為固定參考；R = 初始風險距離 = 進場 − 初始止損）
EXIT_INIT_STOP_ATR = get_env_float("EXIT_INIT_STOP_ATR", 2.0)    # 初始硬止損：進場 − 2×ATR
EXIT_TRAIL_ATR = get_env_float("EXIT_TRAIL_ATR", 3.0)           # Chandelier 移動停損：High20 − 3×ATR
EXIT_TRAIL_ATR_FINAL = get_env_float("EXIT_TRAIL_ATR_FINAL", 2.0)  # 減碼兩次後剩最後一份：收緊至 2×ATR 鎖更多利
EXIT_TP1_R = get_env_float("EXIT_TP1_R", 2.0)                   # 第一獲利目標：進場 + 2R（分批出場）
EXIT_TP2_R = get_env_float("EXIT_TP2_R", 4.0)                   # 第二獲利目標：進場 + 4R（顯示用）
EXIT_TP1_PCT = get_env_float("EXIT_TP1_PCT", 0.20)             # 第一獲利目標的百分比上限（與 +XR 取較早者；高波動股提早落袋）
EXIT_TP2_PCT = get_env_float("EXIT_TP2_PCT", 0.40)             # 第二獲利目標的百分比上限
EXIT_SCALE_OUT_PCT = get_env_float("EXIT_SCALE_OUT_PCT", 0.34)  # 觸及 TP1 時分批賣出比例（約 1/3）
EXIT_BREAKEVEN_AT_R = get_env_float("EXIT_BREAKEVEN_AT_R", 1.0) # 浮盈達 +1R 後，止損上移至保本（進場價）
EXIT_BREAKEVEN_BUFFER_R = get_env_float("EXIT_BREAKEVEN_BUFFER_R", 0.0)  # P6：保本線緩衝（以 R 計）。0=貼在成本價（現況）；0.25=容忍 0.25R 回踩，避開 +1R~+2R 間「碰成本就死」的洗盤走廊。預設 0 不改變行為，先經 optimize --study grace 樣本外驗證再採用
EXIT_TREND_BREAK = get_env_int("EXIT_TREND_BREAK", 1)          # 趨勢轉弱出場：虧損中且跌破 SMA200 即出場（1=啟用）
EXIT_MIN_HOLD_BARS = get_env_int("EXIT_MIN_HOLD_BARS", 1)      # 新倉保護期（含進場日的日線根數）；期內僅初始硬止損生效，保本/移動/趨勢出場一律不啟用

# ── 進場品質閘（P1）：避免追高，新倉須為「突破」或「回檔」且不過度乖離、強於大盤 ──
ENTRY_REQUIRE_TRIGGER = get_env_int("ENTRY_REQUIRE_TRIGGER", 1)        # 1=新倉須通過突破/回檔觸發
ENTRY_PULLBACK_SMA20_PCT = get_env_float("ENTRY_PULLBACK_SMA20_PCT", 0.04)  # 收盤距 SMA20 在此範圍內視為「回檔買點」
ENTRY_BREAKOUT_VOL_MULT = get_env_float("ENTRY_BREAKOUT_VOL_MULT", 1.3)     # 突破須伴隨量 > 均量 × 此倍數
ENTRY_MAX_EXT_ATR = get_env_float("ENTRY_MAX_EXT_ATR", 4.0)            # 乖離上限：收盤不得高於 SMA20 + N×ATR（追高保護）
ENTRY_REQUIRE_RS_POSITIVE = get_env_int("ENTRY_REQUIRE_RS_POSITIVE", 1)     # 1=新倉要求 RS20 vs SPY > 0（強於大盤）

# ── 加碼只加贏家（P2）：部位需已獲利達門檻才允許金字塔加碼，杜絕往下攤平 ──
ADD_REQUIRE_PROFIT = get_env_int("ADD_REQUIRE_PROFIT", 1)             # 1=僅對獲利中部位加碼
ADD_MIN_PROFIT_R = get_env_float("ADD_MIN_PROFIT_R", 0.5)             # 加碼前現價需站上 進場 + N×R

# ── 時間/呆滯止損（P3）：進場 N 根後仍未達 +1R 且 RS 轉弱 → 釋出資金 ──
TIME_STOP_ENABLE = get_env_int("TIME_STOP_ENABLE", 1)
TIME_STOP_BARS = get_env_int("TIME_STOP_BARS", 20)                    # 呆滯判定的持倉日線根數
TIME_STOP_MIN_R = get_env_float("TIME_STOP_MIN_R", 1.0)              # 此期間內須至少達到的浮盈（以 R 計）

# 產業/相關性曝險控制
CATEGORY_MAX_WEIGHT = get_env_float("CATEGORY_MAX_WEIGHT", 0.40)  # 單一半導體次產業總權重上限（進場/加碼閘）
CORR_WARN_THRESHOLD = get_env_float("CORR_WARN_THRESHOLD", 0.70) # 持倉平均兩兩相關性警示門檻
CORR_LOOKBACK_DAYS = get_env_int("CORR_LOOKBACK_DAYS", 60)       # 相關性計算回看天數
# P5：高相關持倉的逐檔風險縮量——「N 檔各 1% 風險」在相關性 0.8+ 時實為一注大部位，
# 相關性從警示升級為行動：scale = clip(1.3 − avg_corr, FLOOR, 1.0)，corr≤0.3 不縮、0.7→0.6、0.9→0.4
CORR_RISK_SCALE_ENABLE = get_env_int("CORR_RISK_SCALE_ENABLE", 1)
CORR_RISK_SCALE_FLOOR = get_env_float("CORR_RISK_SCALE_FLOOR", 0.4)
RS_STRONG_THRESHOLD = get_env_float("RS_STRONG_THRESHOLD", 2.0)
RS_WEAK_THRESHOLD = get_env_float("RS_WEAK_THRESHOLD", -2.0)
OBV_LOOKBACK = get_env_int("OBV_LOOKBACK", 20)
SCANNER_TOP_N = get_env_int("SCANNER_TOP_N", 10)

TRADE_HEADERS_V1 = ["Date", "Ticker", "Type", "Price", "Shares", "Total", "Note"]
TRADE_HEADERS_V2 = [
    "TradeDateTime", "CreatedAt", "Ticker", "Type", "Price", "Shares",
    "GrossTotal", "Fee", "Slippage", "NetTotal", "Note", "OrderID"
]
TRADE_HEADERS_LEGACY6 = ["TradeDateTime", "Ticker", "Type", "Price", "Shares", "Total"]


# ===============================
# Retry
# ===============================
def gsheet_retry(func, max_retries: int = 6, base_sleep: float = 1.5):
    last_error = None
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            last_error = e
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg or "Read requests" in msg:
                time.sleep(base_sleep * (2 ** i))
                continue
            raise
    raise last_error


# ── yfinance 抓取強化（§8 資料層）：退避重試 + 失敗登記 ─────────────────────────
# 過去 yfinance 失敗一律靜默回 None：被限流/瞬斷的標的會無聲消失在掃描結果中，
# 且完全無從得知。現在統一走 yf_retry：指數退避重試，最終失敗登記於 fetch_failures
# 供掃描器/健檢回報；成功後自動移除登記。
YF_MAX_RETRIES = get_env_int("YF_MAX_RETRIES", 3)
YF_RETRY_BASE_SLEEP = get_env_float("YF_RETRY_BASE_SLEEP", 1.0)

_fetch_failures: Dict[str, str] = {}
_fetch_failures_lock = threading.Lock()


def _record_fetch_failure(symbol: str, reason) -> None:
    with _fetch_failures_lock:
        _fetch_failures[normalize_ticker(symbol)] = str(reason)[:200]


def _clear_fetch_failure(symbol: str) -> None:
    with _fetch_failures_lock:
        _fetch_failures.pop(normalize_ticker(symbol), None)


def get_fetch_failures() -> Dict[str, str]:
    """最近抓取失敗的標的與原因（重試耗盡才登記；之後成功會自動移除）。"""
    with _fetch_failures_lock:
        return dict(_fetch_failures)


def yf_retry(fn, symbol: str, retries: Optional[int] = None, base_sleep: Optional[float] = None):
    """對 yfinance 呼叫做指數退避重試；全數失敗回 None 並登記 fetch_failures。"""
    retries = YF_MAX_RETRIES if retries is None else retries
    base_sleep = YF_RETRY_BASE_SLEEP if base_sleep is None else base_sleep
    last = None
    for i in range(max(1, retries)):
        try:
            out = fn()
            _clear_fetch_failure(symbol)
            return out
        except Exception as e:
            last = e
            if i < retries - 1:
                time.sleep(base_sleep * (2 ** i))
    _record_fetch_failure(symbol, f"{type(last).__name__}: {last}")
    return None


# ===============================
# Utility
# ===============================
def normalize_ticker(symbol: str) -> str:
    return str(symbol).upper().strip().replace(".", "-")


def color_pl(val):
    color = "#26A69A" if val > 0 else "#EF5350" if val < 0 else "white"
    return f"color: {color}; font-weight: 600;"


def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def normalize_trade_type(x: str) -> str:
    s = str(x).strip()
    su = s.upper()
    if "買" in s or "BUY" in su:
        return "BUY"
    if "賣" in s or "SELL" in su:
        return "SELL"
    return su


def display_divergence(div: str) -> str:
    div = str(div).strip().upper()
    if div == "BULLISH":
        return "多方量價背離"
    if div == "BEARISH":
        return "空方量價背離"
    return "無明顯量價背離"


def display_market_session(session: str) -> str:
    mapping = {
        "PREMARKET": "盤前",
        "REGULAR": "正常盤",
        "AFTERMARKET": "盤後",
        "CLOSED": "休市",
    }
    return mapping.get(str(session).upper(), session)


def display_market_regime(regime: str) -> str:
    mapping = {
        "RISK_ON": "偏多",
        "RISK_OFF": "偏空",
        "NEUTRAL": "中性",
        "UNKNOWN": "未知",
    }
    return mapping.get(str(regime).upper(), regime)


def calc_target_zone_hit(current_price: float, target_price: Optional[float], tol_pct: float = PRE_ALERT_PCT) -> bool:
    if not target_price or target_price <= 0:
        return False
    return abs(current_price - target_price) / target_price <= tol_pct


def get_market_session() -> str:
    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)

    if now_et.weekday() >= 5:
        return "CLOSED"

    t = now_et.time()
    if datetime.strptime("04:00", "%H:%M").time() <= t < datetime.strptime("09:30", "%H:%M").time():
        return "PREMARKET"
    if datetime.strptime("09:30", "%H:%M").time() <= t < datetime.strptime("16:00", "%H:%M").time():
        return "REGULAR"
    if datetime.strptime("16:00", "%H:%M").time() <= t < datetime.strptime("20:00", "%H:%M").time():
        return "AFTERMARKET"
    return "CLOSED"


def send_telegram_msg(message: str) -> bool:
    if not TG_TOKEN or not TG_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        res = requests.post(
            url,
            json={"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
        if bool(res.json().get("ok")):
            return True
        # Markdown 解析失敗（標的含 _ / - 等特殊字元時常見）→ 退回純文字重送，避免整則漏發
        res = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": message}, timeout=10)
        return bool(res.json().get("ok"))
    except Exception:
        return False


def get_recent_trade_status(ticker: str, trades_df: pd.DataFrame) -> Tuple[bool, bool]:
    if trades_df.empty:
        return False, False
    cutoff_date = datetime.now().date() - timedelta(days=COOLDOWN_DAYS)
    temp_df = trades_df.copy()
    temp_df["TradeDateTime"] = pd.to_datetime(temp_df["TradeDateTime"], errors="coerce")
    temp_df["TradeDate"] = temp_df["TradeDateTime"].dt.date
    recent = temp_df[
        (temp_df["Ticker"] == normalize_ticker(ticker)) &
        (temp_df["TradeDate"] >= cutoff_date)
    ]
    recent_buy = not recent[recent["Type"] == "BUY"].empty
    recent_sell = not recent[recent["Type"] == "SELL"].empty
    return recent_buy, recent_sell


def _is_trade_type_value(x: str) -> bool:
    s = str(x).strip()
    su = s.upper()
    return ("買" in s) or ("賣" in s) or ("BUY" in su) or ("SELL" in su)


def _is_number_value(x) -> bool:
    try:
        float(x)
        return True
    except Exception:
        return False


# ===============================
# Cached market helpers
# ===============================
@lru_cache(maxsize=512)
def get_sp500_tickers() -> List[str]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        df = pd.read_html(response.text)[0]
        df["Display"] = df["Symbol"].astype(str).str.replace(".", "-", regex=False) + " - " + df["Security"].astype(str)
        return sorted(df["Display"].tolist())
    except Exception:
        return ["AAPL - Apple", "MSFT - Microsoft", "NVDA - NVIDIA", "AMZN - Amazon", "TSLA - Tesla"]


@ttl_cache(MARKET_CACHE_TTL, maxsize=1024)
def get_last_price(symbol: str) -> Optional[float]:
    symbol = normalize_ticker(symbol)
    hist = yf_retry(lambda: yf.Ticker(symbol).history(period="5d", auto_adjust=True), symbol)
    if hist is not None and not hist.empty:
        return float(hist["Close"].iloc[-1])
    return None


@lru_cache(maxsize=512)
def get_next_earnings_date(symbol: str) -> Optional[pd.Timestamp]:
    try:
        cal = yf_retry(lambda: yf.Ticker(normalize_ticker(symbol)).calendar, symbol, retries=2)
        if cal is None:
            return None
        if isinstance(cal, pd.DataFrame):
            for val in cal.to_numpy().flatten().tolist():
                dt = pd.to_datetime(val, errors="coerce")
                if pd.notna(dt):
                    return dt
        if isinstance(cal, dict):
            for _, val in cal.items():
                if isinstance(val, (list, tuple)):
                    for x in val:
                        dt = pd.to_datetime(x, errors="coerce")
                        if pd.notna(dt):
                            return dt
                else:
                    dt = pd.to_datetime(val, errors="coerce")
                    if pd.notna(dt):
                        return dt
        dt = pd.to_datetime(cal, errors="coerce")
        if pd.notna(dt):
            return dt
    except Exception:
        pass
    return None


@lru_cache(maxsize=1024)
def get_symbol_market_cap(symbol: str) -> float:
    """僅取市值（fast_info，快）。§8：tk.info 極慢且易觸發限流，掃描熱路徑
    （classify_symbol_bucket ← rank_symbol_strength ← 每檔每次掃描）一律走本函式。"""
    symbol = normalize_ticker(symbol)

    def _fetch():
        fi = yf.Ticker(symbol).fast_info
        try:
            mc = fi["marketCap"]           # 舊版 fast_info：dict-like
        except Exception:
            mc = getattr(fi, "market_cap", None)   # 新版：屬性
        return safe_float(mc or 0.0)

    out = yf_retry(_fetch, symbol, retries=2)
    return out if out is not None else 0.0


@lru_cache(maxsize=512)
def get_symbol_profile(symbol: str) -> Dict:
    """完整檔案（sector/industry 需走慢速 tk.info）。僅供持倉顯示與產業分類
    fallback 用；掃描熱路徑取市值請用 get_symbol_market_cap。"""
    symbol = normalize_ticker(symbol)

    def _fetch():
        long_info = yf.Ticker(symbol).info or {}
        return {
            "market_cap": safe_float(long_info.get("marketCap") or 0.0) or get_symbol_market_cap(symbol),
            "sector": str(long_info.get("sector") or "").strip(),
            "industry": str(long_info.get("industry") or "").strip(),
        }

    out = yf_retry(_fetch, symbol, retries=2)
    return out if out is not None else {"market_cap": get_symbol_market_cap(symbol), "sector": "", "industry": ""}


def classify_symbol_bucket(symbol: str, hist: Optional[pd.DataFrame] = None) -> str:
    market_cap = get_symbol_market_cap(symbol)   # §8：fast_info，不走慢速 tk.info
    last_price = None
    dv20 = 0.0

    if hist is not None and not hist.empty:
        last_price = safe_float(hist.iloc[-1]["Close"])
        dv20 = safe_float(hist.iloc[-1].get("DollarVolume20", 0.0))
    else:
        last_price = get_last_price(symbol)

    if market_cap >= 10_000_000_000:
        return "LARGE_CAP"
    if market_cap > 0 and market_cap < 2_000_000_000:
        return "SMALL_CAP"
    if safe_float(last_price) < 20 or dv20 < 50_000_000:
        return "SMALL_CAP"
    return "LARGE_CAP"


def is_earnings_blocked(symbol: str) -> bool:
    next_dt = get_next_earnings_date(symbol)
    if next_dt is None or pd.isna(next_dt):
        return False
    try:
        next_date = pd.to_datetime(next_dt, errors="coerce")
        if pd.isna(next_date):
            return False
        days = (next_date.date() - datetime.now().date()).days
        # 只擋「即將到來」的財報（今天起算 EARNINGS_BLOCK_DAYS 日內）。
        # 過去用 abs(days) 會把 yfinance 偶爾回傳的「上一次」財報日當成即將財報而誤擋進場。
        return 0 <= days <= EARNINGS_BLOCK_DAYS
    except Exception:
        return False


def _wilder(s: pd.Series, period: int) -> pd.Series:
    """Wilder 平滑 == alpha=1/period 的 EMA，用於 ATR / ADX / RSI 與標準刻度一致。"""
    return s.ewm(alpha=1.0 / period, adjust=False).mean()


@ttl_cache(MARKET_CACHE_TTL, maxsize=8)
def _get_benchmark_close(symbol: str = "SPY", period: str = "2y") -> Optional[pd.Series]:
    """取得對標收盤序列並快取，避免在每檔 get_unified_analysis 內重複下載 SPY。"""
    sym = normalize_ticker(symbol)
    h = yf_retry(lambda: yf.Ticker(sym).history(period=period, auto_adjust=True), sym)
    if h is not None and not h.empty:
        return h["Close"]
    return None


@ttl_cache(MARKET_CACHE_TTL, maxsize=1024)
def get_unified_analysis(symbol: str) -> Optional[pd.DataFrame]:
    symbol = normalize_ticker(symbol)
    df = yf_retry(lambda: yf.Ticker(symbol).history(period="2y", auto_adjust=True), symbol)
    if df is None:
        return None                          # 重試耗盡，原因已登記 fetch_failures
    if df.empty:
        _record_fetch_failure(symbol, "empty history（下市/更名/錯誤代碼？）")
        return None
    try:
        df = df.copy()
        df["SMA20"] = df["Close"].rolling(20).mean()
        df["SMA50"] = df["Close"].rolling(50).mean()
        df["SMA200"] = df["Close"].rolling(200).mean()

        std20 = df["Close"].rolling(20).std()
        df["BB_upper"] = df["SMA20"] + 2 * std20
        df["BB_lower"] = df["SMA20"] - 2 * std20
        df["BB_Width"] = (df["BB_upper"] - df["BB_lower"]) / (df["SMA20"] + 1e-9)

        hl = df["High"] - df["Low"]
        hc = (df["High"] - df["Close"].shift(1)).abs()
        lc = (df["Low"] - df["Close"].shift(1)).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        df["TR"] = tr
        df["ATR"] = _wilder(tr, 14)

        up_move = df["High"].diff()
        down_move = -df["Low"].diff()
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        atr14 = _wilder(tr, 14)
        plus_di = 100 * (_wilder(plus_dm, 14) / (atr14 + 1e-9))
        minus_di = 100 * (_wilder(minus_dm, 14) / (atr14 + 1e-9))
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di + 1e-9)) * 100
        df["ADX"] = _wilder(dx, 14)
        df["PLUS_DI"] = plus_di
        df["MINUS_DI"] = minus_di

        delta = df["Close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        rs = _wilder(gain, 14) / (_wilder(loss, 14) + 1e-9)
        df["RSI"] = 100 - (100 / (1 + rs))

        ema12 = df["Close"].ewm(span=12, adjust=False).mean()
        ema26 = df["Close"].ewm(span=26, adjust=False).mean()
        df["MACD"] = ema12 - ema26
        df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
        df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]

        df["VOL_SMA20"] = df["Volume"].rolling(20).mean()
        df["RollingHigh20"] = df["High"].rolling(20).max()
        df["RollingHigh55"] = df["High"].rolling(55).max()
        df["RollingLow20"] = df["Low"].rolling(20).min()
        df["RollingLow50"] = df["Low"].rolling(50).min()
        df["RollingHigh252"] = df["High"].rolling(252).max()
        df["TrailingStop"] = df["RollingHigh20"] - 3 * df["ATR"]
        df["DollarVolume"] = df["Close"] * df["Volume"]
        df["DollarVolume20"] = df["DollarVolume"].rolling(20).mean()

        # OBV 向量化（§8）：與逐日迴圈等價 — 首日 0；漲加量、跌減量、平不變
        _obv_dir = np.sign(df["Close"].diff()).fillna(0.0)
        df["OBV"] = (_obv_dir * df["Volume"]).cumsum()
        df["OBV_SMA20"] = df["OBV"].rolling(20).mean()
        df["OBV_Slope20"] = df["OBV"].diff(20)

        spy_close_raw = _get_benchmark_close("SPY", "2y")
        if spy_close_raw is not None and not spy_close_raw.empty:
            spy_close = spy_close_raw.reindex(df.index).ffill()
            df["RS_Line_SPY"] = df["Close"] / (spy_close + 1e-9)
            df["Ret20"] = df["Close"].pct_change(RS_LOOKBACK_DAYS)
            df["SPY_Ret20"] = spy_close.pct_change(RS_LOOKBACK_DAYS)
            df["RS20_vs_SPY"] = (df["Ret20"] - df["SPY_Ret20"]) * 100
            df["RS_Line_Slope20"] = df["RS_Line_SPY"].pct_change(RS_LOOKBACK_DAYS) * 100
        else:
            df["RS_Line_SPY"] = pd.NA
            df["Ret20"] = pd.NA
            df["SPY_Ret20"] = pd.NA
            df["RS20_vs_SPY"] = pd.NA
            df["RS_Line_Slope20"] = pd.NA

        return df.dropna().copy()
    except Exception as e:
        _record_fetch_failure(symbol, f"指標計算失敗 {type(e).__name__}: {e}")
        return None


def clear_market_cache():
    get_last_price.cache_clear()
    get_unified_analysis.cache_clear()
    _get_benchmark_close.cache_clear()
    get_next_earnings_date.cache_clear()
    get_symbol_profile.cache_clear()
    get_symbol_market_cap.cache_clear()
    _get_splits.cache_clear()


# ===============================
# Google Sheets
# ===============================
def get_gsheet_client():
    raw = get_env_str("GCP_SERVICE_ACCOUNT", "")
    if raw:
        return gspread.service_account_from_dict(json.loads(raw))
    try:
        import streamlit as st_local
        if "gcp_service_account" in st_local.secrets:
            return gspread.service_account_from_dict(dict(st_local.secrets["gcp_service_account"]))
        if "GCP_SERVICE_ACCOUNT" in st_local.secrets:
            secret_val = st_local.secrets["GCP_SERVICE_ACCOUNT"]
            if isinstance(secret_val, dict):
                return gspread.service_account_from_dict(dict(secret_val))
            raw = str(secret_val).strip()
            if raw:
                return gspread.service_account_from_dict(json.loads(raw))
    except Exception:
        pass
    raise ValueError("GCP_SERVICE_ACCOUNT 未設定")


def get_or_create_worksheet(spreadsheet, title: str, rows: int = 1000, cols: int = 12):
    try:
        return spreadsheet.worksheet(title)
    except Exception:
        return spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def get_spreadsheet():
    return get_gsheet_client().open(PORTFOLIO_SHEET_TITLE)


def ensure_headers(ws, headers: List[str]):
    try:
        first_row = gsheet_retry(lambda: ws.row_values(1))
        if not first_row:
            gsheet_retry(lambda: ws.append_row(headers))
    except Exception:
        gsheet_retry(lambda: ws.append_row(headers))


def ensure_trades_headers_v2(ws):
    try:
        first_row = gsheet_retry(lambda: ws.row_values(1))
        if not first_row:
            gsheet_retry(lambda: ws.append_row(TRADE_HEADERS_V2))
        elif [str(c).strip() for c in first_row[:len(TRADE_HEADERS_V1)]] == TRADE_HEADERS_V1 \
                and [str(c).strip() for c in first_row[:len(TRADE_HEADERS_V2)]] != TRADE_HEADERS_V2:
            gsheet_retry(lambda: ws.update("A1:L1", [TRADE_HEADERS_V2]))
    except Exception:
        pass


def get_trades_worksheet(readonly: bool = True):
    ws = get_or_create_worksheet(get_spreadsheet(), "Trades", rows=10000, cols=14)
    if not readonly:
        ensure_trades_headers_v2(ws)
    return ws


def get_history_worksheet(readonly: bool = True):
    ws = get_or_create_worksheet(get_spreadsheet(), "History", rows=8000, cols=12)
    if not readonly:
        ensure_headers(ws, [
            "Date", "TotalAssets", "Cash", "MarketValue", "RealizedPL", "UnrealizedPL",
            "TotalPL", "DailyReturnPct", "DrawdownPct", "BenchmarkSPY", "BenchmarkReturnPct"
        ])
    return ws


def get_alerts_worksheet(readonly: bool = True):
    ws = get_or_create_worksheet(get_spreadsheet(), "Alerts", rows=12000, cols=12)
    if not readonly:
        ensure_headers(ws, ["DateTime", "Ticker", "Action", "BaseKey", "Price", "Score", "Session", "TargetPrice", "Message", "Fingerprint"])
    return ws


def get_watchlist_worksheet(readonly: bool = True):
    ws = get_or_create_worksheet(get_spreadsheet(), "Watchlist", rows=2000, cols=4)
    if not readonly:
        ensure_headers(ws, ["Ticker", "Enabled", "Category", "Note"])
    return ws


def get_signals_worksheet(readonly: bool = True):
    ws = get_or_create_worksheet(get_spreadsheet(), "Signals", rows=20000, cols=21)
    if not readonly:
        ensure_headers(ws, [
            "DateTime", "Ticker", "Action", "StrategyMode", "Score", "Close",
            "TargetBuyPrice", "TargetSellPrice", "StopLoss", "TrendStop",
            "TakeProfit1", "TakeProfit2", "RS20vsSPY", "ADX", "Regime", "Bucket",
            "SignalState", "Reason", "Fingerprint", "Session", "Source"
        ])
    return ws


def read_worksheet_as_df(ws, expected_headers: List[str]) -> pd.DataFrame:
    try:
        values = gsheet_retry(lambda: ws.get_all_values())
        if not values:
            return pd.DataFrame(columns=expected_headers)
        headers = values[0]
        rows = values[1:] if len(values) > 1 else []
        if not headers:
            return pd.DataFrame(columns=expected_headers)
        clean = []
        for row in rows:
            row = row[:len(expected_headers)] + [""] * max(0, len(expected_headers) - len(row))
            clean.append(row[:len(expected_headers)])
        return pd.DataFrame(clean, columns=expected_headers)
    except Exception:
        return pd.DataFrame(columns=expected_headers)


def clear_app_caches():
    clear_market_cache()
    for fn_name in ["load_watchlist", "load_trades", "load_alerts", "load_history", "load_signals"]:
        try:
            fn = globals().get(fn_name)
            if fn and hasattr(fn, "clear"):
                fn.clear()
        except Exception:
            pass


# ===============================
# Trades / Watchlist / History
# ===============================
def delete_watchlist_ticker(ticker: str) -> Tuple[bool, str]:
    ticker = normalize_ticker(ticker)
    if not ticker:
        return False, "Ticker 不可為空"
    try:
        ws = get_watchlist_worksheet(readonly=True)
        values = gsheet_retry(lambda: ws.get_all_values())
        if not values or len(values) <= 1:
            return False, "Watchlist 目前為空"
        headers, rows = values[0], values[1:]
        ticker_idx = next((i for i, h in enumerate(headers) if str(h).strip().upper() == "TICKER"), None)
        if ticker_idx is None:
            return False, "缺少 Ticker 欄位"
        target_row_number = next((idx for idx, row in enumerate(rows, start=2) if (row[ticker_idx] if ticker_idx < len(row) else "") == ticker), None)
        if target_row_number is None:
            return False, f"{ticker} 不在 Watchlist 中"
        gsheet_retry(lambda: ws.delete_rows(target_row_number))
        clear_app_caches()
        return True, f"已刪除 Watchlist：{ticker}"
    except Exception as e:
        return False, f"刪除失敗：{e}"


def set_watchlist_enabled(ticker: str, enabled: bool) -> Tuple[bool, str]:
    ticker = normalize_ticker(ticker)
    if not ticker:
        return False, "Ticker 不可為空"
    try:
        ws = get_watchlist_worksheet(readonly=True)
        values = gsheet_retry(lambda: ws.get_all_values())
        if not values or len(values) <= 1:
            return False, "Watchlist 目前為空"
        headers, rows = values[0], values[1:]
        ticker_idx = next((i for i, h in enumerate(headers) if str(h).strip().upper() == "TICKER"), None)
        enabled_idx = next((i for i, h in enumerate(headers) if str(h).strip().upper() == "ENABLED"), None)
        if ticker_idx is None or enabled_idx is None:
            return False, "Watchlist 缺少必要欄位"
        for idx, row in enumerate(rows, start=2):
            if (row[ticker_idx] if ticker_idx < len(row) else "") == ticker:
                gsheet_retry(lambda: ws.update_cell(idx, enabled_idx + 1, str(enabled)))
                clear_app_caches()
                return True, f"{ticker} 已{'啟用' if enabled else '停用'}"
        return False, f"{ticker} 不在 Watchlist 中"
    except Exception as e:
        return False, f"更新失敗：{e}"


def _normalize_trade_row_to_v2(row: List[str]) -> Optional[Dict]:
    row = [str(c).strip() for c in list(row)]
    if not any(row):
        return None

    # 去掉尾端空白，保留中間空格
    while row and row[-1] == "":
        row.pop()

    # ── Case 0: 舊資料整列右移 3 格 ─────────────────────────────
    # 例如:
    # ["", "", "", "2025-11-21", "NVDA", "買入 (Buy)", "180.8", "13", "2350.4"]
    if len(row) >= 9 and row[0] == "" and row[1] == "" and row[2] == "":
        shifted = row[3:]
        # 若右移後是 header，直接跳過
        if len(shifted) >= 6 and shifted[:6] == ["TradeDateTime", "CreatedAt", "Ticker", "Type", "Price", "Shares"]:
            return None
        # 若右移後像 legacy row，就用 shifted 再解析
        row = shifted

    # ── Case 0.5: 任何位置出現殘留 header row，一律跳過 ──────────
    header_join = "|".join(row).upper()
    if "TRADEDATETIME" in header_join and "TICKER" in header_join and "TYPE" in header_join:
        # 避免把 header 當資料
        dt_try = pd.to_datetime(row[0], errors="coerce") if row else pd.NaT
        if pd.isna(dt_try):
            return None

    # ── Case 1: 標準 V2 ─────────────────────────────────────────
    if len(row) >= 10:
        c1 = row[1] if len(row) > 1 else ""
        c2 = row[2] if len(row) > 2 else ""
        c3 = row[3] if len(row) > 3 else ""
        c4 = row[4] if len(row) > 4 else ""
        c1_dt = pd.to_datetime(c1, errors="coerce")
        if pd.notna(c1_dt) and not _is_number_value(c2) and _is_trade_type_value(c3) and _is_number_value(c4):
            row12 = row[:12] + [""] * max(0, 12 - len(row))
            return dict(zip(TRADE_HEADERS_V2, row12))

    # ── Case 2: Legacy-6 / Legacy-7 ────────────────────────────
    if len(row) >= 6:
        c0, c1, c2, c3, c4, c5 = row[:6]
        dt0 = pd.to_datetime(c0, errors="coerce")
        if pd.notna(dt0) and not _is_number_value(c1) and _is_trade_type_value(c2) and _is_number_value(c3) and _is_number_value(c4):
            raw_dt = c0 if ":" in c0 else f"{c0} 00:00:00"
            return {
                "TradeDateTime": raw_dt,
                "CreatedAt": raw_dt,
                "Ticker": c1,
                "Type": c2,
                "Price": c3,
                "Shares": c4,
                "GrossTotal": c5,
                "Fee": 0.0,
                "Slippage": 0.0,
                "NetTotal": c5,
                "Note": row[6] if len(row) > 6 else "",
                "OrderID": "",
            }

    # ── Case 3: 舊 V1 fallback ─────────────────────────────────
    if len(row) >= 5:
        row7 = row[:7] + [""] * max(0, 7 - len(row))
        raw_dt = row7[0] if ":" in row7[0] else f"{row7[0]} 00:00:00"
        dt0 = pd.to_datetime(raw_dt, errors="coerce")
        if pd.notna(dt0):
            return {
                "TradeDateTime": raw_dt,
                "CreatedAt": raw_dt,
                "Ticker": row7[1],
                "Type": row7[2],
                "Price": row7[3],
                "Shares": row7[4],
                "GrossTotal": row7[5],
                "Fee": 0.0,
                "Slippage": 0.0,
                "NetTotal": row7[5],
                "Note": row7[6],
                "OrderID": "",
            }

    return None


def _load_trades_raw() -> pd.DataFrame:
    ws = get_trades_worksheet(readonly=True)
    values = gsheet_retry(lambda: ws.get_all_values())
    if not values:
        return pd.DataFrame(columns=TRADE_HEADERS_V2)

    first = [str(x).strip() for x in values[0]]
    is_header_row = (
        first[:len(TRADE_HEADERS_V1)] == TRADE_HEADERS_V1 or
        first[:len(TRADE_HEADERS_V2)] == TRADE_HEADERS_V2 or
        first[:len(TRADE_HEADERS_LEGACY6)] == TRADE_HEADERS_LEGACY6
    )
    rows = values[1:] if is_header_row else values

    normalized_rows = []
    for row in rows:
        item = _normalize_trade_row_to_v2(row)
        if item:
            normalized_rows.append(item)

    df = pd.DataFrame(normalized_rows, columns=TRADE_HEADERS_V2)
    if df.empty:
        return df

    df["Ticker"] = df["Ticker"].astype(str).apply(normalize_ticker)
    df["Type"] = df["Type"].astype(str).apply(normalize_trade_type)

    for col in ["Price", "Shares", "GrossTotal", "Fee", "Slippage", "NetTotal"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["TradeDateTime"] = pd.to_datetime(df["TradeDateTime"], errors="coerce")
    df = df.dropna(subset=["TradeDateTime"])
    df = df[df["Ticker"].str.strip().ne("") & df["Ticker"].str.upper().ne("TICKER")]
    return df.sort_values("TradeDateTime").reset_index(drop=True)


if st:
    @st.cache_data(ttl=120, show_spinner=False)
    def load_trades() -> pd.DataFrame:
        return _load_trades_raw()
else:
    def load_trades() -> pd.DataFrame:
        return _load_trades_raw()


def _load_watchlist_raw() -> pd.DataFrame:
    df = read_worksheet_as_df(get_watchlist_worksheet(readonly=True), ["Ticker", "Enabled", "Category", "Note"])
    if df.empty:
        return df
    df["Ticker"] = df["Ticker"].astype(str).apply(normalize_ticker)
    df["Enabled"] = df["Enabled"].astype(str).str.upper().isin(["TRUE", "1", "YES", "Y", "ON"])
    return df[df["Ticker"] != ""].drop_duplicates(subset=["Ticker"], keep="last").reset_index(drop=True)


if st:
    @st.cache_data(ttl=120, show_spinner=False)
    def load_watchlist() -> pd.DataFrame:
        return _load_watchlist_raw()
else:
    def load_watchlist() -> pd.DataFrame:
        return _load_watchlist_raw()


def save_watchlist(ticker: str, enabled: bool = True, category: str = "General", note: str = "") -> Tuple[bool, str]:
    ticker = normalize_ticker(ticker)
    if not ticker:
        return False, "Ticker 不可為空"
    try:
        ws = get_watchlist_worksheet(readonly=False)
        gsheet_retry(lambda: ws.append_row([ticker, str(enabled), category, note]))
        clear_app_caches()
        return True, "已加入 Watchlist"
    except Exception as e:
        return False, f"失敗：{e}"


def _load_alerts_raw() -> pd.DataFrame:
    df = read_worksheet_as_df(get_alerts_worksheet(readonly=True), ["DateTime", "Ticker", "Action", "BaseKey", "Price", "Score", "Session", "TargetPrice", "Message", "Fingerprint"])
    if df.empty:
        return df
    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    df["Action"] = df["Action"].astype(str).str.upper().str.strip()
    for col in ["Price", "Score", "TargetPrice"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    return df.sort_values("DateTime").reset_index(drop=True)


if st:
    @st.cache_data(ttl=120, show_spinner=False)
    def load_alerts() -> pd.DataFrame:
        return _load_alerts_raw()
else:
    def load_alerts() -> pd.DataFrame:
        return _load_alerts_raw()


def _load_history_raw() -> pd.DataFrame:
    df = read_worksheet_as_df(get_history_worksheet(readonly=True), [
        "Date", "TotalAssets", "Cash", "MarketValue", "RealizedPL", "UnrealizedPL",
        "TotalPL", "DailyReturnPct", "DrawdownPct", "BenchmarkSPY", "BenchmarkReturnPct"
    ])
    if df.empty:
        return df
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)


if st:
    @st.cache_data(ttl=120, show_spinner=False)
    def load_history() -> pd.DataFrame:
        return _load_history_raw()
else:
    def load_history() -> pd.DataFrame:
        return _load_history_raw()


def _load_signals_raw() -> pd.DataFrame:
    cols = [
        "DateTime", "Ticker", "Action", "StrategyMode", "Score", "Close",
        "TargetBuyPrice", "TargetSellPrice", "StopLoss", "TrendStop", "TakeProfit1",
        "TakeProfit2", "RS20vsSPY", "ADX", "Regime", "Bucket", "SignalState",
        "Reason", "Fingerprint", "Session", "Source"
    ]
    df = read_worksheet_as_df(get_signals_worksheet(readonly=True), cols)
    if df.empty:
        return df
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    for col in ["Score", "Close", "TargetBuyPrice", "TargetSellPrice", "StopLoss", "TrendStop", "TakeProfit1", "TakeProfit2", "RS20vsSPY", "ADX"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # 舊資料無 Source 欄（讀入為空字串）→ 標記 LEGACY，與新引擎訊號區分
    df["Source"] = df["Source"].replace("", "LEGACY").fillna("LEGACY")
    return df.sort_values("DateTime").reset_index(drop=True)


if st:
    @st.cache_data(ttl=120, show_spinner=False)
    def load_signals() -> pd.DataFrame:
        return _load_signals_raw()
else:
    def load_signals() -> pd.DataFrame:
        return _load_signals_raw()


def save_trade(trade_dt, ticker, trade_type, price, shares, note="", fee=DEFAULT_COMMISSION, slippage=None, order_id="") -> Tuple[bool, str]:
    from datetime import date as _date

    if isinstance(trade_dt, _date) and not isinstance(trade_dt, datetime):
        trade_dt = datetime(trade_dt.year, trade_dt.month, trade_dt.day)

    ticker, trade_type = normalize_ticker(ticker), normalize_trade_type(trade_type)
    if not ticker or trade_type not in ["BUY", "SELL"] or price <= 0 or shares <= 0:
        return False, "輸入無效"

    # 賣超檢查用「即時」交易資料（繞過 ttl=120 快取），避免短時間連續下單因快取未刷新而賣超
    holding_shares = get_current_holding_shares(_load_trades_raw(), ticker)
    if trade_type == "SELL" and shares > holding_shares + 1e-9:
        return False, f"賣出超過持股 {holding_shares:.4f}"

    ws = get_trades_worksheet(readonly=False)
    gross_total = round(price * shares, 4)
    slippage = round(float(slippage) if slippage is not None else gross_total * DEFAULT_SLIPPAGE_PCT, 4)
    fee = round(float(fee), 4)
    net_total = round(
        gross_total + fee + slippage if trade_type == "BUY" else gross_total - fee - slippage,
        4
    )

    gsheet_retry(lambda: ws.append_row([
        trade_dt.strftime("%Y-%m-%d %H:%M:%S"),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ticker,
        trade_type,
        float(price),
        float(shares),
        float(gross_total),
        float(fee),
        float(slippage),
        float(net_total),
        note,
        order_id
    ]))
    clear_app_caches()
    return True, "交易寫入成功"


def maybe_log_daily_history(total_assets, cash, market_value, realized_pl, unrealized_pl) -> Tuple[bool, str]:
    try:
        ws = get_history_worksheet(readonly=False)
        hist_df = load_history()
        today_str = datetime.now().strftime("%Y-%m-%d")
        if not hist_df.empty and hist_df["Date"].dt.strftime("%Y-%m-%d").iloc[-1] == today_str:
            return False, "今日已記錄"

        daily_ret = drawdown = benchmark_ret = None
        spy_last = get_last_price("SPY")
        if not hist_df.empty:
            prev_a = safe_float(hist_df["TotalAssets"].iloc[-1])
            if prev_a > 0:
                daily_ret = (total_assets / prev_a - 1) * 100
            nav_s = pd.concat([hist_df["TotalAssets"], pd.Series([total_assets])], ignore_index=True)
            drawdown = (nav_s.iloc[-1] / nav_s.cummax().iloc[-1] - 1) * 100 if nav_s.cummax().iloc[-1] > 0 else 0.0
            if spy_last and pd.notna(hist_df["BenchmarkSPY"].iloc[-1]) and hist_df["BenchmarkSPY"].iloc[-1] > 0:
                benchmark_ret = (spy_last / hist_df["BenchmarkSPY"].iloc[-1] - 1) * 100

        gsheet_retry(lambda: ws.append_row([
            today_str, float(total_assets), float(cash), float(market_value), float(realized_pl), float(unrealized_pl),
            float(total_assets - DEFAULT_INITIAL_CAPITAL),
            float(daily_ret) if daily_ret is not None else "",
            float(drawdown) if drawdown is not None else 0.0,
            float(spy_last) if spy_last else "",
            float(benchmark_ret) if benchmark_ret is not None else ""
        ]))
        clear_app_caches()
        return True, "已記錄 NAV"
    except Exception as e:
        return False, str(e)


# ===============================
# Alert / Signal dedup
# ===============================
def build_alert_fingerprint(ticker, action, session, price, score, target_price) -> str:
    tp = round(float(target_price), 2) if target_price is not None and not pd.isna(target_price) else 0.0
    return f"{normalize_ticker(ticker)}|{action}|{session}|{round(float(price), 2)}|{round(float(score), 1)}|{tp}"


def build_signal_state(action, strategy_mode) -> str:
    return f"{str(action).upper()}::{str(strategy_mode).upper()}"


def should_send_alert(alerts_df, ticker, action, current_price, current_score, current_session, target_price=None, state_changed=False) -> bool:
    fp = build_alert_fingerprint(ticker, action, current_session, current_price, current_score, target_price)
    if not alerts_df.empty and "Fingerprint" in alerts_df.columns and fp in alerts_df["Fingerprint"].astype(str).values:
        return False

    if alerts_df.empty:
        return True
    temp = alerts_df[(alerts_df["Ticker"] == ticker) & (alerts_df["Action"] == action)]
    if temp.empty:
        return True
    last = temp.sort_values("DateTime").iloc[-1]

    if state_changed or current_session != last["Session"]:
        return True
    mins = (datetime.now() - last["DateTime"].to_pydatetime()).total_seconds() / 60.0
    lp, ls, lt = safe_float(last["Price"]), safe_float(last["Score"]), safe_float(last["TargetPrice"])
    pc = abs((current_price - lp) / lp) * 100 if lp > 0 else 0
    sc = abs(current_score - ls)
    tc = abs((target_price - lt) / lt) * 100 if target_price and lt > 0 else 0

    return mins >= ALERT_MIN_MINUTES and (pc >= ALERT_MIN_PRICE_CHANGE or sc >= ALERT_MIN_SCORE_CHANGE or tc >= 1.0)


def log_sent_alert(ticker, action, price, score, session, target_price, message) -> bool:
    try:
        ws = get_alerts_worksheet(readonly=False)
        fp = build_alert_fingerprint(ticker, action, session, price, score, target_price)
        gsheet_retry(lambda: ws.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ticker,
            action,
            f"{ticker}_{action}",
            float(price),
            float(score),
            session,
            float(target_price) if target_price else "",
            message,
            fp
        ]))
        clear_app_caches()
        return True
    except Exception:
        return False


def log_signal_snapshot(ticker, action, strategy_mode, score, details, reason, session,
                        source: str = "PORTFOLIO") -> bool:
    try:
        ws = get_signals_worksheet(readonly=False)
        tp = details.get("target_buy_price") or details.get("target_sell_price")
        fp = build_alert_fingerprint(ticker, action, session, safe_float(details.get("close")), score, tp)
        gsheet_retry(lambda: ws.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ticker,
            action,
            strategy_mode,
            float(score),
            float(details.get("close", 0)),
            float(details.get("target_buy_price") or 0) or "",
            float(details.get("target_sell_price") or 0) or "",
            float(details.get("stop_loss") or 0) or "",
            float(details.get("trend_stop") or 0) or "",
            float(details.get("take_profit_1") or 0) or "",
            float(details.get("take_profit_2") or 0) or "",
            float(details.get("rs20_vs_spy") or 0) or "",
            float(details.get("adx") or 0) or "",
            str(details.get("market_regime", "")),
            str(details.get("bucket", "")),
            build_signal_state(action, strategy_mode),
            reason,
            fp,
            session,
            source,
        ]))
        clear_app_caches()
        return True
    except Exception:
        return False


def log_signal_snapshots(candidates: List[Dict], session: str) -> int:
    """
    批次寫入訊號快照到 Signals 表（單一 append_rows 呼叫，避免逐筆 API 與重複清快取）。
    candidates 為 run_auto_scanner 產生的 [{ticker, score, action, details, note}, ...]。
    回傳實際寫入列數。
    """
    if not candidates:
        return 0
    try:
        ws = get_signals_worksheet(readonly=False)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for c in candidates:
            tk = c["ticker"]
            act = c["action"]
            sc = c["score"]
            det = c.get("details", {}) or {}
            mode = det.get("strategy_mode", "")
            tp = det.get("target_buy_price") or det.get("target_sell_price")
            fp = build_alert_fingerprint(tk, act, session, safe_float(det.get("close")), sc, tp)
            rows.append([
                ts, tk, act, mode, float(sc),
                float(det.get("close", 0)),
                float(det.get("target_buy_price") or 0) or "",
                float(det.get("target_sell_price") or 0) or "",
                float(det.get("stop_loss") or 0) or "",
                float(det.get("trend_stop") or 0) or "",
                float(det.get("take_profit_1") or 0) or "",
                float(det.get("take_profit_2") or 0) or "",
                float(det.get("rs20_vs_spy") or 0) or "",
                float(det.get("adx") or 0) or "",
                str(det.get("market_regime", "")),
                str(det.get("bucket", "")),
                build_signal_state(act, mode),
                c.get("note", ""),
                fp, session, "PORTFOLIO",
            ])
        gsheet_retry(lambda: ws.append_rows(rows))
        clear_app_caches()
        return len(rows)
    except Exception:
        return 0


def log_semi_signal_snapshots(results: List[Dict], sox_regime: Dict,
                              min_signal_score: Optional[float] = None) -> int:
    """
    將半導體掃描的買進類訊號（含 'BUY' 的 STRONG_BUY / BUY）批次寫入 Signals 表，Source=SEMI。
    供訊號成效追蹤累積樣本。預設只記分數 >= US_SEMI_SCORE_BUY 者（WATCH 不記，避免灌爆表）。
    結果結構為 _us_semi_score_one 回傳的 dict。
    """
    if min_signal_score is None:
        min_signal_score = US_SEMI_SCORE_BUY
    rows_src = [r for r in (results or [])
                if "BUY" in str(r.get("signal", "")) and safe_float(r.get("score")) >= min_signal_score]
    if not rows_src:
        return 0
    try:
        ws = get_signals_worksheet(readonly=False)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        regime_lbl = f"SOX_{sox_regime.get('trend', 'NEUTRAL')}"
        rows = []
        for r in rows_src:
            tk = r["ticker"]
            sig = r["signal"]
            sc = safe_float(r.get("score"))
            close = safe_float(r.get("close"))
            fp = build_alert_fingerprint(tk, sig, "SEMI", close, sc, close)
            rows.append([
                ts, tk, sig, "US_SEMI", float(sc), float(close),
                float(close),                                   # TargetBuyPrice ≈ 訊號日收盤
                "",                                             # TargetSellPrice（n/a）
                float(r.get("stop_loss") or 0) or "",
                "",                                             # TrendStop（半導體引擎無）
                float(r.get("tp1") or 0) or "",
                float(r.get("tp2") or 0) or "",
                float(r.get("rs20_vs_spy") or 0) or "",
                float(r.get("adx") or 0) or "",
                regime_lbl,
                str(r.get("category", "")),
                build_signal_state(sig, "US_SEMI"),
                " | ".join(r.get("reasons", []) or []),
                fp, "SEMI", "SEMI",                             # Session, Source
            ])
        gsheet_retry(lambda: ws.append_rows(rows))
        clear_app_caches()
        return len(rows)
    except Exception:
        return 0


# ===============================
# Market regime
# ===============================
def _regime_from_indicator_rows(spy_last, qqq_last, vix_last) -> Dict:
    """由 SPY/QQQ/VIX「單根指標列」計算市場 regime。

    抽成獨立函式作為單一真相來源：即時路徑 get_market_regime() 傳 .iloc[-1]，
    回測 (backtest.py) 傳「截至該歷史日」的對應列，兩者評分邏輯保證一致、不漂移。
    spy_last / qqq_last / vix_last 為 pandas 列（Series）或 None。
    """
    if spy_last is None or qqq_last is None:
        # fail-closed（P4）：行情資料異常（SPY/QQQ 取價失敗，多半是限流/斷線）時，
        # 市場守門員不得放行——禁新倉/加碼，既有持倉的出場邏輯不受影響。
        # 過去此處 fail-open（allow=True），等於資料中斷日風控自動下線。
        return {
            "regime": "UNKNOWN",
            "score": 0,
            "allow_new_position": False,
            "allow_add_position": False,
            "risk_multiplier": 0.25,
            "vix": None,
        }

    score = 0
    if safe_float(spy_last["Close"]) > safe_float(spy_last["SMA200"]):
        score += 1
    if safe_float(spy_last["SMA50"]) > safe_float(spy_last["SMA200"]):
        score += 1
    if safe_float(qqq_last["Close"]) > safe_float(qqq_last["SMA200"]):
        score += 1
    if safe_float(spy_last["MACD_Hist"]) > 0:
        score += 1

    vix_ok, vix_level = True, None
    if vix_last is not None:
        vix_level = safe_float(vix_last["Close"])
        if vix_level >= 25:
            vix_ok = False
        elif vix_level < 20:
            score += 1

    if score >= 4 and vix_ok:
        return {"regime": "RISK_ON", "score": score, "allow_new_position": True,
                "allow_add_position": True, "risk_multiplier": 1.0, "vix": vix_level}
    if score <= 2:
        return {"regime": "RISK_OFF", "score": score, "allow_new_position": False,
                "allow_add_position": False, "risk_multiplier": 0.0, "vix": vix_level}
    return {"regime": "NEUTRAL", "score": score, "allow_new_position": False,
            "allow_add_position": True, "risk_multiplier": 0.5, "vix": vix_level}


def get_market_regime() -> Dict:
    spy = get_unified_analysis("SPY")
    qqq = get_unified_analysis("QQQ")
    vix = get_unified_analysis("^VIX")

    spy_last = spy.iloc[-1] if spy is not None and not spy.empty else None
    qqq_last = qqq.iloc[-1] if qqq is not None and not qqq.empty else None
    vix_last = vix.iloc[-1] if vix is not None and not vix.empty else None
    return _regime_from_indicator_rows(spy_last, qqq_last, vix_last)


# ===============================
# Enhanced symbol scoring
# ===============================
def rank_symbol_strength(ticker: str, hist: pd.DataFrame, market_regime: Optional[Dict] = None) -> Tuple[float, Dict]:
    if hist is None or hist.empty:
        return 0.0, {}

    last = hist.iloc[-1]
    prev = hist.iloc[-2] if len(hist) >= 2 else hist.iloc[-1]

    close = safe_float(last["Close"])
    sma20 = safe_float(last["SMA20"])
    sma50 = safe_float(last["SMA50"])
    sma200 = safe_float(last["SMA200"])
    volume = safe_float(last["Volume"])
    vol_sma20 = safe_float(last["VOL_SMA20"])
    macd_hist = safe_float(last["MACD_Hist"])
    rsi = safe_float(last["RSI"])
    adx = safe_float(last.get("ADX", 0))
    bb_width = safe_float(last.get("BB_Width", 1.0))

    reasons = []
    score = 0.0

    if close > sma200 and sma50 > sma200:
        score += 1.4
        reasons.append("長線多頭")
        if sma20 > sma50:
            score += 1.0
            reasons.append("均線多頭排列")

    is_squeeze = bb_width < 0.08
    if is_squeeze:
        reasons.append("📦 BB 壓縮蓄力")
    elif bb_width > 0.12:
        prev_high20 = safe_float(hist["RollingHigh20"].shift(1).iloc[-1])
        if close > prev_high20 and volume > vol_sma20 * 1.3:
            score += 1.5
            reasons.append("🚀 放量突破壓縮區")

    if macd_hist > 0:
        score += 0.8
    if 50 <= rsi <= 72:
        score += 0.8
    elif rsi > 72:
        score -= 0.3
    if adx >= BREAKOUT_ADX_MIN:
        score += 0.8
        reasons.append(f"ADX 趨勢強 ({adx:.0f})")

    rs20 = safe_float(last.get("RS20_vs_SPY", 0))
    if rs20 > RS_STRONG_THRESHOLD:
        score += 1.0
        reasons.append(f"強於大盤 +{rs20:.1f}%")
    elif rs20 < RS_WEAK_THRESHOLD:
        score -= 0.5
        reasons.append(f"弱於大盤 {rs20:.1f}%")

    obv_slope = safe_float(last.get("OBV_Slope20", 0))
    if obv_slope > 0 and close > sma20:
        score += 0.5
        reasons.append("OBV 機構吸籌")

    high_252 = safe_float(last.get("RollingHigh252", 0))
    if high_252 > 0 and close >= high_252 * (1 - NEAR_52W_HIGH_PCT):
        score += 0.8
        reasons.append("接近年高 (領導股)")

    prev_close = safe_float(prev["Close"])
    if volume > vol_sma20 * 1.5 and close > prev_close:
        score += 0.5
        reasons.append("大量上漲日")

    if market_regime:
        score *= safe_float(market_regime.get("risk_multiplier", 1.0), 1.0)

    return score, {
        "bucket": classify_symbol_bucket(ticker, hist),
        "liquid_ok": close >= MIN_PRICE and safe_float(last["DollarVolume20"]) >= MIN_AVG_DOLLAR_VOLUME,
        "earnings_blocked": is_earnings_blocked(ticker),
        "trend_ok": close > sma200 and sma50 > sma200,
        "strong_trend": close > sma200 and sma50 > sma200 and sma20 > sma50,
        "is_squeeze": is_squeeze,
        "reasons": reasons,
        "adx": adx,
        "rsi": rsi,
        "rs20_vs_spy": rs20,
        "obv_slope": obv_slope,
        "bb_width": bb_width,
    }


# ===============================
# Portfolio / Scanner
# ===============================
@ttl_cache(86400, maxsize=512)
def _get_splits(ticker: str) -> tuple:
    """回傳 ((split_date_naive_normalized, ratio), ...)；ratio 為分割倍數（10:1→10.0、1:10→0.1）。"""
    sym = normalize_ticker(ticker)
    s = yf_retry(lambda: yf.Ticker(sym).splits, sym, retries=2)
    try:
        if s is None or len(s) == 0:
            return ()
        out = []
        for d, r in s.items():
            if r and float(r) > 0:
                dd = pd.Timestamp(d)
                if dd.tzinfo is not None:
                    dd = dd.tz_localize(None)
                out.append((dd.normalize(), float(r)))
        return tuple(out)
    except Exception:
        return ()


def _split_factor_after(ticker: str, after_dt) -> float:
    """進場後（嚴格大於 after_dt）所有分割倍數的乘積；用來把名目交易對齊到還原權值價序列。"""
    splits = _get_splits(ticker)
    if not splits or after_dt is None:
        return 1.0
    try:
        ref = pd.Timestamp(after_dt)
        if ref.tzinfo is not None:
            ref = ref.tz_localize(None)
        ref = ref.normalize()
    except Exception:
        return 1.0
    factor = 1.0
    for d, r in splits:
        if d > ref:
            factor *= r
    return factor


def _split_adjust(trades_df: pd.DataFrame) -> pd.DataFrame:
    """
    把每筆交易依「該筆成交日之後發生的分割」對齊：Shares ×= factor、Price ÷= factor
    （金額不變）。使名目交易與 auto_adjust 的還原權值價序列一致，避免分割後出現
    假停損/損益錯誤/FIFO 賣超。預設啟用，AUTO_SPLIT_ADJUST=0 可關閉（若你已手動改股數）。
    """
    if not AUTO_SPLIT_ADJUST or trades_df is None or trades_df.empty:
        return trades_df
    if "Ticker" not in trades_df.columns or "TradeDateTime" not in trades_df.columns:
        return trades_df
    df = trades_df.copy()
    for tk in df["Ticker"].dropna().unique().tolist():
        if not _get_splits(tk):
            continue
        for idx in df.index[df["Ticker"] == tk]:
            factor = _split_factor_after(tk, df.at[idx, "TradeDateTime"])
            if factor != 1.0:
                df.at[idx, "Shares"] = safe_float(df.at[idx, "Shares"]) * factor
                df.at[idx, "Price"] = safe_float(df.at[idx, "Price"]) / factor
    return df


def get_current_holding_shares(trades_df: pd.DataFrame, ticker: str) -> float:
    if trades_df.empty:
        return 0.0
    trades_df = _split_adjust(trades_df)
    t = trades_df[trades_df["Ticker"] == normalize_ticker(ticker)]
    return max(
        0.0,
        t.loc[t["Type"] == "BUY", "Shares"].sum() - t.loc[t["Type"] == "SELL", "Shares"].sum()
    )


def build_portfolio(trades_df: pd.DataFrame, initial_capital: float) -> Tuple[List[Dict], float, float]:
    if trades_df.empty:
        return [], float(initial_capital), 0.0

    trades_df = _split_adjust(trades_df)   # 進場後分割對齊（A）：股數/成本與還原權值價一致
    cash = float(initial_capital)
    portfolio = []
    total_realized_pl = 0.0

    for ticker in sorted(trades_df["Ticker"].dropna().unique().tolist()):
        tdf = trades_df[trades_df["Ticker"] == ticker].sort_values("TradeDateTime").copy()
        lots = []
        realized_pl = 0.0
        run_bought = 0.0   # 本輪持倉（自上次清空後）累計買進股數，供分層減碼用

        for _, row in tdf.iterrows():
            qty = safe_float(row["Shares"])
            pr = safe_float(row["Price"])
            fee = safe_float(row.get("Fee"))
            slip = safe_float(row.get("Slippage"))
            if qty <= 0 or pr <= 0:
                continue

            if normalize_trade_type(row["Type"]) == "BUY":
                cost = pr * qty + fee + slip
                lots.append({"shares": qty, "price": cost / qty, "date": row.get("TradeDateTime")})
                cash -= cost
                run_bought += qty
            else:
                proceeds = pr * qty - fee - slip
                cash += proceeds
                sell_qty = qty
                while sell_qty > 0 and lots:
                    first = lots[0]
                    matched = min(sell_qty, first["shares"])
                    realized_pl += ((proceeds / qty) - first["price"]) * matched
                    first["shares"] -= matched
                    sell_qty -= matched
                    if first["shares"] <= 1e-9:
                        lots.pop(0)
                if not lots:
                    run_bought = 0.0   # 全部平倉→下一輪重新計算

        total_realized_pl += realized_pl
        rem_shares = sum(l["shares"] for l in lots)
        if rem_shares > 1e-9:
            cb = sum(l["shares"] * l["price"] for l in lots)
            avg_cost = cb / rem_shares
            entry_date = lots[0].get("date") if lots else None      # 最早未平倉 lot = 當前持倉進場起點
            last_pr = get_last_price(ticker)
            # 抓不到報價時，現金早已扣除；若直接 continue 會讓部位消失、NAV 被低估。
            # 改用成本價估值並標記 PriceStale，UI 可提示「報價過期」。
            price_stale = not last_pr
            if price_stale:
                last_pr = avg_cost
            mv = rem_shares * last_pr
            portfolio.append({
                "Ticker": ticker,
                "Shares": round(rem_shares, 4),
                "AvgCost": round(avg_cost, 4),
                "LastPrice": round(last_pr, 4),
                "MarketValue": round(mv, 4),
                "Unrealized": round(mv - cb, 4),
                "PL_Pct": round(((last_pr / avg_cost) - 1) * 100, 2),
                "RealizedPL": round(realized_pl, 4),
                "PriceStale": price_stale,
                "EntryDate": entry_date,
                "EntryShares": round(run_bought, 4),
                "SplitAdjusted": _split_factor_after(ticker, entry_date) != 1.0,
            })

    return portfolio, cash, total_realized_pl


def get_bucket_limits(bucket: str) -> Dict:
    return {
        "max_weight": SMALL_CAP_MAX_WEIGHT if bucket == "SMALL_CAP" else LARGE_CAP_MAX_WEIGHT,
        "risk_per_trade_pct": SMALL_CAP_RISK_PER_TRADE_PCT if bucket == "SMALL_CAP" else LARGE_CAP_RISK_PER_TRADE_PCT
    }


def calc_portfolio_heat(portfolio: List[Dict], total_assets: float) -> Dict:
    heat = sum(
        (safe_float(p.get("LastPrice")) - safe_float(p.get("StopLoss", 0))) * safe_float(p.get("Shares"))
        for p in portfolio
        if safe_float(p.get("LastPrice")) > safe_float(p.get("StopLoss", 0)) > 0
    )
    return {"heat_pct": heat / total_assets * 100 if total_assets > 0 else 0.0}


def calc_category_exposure(portfolio: List[Dict], total_assets: float) -> List[Dict]:
    """各半導體次產業的權重（由大到小），用於檢視產業集中度。"""
    if total_assets <= 0 or not portfolio:
        return []
    agg: Dict[str, float] = {}
    for p in portfolio:
        c = get_us_semi_category(p.get("Ticker", "")) or "其他"
        agg[c] = agg.get(c, 0.0) + safe_float(p.get("MarketValue"))
    rows = [{"category": c, "weight_pct": round(v / total_assets * 100, 1),
             "over_cap": (v / total_assets) > CATEGORY_MAX_WEIGHT}
            for c, v in agg.items()]
    return sorted(rows, key=lambda x: -x["weight_pct"])


def calc_portfolio_correlation(portfolio: List[Dict], lookback: int = None) -> Dict:
    """
    估算持倉的兩兩日報酬相關性（衡量分散度）。回傳平均相關性、相關性最高的一對，
    以及是否超過警示門檻。半導體部位通常高度相關，此值偏高即代表「看似多檔、實則一注」。
    """
    if lookback is None:
        lookback = CORR_LOOKBACK_DAYS
    syms = [p.get("Ticker") for p in portfolio if p.get("Ticker")]
    empty = {"avg_corr": None, "max_pair": None, "max_corr": None, "n": len(syms),
             "over_threshold": False}
    if len(syms) < 2:
        return empty

    rets = {}
    for s in syms:
        h = get_unified_analysis(s)
        if h is not None and not h.empty and len(h) > lookback:
            rets[s] = h["Close"].pct_change().tail(lookback).reset_index(drop=True)
    if len(rets) < 2:
        return empty

    rdf = pd.DataFrame(rets).dropna()
    if len(rdf) < 10:
        return empty

    corr = rdf.corr()
    iu = np.triu_indices_from(corr.values, k=1)
    vals = corr.values[iu]
    if vals.size == 0:
        return empty
    avg = float(np.nanmean(vals))
    mi = int(np.nanargmax(vals))
    pairs = list(zip([corr.index[i] for i in iu[0]], [corr.columns[j] for j in iu[1]]))
    return {
        "avg_corr": round(avg, 2),
        "max_pair": pairs[mi],
        "max_corr": round(float(vals[mi]), 2),
        "n": int(len(corr.columns)),
        "over_threshold": avg >= CORR_WARN_THRESHOLD,
    }


def corr_risk_scale(avg_corr: Optional[float]) -> float:
    """P5：依持倉平均兩兩相關性連續縮減逐檔風險預算的係數。

    半導體同板塊持倉相關性常 >0.7，「N 檔各 RISK_PER_TRADE」的名目分散實為一注；
    與其只警示，不如直接把新倉/加碼的風險預算按相關性打折。
    scale = clip(1.3 − avg_corr, CORR_RISK_SCALE_FLOOR, 1.0)。
    avg_corr=None（持倉<2 檔或資料不足）或功能停用時回 1.0（不縮）。
    """
    if not CORR_RISK_SCALE_ENABLE or avg_corr is None:
        return 1.0
    return float(min(1.0, max(CORR_RISK_SCALE_FLOOR, 1.3 - float(avg_corr))))


# ===============================
# Strategy evaluator
# ===============================
def evaluate_strategy(
    ticker,
    hist,
    held_shares,
    current_mkt_value,
    total_assets,
    cash,
    market_regime,
    portfolio_heat_pct,
    portfolio,
    recent_buy: bool = False,
    recent_sell: bool = False,
    avg_corr: Optional[float] = None,   # P5：持倉平均兩兩相關性（由呼叫端計算傳入，None=不縮量）
) -> Tuple[float, str, Dict, str]:
    last = hist.iloc[-1]
    close = safe_float(last["Close"])
    atr = safe_float(last["ATR"])
    sma20 = safe_float(last["SMA20"])
    sma200 = safe_float(last.get("SMA200", 0))
    rsi = safe_float(last["RSI"])
    score, meta = rank_symbol_strength(ticker, hist, market_regime)

    # 出場以「進場成本」為固定參考；新倉以收盤估算（買在收盤）
    avg_cost = next((safe_float(p.get("AvgCost")) for p in portfolio if p.get("Ticker") == ticker), 0.0)
    entry_date = next((p.get("EntryDate") for p in portfolio if p.get("Ticker") == ticker), None)
    entry = avg_cost if (held_shares > 0 and avg_cost > 0) else close

    # 進場後窗口：以「日期」(截到當日 00:00) 篩選、含進場日當天的日線。
    #   peak       → Chandelier 移動停損用（進場後最高 High）
    #   peak_close → 保本上移判定用（進場後最高「收盤」，避免進場日單根長上影誤觸 +1R）
    # 關鍵：取不到任何進場後 K 棒時，一律退回 close，絕不退回含進場前高點的 RollingHigh20，
    #       否則保本/移動停損會被進場前的高點誤觸發（剛買進就被掃出）。
    peak = close
    peak_close = close
    bars_since_entry = EXIT_MIN_HOLD_BARS + 1     # entry_date 缺失時視為成熟部位（不進保護期）
    atr_entry = atr                               # entry-time ATR；預設＝當前 ATR（新倉/抓不到時）
    win_high = None                               # P2：進場後窗口的 High/ATR 序列，供 Chandelier ratchet
    win_atr = None
    if held_shares > 0 and entry_date is not None:
        try:
            ed = pd.Timestamp(entry_date)
            if ed.tzinfo is not None:
                ed = ed.tz_localize(None)
            ed = ed.normalize()                                              # 截到當日 00:00，確保含進場日的日線
            idx_naive = hist.index.tz_localize(None) if hist.index.tz is not None else hist.index
            idx_norm = pd.DatetimeIndex(idx_naive).normalize()
            mask = (idx_norm >= ed)
            if mask.any():
                win_high = hist["High"].to_numpy()[mask]                    # P2：Chandelier ratchet 用
                peak = max(float(win_high.max()), close)
                peak_close = max(float(hist["Close"].to_numpy()[mask].max()), close)
                bars_since_entry = int(mask.sum())
            else:
                bars_since_entry = 1                                        # 今天剛進場、日線尚未成形 → 仍在保護期
            # entry-time ATR：取「進場當根（或最近一根 ≤ 進場日）」的 ATR，使 R 不隨之後波動漂移。
            # Wilder ATR 為因果指標，該根 ATR 不論何時計算皆相同，等同於進場當下寫入的值。
            atr_col = hist["ATR"].to_numpy()
            if mask.any():
                win_atr = atr_col[mask]                                     # P2：Chandelier ratchet 用
            at_or_before = (idx_norm <= ed)
            if at_or_before.any():
                cand_atr = safe_float(atr_col[at_or_before][-1])
            elif mask.any():
                cand_atr = safe_float(atr_col[mask][0])                     # 進場日早於序列起點 → 取最早一根
            else:
                cand_atr = 0.0
            if cand_atr > 0:
                atr_entry = cand_atr
        except Exception:
            pass

    in_grace = held_shares > 0 and bars_since_entry <= EXIT_MIN_HOLD_BARS     # 新倉保護期

    # 本輪進場總股數（供分批減碼與「最後一份 tranche 收緊移動停損」判定）
    entry_shares = next((safe_float(p.get("EntryShares")) for p in portfolio if p.get("Ticker") == ticker), 0.0)
    if entry_shares <= 0:
        entry_shares = held_shares
    # 減碼兩次後剩最後一份（≈1/3）→ 移動停損由 3ATR 收緊至 2ATR
    final_tranche = entry_shares > 0 and held_shares <= entry_shares * (1.0 - 2.0 * EXIT_SCALE_OUT_PCT) + 1e-6
    trail_atr_mult = EXIT_TRAIL_ATR_FINAL if final_tranche else EXIT_TRAIL_ATR

    R = (EXIT_INIT_STOP_ATR * atr_entry) if atr_entry > 0 else entry * 0.07  # 初始風險距離（以 entry-time ATR 計，不漂移）
    initial_stop = max(0.01, entry - R)                                     # 初始硬止損（錨定進場成本，保護期內仍生效）
    # Chandelier（P2 修正）：舊式「peak − mult×當前ATR」在急跌時 ATR 暴增會整條下移——
    # 昨天守 $112 今天變守 $108，恰在最需要停損紀律的時刻放鬆。改取進場後逐日
    # (累計最高High − mult×當日ATR) 的「歷史最大值」：完全因果、跨日只升不降。
    # 最後一份 tranche 收緊 mult 時以現行 mult 全窗重算，只會更緊不會更鬆，與鎖利意圖一致。
    if atr > 0:
        chandelier = peak - trail_atr_mult * atr
        if held_shares > 0 and win_high is not None and len(win_high) > 0:
            _chand_series = np.maximum.accumulate(win_high) - trail_atr_mult * win_atr
            chandelier = max(chandelier, float(np.nanmax(_chand_series)))
    else:
        chandelier = entry * 0.90
    # 保本上移：需「進場後曾收在 +1R 之上」(用收盤，不用盤中 High)，且已過保護期
    reached_1R = (peak_close >= entry + EXIT_BREAKEVEN_AT_R * R) and not in_grace
    # P6：保本線可設緩衝（entry − buffer×R），預設 buffer=0 行為不變；
    # 用於避開 +1R~+2R 之間「回踩成本即被掃出」的洗盤走廊，採用前請先跑 --study grace 樣本外驗證
    floor_be = (entry - EXIT_BREAKEVEN_BUFFER_R * R) if reached_1R else 0.0
    trail_stop = chandelier if not in_grace else 0.0                        # 移動停損保護期內不啟用
    effective_stop = max(initial_stop, trail_stop, floor_be)                # 實際止損：只升不降
    tp1 = min(entry + EXIT_TP1_R * R, entry * (1 + EXIT_TP1_PCT))   # +2R 與 +20% 取較早者
    tp2 = min(entry + EXIT_TP2_R * R, entry * (1 + EXIT_TP2_PCT))   # +4R 與 +40% 取較早者

    # 分層減碼：觸及 TP1 減至 (1−1×比例)、觸及 TP2 減至 (1−2×比例)，最後一份跟著移動停損跑。
    # 用「本輪進場總股數」當基準（已於上方取得），使建議持續到減足目標、且不會重複過賣。
    if close >= tp2:
        target_frac, scale_ref = max(0.0, 1.0 - 2.0 * EXIT_SCALE_OUT_PCT), tp2
    elif close >= tp1:
        target_frac, scale_ref = max(0.0, 1.0 - EXIT_SCALE_OUT_PCT), tp1
    else:
        target_frac, scale_ref = 1.0, tp1
    scale_out_qty = max(0, math.floor(held_shares - entry_shares * target_frac))   # 應再減出的股數

    limits = get_bucket_limits(meta.get("bucket", "LARGE_CAP"))

    _corr_scale = corr_risk_scale(avg_corr)   # P5：高相關持倉 → 逐檔風險預算連續打折
    risk_dollars = (total_assets * limits["risk_per_trade_pct"]
                    * safe_float(market_regime.get("risk_multiplier", 1.0)) * _corr_scale)
    risk_per_share = max(0.01, R)                                           # 新倉風險/股 = R
    qty = math.floor(risk_dollars / risk_per_share) if risk_per_share > 0 else 0

    # 投組熱度閘（P5 修正）：計入「本筆新增風險 qty×R」再比上限。舊式只看既有熱度
    # （heat<5% 即放行），訊號叢發日會系統性超標（4.9% 再進 1% → 事後 5.9%）。新倉/加碼皆適用。
    heat_after_ok = (total_assets > 0 and
                     portfolio_heat_pct / 100.0 + (qty * risk_per_share) / total_assets
                     <= PORTFOLIO_HEAT_LIMIT_PCT)

    action, mode, tp = "WATCH", "NONE", None

    # 產業曝險閘：新倉/加碼後，同一半導體次產業的總權重不得超過上限
    cat = get_us_semi_category(ticker)
    cat_mv = sum(safe_float(p.get("MarketValue")) for p in portfolio
                 if get_us_semi_category(p.get("Ticker", "")) == cat)
    after_cat_weight = (cat_mv + close * qty) / total_assets if total_assets > 0 else 1.0
    category_ok = after_cat_weight <= CATEGORY_MAX_WEIGHT
    category_capped = False

    # ── 進場品質（P1）：新倉須為「突破」或「回檔」、不過度乖離、且強於大盤，杜絕追高 ──
    volume = safe_float(last["Volume"])
    vol_sma20 = safe_float(last.get("VOL_SMA20", 0))
    rs20 = safe_float(meta.get("rs20_vs_spy", 0))
    prev_high20 = safe_float(hist["RollingHigh20"].shift(1).iloc[-1]) if "RollingHigh20" in hist.columns else 0.0
    breakout_ok = prev_high20 > 0 and close > prev_high20 and volume > vol_sma20 * ENTRY_BREAKOUT_VOL_MULT
    pullback_ok_entry = sma20 > 0 and close <= sma20 * (1 + ENTRY_PULLBACK_SMA20_PCT)
    not_extended = (atr <= 0) or (close <= sma20 + ENTRY_MAX_EXT_ATR * atr)
    rs_ok_entry = (not ENTRY_REQUIRE_RS_POSITIVE) or rs20 > 0
    entry_trigger_ok = (not ENTRY_REQUIRE_TRIGGER) or breakout_ok or pullback_ok_entry
    entry_quality_ok = entry_trigger_ok and not_extended and rs_ok_entry

    # 加碼只加贏家（P2）：現價須站上 進場 + N×R（且為正報酬），否則禁止往下攤平
    add_profit_ok = (not ADD_REQUIRE_PROFIT) or (close >= entry + ADD_MIN_PROFIT_R * R)

    if held_shares > 0 and close < effective_stop:
        _bounds = {"RISK_EXIT": initial_stop, "TRAIL_EXIT": trail_stop, "BREAKEVEN_EXIT": floor_be}
        mode = max(_bounds, key=_bounds.get)                                # 標示是哪一道止損生效
        action, tp = "SELL_EXIT", round(effective_stop, 2)

    elif (held_shares > 0 and not in_grace and EXIT_TREND_BREAK and sma200 > 0
          and close <= entry and close < sma200):
        action, mode, tp = "SELL_EXIT", "TREND_EXIT", round(close, 2)       # 虧損中且跌破年線→趨勢轉弱出場

    elif (held_shares > 0 and not in_grace and TIME_STOP_ENABLE
          and bars_since_entry >= TIME_STOP_BARS
          and peak_close < entry + TIME_STOP_MIN_R * R                       # 進場後從未達到 +1R（呆滯）
          and rs20 < 0):                                                     # 且已弱於大盤 → 釋出資金
        action, mode, tp = "SELL_EXIT", "TIME_EXIT", round(close, 2)

    elif held_shares > 0 and close >= tp1 and scale_out_qty >= 1:
        action, mode, tp = "SELL_PARTIAL", "TAKE_PROFIT", round(scale_ref, 2)

    elif held_shares > 0 and meta["trend_ok"] and meta["liquid_ok"] and not meta["earnings_blocked"]:
        current_weight = current_mkt_value / total_assets if total_assets > 0 else 0
        can_add = current_weight < limits["max_weight"] * SCALE_IN_MAX_WEIGHT_RATIO
        pullback_ok = close <= sma20 * 1.03
        avail_cash = cash - (total_assets * CASH_RESERVE_PCT)                                  # 與新進場一致
        after_weight = (current_mkt_value + close * qty) / total_assets if total_assets > 0 else 1.0
        if (score >= SCORE_BUY_ADD_THRESHOLD and can_add and pullback_ok and qty > 0
                and add_profit_ok                                                              # 只對獲利中部位加碼，杜絕往下攤平
                and avail_cash > close * qty                                                   # 補：現金檢查
                and after_weight <= limits["max_weight"]                                       # 補：加碼後權重上限
                and heat_after_ok                                                              # P5：加碼後熱度（含本筆增量）不超限
                and not recent_buy and not recent_sell                                         # 補：冷卻期（買/賣皆計）
                and market_regime.get("allow_add_position")):
            if category_ok:
                action, mode, tp = "BUY_ADD", "SCALE_IN", close
            else:
                category_capped = True                                                        # 訊號達標但被產業曝險上限擋下

    elif held_shares <= 0 and meta["trend_ok"] and meta["liquid_ok"] and not meta["earnings_blocked"]:
        avail_cash = cash - (total_assets * CASH_RESERVE_PCT)
        if (score >= SCORE_BUY_NOW_THRESHOLD and qty > 0 and avail_cash > close * qty
                and heat_after_ok                                                              # P5：進場後熱度（含本筆增量）不超限
                and not recent_buy and not recent_sell                                         # 補：冷卻期（買/賣皆計）
                and entry_quality_ok                                                           # P1：突破/回檔觸發 + 不追高 + 強於大盤
                and market_regime.get("allow_new_position")):
            if category_ok:
                action, mode, tp = "BUY_NOW", "TREND_MOMENTUM", close
            else:
                category_capped = True

    partial_sell_qty = scale_out_qty if action == "SELL_PARTIAL" else 0

    return score, action, {
        "close": close,
        "rsi": rsi,
        "atr": atr,
        "entry_atr": round(atr_entry, 4),
        "trail_atr_mult": trail_atr_mult,
        "corr_risk_scale": round(_corr_scale, 2),   # P5：本次 sizing 套用的相關性縮量係數
        "entry_ref": round(entry, 2),
        "stop_loss": round(effective_stop, 2),
        "trend_stop": round(chandelier, 2),
        "take_profit_1": round(tp1, 2),
        "take_profit_2": round(tp2, 2),
        "suggested_buy_qty": qty,
        "suggested_sell_qty": math.ceil(held_shares) if action == "SELL_EXIT" else partial_sell_qty,
        "category": cat,
        "category_weight": round(after_cat_weight * 100, 1),
        "category_capped": category_capped,
        "target_buy_price": tp if "BUY" in action else None,
        "target_sell_price": tp if "SELL" in action else None,
        "market_regime": market_regime.get("regime"),
        "strategy_mode": mode,
        "bucket": meta.get("bucket"),
        "liquid_ok": meta["liquid_ok"],
        "earnings_blocked": meta["earnings_blocked"],
        "is_squeeze": meta.get("is_squeeze", False),
        "rs20_vs_spy": meta.get("rs20_vs_spy", 0),
        "adx": meta.get("adx", 0),
        "reasons": meta["reasons"],
        # 進場品質診斷（P1/P2）：高分卻未觸發 BUY 時可據此說明原因
        "breakout_ok": breakout_ok,
        "pullback_ok_entry": pullback_ok_entry,
        "not_extended": not_extended,
        "rs_ok_entry": rs_ok_entry,
        "entry_quality_ok": entry_quality_ok,
        "add_profit_ok": add_profit_ok,
        "bars_since_entry": int(bars_since_entry),
    }, " | ".join(meta["reasons"]) if meta["reasons"] else "No Signal"


def enrich_portfolio_with_weight_and_risk(portfolio: List[Dict], total_assets: float, cash: float, market_regime: Dict) -> List[Dict]:
    res = []
    heat = calc_portfolio_heat(portfolio, total_assets).get("heat_pct", 0)
    avg_corr = calc_portfolio_correlation(portfolio).get("avg_corr")   # P5：整組算一次，逐檔傳入
    for p in portfolio:
        hist = get_unified_analysis(p["Ticker"])
        prof = get_symbol_profile(p["Ticker"])
        row = p.copy()
        row["WeightPct"] = (p["MarketValue"] / total_assets * 100) if total_assets > 0 else 0
        row["Sector"] = prof.get("sector", "")
        row["Industry"] = prof.get("industry", "")

        if hist is not None and not hist.empty:
            sc, act, det, _ = evaluate_strategy(
                p["Ticker"], hist, p["Shares"], p["MarketValue"], total_assets, cash, market_regime, heat, portfolio,
                avg_corr=avg_corr,
            )
            row.update({
                "Signal": act,
                "SignalScore": sc,
                "StrategyMode": det["strategy_mode"],
                "StopLoss": det["stop_loss"],
                "TrendStop": det["trend_stop"],
                "TakeProfit1": det["take_profit_1"],
                "TakeProfit2": det["take_profit_2"],
                "TargetBuyPrice": det["target_buy_price"],
                "TargetSellPrice": det["target_sell_price"],
                "SuggestedBuyQty": det["suggested_buy_qty"],
                "SuggestedSellQty": det["suggested_sell_qty"],
                "Bucket": det["bucket"],
                "Category": det.get("category", ""),
                "CategoryWeight": det.get("category_weight", 0),
                "CategoryCapped": det.get("category_capped", False),
                "RS20vsSPY": det.get("rs20_vs_spy", 0),
                "IsSqueeze": det.get("is_squeeze", False),
                "Reasons": det.get("reasons", []),
            })
        res.append(row)
    return res


# ===============================
# Auto scanner
# ===============================
def run_auto_scanner(portfolio, trades_df, cash, total_assets, market_regime, watchlist_df=None) -> Dict:
    alerts_df = load_alerts()
    session = get_market_session()

    # portfolio 已由 enrich_portfolio_with_weight_and_risk 補上 StopLoss → heat 可正確計算
    # （過去此處寫死 0，使 PORTFOLIO_HEAT_LIMIT 在此進場路徑形同虛設）
    heat_pct = calc_portfolio_heat(portfolio, total_assets).get("heat_pct", 0.0)
    avg_corr = calc_portfolio_correlation(portfolio).get("avg_corr")   # P5：整組算一次

    universe = sorted(list(set(
        [p["Ticker"] for p in portfolio] +
        (watchlist_df[watchlist_df["Enabled"]]["Ticker"].tolist() if watchlist_df is not None and not watchlist_df.empty else [])
    )))

    candidates = []

    for tk in universe:
        h = get_unified_analysis(tk)
        if h is None:
            continue
        held = next((p["Shares"] for p in portfolio if p["Ticker"] == tk), 0)
        mkt_val = next((p["MarketValue"] for p in portfolio if p["Ticker"] == tk), 0)
        recent_buy, recent_sell = get_recent_trade_status(tk, trades_df)
        sc, act, det, note = evaluate_strategy(
            tk, h, held, mkt_val, total_assets, cash, market_regime, heat_pct, portfolio,
            recent_buy=recent_buy, recent_sell=recent_sell, avg_corr=avg_corr,
        )
        candidates.append({"ticker": tk, "score": sc, "action": act, "details": det, "note": note})

        if act != "WATCH" and should_send_alert(alerts_df, tk, act, det["close"], sc, session):
            action_emoji = "🟢" if "BUY" in act else "🔴"
            msg = f"{action_emoji} *{tk}* `{act}` | 分數: {sc:.1f}\n{note}"
            if send_telegram_msg(msg):
                log_sent_alert(tk, act, det["close"], sc, session, det.get("target_buy_price"), "")

    # 每次掃描都把可行動訊號（非 WATCH）寫入 Signals 表，供訊號成效追蹤累積樣本。
    logged = log_signal_snapshots([c for c in candidates if c["action"] != "WATCH"], session)

    candidates.sort(key=lambda x: (0 if "SELL" in x["action"] else 1, -x["score"]))
    top_buys = [c for c in candidates if "BUY" in c["action"]][:SCANNER_TOP_N]
    top_exits = [c for c in candidates if "SELL" in c["action"]]
    top_watch = [c for c in candidates if c["action"] == "WATCH"][:5]

    return {
        "candidates": candidates,
        "top_buys": top_buys,
        "top_exits": top_exits,
        "top_watch": top_watch,
        "logs": ["掃描完成"],
        "metrics": {
            "universe_count": len(universe),
            "buy_signals": len(top_buys),
            "sell_signals": len(top_exits),
            "signals_logged": logged,
        },
    }


def calculate_performance_metrics(history_df: pd.DataFrame) -> Dict:
    if history_df.empty or len(history_df) < 2:
        return {"max_drawdown_pct": None, "sharpe": None, "win_rate": None, "total_return_pct": None}
    nav = pd.to_numeric(history_df["TotalAssets"], errors="coerce").dropna()
    rets = nav.pct_change().dropna()
    total_ret = (nav.iloc[-1] / nav.iloc[0] - 1) * 100 if nav.iloc[0] > 0 else 0
    win_rate = (rets > 0).sum() / len(rets) * 100 if len(rets) > 0 else None
    return {
        "max_drawdown_pct": ((nav / nav.cummax() - 1).min() * 100) if not nav.empty else 0,
        "sharpe": (rets.mean() / rets.std() * (252 ** 0.5)) if len(rets) > 1 and rets.std() > 0 else None,
        "win_rate": win_rate,
        "total_return_pct": total_ret,
    }


def build_trade_preview(trades_df, initial_capital, ticker, trade_type, price, shares, fee) -> Dict:
    port, c, _ = build_portfolio(trades_df, initial_capital)
    tot = c + sum(x["MarketValue"] for x in port)
    net = price * shares * (1 + DEFAULT_SLIPPAGE_PCT) + fee if trade_type == "BUY" else price * shares * (1 - DEFAULT_SLIPPAGE_PCT) - fee
    return {
        "current_cash": c,
        "after_cash": c - net if trade_type == "BUY" else c + net,
        "gross_total": price * shares,
        "fee": fee,
        "slippage": price * shares * DEFAULT_SLIPPAGE_PCT,
        "current_weight_pct": 0,
        "after_weight_pct": 0,
        "bucket": "LARGE_CAP",
        "bucket_max_weight_pct": 25,
        "exceed_max_weight": False,
        "sell_exceeds_position": False
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 美股半導體掃描模組
# ═══════════════════════════════════════════════════════════════════════════════
import concurrent.futures as _cf

# ═══════════════════════════════════════════════════════════════════════════════
# 美股半導體完整宇宙（不使用 ETF）
# 目標：盡量完整覆蓋美股可交易的半導體 / 半導體設備 / 材料 / EDA / IP / 封測 / 光子相關公司
# 備註：這裡採「美股上市可交易」口徑，不限是否為美國本土公司
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# 美股半導體完整宇宙（100+ 檔，不使用 ETF）
# 口徑：美股可交易的半導體 / 設備 / 材料 / EDA / 封測 / 光子 / 關聯基礎元件
# ═══════════════════════════════════════════════════════════════════════════════

US_SEMI_CATEGORY_MAP: Dict[str, str] = {
    # ── AI / GPU / CPU / Datacenter / Networking ─────────────────────────
    "NVDA": "AI / GPU / HPC",
    "AMD": "AI / GPU / CPU",
    "INTC": "CPU / IDM",
    "AVGO": "Broadline / Infrastructure Semis",
    "MRVL": "Data Center / Networking",
    "QCOM": "Mobile / Communication Chips",
    "CRDO": "Datacenter Connectivity",
    "ALAB": "Datacenter Networking",
    "ARM": "CPU IP / Architecture",

    # ── Analog / Power / Mixed Signal / MCU ──────────────────────────────
    "ADI": "Analog / Mixed Signal",
    "TXN": "Analog / Power",
    "MCHP": "MCU / Analog",
    "MPWR": "Power Management",
    "NXPI": "Auto / Analog / MCU",
    "ON": "Power / Auto Semis",
    "STM": "IDM / Analog / MCU",
    "SLAB": "IoT / Mixed Signal",
    "SMTC": "Analog / IoT",
    "CRUS": "Mixed Signal / Audio",
    "ALGM": "Power / Motion Control",
    "AOSL": "Power Semiconductors",
    "POWI": "Power Management",
    "DIOD": "Discrete / Analog",
    "VSH": "Discrete / Power",
    "VICR": "Power Modules",
    "MTSI": "RF / Analog",
    "AZTA": "Specialty Components",
    "WOLF": "SiC / Power",
    "IPGP": "Power / Photonics",
    "AEHR": "Burn-in / Test",
    "ATOM": "Power / Embedded",
    "NVEI": "Adjacency / Ignore if needed",

    # ── RF / Wireless / Connectivity / Broadband ─────────────────────────
    "SWKS": "RF / Mobile",
    "QRVO": "RF / Mobile",
    "MACOM": "RF / Optical / Analog",
    "MXL": "Broadband / Connectivity",
    "SIMO": "Controller IC",
    "CEVA": "Wireless / Edge IP",
    "SITM": "Timing / Clock",
    "RMBS": "Interface / Memory IP",
    "SYNA": "Human Interface / Edge AI",
    "HIMX": "Display Driver / Vision",
    "AMBA": "Vision / Edge AI",
    "LSCC": "FPGA / Edge",
    "INDI": "Auto / Edge Semis",

    # ── Memory / Storage / Controllers ───────────────────────────────────
    "MU": "Memory",
    "WDC": "Storage / NAND",
    "RMBS": "Memory / Interface IP",
    "IMMR": "Memory / Haptics / IP",

    # ── Foundry / IDM / Manufacturing ────────────────────────────────────
    "TSM": "Foundry",
    "GFS": "Foundry",
    "UMC": "Foundry",
    "ASX": "Foundry / Specialty",
    "UCTT": "Equipment Subsystems",
    "IFNNY": "Power / IDM",

    # ── EDA / IP / Design Tools / Masks ──────────────────────────────────
    "CDNS": "EDA / Design Tools",
    "SNPS": "EDA / Design Tools",
    "PDFS": "EDA / Yield Software",
    "PLAB": "Photomask",
    "RMBS": "Semiconductor IP",
    "CEVA": "Semiconductor IP",
    "ARM": "Semiconductor IP",

    # ── Wafer Fab Equipment / Test / Metrology ───────────────────────────
    "AMAT": "Wafer Fab Equipment",
    "LRCX": "Wafer Fab Equipment",
    "KLAC": "Process Control / Inspection",
    "ASML": "Lithography",
    "TER": "Test Equipment",
    "ONTO": "Inspection / Metrology",
    "FORM": "Test / Inspection",
    "ACMR": "Cleaning Equipment",
    "ACLS": "Ion Implant Equipment",
    "KLIC": "Packaging Equipment",
    "COHU": "Test / Handler",
    "CAMT": "Metrology / Inspection",
    "NANO": "Inspection Equipment",
    "MKSI": "Process / Vacuum / Control",
    "ICHR": "Fluid Delivery / Subsystems",
    "BRKS": "Equipment Subsystems",
    "VECO": "Process Equipment",
    "ENTG": "Materials / Contamination Control",
    "AEIS": "Power Systems / Equipment Components",
    "CCMP": "Precision Components",
    "ESIO": "Laser Microfabrication",
    "MVIS": "Optical Components / Adjacent",
    "OSIS": "Imaging / Detection / Sensors",
    "KEYS": "Electronic Test / Measurement",
    "AMKR": "OSAT / Packaging",
    "HSTM": "Ignore if needed",

    # ── Materials / Wafers / Compound Semiconductors ─────────────────────
    "AXTI": "Compound Semiconductor Materials",
    "IIVI": "Compound Semis / Photonics",
    "COHR": "Photonics / Laser",
    "LITE": "Optical / Datacom",
    "AAOI": "Optical / Datacom",
    "VIAV": "Optical Components / Test",
    "LASR": "Laser / Photonics",
    "OEPN": "Optical / Adjacent",
    "AEHR": "SiC / Burn-in / Test",
    "WOLF": "SiC / Materials",

    # ── Packaging / Assembly / OSAT / Back-end ───────────────────────────
    "AMKR": "OSAT / Packaging",
    "KLIC": "Wire Bond / Packaging Equipment",
    "FORM": "Back-end Test / Inspection",
    "COHU": "Back-end Test / Handler",
    "ASX": "Foundry / Packaging Adjacent",

    # ── Optical / Photonics / Laser / Datacom ────────────────────────────
    "LITE": "Optical / Datacom",
    "AAOI": "Optical / Datacom",
    "COHR": "Photonics / Laser",
    "IIVI": "Photonics / Compound Semis",
    "IPGP": "Fiber Laser / Photonics",
    "VIAV": "Optical Test / Components",
    "LASR": "Laser / Photonics",
    "OI": "Optical Adjacent",
    "MTSI": "RF / Microwave",
    "MACOM": "Optical / RF",

    # ── Auto / ADAS / Vision / Sensing ───────────────────────────────────
    "MBLY": "Auto / ADAS",
    "INDI": "Auto Semiconductors",
    "AMBA": "Vision / Auto / Edge AI",
    "HIMX": "Display / Vision",
    "OUST": "LiDAR / Sensing",
    "INVZ": "LiDAR / Sensing",
    "LAZR": "LiDAR / Sensing",
    "MVIS": "LiDAR / Optical Sensing",
    "AEVA": "LiDAR / Sensing",
    "CGNX": "Machine Vision / Sensors",

    # ── FPGA / Specialized / Edge / Security / Timing ────────────────────
    "LSCC": "FPGA / Edge",
    "SITM": "Timing / Clocks",
    "AMBA": "Vision / Edge AI",
    "OSIS": "Security / Sensors",
    "CRUS": "Mixed Signal",
    "SYNA": "Edge / Interface",
    "CEVA": "IP / Wireless",
    "RMBS": "IP / Interface",
    "LPL": "Ignore if needed",

    # ── Semiconductor-adjacent compute / infrastructure ──────────────────
    "SMCI": "AI Server / Infrastructure",
    "DELL": "AI Server / Infrastructure",
    "ANET": "Datacenter Networking",
    "CIEN": "Optical Networking",
    "FN": "Electronics Manufacturing / Hardware",
    "SANM": "Electronics Manufacturing",
}

# 已下市/更名/破產（2026-06 宇宙健檢確認取不到資料；接手代碼多已在宇宙內，故直接移除不損覆蓋）：
#   BRKS→AZTA、CCMP→ENTG、ESIO→MKSI、NANO→ONTO、IIVI→COHR（皆已併購/更名）
#   MACOM 為錯誤代碼（公司代碼為 MTSI）
#   WOLF：Wolfspeed 2025 Chapter 11，舊普通股 2025-10 自 NYSE 下市
#   LAZR：Luminar 2025 Chapter 11，2025-12 自 Nasdaq 下市，現於 OTC 以 LAZRQ 交易
_US_SEMI_DELISTED = {"BRKS", "CCMP", "ESIO", "NANO", "IIVI", "MACOM", "WOLF", "LAZR"}

# 半導體宇宙（已過濾掉下市/錯誤代碼）
US_SEMI_UNIVERSE: List[str] = sorted(t for t in dict.fromkeys([
    # AI / GPU / CPU / Networking
    "NVDA", "AMD", "INTC", "AVGO", "MRVL", "QCOM", "CRDO", "ALAB", "ARM",

    # Analog / Power / Mixed signal / MCU
    "ADI", "TXN", "MCHP", "MPWR", "NXPI", "ON", "STM", "SLAB", "SMTC",
    "CRUS", "ALGM", "AOSL", "POWI", "DIOD", "VSH", "VICR", "MTSI", "ATOM",

    # RF / Wireless / Connectivity
    "SWKS", "QRVO", "MACOM", "MXL", "SIMO", "CEVA", "SITM", "RMBS", "SYNA",
    "HIMX", "AMBA", "LSCC", "INDI",

    # Memory / Storage
    "MU", "WDC", "IMMR",

    # Foundry / IDM / manufacturing
    "TSM", "GFS", "UMC", "ASX", "IFNNY",

    # EDA / IP / masks
    "CDNS", "SNPS", "PDFS", "PLAB",

    # Equipment / metrology / test
    "AMAT", "LRCX", "KLAC", "ASML", "TER", "ONTO", "FORM", "ACMR", "ACLS",
    "AEHR", "KLIC", "COHU", "CAMT", "NANO", "MKSI", "ICHR", "UCTT", "BRKS",
    "VECO", "ENTG", "AEIS", "CCMP", "ESIO", "KEYS",

    # Materials / compound semiconductors / photonics
    "AXTI", "IIVI", "COHR", "LITE", "AAOI", "VIAV", "IPGP", "LASR", "WOLF",

    # Packaging / OSAT
    "AMKR",

    # Auto / ADAS / sensing / vision
    "MBLY", "OUST", "INVZ", "LAZR", "MVIS", "AEVA", "CGNX",

    # Adjacent but tradable semi ecosystem
    "SMCI", "ANET", "CIEN", "DELL", "FN", "SANM",

    # Additional niche / specialty names
    "ALAB", "AZTA", "ATOM", "OSIS", "MACOM", "MTSI", "SYNA", "HIMX",
    "INDI", "LSCC", "SITM", "PLAB", "PDFS", "SIMO", "MXL", "CEVA",
]) if t not in _US_SEMI_DELISTED)

US_SEMI_GROUPS: Dict[str, List[str]] = {
    "AI / GPU / CPU": ["NVDA", "AMD", "INTC", "AVGO", "MRVL", "QCOM", "CRDO", "ALAB", "ARM"],
    "Analog / Power / MCU": ["ADI", "TXN", "MCHP", "MPWR", "NXPI", "ON", "STM", "SLAB", "SMTC", "CRUS", "ALGM", "AOSL", "POWI", "DIOD", "VSH", "VICR"],
    "RF / Connectivity": ["SWKS", "QRVO", "MACOM", "MXL", "SIMO", "CEVA", "SITM", "RMBS"],
    "Memory / Storage": ["MU", "WDC", "IMMR"],
    "Foundry / IDM": ["TSM", "GFS", "UMC", "ASX", "IFNNY"],
    "EDA / IP": ["CDNS", "SNPS", "PDFS", "PLAB", "ARM", "RMBS", "CEVA"],
    "Equipment / Test / Metrology": ["AMAT", "LRCX", "KLAC", "ASML", "TER", "ONTO", "FORM", "ACMR", "ACLS", "AEHR", "KLIC", "COHU", "CAMT", "NANO", "MKSI", "ICHR", "UCTT", "BRKS", "VECO", "KEYS"],
    "Materials / Compound / Photonics": ["ENTG", "AEIS", "CCMP", "AXTI", "IIVI", "COHR", "LITE", "AAOI", "VIAV", "IPGP", "LASR", "WOLF"],
    "Packaging / OSAT": ["AMKR"],
    "Auto / ADAS / Vision": ["MBLY", "INDI", "AMBA", "HIMX", "OUST", "INVZ", "LAZR", "MVIS", "AEVA", "CGNX"],
    "Infrastructure / Adjacent": ["SMCI", "ANET", "CIEN", "DELL", "FN", "SANM"],
}


@lru_cache(maxsize=1)
@ttl_cache(86400, maxsize=16)
def _fetch_etf_holdings(etf: str = "SOXX") -> List[str]:
    try:
        tk = yf.Ticker(etf)
        holdings = tk.funds_data.top_holdings if hasattr(tk, "funds_data") else None
        if holdings is None or holdings.empty:
            return []
        tickers_from_etf = [
            normalize_ticker(str(t))
            for t in holdings.index.tolist()
            if str(t).strip() and len(str(t).strip()) <= 6
        ]
        return tickers_from_etf[:50]
    except Exception:
        return []


def get_us_semi_universe(include_etf: bool = False) -> List[str]:
    """
    取得完整美股半導體宇宙。include_etf=True 時併入 SOXX/SMH 成分股，
    讓宇宙隨新上市/權重變化自我更新（每日快取），不再只靠靜態硬編碼清單。
    """
    tickers = [normalize_ticker(t) for t in US_SEMI_UNIVERSE]
    if include_etf:
        for etf in ("SOXX", "SMH"):
            tickers += _fetch_etf_holdings(etf)
    return sorted(list(dict.fromkeys(tickers)))


def get_us_semi_category(ticker: str) -> str:
    ticker = normalize_ticker(ticker)
    if ticker in US_SEMI_CATEGORY_MAP:
        return US_SEMI_CATEGORY_MAP[ticker]

    prof = get_symbol_profile(ticker)
    industry = str(prof.get("industry") or "").strip()
    sector = str(prof.get("sector") or "").strip()

    if industry:
        return industry
    if sector:
        return sector
    return "Semiconductor / Other"


US_SEMI_SCORE_STRONG = get_env_float("US_SEMI_SCORE_STRONG", 5.5)
US_SEMI_SCORE_BUY = get_env_float("US_SEMI_SCORE_BUY", 3.5)
US_SEMI_SCORE_WATCH = get_env_float("US_SEMI_SCORE_WATCH", 2.0)
US_SEMI_MIN_DOLLAR_VOL = get_env_float("US_SEMI_MIN_DOLLAR_VOL", 20_000_000)
US_SEMI_MIN_PRICE = get_env_float("US_SEMI_MIN_PRICE", 10.0)
US_SEMI_SCAN_WORKERS = get_env_int("US_SEMI_SCAN_WORKERS", 6)  # 過高併發易觸發 yfinance 限流→該檔靜默漏掉
US_SEMI_TOP_N = get_env_int("US_SEMI_TOP_N", 15)

def _get_sox_regime() -> Dict:
    try:
        soxx = yf.Ticker("SOXX").history(period="1y", auto_adjust=True)
        spy = yf.Ticker("SPY").history(period="1y", auto_adjust=True)
        if soxx.empty or spy.empty:
            return {"trend": "NEUTRAL", "rs_vs_spy": 0.0, "score": 0}

        soxx_close = float(soxx["Close"].iloc[-1])
        soxx_sma50 = float(soxx["Close"].rolling(50).mean().iloc[-1])
        soxx_sma200 = float(soxx["Close"].rolling(200).mean().iloc[-1])

        spy_ret20 = (spy["Close"].iloc[-1] / spy["Close"].iloc[-20] - 1) * 100
        soxx_ret20 = (soxx["Close"].iloc[-1] / soxx["Close"].iloc[-20] - 1) * 100
        rs_vs_spy = round(soxx_ret20 - spy_ret20, 2)

        score = 0
        if soxx_close > soxx_sma50:
            score += 1
        if soxx_close > soxx_sma200:
            score += 1
        if soxx_sma50 > soxx_sma200:
            score += 1
        if rs_vs_spy > 0:
            score += 1

        trend = "BULL" if score >= 3 else "BEAR" if score <= 1 else "NEUTRAL"
        return {
            "trend": trend,
            "rs_vs_spy": rs_vs_spy,
            "score": score,
            "soxx_price": round(soxx_close, 2)
        }
    except Exception:
        return {"trend": "NEUTRAL", "rs_vs_spy": 0.0, "score": 0}


def _us_semi_score_one(ticker: str, sox_regime: Dict) -> Optional[Dict]:
    ticker = normalize_ticker(ticker)
    try:
        hist = get_unified_analysis(ticker)
        if hist is None or hist.empty or len(hist) < 60:
            return None

        last = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) >= 2 else last
        close = safe_float(last["Close"])
        atr = safe_float(last.get("ATR", 0))
        sma20 = safe_float(last["SMA20"])
        sma50 = safe_float(last["SMA50"])
        sma200 = safe_float(last["SMA200"])
        volume = safe_float(last["Volume"])
        vsma20 = safe_float(last["VOL_SMA20"])
        rsi = safe_float(last["RSI"])
        adx = safe_float(last.get("ADX", 0))
        macd_h = safe_float(last["MACD_Hist"])
        bb_w = safe_float(last.get("BB_Width", 1.0))
        rs20 = safe_float(last.get("RS20_vs_SPY", 0))
        obv_slp = safe_float(last.get("OBV_Slope20", 0))
        high252 = safe_float(last.get("RollingHigh252", 0))
        dv20 = safe_float(last.get("DollarVolume20", 0))

        if close < US_SEMI_MIN_PRICE or dv20 < US_SEMI_MIN_DOLLAR_VOL:
            return None
        if is_earnings_blocked(ticker):
            return None

        score, reasons = 0.0, []

        above_200 = close > sma200
        above_50 = close > sma50
        if above_200 and sma50 > sma200:
            score += 1.5
            reasons.append("SMA 長線多頭")
            if above_50 and sma20 > sma50:
                score += 1.0
                reasons.append("均線完美排列")
        elif above_200:
            score += 0.8
            reasons.append("站上 SMA200")
        else:
            score -= 1.0

        if macd_h > 0 and safe_float(prev.get("MACD_Hist", 0)) < macd_h:
            score += 0.8
            reasons.append("MACD 翻多加速")
        elif macd_h > 0:
            score += 0.4

        if 50 <= rsi <= 72:
            score += 0.8
            reasons.append(f"RSI 健康 ({rsi:.0f})")
        elif rsi < 40:
            score += 0.3
        elif rsi > 75:
            score -= 0.4

        if adx >= BREAKOUT_ADX_MIN:
            score += 0.8
            reasons.append(f"ADX 強趨勢 ({adx:.0f})")

        if rs20 > RS_STRONG_THRESHOLD:
            score += 1.0
            reasons.append(f"強於大盤 +{rs20:.1f}%")
        elif rs20 < RS_WEAK_THRESHOLD:
            score -= 0.6

        sox_rs = sox_regime.get("rs_vs_spy", 0)
        if rs20 > sox_rs + 2.0:
            score += 0.8
            reasons.append("優於半導體指數")

        if obv_slp > 0 and close > sma20:
            score += 0.5
            reasons.append("OBV 法人吸籌")
        if volume > vsma20 * 1.5 and close > safe_float(prev["Close"]):
            score += 0.5
            reasons.append("放量上漲日")

        prev_bw = safe_float(prev.get("BB_Width", 1.0))
        if bb_w < 0.08:
            reasons.append("BB 壓縮蓄力")
        if bb_w > 0.08 and prev_bw < 0.08 and close > safe_float(last.get("BB_upper", 0)) and volume > vsma20 * 1.3:
            score += 1.5
            reasons.append("BB 壓縮放量突破")

        if high252 > 0 and close >= high252 * (1 - NEAR_52W_HIGH_PCT):
            score += 0.8
            reasons.append("接近 52W 年高")

        sox_mult = {"BULL": 1.0, "NEUTRAL": 0.85, "BEAR": 0.65}.get(sox_regime.get("trend", "NEUTRAL"), 0.85)
        score = round(score * sox_mult, 2)

        if score >= US_SEMI_SCORE_STRONG:
            signal = "STRONG_BUY"
        elif score >= US_SEMI_SCORE_BUY:
            signal = "BUY"
        elif score >= US_SEMI_SCORE_WATCH:
            signal = "WATCH"
        else:
            return None

        stop_loss = round(max(0.01, close - 2.0 * atr) if atr > 0 else close * 0.93, 2)
        tp1 = round(close + 2.0 * atr if atr > 0 else close * 1.08, 2)
        tp2 = round(close + 4.0 * atr if atr > 0 else close * 1.15, 2)

        risk_per_share = max(0.01, close - stop_loss)
        risk_dollars = DEFAULT_INITIAL_CAPITAL * LARGE_CAP_RISK_PER_TRADE_PCT
        suggested_qty = max(1, math.floor(risk_dollars / risk_per_share))

        # 進場品質（P1）：突破/回檔觸發、不過度乖離、強於大盤 — 與 evaluate_strategy 一致
        prev_high20 = safe_float(prev.get("RollingHigh20", 0))
        breakout_ok = prev_high20 > 0 and close > prev_high20 and volume > vsma20 * ENTRY_BREAKOUT_VOL_MULT
        pullback_ok = sma20 > 0 and close <= sma20 * (1 + ENTRY_PULLBACK_SMA20_PCT)
        not_extended = (atr <= 0) or (close <= sma20 + ENTRY_MAX_EXT_ATR * atr)
        rs_ok = (not ENTRY_REQUIRE_RS_POSITIVE) or rs20 > 0
        entry_quality_ok = ((not ENTRY_REQUIRE_TRIGGER) or breakout_ok or pullback_ok) and not_extended and rs_ok

        return {
            "ticker": ticker,
            "category": get_us_semi_category(ticker),
            "score": score,
            "signal": signal,
            "entry_quality_ok": entry_quality_ok,
            "extended": not not_extended,
            "close": round(close, 2),
            "stop_loss": stop_loss,
            "tp1": tp1,
            "tp2": tp2,
            "suggested_qty": suggested_qty,
            "bucket": classify_symbol_bucket(ticker, hist),
            "rsi": round(rsi, 1),
            "adx": round(adx, 1),
            "rs20_vs_spy": round(rs20, 2),
            "reasons": reasons[:5],
            "atr": round(atr, 2),
            "dv20_m": round(dv20 / 1_000_000, 1),
        }
    except Exception:
        return None


def migrate_trades_v1_to_v2() -> Tuple[bool, str]:
    try:
        ws = get_trades_worksheet(readonly=True)
        values = gsheet_retry(lambda: ws.get_all_values())

        if not values:
            return False, "工作表為空"

        first = [str(x).strip() for x in values[0]]
        is_hdr = (
            first[:len(TRADE_HEADERS_V1)] == TRADE_HEADERS_V1 or
            first[:len(TRADE_HEADERS_V2)] == TRADE_HEADERS_V2 or
            first[:len(TRADE_HEADERS_LEGACY6)] == TRADE_HEADERS_LEGACY6
        )
        rows = values[1:] if is_hdr else values

        migrated = []
        skipped = 0
        skipped_headers = 0

        for row in rows:
            row = [str(c).strip() for c in list(row)]

            if not any(row):
                skipped += 1
                continue

            # 先移除尾端空白
            while row and row[-1] == "":
                row.pop()

            # 特判：右移 3 格的殘留 header row
            if len(row) >= 9 and row[0] == "" and row[1] == "" and row[2] == "":
                shifted = row[3:]
                if len(shifted) >= 6 and shifted[:6] == ["TradeDateTime", "CreatedAt", "Ticker", "Type", "Price", "Shares"]:
                    skipped_headers += 1
                    continue

            # 一般 header row 殘留直接跳過
            joined = "|".join(row).upper()
            if "TRADEDATETIME" in joined and "TICKER" in joined and "TYPE" in joined:
                dt_try = pd.to_datetime(row[0], errors="coerce") if row else pd.NaT
                if pd.isna(dt_try):
                    skipped_headers += 1
                    continue

            item = _normalize_trade_row_to_v2(row)
            if item is None:
                skipped += 1
                continue

            migrated.append([
                item["TradeDateTime"],
                item["CreatedAt"],
                normalize_ticker(item["Ticker"]),
                normalize_trade_type(item["Type"]),
                float(item["Price"]) if str(item["Price"]).strip() != "" else 0.0,
                float(item["Shares"]) if str(item["Shares"]).strip() != "" else 0.0,
                float(item["GrossTotal"]) if str(item["GrossTotal"]).strip() != "" else 0.0,
                float(item["Fee"]) if str(item["Fee"]).strip() != "" else 0.0,
                float(item["Slippage"]) if str(item["Slippage"]).strip() != "" else 0.0,
                float(item["NetTotal"]) if str(item["NetTotal"]).strip() != "" else 0.0,
                item["Note"],
                item["OrderID"],
            ])

        if not migrated:
            return False, "無可遷移資料"

        # 依交易時間排序，避免修復後順序混亂
        migrated.sort(key=lambda x: pd.to_datetime(x[0], errors="coerce"))

        new_data = [TRADE_HEADERS_V2] + migrated
        gsheet_retry(lambda: ws.clear())
        gsheet_retry(lambda: ws.update("A1", new_data))
        clear_app_caches()

        return True, f"✅ 遷移完成：{len(migrated)} 列，跳過空白 {skipped} 列，跳過表頭殘留 {skipped_headers} 列"
    except Exception as e:
        return False, f"❌ 遷移失敗：{e}"


def _held_tickers_from_trades(trades_df) -> set:
    """直接由交易紀錄計算目前淨持有(>0)的代碼，不依賴抓價，較 build_portfolio 穩健。"""
    if trades_df is None or getattr(trades_df, "empty", True):
        return set()
    try:
        trades_df = _split_adjust(trades_df)   # 分割對齊：避免分割後淨股數計算偏差
        net: Dict[str, float] = {}
        for _, row in trades_df.iterrows():
            tk = normalize_ticker(row.get("Ticker", ""))
            if not tk:
                continue
            q = safe_float(row.get("Shares"))
            if normalize_trade_type(row.get("Type")) == "BUY":
                net[tk] = net.get(tk, 0.0) + q
            else:
                net[tk] = net.get(tk, 0.0) - q
        return {tk for tk, n in net.items() if n > 1e-9}
    except Exception:
        return set()


def _annotate_semi_candidate(r: Dict, exposure: Dict, held_set: set,
                             trades_df, cap_pct: float) -> None:
    """對半導體候選標註持倉層級狀態：已持有(加碼)、產業曝險已達上限、冷卻期(剛賣出)。"""
    tk = normalize_ticker(r.get("ticker", ""))
    warnings = []

    r["held"] = tk in held_set
    if r["held"]:
        warnings.append("ℹ️ 已持有（此為加碼）")

    catinfo = exposure.get(r.get("category"))
    r["cat_weight"] = float(catinfo["weight_pct"]) if catinfo else 0.0
    r["category_full"] = bool(catinfo and catinfo["weight_pct"] >= cap_pct)
    if r["category_full"]:
        warnings.append(f"⚠️ {r.get('category')} 曝險已達 {r['cat_weight']:.0f}%（上限 {cap_pct:.0f}%）")

    recent_sell = False
    if trades_df is not None:
        try:
            _rb, recent_sell = get_recent_trade_status(tk, trades_df)
        except Exception:
            recent_sell = False
    r["cooldown"] = bool(recent_sell)
    if recent_sell:
        warnings.append(f"⚠️ 冷卻期（{COOLDOWN_DAYS} 日內剛賣出）")

    r["warnings"] = warnings


def apply_entry_risk_gates(cand: Dict, portfolio: List[Dict], total_assets: float,
                           cash: float, heat_pct: float, market_regime: Dict,
                           avg_corr: Optional[float] = None) -> None:
    """
    半導體進場引擎的統一風控閘（就地更新 cand）。
    過去 _us_semi_score_one 算出的 suggested_qty 只看 ATR 風險、且用固定 32000 計本金，
    完全無視 regime / 現金 / 單檔權重 / 產業曝險 / 投組熱度 / 冷卻期。此函式補上這些閘，
    並改用「即時權益」做 sizing，使進場引擎與 evaluate_strategy 的風控一致。

    寫回欄位：
      suggested_qty_raw → 原始（僅 ATR 風險、固定本金）股數
      suggested_qty     → 套用所有風控閘後的最終可買股數（可為 0）
      gate_blocked      → True 表示被風控完全擋下（qty=0）
      warnings          → 追加縮減/封鎖原因（沿用既有渲染）
    """
    warnings = cand.setdefault("warnings", [])
    cand["suggested_qty_raw"] = safe_int(cand.get("suggested_qty", 0))

    close = safe_float(cand.get("close"))
    stop = safe_float(cand.get("stop_loss"))
    cat = cand.get("category", "")
    held = bool(cand.get("held"))
    tk = normalize_ticker(cand.get("ticker", ""))
    limits = get_bucket_limits(cand.get("bucket", "LARGE_CAP"))

    if total_assets <= 0 or close <= 0:
        cand["suggested_qty"] = 0
        cand["gate_blocked"] = True
        return

    risk_per_share = max(0.01, close - stop)
    risk_mult = safe_float(market_regime.get("risk_multiplier", 1.0), 1.0)

    # ③ 即時權益 sizing（取代固定 DEFAULT_INITIAL_CAPITAL）＋ regime 風險係數 ＋ 相關性縮量（P5）
    _cs = corr_risk_scale(avg_corr)
    qty = math.floor(total_assets * limits["risk_per_trade_pct"] * risk_mult * _cs / risk_per_share)
    if _cs < 1.0:
        cand["corr_risk_scale"] = round(_cs, 2)
    notes = []

    def _cut(cap_qty: int, hard_msg: str, soft_msg: str):
        nonlocal qty
        if cap_qty < qty:
            notes.append(hard_msg if cap_qty <= 0 else soft_msg.format(n=max(0, cap_qty)))
            qty = max(0, min(qty, cap_qty))

    # ① 市場 regime：新倉看 allow_new_position，加碼看 allow_add_position
    regime_allow = market_regime.get("allow_add_position") if held else market_regime.get("allow_new_position")
    if not regime_allow or risk_mult <= 0:
        qty = 0
        notes.append(f"🚫 市場狀態 {market_regime.get('regime', '?')} 暫停{'加碼' if held else '新進場'}")

    # 現金閘（保留現金準備金）
    avail_cash = cash - total_assets * CASH_RESERVE_PCT
    _cut(math.floor(avail_cash / close), "🚫 可用現金不足（已扣除現金準備）", "✂️ 現金上限 {n} 股")

    # ④ 單檔權重上限（加碼另需未達 max_weight×SCALE_IN 比例才可加）
    cur_mv = sum(safe_float(p.get("MarketValue")) for p in portfolio
                 if normalize_ticker(p.get("Ticker", "")) == tk)
    max_w = limits["max_weight"]
    if held and total_assets > 0 and (cur_mv / total_assets) >= max_w * SCALE_IN_MAX_WEIGHT_RATIO:
        qty = 0
        notes.append(f"🚫 已達加碼權重上限（{cur_mv / total_assets * 100:.0f}%）")
    else:
        _cut(math.floor((max_w * total_assets - cur_mv) / close),
             f"🚫 單檔權重已達上限 {max_w * 100:.0f}%", "✂️ 單檔權重上限 {n} 股")

    # ⑤ 半導體次產業曝險上限
    cat_mv = sum(safe_float(p.get("MarketValue")) for p in portfolio
                 if get_us_semi_category(p.get("Ticker", "")) == cat)
    _cut(math.floor((CATEGORY_MAX_WEIGHT * total_assets - cat_mv) / close),
         f"🚫 {cat} 產業曝險已達上限 {CATEGORY_MAX_WEIGHT * 100:.0f}%", "✂️ 產業曝險上限 {n} 股")

    # ⑥ 投組總熱度上限（此版本才真正生效）
    heat_room = (PORTFOLIO_HEAT_LIMIT_PCT - heat_pct / 100.0) * total_assets
    _cut(math.floor(heat_room / risk_per_share),
         f"🚫 投組總熱度已達上限 {PORTFOLIO_HEAT_LIMIT_PCT * 100:.0f}%", "✂️ 熱度上限 {n} 股")

    # ⑦ 冷卻期（同檔近 N 日剛賣出）
    if cand.get("cooldown"):
        qty = 0
        notes.append(f"🚫 冷卻期（{COOLDOWN_DAYS} 日內剛賣出），暫不進場")

    # ⑧ 進場品質（P1 新倉）/ 只加贏家（P2 加碼）— 與 evaluate_strategy 一致
    if held:
        if ADD_REQUIRE_PROFIT:
            avg_cost = next((safe_float(p.get("AvgCost")) for p in portfolio
                             if normalize_ticker(p.get("Ticker", "")) == tk), 0.0)
            if avg_cost > 0 and close < avg_cost + ADD_MIN_PROFIT_R * risk_per_share:
                qty = 0
                notes.append("🚫 加碼需部位已獲利（不往下攤平）")
    elif not cand.get("entry_quality_ok", True):
        qty = 0
        notes.append("🚫 進場品質未達（追高/無突破回檔/弱於大盤）")

    qty = max(0, int(qty))
    cand["suggested_qty"] = qty
    cand["gate_blocked"] = qty == 0
    if notes:
        warnings.extend(notes)


def run_us_semi_scanner(extra_tickers: Optional[List[str]] = None,
                        log_signals: bool = False,
                        trades_df=None) -> Dict:
    sox_regime = _get_sox_regime()

    base_universe = get_us_semi_universe(include_etf=False)
    universe = list(dict.fromkeys(
        base_universe + [normalize_ticker(t) for t in (extra_tickers or [])]
    ))

    results = []
    with _cf.ThreadPoolExecutor(max_workers=US_SEMI_SCAN_WORKERS) as ex:
        futs = {ex.submit(_us_semi_score_one, tk, sox_regime): tk for tk in universe}
        for fut in _cf.as_completed(futs):
            try:
                r = fut.result()
            except Exception:
                r = None
            if r:
                results.append(r)

    results.sort(key=lambda x: -x["score"])

    # ── 持倉脈絡：對候選標註集中度/冷卻，並套用統一進場風控閘（你的實際進場在此引擎）──
    if trades_df is None:
        try:
            trades_df = load_trades()
        except Exception:
            trades_df = None
    portfolio, total_assets, cash = [], DEFAULT_INITIAL_CAPITAL, DEFAULT_INITIAL_CAPITAL
    market_regime = {"regime": "UNKNOWN", "allow_new_position": True,
                     "allow_add_position": True, "risk_multiplier": 0.5}
    heat_pct = 0.0
    if trades_df is not None and not trades_df.empty:
        try:
            portfolio_raw, cash, _ = build_portfolio(trades_df, DEFAULT_INITIAL_CAPITAL)
            total_assets = cash + sum(safe_float(p.get("MarketValue")) for p in portfolio_raw)
            market_regime = get_market_regime()
            # enrich 後才有 StopLoss → heat 才算得出來（raw portfolio 的 heat 恆為 0）
            portfolio = enrich_portfolio_with_weight_and_risk(
                portfolio_raw, total_assets, cash, market_regime) if portfolio_raw else []
            heat_pct = calc_portfolio_heat(portfolio, total_assets).get("heat_pct", 0.0)
        except Exception:
            portfolio = []
    exposure = {c["category"]: c for c in calc_category_exposure(portfolio, total_assets)}
    held_set = _held_tickers_from_trades(trades_df)            # 直接由交易算持倉，不依賴抓價
    avg_corr = calc_portfolio_correlation(portfolio).get("avg_corr") if portfolio else None   # P5
    cap_pct = CATEGORY_MAX_WEIGHT * 100
    for r in results:
        _annotate_semi_candidate(r, exposure, held_set, trades_df, cap_pct)
        apply_entry_risk_gates(r, portfolio, total_assets, cash, heat_pct, market_regime,
                               avg_corr=avg_corr)

    strong_buy = [r for r in results if r["signal"] == "STRONG_BUY"]
    buy = [r for r in results if r["signal"] == "BUY"]
    watch = [r for r in results if r["signal"] == "WATCH"]

    # 取不到資料的代碼（皆為快取命中，不再連網）：通常是下市/更名/錯誤代碼或被限流
    no_data_tickers = [tk for tk in universe if get_unified_analysis(tk) is None]

    # 僅在排程掃描時寫入 Signals（log_signals=True），避免 App 互動點擊重複灌列
    signals_logged = log_semi_signal_snapshots(strong_buy + buy, sox_regime) if log_signals else 0

    eastern = pytz.timezone("US/Eastern")
    scan_date = datetime.now(eastern).strftime("%Y-%m-%d")

    return {
        "strong_buy": strong_buy,
        "buy": buy,
        "watch": watch,
        "all_results": results,
        "sox_regime": sox_regime,
        "total_scanned": len(universe),
        "total_hits": len(results),
        "no_data_count": len(no_data_tickers),
        "no_data_tickers": no_data_tickers,
        "signals_logged": signals_logged,
        "scan_date": scan_date,
    }


def format_us_semi_tg_messages(scan_result: Dict) -> List[str]:
    sox = scan_result["sox_regime"]
    strong = scan_result["strong_buy"]
    buys = scan_result["buy"]
    watches = scan_result["watch"]
    date = scan_result["scan_date"]
    n_scan = scan_result["total_scanned"]
    n_hit = scan_result["total_hits"]

    SOX_EMOJI = {"BULL": "🐂", "NEUTRAL": "➡️", "BEAR": "🐻"}
    RANK = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

    sox_trend = sox.get("trend", "NEUTRAL")
    sox_rs = sox.get("rs_vs_spy", 0)
    sox_price = sox.get("soxx_price", 0)

    header = [
        "📡 *美股半導體強勢股 · 收盤掃描*",
        f"📅 {date} (美東)  |  台灣時間 09:00 掃描",
        f"📊 SOX {SOX_EMOJI[sox_trend]} {sox_trend}  vs SPY {sox_rs:+.1f}%  |  SOXX ${sox_price}",
        f"掃描 {n_scan} 檔  |  入選 {n_hit} 檔",
        f"🔴 強力 {len(strong)}  🟢 積極 {len(buys)}  🟡 留意 {len(watches)}",
        "─────────────────────────────",
        "",
    ]

    def _stock_block(i: int, r: dict) -> List[str]:
        rank = RANK[i] if i < len(RANK) else f"{i+1}."
        stars = "⭐" * min(5, max(1, round(r["score"] / 1.5)))
        sig_lbl = {"STRONG_BUY": "🔴 強力買進", "BUY": "🟢 積極買進", "WATCH": "🟡 留意"}.get(r["signal"], "")
        reasons = "、".join(r["reasons"][:3]) if r["reasons"] else "—"
        category = r.get("category", "Semiconductor / Other")
    
        lines = [
            f"{rank} *{r['ticker']}*  {stars}  {sig_lbl}",
            f"   類別: `{category}`",
            f"   分數 *{r['score']:.1f}*  |  RSI {r['rsi']:.0f}  ADX {r['adx']:.0f}",
            f"   現價 ${r['close']}  |  RS vs SPY {r['rs20_vs_spy']:+.1f}%",
            f"   📈 {reasons}",
            f"   🛑 ${r['stop_loss']}  🎯 TP1 ${r['tp1']}  TP2 ${r['tp2']}",
            f"   建議股數 {r['suggested_qty']} 股  |  日均量 ${r['dv20_m']:.0f}M",
        ]
        warns = r.get("warnings") or []
        if warns:
            lines.append("   " + "　".join(warns))
        lines.append("")
        return lines

    footer = ["─────────────────────────────", "⚠️ 本訊息僅供參考，不構成投資建議"]

    MAX = 4000
    sep = "\n"
    msgs, cur = [], list(header)
    is_first = True
    all_stocks = strong + buys + watches

    for i, r in enumerate(all_stocks):
        blk = sep.join(_stock_block(i, r))
        cur_txt = sep.join(cur)
        if len(cur_txt) + len(blk) + 1 > MAX and not is_first:
            msgs.append(cur_txt)
            cur = [f"📡 *美股半導體 · 續篇 ({len(msgs) + 1})*", ""]
        cur.extend(_stock_block(i, r))
        is_first = False

    cur.extend(footer)
    msgs.append(sep.join(cur))
    return msgs


def send_us_semi_tg(messages: List[str]) -> bool:
    if not TG_TOKEN or not TG_CHAT_ID:
        return False
    ok = True
    for msg in messages:
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                timeout=15,
            )
            if not r.json().get("ok"):
                ok = False
        except Exception:
            ok = False
    return ok

# ═══════════════════════════════════════════════════════════════════════════════
# 訊號成效追蹤 / 真實交易統計 / 宇宙健檢（新增）
# ═══════════════════════════════════════════════════════════════════════════════
def evaluate_signal_outcomes(lookahead_days: int = SIGNAL_LOG_LOOKAHEAD_DAYS,
                             dedup_window_days: Optional[int] = None,
                             source: Optional[str] = None,
                             benchmark: Optional[str] = "SOXX") -> pd.DataFrame:
    """
    回顧 Signals 表中已「成熟」的訊號，計算其前瞻表現（+N 交易日報酬、MFE/MAE）。
    用途：驗證評分是否真有 edge，據此調整分數門檻與權重。
    source：可指定只評估特定來源（"SEMI" / "PORTFOLIO" / "LEGACY"）；None 表示全部。
            由於兩套引擎分數尺度不同，分析時建議指定單一來源（多半用 "SEMI"）。
    benchmark（P3）：同窗口的對標報酬與超額報酬（BenchRetPct / ExcessRetPct）。
            多頭市場中「原始前瞻報酬為正」幾乎人人辦得到，無法區分評分有效與板塊 beta；
            edge 的證據必須看「超額報酬」是否隨分數分箱單調遞增。預設 SOXX（半導體宇宙），
            跨產業訊號可傳 "SPY"，傳 None 可停用。
    去重：同一檔、同方向（BUY/SELL）在 dedup_window_days 日內只計第一筆，避免持續訊號被每日
          快照重複計數，亦避免前瞻窗口重疊造成樣本相關性灌水（預設等於 lookahead_days）。
    限制：get_unified_analysis 僅含約 2 年歷史，更早的訊號評不到；交易日對齊以日曆日緩衝近似；
          前瞻報酬為「未套用停損」的原始報酬，實盤虧損端通常因停損而更小。
    """
    if dedup_window_days is None:
        dedup_window_days = lookahead_days
    cols = ["Ticker", "DateTime", "Action", "StrategyMode", "Source", "Score",
            "EntryPx", "FwdRetPct", "StopRetPct", "BenchRetPct", "ExcessRetPct",
            "MFEPct", "MAEPct", "Win"]
    sig = load_signals()
    if sig is None or sig.empty:
        return pd.DataFrame(columns=cols)

    # 對標序列（一次抓取；之後逐訊號以日期對齊同窗口）
    bench_dates = None
    bench_close = None
    if benchmark:
        bh = get_unified_analysis(benchmark)
        if bh is not None and not bh.empty:
            try:
                bench_dates = pd.DatetimeIndex(bh.index.tz_localize(None)).normalize()
            except (TypeError, AttributeError):
                bench_dates = pd.DatetimeIndex(bh.index).normalize()
            bench_close = bh["Close"].to_numpy(dtype="float64")

    sig = sig.dropna(subset=["DateTime", "Ticker", "Close"]).copy()
    if source is not None and "Source" in sig.columns:
        sig = sig[sig["Source"] == source]
    cutoff = pd.Timestamp(datetime.now()) - pd.Timedelta(days=int(lookahead_days * 1.6))
    mature = sig[sig["DateTime"] <= cutoff]
    if mature.empty:
        return pd.DataFrame(columns=cols)

    def _dir(action) -> str:
        a = str(action).upper()
        return "BUY" if "BUY" in a else ("SELL" if "SELL" in a else a)

    rows = []
    for tk, grp in mature.groupby("Ticker"):
        hist = get_unified_analysis(tk)
        if hist is None or hist.empty:
            continue
        closes = hist["Close"]
        lows = hist["Low"]
        idx = hist.index
        try:
            idx_dates = pd.DatetimeIndex(idx.tz_localize(None)).normalize()
        except (TypeError, AttributeError):
            idx_dates = pd.DatetimeIndex(idx).normalize()

        last_counted: Dict[str, pd.Timestamp] = {}
        for _, s in grp.sort_values("DateTime").iterrows():
            entry_dt = pd.Timestamp(s["DateTime"]).normalize()
            bucket = _dir(s.get("Action", ""))
            prev = last_counted.get(bucket)
            if prev is not None and (entry_dt - prev).days < dedup_window_days:
                continue                                          # 去重：窗口內同方向只計第一筆
            pos = int(idx_dates.searchsorted(entry_dt))           # 訊號日或之後第一個交易日
            if pos >= len(idx) - 1:
                continue
            entry_px = float(closes.iloc[pos])
            if entry_px <= 0:
                continue
            last_counted[bucket] = entry_dt
            fwd_pos = min(pos + lookahead_days, len(idx) - 1)
            fwd_px = float(closes.iloc[fwd_pos])
            window = closes.iloc[pos:fwd_pos + 1]
            low_window = lows.iloc[pos:fwd_pos + 1]
            ret = (fwd_px / entry_px - 1) * 100

            # 套用停損後報酬：買進類訊號若窗口內最低價曾觸及當時記錄的 StopLoss，
            # 視為在停損價出場（原始 FwdRetPct 未停損，會高估虧損端、灌水 edge）。
            stop_px = safe_float(s.get("StopLoss")) if "StopLoss" in s.index else 0.0
            stop_ret = ret
            if bucket == "BUY" and stop_px > 0 and float(low_window.min()) <= stop_px:
                stop_ret = (stop_px / entry_px - 1) * 100

            # 對標同窗口報酬（P3）：以日期對齊 benchmark 自身的交易日序列
            bench_ret = None
            if bench_close is not None:
                b_ent = int(bench_dates.searchsorted(entry_dt))
                if b_ent < len(bench_close) and bench_close[b_ent] > 0:
                    b_ext = min(int(bench_dates.searchsorted(idx_dates[fwd_pos])),
                                len(bench_close) - 1)
                    bench_ret = (bench_close[b_ext] / bench_close[b_ent] - 1) * 100

            rows.append({
                "Ticker": tk,
                "DateTime": s["DateTime"],
                "Action": str(s.get("Action", "")),
                "StrategyMode": str(s.get("StrategyMode", "")),
                "Source": str(s.get("Source", "")),
                "Score": safe_float(s["Score"]) if pd.notna(s["Score"]) else None,
                "EntryPx": round(entry_px, 2),
                "FwdRetPct": round(ret, 2),
                "StopRetPct": round(stop_ret, 2),  # 觸及停損即視為停損出場後的報酬
                "BenchRetPct": round(bench_ret, 2) if bench_ret is not None else None,
                "ExcessRetPct": round(ret - bench_ret, 2) if bench_ret is not None else None,
                "MFEPct": round((float(window.max()) / entry_px - 1) * 100, 2),  # 最大有利偏移
                "MAEPct": round((float(window.min()) / entry_px - 1) * 100, 2),  # 最大不利偏移→驗證停損
                "Win": ret > 0,
            })
    return pd.DataFrame(rows, columns=cols)


def summarize_signal_edge(outcomes: Optional[pd.DataFrame] = None,
                          lookahead_days: int = SIGNAL_LOG_LOOKAHEAD_DAYS,
                          source: Optional[str] = None,
                          benchmark: Optional[str] = "SOXX") -> pd.DataFrame:
    """
    將買進類訊號依分數分箱，彙總命中率/平均報酬/MAE。
    主要判準（P3）＝「超額報酬 vs benchmark」：多頭市場中原始報酬近乎人人為正，
    無法區分「評分有效」與「板塊 beta」；只有超額報酬隨分數分箱單調遞增，
    才是評分有 edge 的證據。原始報酬欄保留供對照。
    source：限定來源（建議單一來源，因兩套引擎分數尺度不同）。
    """
    if outcomes is None:
        outcomes = evaluate_signal_outcomes(lookahead_days, source=source, benchmark=benchmark)
    if outcomes is None or outcomes.empty:
        return pd.DataFrame()

    buys = outcomes[outcomes["Action"].astype(str).str.contains("BUY", na=False)].copy()
    buys = buys.dropna(subset=["Score"])
    if buys.empty:
        return pd.DataFrame()

    buys["分數區間"] = pd.cut(
        buys["Score"], bins=[-99, 3.5, 4.5, 5.5, 999],
        labels=["<3.5", "3.5–4.5", "4.5–5.5", "≥5.5"],
    )
    g = buys.groupby("分數區間", observed=True)
    cols = {"樣本數": g["FwdRetPct"].count()}
    if "ExcessRetPct" in buys.columns and buys["ExcessRetPct"].notna().any():
        cols[f"超額{lookahead_days}日報酬%"] = g["ExcessRetPct"].mean().round(2)
        cols["超額勝率%"] = g["ExcessRetPct"].apply(
            lambda s: round(float((s.dropna() > 0).mean()) * 100, 1) if s.notna().any() else None)
    cols["勝率%(原始)"] = (g["Win"].mean() * 100).round(1)
    cols[f"平均{lookahead_days}日報酬%(原始)"] = g["FwdRetPct"].mean().round(2)
    cols["中位數%(原始)"] = g["FwdRetPct"].median().round(2)
    if "StopRetPct" in buys.columns:
        cols["停損後平均%"] = g["StopRetPct"].mean().round(2)  # 貼近實盤（觸停損即出場）的報酬
    cols["平均MAE%"] = g["MAEPct"].mean().round(2)
    return pd.DataFrame(cols).reset_index()


def calc_realized_trade_stats(trades_df: pd.DataFrame, split_adjust: bool = True) -> Dict:
    """
    以 FIFO 配對計算「已平倉」交易的真實統計：勝率、平均盈虧、盈虧比、獲利因子、期望值。
    取代以「上漲天數比例」當勝率的誤導指標。每個 FIFO 配對視為一筆平倉結果（lot 層級）。

    split_adjust：實盤交易以「名目價」記錄，須對齊到還原權值價（預設 True）。
      回測產生的交易本就成交在 auto_adjust 還原權值價序列上，若再對齊會「二次分割調整」
      → 含分割窗口（如 NVDA 2024/06 10:1）的已實現統計會被扭曲，故回測須傳 False。
    """
    empty = {"closed_trades": 0, "win_rate": None, "avg_win": None, "avg_loss": None,
             "payoff_ratio": None, "profit_factor": None, "expectancy": None,
             "gross_profit": 0.0, "gross_loss": 0.0, "net_realized": 0.0}
    if trades_df is None or trades_df.empty:
        return empty

    if split_adjust:
        trades_df = _split_adjust(trades_df)   # 分割對齊：FIFO 配對不因分割產生賣超/錯誤盈虧
    pnls: List[float] = []
    for ticker in trades_df["Ticker"].dropna().unique().tolist():
        tdf = trades_df[trades_df["Ticker"] == ticker].sort_values("TradeDateTime")
        lots: List[Dict] = []
        for _, row in tdf.iterrows():
            qty = safe_float(row["Shares"])
            pr = safe_float(row["Price"])
            fee = safe_float(row.get("Fee"))
            slip = safe_float(row.get("Slippage"))
            if qty <= 0 or pr <= 0:
                continue
            if normalize_trade_type(row["Type"]) == "BUY":
                lots.append({"shares": qty, "price": (pr * qty + fee + slip) / qty})
            else:
                proceeds_ps = (pr * qty - fee - slip) / qty
                sell_qty = qty
                while sell_qty > 1e-9 and lots:
                    first = lots[0]
                    matched = min(sell_qty, first["shares"])
                    pnls.append((proceeds_ps - first["price"]) * matched)
                    first["shares"] -= matched
                    sell_qty -= matched
                    if first["shares"] <= 1e-9:
                        lots.pop(0)

    if not pnls:
        return empty

    s = pd.Series(pnls, dtype="float64")
    wins, losses = s[s > 0], s[s < 0]
    gross_profit, gross_loss = float(wins.sum()), float(-losses.sum())
    avg_win = float(wins.mean()) if len(wins) else 0.0
    avg_loss = float(-losses.mean()) if len(losses) else 0.0
    return {
        "closed_trades": int(len(s)),
        "win_rate": round(len(wins) / len(s) * 100, 1),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "payoff_ratio": round(avg_win / avg_loss, 2) if avg_loss > 0 else None,
        "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss > 0 else None,
        "expectancy": round(float(s.mean()), 2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        "net_realized": round(float(s.sum()), 2),
    }


def audit_universe(tickers: List[str]) -> pd.DataFrame:
    """
    逐檔檢查資料可取得性，用於汰除下市/更名/錯誤代碼（會連網，建議偶爾手動執行）。
    取不到資料者（可取得資料=False）排在最前面。
    """
    rows = []
    for t in sorted(set(normalize_ticker(x) for x in tickers)):
        h = get_unified_analysis(t)
        ok = h is not None and not h.empty
        rows.append({
            "Ticker": t,
            "可取得資料": ok,
            "最後價": round(float(h["Close"].iloc[-1]), 2) if ok else None,
            "日均額M": round(float(h["DollarVolume20"].iloc[-1]) / 1e6, 1) if ok else None,
        })
    return pd.DataFrame(rows).sort_values(["可取得資料", "Ticker"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 跨產業廣度發現掃描（P4）— 不限半導體，找出全市場的趨勢領導股
# ═══════════════════════════════════════════════════════════════════════════════
# 預設宇宙：跨產業、高流動性的動能領導股（互動掃描用，避免一次拉 500 檔被限流）。
# 排程深掃可改傳 get_sp500_tickers() 解析後的完整清單。
BROAD_UNIVERSE_DEFAULT: List[str] = [
    # Mega-cap tech / AI
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "AVGO", "TSLA", "AMD", "NFLX",
    "CRM", "ORCL", "ADBE", "NOW", "PLTR", "SNOW", "PANW", "CRWD", "ANET", "MU",
    # Semis / hardware (代表性，完整半導體用半導體掃描)
    "QCOM", "TXN", "INTC", "ARM", "MRVL", "LRCX", "KLAC", "AMAT", "ASML", "TSM",
    # Consumer / internet / payments
    "COST", "WMT", "HD", "MCD", "NKE", "SBUX", "V", "MA", "PYPL", "SHOP",
    "UBER", "ABNB", "BKNG", "DIS", "DASH",
    # Financials / industrials / energy / health
    "JPM", "GS", "BRK-B", "CAT", "DE", "GE", "BA", "XOM", "CVX", "LLY",
    "UNH", "ISRG", "NVO", "VRTX", "REGN",
]


def _broad_score_one(ticker: str, market_regime: Dict) -> Optional[Dict]:
    """跨產業單檔評分（複用 rank_symbol_strength + 進場品質閘），僅回傳達標的買進候選。"""
    ticker = normalize_ticker(ticker)
    try:
        hist = get_unified_analysis(ticker)
        if hist is None or hist.empty or len(hist) < 60:
            return None
        score, meta = rank_symbol_strength(ticker, hist, market_regime)
        if not (meta.get("trend_ok") and meta.get("liquid_ok")) or meta.get("earnings_blocked"):
            return None

        last = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) >= 2 else last
        close = safe_float(last["Close"])
        atr = safe_float(last.get("ATR", 0))
        sma20 = safe_float(last["SMA20"])
        volume = safe_float(last["Volume"])
        vsma20 = safe_float(last.get("VOL_SMA20", 0))
        rs20 = safe_float(meta.get("rs20_vs_spy", 0))

        prev_high20 = safe_float(prev.get("RollingHigh20", 0))
        breakout_ok = prev_high20 > 0 and close > prev_high20 and volume > vsma20 * ENTRY_BREAKOUT_VOL_MULT
        pullback_ok = sma20 > 0 and close <= sma20 * (1 + ENTRY_PULLBACK_SMA20_PCT)
        not_extended = (atr <= 0) or (close <= sma20 + ENTRY_MAX_EXT_ATR * atr)
        rs_ok = (not ENTRY_REQUIRE_RS_POSITIVE) or rs20 > 0
        entry_quality_ok = ((not ENTRY_REQUIRE_TRIGGER) or breakout_ok or pullback_ok) and not_extended and rs_ok

        if score < SCORE_BUY_NOW_THRESHOLD or not entry_quality_ok:
            return None

        stop = round(max(0.01, close - EXIT_INIT_STOP_ATR * atr) if atr > 0 else close * 0.93, 2)
        tp1 = round(close + EXIT_TP1_R * EXIT_INIT_STOP_ATR * atr if atr > 0 else close * 1.10, 2)
        prof = get_symbol_profile(ticker)
        signal = "STRONG_BUY" if score >= SCORE_BUY_ADD_THRESHOLD else "BUY"
        return {
            "ticker": ticker,
            "score": round(score, 2),
            "signal": signal,
            "close": round(close, 2),
            "stop_loss": stop,
            "tp1": tp1,
            "rs20_vs_spy": round(rs20, 2),
            "adx": round(safe_float(meta.get("adx", 0)), 1),
            "sector": prof.get("sector", ""),
            "trigger": "突破" if breakout_ok else "回檔",
            "reasons": meta.get("reasons", [])[:4],
        }
    except Exception:
        return None


def run_broad_scanner(universe: Optional[List[str]] = None,
                      top_n: int = 15,
                      max_tickers: int = 120) -> Dict:
    """
    跨產業廣度發現掃描：對給定宇宙套用趨勢動能評分 + 進場品質閘，回傳全市場買進候選。
    universe 預設為 BROAD_UNIVERSE_DEFAULT（互動用）；排程深掃可傳入更大的清單（會截到 max_tickers）。
    """
    uni = [normalize_ticker(t) for t in (universe or BROAD_UNIVERSE_DEFAULT)]
    uni = list(dict.fromkeys(uni))[:max_tickers]
    market_regime = get_market_regime()

    results = []
    with _cf.ThreadPoolExecutor(max_workers=US_SEMI_SCAN_WORKERS) as ex:
        futs = {ex.submit(_broad_score_one, tk, market_regime): tk for tk in uni}
        for fut in _cf.as_completed(futs):
            try:
                r = fut.result()
            except Exception:
                r = None
            if r:
                results.append(r)

    results.sort(key=lambda x: -x["score"])
    strong = [r for r in results if r["signal"] == "STRONG_BUY"]
    buy = [r for r in results if r["signal"] == "BUY"]
    eastern = pytz.timezone("US/Eastern")
    return {
        "strong_buy": strong,
        "buy": buy,
        "all_results": results[:top_n],
        "regime": market_regime.get("regime"),
        "allow_new_position": market_regime.get("allow_new_position"),
        "total_scanned": len(uni),
        "total_hits": len(results),
        "scan_date": datetime.now(eastern).strftime("%Y-%m-%d %H:%M"),
    }

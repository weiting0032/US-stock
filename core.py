import json
import math
import os
import time
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import gspread
import pandas as pd
import pytz
import requests
import yfinance as yf

try:
    import streamlit as st
except Exception:
    st = None


# ===============================
# Env helpers
# ===============================
def get_env_str(name: str, default: str = "") -> str:
    val = os.getenv(name)
    if val is None:
        return default
    val = str(val).strip()
    return val if val != "" else default


def get_env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None:
        return float(default)
    val = str(val).strip()
    if val == "":
        return float(default)
    try:
        return float(val)
    except Exception:
        return float(default)


def get_env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return int(default)
    val = str(val).strip()
    if val == "":
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
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        res = requests.post(url, json=payload, timeout=10)
        data = res.json()
        return bool(data.get("ok"))
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


def _fetch_wespai_price(symbol: str) -> Optional[float]:
    return None


@lru_cache(maxsize=1024)
def get_last_price(symbol: str) -> Optional[float]:
    wespai_price = _fetch_wespai_price(symbol)
    if wespai_price is not None:
        return wespai_price

    try:
        hist = yf.Ticker(normalize_ticker(symbol)).history(period="5d", auto_adjust=True)
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return None


@lru_cache(maxsize=512)
def get_next_earnings_date(symbol: str) -> Optional[pd.Timestamp]:
    try:
        tk = yf.Ticker(normalize_ticker(symbol))
        cal = tk.calendar
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


@lru_cache(maxsize=512)
def get_symbol_profile(symbol: str) -> Dict:
    symbol = normalize_ticker(symbol)
    try:
        tk = yf.Ticker(symbol)
        info = tk.fast_info if hasattr(tk, "fast_info") else {}
        long_info = tk.info or {}
        return {
            "market_cap": safe_float(long_info.get("marketCap") or info.get("marketCap") or 0.0),
            "sector": str(long_info.get("sector") or "").strip(),
            "industry": str(long_info.get("industry") or "").strip(),
        }
    except Exception:
        return {"market_cap": 0.0, "sector": "", "industry": ""}


def classify_symbol_bucket(symbol: str, hist: Optional[pd.DataFrame] = None) -> str:
    prof = get_symbol_profile(symbol)
    market_cap = safe_float(prof.get("market_cap"))
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
        return abs(days) <= EARNINGS_BLOCK_DAYS
    except Exception:
        return False


@lru_cache(maxsize=1024)
def get_unified_analysis(symbol: str) -> Optional[pd.DataFrame]:
    try:
        symbol = normalize_ticker(symbol)
        df = yf.Ticker(symbol).history(period="2y", auto_adjust=True)
        if df.empty:
            return None

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
        df["ATR"] = tr.rolling(14).mean()

        up_move = df["High"].diff()
        down_move = -df["Low"].diff()
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        atr14 = df["TR"].rolling(14).mean()
        plus_di = 100 * (plus_dm.rolling(14).mean() / (atr14 + 1e-9))
        minus_di = 100 * (minus_dm.rolling(14).mean() / (atr14 + 1e-9))
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di + 1e-9)) * 100
        df["ADX"] = dx.rolling(14).mean()
        df["PLUS_DI"] = plus_di
        df["MINUS_DI"] = minus_di

        delta = df["Close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(14).mean()
        avg_loss = loss.rolling(14).mean()
        rs = avg_gain / (avg_loss + 1e-9)
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

        obv = [0]
        closes = df["Close"].tolist()
        vols = df["Volume"].tolist()
        for i in range(1, len(df)):
            if closes[i] > closes[i - 1]:
                obv.append(obv[-1] + vols[i])
            elif closes[i] < closes[i - 1]:
                obv.append(obv[-1] - vols[i])
            else:
                obv.append(obv[-1])
        df["OBV"] = obv

        obv_s = pd.Series(obv, index=df.index)
        df["OBV_SMA20"] = obv_s.rolling(20).mean()
        df["OBV_Slope20"] = obv_s.diff(20)

        spy = yf.Ticker("SPY").history(period="2y", auto_adjust=True)
        if spy is not None and not spy.empty:
            spy_close = spy["Close"].reindex(df.index).ffill()
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
    except Exception:
        return None


def clear_market_cache():
    get_last_price.cache_clear()
    get_unified_analysis.cache_clear()
    get_next_earnings_date.cache_clear()
    get_symbol_profile.cache_clear()


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
    ws = get_or_create_worksheet(get_spreadsheet(), "Signals", rows=20000, cols=20)
    if not readonly:
        ensure_headers(ws, [
            "DateTime", "Ticker", "Action", "StrategyMode", "Score", "Close",
            "TargetBuyPrice", "TargetSellPrice", "StopLoss", "TrendStop",
            "TakeProfit1", "TakeProfit2", "RS20vsSPY", "ADX", "Regime", "Bucket",
            "SignalState", "Reason", "Fingerprint", "Session"
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

    # Case 1: 標準 V2
    if len(row) >= 10:
        c1 = row[1] if len(row) > 1 else ""
        c2 = row[2] if len(row) > 2 else ""
        c3 = row[3] if len(row) > 3 else ""
        c4 = row[4] if len(row) > 4 else ""
        c1_dt = pd.to_datetime(c1, errors="coerce")
        if pd.notna(c1_dt) and not _is_number_value(c2) and _is_trade_type_value(c3) and _is_number_value(c4):
            row12 = row[:12] + [""] * max(0, 12 - len(row))
            return dict(zip(TRADE_HEADERS_V2, row12))

    # Case 2: Legacy-6 / Legacy-7
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

    # Case 3: 舊 V1 fallback
    if len(row) >= 5:
        row7 = row[:7] + [""] * max(0, 7 - len(row))
        raw_dt = row7[0] if ":" in row7[0] else f"{row7[0]} 00:00:00"
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
        "Reason", "Fingerprint", "Session"
    ]
    df = read_worksheet_as_df(get_signals_worksheet(readonly=True), cols)
    if df.empty:
        return df
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    for col in ["Score", "Close", "TargetBuyPrice", "TargetSellPrice", "StopLoss", "TrendStop", "TakeProfit1", "TakeProfit2", "RS20vsSPY", "ADX"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
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

    trades_df = load_trades()
    holding_shares = get_current_holding_shares(trades_df, ticker)
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


def log_signal_snapshot(ticker, action, strategy_mode, score, details, reason, session) -> bool:
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
            session
        ]))
        clear_app_caches()
        return True
    except Exception:
        return False


# ===============================
# Market regime
# ===============================
def get_market_regime() -> Dict:
    spy = get_unified_analysis("SPY")
    qqq = get_unified_analysis("QQQ")
    vix = get_unified_analysis("^VIX")

    if spy is None or spy.empty or qqq is None or qqq.empty:
        return {
            "regime": "UNKNOWN",
            "score": 0,
            "allow_new_position": True,
            "allow_add_position": True,
            "risk_multiplier": 0.5,
            "vix": None
        }

    score = 0
    if safe_float(spy.iloc[-1]["Close"]) > safe_float(spy.iloc[-1]["SMA200"]):
        score += 1
    if safe_float(spy.iloc[-1]["SMA50"]) > safe_float(spy.iloc[-1]["SMA200"]):
        score += 1
    if safe_float(qqq.iloc[-1]["Close"]) > safe_float(qqq.iloc[-1]["SMA200"]):
        score += 1
    if safe_float(spy.iloc[-1]["MACD_Hist"]) > 0:
        score += 1

    vix_ok, vix_level = True, None
    if vix is not None and not vix.empty:
        vix_level = safe_float(vix.iloc[-1]["Close"])
        if vix_level >= 25:
            vix_ok = False
        elif vix_level < 20:
            score += 1

    if score >= 4 and vix_ok:
        return {
            "regime": "RISK_ON",
            "score": score,
            "allow_new_position": True,
            "allow_add_position": True,
            "risk_multiplier": 1.0,
            "vix": vix_level
        }
    if score <= 2:
        return {
            "regime": "RISK_OFF",
            "score": score,
            "allow_new_position": False,
            "allow_add_position": False,
            "risk_multiplier": 0.0,
            "vix": vix_level
        }
    return {
        "regime": "NEUTRAL",
        "score": score,
        "allow_new_position": False,
        "allow_add_position": True,
        "risk_multiplier": 0.5,
        "vix": vix_level
    }


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
def get_current_holding_shares(trades_df: pd.DataFrame, ticker: str) -> float:
    if trades_df.empty:
        return 0.0
    t = trades_df[trades_df["Ticker"] == normalize_ticker(ticker)]
    return max(
        0.0,
        t.loc[t["Type"] == "BUY", "Shares"].sum() - t.loc[t["Type"] == "SELL", "Shares"].sum()
    )


def build_portfolio(trades_df: pd.DataFrame, initial_capital: float) -> Tuple[List[Dict], float, float]:
    if trades_df.empty:
        return [], float(initial_capital), 0.0

    cash = float(initial_capital)
    portfolio = []
    total_realized_pl = 0.0

    for ticker in sorted(trades_df["Ticker"].dropna().unique().tolist()):
        tdf = trades_df[trades_df["Ticker"] == ticker].sort_values("TradeDateTime").copy()
        lots = []
        realized_pl = 0.0

        for _, row in tdf.iterrows():
            qty = safe_float(row["Shares"])
            pr = safe_float(row["Price"])
            fee = safe_float(row.get("Fee"))
            slip = safe_float(row.get("Slippage"))
            if qty <= 0 or pr <= 0:
                continue

            if normalize_trade_type(row["Type"]) == "BUY":
                cost = pr * qty + fee + slip
                lots.append({"shares": qty, "price": cost / qty})
                cash -= cost
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

        total_realized_pl += realized_pl
        rem_shares = sum(l["shares"] for l in lots)
        if rem_shares > 1e-9:
            last_pr = get_last_price(ticker)
            if not last_pr:
                continue
            cb = sum(l["shares"] * l["price"] for l in lots)
            mv = rem_shares * last_pr
            portfolio.append({
                "Ticker": ticker,
                "Shares": round(rem_shares, 4),
                "AvgCost": round(cb / rem_shares, 4),
                "LastPrice": round(last_pr, 4),
                "MarketValue": round(mv, 4),
                "Unrealized": round(mv - cb, 4),
                "PL_Pct": round(((last_pr / (cb / rem_shares)) - 1) * 100, 2),
                "RealizedPL": round(realized_pl, 4)
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
    portfolio
) -> Tuple[float, str, Dict, str]:
    last = hist.iloc[-1]
    close = safe_float(last["Close"])
    atr = safe_float(last["ATR"])
    sma20 = safe_float(last["SMA20"])
    rsi = safe_float(last["RSI"])

    score, meta = rank_symbol_strength(ticker, hist, market_regime)

    stop_loss = max(0.01, close - 2.0 * atr) if atr > 0 else close * 0.93
    trend_stop = safe_float(last.get("TrailingStop", stop_loss))
    tp1 = close + 2.0 * atr
    tp2 = close + 4.0 * atr

    limits = get_bucket_limits(meta.get("bucket", "LARGE_CAP"))

    risk_dollars = total_assets * limits["risk_per_trade_pct"] * safe_float(market_regime.get("risk_multiplier", 1.0))
    risk_per_share = max(0.01, close - stop_loss)
    qty = math.floor(risk_dollars / risk_per_share) if risk_per_share > 0 else 0

    action, mode, tp = "WATCH", "NONE", None

    if held_shares > 0 and close < stop_loss:
        action, mode, tp = "SELL_EXIT", "RISK_EXIT", close

    elif held_shares > 0 and close < trend_stop:
        action, mode, tp = "SELL_EXIT", "TRAIL_EXIT", trend_stop

    elif held_shares > 0 and close >= tp1 and rsi > 72:
        action, mode, tp = "SELL_PARTIAL", "TAKE_PROFIT", tp1

    elif held_shares > 0 and meta["trend_ok"] and meta["liquid_ok"] and not meta["earnings_blocked"]:
        current_weight = current_mkt_value / total_assets if total_assets > 0 else 0
        can_add = current_weight < limits["max_weight"] * SCALE_IN_MAX_WEIGHT_RATIO
        pullback_ok = close <= sma20 * 1.03
        if score >= SCORE_BUY_ADD_THRESHOLD and can_add and pullback_ok and qty > 0 and market_regime.get("allow_add_position"):
            action, mode, tp = "BUY_ADD", "SCALE_IN", close

    elif held_shares <= 0 and meta["trend_ok"] and meta["liquid_ok"] and not meta["earnings_blocked"]:
        avail_cash = cash - (total_assets * CASH_RESERVE_PCT)
        heat_ok = portfolio_heat_pct < PORTFOLIO_HEAT_LIMIT_PCT * 100
        if score >= SCORE_BUY_NOW_THRESHOLD and qty > 0 and avail_cash > close * qty and heat_ok and market_regime.get("allow_new_position"):
            action, mode, tp = "BUY_NOW", "TREND_MOMENTUM", close

    partial_sell_qty = max(1, math.floor(held_shares * 0.5)) if action == "SELL_PARTIAL" else 0

    return score, action, {
        "close": close,
        "rsi": rsi,
        "atr": atr,
        "stop_loss": stop_loss,
        "trend_stop": trend_stop,
        "take_profit_1": tp1,
        "take_profit_2": tp2,
        "suggested_buy_qty": qty,
        "suggested_sell_qty": math.ceil(held_shares) if action == "SELL_EXIT" else partial_sell_qty,
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
    }, " | ".join(meta["reasons"]) if meta["reasons"] else "No Signal"


def enrich_portfolio_with_weight_and_risk(portfolio: List[Dict], total_assets: float, cash: float, market_regime: Dict) -> List[Dict]:
    res = []
    heat = calc_portfolio_heat(portfolio, total_assets).get("heat_pct", 0)
    for p in portfolio:
        hist = get_unified_analysis(p["Ticker"])
        row = p.copy()
        row["WeightPct"] = (p["MarketValue"] / total_assets * 100) if total_assets > 0 else 0
        if hist is not None and not hist.empty:
            sc, act, det, _ = evaluate_strategy(
                p["Ticker"], hist, p["Shares"], p["MarketValue"], total_assets, cash, market_regime, heat, portfolio
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
        sc, act, det, note = evaluate_strategy(tk, h, held, mkt_val, total_assets, cash, market_regime, 0, portfolio)
        candidates.append({"ticker": tk, "score": sc, "action": act, "details": det, "note": note})

        if act != "WATCH" and should_send_alert(alerts_df, tk, act, det["close"], sc, session):
            action_emoji = "🟢" if "BUY" in act else "🔴"
            msg = f"{action_emoji} *{tk}* `{act}` | 分數: {sc:.1f}\n{note}"
            if send_telegram_msg(msg):
                log_sent_alert(tk, act, det["close"], sc, session, det.get("target_buy_price"), "")

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

US_SEMI_UNIVERSE: List[str] = [
    "NVDA", "AMD", "INTC", "QCOM", "MRVL",
    "AVGO", "ADI", "TXN", "MPWR", "MCHP", "SWKS", "QRVO", "NXPI", "ON",
    "SMTC", "RMBS", "AMBA", "SLAB", "CRUS", "MTSI", "ALGM", "AOSL",
    "LSCC", "SITM", "CRDO", "MXL", "CEVA", "POWI", "DIOD", "VSH",
    "OSIS", "PLAB", "SIMO", "WOLF", "AXTI", "AEHR", "PDFS", "VICR",
    "CLFD", "MACOM", "ARM",
    "TSM", "GFS", "UMC",
    "MU", "WDC",
    "AMAT", "LRCX", "KLAC", "TER", "ONTO", "FORM", "ICHR", "ACMR",
    "COHU", "KLIC", "ENTG", "ACLS", "AEIS", "ESIO", "NANO", "MKSI",
    "CCMP", "IPGP", "VIAV", "BRKS", "UCTT", "CAMT", "KEYS",
    "CDNS", "SNPS",
    "AMKR", "ASX",
    "LITE", "AAOI", "IIVI", "COHR",
    "SMCI",
    "MBLY", "LAZR", "INVZ", "OUST",
]


@lru_cache(maxsize=1)
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


def get_us_semi_universe(include_etf: bool = True) -> List[str]:
    base = [normalize_ticker(t) for t in US_SEMI_UNIVERSE]
    if not include_etf:
        return sorted(list(dict.fromkeys(base)))

    etf_extra: List[str] = []
    for etf in ["SOXX", "SMH"]:
        etf_extra.extend(_fetch_etf_holdings(etf))

    combined = list(dict.fromkeys(base + etf_extra))
    return sorted(combined)


US_SEMI_SCORE_STRONG = get_env_float("US_SEMI_SCORE_STRONG", 5.5)
US_SEMI_SCORE_BUY = get_env_float("US_SEMI_SCORE_BUY", 3.5)
US_SEMI_SCORE_WATCH = get_env_float("US_SEMI_SCORE_WATCH", 2.0)
US_SEMI_MIN_DOLLAR_VOL = get_env_float("US_SEMI_MIN_DOLLAR_VOL", 20_000_000)
US_SEMI_MIN_PRICE = get_env_float("US_SEMI_MIN_PRICE", 10.0)
US_SEMI_SCAN_WORKERS = get_env_int("US_SEMI_SCAN_WORKERS", 10)
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

        return {
            "ticker": ticker,
            "score": score,
            "signal": signal,
            "close": round(close, 2),
            "stop_loss": stop_loss,
            "tp1": tp1,
            "tp2": tp2,
            "suggested_qty": suggested_qty,
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

        for row in rows:
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

        new_data = [TRADE_HEADERS_V2] + migrated
        gsheet_retry(lambda: ws.clear())
        gsheet_retry(lambda: ws.update("A1", new_data))
        clear_app_caches()

        return True, f"✅ 遷移完成：{len(migrated)} 列，跳過空白 {skipped} 列"
    except Exception as e:
        return False, f"❌ 遷移失敗：{e}"


def run_us_semi_scanner(extra_tickers: Optional[List[str]] = None) -> Dict:
    sox_regime = _get_sox_regime()

    base_universe = get_us_semi_universe(include_etf=True)
    universe = list(dict.fromkeys(
        base_universe + [normalize_ticker(t) for t in (extra_tickers or [])]
    ))

    results = []
    with _cf.ThreadPoolExecutor(max_workers=US_SEMI_SCAN_WORKERS) as ex:
        futs = {ex.submit(_us_semi_score_one, tk, sox_regime): tk for tk in universe}
        for fut in _cf.as_completed(futs):
            r = fut.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: -x["score"])

    strong_buy = [r for r in results if r["signal"] == "STRONG_BUY"]
    buy = [r for r in results if r["signal"] == "BUY"]
    watch = [r for r in results if r["signal"] == "WATCH"]

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
        rank = RANK[i] if i < len(RANK) else f"{i + 1}."
        stars = "⭐" * min(5, max(1, round(r["score"] / 1.5)))
        sig_lbl = {"STRONG_BUY": "🔴 強力買進", "BUY": "🟢 積極買進", "WATCH": "🟡 留意"}.get(r["signal"], "")
        reasons = "、".join(r["reasons"][:3]) if r["reasons"] else "—"
        return [
            f"{rank} *{r['ticker']}*  {stars}  {sig_lbl}",
            f"   分數 *{r['score']:.1f}*  |  RSI {r['rsi']:.0f}  ADX {r['adx']:.0f}",
            f"   現價 ${r['close']}  |  RS vs SPY {r['rs20_vs_spy']:+.1f}%",
            f"   📈 {reasons}",
            f"   🛑 ${r['stop_loss']}  🎯 TP1 ${r['tp1']}  TP2 ${r['tp2']}",
            f"   建議股數 {r['suggested_qty']} 股  |  日均量 ${r['dv20_m']:.0f}M",
            "",
        ]

    footer = ["─────────────────────────────", "⚠️ 本訊息僅供參考，不構成投資建議"]

    MAX = 4000
    sep = "\n"
    msgs, cur = [], list(header)
    is_first = True
    all_stocks = (strong + buys + watches)[:US_SEMI_TOP_N]

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

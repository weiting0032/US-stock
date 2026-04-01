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

TRADE_HEADERS_V1 = ["Date", "Ticker", "Type", "Price", "Shares", "Total", "Note"]
TRADE_HEADERS_V2 = [
    "TradeDateTime", "CreatedAt", "Ticker", "Type", "Price", "Shares",
    "GrossTotal", "Fee", "Slippage", "NetTotal", "Note", "OrderID"
]


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
    s = str(x).strip().upper()
    if "買" in s or "BUY" in s:
        return "BUY"
    if "賣" in s or "SELL" in s:
        return "SELL"
    return s


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
        return [
            "AAPL - Apple",
            "MSFT - Microsoft",
            "NVDA - NVIDIA",
            "AMZN - Amazon",
            "TSLA - Tesla",
        ]


@lru_cache(maxsize=1024)
def get_last_price(symbol: str) -> Optional[float]:
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
        long_info = {}
        try:
            long_info = tk.info or {}
        except Exception:
            long_info = {}

        market_cap = safe_float(
            long_info.get("marketCap") or info.get("marketCap") or 0.0
        )
        sector = str(long_info.get("sector") or "").strip()
        industry = str(long_info.get("industry") or "").strip()

        return {
            "market_cap": market_cap,
            "sector": sector,
            "industry": industry,
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
        creds = json.loads(raw)
        return gspread.service_account_from_dict(creds)

    try:
        import streamlit as st

        if "gcp_service_account" in st.secrets:
            return gspread.service_account_from_dict(dict(st.secrets["gcp_service_account"]))

        if "GCP_SERVICE_ACCOUNT" in st.secrets:
            secret_val = st.secrets["GCP_SERVICE_ACCOUNT"]
            if isinstance(secret_val, dict):
                return gspread.service_account_from_dict(dict(secret_val))

            raw = str(secret_val).strip()
            if raw:
                creds = json.loads(raw)
                return gspread.service_account_from_dict(creds)
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


def get_trades_worksheet(readonly: bool = True):
    ss = get_spreadsheet()
    ws = get_or_create_worksheet(ss, "Trades", rows=10000, cols=14)
    if not readonly:
        try:
            first_row = gsheet_retry(lambda: ws.row_values(1))
            if not first_row:
                gsheet_retry(lambda: ws.append_row(TRADE_HEADERS_V2))
        except Exception:
            gsheet_retry(lambda: ws.append_row(TRADE_HEADERS_V2))
    return ws


def get_history_worksheet(readonly: bool = True):
    ss = get_spreadsheet()
    ws = get_or_create_worksheet(ss, "History", rows=8000, cols=12)
    if not readonly:
        ensure_headers(ws, [
            "Date", "TotalAssets", "Cash", "MarketValue", "RealizedPL",
            "UnrealizedPL", "TotalPL", "DailyReturnPct", "DrawdownPct",
            "BenchmarkSPY", "BenchmarkReturnPct"
        ])
    return ws


def get_alerts_worksheet(readonly: bool = True):
    ss = get_spreadsheet()
    ws = get_or_create_worksheet(ss, "Alerts", rows=12000, cols=12)
    if not readonly:
        ensure_headers(ws, [
            "DateTime", "Ticker", "Action", "BaseKey", "Price",
            "Score", "Session", "TargetPrice", "Message", "Fingerprint"
        ])
    return ws


def get_watchlist_worksheet(readonly: bool = True):
    ss = get_spreadsheet()
    ws = get_or_create_worksheet(ss, "Watchlist", rows=2000, cols=4)
    if not readonly:
        ensure_headers(ws, ["Ticker", "Enabled", "Category", "Note"])
    return ws


def get_signals_worksheet(readonly: bool = True):
    ss = get_spreadsheet()
    ws = get_or_create_worksheet(ss, "Signals", rows=20000, cols=20)
    if not readonly:
        ensure_headers(ws, [
            "DateTime", "Ticker", "Action", "StrategyMode", "Score", "Close",
            "TargetBuyPrice", "TargetSellPrice", "StopLoss", "TrendStop",
            "TakeProfit1", "TakeProfit2", "RS20vsSPY", "ADX", "Regime",
            "Bucket", "SignalState", "Reason", "Fingerprint", "Session"
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
        ws_watchlist = get_watchlist_worksheet(readonly=True)
        values = gsheet_retry(lambda: ws_watchlist.get_all_values())

        if not values or len(values) <= 1:
            return False, "Watchlist 目前為空"

        headers = values[0]
        rows = values[1:]

        ticker_idx = None
        for i, h in enumerate(headers):
            if str(h).strip().upper() == "TICKER":
                ticker_idx = i
                break

        if ticker_idx is None:
            return False, "Watchlist 表頭缺少 Ticker 欄位"

        target_row_number = None
        for idx, row in enumerate(rows, start=2):
            val = row[ticker_idx] if ticker_idx < len(row) else ""
            if normalize_ticker(val) == ticker:
                target_row_number = idx
                break

        if target_row_number is None:
            return False, f"{ticker} 不在 Watchlist 中"

        gsheet_retry(lambda: ws_watchlist.delete_rows(target_row_number))
        clear_app_caches()
        return True, f"已刪除 Watchlist：{ticker}"

    except Exception as e:
        return False, f"刪除 Watchlist 失敗：{e}"


def set_watchlist_enabled(ticker: str, enabled: bool) -> Tuple[bool, str]:
    ticker = normalize_ticker(ticker)
    if not ticker:
        return False, "Ticker 不可為空"

    try:
        ws_watchlist = get_watchlist_worksheet(readonly=True)
        values = gsheet_retry(lambda: ws_watchlist.get_all_values())

        if not values or len(values) <= 1:
            return False, "Watchlist 目前為空"

        headers = values[0]
        rows = values[1:]

        ticker_idx = None
        enabled_idx = None

        for i, h in enumerate(headers):
            hh = str(h).strip().upper()
            if hh == "TICKER":
                ticker_idx = i
            elif hh == "ENABLED":
                enabled_idx = i

        if ticker_idx is None or enabled_idx is None:
            return False, "Watchlist 缺少必要欄位"

        for idx, row in enumerate(rows, start=2):
            val = row[ticker_idx] if ticker_idx < len(row) else ""
            if normalize_ticker(val) == ticker:
                cell_row = idx
                cell_col = enabled_idx + 1
                gsheet_retry(lambda: ws_watchlist.update_cell(cell_row, cell_col, str(enabled)))
                clear_app_caches()
                return True, f"{ticker} 已{'啟用' if enabled else '停用'}"

        return False, f"{ticker} 不在 Watchlist 中"

    except Exception as e:
        return False, f"更新 Watchlist 狀態失敗：{e}"


def _load_trades_raw() -> pd.DataFrame:
    ws_trades = get_trades_worksheet(readonly=True)
    values = gsheet_retry(lambda: ws_trades.get_all_values())
    if not values:
        return pd.DataFrame(columns=TRADE_HEADERS_V2)

    headers = [str(x).strip() for x in values[0]]
    rows = values[1:]

    if not headers:
        return pd.DataFrame(columns=TRADE_HEADERS_V2)

    normalized_rows = []

    is_legacy = headers[:len(TRADE_HEADERS_V1)] == TRADE_HEADERS_V1
    is_v2 = headers[:len(TRADE_HEADERS_V2)] == TRADE_HEADERS_V2

    for row in rows:
        row = list(row)

        if is_legacy:
            row7 = row[:7] + [""] * max(0, 7 - len(row[:7]))
            row7 = row7[:7]
            date_val, ticker, trade_type, price, shares, total, note = row7
            normalized_rows.append({
                "TradeDateTime": pd.to_datetime(date_val, errors="coerce"),
                "CreatedAt": pd.to_datetime(date_val, errors="coerce"),
                "Ticker": ticker,
                "Type": trade_type,
                "Price": price,
                "Shares": shares,
                "GrossTotal": total,
                "Fee": 0.0,
                "Slippage": 0.0,
                "NetTotal": total,
                "Note": note,
                "OrderID": "",
            })
        elif is_v2:
            row12 = row[:12] + [""] * max(0, 12 - len(row))
            row12 = row12[:12]
            normalized_rows.append({
                "TradeDateTime": row12[0],
                "CreatedAt": row12[1],
                "Ticker": row12[2],
                "Type": row12[3],
                "Price": row12[4],
                "Shares": row12[5],
                "GrossTotal": row12[6],
                "Fee": row12[7],
                "Slippage": row12[8],
                "NetTotal": row12[9],
                "Note": row12[10],
                "OrderID": row12[11],
            })
        else:
            if len(row) >= 12:
                row12 = row[:12]
                normalized_rows.append({
                    "TradeDateTime": row12[0],
                    "CreatedAt": row12[1],
                    "Ticker": row12[2],
                    "Type": row12[3],
                    "Price": row12[4],
                    "Shares": row12[5],
                    "GrossTotal": row12[6],
                    "Fee": row12[7],
                    "Slippage": row12[8],
                    "NetTotal": row12[9],
                    "Note": row12[10],
                    "OrderID": row12[11],
                })
            elif len(row) >= 7:
                row7 = row[:7]
                normalized_rows.append({
                    "TradeDateTime": pd.to_datetime(row7[0], errors="coerce"),
                    "CreatedAt": pd.to_datetime(row7[0], errors="coerce"),
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
                })

    df = pd.DataFrame(normalized_rows, columns=TRADE_HEADERS_V2)
    if df.empty:
        return df

    df["Ticker"] = df["Ticker"].astype(str).apply(normalize_ticker)
    df["Type"] = df["Type"].astype(str).apply(normalize_trade_type)

    for col in ["Price", "Shares", "GrossTotal", "Fee", "Slippage", "NetTotal"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["TradeDateTime"] = pd.to_datetime(df["TradeDateTime"], errors="coerce")
    df["CreatedAt"] = pd.to_datetime(df["CreatedAt"], errors="coerce")
    df = df.dropna(subset=["TradeDateTime"]).sort_values("TradeDateTime").reset_index(drop=True)
    return df


if st:
    @st.cache_data(ttl=120, show_spinner=False)
    def load_trades() -> pd.DataFrame:
        return _load_trades_raw()
else:
    def load_trades() -> pd.DataFrame:
        return _load_trades_raw()


def _load_watchlist_raw() -> pd.DataFrame:
    ws_watchlist = get_watchlist_worksheet(readonly=True)
    cols = ["Ticker", "Enabled", "Category", "Note"]
    df = read_worksheet_as_df(ws_watchlist, cols)
    if df.empty:
        return pd.DataFrame(columns=cols)

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


def save_watchlist(
    ticker: str,
    enabled: bool = True,
    category: str = "General",
    note: str = ""
) -> Tuple[bool, str]:
    ticker = normalize_ticker(ticker)
    if not ticker:
        return False, "Ticker 不可為空"

    try:
        ws_watchlist = get_watchlist_worksheet(readonly=False)
        gsheet_retry(lambda: ws_watchlist.append_row([ticker, str(enabled), category, note]))
        clear_app_caches()
        return True, "已加入 Watchlist"
    except Exception as e:
        return False, f"加入 Watchlist 失敗：{e}"


def _load_alerts_raw() -> pd.DataFrame:
    ws_alerts = get_alerts_worksheet(readonly=True)
    cols = [
        "DateTime", "Ticker", "Action", "BaseKey", "Price",
        "Score", "Session", "TargetPrice", "Message", "Fingerprint"
    ]
    df = read_worksheet_as_df(ws_alerts, cols)
    if df.empty:
        return pd.DataFrame(columns=cols)

    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    df["Action"] = df["Action"].astype(str).str.upper().str.strip()
    df["BaseKey"] = df["BaseKey"].astype(str).str.strip()
    df["Session"] = df["Session"].astype(str).str.strip()
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
    df["TargetPrice"] = pd.to_numeric(df["TargetPrice"], errors="coerce")
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    df["Fingerprint"] = df["Fingerprint"].astype(str) if "Fingerprint" in df.columns else ""
    return df.sort_values("DateTime").reset_index(drop=True)


if st:
    @st.cache_data(ttl=120, show_spinner=False)
    def load_alerts() -> pd.DataFrame:
        return _load_alerts_raw()
else:
    def load_alerts() -> pd.DataFrame:
        return _load_alerts_raw()


def _load_history_raw() -> pd.DataFrame:
    ws_history = get_history_worksheet(readonly=True)
    cols = [
        "Date", "TotalAssets", "Cash", "MarketValue", "RealizedPL",
        "UnrealizedPL", "TotalPL", "DailyReturnPct", "DrawdownPct",
        "BenchmarkSPY", "BenchmarkReturnPct"
    ]
    df = read_worksheet_as_df(ws_history, cols)
    if df.empty:
        return pd.DataFrame(columns=cols)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    num_cols = [c for c in cols if c != "Date"]
    for col in num_cols:
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
    ws = get_signals_worksheet(readonly=True)
    cols = [
        "DateTime", "Ticker", "Action", "StrategyMode", "Score", "Close",
        "TargetBuyPrice", "TargetSellPrice", "StopLoss", "TrendStop",
        "TakeProfit1", "TakeProfit2", "RS20vsSPY", "ADX", "Regime",
        "Bucket", "SignalState", "Reason", "Fingerprint", "Session"
    ]
    df = read_worksheet_as_df(ws, cols)
    if df.empty:
        return pd.DataFrame(columns=cols)

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


def save_trade(
    trade_dt: datetime,
    ticker: str,
    trade_type: str,
    price: float,
    shares: float,
    note: str = "",
    fee: float = DEFAULT_COMMISSION,
    slippage: Optional[float] = None,
    order_id: str = "",
) -> Tuple[bool, str]:
    ticker = normalize_ticker(ticker)
    trade_type = normalize_trade_type(trade_type)

    if not ticker:
        return False, "Ticker 不可為空"
    if trade_type not in ["BUY", "SELL"]:
        return False, "交易類型錯誤"
    if price <= 0 or shares <= 0:
        return False, "價格與股數需大於 0"

    trades_df = load_trades()
    holding_shares = get_current_holding_shares(trades_df, ticker)
    if trade_type == "SELL" and shares > holding_shares + 1e-9:
        return False, f"賣出超過持股，目前持有 {holding_shares:.4f}"

    ws_trades = get_trades_worksheet(readonly=False)

    gross_total = round(price * shares, 4)
    if slippage is None:
        slippage = round(gross_total * DEFAULT_SLIPPAGE_PCT, 4)
    else:
        slippage = round(float(slippage), 4)

    fee = round(float(fee), 4)
    net_total = round(
        gross_total + fee + slippage if trade_type == "BUY"
        else gross_total - fee - slippage,
        4
    )

    try:
        headers = gsheet_retry(lambda: ws_trades.row_values(1))
        headers = [str(x).strip() for x in headers]
    except Exception:
        headers = []

    is_legacy = headers[:len(TRADE_HEADERS_V1)] == TRADE_HEADERS_V1
    is_v2 = headers[:len(TRADE_HEADERS_V2)] == TRADE_HEADERS_V2

    if is_legacy:
        total_col = round(price * shares, 4)
        gsheet_retry(lambda: ws_trades.append_row([
            trade_dt.strftime("%Y-%m-%d"),
            ticker,
            trade_type,
            float(price),
            float(shares),
            float(total_col),
            note,
        ]))
        clear_app_caches()
        return True, "交易已寫入（舊版 Trades 格式）"

    if not is_v2 and not headers:
        gsheet_retry(lambda: ws_trades.append_row(TRADE_HEADERS_V2))

    gsheet_retry(lambda: ws_trades.append_row([
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
        order_id,
    ]))

    clear_app_caches()
    return True, f"交易已寫入（滑價自動套用 0.1% = ${slippage:,.4f}）"


def maybe_log_daily_history(
    total_assets: float,
    cash: float,
    market_value: float,
    realized_pl: float,
    unrealized_pl: float,
) -> Tuple[bool, str]:
    try:
        ws_history = get_history_worksheet(readonly=False)
        hist_df = load_history()

        today_str = datetime.now().strftime("%Y-%m-%d")
        if not hist_df.empty and hist_df["Date"].dt.strftime("%Y-%m-%d").iloc[-1] == today_str:
            return False, "今日 NAV 已存在"

        total_pl = total_assets - DEFAULT_INITIAL_CAPITAL
        daily_return_pct = None
        drawdown_pct = 0.0
        benchmark_spy = get_last_price("SPY")

        if not hist_df.empty:
            prev_assets = safe_float(hist_df["TotalAssets"].iloc[-1])
            if prev_assets > 0:
                daily_return_pct = (total_assets / prev_assets - 1) * 100

            nav_series = pd.concat([hist_df["TotalAssets"], pd.Series([total_assets])], ignore_index=True)
            running_peak = nav_series.cummax()
            current_dd = nav_series.iloc[-1] / running_peak.iloc[-1] - 1 if running_peak.iloc[-1] > 0 else 0.0
            drawdown_pct = current_dd * 100

        benchmark_return_pct = None
        if not hist_df.empty and pd.notna(hist_df["BenchmarkSPY"].iloc[-1]) and benchmark_spy:
            prev_spy = safe_float(hist_df["BenchmarkSPY"].iloc[-1])
            if prev_spy > 0:
                benchmark_return_pct = (benchmark_spy / prev_spy - 1) * 100

        gsheet_retry(lambda: ws_history.append_row([
            today_str,
            float(total_assets),
            float(cash),
            float(market_value),
            float(realized_pl),
            float(unrealized_pl),
            float(total_pl),
            "" if daily_return_pct is None else float(daily_return_pct),
            float(drawdown_pct),
            "" if benchmark_spy is None else float(benchmark_spy),
            "" if benchmark_return_pct is None else float(benchmark_return_pct),
        ]))

        clear_app_caches()
        return True, "已寫入每日 NAV"
    except Exception as e:
        return False, f"寫入 NAV 失敗：{e}"

# ===============================
# Alert / Signal dedup
# ===============================
def build_alert_fingerprint(
    ticker: str,
    action: str,
    session: str,
    price: float,
    score: float,
    target_price: Optional[float]
) -> str:
    tp = round(float(target_price), 2) if target_price is not None and not pd.isna(target_price) else 0.0
    return f"{normalize_ticker(ticker)}|{action}|{session}|{round(float(price), 2)}|{round(float(score), 1)}|{tp}"


def build_signal_state(action: str, strategy_mode: str) -> str:
    return f"{str(action).upper()}::{str(strategy_mode).upper()}"


def has_same_fingerprint(alerts_df: pd.DataFrame, fingerprint: str) -> bool:
    if alerts_df.empty or "Fingerprint" not in alerts_df.columns:
        return False
    return fingerprint in alerts_df["Fingerprint"].astype(str).values


def get_last_sent_alert(alerts_df: pd.DataFrame, ticker: str, action: str) -> Optional[dict]:
    if alerts_df.empty:
        return None

    temp = alerts_df[
        (alerts_df["Ticker"].astype(str).str.upper() == normalize_ticker(ticker)) &
        (alerts_df["Action"].astype(str).str.upper() == str(action).upper())
    ]
    if temp.empty:
        return None

    last = temp.sort_values("DateTime").iloc[-1]
    return {
        "DateTime": last["DateTime"],
        "Price": safe_float(last["Price"]),
        "Score": safe_float(last["Score"]),
        "Session": str(last["Session"]),
        "TargetPrice": safe_float(last["TargetPrice"]),
        "Fingerprint": str(last.get("Fingerprint", "")),
    }


def get_last_signal_state(signals_df: pd.DataFrame, ticker: str) -> Optional[str]:
    if signals_df.empty:
        return None
    temp = signals_df[signals_df["Ticker"].astype(str).str.upper() == normalize_ticker(ticker)]
    if temp.empty:
        return None
    last = temp.sort_values("DateTime").iloc[-1]
    return str(last.get("SignalState", "")).strip() or None


def should_send_alert(
    alerts_df: pd.DataFrame,
    ticker: str,
    action: str,
    current_price: float,
    current_score: float,
    current_session: str,
    target_price: Optional[float] = None,
    state_changed: bool = False,
) -> bool:
    fingerprint = build_alert_fingerprint(
        ticker=ticker,
        action=action,
        session=current_session,
        price=current_price,
        score=current_score,
        target_price=target_price
    )

    if has_same_fingerprint(alerts_df, fingerprint):
        return False

    last_alert = get_last_sent_alert(alerts_df, ticker, action)
    if last_alert is None:
        return True

    last_dt = last_alert["DateTime"]
    if pd.isna(last_dt):
        return True

    # 若訊號狀態有切換，優先允許發送
    if state_changed:
        return True

    if current_session != last_alert["Session"]:
        return True

    minutes_diff = (datetime.now() - last_dt.to_pydatetime()).total_seconds() / 60.0
    last_price = safe_float(last_alert["Price"])
    price_change_pct = 0.0 if last_price <= 0 else abs((current_price - last_price) / last_price) * 100
    score_change = abs(current_score - safe_float(last_alert["Score"]))

    last_target = safe_float(last_alert["TargetPrice"])
    target_change = 0.0
    if target_price and last_target > 0:
        target_change = abs((target_price - last_target) / last_target) * 100

    if minutes_diff >= ALERT_MIN_MINUTES and (
        price_change_pct >= ALERT_MIN_PRICE_CHANGE or
        score_change >= ALERT_MIN_SCORE_CHANGE or
        target_change >= 1.0
    ):
        return True

    return False


def log_sent_alert(
    ticker: str,
    action: str,
    price: float,
    score: float,
    session: str,
    target_price: Optional[float],
    message: str
) -> bool:
    try:
        ws_alerts = get_alerts_worksheet(readonly=False)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_key = f"{normalize_ticker(ticker)}_{str(action).upper()}"
        fingerprint = build_alert_fingerprint(ticker, action, session, price, score, target_price)

        gsheet_retry(lambda: ws_alerts.append_row([
            now_str,
            normalize_ticker(ticker),
            str(action).upper().strip(),
            base_key,
            float(price),
            float(score),
            session,
            float(target_price) if target_price else "",
            message,
            fingerprint
        ]))
        clear_app_caches()
        return True
    except Exception:
        return False


def log_signal_snapshot(
    ticker: str,
    action: str,
    strategy_mode: str,
    score: float,
    details: Dict,
    reason: str,
    session: str,
) -> bool:
    try:
        ws = get_signals_worksheet(readonly=False)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        signal_state = build_signal_state(action, strategy_mode)
        fingerprint = build_alert_fingerprint(
            ticker=ticker,
            action=action,
            session=session,
            price=safe_float(details.get("close", 0.0)),
            score=score,
            target_price=details.get("target_buy_price") or details.get("target_sell_price"),
        )

        gsheet_retry(lambda: ws.append_row([
            now_str,
            normalize_ticker(ticker),
            str(action).upper(),
            str(strategy_mode).upper(),
            float(score),
            float(safe_float(details.get("close"))),
            "" if details.get("target_buy_price") is None else float(details.get("target_buy_price")),
            "" if details.get("target_sell_price") is None else float(details.get("target_sell_price")),
            "" if details.get("stop_loss") is None else float(details.get("stop_loss")),
            "" if details.get("trend_stop") is None else float(details.get("trend_stop")),
            "" if details.get("take_profit_1") is None else float(details.get("take_profit_1")),
            "" if details.get("take_profit_2") is None else float(details.get("take_profit_2")),
            "" if details.get("rs20_vs_spy") is None else float(details.get("rs20_vs_spy")),
            "" if details.get("adx") is None else float(details.get("adx")),
            str(details.get("market_regime", "")),
            str(details.get("bucket", "")),
            signal_state,
            str(reason),
            fingerprint,
            str(session),
        ]))
        clear_app_caches()
        return True
    except Exception:
        return False


# ===============================
# Market regime / filters
# ===============================
def get_market_regime() -> Dict:
    spy = get_unified_analysis("SPY")
    qqq = get_unified_analysis("QQQ")
    iwm = get_unified_analysis("IWM")
    smh = get_unified_analysis("SMH")
    vix = get_unified_analysis("^VIX")

    if spy is None or spy.empty or qqq is None or qqq.empty:
        return {
            "regime": "UNKNOWN",
            "score": 0,
            "allow_new_position": True,
            "allow_add_position": True,
            "risk_multiplier": 0.5,
            "vix": None,
        }

    score = 0
    breadth_score = 0

    spy_last = spy.iloc[-1]
    qqq_last = qqq.iloc[-1]

    if safe_float(spy_last["Close"]) > safe_float(spy_last["SMA200"]):
        score += 1
    if safe_float(spy_last["SMA50"]) > safe_float(spy_last["SMA200"]):
        score += 1
    if safe_float(qqq_last["Close"]) > safe_float(qqq_last["SMA200"]):
        score += 1
    if safe_float(spy_last["MACD_Hist"]) > 0:
        score += 1

    if iwm is not None and not iwm.empty:
        if safe_float(iwm.iloc[-1]["Close"]) > safe_float(iwm.iloc[-1]["SMA200"]):
            breadth_score += 1

    if smh is not None and not smh.empty:
        if safe_float(smh.iloc[-1]["Close"]) > safe_float(smh.iloc[-1]["SMA200"]):
            breadth_score += 1

    score += breadth_score

    vix_ok = True
    vix_level = None
    if vix is not None and not vix.empty:
        vix_level = safe_float(vix.iloc[-1]["Close"])
        if vix_level >= 25:
            vix_ok = False
        elif vix_level < 20:
            score += 1

    if score >= 5 and vix_ok:
        return {
            "regime": "RISK_ON",
            "score": score,
            "allow_new_position": True,
            "allow_add_position": True,
            "risk_multiplier": 1.0,
            "vix": vix_level,
        }

    if score <= 2:
        return {
            "regime": "RISK_OFF",
            "score": score,
            "allow_new_position": False,
            "allow_add_position": False,
            "risk_multiplier": 0.0,
            "vix": vix_level,
        }

    return {
        "regime": "NEUTRAL",
        "score": score,
        "allow_new_position": False,
        "allow_add_position": True,
        "risk_multiplier": 0.5,
        "vix": vix_level,
    }


def detect_volume_price_divergence(hist: pd.DataFrame) -> str:
    if hist is None or hist.empty or len(hist) < 40:
        return "NONE"

    recent = hist.tail(20)
    prev = hist.tail(40).head(20)

    if recent["Close"].max() > prev["Close"].max() and recent["OBV"].iloc[-1] < prev["OBV"].iloc[-1]:
        return "BEARISH"
    if recent["Close"].min() < prev["Close"].min() and recent["OBV"].iloc[-1] > prev["OBV"].iloc[-1]:
        return "BULLISH"
    return "NONE"


def passes_liquidity_filter(hist: pd.DataFrame, bucket: str = "LARGE_CAP") -> bool:
    if hist is None or hist.empty:
        return False

    last = hist.iloc[-1]
    close = safe_float(last["Close"])
    dv20 = safe_float(last["DollarVolume20"])

    if bucket == "SMALL_CAP":
        return close >= 5 and dv20 >= max(5_000_000, MIN_AVG_DOLLAR_VOLUME * 0.25)

    return close >= MIN_PRICE and dv20 >= MIN_AVG_DOLLAR_VOLUME


def calc_relative_strength_score(hist: pd.DataFrame) -> Tuple[float, Dict]:
    if hist is None or hist.empty:
        return 0.0, {"rs20_vs_spy": None, "rs_line_slope20": None}

    last = hist.iloc[-1]
    rs20 = safe_float(last.get("RS20_vs_SPY", 0.0), 0.0)
    rs_slope = safe_float(last.get("RS_Line_Slope20", 0.0), 0.0)

    score = 0.0
    if rs20 > 0:
        score += 0.8
    if rs20 > 5:
        score += 0.6
    if rs_slope > 0:
        score += 0.4

    return score, {
        "rs20_vs_spy": rs20,
        "rs_line_slope20": rs_slope,
    }


def calc_52w_high_score(hist: pd.DataFrame) -> Tuple[float, Dict]:
    if hist is None or hist.empty:
        return 0.0, {"distance_to_52w_high_pct": None}

    last = hist.iloc[-1]
    close = safe_float(last["Close"])
    high_252 = safe_float(last.get("RollingHigh252", 0.0))
    if close <= 0 or high_252 <= 0:
        return 0.0, {"distance_to_52w_high_pct": None}

    dist = (high_252 - close) / high_252
    score = 0.0
    if dist <= NEAR_52W_HIGH_PCT:
        score += 0.8
    if dist <= 0.05:
        score += 0.6

    return score, {"distance_to_52w_high_pct": dist * 100}


def rank_symbol_strength(
    ticker: str,
    hist: pd.DataFrame,
    market_regime: Optional[Dict] = None,
) -> Tuple[float, Dict]:
    if hist is None or hist.empty:
        return 0.0, {}

    last = hist.iloc[-1]
    prev = hist.iloc[-2] if len(hist) >= 2 else last

    close = safe_float(last["Close"])
    sma20 = safe_float(last["SMA20"])
    sma50 = safe_float(last["SMA50"])
    sma200 = safe_float(last["SMA200"])
    rsi = safe_float(last["RSI"])
    macd_hist = safe_float(last["MACD_Hist"])
    prev_macd_hist = safe_float(prev["MACD_Hist"])
    volume = safe_float(last["Volume"])
    vol_sma20 = safe_float(last["VOL_SMA20"])
    adx = safe_float(last.get("ADX", 0.0))
    plus_di = safe_float(last.get("PLUS_DI", 0.0))
    minus_di = safe_float(last.get("MINUS_DI", 0.0))

    bucket = classify_symbol_bucket(ticker, hist)
    divergence = detect_volume_price_divergence(hist)
    liquid_ok = passes_liquidity_filter(hist, bucket=bucket)
    earnings_blocked = is_earnings_blocked(ticker)

    reasons = []

    trend_score = 0.0
    momentum_score = 0.0
    volume_score = 0.0
    regime_score = 0.0
    risk_score = 0.0

    trend_ok = close > sma200 and sma50 > sma200
    strong_trend = trend_ok and sma20 > sma50
    breakout_20 = close > safe_float(hist["RollingHigh20"].shift(1).iloc[-1], 0)
    breakout_55 = close > safe_float(hist["RollingHigh55"].shift(1).iloc[-1], 0)
    breakout_volume = volume > vol_sma20 * 1.5 if vol_sma20 > 0 else False

    if trend_ok:
        trend_score += 1.4
        reasons.append("長線趨勢成立")
    if strong_trend:
        trend_score += 1.0
        reasons.append("SMA20 > SMA50 > SMA200")
    if close > sma20:
        trend_score += 0.5

    if macd_hist > 0:
        momentum_score += 0.8
        reasons.append("MACD Hist > 0")
    if macd_hist >= prev_macd_hist:
        momentum_score += 0.4
    if 45 <= rsi <= 70:
        momentum_score += 0.8
        reasons.append("RSI 健康區")
    elif 40 <= rsi < 45:
        momentum_score += 0.4

    if breakout_20:
        volume_score += 0.5
        reasons.append("突破 20 日高點")
    if breakout_55:
        volume_score += 1.0
        reasons.append("突破 55 日高點")
    if breakout_volume:
        volume_score += 1.0
        reasons.append("放量突破")
    elif volume > vol_sma20:
        volume_score += 0.4

    if adx >= BREAKOUT_ADX_MIN:
        volume_score += 0.8
        reasons.append("ADX 趨勢強")
    if plus_di > minus_di:
        volume_score += 0.3

    rs_score, rs_meta = calc_relative_strength_score(hist)
    high52_score, high52_meta = calc_52w_high_score(hist)

    if rs_score > 0:
        reasons.append("相對 SPY 強勢")
    if high52_score > 0:
        reasons.append("接近 52 週高點")

    regime_name = market_regime.get("regime", "UNKNOWN") if market_regime else "UNKNOWN"
    if regime_name == "RISK_ON":
        regime_score += 0.8
        reasons.append("市場偏多")
    elif regime_name == "NEUTRAL":
        regime_score += 0.2
    elif regime_name == "RISK_OFF":
        regime_score -= 1.2
        reasons.append("市場偏空")

    if divergence == "BULLISH":
        volume_score += 0.4
        reasons.append("多方量價背離")
    elif divergence == "BEARISH":
        risk_score -= 0.8
        reasons.append("空方量價背離")

    if not liquid_ok:
        risk_score -= 2.0
        reasons.append("流動性不足")
    if earnings_blocked:
        risk_score -= 1.2
        reasons.append("財報事件風險")

    score = trend_score + momentum_score + volume_score + regime_score + risk_score + rs_score + high52_score

    meta = {
        "bucket": bucket,
        "trend_ok": trend_ok,
        "strong_trend": strong_trend,
        "breakout_20": breakout_20,
        "breakout_55": breakout_55,
        "breakout_volume": breakout_volume,
        "adx": adx,
        "plus_di": plus_di,
        "minus_di": minus_di,
        "divergence": divergence,
        "liquid_ok": liquid_ok,
        "earnings_blocked": earnings_blocked,
        "trend_score": trend_score,
        "momentum_score": momentum_score,
        "volume_score": volume_score,
        "regime_score": regime_score,
        "risk_score": risk_score,
        "rs_score": rs_score,
        "high52_score": high52_score,
        "reasons": reasons,
        **rs_meta,
        **high52_meta,
    }
    return score, meta


# ===============================
# FIFO Portfolio
# ===============================
def get_current_holding_shares(trades_df: pd.DataFrame, ticker: str) -> float:
    ticker = normalize_ticker(ticker)
    if trades_df.empty:
        return 0.0
    temp = trades_df[trades_df["Ticker"] == ticker]
    buy_shares = temp.loc[temp["Type"] == "BUY", "Shares"].sum()
    sell_shares = temp.loc[temp["Type"] == "SELL", "Shares"].sum()
    return max(0.0, buy_shares - sell_shares)


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
            trade_type = normalize_trade_type(row["Type"])
            price = safe_float(row["Price"])
            shares = safe_float(row["Shares"])
            fee = safe_float(row.get("Fee", 0))
            slippage = safe_float(row.get("Slippage", 0))
            gross_total = price * shares

            if shares <= 0 or price <= 0:
                continue

            if trade_type == "BUY":
                actual_cost = gross_total + fee + slippage
                lots.append({
                    "shares": shares,
                    "price": actual_cost / shares,
                    "datetime": row["TradeDateTime"]
                })
                cash -= actual_cost

            elif trade_type == "SELL":
                actual_proceeds = gross_total - fee - slippage
                cash += actual_proceeds
                sell_qty = shares

                while sell_qty > 0 and lots:
                    first_lot = lots[0]
                    lot_shares = safe_float(first_lot["shares"])
                    lot_price = safe_float(first_lot["price"])

                    matched_qty = min(sell_qty, lot_shares)
                    realized_pl += ((actual_proceeds / shares) - lot_price) * matched_qty

                    first_lot["shares"] = lot_shares - matched_qty
                    sell_qty -= matched_qty

                    if first_lot["shares"] <= 1e-9:
                        lots.pop(0)

        total_realized_pl += realized_pl
        remaining_shares = sum(lot["shares"] for lot in lots)

        if remaining_shares > 1e-9:
            last_price = get_last_price(ticker)
            if last_price is None:
                continue

            fifo_cost_basis = sum(lot["shares"] * lot["price"] for lot in lots)
            avg_cost_val = fifo_cost_basis / remaining_shares
            market_value = remaining_shares * last_price
            unrealized = market_value - fifo_cost_basis
            pl_pct = ((last_price / avg_cost_val) - 1) * 100 if avg_cost_val > 0 else 0.0

            profile = get_symbol_profile(ticker)

            portfolio.append({
                "Ticker": ticker,
                "Shares": round(remaining_shares, 4),
                "AvgCost": round(avg_cost_val, 4),
                "FIFOCostBasis": round(fifo_cost_basis, 4),
                "LastPrice": round(last_price, 4),
                "MarketValue": round(market_value, 4),
                "Unrealized": round(unrealized, 4),
                "PL_Pct": round(pl_pct, 2),
                "RealizedPL": round(realized_pl, 4),
                "Sector": profile.get("sector", ""),
                "Industry": profile.get("industry", ""),
            })

    return portfolio, cash, total_realized_pl


# ===============================
# Portfolio risk / heat / caps
# ===============================
def get_bucket_limits(bucket: str) -> Dict:
    if bucket == "SMALL_CAP":
        return {
            "max_weight": SMALL_CAP_MAX_WEIGHT,
            "risk_per_trade_pct": SMALL_CAP_RISK_PER_TRADE_PCT,
        }
    return {
        "max_weight": LARGE_CAP_MAX_WEIGHT,
        "risk_per_trade_pct": LARGE_CAP_RISK_PER_TRADE_PCT,
    }


def calc_portfolio_heat(portfolio: List[Dict], total_assets: float) -> Dict:
    if not portfolio or total_assets <= 0:
        return {
            "heat_amount": 0.0,
            "heat_pct": 0.0,
            "sector_exposure": {},
        }

    heat_amount = 0.0
    sector_exposure = {}

    for item in portfolio:
        shares = safe_float(item.get("Shares", 0.0))
        last_price = safe_float(item.get("LastPrice", 0.0))
        stop_loss = safe_float(item.get("StopLoss", 0.0))
        sector = str(item.get("Sector", "")).strip() or "UNKNOWN"

        if shares > 0 and last_price > 0 and stop_loss > 0 and last_price > stop_loss:
            heat_amount += (last_price - stop_loss) * shares

        sector_exposure[sector] = sector_exposure.get(sector, 0.0) + safe_float(item.get("MarketValue", 0.0))

    heat_pct = heat_amount / total_assets if total_assets > 0 else 0.0

    sector_exposure_pct = {
        k: (v / total_assets * 100 if total_assets > 0 else 0.0)
        for k, v in sector_exposure.items()
    }

    return {
        "heat_amount": round(heat_amount, 2),
        "heat_pct": round(heat_pct * 100, 2),
        "sector_exposure": sector_exposure_pct,
    }


def sector_position_limit_ok(
    portfolio: List[Dict],
    ticker: str,
    additional_value: float,
    total_assets: float,
    sector_cap_pct: float = 35.0,
) -> bool:
    profile = get_symbol_profile(ticker)
    target_sector = str(profile.get("sector", "")).strip() or "UNKNOWN"

    current_sector_value = 0.0
    for item in portfolio:
        sector = str(item.get("Sector", "")).strip() or "UNKNOWN"
        if sector == target_sector:
            current_sector_value += safe_float(item.get("MarketValue", 0.0))

    after_pct = ((current_sector_value + additional_value) / total_assets * 100) if total_assets > 0 else 0.0
    return after_pct <= sector_cap_pct


# ===============================
# Strategy engine
# ===============================
def calc_position_size(
    total_assets: float,
    cash: float,
    current_mkt_value: float,
    entry_price: float,
    stop_price: float,
    market_regime: Optional[Dict] = None,
    bucket: str = "LARGE_CAP",
    current_heat_pct: float = 0.0,
) -> Dict:
    if entry_price <= 0 or stop_price <= 0 or entry_price <= stop_price:
        return {"qty_by_risk": 0, "qty_by_weight": 0, "qty_by_cash": 0, "qty_by_heat": 0, "final_qty": 0}

    regime_mult = 1.0
    if market_regime:
        regime_mult = safe_float(market_regime.get("risk_multiplier", 1.0), 1.0)

    limits = get_bucket_limits(bucket)
    risk_per_trade_pct = safe_float(limits["risk_per_trade_pct"], RISK_PER_TRADE_PCT)
    max_weight = safe_float(limits["max_weight"], MAX_POSITION_WEIGHT)

    risk_budget = total_assets * risk_per_trade_pct * regime_mult
    risk_per_share = max(0.01, entry_price - stop_price)
    qty_by_risk = math.floor(risk_budget / risk_per_share)

    max_position_value = total_assets * max_weight
    remaining_position_value = max(0.0, max_position_value - current_mkt_value)
    qty_by_weight = math.floor(remaining_position_value / entry_price)

    usable_cash = max(0.0, cash - total_assets * CASH_RESERVE_PCT)
    qty_by_cash = math.floor(usable_cash / entry_price)

    # portfolio heat 控制
    heat_limit_amount = max(0.0, total_assets * PORTFOLIO_HEAT_LIMIT_PCT - total_assets * (current_heat_pct / 100.0))
    qty_by_heat = math.floor(heat_limit_amount / risk_per_share) if risk_per_share > 0 else 0
    if heat_limit_amount <= 0:
        qty_by_heat = 0

    final_qty = max(0, min(qty_by_risk, qty_by_weight, qty_by_cash, qty_by_heat))
    return {
        "qty_by_risk": qty_by_risk,
        "qty_by_weight": qty_by_weight,
        "qty_by_cash": qty_by_cash,
        "qty_by_heat": qty_by_heat,
        "final_qty": final_qty,
    }


def evaluate_strategy(
    ticker: str,
    hist: pd.DataFrame,
    held_shares: float,
    current_mkt_value: float,
    total_assets: float,
    cash: float,
    market_regime: Optional[Dict] = None,
    portfolio_heat_pct: float = 0.0,
    portfolio: Optional[List[Dict]] = None,
) -> Tuple[float, str, Dict, str]:
    last = hist.iloc[-1]
    prev = hist.iloc[-2] if len(hist) >= 2 else last

    close = safe_float(last["Close"])
    sma20 = safe_float(last["SMA20"])
    sma50 = safe_float(last["SMA50"])
    sma200 = safe_float(last["SMA200"])
    rsi = safe_float(last["RSI"])
    atr = safe_float(last["ATR"])
    bb_upper = safe_float(last["BB_upper"])
    bb_lower = safe_float(last["BB_lower"])
    macd_hist = safe_float(last["MACD_Hist"])
    prev_macd_hist = safe_float(prev["MACD_Hist"])
    volume = safe_float(last["Volume"])
    vol_sma20 = safe_float(last["VOL_SMA20"])
    trailing_stop = safe_float(last["TrailingStop"])
    rolling_high20_prev = safe_float(hist["RollingHigh20"].shift(1).iloc[-1], 0)
    rolling_high55_prev = safe_float(hist["RollingHigh55"].shift(1).iloc[-1], 0)
    adx = safe_float(last.get("ADX", 0.0))
    rs20_vs_spy = safe_float(last.get("RS20_vs_SPY", 0.0), 0.0)
    dollar_volume20 = safe_float(last["DollarVolume20"])

    strength_score, strength_meta = rank_symbol_strength(
        ticker=ticker,
        hist=hist,
        market_regime=market_regime,
    )

    bucket = strength_meta.get("bucket", "LARGE_CAP")
    limits = get_bucket_limits(bucket)
    max_weight_for_bucket = safe_float(limits["max_weight"], MAX_POSITION_WEIGHT)

    divergence = strength_meta.get("divergence", "NONE")
    regime_name = market_regime.get("regime", "UNKNOWN") if market_regime else "UNKNOWN"
    allow_new_position = market_regime.get("allow_new_position", True) if market_regime else True
    allow_add_position = market_regime.get("allow_add_position", True) if market_regime else True

    reasons = list(strength_meta.get("reasons", []))

    current_weight = current_mkt_value / total_assets if total_assets > 0 else 0.0
    strong_trend = bool(strength_meta.get("strong_trend"))
    trend_ok = bool(strength_meta.get("trend_ok"))
    breakout_20 = bool(strength_meta.get("breakout_20"))
    breakout_55 = bool(strength_meta.get("breakout_55"))
    breakout_volume = bool(strength_meta.get("breakout_volume"))
    liquid_ok = bool(strength_meta.get("liquid_ok"))
    earnings_blocked = bool(strength_meta.get("earnings_blocked"))

    near_sma20 = abs(close - sma20) / close <= 0.03 if close > 0 and sma20 > 0 else False
    shallow_pullback = close <= sma20 and close >= (sma20 - atr) if atr > 0 and sma20 > 0 else False
    deep_pullback = close <= sma50 and close >= (sma50 - 1.2 * atr) if atr > 0 and sma50 > 0 else False
    adx_ok_for_breakout = adx >= BREAKOUT_ADX_MIN

    # 初始 / 趨勢停損
    stop_loss = max(0.01, close - 2 * atr) if atr > 0 else max(0.01, close * 0.93)
    trend_stop = trailing_stop if trailing_stop > 0 else stop_loss

    pullback_entry = sma20 if sma20 > 0 else close
    deep_pullback_entry = max(sma50, sma20 - atr) if sma50 > 0 and atr > 0 else pullback_entry
    breakout_entry = max(rolling_high20_prev, close)
    donchian_entry = max(rolling_high55_prev, close)

    take_profit_1 = close + 2 * atr if atr > 0 else close * 1.08
    take_profit_2 = close + 4 * atr if atr > 0 else close * 1.15

    pullback_sizing = calc_position_size(
        total_assets=total_assets,
        cash=cash,
        current_mkt_value=current_mkt_value,
        entry_price=pullback_entry,
        stop_price=max(0.01, pullback_entry - 2 * atr) if atr > 0 else stop_loss,
        market_regime=market_regime,
        bucket=bucket,
        current_heat_pct=portfolio_heat_pct,
    )

    deep_pullback_sizing = calc_position_size(
        total_assets=total_assets,
        cash=cash,
        current_mkt_value=current_mkt_value,
        entry_price=deep_pullback_entry,
        stop_price=max(0.01, deep_pullback_entry - 1.8 * atr) if atr > 0 else stop_loss,
        market_regime=market_regime,
        bucket=bucket,
        current_heat_pct=portfolio_heat_pct,
    )

    breakout_sizing = calc_position_size(
        total_assets=total_assets,
        cash=cash,
        current_mkt_value=current_mkt_value,
        entry_price=max(close, breakout_entry),
        stop_price=max(0.01, close - 1.5 * atr) if atr > 0 else stop_loss,
        market_regime=market_regime,
        bucket=bucket,
        current_heat_pct=portfolio_heat_pct,
    )

    action = "WATCH"
    strategy_mode = "NONE"
    target_buy_price = None
    target_sell_price = None

    can_open_new = allow_new_position and held_shares <= 0
    can_add = allow_add_position and held_shares > 0 and current_weight < max_weight_for_bucket

    sector_ok = True
    if portfolio is not None:
        estimated_buy_value = max(
            pullback_sizing["final_qty"] * max(pullback_entry, 0),
            breakout_sizing["final_qty"] * max(close, 0),
            deep_pullback_sizing["final_qty"] * max(deep_pullback_entry, 0),
        )
        sector_ok = sector_position_limit_ok(
            portfolio=portfolio,
            ticker=ticker,
            additional_value=estimated_buy_value,
            total_assets=total_assets,
            sector_cap_pct=35.0,
        )

    if not sector_ok:
        reasons.append("產業曝險過高")

    # ===== 出場邏輯 =====
    hard_stop_trigger = held_shares > 0 and close < stop_loss
    trail_exit_trigger = held_shares > 0 and trend_stop > 0 and close < trend_stop
    trend_weaken_trigger = held_shares > 0 and close < sma50 and macd_hist < 0
    partial_take_profit_trigger = held_shares > 0 and (
        (rsi >= 78 and close > bb_upper) or
        (close > take_profit_1 and macd_hist < prev_macd_hist)
    )

    if hard_stop_trigger:
        action = "SELL_EXIT"
        strategy_mode = "RISK_EXIT"
        target_sell_price = close
    elif trail_exit_trigger:
        action = "SELL_EXIT"
        strategy_mode = "TRAIL_EXIT"
        target_sell_price = trend_stop
    elif trend_weaken_trigger:
        action = "SELL_PARTIAL"
        strategy_mode = "TREND_WEAKEN"
        target_sell_price = sma20 if sma20 > 0 else close
    elif partial_take_profit_trigger:
        action = "SELL_PARTIAL"
        strategy_mode = "TAKE_PROFIT"
        target_sell_price = max(close, bb_upper) if bb_upper > 0 else close
    else:
        # ===== 買入邏輯 =====
        if (
            can_open_new and
            liquid_ok and
            not earnings_blocked and
            sector_ok and
            strong_trend and
            shallow_pullback and
            42 <= rsi <= 60 and
            rs20_vs_spy >= 0 and
            pullback_sizing["final_qty"] >= 1
        ):
            action = "BUY_NOW"
            strategy_mode = "TREND_PULLBACK"
            target_buy_price = pullback_entry

        elif (
            can_add and
            liquid_ok and
            not earnings_blocked and
            sector_ok and
            strong_trend and
            shallow_pullback and
            42 <= rsi <= 60 and
            rs20_vs_spy >= 0 and
            pullback_sizing["final_qty"] >= 1
        ):
            action = "BUY_NOW"
            strategy_mode = "ADD_PULLBACK"
            target_buy_price = pullback_entry

        elif (
            (can_open_new or can_add) and
            liquid_ok and
            not earnings_blocked and
            sector_ok and
            trend_ok and
            breakout_20 and
            breakout_volume and
            adx_ok_for_breakout and
            rs20_vs_spy > 0 and
            rsi < 75 and
            breakout_sizing["final_qty"] >= 1
        ):
            action = "BUY_NOW"
            strategy_mode = "BREAKOUT_20"
            target_buy_price = close

        elif (
            (can_open_new or can_add) and
            liquid_ok and
            not earnings_blocked and
            sector_ok and
            trend_ok and
            breakout_55 and
            breakout_volume and
            adx_ok_for_breakout and
            rs20_vs_spy > 0 and
            rsi < 78 and
            breakout_sizing["final_qty"] >= 1
        ):
            action = "BUY_NOW"
            strategy_mode = "BREAKOUT_55"
            target_buy_price = max(close, donchian_entry)

        elif (
            can_open_new and
            bucket == "SMALL_CAP" and
            liquid_ok and
            not earnings_blocked and
            sector_ok and
            trend_ok and
            deep_pullback and
            rs20_vs_spy >= 0 and
            deep_pullback_sizing["final_qty"] >= 1
        ):
            action = "BUY_NOW"
            strategy_mode = "SMALLCAP_DEEP_PULLBACK"
            target_buy_price = deep_pullback_entry

        elif (
            (can_open_new or can_add) and
            liquid_ok and
            not earnings_blocked and
            strong_trend and
            near_sma20 and
            calc_target_zone_hit(close, pullback_entry)
        ):
            action = "BUY_PULLBACK"
            strategy_mode = "PULLBACK_READY"
            target_buy_price = pullback_entry

        elif held_shares > 0 and target_sell_price is None:
            sell_ready_zone = bb_upper if bb_upper > 0 else take_profit_1
            if calc_target_zone_hit(close, sell_ready_zone):
                action = "SELL_READY"
                strategy_mode = "REDUCE_READY"
                target_sell_price = sell_ready_zone

    # 建議數量
    if action == "SELL_EXIT":
        suggested_sell_qty = math.ceil(held_shares)
    elif action == "SELL_PARTIAL":
        if strategy_mode == "TAKE_PROFIT":
            suggested_sell_qty = math.ceil(held_shares * 0.25)
        elif strategy_mode == "TREND_WEAKEN":
            suggested_sell_qty = math.ceil(held_shares * 0.5)
        else:
            suggested_sell_qty = math.ceil(held_shares * 0.33)
    elif action == "SELL_READY":
        suggested_sell_qty = math.ceil(held_shares * 0.25)
    else:
        suggested_sell_qty = 0

    suggested_buy_qty = 0
    if strategy_mode in ["TREND_PULLBACK", "ADD_PULLBACK", "PULLBACK_READY"]:
        suggested_buy_qty = pullback_sizing["final_qty"]
    elif strategy_mode in ["BREAKOUT_20", "BREAKOUT_55"]:
        suggested_buy_qty = breakout_sizing["final_qty"]
    elif strategy_mode == "SMALLCAP_DEEP_PULLBACK":
        suggested_buy_qty = deep_pullback_sizing["final_qty"]

    total_score = strength_score

    details = {
        "close": close,
        "rsi": rsi,
        "atr": atr,
        "adx": adx,
        "current_weight": current_weight,
        "stop_loss": stop_loss,
        "trend_stop": trend_stop,
        "take_profit_1": take_profit_1,
        "take_profit_2": take_profit_2,
        "suggested_buy_qty": suggested_buy_qty,
        "suggested_sell_qty": suggested_sell_qty,
        "target_buy_price": target_buy_price,
        "target_sell_price": target_sell_price,
        "pullback_entry": pullback_entry,
        "deep_pullback_entry": deep_pullback_entry,
        "breakout_entry": breakout_entry,
        "donchian_entry": donchian_entry,
        "divergence": divergence,
        "market_regime": regime_name,
        "strategy_mode": strategy_mode,
        "trend_score": strength_meta.get("trend_score"),
        "momentum_score": strength_meta.get("momentum_score"),
        "pullback_score": 0.0,
        "volume_score": strength_meta.get("volume_score"),
        "regime_score": strength_meta.get("regime_score"),
        "risk_score": strength_meta.get("risk_score"),
        "liquid_ok": liquid_ok,
        "earnings_blocked": earnings_blocked,
        "dollar_volume20": dollar_volume20,
        "bucket": bucket,
        "rs20_vs_spy": strength_meta.get("rs20_vs_spy"),
        "rs_line_slope20": strength_meta.get("rs_line_slope20"),
        "distance_to_52w_high_pct": strength_meta.get("distance_to_52w_high_pct"),
        "pullback_qty_by_risk": pullback_sizing["qty_by_risk"],
        "pullback_qty_by_weight": pullback_sizing["qty_by_weight"],
        "pullback_qty_by_cash": pullback_sizing["qty_by_cash"],
        "pullback_qty_by_heat": pullback_sizing["qty_by_heat"],
        "breakout_qty_by_risk": breakout_sizing["qty_by_risk"],
        "breakout_qty_by_weight": breakout_sizing["qty_by_weight"],
        "breakout_qty_by_cash": breakout_sizing["qty_by_cash"],
        "breakout_qty_by_heat": breakout_sizing["qty_by_heat"],
        "deep_pullback_qty_by_risk": deep_pullback_sizing["qty_by_risk"],
        "deep_pullback_qty_by_weight": deep_pullback_sizing["qty_by_weight"],
        "deep_pullback_qty_by_cash": deep_pullback_sizing["qty_by_cash"],
        "deep_pullback_qty_by_heat": deep_pullback_sizing["qty_by_heat"],
        "reasons": reasons,
        "allow_new_position": allow_new_position,
        "allow_add_position": allow_add_position,
        "max_weight_for_bucket": max_weight_for_bucket,
        "portfolio_heat_pct": portfolio_heat_pct,
        "sector_ok": sector_ok,
    }

    note = " | ".join(reasons) if reasons else "No strong signal"
    return total_score, action, details, note


def enrich_portfolio_with_weight_and_risk(
    portfolio: List[Dict],
    total_assets: float,
    cash: float,
    market_regime: Dict
) -> List[Dict]:
    result = []

    temp_rows = []
    for item in portfolio:
        row = item.copy()
        row["StopLoss"] = None
        row["TrendStop"] = None
        temp_rows.append(row)

    temp_heat = calc_portfolio_heat(temp_rows, total_assets)
    portfolio_heat_pct = safe_float(temp_heat.get("heat_pct", 0.0))

    for item in portfolio:
        ticker = item["Ticker"]
        hist = get_unified_analysis(ticker)

        signal = "WATCH"
        divergence = "NONE"
        stop_loss = None
        take_profit_1 = None
        take_profit_2 = None
        trend_stop = None
        signal_score = None
        strategy_mode = None
        bucket = classify_symbol_bucket(ticker, hist) if hist is not None and not hist.empty else "LARGE_CAP"
        rs20_vs_spy = None
        adx = None

        if hist is not None and not hist.empty:
            signal_score, action, details, _ = evaluate_strategy(
                ticker=ticker,
                hist=hist,
                held_shares=item["Shares"],
                current_mkt_value=item["MarketValue"],
                total_assets=total_assets,
                cash=cash,
                market_regime=market_regime,
                portfolio_heat_pct=portfolio_heat_pct,
                portfolio=portfolio,
            )
            signal = action
            divergence = details.get("divergence", "NONE")
            stop_loss = details.get("stop_loss")
            take_profit_1 = details.get("take_profit_1")
            take_profit_2 = details.get("take_profit_2")
            trend_stop = details.get("trend_stop")
            strategy_mode = details.get("strategy_mode")
            bucket = details.get("bucket", bucket)
            rs20_vs_spy = details.get("rs20_vs_spy")
            adx = details.get("adx")

        weight = (item["MarketValue"] / total_assets) if total_assets > 0 else 0.0
        distance_to_stop_pct = None
        distance_to_tp1_pct = None
        distance_to_trend_stop_pct = None

        if item["LastPrice"] > 0:
            if stop_loss:
                distance_to_stop_pct = (item["LastPrice"] / stop_loss - 1) * 100
            if take_profit_1:
                distance_to_tp1_pct = (take_profit_1 / item["LastPrice"] - 1) * 100
            if trend_stop:
                distance_to_trend_stop_pct = (item["LastPrice"] / trend_stop - 1) * 100

        row = item.copy()
        row["WeightPct"] = round(weight * 100, 2)
        row["StopLoss"] = round(stop_loss, 4) if stop_loss else None
        row["TakeProfit1"] = round(take_profit_1, 4) if take_profit_1 else None
        row["TakeProfit2"] = round(take_profit_2, 4) if take_profit_2 else None
        row["TrendStop"] = round(trend_stop, 4) if trend_stop else None
        row["Signal"] = signal
        row["SignalScore"] = round(signal_score, 2) if signal_score is not None else None
        row["StrategyMode"] = strategy_mode
        row["Divergence"] = divergence
        row["Bucket"] = bucket
        row["RS20vsSPY"] = round(rs20_vs_spy, 2) if rs20_vs_spy is not None else None
        row["ADX"] = round(adx, 2) if adx is not None else None
        row["DistanceToStopPct"] = round(distance_to_stop_pct, 2) if distance_to_stop_pct is not None else None
        row["DistanceToTP1Pct"] = round(distance_to_tp1_pct, 2) if distance_to_tp1_pct is not None else None
        row["DistanceToTrendStopPct"] = round(distance_to_trend_stop_pct, 2) if distance_to_trend_stop_pct is not None else None
        row["CanAdd"] = weight < get_bucket_limits(bucket)["max_weight"]
        result.append(row)

    return result


# ===============================
# Scan universe helpers
# ===============================
def get_scan_universe(portfolio: List[Dict], watchlist_df: pd.DataFrame) -> List[str]:
    tickers = set()

    for item in portfolio:
        t = normalize_ticker(item["Ticker"])
        if t:
            tickers.add(t)

    if not watchlist_df.empty:
        for t in watchlist_df[watchlist_df["Enabled"]]["Ticker"].tolist():
            t = normalize_ticker(t)
            if t:
                tickers.add(t)

    return sorted(tickers)


# ===============================
# Scanner / monitor
# ===============================
def run_auto_scanner(
    portfolio: List[Dict],
    trades_df: pd.DataFrame,
    cash: float,
    total_assets: float,
    market_regime: Dict,
    watchlist_df: Optional[pd.DataFrame] = None,
) -> Dict:
    start_ts = time.time()
    logs = []
    sent_count = 0
    blocked_count = 0
    failed_count = 0
    fetch_failed = 0
    dedup_blocked = 0
    state_change_count = 0
    signal_logged_count = 0

    current_session = get_market_session()
    alerts_df = load_alerts()
    signals_df = load_signals()
    sent_fingerprints_in_run = set()
    watchlist_df = watchlist_df if watchlist_df is not None else pd.DataFrame(columns=["Ticker", "Enabled", "Category", "Note"])

    universe = get_scan_universe(portfolio, watchlist_df)
    holdings_map = {p["Ticker"]: p for p in portfolio}
    heat_info = calc_portfolio_heat(portfolio, total_assets)
    portfolio_heat_pct = safe_float(heat_info.get("heat_pct", 0.0))

    if not universe:
        return {
            "logs": ["Universe 為空，略過掃描"],
            "metrics": {
                "scan_seconds": round(time.time() - start_ts, 2),
                "universe_count": 0,
                "sent_count": 0,
                "blocked_count": 0,
                "failed_count": 0,
                "fetch_failed": 0,
                "dedup_blocked": 0,
                "state_change_count": 0,
                "signal_logged_count": 0,
                "portfolio_heat_pct": portfolio_heat_pct,
            }
        }

    for ticker in universe:
        holding_item = holdings_map.get(ticker, {})
        held_shares = safe_float(holding_item.get("Shares", 0.0))
        current_mkt_value = safe_float(holding_item.get("MarketValue", 0.0))

        hist = get_unified_analysis(ticker)
        if hist is None or hist.empty:
            logs.append(f"{ticker}: 無法取得歷史資料")
            fetch_failed += 1
            continue

        recent_buy, recent_sell = get_recent_trade_status(ticker, trades_df)

        score, action, details, note = evaluate_strategy(
            ticker=ticker,
            hist=hist,
            held_shares=held_shares,
            current_mkt_value=current_mkt_value,
            total_assets=total_assets,
            cash=cash,
            market_regime=market_regime,
            portfolio_heat_pct=portfolio_heat_pct,
            portfolio=portfolio,
        )

        strategy_mode = details.get("strategy_mode", "NONE")
        current_signal_state = build_signal_state(action, strategy_mode)
        prev_signal_state = get_last_signal_state(signals_df, ticker)
        state_changed = prev_signal_state is not None and prev_signal_state != current_signal_state

        # 記錄所有非純 WATCH 訊號，方便後續回測
        if action != "WATCH":
            signal_ok = log_signal_snapshot(
                ticker=ticker,
                action=action,
                strategy_mode=strategy_mode,
                score=score,
                details=details,
                reason=note,
                session=current_session,
            )
            if signal_ok:
                signal_logged_count += 1

        if state_changed:
            state_change_count += 1
            logs.append(f"{ticker}: 訊號狀態切換 {prev_signal_state} -> {current_signal_state}")

        send_msg = None
        target_price = None

        if current_session == "CLOSED" and action in ["BUY_NOW", "SELL_EXIT", "SELL_PARTIAL"]:
            logs.append(f"{ticker}: 休市期間，略過即時交易訊號")
            blocked_count += 1
            continue

        if action == "BUY_NOW" and not recent_buy:
            qty = details["suggested_buy_qty"]
            target_price = details["target_buy_price"]
            if qty >= 1:
                send_msg = (
                    f"🟢 *買入訊號* `{ticker}`\n"
                    f"策略：`{details['strategy_mode']}`\n"
                    f"分組：`{details['bucket']}`\n"
                    f"市場時段：`{display_market_session(current_session)}`\n"
                    f"市場狀態：`{display_market_regime(details['market_regime'])}`\n"
                    f"策略分數：`{score:.1f}`\n"
                    f"現價：`${details['close']:.2f}`\n"
                    f"建議進場價：`${target_price:.2f}`\n"
                    f"建議股數：`{qty}`\n"
                    f"停損價：`${details['stop_loss']:.2f}`\n"
                    f"趨勢停損：`${details['trend_stop']:.2f}`\n"
                    f"目標 1：`${details['take_profit_1']:.2f}`\n"
                    f"目標 2：`${details['take_profit_2']:.2f}`\n"
                    f"RS vs SPY：`{safe_float(details.get('rs20_vs_spy')):.2f}`\n"
                    f"ADX：`{safe_float(details.get('adx')):.1f}`\n"
                    f"組合 Heat：`{portfolio_heat_pct:.2f}%`\n"
                    f"依據：{note}"
                )

        elif action == "BUY_PULLBACK" and not recent_buy:
            target_price = details["target_buy_price"]
            send_msg = (
                f"🟡 *回檔準備訊號* `{ticker}`\n"
                f"策略：`{details['strategy_mode']}`\n"
                f"分組：`{details['bucket']}`\n"
                f"市場時段：`{display_market_session(current_session)}`\n"
                f"市場狀態：`{display_market_regime(details['market_regime'])}`\n"
                f"現價：`${details['close']:.2f}`\n"
                f"回檔目標區：`${target_price*(1-PRE_ALERT_PCT):.2f}` ~ `${target_price*(1+PRE_ALERT_PCT):.2f}`\n"
                f"目標價：`${target_price:.2f}`\n"
                f"預估股數：`{details['suggested_buy_qty']}`\n"
                f"RS vs SPY：`{safe_float(details.get('rs20_vs_spy')):.2f}`\n"
                f"ADX：`{safe_float(details.get('adx')):.1f}`\n"
                f"依據：{note}"
            )

        elif action == "SELL_PARTIAL" and not recent_sell and held_shares > 0:
            qty = details["suggested_sell_qty"]
            target_price = details["target_sell_price"] or details["close"]
            if qty >= 1:
                send_msg = (
                    f"🟠 *部分減碼訊號* `{ticker}`\n"
                    f"策略：`{details['strategy_mode']}`\n"
                    f"市場時段：`{display_market_session(current_session)}`\n"
                    f"市場狀態：`{display_market_regime(details['market_regime'])}`\n"
                    f"策略分數：`{score:.1f}`\n"
                    f"現價：`${details['close']:.2f}`\n"
                    f"建議減碼價：`${target_price:.2f}`\n"
                    f"建議股數：`{qty}`\n"
                    f"RSI：`{details['rsi']:.1f}`\n"
                    f"趨勢停損：`${details['trend_stop']:.2f}`\n"
                    f"依據：{note}"
                )

        elif action == "SELL_EXIT" and not recent_sell and held_shares > 0:
            qty = details["suggested_sell_qty"]
            target_price = details["target_sell_price"] or details["close"]
            if qty >= 1:
                send_msg = (
                    f"🔴 *出場訊號* `{ticker}`\n"
                    f"策略：`{details['strategy_mode']}`\n"
                    f"市場時段：`{display_market_session(current_session)}`\n"
                    f"市場狀態：`{display_market_regime(details['market_regime'])}`\n"
                    f"策略分數：`{score:.1f}`\n"
                    f"現價：`${details['close']:.2f}`\n"
                    f"建議出場價：`${target_price:.2f}`\n"
                    f"建議股數：`{qty}`\n"
                    f"停損價：`${details['stop_loss']:.2f}`\n"
                    f"趨勢停損：`${details['trend_stop']:.2f}`\n"
                    f"依據：{note}"
                )

        elif action == "SELL_READY" and not recent_sell and held_shares > 0:
            target_price = details["target_sell_price"]
            send_msg = (
                f"🟠 *賣出準備訊號* `{ticker}`\n"
                f"策略：`{details['strategy_mode']}`\n"
                f"市場時段：`{display_market_session(current_session)}`\n"
                f"現價：`${details['close']:.2f}`\n"
                f"目標區間：`${target_price*(1-PRE_ALERT_PCT):.2f}` ~ `${target_price*(1+PRE_ALERT_PCT):.2f}`\n"
                f"目標價格：`${target_price:.2f}`\n"
                f"預計股數：`{details['suggested_sell_qty']}`\n"
                f"依據：{note}"
            )

        if not send_msg:
            logs.append(f"{ticker}: 本輪無需發送訊息")
            continue

        fingerprint = build_alert_fingerprint(
            ticker=ticker,
            action=action,
            session=current_session,
            price=details["close"],
            score=score,
            target_price=target_price,
        )

        if fingerprint in sent_fingerprints_in_run:
            logs.append(f"{ticker}: 本輪掃描內重複訊號，略過")
            dedup_blocked += 1
            continue

        allow_send = should_send_alert(
            alerts_df=alerts_df,
            ticker=ticker,
            action=action,
            current_price=details["close"],
            current_score=score,
            current_session=current_session,
            target_price=target_price,
            state_changed=state_changed,
        )

        if not allow_send:
            logs.append(f"{ticker}: 被去重規則擋下")
            dedup_blocked += 1
            continue

        ok = send_telegram_msg(send_msg)
        if ok:
            success = log_sent_alert(
                ticker=ticker,
                action=action,
                price=details["close"],
                score=score,
                session=current_session,
                target_price=target_price,
                message=send_msg,
            )
            if success:
                sent_fingerprints_in_run.add(fingerprint)
                alerts_df = pd.concat([
                    alerts_df,
                    pd.DataFrame([{
                        "DateTime": pd.to_datetime(datetime.now()),
                        "Ticker": ticker,
                        "Action": action,
                        "BaseKey": f"{ticker}_{action}",
                        "Price": details["close"],
                        "Score": score,
                        "Session": current_session,
                        "TargetPrice": target_price,
                        "Message": send_msg,
                        "Fingerprint": fingerprint,
                    }])
                ], ignore_index=True)
                logs.append(f"{ticker}: 提醒已發送 -> {action}")
                sent_count += 1
            else:
                logs.append(f"{ticker}: Telegram 已送出，但寫入提醒紀錄失敗")
                failed_count += 1
        else:
            logs.append(f"{ticker}: Telegram 發送失敗")
            failed_count += 1

    return {
        "logs": logs,
        "metrics": {
            "scan_seconds": round(time.time() - start_ts, 2),
            "universe_count": len(universe),
            "sent_count": sent_count,
            "blocked_count": blocked_count,
            "failed_count": failed_count,
            "fetch_failed": fetch_failed,
            "dedup_blocked": dedup_blocked,
            "state_change_count": state_change_count,
            "signal_logged_count": signal_logged_count,
            "portfolio_heat_pct": portfolio_heat_pct,
        }
    }


# ===============================
# Performance metrics
# ===============================
def calculate_performance_metrics(history_df: pd.DataFrame) -> Dict:
    if history_df.empty or "TotalAssets" not in history_df.columns:
        return {
            "max_drawdown_pct": None,
            "total_return_pct": None,
            "daily_vol_pct": None,
            "sharpe": None,
            "history_points": 0,
        }

    temp = history_df.copy()
    temp["Date"] = pd.to_datetime(temp["Date"], errors="coerce")
    temp["TotalAssets"] = pd.to_numeric(temp["TotalAssets"], errors="coerce")
    temp = temp.dropna(subset=["Date", "TotalAssets"]).sort_values("Date").reset_index(drop=True)

    if temp.empty:
        return {
            "max_drawdown_pct": None,
            "total_return_pct": None,
            "daily_vol_pct": None,
            "sharpe": None,
            "history_points": 0,
        }

    nav = temp["TotalAssets"]

    if len(nav) < 2:
        return {
            "max_drawdown_pct": 0.0,
            "total_return_pct": 0.0,
            "daily_vol_pct": None,
            "sharpe": None,
            "history_points": len(nav),
        }

    returns = nav.pct_change().replace([float("inf"), float("-inf")], pd.NA).dropna()

    total_return_pct = (nav.iloc[-1] / nav.iloc[0] - 1) * 100 if nav.iloc[0] > 0 else None

    rolling_peak = nav.cummax()
    drawdown = nav / rolling_peak - 1
    max_drawdown_pct = drawdown.min() * 100 if not drawdown.empty else 0.0

    daily_vol_pct = returns.std() * 100 if not returns.empty else None

    sharpe = None
    if len(returns) >= 2 and returns.std() is not None and returns.std() > 0:
        sharpe = (returns.mean() / returns.std()) * (252 ** 0.5)

    return {
        "max_drawdown_pct": round(max_drawdown_pct, 2) if max_drawdown_pct is not None else None,
        "total_return_pct": round(total_return_pct, 2) if total_return_pct is not None else None,
        "daily_vol_pct": round(daily_vol_pct, 2) if daily_vol_pct is not None else None,
        "sharpe": round(sharpe, 2) if sharpe is not None else None,
        "history_points": len(nav),
    }


def build_trade_preview(
    trades_df: pd.DataFrame,
    initial_capital: float,
    ticker: str,
    trade_type: str,
    price: float,
    shares: float,
    fee: float,
) -> Dict:
    portfolio_raw, cash, _ = build_portfolio(trades_df, initial_capital)
    current_holding = next((x for x in portfolio_raw if x["Ticker"] == normalize_ticker(ticker)), None)
    current_shares = safe_float(current_holding["Shares"]) if current_holding else 0.0
    current_mkt_value = safe_float(current_holding["MarketValue"]) if current_holding else 0.0
    total_assets = cash + sum(x["MarketValue"] for x in portfolio_raw)

    gross = price * shares
    slippage = gross * DEFAULT_SLIPPAGE_PCT
    net = gross + fee + slippage if normalize_trade_type(trade_type) == "BUY" else gross - fee - slippage
    after_cash = cash - net if normalize_trade_type(trade_type) == "BUY" else cash + net

    after_shares = (
        current_shares + shares
        if normalize_trade_type(trade_type) == "BUY"
        else max(0.0, current_shares - shares)
    )
    after_position_value = after_shares * price
    after_weight_pct = (after_position_value / total_assets * 100) if total_assets > 0 else 0.0

    hist = get_unified_analysis(ticker)
    bucket = classify_symbol_bucket(ticker, hist) if hist is not None and not hist.empty else "LARGE_CAP"
    bucket_limit = get_bucket_limits(bucket)["max_weight"] * 100

    return {
        "current_cash": round(cash, 2),
        "after_cash": round(after_cash, 2),
        "current_shares": round(current_shares, 4),
        "after_shares": round(after_shares, 4),
        "gross_total": round(gross, 2),
        "fee": round(fee, 2),
        "slippage": round(slippage, 2),
        "net_total": round(net, 2),
        "current_weight_pct": round((current_mkt_value / total_assets * 100) if total_assets > 0 else 0.0, 2),
        "after_weight_pct": round(after_weight_pct, 2),
        "bucket": bucket,
        "bucket_max_weight_pct": round(bucket_limit, 2),
        "exceed_max_weight": after_weight_pct > bucket_limit,
        "sell_exceeds_position": normalize_trade_type(trade_type) == "SELL" and shares > current_shares + 1e-9,
    }

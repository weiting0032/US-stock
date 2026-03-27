import json
import math
import os
import time
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import gspread
import pandas as pd
import pytz
import requests
import yfinance as yf


PORTFOLIO_SHEET_TITLE = os.getenv("PORTFOLIO_SHEET_TITLE", "US Stock").strip()

TG_TOKEN = os.getenv("TG_TOKEN", "").strip()
TG_CHAT_ID = str(os.getenv("TG_CHAT_ID", "")).strip()

DEFAULT_INITIAL_CAPITAL = float(os.getenv("INITIAL_CAPITAL", "32000"))
MAX_POSITION_WEIGHT = float(os.getenv("MAX_POSITION_WEIGHT", "0.30"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.01"))
CASH_RESERVE_PCT = float(os.getenv("CASH_RESERVE_PCT", "0.10"))
COOLDOWN_DAYS = int(os.getenv("COOLDOWN_DAYS", "3"))

PRE_ALERT_PCT = float(os.getenv("PRE_ALERT_PCT", "0.01"))
ALERT_MIN_MINUTES = int(os.getenv("ALERT_MIN_MINUTES", "30"))
ALERT_MIN_PRICE_CHANGE = float(os.getenv("ALERT_MIN_PRICE_CHANGE", "1.0"))
ALERT_MIN_SCORE_CHANGE = float(os.getenv("ALERT_MIN_SCORE_CHANGE", "0.8"))

MIN_AVG_DOLLAR_VOLUME = float(os.getenv("MIN_AVG_DOLLAR_VOLUME", "20000000"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "10"))
EARNINGS_BLOCK_DAYS = int(os.getenv("EARNINGS_BLOCK_DAYS", "2"))
DEFAULT_COMMISSION = float(os.getenv("DEFAULT_COMMISSION", "0"))
DEFAULT_SLIPPAGE_PCT = float(os.getenv("DEFAULT_SLIPPAGE_PCT", "0.0005"))


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


@lru_cache(maxsize=512)
def get_last_price(symbol: str) -> Optional[float]:
    try:
        hist = yf.Ticker(normalize_ticker(symbol)).history(period="5d", auto_adjust=True)
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return None


@lru_cache(maxsize=512)
def get_next_earnings_date(symbol: str) -> Optional[datetime]:
    try:
        tk = yf.Ticker(normalize_ticker(symbol))
        cal = tk.calendar
        if cal is None or len(cal) == 0:
            return None

        if isinstance(cal, pd.DataFrame):
            vals = cal.values.flatten().tolist()
            for x in vals:
                dt = pd.to_datetime(x, errors="coerce")
                if pd.notna(dt):
                    return dt.to_pydatetime()

        if isinstance(cal, dict):
            for _, v in cal.items():
                dt = pd.to_datetime(v, errors="coerce")
                if pd.notna(dt):
                    return dt.to_pydatetime()
    except Exception:
        pass
    return None


@lru_cache(maxsize=512)
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
        df["ATR"] = tr.rolling(14).mean()

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

        return df.dropna().copy()
    except Exception:
        return None


def clear_market_cache():
    get_last_price.cache_clear()
    get_unified_analysis.cache_clear()
    get_next_earnings_date.cache_clear()


# ===============================
# Google Sheets
# ===============================
def get_gsheet_client():
    raw = os.getenv("GCP_SERVICE_ACCOUNT", "").strip()
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


def get_sheet_handles():
    ss = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE)
    ws_trades = get_or_create_worksheet(ss, "Trades", rows=10000, cols=14)
    ws_history = get_or_create_worksheet(ss, "History", rows=8000, cols=12)
    ws_alerts = get_or_create_worksheet(ss, "Alerts", rows=12000, cols=12)
    ws_watchlist = get_or_create_worksheet(ss, "Watchlist", rows=2000, cols=4)

    if not ws_trades.get_all_values():
        ws_trades.append_row([
            "TradeDateTime", "CreatedAt", "Ticker", "Type", "Price", "Shares",
            "GrossTotal", "Fee", "Slippage", "NetTotal", "Note", "OrderID"
        ])

    if not ws_history.get_all_values():
        ws_history.append_row([
            "Date", "TotalAssets", "Cash", "MarketValue", "RealizedPL",
            "UnrealizedPL", "TotalPL", "DailyReturnPct", "DrawdownPct",
            "BenchmarkSPY", "BenchmarkReturnPct"
        ])

    if not ws_alerts.get_all_values():
        ws_alerts.append_row([
            "DateTime", "Ticker", "Action", "BaseKey", "Price",
            "Score", "Session", "TargetPrice", "Message", "Fingerprint"
        ])

    if not ws_watchlist.get_all_values():
        ws_watchlist.append_row(["Ticker", "Enabled", "Category", "Note"])

    return ss, ws_trades, ws_history, ws_alerts, ws_watchlist


def read_worksheet_as_df(ws, expected_headers: List[str]) -> pd.DataFrame:
    try:
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame(columns=expected_headers)
        rows = values[1:] if len(values) > 1 else []
        clean = []
        for row in rows:
            row = row[:len(expected_headers)] + [""] * max(0, len(expected_headers) - len(row))
            clean.append(row[:len(expected_headers)])
        return pd.DataFrame(clean, columns=expected_headers)
    except Exception:
        return pd.DataFrame(columns=expected_headers)


# ===============================
# Trades / Watchlist / History
# ===============================
def load_trades() -> pd.DataFrame:
    _, ws_trades, _, _, _ = get_sheet_handles()

    values = ws_trades.get_all_values()
    if not values:
        return pd.DataFrame(columns=[
            "TradeDateTime", "CreatedAt", "Ticker", "Type", "Price", "Shares",
            "GrossTotal", "Fee", "Slippage", "NetTotal", "Note", "OrderID"
        ])

    headers = values[0]
    rows = values[1:]

    # backward compatibility
    if headers == ["Date", "Ticker", "Type", "Price", "Shares", "Total", "Note"]:
        df = pd.DataFrame(rows, columns=headers)
        df["TradeDateTime"] = pd.to_datetime(df["Date"], errors="coerce")
        df["CreatedAt"] = pd.to_datetime(df["Date"], errors="coerce")
        df["GrossTotal"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0.0)
        df["Fee"] = 0.0
        df["Slippage"] = 0.0
        df["NetTotal"] = df["GrossTotal"]
        df["OrderID"] = ""
        df = df[[
            "TradeDateTime", "CreatedAt", "Ticker", "Type", "Price", "Shares",
            "GrossTotal", "Fee", "Slippage", "NetTotal", "Note", "OrderID"
        ]]
    else:
        cols = [
            "TradeDateTime", "CreatedAt", "Ticker", "Type", "Price", "Shares",
            "GrossTotal", "Fee", "Slippage", "NetTotal", "Note", "OrderID"
        ]
        df = read_worksheet_as_df(ws_trades, cols)

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


def load_watchlist() -> pd.DataFrame:
    _, _, _, _, ws_watchlist = get_sheet_handles()
    cols = ["Ticker", "Enabled", "Category", "Note"]
    df = read_worksheet_as_df(ws_watchlist, cols)
    if df.empty:
        return pd.DataFrame(columns=cols)

    df["Ticker"] = df["Ticker"].astype(str).apply(normalize_ticker)
    df["Enabled"] = df["Enabled"].astype(str).str.upper().isin(["TRUE", "1", "YES", "Y", "ON"])
    return df[df["Ticker"] != ""].reset_index(drop=True)


def save_watchlist(ticker: str, enabled: bool = True, category: str = "General", note: str = "") -> Tuple[bool, str]:
    ticker = normalize_ticker(ticker)
    if not ticker:
        return False, "Ticker 不可為空"

    try:
        _, _, _, _, ws_watchlist = get_sheet_handles()
        ws_watchlist.append_row([ticker, str(enabled), category, note])
        return True, "已加入 Watchlist"
    except Exception as e:
        return False, f"加入 Watchlist 失敗：{e}"


def load_alerts() -> pd.DataFrame:
    _, _, _, ws_alerts, _ = get_sheet_handles()
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
    if "Fingerprint" not in df.columns:
        df["Fingerprint"] = ""
    return df.sort_values("DateTime").reset_index(drop=True)


def load_history() -> pd.DataFrame:
    _, _, ws_history, _, _ = get_sheet_handles()
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


def save_trade(
    trade_dt: datetime,
    ticker: str,
    trade_type: str,
    price: float,
    shares: float,
    note: str = "",
    fee: float = DEFAULT_COMMISSION,
    slippage: float = 0.0,
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

    _, ws_trades, _, _, _ = get_sheet_handles()
    gross_total = round(price * shares, 4)
    net_total = round(gross_total + fee + slippage if trade_type == "BUY" else gross_total - fee - slippage, 4)

    ws_trades.append_row([
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
    ])
    clear_market_cache()
    return True, "交易已寫入"


def maybe_log_daily_history(
    total_assets: float,
    cash: float,
    market_value: float,
    realized_pl: float,
    unrealized_pl: float,
) -> Tuple[bool, str]:
    try:
        _, _, ws_history, _, _ = get_sheet_handles()
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
            peak = nav_series.cummax().iloc[-1]
            if peak > 0:
                drawdown_pct = (total_assets / peak - 1) * 100

        benchmark_return_pct = None
        if not hist_df.empty and pd.notna(hist_df["BenchmarkSPY"].iloc[-1]) and benchmark_spy:
            prev_spy = safe_float(hist_df["BenchmarkSPY"].iloc[-1])
            if prev_spy > 0:
                benchmark_return_pct = (benchmark_spy / prev_spy - 1) * 100

        ws_history.append_row([
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
        ])
        return True, "已寫入每日 NAV"
    except Exception as e:
        return False, f"寫入 NAV 失敗：{e}"


# ===============================
# Alert Dedup
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
    return f"{ticker}|{action}|{session}|{round(float(price), 2)}|{round(float(score), 1)}|{tp}"


def has_same_fingerprint(alerts_df: pd.DataFrame, fingerprint: str) -> bool:
    if alerts_df.empty or "Fingerprint" not in alerts_df.columns:
        return False
    return fingerprint in alerts_df["Fingerprint"].astype(str).values


def get_last_sent_alert(alerts_df: pd.DataFrame, ticker: str, action: str) -> Optional[dict]:
    if alerts_df.empty:
        return None
    base_key = f"{ticker}_{action}"
    temp = alerts_df[alerts_df["BaseKey"] == base_key]
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


def should_send_alert(
    alerts_df: pd.DataFrame,
    ticker: str,
    action: str,
    current_price: float,
    current_score: float,
    current_session: str,
    target_price: Optional[float] = None,
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
        _, _, _, ws_alerts, _ = get_sheet_handles()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_key = f"{ticker}_{action}"
        fingerprint = build_alert_fingerprint(ticker, action, session, price, score, target_price)

        ws_alerts.append_row([
            now_str,
            ticker.upper().strip(),
            action.upper().strip(),
            base_key,
            float(price),
            float(score),
            session,
            float(target_price) if target_price else "",
            message,
            fingerprint
        ])
        return True
    except Exception:
        return False


# ===============================
# Market regime / filters
# ===============================
def get_market_regime() -> Dict:
    spy = get_unified_analysis("SPY")
    qqq = get_unified_analysis("QQQ")
    vix = get_unified_analysis("^VIX")

    if spy is None or spy.empty or qqq is None or qqq.empty:
        return {"regime": "UNKNOWN", "score": 0, "allow_buy": True, "risk_multiplier": 0.5}

    score = 0

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

    vix_ok = True
    vix_level = None
    if vix is not None and not vix.empty:
        vix_level = safe_float(vix.iloc[-1]["Close"])
        if vix_level >= 22:
            vix_ok = False
        else:
            score += 1

    if score >= 4 and vix_ok:
        return {
            "regime": "RISK_ON",
            "score": score,
            "allow_buy": True,
            "risk_multiplier": 1.0,
            "vix": vix_level,
        }

    if score <= 2:
        return {
            "regime": "RISK_OFF",
            "score": score,
            "allow_buy": False,
            "risk_multiplier": 0.0,
            "vix": vix_level,
        }

    return {
        "regime": "NEUTRAL",
        "score": score,
        "allow_buy": True,
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


def passes_liquidity_filter(hist: pd.DataFrame) -> bool:
    if hist is None or hist.empty:
        return False
    last = hist.iloc[-1]
    close = safe_float(last["Close"])
    dv20 = safe_float(last["DollarVolume20"])
    return close >= MIN_PRICE and dv20 >= MIN_AVG_DOLLAR_VOLUME


def is_earnings_blocked(symbol: str) -> bool:
    next_dt = get_next_earnings_date(symbol)
    if not next_dt:
        return False
    days = (next_dt.date() - datetime.now().date()).days
    return abs(days) <= EARNINGS_BLOCK_DAYS


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
                lots.append({"shares": shares, "price": actual_cost / shares, "datetime": row["TradeDateTime"]})
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
            })

    return portfolio, cash, total_realized_pl


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
) -> Dict:
    if entry_price <= 0 or stop_price <= 0 or entry_price <= stop_price:
        return {"qty_by_risk": 0, "qty_by_weight": 0, "qty_by_cash": 0, "final_qty": 0}

    regime_mult = 1.0
    if market_regime:
        regime_mult = safe_float(market_regime.get("risk_multiplier", 1.0), 1.0)

    risk_budget = total_assets * RISK_PER_TRADE_PCT * regime_mult
    risk_per_share = max(0.01, entry_price - stop_price)
    qty_by_risk = math.floor(risk_budget / risk_per_share)

    max_position_value = total_assets * MAX_POSITION_WEIGHT
    remaining_position_value = max(0.0, max_position_value - current_mkt_value)
    qty_by_weight = math.floor(remaining_position_value / entry_price)

    usable_cash = max(0.0, cash - total_assets * CASH_RESERVE_PCT)
    qty_by_cash = math.floor(usable_cash / entry_price)

    final_qty = max(0, min(qty_by_risk, qty_by_weight, qty_by_cash))
    return {
        "qty_by_risk": qty_by_risk,
        "qty_by_weight": qty_by_weight,
        "qty_by_cash": qty_by_cash,
        "final_qty": final_qty,
    }


def evaluate_strategy(
    ticker: str,
    hist: pd.DataFrame,
    held_shares: float,
    current_mkt_value: float,
    total_assets: float,
    cash: float,
    market_regime: Optional[Dict] = None
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
    rolling_high20 = safe_float(last["RollingHigh20"])
    rolling_high55_prev = safe_float(hist["RollingHigh55"].shift(1).iloc[-1])
    dollar_volume20 = safe_float(last["DollarVolume20"])

    divergence = detect_volume_price_divergence(hist)
    regime_name = market_regime.get("regime", "UNKNOWN") if market_regime else "UNKNOWN"
    regime_allow_buy = market_regime.get("allow_buy", True) if market_regime else True

    reasons = []
    trend_score = 0.0
    momentum_score = 0.0
    pullback_score = 0.0
    volume_score = 0.0
    regime_score = 0.0
    risk_score = 0.0

    trend_ok = close > sma200 and sma50 > sma200
    strong_trend = trend_ok and sma20 > sma50
    near_sma20 = abs(close - sma20) / close <= 0.03 if close > 0 and sma20 > 0 else False
    pullback_zone = close <= sma20 and close >= (sma20 - atr) if atr > 0 and sma20 > 0 else False
    breakout_20 = close > safe_float(hist["RollingHigh20"].shift(1).iloc[-1], 0)
    breakout_55 = close > rolling_high55_prev if rolling_high55_prev > 0 else False
    breakout_volume = volume > vol_sma20 * 1.5 if vol_sma20 > 0 else False

    if trend_ok:
        trend_score += 1.5
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
    if 45 <= rsi <= 65:
        momentum_score += 0.8
        reasons.append("RSI 健康區")
    elif 40 <= rsi < 45:
        momentum_score += 0.4

    if near_sma20:
        pullback_score += 0.6
        reasons.append("接近 SMA20")
    if pullback_zone:
        pullback_score += 1.0
        reasons.append("趨勢回檔區")
    if close < bb_lower:
        pullback_score += 0.6
        reasons.append("接近下布林")

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

    if divergence == "BULLISH":
        volume_score += 0.4
        reasons.append("多方量價背離")
    elif divergence == "BEARISH":
        risk_score -= 0.8
        reasons.append("空方量價背離")

    if regime_name == "RISK_ON":
        regime_score += 0.8
        reasons.append("市場偏多")
    elif regime_name == "NEUTRAL":
        regime_score += 0.2
    elif regime_name == "RISK_OFF":
        regime_score -= 1.2
        reasons.append("市場偏空")

    liquid_ok = passes_liquidity_filter(hist)
    earnings_blocked = is_earnings_blocked(ticker)

    if not liquid_ok:
        risk_score -= 2.0
        reasons.append("流動性不足")
    if earnings_blocked:
        risk_score -= 1.2
        reasons.append("財報事件風險")

    total_score = trend_score + momentum_score + pullback_score + volume_score + regime_score + risk_score
    current_weight = current_mkt_value / total_assets if total_assets > 0 else 0.0

    # 交易區間與風控
    pullback_entry = sma20 if sma20 > 0 else close
    deep_pullback_entry = max(sma50, sma20 - atr) if sma50 > 0 and atr > 0 else pullback_entry
    breakout_entry = max(safe_float(hist["RollingHigh20"].shift(1).iloc[-1], close), close)
    stop_loss = max(0.01, close - 2 * atr) if atr > 0 else max(0.01, close * 0.93)
    trend_stop = trailing_stop if trailing_stop > 0 else stop_loss

    take_profit_1 = close + 2 * atr if atr > 0 else close * 1.08
    take_profit_2 = close + 4 * atr if atr > 0 else close * 1.15

    # 分策略 sizing
    pullback_sizing = calc_position_size(
        total_assets=total_assets,
        cash=cash,
        current_mkt_value=current_mkt_value,
        entry_price=pullback_entry,
        stop_price=max(0.01, pullback_entry - 2 * atr) if atr > 0 else stop_loss,
        market_regime=market_regime,
    )

    breakout_sizing = calc_position_size(
        total_assets=total_assets,
        cash=cash,
        current_mkt_value=current_mkt_value,
        entry_price=max(close, breakout_entry),
        stop_price=max(0.01, close - 1.5 * atr) if atr > 0 else stop_loss,
        market_regime=market_regime,
    )

    action = "WATCH"
    strategy_mode = "NONE"
    target_buy_price = None
    target_sell_price = None

    # 賣出邏輯分級
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
        # 買入邏輯拆分
        if (
            regime_allow_buy and
            liquid_ok and
            not earnings_blocked and
            strong_trend and
            pullback_zone and
            40 <= rsi <= 60 and
            pullback_sizing["final_qty"] >= 1 and
            current_weight < MAX_POSITION_WEIGHT
        ):
            action = "BUY_NOW"
            strategy_mode = "TREND_PULLBACK"
            target_buy_price = pullback_entry

        elif (
            regime_allow_buy and
            liquid_ok and
            not earnings_blocked and
            trend_ok and
            breakout_20 and
            breakout_volume and
            rsi < 75 and
            breakout_sizing["final_qty"] >= 1 and
            current_weight < MAX_POSITION_WEIGHT
        ):
            action = "BUY_NOW"
            strategy_mode = "BREAKOUT"
            target_buy_price = close

        elif (
            regime_allow_buy and
            liquid_ok and
            not earnings_blocked and
            strong_trend and
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

    # 分級賣出數量
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
    if strategy_mode == "TREND_PULLBACK":
        suggested_buy_qty = pullback_sizing["final_qty"]
    elif strategy_mode == "BREAKOUT":
        suggested_buy_qty = breakout_sizing["final_qty"]
    elif strategy_mode == "PULLBACK_READY":
        suggested_buy_qty = pullback_sizing["final_qty"]

    details = {
        "close": close,
        "rsi": rsi,
        "atr": atr,
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
        "divergence": divergence,
        "market_regime": regime_name,
        "strategy_mode": strategy_mode,
        "trend_score": trend_score,
        "momentum_score": momentum_score,
        "pullback_score": pullback_score,
        "volume_score": volume_score,
        "regime_score": regime_score,
        "risk_score": risk_score,
        "liquid_ok": liquid_ok,
        "earnings_blocked": earnings_blocked,
        "dollar_volume20": dollar_volume20,
        "pullback_qty_by_risk": pullback_sizing["qty_by_risk"],
        "pullback_qty_by_weight": pullback_sizing["qty_by_weight"],
        "pullback_qty_by_cash": pullback_sizing["qty_by_cash"],
        "breakout_qty_by_risk": breakout_sizing["qty_by_risk"],
        "breakout_qty_by_weight": breakout_sizing["qty_by_weight"],
        "breakout_qty_by_cash": breakout_sizing["qty_by_cash"],
        "reasons": reasons,
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

        if hist is not None and not hist.empty:
            signal_score, action, details, _ = evaluate_strategy(
                ticker=ticker,
                hist=hist,
                held_shares=item["Shares"],
                current_mkt_value=item["MarketValue"],
                total_assets=total_assets,
                cash=cash,
                market_regime=market_regime,
            )
            signal = action
            divergence = details.get("divergence", "NONE")
            stop_loss = details.get("stop_loss")
            take_profit_1 = details.get("take_profit_1")
            take_profit_2 = details.get("take_profit_2")
            trend_stop = details.get("trend_stop")
            strategy_mode = details.get("strategy_mode")

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
        row["DistanceToStopPct"] = round(distance_to_stop_pct, 2) if distance_to_stop_pct is not None else None
        row["DistanceToTP1Pct"] = round(distance_to_tp1_pct, 2) if distance_to_tp1_pct is not None else None
        row["DistanceToTrendStopPct"] = round(distance_to_trend_stop_pct, 2) if distance_to_trend_stop_pct is not None else None
        row["CanAdd"] = weight < MAX_POSITION_WEIGHT * 100
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

    current_session = get_market_session()
    alerts_df = load_alerts()
    sent_fingerprints_in_run = set()
    watchlist_df = watchlist_df if watchlist_df is not None else pd.DataFrame(columns=["Ticker", "Enabled", "Category", "Note"])

    universe = get_scan_universe(portfolio, watchlist_df)
    holdings_map = {p["Ticker"]: p for p in portfolio}

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
        )

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
                    f"依據：{note}"
                )

        elif action == "BUY_PULLBACK" and not recent_buy:
            target_price = details["target_buy_price"]
            send_msg = (
                f"🟡 *回檔準備訊號* `{ticker}`\n"
                f"策略：`{details['strategy_mode']}`\n"
                f"市場時段：`{display_market_session(current_session)}`\n"
                f"市場狀態：`{display_market_regime(details['market_regime'])}`\n"
                f"現價：`${details['close']:.2f}`\n"
                f"回檔目標區：`${target_price*(1-PRE_ALERT_PCT):.2f}` ~ `${target_price*(1+PRE_ALERT_PCT):.2f}`\n"
                f"目標價：`${target_price:.2f}`\n"
                f"預估股數：`{details['suggested_buy_qty']}`\n"
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
        }
    }


# ===============================
# Performance metrics
# ===============================
def calculate_performance_metrics(history_df: pd.DataFrame) -> Dict:
    if history_df.empty or len(history_df) < 2:
        return {
            "max_drawdown_pct": None,
            "total_return_pct": None,
            "daily_vol_pct": None,
            "sharpe": None,
        }

    nav = history_df["TotalAssets"].dropna()
    returns = nav.pct_change().dropna()

    total_return_pct = (nav.iloc[-1] / nav.iloc[0] - 1) * 100 if nav.iloc[0] > 0 else None
    rolling_peak = nav.cummax()
    drawdown = nav / rolling_peak - 1
    max_drawdown_pct = drawdown.min() * 100 if not drawdown.empty else None
    daily_vol_pct = returns.std() * 100 if not returns.empty else None

    sharpe = None
    if returns.std() and returns.std() > 0:
        sharpe = (returns.mean() / returns.std()) * (252 ** 0.5)

    return {
        "max_drawdown_pct": round(max_drawdown_pct, 2) if max_drawdown_pct is not None else None,
        "total_return_pct": round(total_return_pct, 2) if total_return_pct is not None else None,
        "daily_vol_pct": round(daily_vol_pct, 2) if daily_vol_pct is not None else None,
        "sharpe": round(sharpe, 2) if sharpe is not None else None,
    }


def build_trade_preview(
    trades_df: pd.DataFrame,
    initial_capital: float,
    ticker: str,
    trade_type: str,
    price: float,
    shares: float,
    fee: float,
    slippage: float,
) -> Dict:
    portfolio_raw, cash, _ = build_portfolio(trades_df, initial_capital)
    current_holding = next((x for x in portfolio_raw if x["Ticker"] == normalize_ticker(ticker)), None)
    current_shares = safe_float(current_holding["Shares"]) if current_holding else 0.0
    current_mkt_value = safe_float(current_holding["MarketValue"]) if current_holding else 0.0
    total_assets = cash + sum(x["MarketValue"] for x in portfolio_raw)

    gross = price * shares
    net = gross + fee + slippage if normalize_trade_type(trade_type) == "BUY" else gross - fee - slippage
    after_cash = cash - net if normalize_trade_type(trade_type) == "BUY" else cash + net

    after_shares = current_shares + shares if normalize_trade_type(trade_type) == "BUY" else max(0.0, current_shares - shares)
    after_position_value = after_shares * price
    after_weight_pct = (after_position_value / total_assets * 100) if total_assets > 0 else 0.0

    return {
        "current_cash": round(cash, 2),
        "after_cash": round(after_cash, 2),
        "current_shares": round(current_shares, 4),
        "after_shares": round(after_shares, 4),
        "gross_total": round(gross, 2),
        "net_total": round(net, 2),
        "current_weight_pct": round((current_mkt_value / total_assets * 100) if total_assets > 0 else 0.0, 2),
        "after_weight_pct": round(after_weight_pct, 2),
        "exceed_max_weight": after_weight_pct > MAX_POSITION_WEIGHT * 100,
        "sell_exceeds_position": normalize_trade_type(trade_type) == "SELL" and shares > current_shares + 1e-9,
    }

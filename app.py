import math
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple

import gspread
import pandas as pd
import plotly.graph_objects as go
import pytz
import requests
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh


# ===============================
# 0. App Config
# ===============================
st.set_page_config(page_title="US Stock Portfolio Pro", layout="wide")
st_autorefresh(interval=120000, limit=None, key="heartbeat_120s")

st.markdown(
    """
    <style>
    [data-testid="stMetricValue"] {
        font-size: 1.6rem !important;
        white-space: nowrap !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.95rem !important;
    }
    .stMetric {
        border: 1px solid rgba(128, 128, 128, 0.25);
        padding: 10px !important;
        border-radius: 12px;
        background: rgba(255,255,255,0.02);
    }
    .price-box {
        background-color: rgba(128, 128, 128, 0.08);
        padding: 14px;
        border-radius: 12px;
        border-left: 5px solid #17BECF;
        margin-bottom: 14px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

APP_TITLE = "🏛️ US Stock Portfolio Pro"
PORTFOLIO_SHEET_TITLE = "US Stock"

TG_TOKEN = str(st.secrets.get("TG_TOKEN", "")).strip()
TG_CHAT_ID = str(st.secrets.get("TG_CHAT_ID", "")).strip()

DEFAULT_INITIAL_CAPITAL = 32000.0
MAX_POSITION_WEIGHT = 0.30
RISK_PER_TRADE_PCT = 0.01
CASH_RESERVE_PCT = 0.10
COOLDOWN_DAYS = 3

PRE_ALERT_PCT = 0.01
ALERT_MIN_MINUTES = 30
ALERT_MIN_PRICE_CHANGE = 1.0
ALERT_MIN_SCORE_CHANGE = 0.8


# ===============================
# 1. Session Defaults
# ===============================
def init_session_state():
    defaults = {
        "trade_ticker": "NVDA",
        "trade_type": "BUY",
        "trade_price": 100.0,
        "trade_shares": 1.0,
        "trade_note": "",
        "nav_synced_today": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


init_session_state()


# ===============================
# 2. Utility Functions
# ===============================
def normalize_ticker(symbol: str) -> str:
    return str(symbol).upper().strip().replace(".", "-")


def normalize_trade_type(x: str) -> str:
    s = str(x).strip().upper()
    if "買" in s or "BUY" in s:
        return "BUY"
    if "賣" in s or "SELL" in s:
        return "SELL"
    return s


def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def color_pl(val):
    color = "#26A69A" if val > 0 else "#EF5350" if val < 0 else "white"
    return f"color: {color}; font-weight: 600;"


def display_na(val, prefix: str = "", suffix: str = "", decimals: int = 2) -> str:
    if val is None or pd.isna(val):
        return "N/A"
    try:
        return f"{prefix}{float(val):,.{decimals}f}{suffix}"
    except Exception:
        return "N/A"


def display_divergence(div: str) -> str:
    div = str(div).strip().upper()
    if div == "BULLISH":
        return "多方量價背離"
    elif div == "BEARISH":
        return "空方量價背離"
    return "無明顯量價背離"


def send_telegram_msg(message: str) -> bool:
    if not TG_TOKEN or not TG_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        res = requests.post(url, json=payload, timeout=8)
        data = res.json()
        return bool(data.get("ok"))
    except Exception:
        return False


def get_recent_trade_status(ticker: str, trades_df: pd.DataFrame) -> Tuple[bool, bool]:
    if trades_df.empty:
        return False, False
    cutoff_date = date.today() - timedelta(days=COOLDOWN_DAYS)
    temp_df = trades_df.copy()
    temp_df["Date"] = pd.to_datetime(temp_df["Date"], errors="coerce").dt.date
    recent = temp_df[(temp_df["Ticker"] == ticker) & (temp_df["Date"] >= cutoff_date)]
    recent_buy = not recent[recent["Type"] == "BUY"].empty
    recent_sell = not recent[recent["Type"] == "SELL"].empty
    return recent_buy, recent_sell


def read_worksheet_as_df(ws, expected_headers: List[str]) -> pd.DataFrame:
    try:
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame(columns=expected_headers)
        data_rows = values[1:] if len(values) > 1 else []
        cleaned_rows = []
        for row in data_rows:
            row = row[:len(expected_headers)] + [""] * max(0, len(expected_headers) - len(row))
            cleaned_rows.append(row[:len(expected_headers)])
        return pd.DataFrame(cleaned_rows, columns=expected_headers)
    except Exception:
        return pd.DataFrame(columns=expected_headers)


def get_market_session() -> str:
    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)
    if now_et.weekday() >= 5:
        return "CLOSED"

    t = now_et.time()
    if datetime.strptime("04:00", "%H:%M").time() <= t < datetime.strptime("09:30", "%H:%M").time():
        return "PREMARKET"
    elif datetime.strptime("09:30", "%H:%M").time() <= t < datetime.strptime("16:00", "%H:%M").time():
        return "REGULAR"
    elif datetime.strptime("16:00", "%H:%M").time() <= t < datetime.strptime("20:00", "%H:%M").time():
        return "AFTERMARKET"
    return "CLOSED"


def calc_target_zone_hit(current_price: float, target_price: Optional[float], tol_pct: float = PRE_ALERT_PCT) -> bool:
    if not target_price or target_price <= 0:
        return False
    diff_pct = abs(current_price - target_price) / target_price
    return diff_pct <= tol_pct


# ===============================
# 3. Alert Dedup Helpers
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


def normalize_alert_df(alerts_df: pd.DataFrame) -> pd.DataFrame:
    if alerts_df is None or alerts_df.empty:
        return pd.DataFrame(columns=[
            "DateTime", "Ticker", "Action", "BaseKey", "Price",
            "Score", "Session", "TargetPrice", "Message", "Fingerprint"
        ])

    df = alerts_df.copy()
    if "Fingerprint" not in df.columns:
        df["Fingerprint"] = ""

    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    df["Action"] = df["Action"].astype(str).str.upper().str.strip()
    df["BaseKey"] = df["BaseKey"].astype(str).str.strip()
    df["Session"] = df["Session"].astype(str).str.strip()
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
    df["TargetPrice"] = pd.to_numeric(df["TargetPrice"], errors="coerce")
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    return df


def has_same_fingerprint(alerts_df: pd.DataFrame, fingerprint: str) -> bool:
    if alerts_df is None or alerts_df.empty:
        return False
    if "Fingerprint" not in alerts_df.columns:
        return False
    temp = alerts_df["Fingerprint"].astype(str).str.strip()
    return fingerprint in temp.values


def get_last_sent_alert(alerts_df: pd.DataFrame, ticker: str, action: str) -> Optional[dict]:
    if alerts_df is None or alerts_df.empty:
        return None

    base_key = f"{ticker}_{action}"
    temp = alerts_df[alerts_df["BaseKey"] == base_key]
    if temp.empty:
        return None

    last_row = temp.sort_values("DateTime").iloc[-1]
    return {
        "DateTime": last_row["DateTime"],
        "Ticker": last_row["Ticker"],
        "Action": last_row["Action"],
        "BaseKey": last_row["BaseKey"],
        "Price": safe_float(last_row["Price"]),
        "Score": safe_float(last_row["Score"]),
        "Session": str(last_row["Session"]),
        "TargetPrice": safe_float(last_row["TargetPrice"]),
        "Message": str(last_row["Message"]),
        "Fingerprint": str(last_row.get("Fingerprint", "")),
    }


# ===============================
# 4. Google Sheets Layer
# ===============================
@st.cache_resource
def get_gsheet_client():
    return gspread.service_account_from_dict(st.secrets["gcp_service_account"])


def get_or_create_worksheet(spreadsheet, title: str, rows: int = 1000, cols: int = 12):
    try:
        return spreadsheet.worksheet(title)
    except Exception:
        return spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))


@st.cache_resource
def get_sheet_handles():
    ss = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE)

    ws_trades = get_or_create_worksheet(ss, "Trades", rows=5000, cols=10)
    ws_history = get_or_create_worksheet(ss, "History", rows=5000, cols=6)
    ws_alerts = get_or_create_worksheet(ss, "Alerts", rows=8000, cols=12)

    if not ws_trades.get_all_values():
        ws_trades.append_row(["Date", "Ticker", "Type", "Price", "Shares", "Total", "Note"])

    if not ws_history.get_all_values():
        ws_history.append_row(["Date", "Total Assets", "Cash", "Market Value", "Total P/L"])

    if not ws_alerts.get_all_values():
        ws_alerts.append_row([
            "DateTime", "Ticker", "Action", "BaseKey", "Price",
            "Score", "Session", "TargetPrice", "Message", "Fingerprint"
        ])

    return ss, ws_trades, ws_history, ws_alerts


@st.cache_data(ttl=600)
def load_trades() -> pd.DataFrame:
    try:
        _, ws_trades, _, _ = get_sheet_handles()
        expected_cols = ["Date", "Ticker", "Type", "Price", "Shares", "Total", "Note"]
        df = read_worksheet_as_df(ws_trades, expected_cols)

        if df.empty:
            return pd.DataFrame(columns=expected_cols)

        df["Ticker"] = df["Ticker"].astype(str).apply(normalize_ticker)
        df["Type"] = df["Type"].astype(str).apply(normalize_trade_type)
        for col in ["Price", "Shares", "Total"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"])
        df = df.sort_values("Date").reset_index(drop=True)
        return df
    except Exception as e:
        st.error(f"讀取交易紀錄失敗：{e}")
        return pd.DataFrame(columns=["Date", "Ticker", "Type", "Price", "Shares", "Total", "Note"])


@st.cache_data(ttl=600)
def load_alerts() -> pd.DataFrame:
    try:
        _, _, _, ws_alerts = get_sheet_handles()
        expected_cols = [
            "DateTime", "Ticker", "Action", "BaseKey", "Price",
            "Score", "Session", "TargetPrice", "Message", "Fingerprint"
        ]
        df = read_worksheet_as_df(ws_alerts, expected_cols)
        if df.empty:
            return pd.DataFrame(columns=expected_cols)
        df = normalize_alert_df(df)
        return df.sort_values("DateTime").reset_index(drop=True)
    except Exception as e:
        st.warning(f"讀取 Alerts 失敗：{e}")
        return pd.DataFrame(columns=[
            "DateTime", "Ticker", "Action", "BaseKey", "Price",
            "Score", "Session", "TargetPrice", "Message", "Fingerprint"
        ])


def get_current_holding_shares(trades_df: pd.DataFrame, ticker: str) -> float:
    ticker = normalize_ticker(ticker)
    if trades_df.empty:
        return 0.0
    temp = trades_df[trades_df["Ticker"] == ticker].copy()
    if temp.empty:
        return 0.0
    buy_shares = temp.loc[temp["Type"] == "BUY", "Shares"].sum()
    sell_shares = temp.loc[temp["Type"] == "SELL", "Shares"].sum()
    return max(0.0, buy_shares - sell_shares)


def save_trade(trade_date: date, ticker: str, trade_type: str, price: float, shares: float, note: str = "") -> Tuple[bool, str]:
    ticker = normalize_ticker(ticker)
    trade_type = normalize_trade_type(trade_type)
    price = safe_float(price)
    shares = safe_float(shares)

    if not ticker:
        return False, "Ticker 不可為空。"
    if trade_type not in ["BUY", "SELL"]:
        return False, "交易類型必須為 BUY 或 SELL。"
    if price <= 0 or shares <= 0:
        return False, "價格與股數必須大於 0。"
    if trade_date > date.today():
        return False, "交易日期不可大於今天。"

    trades_df = load_trades()
    holding_shares = get_current_holding_shares(trades_df, ticker)

    if trade_type == "SELL" and shares > holding_shares:
        return False, f"賣出股數超過持有股數，目前僅持有 {holding_shares:.4f} 股。"

    try:
        _, ws_trades, _, _ = get_sheet_handles()
        total = round(price * shares, 4)
        ws_trades.append_row([
            str(trade_date), ticker, trade_type,
            float(price), float(shares), float(total), note
        ])
        st.cache_data.clear()
        return True, "交易已成功寫入雲端。"
    except Exception as e:
        return False, f"寫入失敗：{e}"


def sync_nav_history(total_assets: float, cash: float, market_value: float, total_pl: float) -> Optional[pd.DataFrame]:
    try:
        _, _, ws_history, _ = get_sheet_handles()
        expected_cols = ["Date", "Total Assets", "Cash", "Market Value", "Total P/L"]

        today_str = date.today().strftime("%Y-%m-%d")
        if st.session_state.get("nav_synced_today") != today_str:
            values = ws_history.get_all_values()
            last_date = None
            if len(values) > 1:
                last_date = str(values[-1][0]).strip()

            if last_date != today_str:
                ws_history.append_row([
                    today_str, float(total_assets), float(cash),
                    float(market_value), float(total_pl)
                ])
            st.session_state["nav_synced_today"] = today_str

        history_df = read_worksheet_as_df(ws_history, expected_cols)
        if not history_df.empty:
            for c in ["Total Assets", "Cash", "Market Value", "Total P/L"]:
                history_df[c] = pd.to_numeric(history_df[c], errors="coerce")
        return history_df
    except Exception as e:
        st.warning(f"歷史 NAV 同步失敗：{e}")
        return None


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
        _, _, _, ws_alerts = get_sheet_handles()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_key = f"{ticker}_{action}"
        fingerprint = build_alert_fingerprint(
            ticker=ticker,
            action=action,
            session=session,
            price=price,
            score=score,
            target_price=target_price
        )

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
        st.cache_data.clear()
        return True
    except Exception as e:
        st.warning(f"寫入 Alerts 失敗：{e}")
        return False


# ===============================
# 5. Alert Decision
# ===============================
def should_send_alert(
    alerts_df: pd.DataFrame,
    ticker: str,
    action: str,
    current_price: float,
    current_score: float,
    current_session: str,
    target_price: Optional[float] = None,
    min_minutes: int = ALERT_MIN_MINUTES,
    min_price_change_pct: float = ALERT_MIN_PRICE_CHANGE,
    min_score_change: float = ALERT_MIN_SCORE_CHANGE,
) -> bool:
    alerts_df = normalize_alert_df(alerts_df)

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
    last_price = safe_float(last_alert["Price"])
    last_score = safe_float(last_alert["Score"])
    last_session = last_alert["Session"]
    last_target = safe_float(last_alert["TargetPrice"])

    if pd.isna(last_dt):
        return True

    if current_session != last_session:
        return True

    minutes_diff = (datetime.now() - last_dt.to_pydatetime()).total_seconds() / 60.0

    price_change_pct = 0.0
    if last_price > 0:
        price_change_pct = abs((current_price - last_price) / last_price) * 100

    score_change = abs(current_score - last_score)

    target_change = 0.0
    if target_price and last_target > 0:
        target_change = abs((target_price - last_target) / last_target) * 100

    if minutes_diff >= min_minutes and (
        price_change_pct >= min_price_change_pct or
        score_change >= min_score_change or
        target_change >= 1.0
    ):
        return True

    return False


# ===============================
# 6. Market Data / Indicator Layer
# ===============================
@st.cache_data(ttl=86400)
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


@st.cache_data(ttl=600)
def get_unified_analysis(symbol: str) -> Optional[pd.DataFrame]:
    try:
        symbol = normalize_ticker(symbol)
        df = yf.Ticker(symbol).history(period="2y", auto_adjust=False)
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
        df["VOL_SMA50"] = df["Volume"].rolling(50).mean()

        df["RollingHigh20"] = df["High"].rolling(20).max()
        df["TrailingStop"] = df["RollingHigh20"] - 3 * df["ATR"]

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
        df["OBV_SMA20"] = df["OBV"].rolling(20).mean()

        return df.dropna().copy()
    except Exception:
        return None


def get_last_price(symbol: str) -> Optional[float]:
    try:
        ticker = yf.Ticker(normalize_ticker(symbol))
        fi = getattr(ticker, "fast_info", None)
        if fi and getattr(fi, "last_price", None):
            return float(fi.last_price)
    except Exception:
        pass

    try:
        hist = yf.Ticker(normalize_ticker(symbol)).history(period="5d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass

    return None


@st.cache_data(ttl=600)
def get_market_regime() -> Dict:
    spy = get_unified_analysis("SPY")
    if spy is None or spy.empty:
        return {"regime": "UNKNOWN", "score": 0, "allow_buy": True}

    last = spy.iloc[-1]
    close = safe_float(last["Close"])
    sma50 = safe_float(last["SMA50"])
    sma200 = safe_float(last["SMA200"])
    macd_hist = safe_float(last["MACD_Hist"])
    rsi = safe_float(last["RSI"])

    score = 0
    if close > sma200:
        score += 1
    if sma50 > sma200:
        score += 1
    if macd_hist > 0:
        score += 1
    if rsi > 45:
        score += 1

    if score >= 3:
        regime = "RISK_ON"
        allow_buy = True
    elif score <= 1:
        regime = "RISK_OFF"
        allow_buy = False
    else:
        regime = "NEUTRAL"
        allow_buy = True

    return {"regime": regime, "score": score, "allow_buy": allow_buy}


def detect_volume_price_divergence(hist: pd.DataFrame) -> str:
    if hist is None or hist.empty or len(hist) < 30:
        return "NONE"

    recent = hist.tail(20)
    prev = hist.tail(40).head(20)
    if recent.empty or prev.empty:
        return "NONE"

    recent_price_high = recent["Close"].max()
    prev_price_high = prev["Close"].max()
    recent_price_low = recent["Close"].min()
    prev_price_low = prev["Close"].min()

    recent_obv = recent["OBV"].iloc[-1]
    prev_obv = prev["OBV"].iloc[-1]

    recent_vol_avg = recent["Volume"].mean()
    prev_vol_avg = prev["Volume"].mean()

    if recent_price_high > prev_price_high and (recent_obv < prev_obv or recent_vol_avg < prev_vol_avg):
        return "BEARISH"
    if recent_price_low < prev_price_low and (recent_obv > prev_obv or recent_vol_avg > prev_vol_avg):
        return "BULLISH"
    return "NONE"


# ===============================
# 7. FIFO Portfolio Calculation
# ===============================
def build_portfolio(trades_df: pd.DataFrame, initial_capital: float) -> Tuple[List[Dict], float, float]:
    if trades_df.empty:
        return [], float(initial_capital), 0.0

    cash = float(initial_capital)
    portfolio = []
    total_realized_pl = 0.0

    for ticker in sorted(trades_df["Ticker"].dropna().unique().tolist()):
        tdf = trades_df[trades_df["Ticker"] == ticker].sort_values("Date").copy()

        lots = []
        realized_pl = 0.0

        for _, row in tdf.iterrows():
            trade_type = normalize_trade_type(row["Type"])
            price = safe_float(row["Price"])
            shares = safe_float(row["Shares"])
            total = price * shares

            if shares <= 0 or price <= 0:
                continue

            if trade_type == "BUY":
                lots.append({
                    "shares": shares,
                    "price": price,
                    "date": row["Date"]
                })
                cash -= total

            elif trade_type == "SELL":
                sell_qty = shares
                cash += total

                while sell_qty > 0 and lots:
                    first_lot = lots[0]
                    lot_shares = safe_float(first_lot["shares"])
                    lot_price = safe_float(first_lot["price"])

                    matched_qty = min(sell_qty, lot_shares)
                    realized_pl += (price - lot_price) * matched_qty

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
            avg_cost_val = fifo_cost_basis / remaining_shares if remaining_shares > 0 else 0.0
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
# 8. Strategy Engine
# ===============================
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

    close = safe_float(last["Close"])
    sma20 = safe_float(last["SMA20"])
    sma50 = safe_float(last["SMA50"])
    sma200 = safe_float(last["SMA200"])
    rsi = safe_float(last["RSI"])
    atr = safe_float(last["ATR"])
    bb_upper = safe_float(last["BB_upper"])
    bb_lower = safe_float(last["BB_lower"])
    macd_hist = safe_float(last["MACD_Hist"])
    volume = safe_float(last["Volume"])
    vol_sma20 = safe_float(last["VOL_SMA20"])
    trailing_stop = safe_float(last["TrailingStop"])
    divergence = detect_volume_price_divergence(hist)

    score = 0.0
    reasons = []

    if close > sma200:
        score += 1.5
        reasons.append("Price > SMA200")
    if sma20 > sma50 > sma200:
        score += 1.5
        reasons.append("SMA20 > SMA50 > SMA200")
    if macd_hist > 0:
        score += 1.0
        reasons.append("MACD Hist > 0")
    if 45 <= rsi <= 65:
        score += 1.0
        reasons.append("RSI healthy zone")
    elif rsi < 35:
        score += 0.5
        reasons.append("RSI oversold")
    if close < bb_lower:
        score += 0.8
        reasons.append("Below lower BB")
    if volume > vol_sma20:
        score += 0.7
        reasons.append("Volume > 20D Avg")

    if divergence == "BULLISH":
        score += 0.7
        reasons.append("Bullish volume divergence")
    elif divergence == "BEARISH":
        score -= 0.8
        reasons.append("Bearish volume divergence")

    regime_name = "UNKNOWN"
    regime_allow_buy = True
    if market_regime:
        regime_name = market_regime.get("regime", "UNKNOWN")
        regime_allow_buy = market_regime.get("allow_buy", True)
        if regime_name == "RISK_ON":
            score += 0.5
            reasons.append("Market regime: Risk On")
        elif regime_name == "RISK_OFF":
            score -= 1.0
            reasons.append("Market regime: Risk Off")

    current_weight = current_mkt_value / total_assets if total_assets > 0 else 0.0

    stop_loss = close - 2 * atr if atr > 0 else None
    take_profit = close + 3 * atr if atr > 0 else None

    target_buy_price = (bb_lower + sma20) / 2 if bb_lower > 0 and sma20 > 0 else close
    target_sell_price = bb_upper if bb_upper > 0 else close

    risk_budget = total_assets * RISK_PER_TRADE_PCT
    risk_per_share = max(0.01, 2 * atr) if atr > 0 else max(0.01, close * 0.05)
    qty_by_risk = math.floor(risk_budget / risk_per_share)

    max_position_value = total_assets * MAX_POSITION_WEIGHT
    remaining_position_value = max(0.0, max_position_value - current_mkt_value)
    qty_by_weight = math.floor(remaining_position_value / close) if close > 0 else 0

    usable_cash = max(0.0, cash - total_assets * CASH_RESERVE_PCT)
    qty_by_cash = math.floor(usable_cash / close) if close > 0 else 0

    suggested_buy_qty = max(0, min(qty_by_risk, qty_by_weight, qty_by_cash))
    suggested_sell_qty = math.ceil(held_shares * 0.33) if held_shares > 0 else 0

    action = "WATCH"

    if score >= 4.0 and suggested_buy_qty >= 1 and current_weight < MAX_POSITION_WEIGHT and regime_allow_buy:
        action = "BUY"
    elif ((score <= 1.5) or (rsi >= 75) or (close > bb_upper) or (held_shares > 0 and trailing_stop > 0 and close < trailing_stop)) and held_shares > 0:
        action = "SELL"
    else:
        if regime_allow_buy and suggested_buy_qty >= 1 and calc_target_zone_hit(close, target_buy_price):
            action = "BUY_READY"
        elif held_shares > 0 and calc_target_zone_hit(close, target_sell_price):
            action = "SELL_READY"

    details = {
        "close": close,
        "sma20": sma20,
        "sma50": sma50,
        "sma200": sma200,
        "rsi": rsi,
        "atr": atr,
        "bb_upper": bb_upper,
        "bb_lower": bb_lower,
        "macd_hist": macd_hist,
        "current_weight": current_weight,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "trailing_stop": trailing_stop,
        "suggested_buy_qty": suggested_buy_qty,
        "suggested_sell_qty": suggested_sell_qty,
        "qty_by_risk": qty_by_risk,
        "qty_by_weight": qty_by_weight,
        "qty_by_cash": qty_by_cash,
        "reasons": reasons,
        "target_buy_price": target_buy_price,
        "target_sell_price": target_sell_price,
        "divergence": divergence,
        "market_regime": regime_name,
    }

    note = " | ".join(reasons) if reasons else "No strong signal"
    return score, action, details, note


def enrich_portfolio_with_weight_and_risk(portfolio: List[Dict], total_assets: float, cash: float, market_regime: Dict) -> List[Dict]:
    result = []
    for item in portfolio:
        ticker = item["Ticker"]
        hist = get_unified_analysis(ticker)
        atr = None
        stop_loss = None
        take_profit = None
        trailing_stop = None
        signal = "WATCH"
        divergence = "NONE"

        if hist is not None and not hist.empty:
            last = hist.iloc[-1]
            atr = safe_float(last.get("ATR"))
            lp = item["LastPrice"]
            trailing_stop = safe_float(last.get("TrailingStop"))
            if atr > 0:
                stop_loss = lp - 2 * atr
                take_profit = lp + 3 * atr

            _, action, details, _ = evaluate_strategy(
                ticker=ticker,
                hist=hist,
                held_shares=item["Shares"],
                current_mkt_value=item["MarketValue"],
                total_assets=total_assets,
                cash=cash,
                market_regime=market_regime
            )
            signal = action
            divergence = details.get("divergence", "NONE")

        weight = (item["MarketValue"] / total_assets) if total_assets > 0 else 0.0

        row = item.copy()
        row["WeightPct"] = round(weight * 100, 2)
        row["ATR"] = round(atr, 4) if atr else None
        row["StopLoss"] = round(stop_loss, 4) if stop_loss else None
        row["TakeProfit"] = round(take_profit, 4) if take_profit else None
        row["TrailingStop"] = round(trailing_stop, 4) if trailing_stop else None
        row["Signal"] = signal
        row["Divergence"] = divergence
        result.append(row)

    return result


# ===============================
# 9. Auto Scanner
# ===============================
def run_auto_scanner(portfolio: List[Dict], trades_df: pd.DataFrame, cash: float, total_assets: float, market_regime: Dict):
    if not portfolio:
        return

    current_session = get_market_session()
    alerts_df = normalize_alert_df(load_alerts())
    sent_fingerprints_in_run = set()

    for item in portfolio:
        ticker = item["Ticker"]
        hist = get_unified_analysis(ticker)
        if hist is None or hist.empty:
            continue

        recent_buy, recent_sell = get_recent_trade_status(ticker, trades_df)

        score, action, details, note = evaluate_strategy(
            ticker=ticker,
            hist=hist,
            held_shares=item["Shares"],
            current_mkt_value=item["MarketValue"],
            total_assets=total_assets,
            cash=cash,
            market_regime=market_regime
        )

        send_msg = None
        target_price = None

        # 休市不發正式 BUY/SELL，避免重複轟炸
        if current_session == "CLOSED" and action in ["BUY", "SELL"]:
            continue

        if action == "BUY" and not recent_buy:
            qty = details["suggested_buy_qty"]
            target_price = details["target_buy_price"]
            if qty >= 1:
                send_msg = (
                    f"🔥 *BUY Signal* `{ticker}`\n"
                    f"Session: `{current_session}`\n"
                    f"Regime: `{details['market_regime']}`\n"
                    f"Score: `{score:.1f}`\n"
                    f"Price: `${details['close']:.2f}`\n"
                    f"Target: `${target_price:.2f}`\n"
                    f"Qty: `{qty}`\n"
                    f"Stop: `${details['stop_loss']:.2f}`\n"
                    f"Trail: `${details['trailing_stop']:.2f}`\n"
                    f"TP: `${details['take_profit']:.2f}`\n"
                    f"{note}"
                )

        elif action == "SELL" and not recent_sell:
            qty = details["suggested_sell_qty"]
            target_price = details["target_sell_price"]
            if qty >= 1:
                send_msg = (
                    f"⚠️ *SELL Signal* `{ticker}`\n"
                    f"Session: `{current_session}`\n"
                    f"Regime: `{details['market_regime']}`\n"
                    f"Score: `{score:.1f}`\n"
                    f"Price: `${details['close']:.2f}`\n"
                    f"Target: `${target_price:.2f}`\n"
                    f"Qty: `{qty}`\n"
                    f"RSI: `{details['rsi']:.1f}`\n"
                    f"Trail: `${details['trailing_stop']:.2f}`\n"
                    f"{note}"
                )

        elif action == "BUY_READY" and not recent_buy:
            target_price = details["target_buy_price"]
            send_msg = (
                f"🟡 *BUY READY* `{ticker}`\n"
                f"Session: `{current_session}`\n"
                f"Regime: `{details['market_regime']}`\n"
                f"Current: `${details['close']:.2f}`\n"
                f"Target Zone: `${target_price*(1-PRE_ALERT_PCT):.2f}` ~ `${target_price*(1+PRE_ALERT_PCT):.2f}`\n"
                f"Target: `${target_price:.2f}`\n"
                f"Qty Plan: `{details['suggested_buy_qty']}`\n"
                f"{note}"
            )

        elif action == "SELL_READY" and not recent_sell:
            target_price = details["target_sell_price"]
            send_msg = (
                f"🟠 *SELL READY* `{ticker}`\n"
                f"Session: `{current_session}`\n"
                f"Current: `${details['close']:.2f}`\n"
                f"Target Zone: `${target_price*(1-PRE_ALERT_PCT):.2f}` ~ `${target_price*(1+PRE_ALERT_PCT):.2f}`\n"
                f"Target: `${target_price:.2f}`\n"
                f"Qty Plan: `{details['suggested_sell_qty']}`\n"
                f"{note}"
            )

        if not send_msg:
            continue

        fingerprint = build_alert_fingerprint(
            ticker=ticker,
            action=action,
            session=current_session,
            price=details["close"],
            score=score,
            target_price=target_price
        )

        if fingerprint in sent_fingerprints_in_run:
            continue

        allow_send = should_send_alert(
            alerts_df=alerts_df,
            ticker=ticker,
            action=action,
            current_price=details["close"],
            current_score=score,
            current_session=current_session,
            target_price=target_price,
            min_minutes=ALERT_MIN_MINUTES,
            min_price_change_pct=ALERT_MIN_PRICE_CHANGE,
            min_score_change=ALERT_MIN_SCORE_CHANGE,
        )

        if allow_send:
            ok = send_telegram_msg(send_msg)
            if ok:
                success = log_sent_alert(
                    ticker=ticker,
                    action=action,
                    price=details["close"],
                    score=score,
                    session=current_session,
                    target_price=target_price,
                    message=send_msg
                )
                if success:
                    sent_fingerprints_in_run.add(fingerprint)
                    new_row = pd.DataFrame([{
                        "DateTime": pd.to_datetime(datetime.now()),
                        "Ticker": ticker,
                        "Action": action,
                        "BaseKey": f"{ticker}_{action}",
                        "Price": details["close"],
                        "Score": score,
                        "Session": current_session,
                        "TargetPrice": target_price,
                        "Message": send_msg,
                        "Fingerprint": fingerprint
                    }])
                    alerts_df = pd.concat([alerts_df, new_row], ignore_index=True)


# ===============================
# 10. Sidebar
# ===============================
st.sidebar.title("🎮 Control Center")

if st.sidebar.button("🔄 Manual Refresh"):
    st.cache_data.clear()
    st.rerun()

if st.sidebar.button("📨 發送 Telegram 測試訊息"):
    if send_telegram_msg("✅ Telegram 連線測試成功"):
        st.sidebar.success("訊息已送出")
    else:
        st.sidebar.warning("送出失敗，請檢查 TG_TOKEN / TG_CHAT_ID")

st.sidebar.divider()

initial_capital = st.sidebar.number_input(
    "Initial Capital (USD)",
    min_value=1000.0,
    value=float(DEFAULT_INITIAL_CAPITAL),
    step=1000.0
)

enable_auto_scan = st.sidebar.toggle("🤖 啟用自動掃描", value=True)


# ===============================
# 11. Load Core Data
# ===============================
trades_df = load_trades()
portfolio_raw, cash, total_realized_pl = build_portfolio(trades_df, initial_capital)
market_value = sum(x["MarketValue"] for x in portfolio_raw)
total_assets = cash + market_value
total_pl = total_assets - initial_capital

market_regime = get_market_regime()
portfolio = enrich_portfolio_with_weight_and_risk(portfolio_raw, total_assets, cash, market_regime) if portfolio_raw else []

history_df = sync_nav_history(
    total_assets=total_assets,
    cash=cash,
    market_value=market_value,
    total_pl=total_pl
)

if enable_auto_scan and portfolio:
    run_auto_scanner(portfolio, trades_df, cash, total_assets, market_regime)


# ===============================
# 12. Main UI
# ===============================
st.title(APP_TITLE)
st.caption(f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("NAV", f"${total_assets:,.2f}")
m2.metric("Cash", f"${cash:,.2f}")
m3.metric("Market Value", f"${market_value:,.2f}")
m4.metric("Realized P/L", f"${total_realized_pl:,.2f}")
m5.metric("Unrealized P/L", f"${sum(p['Unrealized'] for p in portfolio):,.2f}")
m6.metric("Total P/L", f"${total_pl:,.2f}", f"{(total_pl / initial_capital * 100):.2f}%")

st.info(f"📡 Market Regime: {market_regime['regime']} | Score: {market_regime['score']}")

tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "📝 Trade Center", "🎯 Strategy Center", "⚙️ Monitor"])


# ===============================
# Tab 1 Dashboard
# ===============================
with tab1:
    left, right = st.columns([6, 4])

    with left:
        st.subheader("📈 NAV Curve")
        if history_df is not None and not history_df.empty:
            fig_nav = go.Figure()
            fig_nav.add_trace(
                go.Scatter(
                    x=history_df["Date"],
                    y=history_df["Total Assets"],
                    mode="lines+markers",
                    fill="tozeroy",
                    name="NAV",
                    line=dict(color="#00FFCC", width=2),
                )
            )
            fig_nav.update_layout(
                template="plotly_dark",
                height=320,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_title="Date",
                yaxis_title="Total Assets",
            )
            st.plotly_chart(fig_nav, use_container_width=True)
        else:
            st.info("尚無 NAV 歷史資料。")

    with right:
        st.subheader("📌 Portfolio Snapshot")
        invested_ratio = (market_value / total_assets * 100) if total_assets > 0 else 0
        cash_ratio = (cash / total_assets * 100) if total_assets > 0 else 0
        max_weight = max([p["WeightPct"] for p in portfolio], default=0)

        s1, s2, s3 = st.columns(3)
        s1.metric("Invested", f"{invested_ratio:.1f}%")
        s2.metric("Cash Ratio", f"{cash_ratio:.1f}%")
        s3.metric("Max Position", f"{max_weight:.1f}%")

        st.markdown("### 持倉分布")
        if portfolio:
            pie_fig = go.Figure(data=[
                go.Pie(
                    labels=[p["Ticker"] for p in portfolio],
                    values=[p["MarketValue"] for p in portfolio],
                    hole=0.45
                )
            ])
            pie_fig.update_layout(template="plotly_dark", height=320, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(pie_fig, use_container_width=True)
        else:
            st.info("目前無持倉。")

    st.subheader("📋 Current Holdings")
    if portfolio:
        holdings_df = pd.DataFrame(portfolio)
        display_cols = [
            "Ticker", "Shares", "AvgCost", "FIFOCostBasis", "LastPrice", "MarketValue",
            "Unrealized", "PL_Pct", "WeightPct", "ATR", "StopLoss",
            "TakeProfit", "TrailingStop", "Signal", "Divergence"
        ]
        holdings_df = holdings_df[display_cols].sort_values("MarketValue", ascending=False)

        for col in ["ATR", "StopLoss", "TakeProfit", "TrailingStop", "FIFOCostBasis"]:
            if col in holdings_df.columns:
                holdings_df[col] = pd.to_numeric(holdings_df[col], errors="coerce")

        styled = holdings_df.style.applymap(color_pl, subset=["Unrealized", "PL_Pct"]).format({
            "AvgCost": "${:,.2f}",
            "FIFOCostBasis": "${:,.2f}",
            "LastPrice": "${:,.2f}",
            "MarketValue": "${:,.2f}",
            "Unrealized": "${:,.2f}",
            "PL_Pct": "{:.2f}%",
            "WeightPct": "{:.2f}%",
            "ATR": "{:.2f}",
            "StopLoss": "${:,.2f}",
            "TakeProfit": "${:,.2f}",
            "TrailingStop": "${:,.2f}",
        })
        st.dataframe(styled, use_container_width=True)
    else:
        st.info("尚無持倉資料。")


# ===============================
# Tab 2 Trade Center
# ===============================
with tab2:
    st.subheader("📝 Add New Trade")
    sp500_list = get_sp500_tickers()

    with st.form("trade_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)

        with c1:
            manual_input = st.checkbox("Manual Ticker Input", value=True)
            if manual_input:
                ticker_input = normalize_ticker(
                    st.text_input("Ticker", value=st.session_state.get("trade_ticker", "NVDA"))
                )
            else:
                default_ticker = st.session_state.get("trade_ticker", "NVDA")
                default_index = 0
                for i, item in enumerate(sp500_list):
                    if item.startswith(default_ticker + " -"):
                        default_index = i
                        break
                selected_stock = st.selectbox("Search Stock", options=sp500_list, index=default_index)
                ticker_input = normalize_ticker(selected_stock.split(" - ")[0]) if selected_stock else ""

        with c2:
            default_trade_type = st.session_state.get("trade_type", "BUY")
            trade_type = st.selectbox("Type", ["BUY", "SELL"], index=0 if default_trade_type == "BUY" else 1)
            trade_date = st.date_input("Date", value=date.today())

        with c3:
            trade_price = st.number_input(
                "Price",
                min_value=0.01,
                value=float(st.session_state.get("trade_price", 100.00)),
                format="%.2f"
            )
            trade_shares = st.number_input(
                "Shares",
                min_value=0.0001,
                value=float(st.session_state.get("trade_shares", 1.0)),
                format="%.4f"
            )

        note = st.text_input("Note", value=st.session_state.get("trade_note", ""))
        submitted = st.form_submit_button("☁️ Sync to Cloud")

        if submitted:
            ok, msg = save_trade(
                trade_date=trade_date,
                ticker=ticker_input,
                trade_type=trade_type,
                price=trade_price,
                shares=trade_shares,
                note=note,
            )
            if ok:
                st.success(msg)
                st.session_state["trade_ticker"] = ticker_input
                st.session_state["trade_type"] = trade_type
                st.session_state["trade_price"] = trade_price
                st.session_state["trade_shares"] = trade_shares
                st.session_state["trade_note"] = note
                st.rerun()
            else:
                st.error(msg)

    st.divider()
    st.subheader("📚 Trade Records")
    if not trades_df.empty:
        show_df = trades_df.copy()
        show_df["Date"] = show_df["Date"].dt.strftime("%Y-%m-%d")
        show_df["Price"] = show_df["Price"].round(2)
        show_df["Shares"] = show_df["Shares"].round(4)
        show_df["Total"] = show_df["Total"].round(2)
        st.dataframe(show_df.sort_values("Date", ascending=False), use_container_width=True)
    else:
        st.info("尚無交易資料。")


# ===============================
# Tab 3 Strategy Center
# ===============================
with tab3:
    st.subheader("🎯 Strategy Decision Center")

    sp500_list = get_sp500_tickers()
    analysis_mode = st.radio("選擇分析對象", ["我的持股", "搜尋全市場標的"], horizontal=True)

    if analysis_mode == "我的持股":
        available = [p["Ticker"] for p in portfolio] if portfolio else ["NVDA"]
        analyze_ticker = st.selectbox("選擇標的", options=available)
    else:
        col_a, col_b = st.columns([1, 2])
        with col_a:
            search_manual = st.checkbox("手動輸入代碼", value=False)
        with col_b:
            if search_manual:
                analyze_ticker = normalize_ticker(st.text_input("請輸入代碼", value="NVDA"))
            else:
                selected_s = st.selectbox("從 S&P 500 搜尋", options=sp500_list)
                analyze_ticker = normalize_ticker(selected_s.split(" - ")[0]) if selected_s else "NVDA"

    hist = get_unified_analysis(analyze_ticker)

    if hist is None or hist.empty:
        st.error("無法取得該股票資料。")
    else:
        last = hist.iloc[-1]
        current_price = float(last["Close"])

        held_shares = 0.0
        current_mkt_value = 0.0
        for p in portfolio:
            if p["Ticker"] == analyze_ticker:
                held_shares = p["Shares"]
                current_mkt_value = p["MarketValue"]
                break

        recent_buy, recent_sell = get_recent_trade_status(analyze_ticker, trades_df)
        score, action, details, note = evaluate_strategy(
            ticker=analyze_ticker,
            hist=hist,
            held_shares=held_shares,
            current_mkt_value=current_mkt_value,
            total_assets=total_assets,
            cash=cash,
            market_regime=market_regime
        )

        left, right = st.columns([3, 7])

        with left:
            st.subheader(f"🛠️ {analyze_ticker}")
            st.markdown(
                f'<div class="price-box">現價: <span style="font-size: 1.8rem;">${current_price:.2f}</span></div>',
                unsafe_allow_html=True
            )

            st.write(f"**Strategy Score:** `{display_na(score, decimals=1)}`")
            st.write(f"**Current Weight:** `{display_na(details.get('current_weight', 0) * 100, suffix='%', decimals=2)}`")
            st.write(f"**Held Shares:** `{display_na(held_shares, decimals=4)}`")
            st.write(f"**RSI:** `{display_na(details.get('rsi'), decimals=1)}`")
            st.write(f"**ATR:** `{display_na(details.get('atr'), decimals=2)}`")
            st.write(f"**Market Regime:** `{details.get('market_regime', 'N/A')}`")
            st.write(f"**Volume Divergence:** `{display_divergence(details.get('divergence', 'NONE'))}`")

            if recent_buy:
                st.info(f"⏳ 建倉冷卻中：{COOLDOWN_DAYS} 天內已有買入紀錄")
            if recent_sell:
                st.info(f"⏳ 減碼冷卻中：{COOLDOWN_DAYS} 天內已有賣出紀錄")

            quick_trade_type = "BUY"
            quick_trade_qty = max(1, int(details["suggested_buy_qty"])) if details["suggested_buy_qty"] >= 1 else 1
            quick_trade_price = float(details["target_buy_price"]) if details["target_buy_price"] > 0 else float(details["close"])
            quick_trade_note = f"Strategy quick order | Score={score:.1f} | {action}"

            if action == "BUY" and not recent_buy:
                st.success("🔥 建議：分批買入")
                st.markdown(f"- 建議買入股數：`{details['suggested_buy_qty']}`")
                st.markdown(f"- 建議進場價：`${details['target_buy_price']:.2f}`")
                st.markdown(f"- 風險股數上限：`{details['qty_by_risk']}`")
                st.markdown(f"- 權重股數上限：`{details['qty_by_weight']}`")
                st.markdown(f"- 現金股數上限：`{details['qty_by_cash']}`")

                quick_trade_type = "BUY"
                quick_trade_qty = max(1, int(details["suggested_buy_qty"])) if details["suggested_buy_qty"] >= 1 else 1
                quick_trade_price = float(details["target_buy_price"])

            elif action == "SELL" and not recent_sell and held_shares > 0:
                st.error("⚠️ 建議：分批減碼")
                st.markdown(f"- 建議賣出股數：`{details['suggested_sell_qty']}`")
                st.markdown(f"- 建議出場價：`${details['target_sell_price']:.2f}`")
                st.markdown(f"- 移動止損價：`${display_na(details.get('trailing_stop'), decimals=2)}`")

                quick_trade_type = "SELL"
                quick_trade_qty = max(1, int(details["suggested_sell_qty"])) if details["suggested_sell_qty"] >= 1 else 1
                quick_trade_price = float(details["target_sell_price"])

            elif action == "BUY_READY" and not recent_buy:
                st.warning("🟡 預警：接近買入區間，可準備掛單")
                st.markdown(f"- 目標價：`${details['target_buy_price']:.2f}`")
                st.markdown(
                    f"- 預警區間：`${details['target_buy_price']*(1-PRE_ALERT_PCT):.2f}` ~ `${details['target_buy_price']*(1+PRE_ALERT_PCT):.2f}`"
                )
                st.markdown(f"- 計畫股數：`{details['suggested_buy_qty']}`")

                quick_trade_type = "BUY"
                quick_trade_qty = max(1, int(details["suggested_buy_qty"])) if details["suggested_buy_qty"] >= 1 else 1
                quick_trade_price = float(details["target_buy_price"])

            elif action == "SELL_READY" and not recent_sell and held_shares > 0:
                st.warning("🟠 預警：接近賣出區間，可準備減碼")
                st.markdown(f"- 目標價：`${details['target_sell_price']:.2f}`")
                st.markdown(
                    f"- 預警區間：`${details['target_sell_price']*(1-PRE_ALERT_PCT):.2f}` ~ `${details['target_sell_price']*(1+PRE_ALERT_PCT):.2f}`"
                )
                st.markdown(f"- 計畫股數：`{details['suggested_sell_qty']}`")

                quick_trade_type = "SELL"
                quick_trade_qty = max(1, int(details["suggested_sell_qty"])) if details["suggested_sell_qty"] >= 1 else 1
                quick_trade_price = float(details["target_sell_price"])

            else:
                st.warning("⚖️ 建議：觀望")

            st.divider()
            st.markdown("**風控建議**")
            st.markdown(f"- Stop Loss：`{display_na(details.get('stop_loss'), prefix='$')}`")
            st.markdown(f"- Take Profit：`{display_na(details.get('take_profit'), prefix='$')}`")
            st.markdown(f"- Trailing Stop：`{display_na(details.get('trailing_stop'), prefix='$')}`")

            st.divider()
            st.markdown("**訊號原因**")
            for r in details["reasons"]:
                st.write(f"- {r}")
            if not details["reasons"]:
                st.write("- 暫無強訊號")
            st.caption(note)

            st.divider()
            st.markdown("### ⚡ 快速填入交易單")
            if st.button("帶入交易中心表單", key=f"quick_fill_{analyze_ticker}_{action}"):
                st.session_state["trade_ticker"] = analyze_ticker
                st.session_state["trade_type"] = quick_trade_type
                st.session_state["trade_price"] = round(quick_trade_price, 2)
                st.session_state["trade_shares"] = float(quick_trade_qty)
                st.session_state["trade_note"] = quick_trade_note
                st.success("已帶入交易中心表單，請切換到 Tab 2 確認送出。")

        with right:
            plot_df = hist.tail(120)

            fig = make_subplots(
                rows=4, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.04,
                row_heights=[0.50, 0.16, 0.17, 0.17]
            )

            fig.add_trace(
                go.Candlestick(
                    x=plot_df.index,
                    open=plot_df["Open"],
                    high=plot_df["High"],
                    low=plot_df["Low"],
                    close=plot_df["Close"],
                    name="Candlestick"
                ),
                row=1, col=1
            )

            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["SMA20"], name="SMA20", line=dict(color="#17BECF", width=1.3)), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["SMA50"], name="SMA50", line=dict(color="#FF9800", width=1.2)), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["SMA200"], name="SMA200", line=dict(color="#D62728", width=1.2)), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["BB_upper"], name="BB Upper", line=dict(color="rgba(173,216,230,0.8)", width=1)), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["BB_lower"], name="BB Lower", line=dict(color="rgba(173,216,230,0.8)", width=1)), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["TrailingStop"], name="Trailing Stop", line=dict(color="#FF5252", width=1.2, dash="dash")), row=1, col=1)

            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["RSI"], name="RSI", line=dict(color="#00E676", width=1.2)), row=2, col=1)
            fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)

            fig.add_trace(
                go.Bar(
                    x=plot_df.index,
                    y=plot_df["MACD_Hist"],
                    name="MACD Hist",
                    marker_color=["#26A69A" if x >= 0 else "#EF5350" for x in plot_df["MACD_Hist"]]
                ),
                row=3, col=1
            )
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["MACD"], name="MACD", line=dict(color="#42A5F5", width=1.2)), row=3, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["MACD_Signal"], name="Signal", line=dict(color="#FFCA28", width=1.2)), row=3, col=1)

            fig.add_trace(go.Bar(x=plot_df.index, y=plot_df["Volume"], name="Volume", marker_color="#7E57C2"), row=4, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["VOL_SMA20"], name="VOL SMA20", line=dict(color="#FFD54F", width=1.2)), row=4, col=1)

            fig.update_layout(
                template="plotly_dark",
                height=860,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_rangeslider_visible=False,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
            )

            fig.update_yaxes(title_text="Price", row=1, col=1)
            fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100])
            fig.update_yaxes(title_text="MACD", row=3, col=1)
            fig.update_yaxes(title_text="Volume", row=4, col=1)

            st.plotly_chart(fig, use_container_width=True)


# ===============================
# Tab 4 Monitor
# ===============================
with tab4:
    st.subheader("⚙️ System Monitor")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### 系統狀態")
        st.write(f"- 自動掃描：`{'啟用' if enable_auto_scan else '停用'}`")
        st.write(f"- Telegram Token：`{'已設定' if TG_TOKEN else '未設定'}`")
        st.write(f"- Telegram Chat ID：`{'已設定' if TG_CHAT_ID else '未設定'}`")
        st.write(f"- 交易筆數：`{len(trades_df)}`")
        st.write(f"- 持倉檔數：`{len(portfolio)}`")
        st.write(f"- 更新時間：`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")
        st.write(f"- Market Session：`{get_market_session()}`")

    with c2:
        st.markdown("### 風險參數")
        st.write(f"- 單一持股上限：`{MAX_POSITION_WEIGHT*100:.0f}%`")
        st.write(f"- 單筆風險上限：`{RISK_PER_TRADE_PCT*100:.0f}%`")
        st.write(f"- 現金保留：`{CASH_RESERVE_PCT*100:.0f}%`")
        st.write(f"- 冷卻期：`{COOLDOWN_DAYS}` 天")
        st.write(f"- 預警區間：`±{PRE_ALERT_PCT*100:.1f}%`")
        st.write(f"- Alert 最短重發間隔：`{ALERT_MIN_MINUTES}` 分鐘")

    st.divider()

    with st.expander("Alerts Log", expanded=False):
        alerts_df = load_alerts()
        if not alerts_df.empty:
            st.dataframe(alerts_df.sort_values("DateTime", ascending=False), use_container_width=True)
        else:
            st.info("目前沒有 Alerts 紀錄。")

    with st.expander("Debug - Trades Data", expanded=False):
        st.write("Trades rows:", len(trades_df))
        st.dataframe(trades_df, use_container_width=True)

    if st.button("清除快取"):
        st.cache_data.clear()
        st.success("已清除完成，請重新整理。")

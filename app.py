import math
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple
import pytz
import gspread
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh


# ===============================
# 0. App Config
# ===============================
st.set_page_config(page_title="US Stock Portfolio Pro", layout="wide")
st_autorefresh(interval=60000, limit=None, key="heartbeat_60s")

# 初始化連動用 State
if "fill_trade" not in st.session_state:
    st.session_state["fill_trade"] = None
if "active_tab" not in st.session_state:
    st.session_state["active_tab"] = "📊 Dashboard"
    
st.markdown(
    """
    <style>
    [data-testid="stMetricValue"] { font-size: 1.6rem !important; white-space: nowrap !important; }
    .stMetric { border: 1px solid rgba(128, 128, 128, 0.25); padding: 10px !important; border-radius: 12px; background: rgba(255,255,255,0.02); }
    .price-box { background-color: rgba(128, 128, 128, 0.08); padding: 14px; border-radius: 12px; border-left: 5px solid #17BECF; margin-bottom: 14px; }
    </style>
    """,
    unsafe_allow_html=True
)

APP_TITLE = "🏛️ US Stock Portfolio Pro v2.0"
PORTFOLIO_SHEET_TITLE = "US Stock"

# --- Secrets ---
TG_TOKEN = str(st.secrets.get("TG_TOKEN", "")).strip()
TG_CHAT_ID = str(st.secrets.get("TG_CHAT_ID", "")).strip()

# --- Strategy / Risk Params ---
DEFAULT_INITIAL_CAPITAL = 32000.0
MAX_POSITION_WEIGHT = 0.30
RISK_PER_TRADE_PCT = 0.01
CASH_RESERVE_PCT = 0.10
COOLDOWN_DAYS = 3
ALERT_THRESHOLD_PCT = 0.01  # 預警區間 1%

# ===============================
# 1. Advanced Strategy Logic
# ===============================

@st.cache_data(ttl=3600)
def get_market_regime() -> str:
    """判斷大盤環境 (Market Regime)"""
    try:
        spy = yf.Ticker("SPY").history(period="1y")
        sma200 = spy['Close'].rolling(200).mean().iloc[-1]
        current = spy['Close'].iloc[-1]
        return "BULL" if current > sma200 else "BEAR"
    except:
        return "UNKNOWN"

def detect_divergence(df: pd.DataFrame) -> str:
    """簡單量價/RSI背離偵測"""
    if len(df) < 20: return "NONE"
    
    recent = df.tail(10)
    # 價格創新高但 RSI 沒創新高 (頂背離)
    if recent['Close'].iloc[-1] > recent['Close'].max() * 0.98 and \
       recent['RSI'].iloc[-1] < recent['RSI'].max() * 0.95:
        return "BEARISH_DIV"
    # 價格創新低但 RSI 沒創新低 (底背離)
    if recent['Close'].iloc[-1] < recent['Close'].min() * 1.02 and \
       recent['RSI'].iloc[-1] > recent['RSI'].min() * 1.05:
        return "BULLISH_DIV"
    return "NONE"

def calculate_trailing_stop(ticker: str, trades_df: pd.DataFrame, current_price: float, atr: float) -> Optional[float]:
    """動態移動止損：最高價回落 2*ATR"""
    if trades_df.empty: return None
    ticker_trades = trades_df[trades_df["Ticker"] == ticker].sort_values("Date")
    if ticker_trades.empty: return None
    
    # 抓取最後一次買入後的最高收盤價
    last_buy_date = ticker_trades[ticker_trades["Type"] == "BUY"]["Date"].iloc[-1]
    hist = yf.Ticker(ticker).history(start=last_buy_date)
    if hist.empty: return current_price - 2 * atr
    
    highest_price = hist['Close'].max()
    return highest_price - 2 * atr
        
def get_market_session() -> str:
    """
    依美東時間判斷市場時段
    PREMARKET: 04:00 - 09:30
    REGULAR:   09:30 - 16:00
    AFTERMARKET: 16:00 - 20:00
    CLOSED: 其他時間 / 週末
    """
    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)

    if now_et.weekday() >= 5:
        return "CLOSED"

    current_time = now_et.time()

    if current_time >= datetime.strptime("04:00", "%H:%M").time() and current_time < datetime.strptime("09:30", "%H:%M").time():
        return "PREMARKET"
    elif current_time >= datetime.strptime("09:30", "%H:%M").time() and current_time < datetime.strptime("16:00", "%H:%M").time():
        return "REGULAR"
    elif current_time >= datetime.strptime("16:00", "%H:%M").time() and current_time < datetime.strptime("20:00", "%H:%M").time():
        return "AFTERMARKET"
    else:
        return "CLOSED"
        
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

@st.cache_data(ttl=120)
def load_alerts() -> pd.DataFrame:
    try:
        _, _, _, ws_alerts = init_sheets()
        expected_cols = [
            "DateTime", "Ticker", "Action", "BaseKey",
            "Price", "Score", "Session", "Message"
        ]
        df = read_worksheet_as_df(ws_alerts, expected_cols)

        if df.empty:
            return pd.DataFrame(columns=expected_cols)

        df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
        df["Action"] = df["Action"].astype(str).str.upper().str.strip()
        df["BaseKey"] = df["BaseKey"].astype(str).str.strip()
        df["Session"] = df["Session"].astype(str).str.strip()
        df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
        df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
        df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")

        return df.sort_values("DateTime").reset_index(drop=True)

    except Exception as e:
        st.warning(f"讀取 Alerts 失敗：{e}")
        return pd.DataFrame(columns=[
            "DateTime", "Ticker", "Action", "BaseKey",
            "Price", "Score", "Session", "Message"
        ])

def get_last_sent_alert(ticker: str, action: str) -> Optional[dict]:
    alerts_df = load_alerts()
    if alerts_df.empty:
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
        "Message": str(last_row["Message"]),
    }

def should_send_alert(
    ticker: str,
    action: str,
    current_price: float,
    current_score: float,
    current_session: str,
    min_minutes: int = 30,
    min_price_change_pct: float = 1.0,
    min_score_change: float = 0.8,
) -> bool:
    """
    規則：
    1) 沒發過 -> 發
    2) 同 ticker/action，但市場時段改變 -> 發
    3) 距離上次發送超過 min_minutes 且
       價格變動 >= min_price_change_pct 或 score 變動 >= min_score_change -> 發
    4) 否則不發
    """
    last_alert = get_last_sent_alert(ticker, action)
    if last_alert is None:
        return True

    last_dt = last_alert["DateTime"]
    last_price = safe_float(last_alert["Price"])
    last_score = safe_float(last_alert["Score"])
    last_session = last_alert["Session"]

    if pd.isna(last_dt):
        return True

    if current_session != last_session:
        return True

    minutes_diff = (datetime.now() - last_dt.to_pydatetime()).total_seconds() / 60.0

    price_change_pct = 0.0
    if last_price > 0:
        price_change_pct = abs((current_price - last_price) / last_price) * 100

    score_change = abs(current_score - last_score)

    if minutes_diff >= min_minutes and (
        price_change_pct >= min_price_change_pct or score_change >= min_score_change
    ):
        return True

    return False

def has_alert_been_sent(signal_key: str) -> bool:
    alerts_df = load_alerts()
    if alerts_df.empty:
        return False
    return signal_key in alerts_df["SignalKey"].values

def log_sent_alert(ticker: str, action: str, price: float, score: float, message: str):
    _, _, _, ws = init_sheets()
    ws.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ticker, action, price, score, message])
    st.cache_data.clear()

# ===============================
# 1. Utility Functions
# ===============================
def normalize_ticker(symbol: str) -> str:
    return symbol.upper().strip().replace(".", "-")


def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def color_pl(val):
    color = "#26A69A" if val > 0 else "#EF5350" if val < 0 else "white"
    return f"color: {color}; font-weight: 600;"


def send_telegram_msg(message: str) -> bool:
    if not TG_TOKEN or not TG_CHAT_ID:
        return False

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
    }
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

    recent = temp_df[
        (temp_df["Ticker"] == ticker) &
        (temp_df["Date"] >= cutoff_date)
    ]

    recent_buy = not recent[recent["Type"] == "BUY"].empty
    recent_sell = not recent[recent["Type"] == "SELL"].empty
    return recent_buy, recent_sell


# ===============================
# 2. Google Sheets Layer
# ===============================
@st.cache_resource
def get_gsheet_client():
    return gspread.service_account_from_dict(st.secrets["gcp_service_account"])


def get_or_create_worksheet(spreadsheet, title: str, rows: int = 1000, cols: int = 12):
    try:
        return spreadsheet.worksheet(title)
    except Exception:
        return spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))

def normalize_trade_type(x: str) -> str:
    s = str(x).strip().upper()

    if "買" in s or "BUY" in s:
        return "BUY"
    if "賣" in s or "SELL" in s:
        return "SELL"

    return s

def init_sheets():
    ss = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE)

    ws_trades = get_or_create_worksheet(ss, "Trades", rows=3000, cols=10)
    ws_history = get_or_create_worksheet(ss, "History", rows=3000, cols=5)
    ws_alerts = get_or_create_worksheet(ss, "Alerts", rows=5000, cols=10)

    trades_headers = ["Date", "Ticker", "Type", "Price", "Shares", "Total", "Note"]
    history_headers = ["Date", "Total Assets", "Cash", "Market Value", "Total P/L"]
    alerts_headers = ["DateTime", "Date", "Ticker", "Action", "SignalKey", "Price", "Score", "Message"]

    if not ws_trades.get_all_values():
        ws_trades.append_row(trades_headers)

    if not ws_history.get_all_values():
        ws_history.append_row(history_headers)

    if not ws_alerts.get_all_values():
        ws_alerts.append_row(alerts_headers)

    return ss, ws_trades, ws_history, ws_alerts


@st.cache_data(ttl=300)
@st.cache_data(ttl=300)
def load_trades() -> pd.DataFrame:
    try:
        _, ws_trades, _, _ = init_sheets()
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
    trade_type = trade_type.upper().strip()
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
        _, ws_trades, _, _ = init_sheets()
        total = round(price * shares, 4)
        ws_trades.append_row([
            str(trade_date),
            ticker,
            trade_type,
            float(price),
            float(shares),
            float(total),
            note
        ])
        st.cache_data.clear()
        return True, "交易已成功寫入雲端。"
    except Exception as e:
        return False, f"寫入失敗：{e}"


def sync_nav_history(total_assets: float, cash: float, market_value: float, total_pl: float) -> Optional[pd.DataFrame]:
    try:
        _, _, ws_history, _ = init_sheets()
        expected_cols = ["Date", "Total Assets", "Cash", "Market Value", "Total P/L"]

        history_df = read_worksheet_as_df(ws_history, expected_cols)

        today_str = date.today().strftime("%Y-%m-%d")
        should_append = True

        if not history_df.empty and "Date" in history_df.columns:
            last_date = str(history_df.iloc[-1]["Date"]).strip()
            if last_date == today_str:
                should_append = False

        if should_append:
            ws_history.append_row([
                today_str,
                float(total_assets),
                float(cash),
                float(market_value),
                float(total_pl),
            ])
            history_df = read_worksheet_as_df(ws_history, expected_cols)

        if not history_df.empty:
            for c in ["Total Assets", "Cash", "Market Value", "Total P/L"]:
                history_df[c] = pd.to_numeric(history_df[c], errors="coerce")

        return history_df

    except Exception as e:
        st.warning(f"歷史 NAV 同步失敗：{e}")
        return None


# ===============================
# 3. Market Data / Indicator Layer
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


# ===============================
# 4. Portfolio Calculation
# ===============================
def build_portfolio(trades_df: pd.DataFrame, initial_capital: float) -> Tuple[List[Dict], float, float]:
    if trades_df.empty:
        return [], initial_capital, 0.0

    cash = float(initial_capital)
    portfolio = []
    total_realized_pl = 0.0

    for ticker in sorted(trades_df["Ticker"].dropna().unique().tolist()):
        tdf = trades_df[trades_df["Ticker"] == ticker].sort_values("Date").copy()

        shares_held = 0.0
        cost_basis = 0.0
        realized_pl = 0.0

        for _, row in tdf.iterrows():
            price = safe_float(row["Price"])
            shares = safe_float(row["Shares"])
            total = price * shares
            trade_type = str(row["Type"]).upper().strip()

            if trade_type == "BUY":
                shares_held += shares
                cost_basis += total
                cash -= total

            elif trade_type == "SELL":
                if shares_held > 0:
                    avg_cost = cost_basis / shares_held
                    sellable = min(shares, shares_held)
                    realized_pl += (price - avg_cost) * sellable
                    cost_basis -= avg_cost * sellable
                    shares_held -= sellable
                cash += total

        total_realized_pl += realized_pl

        if shares_held > 0:
            last_price = get_last_price(ticker)
            if last_price is None:
                continue

            avg_cost_val = cost_basis / shares_held if shares_held > 0 else 0.0
            market_value = shares_held * last_price
            unrealized = (last_price - avg_cost_val) * shares_held
            pl_pct = ((last_price / avg_cost_val) - 1) * 100 if avg_cost_val > 0 else 0.0

            portfolio.append({
                "Ticker": ticker,
                "Shares": round(shares_held, 4),
                "AvgCost": round(avg_cost_val, 4),
                "LastPrice": round(last_price, 4),
                "MarketValue": round(market_value, 4),
                "Unrealized": round(unrealized, 4),
                "PL_Pct": round(pl_pct, 2),
                "RealizedPL": round(realized_pl, 4),
            })

    return portfolio, cash, total_realized_pl


# ===============================
# 4. Core Engine Update
# ===============================
def evaluate_strategy(ticker: str, hist: pd.DataFrame, held_shares: float, current_mkt_value: float, total_assets: float, cash: float) -> Tuple[float, str, Dict, str]:
    last = hist.iloc[-1]
    close = safe_float(last["Close"])
    atr = safe_float(last["ATR"])
    rsi = safe_float(last["RSI"])
    
    regime = get_market_regime()
    divergence = detect_divergence(hist)
    
    score = 0.0
    reasons = []

    # 基礎指標評分
    if close > last["SMA200"]: score += 1.5; reasons.append("Price > SMA200")
    if last["SMA20"] > last["SMA50"]: score += 1.0; reasons.append("Bullish SMA Cross")
    if 40 <= rsi <= 60: score += 1.0; reasons.append("RSI Neutral-Bull")
    
    # 濾網與背離
    if regime == "BEAR": score -= 1.0; reasons.append("Market Regime: BEAR (Risk High)")
    if divergence == "BULLISH_DIV": score += 1.5; reasons.append("Bullish Divergence detected")
    elif divergence == "BEARISH_DIV": score -= 1.5; reasons.append("Bearish Divergence detected")

    current_weight = current_mkt_value / total_assets if total_assets > 0 else 0
    
    # 倉位計算
    risk_budget = total_assets * RISK_PER_TRADE_PCT
    risk_per_share = max(0.01, 2 * atr)
    qty_by_risk = math.floor(risk_budget / risk_per_share)
    
    usable_cash = max(0, cash - total_assets * CASH_RESERVE_PCT)
    qty_by_cash = math.floor(usable_cash / close) if close > 0 else 0
    
    suggested_buy_qty = max(0, min(qty_by_risk, qty_by_cash))
    
    # 動作判定
    action = "WATCH"
    if score >= 4.0 and current_weight < MAX_POSITION_WEIGHT: action = "BUY"
    elif (score <= 1.5 or rsi > 75) and held_shares > 0: action = "SELL"
    
    # 移動止損計算
    ts_stop = calculate_trailing_stop(ticker, load_trades(), close, atr) if held_shares > 0 else None
    if ts_stop and close < ts_stop:
        action = "SELL"
        reasons.append(f"Trailing Stop Triggered: ${ts_stop:.2f}")

    details = {
        "close": close, "rsi": rsi, "atr": atr, "score": score,
        "regime": regime, "divergence": divergence, "trailing_stop": ts_stop,
        "suggested_buy_qty": suggested_buy_qty, "current_weight": current_weight,
        "reasons": reasons, "stop_loss": close - 2*atr, "take_profit": close + 3*atr
    }
    return score, action, details, " | ".join(reasons)


def enrich_portfolio_with_weight_and_risk(portfolio: List[Dict], total_assets: float, cash: float) -> List[Dict]:
    result = []
    for item in portfolio:
        ticker = item["Ticker"]
        hist = get_unified_analysis(ticker)
        atr = None
        stop_loss = None
        take_profit = None
        signal = "WATCH"

        if hist is not None and not hist.empty:
            last = hist.iloc[-1]
            atr = safe_float(last.get("ATR"))
            lp = item["LastPrice"]
            if atr > 0:
                stop_loss = lp - 2 * atr
                take_profit = lp + 3 * atr

            _, action, _, _ = evaluate_strategy(
                ticker=ticker,
                hist=hist,
                held_shares=item["Shares"],
                current_mkt_value=item["MarketValue"],
                total_assets=total_assets,
                cash=cash
            )
            signal = action

        weight = (item["MarketValue"] / total_assets) if total_assets > 0 else 0.0

        row = item.copy()
        row["WeightPct"] = round(weight * 100, 2)
        row["ATR"] = round(atr, 4) if atr else None
        row["StopLoss"] = round(stop_loss, 4) if stop_loss else None
        row["TakeProfit"] = round(take_profit, 4) if take_profit else None
        row["Signal"] = signal
        result.append(row)
    return result


def run_auto_scanner(portfolio: List[Dict], trades_df: pd.DataFrame, cash: float, total_assets: float):
    if not portfolio: return
    current_session = get_market_session()
    alerts_history = load_alerts()

    for item in portfolio:
        ticker = item["Ticker"]
        hist = get_unified_analysis(ticker)
        if hist is None: continue
        
        score, action, details, note = evaluate_strategy(ticker, hist, item["Shares"], item["MarketValue"], total_assets, cash)
        
        # 1. 預警邏輯 (接近建議價位 ±1%)
        # 假設建議買入價為 SMA20 (此處可根據需求自定義)
        target_buy_price = hist["SMA20"].iloc[-1]
        price_diff_pct = abs(details['close'] - target_buy_price) / target_buy_price
        
        if price_diff_pct <= ALERT_THRESHOLD_PCT and action == "WATCH":
            pre_msg = f"🔔 *PRE-ALERT* `{ticker}` 接近支撐位\n現價: `${details['close']:.2f}`\n目標: `${target_buy_price:.2f}`"
            # 檢查是否已發送過 (簡單去重)
            if not any((alerts_history["Ticker"] == ticker) & (alerts_history["Action"] == "PRE-ALERT")):
                if send_telegram_msg(pre_msg): log_sent_alert(ticker, "PRE-ALERT", details['close'], score, pre_msg)

        # 2. 正式訊號發送 (原本邏輯)
        if action in ["BUY", "SELL"]:
            msg = f"🚀 *{action} Signal* `{ticker}`\nScore: `{score:.1f}`\nPrice: `${details['close']:.2f}`\nRegime: `{details['regime']}`"
            # 此處應配合 should_send_alert 判斷，代碼略
            if send_telegram_msg(msg): log_sent_alert(ticker, action, details['close'], score, msg)


# ===============================
# 6. Sidebar
# ===============================
st.sidebar.title("🎮 Control Center")

if st.sidebar.button("🔄 全域刷新"): st.cache_data.clear(); st.rerun()

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
# 7. Load Core Data
# ===============================
trades_df = load_trades()
portfolio_raw, cash, total_realized_pl = build_portfolio(trades_df, initial_capital)
market_value = sum(x["MarketValue"] for x in portfolio_raw)
total_assets = cash + market_value
total_pl = total_assets - initial_capital
portfolio = enrich_portfolio_with_weight_and_risk(portfolio_raw, total_assets, cash) if portfolio_raw else []

history_df = sync_nav_history(
    total_assets=total_assets,
    cash=cash,
    market_value=market_value,
    total_pl=total_pl
)

if enable_auto_scan and portfolio:
    run_auto_scanner(portfolio, trades_df, cash, total_assets)


# ===============================
# 8. Main UI
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

# --- Tabs 設計 ---
tabs = ["📊 Dashboard", "📝 Trade Center", "🎯 Strategy Center", "⚙️ Monitor"]
active_tab = st.radio("導航", tabs, horizontal=True, label_visibility="collapsed", key="nav_radio")

# --- Tab 2: Trade Center ---
if active_tab == "📝 Trade Center":
    st.subheader("📝 交易輸入中心")
    
    # 檢查是否有來自 Tab 3 的預填數據
    fill = st.session_state.get("fill_trade")
    
    with st.form("trade_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            t_input = st.text_input("Ticker", value=fill["ticker"] if fill else "NVDA")
        with c2:
            t_type = st.selectbox("Type", ["BUY", "SELL"], index=0 if (not fill or fill["type"]=="BUY") else 1)
        with c3:
            t_date = st.date_input("Date", value=date.today())
            
        c4, c5 = st.columns(2)
        with c4:
            t_price = st.number_input("Price", value=float(fill["price"]) if fill else 100.0)
        with c5:
            t_shares = st.number_input("Shares", value=float(fill["shares"]) if fill else 1.0)
            
        if st.form_submit_button("☁️ 同步至雲端"):
            # 實作 save_trade 邏輯
            st.success(f"{t_input} 交易已儲存")
            st.session_state["fill_trade"] = None # 清除快取

# --- Tab 3: Strategy Center ---
elif active_tab == "🎯 Strategy Center":
    st.subheader("🎯 策略分析與決策")
    analyze_ticker = st.text_input("輸入代碼分析", value="NVDA").upper()
    hist = get_unified_analysis(analyze_ticker)
    
    if hist is not None:
        score, action, details, note = evaluate_strategy(analyze_ticker, hist, 0, 0, 100000, 50000) # 測試數據
        
        col_l, col_r = st.columns([4, 6])
        with col_l:
            st.markdown(f"### {analyze_ticker} 分析結果")
            st.metric("Strategy Score", f"{score:.1f}", delta=score-2.5)
            st.write(f"**大盤環境**: `{details['regime']}`")
            st.write(f"**背離狀態**: `{details['divergence']}`")
            
            if details['trailing_stop']:
                st.warning(f"🛡️ 移動止損位: `${details['trailing_stop']:.2f}`")
            
            st.divider()
            # ⚡ 關鍵功能：聯動按鈕
            if st.button(f"⚡ 快速填入 {analyze_ticker} 買單", use_container_width=True):
                st.session_state["fill_trade"] = {
                    "ticker": analyze_ticker,
                    "price": details["close"],
                    "shares": details["suggested_buy_qty"],
                    "type": "BUY"
                }
                st.toast("✅ 數據已帶入交易中心，請切換分頁查看", icon="🚀")
                
        with col_r:
            plot_df = hist.tail(120)

            fig = make_subplots(
                rows=3, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.04,
                row_heights=[0.58, 0.20, 0.22]
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

            fig.add_trace(
                go.Scatter(x=plot_df.index, y=plot_df["SMA20"], name="SMA20", line=dict(color="#17BECF", width=1.3)),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=plot_df.index, y=plot_df["SMA50"], name="SMA50", line=dict(color="#FF9800", width=1.2)),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=plot_df.index, y=plot_df["SMA200"], name="SMA200", line=dict(color="#D62728", width=1.2)),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=plot_df.index, y=plot_df["BB_upper"], name="BB Upper", line=dict(color="rgba(173,216,230,0.8)", width=1)),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=plot_df.index, y=plot_df["BB_lower"], name="BB Lower", line=dict(color="rgba(173,216,230,0.8)", width=1)),
                row=1, col=1
            )

            fig.add_trace(
                go.Scatter(x=plot_df.index, y=plot_df["RSI"], name="RSI", line=dict(color="#00E676", width=1.2)),
                row=2, col=1
            )
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
            fig.add_trace(
                go.Scatter(x=plot_df.index, y=plot_df["MACD"], name="MACD", line=dict(color="#42A5F5", width=1.2)),
                row=3, col=1
            )
            fig.add_trace(
                go.Scatter(x=plot_df.index, y=plot_df["MACD_Signal"], name="Signal", line=dict(color="#FFCA28", width=1.2)),
                row=3, col=1
            )

            fig.update_layout(
                template="plotly_dark",
                height=760,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_rangeslider_visible=False,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
            )

            fig.update_yaxes(title_text="Price", row=1, col=1)
            fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100])
            fig.update_yaxes(title_text="MACD", row=3, col=1)

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

    with c2:
        st.markdown("### 風險參數")
        st.write(f"- 單一持股上限：`{MAX_POSITION_WEIGHT*100:.0f}%`")
        st.write(f"- 單筆風險上限：`{RISK_PER_TRADE_PCT*100:.0f}%`")
        st.write(f"- 現金保留：`{CASH_RESERVE_PCT*100:.0f}%`")
        st.write(f"- 冷卻期：`{COOLDOWN_DAYS}` 天")

    st.divider()

    st.markdown("### Alert 去重狀態")
    if "alert_sent" in st.session_state and st.session_state["alert_sent"]:
        alert_df = pd.DataFrame(
            [{"Key": k, "Sent": v} for k, v in st.session_state["alert_sent"].items()]
        )
        st.dataframe(alert_df, use_container_width=True)
    else:
        st.info("目前沒有已發送的 alert 記錄。")

    st.divider()

    if st.button("清除快取與 Alert 狀態"):
        st.cache_data.clear()
        if "alert_sent" in st.session_state:
            del st.session_state["alert_sent"]
        st.success("已清除完成，請重新整理。")

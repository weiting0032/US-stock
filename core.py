import math
import os
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple

import streamlit as st
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


# ===============================
# Utility
# ===============================
def normalize_ticker(symbol: str) -> str:
    return str(symbol).upper().strip().replace(".", "-")

def color_pl(val):
    color = "#26A69A" if val > 0 else "#EF5350" if val < 0 else "white"
    return f"color: {color}; font-weight: 600;"

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


def display_divergence(div: str) -> str:
    div = str(div).strip().upper()
    if div == "BULLISH":
        return "多方量價背離"
    if div == "BEARISH":
        return "空方量價背離"
    return "無明顯量價背離"


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

def display_market_session(session: str) -> str:
    mapping = {
        "PREMARKET": "盤前",
        "REGULAR": "正常盤",
        "AFTERMARKET": "盤後",
        "CLOSED": "休市",
    }
    return mapping.get(str(session).upper(), session)

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


def calc_target_zone_hit(current_price: float, target_price: Optional[float], tol_pct: float = PRE_ALERT_PCT) -> bool:
    if not target_price or target_price <= 0:
        return False
    return abs(current_price - target_price) / target_price <= tol_pct


# ===============================
# Google Sheets
# ===============================
def get_gsheet_client():
    raw = os.getenv("GCP_SERVICE_ACCOUNT", "").strip()
    if raw:
        import json
        creds = json.loads(raw)
        return gspread.service_account_from_dict(creds)

    try:
        import streamlit as st

        if "gcp_service_account" in st.secrets:
            creds = dict(st.secrets["gcp_service_account"])
            return gspread.service_account_from_dict(creds)

        if "GCP_SERVICE_ACCOUNT" in st.secrets:
            secret_val = st.secrets["GCP_SERVICE_ACCOUNT"]
            if isinstance(secret_val, dict):
                return gspread.service_account_from_dict(dict(secret_val))

            raw = str(secret_val).strip()
            if raw:
                import json
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


def load_trades() -> pd.DataFrame:
    _, ws_trades, _, _ = get_sheet_handles()
    cols = ["Date", "Ticker", "Type", "Price", "Shares", "Total", "Note"]
    df = read_worksheet_as_df(ws_trades, cols)
    if df.empty:
        return pd.DataFrame(columns=cols)

    df["Ticker"] = df["Ticker"].astype(str).apply(normalize_ticker)
    df["Type"] = df["Type"].astype(str).apply(normalize_trade_type)
    for col in ["Price", "Shares", "Total"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    return df


def load_alerts() -> pd.DataFrame:
    _, _, _, ws_alerts = get_sheet_handles()
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


def save_trade(trade_date: date, ticker: str, trade_type: str, price: float, shares: float, note: str = "") -> Tuple[bool, str]:
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
    if trade_type == "SELL" and shares > holding_shares:
        return False, f"賣出超過持股，目前持有 {holding_shares:.4f}"

    _, ws_trades, _, _ = get_sheet_handles()
    total = round(price * shares, 4)
    ws_trades.append_row([
        str(trade_date), ticker, trade_type,
        float(price), float(shares), float(total), note
    ])
    return True, "交易已寫入"


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


# ===============================
# Market Data
# ===============================
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

        return df.dropna().copy()
    except Exception:
        return None


def get_last_price(symbol: str) -> Optional[float]:
    try:
        hist = yf.Ticker(normalize_ticker(symbol)).history(period="5d", auto_adjust=True)
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return None


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
        return {"regime": "RISK_ON", "score": score, "allow_buy": True}
    if score <= 1:
        return {"regime": "RISK_OFF", "score": score, "allow_buy": False}
    return {"regime": "NEUTRAL", "score": score, "allow_buy": True}


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
                lots.append({"shares": shares, "price": price, "date": row["Date"]})
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
# Strategy
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
        "rsi": rsi,
        "atr": atr,
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
# Scanner
# ===============================
def run_auto_scanner(portfolio: List[Dict], trades_df: pd.DataFrame, cash: float, total_assets: float, market_regime: Dict) -> List[str]:
    logs = []
    if not portfolio:
        return logs

    current_session = get_market_session()
    alerts_df = load_alerts()
    sent_fingerprints_in_run = set()

    for item in portfolio:
        ticker = item["Ticker"]
        hist = get_unified_analysis(ticker)
        if hist is None or hist.empty:
            logs.append(f"{ticker}: 無法取得歷史資料")
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

        if current_session == "CLOSED" and action in ["BUY", "SELL"]:
            logs.append(f"{ticker}: 休市期間，略過 BUY/SELL 訊號")
            continue

        if action == "BUY" and not recent_buy:
            qty = details["suggested_buy_qty"]
            target_price = details["target_buy_price"]
            if qty >= 1:
                send_msg = (
                    f"🟢 *買入訊號* `{ticker}`\n"
                    f"市場時段：`{display_market_session(current_session)}`\n"
                    f"市場狀態：`{details['market_regime']}`\n"
                    f"策略分數：`{score:.1f}`\n"
                    f"現價：`${details['close']:.2f}`\n"
                    f"建議買入價：`${target_price:.2f}`\n"
                    f"建議股數：`{qty}`\n"
                    f"停損價：`${details['stop_loss']:.2f}`\n"
                    f"移動停損：`${details['trailing_stop']:.2f}`\n"
                    f"停利價：`${details['take_profit']:.2f}`\n"
                    f"依據：{note}"
                )

        elif action == "SELL" and not recent_sell:
            qty = details["suggested_sell_qty"]
            target_price = details["target_sell_price"]
            if qty >= 1:
                send_msg = (
                    f"🔴 *賣出訊號* `{ticker}`\n"
                    f"市場時段：`{display_market_session(current_session)}`\n"
                    f"市場狀態：`{details['market_regime']}`\n"
                    f"策略分數：`{score:.1f}`\n"
                    f"現價：`${details['close']:.2f}`\n"
                    f"建議賣出價：`${target_price:.2f}`\n"
                    f"建議股數：`{qty}`\n"
                    f"RSI：`{details['rsi']:.1f}`\n"
                    f"移動停損：`${details['trailing_stop']:.2f}`\n"
                    f"依據：{note}"
                )

        elif action == "BUY_READY" and not recent_buy:
            target_price = details["target_buy_price"]
            send_msg = (
                f"🟡 *買入準備訊號* `{ticker}`\n"
                f"市場時段：`{display_market_session(current_session)}`\n"
                f"市場狀態：`{details['market_regime']}`\n"
                f"現價：`${details['close']:.2f}`\n"
                f"目標區間：`${target_price*(1-PRE_ALERT_PCT):.2f}` ~ `${target_price*(1+PRE_ALERT_PCT):.2f}`\n"
                f"目標價格：`${target_price:.2f}`\n"
                f"預計股數：`{details['suggested_buy_qty']}`\n"
                f"依據：{note}"
            )

        elif action == "SELL_READY" and not recent_sell:
            target_price = details["target_sell_price"]
            send_msg = (
                f"🟠 *賣出準備訊號* `{ticker}`\n"
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
            target_price=target_price
        )

        if fingerprint in sent_fingerprints_in_run:
            logs.append(f"{ticker}: 本輪掃描內重複訊號，略過")
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
                logs.append(f"{ticker}: 提醒已發送 -> {action}")
            else:
                logs.append(f"{ticker}: Telegram 已送出，但寫入提醒紀錄失敗")
        else:
            logs.append(f"{ticker}: Telegram 發送失敗")

    return logs

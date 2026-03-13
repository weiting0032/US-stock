# ===============================
# 0. 基礎設定 (UI 與 核心庫)
# ===============================

import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date, timedelta
import math
import gspread
import requests
from streamlit_autorefresh import st_autorefresh

PORTFOLIO_SHEET_TITLE = 'US Stock' 
st.set_page_config(page_title="Pro 量化投資戰情室 V9.1", layout="wide")
st_autorefresh(interval=15000, limit=None, key="heartbeat")

st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 1.8rem !important; white-space: nowrap !important; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem !important; }
    .stMetric { border: 1px solid rgba(128, 128, 128, 0.3); padding: 10px !important; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

# ===============================
# 1. 數據獲取 (S&P 500 完整爬蟲)
# ===============================
@st.cache_data(ttl=86400)
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        # 確保抓取完整表格
        df = pd.read_html(response.text)[0]
        # 格式化顯示名稱，方便下拉選單搜尋
        df['Display'] = df['Symbol'].str.replace('.', '-', regex=False) + " - " + df['Security']
        return sorted(df['Display'].tolist())
    except Exception as e:
        st.error(f"S&P 500 抓取失敗: {e}")
        return ["NVDA - NVIDIA", "AAPL - Apple", "TSLA - Tesla", "MSFT - Microsoft", "GOOGL - Alphabet"]

# ===============================
# 2. 技術指標核心 (含 ATR 與成交量)
# ===============================
@st.cache_data(ttl=600)
def get_analysis(symbol):
    try:
        df = yf.Ticker(symbol).history(period="2y")
        if df.empty: return None
        df['SMA20'] = df['Close'].rolling(20).mean()
        df['SMA200'] = df['Close'].rolling(200).mean()
        std = df['Close'].rolling(20).std()
        df['BB_upper'] = df['SMA20'] + 2 * std
        df['BB_lower'] = df['SMA20'] - 2 * std
        
        # ATR 計算
        high_low = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift()).abs()
        low_close = (df['Low'] - df['Close'].shift()).abs()
        df['ATR'] = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1).rolling(14).mean()
        
        # RSI & MACD
        delta = df['Close'].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        ema12 = df['Close'].ewm(span=12, adjust=False).mean()
        ema26 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = ema12 - ema26
        df['Hist'] = df['MACD'] - df['MACD'].ewm(span=9, adjust=False).mean()
        
        # 成交量均線
        df['Vol_MA20'] = df['Volume'].rolling(20).mean()
        return df
    except: return None

# ===============================
# 3. Google Sheets 整合
# ===============================
def get_gsheet_client():
    return gspread.service_account_from_dict(st.secrets["gcp_service_account"])

@st.cache_data(ttl=300)
def load_trades():
    try:
        sh = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE).sheet1
        df = pd.DataFrame(sh.get_all_records())
        if df.empty: return pd.DataFrame(columns=['Date','Ticker','Type','Price','Shares','Total'])
        for c in ['Price','Shares','Total']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        return df
    except:
        return pd.DataFrame(columns=['Date','Ticker','Type','Price','Shares','Total'])

def save_trade(d, ticker, t, p, s):
    try:
        sh = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE).sheet1
        sh.append_row([str(d), ticker.upper().strip(), t, float(p), float(s), float(p*s)])
        return True
    except: return False

def sync_nav_history(total_assets):
    try:
        ss = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE)
        try:
            ws_history = ss.worksheet("History")
        except gspread.exceptions.WorksheetNotFound:
            ws_history = ss.add_worksheet(title="History", rows="1000", cols="2")
            ws_history.append_row(["Date", "Total Assets"])
        
        existing_data = ws_history.get_all_records()
        today_str = date.today().strftime('%Y-%m-%d')
        if not existing_data or existing_data[-1].get("Date") != today_str:
            ws_history.append_row([today_str, float(total_assets)])
        return pd.DataFrame(ws_history.get_all_records())
    except: return None

# ===============================
# 4. Sidebar 控制中心
# ===============================
st.sidebar.title("🎮 Command Center")
initial_capital = st.sidebar.number_input("Initial Fund (USD)", value=32000, step=1000)

sp500_list = get_sp500_tickers()
is_manual = st.sidebar.checkbox("Manual Input (for Small Caps)")

with st.sidebar.form("trade_entry"):
    if is_manual:
        ticker_clean = st.text_input("Enter Ticker").upper().strip()
    else:
        # 這裡會顯示完整的 S&P 500 清單
        selected_stock = st.selectbox("Search Stock", options=sp500_list)
        ticker_clean = selected_stock.split(" - ")[0] if selected_stock else ""
    
    t_type = st.selectbox("Type", ["買入 (Buy)", "賣出 (Sell)"])
    t_date = st.date_input("Date", date.today())
    t_price = st.number_input("Price", min_value=0.01, format="%.2f")
    t_shares = st.number_input("Shares", min_value=0.01, format="%.2f")
    
    if st.form_submit_button("Sync to Cloud"):
        if ticker_clean and save_trade(t_date, ticker_clean, t_type, t_price, t_shares):
            st.success("Synced!")
            st.cache_data.clear()
            st.rerun()

# ===============================
# 5. 資產運算 (NAV & P/L)
# ===============================
trades_df = load_trades()
unique_tickers = trades_df['Ticker'].unique().tolist() if not trades_df.empty else []
portfolio_cal = []
cash = initial_capital
total_realized_pl = 0

for ticker in unique_tickers:
    t_df = trades_df[trades_df['Ticker'] == ticker]
    shares_h, cost_b = 0, 0
    ticker_realized_pl = 0
    for _, r in t_df.iterrows():
        val = r['Price'] * r['Shares']
        if "買入" in r['Type']:
            shares_h += r['Shares']
            cost_b += val
            cash -= val
        else:
            if shares_h > 0:
                avg_cost = cost_b / shares_h
                ticker_realized_pl += (r['Price'] - avg_cost) * r['Shares']
                cost_b -= avg_cost * r['Shares']
            shares_h -= r['Shares']
            cash += val
    total_realized_pl += ticker_realized_pl
    if shares_h > 0:
        try:
            real_p = yf.Ticker(ticker).fast_info.last_price
            avg_cost_val = cost_b / shares_h
            portfolio_cal.append({
                "Ticker": ticker, "Shares": shares_h, "AvgCost": avg_cost_val, 
                "MktVal": shares_h*real_p, "RealPrice": real_p, 
                "Unrealized": (real_p - avg_cost_val) * shares_h, 
                "PL_Pct": ((real_p / avg_cost_val) - 1) * 100
            })
        except: pass

total_unrealized_pl = sum(p['Unrealized'] for p in portfolio_cal)
total_assets = (sum(p['MktVal'] for p in portfolio_cal)) + cash
total_pl_v = total_assets - initial_capital

# 自動更新歷史紀錄
history_df = sync_nav_history(total_assets)

# UI: 總覽
st.title("🏛️ 專業級資產配置管理 V9.1")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("NAV 總值", f"${total_assets:,.1f}") 
c2.metric("Cash 購買力", f"${cash:,.1f}")
c3.metric("Realized 實現", f"${total_realized_pl:,.1f}")
c4.metric("Unrealized 未實現", f"${total_unrealized_pl:,.1f}")
c5.metric("Total P/L 總損益", f"${total_pl_v:,.1f}", f"{(total_pl_v/initial_capital*100):.2f}%")

# NAV 曲線
if history_df is not None and not history_df.empty:
    with st.expander("📈 投資組合績效回測追蹤 (NAV Curve)", expanded=False):
        fig_nav = go.Figure()
        fig_nav.add_trace(go.Scatter(x=history_df['Date'], y=history_df['Total Assets'], mode='lines+markers', fill='tozeroy', name='Net Asset Value'))
        fig_nav.update_layout(template="plotly_dark", height=300, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig_nav, use_container_width=True)

# ===============================
# 6. 量化策略引擎 (含建議股數修復)
# ===============================
st.divider()
st.subheader("🎯 策略決策中心")

analyze_target = st.selectbox("選擇或搜尋分析標的", options=unique_tickers + sp500_list)
analyze_ticker = analyze_target.split(" - ")[0]

hist = get_analysis(analyze_ticker)
if hist is not None:
    l_col, r_col = st.columns([3, 7])
    last = hist.iloc[-1]
    curr_p = last['Close']
    curr_atr = last['ATR']
    
    with l_col:
        st.subheader(f"🛠️ 建議策略 ({analyze_ticker})")
        target_info = next((item for item in portfolio_cal if item["Ticker"] == analyze_ticker), None)
        held_shares = target_info['Shares'] if target_info else 0
        current_weight = (target_info['MktVal'] / total_assets) if total_assets > 0 and target_info else 0

        # --- 評分系統 ---
        score = 0
        if curr_p > last['SMA200']: score += 2 
        if last['RSI'] < 45: score += 1 
        if last['Hist'] > 0: score += 1 
        # 新增：帶量突破判定 (量大於均量 1.5 倍)
        vol_ratio = last['Volume'] / last['Vol_MA20']
        if vol_ratio > 1.5 and last['Close'] > last['Open']: score += 1
        
        # --- 決策輸出與建議股數 ---
        if score >= 3:
            buy_price = (last['BB_lower'] + last['SMA20']) / 2
            # 修復：計算建議買入股數 (以總資產 10% 為單筆建倉單位)
            suggest_qty = math.floor((total_assets * 0.1) / curr_p)
            st.success(f"🔥 建議：分批買入 (評分: {score}/5)")
            st.markdown(f"📍 建議進場價: :green[${buy_price:.2f}] 以下")
            if suggest_qty > 0: st.markdown(f"📋 建議買進股數: :orange[{suggest_qty}] 股")
            if vol_ratio > 1.5: st.info(f"🚀 帶量訊號確認！量比: {vol_ratio:.1f}")
        elif score <= 1 and held_shares > 0:
            sell_qty = math.ceil(held_shares * 0.5)
            st.error(f"⚠️ 建議：分批減碼 (評分: {score}/5)")
            st.markdown(f"📋 建議減碼股數: :orange[{sell_qty}] 股")
        else: 
            st.warning("⚖️ 狀態：觀望 (Hold)")

        # --- ATR 動態風控 ---
        st.divider()
        st.write("🛡️ **ATR 動態風控指標**")
        st.write(f"- 建議停損 (2*ATR): :red[${(curr_p - 2*curr_atr):.2f}]")
        st.write(f"- 建議利潤 (3*ATR): :green[${(curr_p + 3*curr_atr):.2f}]")

    with r_col:
        st.subheader("📊 技術分析圖表")
        df_plot = hist.tail(100)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)
        fig.add_trace(go.Candlestick(x=df_plot.index, open=df_plot['Open'], high=df_plot['High'], low=df_plot['Low'], close=df_plot['Close'], name="K線"), 1, 1)
        for ma, color in zip(['SMA20','SMA200'], ['#17BECF','#D62728']):
            fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[ma], name=ma, line=dict(width=1, color=color)), 1, 1)
        
        # 下方成交量
        vol_colors = ['red' if c < o else 'green' for c, o in zip(df_plot['Close'], df_plot['Open'])]
        fig.add_trace(go.Bar(x=df_plot.index, y=df_plot['Volume'], name="Volume", marker_color=vol_colors), 2, 1)
        
        fig.update_layout(height=500, template="plotly_dark", xaxis_rangeslider_visible=False, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)

# 個股明細表格
if portfolio_cal:
    with st.expander("🔍 詳情：當前持倉明細", expanded=True):
        st.dataframe(pd.DataFrame(portfolio_cal), use_container_width=True)

# ===============================
# 0. 基礎設定
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

# ===============================
# 0. 基礎設定 (UI 優化)
# ===============================
PORTFOLIO_SHEET_TITLE = 'US Stock' # 建議更名以符合多股需求
st.set_page_config(page_title="Pro 量化投資戰情室 V8.3", layout="wide")
st_autorefresh(interval=15000, limit=None, key="heartbeat")

st.markdown("""
    <style>
    .stMetric, div[data-testid="metric-container"] {
        border: 1px solid rgba(128, 128, 128, 0.3);
        padding: 15px;
        border-radius: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

# ===============================
# 1. 數據獲取 (S&P 500 爬蟲)
# ===============================
@st.cache_data(ttl=86400)
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        df = pd.read_html(response.text)[0]
        df['Display'] = df['Symbol'].str.replace('.', '-', regex=False) + " - " + df['Security']
        return sorted(df['Display'].tolist())
    except:
        return ["NVDA - NVIDIA", "AAPL - Apple", "TSLA - Tesla", "MSFT - Microsoft"]

# ===============================
# 2. 技術指標核心
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
        high_low = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift()).abs()
        low_close = (df['Low'] - df['Close'].shift()).abs()
        df['ATR'] = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1).rolling(14).mean()
        delta = df['Close'].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        ema12 = df['Close'].ewm(span=12, adjust=False).mean()
        ema26 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = ema12 - ema26
        df['Hist'] = df['MACD'] - df['MACD'].ewm(span=9, adjust=False).mean()
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

# ===============================
# 4. Sidebar 控制中心
# ===============================
st.sidebar.title("🎮 Command Center")
initial_capital = st.sidebar.number_input("Initial Fund (USD)", value=32000, step=1000)

sp500_list = get_sp500_tickers()
is_manual = st.sidebar.checkbox("Manual Input (for AXTI, ONDS...)")

with st.sidebar.form("trade_entry"):
    if is_manual:
        ticker_clean = st.text_input("Enter Ticker").upper().strip()
    else:
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
# 5. 資產運算 (核心更新：實現/未實現損益)
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
        real_p = yf.Ticker(ticker).fast_info.last_price
        unrealized_pl = (real_p - (cost_b / shares_h)) * shares_h
        portfolio_cal.append({
            "Ticker": ticker, "Shares": shares_h, "AvgCost": cost_b/shares_h, 
            "MktVal": shares_h*real_p, "RealPrice": real_p, "Unrealized": unrealized_pl
        })

total_unrealized_pl = sum(p['Unrealized'] for p in portfolio_cal)
total_assets = (sum(p['MktVal'] for p in portfolio_cal)) + cash
total_pl_v = total_assets - initial_capital

# UI: 頂部總覽 (五格顯示)
st.title("🏛️ 專業級資產配置管理")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("NAV 總資產淨值", f"${total_assets:,.2f}")
c2.metric("Cash 剩餘購買力", f"${cash:,.2f}")
c3.metric("Realized 已實現損益", f"${total_realized_pl:,.2f}")
c4.metric("Unrealized 未實現損益", f"${total_unrealized_pl:,.2f}")
c5.metric("Total P/L 總損益", f"${total_pl_v:,.2f}", f"{(total_pl_v/initial_capital*100):.2f}%")

# ===============================
# 6. 量化策略引擎
# ===============================
st.divider()
analyze_target = st.selectbox("🎯 Target Analysis", options=unique_tickers if unique_tickers else ["NVDA"])

hist = get_analysis(analyze_target)
if hist is not None:
    l_col, r_col = st.columns([7, 3])
    last = hist.iloc[-1]
    curr_p = last['Close']
    
    with l_col:
        st.subheader(f"📊 {analyze_target} 技術面動態圖表")
        df_plot = hist.tail(100)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)
        fig.add_trace(go.Candlestick(x=df_plot.index, open=df_plot['Open'], high=df_plot['High'], low=df_plot['Low'], close=df_plot['Close'], name="K線"), 1, 1)
        for ma, color in zip(['SMA20','SMA200'], ['#17BECF','#D62728']):
            fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[ma], name=ma, line=dict(width=1, color=color)), 1, 1)
        fig.update_layout(height=600, template="plotly_dark", xaxis_rangeslider_visible=False)
        st.plotly_chart(fig, use_container_width=True)

    with r_col:
        st.subheader("🛠️ 量化策略建議 (V8.3)")
        target_info = next((item for item in portfolio_cal if item["Ticker"] == analyze_target), None)
        held_shares = target_info['Shares'] if target_info else 0
        current_weight = (target_info['MktVal'] / total_assets) if total_assets > 0 and target_info else 0
        
        # 今日冷卻偵測
        today_str = date.today().strftime('%Y-%m-%d')
        today_trades = trades_df[(trades_df['Ticker'] == analyze_target) & (trades_df['Date'] == today_str)]
        has_sold_today = not today_trades[today_trades['Type'].str.contains("賣出")].empty
        has_bought_today = not today_trades[today_trades['Type'].str.contains("買入")].empty

        score = 0
        if curr_p > last['SMA200']: score += 2 
        if last['RSI'] < 45: score += 1 
        if last['Hist'] > 0: score += 1 
        
        if has_sold_today: st.info("✅ 今日已執行減碼。")
        elif has_bought_today: st.info("✅ 今日已執行加碼。")
        elif score >= 3:
            buy_price = (last['BB_lower'] + last['SMA20']) / 2
            suggest_qty = math.floor((cash * 0.2) / buy_price)
            st.success(f"🔥 建議狀態：分批買入 (Buy)")
            st.markdown(f"📍 **建議買入價格**: :green[**${buy_price:.2f}**] 以下")
            if current_weight >= 0.3: st.warning("⚠️ 警示：單一持股佔比超過 30%。")
            elif suggest_qty >= 1: st.markdown(f"📋 **建議操作股數**: :orange[**{suggest_qty}**] 股")
        elif score <= 1 and held_shares > 1:
            sell_price = last['BB_upper']
            sell_qty = math.ceil(held_shares * 0.25)
            st.error(f"⚠️ 建議狀態：分批減碼 (Sell)")
            st.markdown(f"📍 **建議賣出價格**: :red[**${sell_price:.2f}**] 以上")
            st.markdown(f"📋 **建議操作股數**: :orange[**{sell_qty}**] 股")
        else: st.warning("⚖️ 建議狀態：觀望 (Hold)")
            
        st.divider()
        st.write(f"**持倉數據摘要**:")
        st.write(f"- 持有股數: `{held_shares:.2f}`")
        st.write(f"- 組合權重: `{current_weight*100:.1f}%` (風控: 30%)")

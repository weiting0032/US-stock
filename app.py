import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date, timedelta
import math
import gspread
import time
import requests
from streamlit_autorefresh import st_autorefresh

# ===============================
# 0. 基礎與 UI 設定
# ===============================
PORTFOLIO_SHEET_TITLE = 'Streamlit US Stock' # 建議更名以符合多股需求
st.set_page_config(page_title="Pro 量化投資戰情室", layout="wide")
st_autorefresh(interval=15000, limit=None, key="heartbeat")

# 自定義 CSS 讓介面更專業
st.markdown("""
    <style>
    .metric-card {
        background-color: #1e2130;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #3d4156;
    }
    .stMetric {
        background-color: #1e2130;
        padding: 10px;
        border-radius: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

# ===============================
# 1. 數據獲取 (S&P 500 爬蟲 + User-Agent)
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
        return ["NVDA - NVIDIA", "AAPL - Apple", "TSLA - Tesla"]

# ===============================
# 2. 技術指標與量化核心 (新增價位邏輯)
# ===============================
@st.cache_data(ttl=600)
def get_analysis(symbol):
    df = yf.Ticker(symbol).history(period="2y")
    if df.empty: return None
    
    # 基礎指標
    df['SMA20'] = df['Close'].rolling(20).mean()
    df['SMA60'] = df['Close'].rolling(60).mean()
    df['SMA200'] = df['Close'].rolling(200).mean()
    
    # 布林通道 (用於支撐壓力建議)
    std = df['Close'].rolling(20).std()
    df['BB_upper'] = df['SMA20'] + 2 * std
    df['BB_lower'] = df['SMA20'] - 2 * std
    df['BB_pos'] = (df['Close'] - df['BB_lower']) / (df['BB_upper'] - df['BB_lower'] + 1e-9) * 100
    
    # ATR 波幅 (用於停損停利)
    high_low = df['High'] - df['Low']
    high_close = (df['High'] - df['Close'].shift()).abs()
    low_close = (df['Low'] - df['Close'].shift()).abs()
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    df['ATR'] = ranges.max(axis=1).rolling(14).mean()
    
    # RSI & MACD
    delta = df['Close'].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
    
    ema12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = ema12 - ema26
    df['Hist'] = df['MACD'] - df['MACD'].ewm(span=9, adjust=False).mean()
    
    return df

# ===============================
# 3. Google Sheets (與先前邏輯一致)
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
        sh.append_row([str(d), ticker.upper(), t, float(p), float(s), float(p*s)])
        return True
    except: return False

# ===============================
# 4. Sidebar 控制中心 (支援手動輸入)
# ===============================
st.sidebar.title("🎮 Command Center")
initial_capital = st.sidebar.number_input("Initial Fund (USD)", value=32000)

sp500_list = get_sp500_tickers()
is_manual = st.sidebar.checkbox("Manual Input (for AXTI, ONDS...)")

with st.sidebar.form("trade_entry"):
    if is_manual:
        ticker_clean = st.text_input("Enter Ticker (e.g. ONDS)").upper().strip()
    else:
        selected_stock = st.selectbox("Search Stock", options=sp500_list)
        ticker_clean = selected_stock.split(" - ")[0]
    
    t_type = st.selectbox("Type", ["買入 (Buy)", "賣出 (Sell)"])
    t_date = st.date_input("Date", date.today())
    t_price = st.number_input("Price", min_value=0.01)
    t_shares = st.number_input("Shares", min_value=0.01)
    
    if st.form_submit_button("Sync to Cloud"):
        if save_trade(t_date, ticker_clean, t_type, t_price, t_shares):
            st.success("Synced!")
            st.cache_data.clear()
            st.rerun()

# ===============================
# 5. 資產運算與 Dashboard
# ===============================
trades_df = load_trades()
unique_tickers = trades_df['Ticker'].unique().tolist() if not trades_df.empty else []
portfolio = []
cash = initial_capital

for ticker in unique_tickers:
    t_df = trades_df[trades_df['Ticker'] == ticker]
    shares, cost_basis = 0, 0
    for _, r in t_df.iterrows():
        val = r['Price'] * r['Shares']
        if "買入" in r['Type']:
            shares += r['Shares']; cost_basis += val; cash -= val
        else:
            if shares > 0: cost_basis -= (cost_basis/shares) * r['Shares']
            shares -= r['Shares']; cash += val
    
    if shares > 0:
        real_p = yf.Ticker(ticker).fast_info.last_price
        portfolio.append({"Ticker": ticker, "Shares": shares, "AvgCost": cost_basis/shares, "MktVal": shares*real_p})

total_assets = sum(p['MktVal'] for p in portfolio) + cash
total_pl = total_assets - initial_capital

# UI: 頂部總覽
st.title("🏛️ 專業級資產配置管理")
c1, c2, c3, c4 = st.columns(4)
c1.metric("NAV 總資產", f"${total_assets:,.2f}")
c2.metric("Cash 購買力", f"${cash:,.2f}")
c3.metric("Profit/Loss", f"${total_pl:,.2f}", f"{(total_pl/initial_capital*100):.2f}%")
c4.metric("Position 佔比", f"{( (total_assets-cash)/total_assets*100 ):.1f}%")

# ===============================
# 6. 策略引擎 (新增價位建議)
# ===============================
st.divider()
analyze_target = st.selectbox("🎯 Target Analysis", options=unique_tickers if unique_tickers else ["NVDA"])

hist = get_analysis(analyze_target)
if hist is not None:
    l_col, r_col = st.columns([7, 3])
    last = hist.iloc[-1]
    curr_p = last['Close']
    
    with l_col:
        # 繪製圖表 (包含支撐壓力線)
        df_p = hist.tail(100)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)
        fig.add_trace(go.Candlestick(x=df_p.index, open=df_p['Open'], high=df_p['High'], low=df_p['Low'], close=df_p['Close'], name="Price"), 1, 1)
        fig.add_trace(go.Scatter(x=df_p.index, y=df_p['BB_upper'], name="Resistance (BB Upper)", line=dict(dash='dot', color='rgba(255,0,0,0.5)')), 1, 1)
        fig.add_trace(go.Scatter(x=df_p.index, y=df_p['BB_lower'], name="Support (BB Lower)", line=dict(dash='dot', color='rgba(0,255,0,0.5)')), 1, 1)
        fig.add_trace(go.Bar(x=df_p.index, y=df_p['Hist'], name="MACD", marker_color='gray'), 2, 1)
        fig.update_layout(height=600, template="plotly_dark", xaxis_rangeslider_visible=False)
        st.plotly_chart(fig, use_container_width=True)

    with r_col:
        st.subheader("🛠️ 量化策略建議")
        
        # 策略優化計算
        atr = last['ATR']
        buy_zone = (last['BB_lower'] + last['SMA20']) / 2
        sell_zone = last['BB_upper']
        stop_loss = curr_p - (2 * atr)  # 2倍 ATR 停損
        
        # 狀態判斷
        score = 0
        if curr_p > last['SMA200']: score += 2
        if last['RSI'] < 45: score += 1
        if last['Hist'] > 0: score += 1
        
        # UI 顯示建議
        if score >= 3:
            st.success("🔥 建議狀態：分批買入 (Buy)")
            st.write(f"📍 **建議買入區間**: `${buy_zone:.2f}` ~ `${last['BB_lower']:.2f}`")
        elif score <= 1:
            st.error("⚠️ 建議狀態：分批減碼 (Sell)")
            st.write(f"📍 **建議賣出區間**: `${sell_zone:.2f}` 以上")
        else:
            st.warning("⚖️ 建議狀態：觀望 (Hold)")
            
        st.markdown("---")
        st.write("📊 **風控參考價位**")
        st.info(f"🎯 **目標獲利 (TP)**: `${(curr_p + 1.5*atr):.2f}`")
        st.error(f"🛑 **硬性停損 (SL)**: `${stop_loss:.2f}`")
        
        st.write("---")
        st.write(f"**當前指標狀態**:")
        st.write(f"- RSI: {last['RSI']:.1f} ({'超賣' if last['RSI']<30 else '超買' if last['RSI']>70 else '中性'})")
        st.write(f"- MACD: {'多方動能' if last['Hist']>0 else '空方動能'}")

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
# 0. 基礎設定 (UI 配色優化 - 清晰版)
# ===============================
PORTFOLIO_SHEET_TITLE = 'Streamlit US Stock' # 建議更名以符合多股需求
st.set_page_config(page_title="Pro 量化投資戰情室", layout="wide")
st_autorefresh(interval=15000, limit=None, key="heartbeat")

# [修正] 移除原本自定義的 #1e2130 深色 CSS，確保 Streamlit 預設字體絕對清晰
st.markdown("""
    <style>
    /* 這裡可以選擇性加入微調，例如只讓標題和卡片有淡淡的框，但不要強制深色背景 */
    .stMetric, div[data-testid="metric-container"] {
        border: 1px solid rgba(128, 128, 128, 0.3);
        padding: 15px;
        border-radius: 10px;
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
        # 建立 "Ticker - Security" 格式
        df['Display'] = df['Symbol'].str.replace('.', '-', regex=False) + " - " + df['Security']
        return sorted(df['Display'].tolist())
    except:
        return ["NVDA - NVIDIA", "AAPL - Apple", "TSLA - Tesla", "MSFT - Microsoft"]

# ===============================
# 2. 技術指標與量化核心 (ATR, BB, RSI, MACD)
# ===============================
@st.cache_data(ttl=600)
def get_analysis(symbol):
    try:
        df = yf.Ticker(symbol).history(period="2y")
        if df.empty: return None
        
        # 指標計算
        df['SMA20'] = df['Close'].rolling(20).mean()
        df['SMA60'] = df['Close'].rolling(60).mean()
        df['SMA200'] = df['Close'].rolling(200).mean()
        
        # BB 通道
        std = df['Close'].rolling(20).std()
        df['BB_upper'] = df['SMA20'] + 2 * std
        df['BB_lower'] = df['SMA20'] - 2 * std
        df['BB_pos'] = (df['Close'] - df['BB_lower']) / (df['BB_upper'] - df['BB_lower'] + 1e-9) * 100
        
        # ATR 波幅
        high_low = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift()).abs()
        low_close = (df['Low'] - df['Close'].shift()).abs()
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        df['ATR'] = ranges.max(axis=1).rolling(14).mean()
        
        # RSI
        delta = df['Close'].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        
        # MACD
        ema12 = df['Close'].ewm(span=12, adjust=False).mean()
        ema26 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = ema12 - ema26
        df['Hist'] = df['MACD'] - df['MACD'].ewm(span=9, adjust=False).mean()
        
        return df
    except: return None

# ===============================
# 3. Google Sheets (交易錄入)
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
        ticker_clean = st.text_input("Enter Ticker (e.g. AXTI)").upper().strip()
    else:
        selected_stock = st.selectbox("Search Stock", options=sp500_list)
        ticker_clean = selected_stock.split(" - ")[0] if selected_stock else ""
    
    t_type = st.selectbox("Type", ["買入 (Buy)", "賣出 (Sell)"])
    t_date = st.date_input("Date", date.today())
    # 設定 ORCL 目前的價位區間作為預設值，方便測試
    t_price = st.number_input("Price", min_value=0.01, value=164.0, format="%.2f")
    t_shares = st.number_input("Shares", min_value=0.01, value=100.0, format="%.2f")
    
    if st.form_submit_button("Sync to Cloud"):
        if not ticker_clean: st.error("Please enter a valid ticker.")
        elif save_trade(t_date, ticker_clean, t_type, t_price, t_shares):
            st.success("Synced!")
            st.cache_data.clear()
            st.rerun()

# ===============================
# 5. 資產運算與 Dashboard UI (清晰版)
# ===============================
trades_df = load_trades()
unique_tickers = trades_df['Ticker'].unique().tolist() if not trades_df.empty else []
portfolio_cal = []
cash = initial_capital

# [修正] 批次獲取所有持股的最新的快照價格，提升效能
if unique_tickers:
    batch_info = yf.download(unique_tickers, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
    batch_prev = yf.download(unique_tickers, period="2d", interval="1d", progress=False)['Close'].iloc[0]
else:
    batch_info = pd.Series()
    batch_prev = pd.Series()

# 移動平均成本法計算
for ticker in unique_tickers:
    t_df = trades_df[trades_df['Ticker'] == ticker]
    shares_h, cost_b = 0, 0
    for _, r in t_df.iterrows():
        val = r['Price'] * r['Shares']
        if "買入" in r['Type']:
            shares_h += r['Shares']; cost_b += val; cash -= val
        else:
            if shares_h > 0: cost_b -= (cost_b/shares_h) * r['Shares']
            shares_h -= r['Shares']; cash += val
    
    if shares_h > 0:
        # 處理批次獲取可能失敗的情況 (AXTI, ONDS 可能要在batch_info字典裡正確索引)
        try:
            real_p = batch_info[ticker] if len(unique_tickers) > 1 else batch_info
            prev_p = batch_prev[ticker] if len(unique_tickers) > 1 else batch_prev
        except:
            ticker_obj = yf.Ticker(ticker).fast_info
            real_p = ticker_obj.last_price
            prev_p = ticker_obj.previous_close

        portfolio_cal.append({
            "Ticker": ticker, "Shares": shares_h, "AvgCost": cost_b/shares_h, 
            "MktVal": shares_h*real_p, "RealPrice": real_p, "PrevPrice": prev_p
        })

# 資產總計
total_mkt_val = sum(p['MktVal'] for p in portfolio_cal)
total_assets = total_mkt_val + cash
total_pl_v = total_assets - initial_capital
total_pl_p = (total_pl_v / initial_capital * 100) if initial_capital > 0 else 0
pos_ratio = (total_mkt_val / total_assets * 100) if total_assets > 0 else 0

# UI: 頂部總覽 (移除自定義深色 CSS 後，文字清晰度大幅提升)
st.title("🏛️ 專業級資產配置管理")
c1, c2, c3, c4 = st.columns(4)
c1.metric("NAV 總資產淨值", f"${total_assets:,.2f}")
c2.metric("Cash 剩餘購買力", f"${cash:,.2f}")
# [修正] 損益百分比的綠色顯示將清晰可見
c3.metric("Profit/Loss 總損益", f"${total_pl_v:,.2f}", f"{total_pl_p:.2f}%")
c4.metric("Position 持倉佔比", f"{pos_ratio:.1f}%")

# 持股狀態表格 (新增實時價格)
if portfolio_cal:
    st.subheader("📋 持股穿透明細")
    p_df = pd.DataFrame(portfolio_cal)
    # 格式化顯示
    st.dataframe(p_df.style.format({
        'AvgCost': '{:.2f}', 'RealPrice': '{:.2f}', 'MktVal': '{:.2f}'
    }), use_container_width=True)

# ===============================
# 6. 量化策略引擎 (恢復股數顯示 + 清晰介面)
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
        # K線 & 均線 & 布林
        fig.add_trace(go.Candlestick(x=df_plot.index, open=df_plot['Open'], high=df_plot['High'], low=df_plot['Low'], close=df_plot['Close'], name="K線"), 1, 1)
        for ma, color in zip(['SMA20','SMA200'], ['#17BECF','#D62728']):
            fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[ma], name=ma, line=dict(width=1, color=color)), 1, 1)
        fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['BB_upper'], name="Resistance", line=dict(dash='dot', color='rgba(255,0,0,0.5)')), 1, 1)
        fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['BB_lower'], name="Support", line=dict(dash='dot', color='rgba(0,255,0,0.5)')), 1, 1)
        # MACD
        hist_col = ['green' if v >=0 else 'red' for v in df_plot['Hist']]
        fig.add_trace(go.Bar(x=df_plot.index, y=df_plot['Hist'], name="MACD", marker_color=hist_col), 2, 1)
        fig.update_layout(height=650, template="plotly_dark", xaxis_rangeslider_visible=False, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with r_col:
        st.subheader("🛠️ 量化策略建議 (Pro V7.9)")
        
        # [修正] 恢復計算該股票的現有持股與總資產權重，用於計算股數
        target_info = next((item for item in portfolio_cal if item["Ticker"] == analyze_target), None)
        held_shares = target_info['Shares'] if target_info else 0
        current_weight = (target_info['MktVal'] / total_assets) if total_assets > 0 and target_info else 0
        
        # 支撐壓力位 & 量化核心
        atr = last['ATR']
        buy_entry = (last['BB_lower'] + last['SMA20']) / 2 # 建議支撐買點
        sell_exit = last['BB_upper'] # 建議壓力賣點
        
        # 策略評分 (RSI + MACD + SMA)
        score = 0
        if curr_p > last['SMA200']: score += 2 # 多頭趨勢
        if last['RSI'] < 45: score += 1 # 股價偏低
        if last['Hist'] > 0: score += 1 # MACD 翻多
        
        # UI 建議卡片顯示
        if score >= 3:
            # [修正] 買入建議中顯示建議股數 (基於 20% 剩餘現金)
            suggest_qty = math.floor((cash * 0.2) / buy_entry)
            st.success(f"🔥 建議狀態：分批買入 (Buy)\n(預計配置：20% 現金)")
            st.write(f"📍 **建議買入區間**: `${buy_entry:.2f}` ~ `${last['BB_lower']:.2f}`")
            if current_weight > 0.3:
                st.warning("⚠️ 警示：單一持股佔比過高（>30%），不建議再加碼。")
            elif suggest_qty > 0:
                st.write(f"📋 **建議操作股數**: `{suggest_qty}` 股") # 恢復顯示

        elif score <= 1 and held_shares > 0:
            # [修正] 賣出建議中顯示建議股數 (基於 25% 現有持股)
            sell_qty = math.ceil(held_shares * 0.25)
            st.error(f"⚠️ 建議狀態：分批減碼 (Sell)\n(預計減持：25% 倉位)")
            st.write(f"📍 **建議賣出區間**: `${sell_exit:.2f}` 以上")
            if sell_qty > 0:
                st.write(f"📋 **建議操作股數**: `{sell_qty}` 股") # 恢復顯示
        else:
            st.warning("⚖️ 建議狀態：觀望 (Hold)")
            st.write("📊 **目前處於中性區間**，等待突破或回測。")
            
        st.markdown("---")
        st.write("📊 **風控與停損停利參考 (ATR 策略)**")
        st.info(f"🎯 **目標獲利 (TP)**: `${(curr_p + 1.5*atr):.2f}`")
        st.error(f"🛑 **硬性停損 (SL)**: `${(curr_p - 2*atr):.2f}`")
        
        # RSI 實時狀態卡片 (清晰版)
        st.divider()
        st.write(f"**核心指標狀態 (RSI 14)**:")
        rsi_v = last['RSI']
        st.metric("RSI 指數", f"{rsi_v:.1f}", help=">70超買, <30超賣")

with st.expander("📝 查看完整雲端交易日誌"):
    st.dataframe(trades_df.sort_index(ascending=False), use_container_width=True)

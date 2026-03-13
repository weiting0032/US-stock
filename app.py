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
st.set_page_config(page_title="Pro 量化投資戰情室 V9.0", layout="wide")
st_autorefresh(interval=15000, limit=None, key="heartbeat")

st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 1.8rem !important; white-space: nowrap !important; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem !important; }
    .stMetric { border: 1px solid rgba(128, 128, 128, 0.3); padding: 10px !important; border-radius: 10px; }
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
# 2. 技術指標核心 (新增成交量與 ATR 邏輯)
# ===============================
@st.cache_data(ttl=600)
def get_analysis(symbol):
    try:
        df = yf.Ticker(symbol).history(period="2y")
        if df.empty: return None
        # 價格均線
        df['SMA20'] = df['Close'].rolling(20).mean()
        df['SMA200'] = df['Close'].rolling(200).mean()
        # 布林帶
        std = df['Close'].rolling(20).std()
        df['BB_upper'] = df['SMA20'] + 2 * std
        df['BB_lower'] = df['SMA20'] - 2 * std
        # ATR 動態波動
        high_low = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift()).abs()
        low_close = (df['Low'] - df['Close'].shift()).abs()
        df['ATR'] = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1).rolling(14).mean()
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
        # 成交量分析 (20日均量)
        df['Vol_MA20'] = df['Volume'].rolling(20).mean()
        df['Vol_Ratio'] = df['Volume'] / df['Vol_MA20']
        
        return df
    except: return None

# ===============================
# 3. Google Sheets 整合 (新增 History 功能)
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
    """每日自動紀錄淨值 (NAV)"""
    try:
        ss = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE)
        try:
            ws_history = ss.worksheet("History")
        except gspread.exceptions.WorksheetNotFound:
            ws_history = ss.add_worksheet(title="History", rows="1000", cols="5")
            ws_history.append_row(["Date", "Total Assets"])
        
        existing_data = ws_history.get_all_records()
        today_str = date.today().strftime('%Y-%m-%d')
        
        # 檢查今天是否已紀錄，若無則更新
        if not existing_data or existing_data[-1].get("Date") != today_str:
            ws_history.append_row([today_str, float(total_assets)])
        return pd.DataFrame(ws_history.get_all_records())
    except:
        return None

# ===============================
# 4. Sidebar 控制中心
# ===============================
st.sidebar.title("🎮 Command Center")
initial_capital = st.sidebar.number_input("Initial Fund (USD)", value=32000, step=1000)

sp500_list = get_sp500_tickers()
is_manual = st.sidebar.checkbox("Manual Input (Ticker)")

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
# 5. 資產運算與歷史紀錄
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
            unrealized_pl = (real_p - avg_cost_val) * shares_h
            unrealized_pct = ((real_p / avg_cost_val) - 1) * 100
            portfolio_cal.append({
                "Ticker": ticker, "Shares": shares_h, "AvgCost": avg_cost_val, 
                "MktVal": shares_h*real_p, "RealPrice": real_p, 
                "Unrealized": unrealized_pl, "PL_Pct": unrealized_pct
            })
        except: pass

total_unrealized_pl = sum(p['Unrealized'] for p in portfolio_cal)
total_assets = (sum(p['MktVal'] for p in portfolio_cal)) + cash
total_pl_v = total_assets - initial_capital

# 執行 NAV 歷史同步
history_df = sync_nav_history(total_assets)

# UI: 頂部總覽
st.title("🏛️ 專業級資產配置管理 V9")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("NAV 總值", f"${total_assets:,.1f}") 
c2.metric("Cash 購買力", f"${cash:,.1f}")
c3.metric("Realized 實現", f"${total_realized_pl:,.1f}")
c4.metric("Unrealized 未實現", f"${total_unrealized_pl:,.1f}")
c5.metric("Total P/L 總損益", f"${total_pl_v:,.1f}", f"{(total_pl_v/initial_capital*100):.2f}%")

# --- 新增功能：NAV 績效曲線圖 ---
if history_df is not None and not history_df.empty:
    with st.expander("📈 投資組合淨值趨勢 (NAV Curve)", expanded=False):
        fig_nav = go.Figure()
        fig_nav.add_trace(go.Scatter(x=history_df['Date'], y=history_df['Total Assets'], 
                                     mode='lines+markers', name='Total Assets',
                                     line=dict(color='#00FFCC', width=3),
                                     fill='tozeroy', fillcolor='rgba(0, 255, 204, 0.1)'))
        fig_nav.update_layout(template="plotly_dark", height=400, margin=dict(l=20, r=20, t=20, b=20),
                              xaxis_title="日期", yaxis_title="資產總額 (USD)")
        st.plotly_chart(fig_nav, use_container_width=True)

if portfolio_cal:
    with st.expander("🔍 查看個股即時損益明細", expanded=True):
        detail_df = pd.DataFrame(portfolio_cal)
        display_df = detail_df.copy()
        for col in ['AvgCost', 'RealPrice', 'Unrealized', 'MktVal']:
            display_df[col] = display_df[col].map('${:,.2f}'.format)
        display_df['PL_Pct'] = display_df['PL_Pct'].map('{:,.2f}%'.format)
        st.dataframe(display_df[['Ticker', 'Shares', 'AvgCost', 'RealPrice', 'Unrealized', 'PL_Pct', 'MktVal']], use_container_width=True)

# ===============================
# 6. 量化策略引擎 (帶量突破 + ATR)
# ===============================
st.divider()
st.subheader("🎯 策略決策中心")

analysis_mode = st.radio("選擇分析對象", ["我的持股", "搜尋全市場標的"], horizontal=True)

if analysis_mode == "我的持股":
    analyze_target = st.selectbox("選擇持倉標的", options=unique_tickers if unique_tickers else ["NVDA"])
else:
    col_s1, col_s2 = st.columns([1, 2])
    with col_s1: search_manual = st.checkbox("手動輸入代碼")
    with col_s2:
        if search_manual: analyze_target = st.text_input("請輸入代碼", value="NVDA").upper().strip()
        else:
            selected_s = st.selectbox("從 S&P 500 搜尋", options=sp500_list)
            analyze_target = selected_s.split(" - ")[0] if selected_s else "NVDA"

hist = get_analysis(analyze_target)
if hist is not None:
    l_col, r_col = st.columns([3, 7])
    last = hist.iloc[-1]
    curr_p = last['Close']
    curr_atr = last['ATR']
    
    with l_col:
        st.subheader(f"🛠️ 建議策略 ({analyze_target})")
        target_info = next((item for item in portfolio_cal if item["Ticker"] == analyze_target), None)
        held_shares = target_info['Shares'] if target_info else 0
        
        # --- 策略邏輯強化 ---
        score = 0
        if curr_p > last['SMA200']: score += 2 
        if last['RSI'] < 45: score += 1 
        if last['Hist'] > 0: score += 1 
        # 新增：帶量突破判定 (成交量 > 20MA 1.5倍 且 收盤 > 開盤)
        is_volume_breakout = last['Vol_Ratio'] > 1.5 and last['Close'] > last['Open']
        if is_volume_breakout: score += 1
        
        # 顯示帶量狀態
        if is_volume_breakout:
            st.markdown("🚀 **偵測到帶量突破！** (成交量為均量 :orange[{:.1f}] 倍)".format(last['Vol_Ratio']))

        # 決策輸出
        if score >= 3:
            buy_price = (last['BB_lower'] + last['SMA20']) / 2
            st.success(f"🔥 建議：分批買入 (評分: {score}/5)")
            st.markdown(f"📍 建議進場: :green[${buy_price:.2f}] 以下")
        elif score <= 1 and held_shares > 1:
            st.error(f"⚠️ 建議：分批減碼 (評分: {score}/5)")
        else: 
            st.warning(f"⚖️ 狀態：觀望 (評分: {score}/5)")

        # --- 新增：ATR 動態風控區 ---
        st.divider()
        st.markdown("🛡️ **ATR 動態風控設置**")
        stop_loss = curr_p - (2.0 * curr_atr)
        take_profit = curr_p + (3.0 * curr_atr)
        st.write(f"- 建議停損 (2.0 ATR): :red[${stop_loss:.2f}]")
        st.write(f"- 建議獲利 (3.0 ATR): :green[${take_profit:.2f}]")
        st.caption(f"當前 14D ATR 波動值: {curr_atr:.2f}")
            
    with r_col:
        st.subheader(f"📊 {analyze_target} 技術面動態圖表")
        df_plot = hist.tail(100)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)
        # 主圖：K線與均線
        fig.add_trace(go.Candlestick(x=df_plot.index, open=df_plot['Open'], high=df_plot['High'], low=df_plot['Low'], close=df_plot['Close'], name="K線"), 1, 1)
        for ma, color in zip(['SMA20','SMA200'], ['#17BECF','#D62728']):
            fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[ma], name=ma, line=dict(width=1, color=color)), 1, 1)
        # 副圖：成交量 (配合量比顯示)
        vol_colors = ['#EF5350' if row['Close'] < row['Open'] else '#26A69A' for _, row in df_plot.iterrows()]
        fig.add_trace(go.Bar(x=df_plot.index, y=df_plot['Volume'], name="Volume", marker_color=vol_colors), 2, 1)
        
        fig.update_layout(height=600, template="plotly_dark", xaxis_rangeslider_visible=False, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)
else:
    st.error(f"無法獲取標的 {analyze_target} 的數據。")

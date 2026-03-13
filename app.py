# ===============================
# 0. 基礎設定 (V9.95 整合精確指令版)
# ===============================
import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date
import math
import gspread
import requests
from streamlit_autorefresh import st_autorefresh

# --- 憑證設定 ---
# 優先讀取 Secrets，若無則使用您提供的數值
TG_TOKEN = st.secrets.get("TG_TOKEN", "8252298047:AAHJ_HSd_vrZlAC6RHtNQYaW6nJ1eywdKx4").strip()
TG_CHAT_ID = str(st.secrets.get("TG_CHAT_ID", "6484933731")).strip()
PORTFOLIO_SHEET_TITLE = 'US Stock' 

st.set_page_config(page_title="Pro 量化投資戰情室 V9.95", layout="wide")
# 每 15 秒自動刷新，確保自動掃描引擎持續運行
st_autorefresh(interval=15000, limit=None, key="heartbeat")

# 自定義 CSS
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 1.8rem !important; white-space: nowrap !important; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem !important; }
    .stMetric { border: 1px solid rgba(128, 128, 128, 0.3); padding: 10px !important; border-radius: 10px; }
    .price-box {
        background-color: rgba(128, 128, 128, 0.1);
        padding: 15px; border-radius: 10px; border-left: 5px solid #17BECF; margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

# ===============================
# 1. 通訊與數據核心函數
# ===============================
def send_telegram_msg(message):
    """發送訊息至 Telegram"""
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        res = requests.post(url, json=payload, timeout=5)
        return res.json().get("ok")
    except: return False

@st.cache_data(ttl=600)
def get_unified_analysis(symbol):
    """統一技術指標計算邏輯"""
    try:
        df = yf.Ticker(symbol).history(period="2y")
        if df.empty: return None
        # 均線與布林帶
        df['SMA20'] = df['Close'].rolling(20).mean()
        df['SMA200'] = df['Close'].rolling(200).mean()
        std = df['Close'].rolling(20).std()
        df['BB_upper'] = df['SMA20'] + 2 * std
        df['BB_lower'] = df['SMA20'] - 2 * std
        # ATR 風控計算
        hl = df['High'] - df['Low']
        hc = (df['High'] - df['Close'].shift()).abs()
        lc = (df['Low'] - df['Close'].shift()).abs()
        df['ATR'] = pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(14).mean()
        # RSI 與 MACD
        delta = df['Close'].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        ema12 = df['Close'].ewm(span=12, adjust=False).mean()
        ema26 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = ema12 - ema26
        df['Hist'] = df['MACD'] - df['MACD'].ewm(span=9, adjust=False).mean()
        df['Vol_MA20'] = df['Volume'].rolling(20).mean()
        return df
    except: return None

# ===============================
# 2. Google Sheets 雲端連線
# ===============================
def get_gsheet_client():
    return gspread.service_account_from_dict(st.secrets["gcp_service_account"])

@st.cache_data(ttl=300)
def load_trades():
    try:
        sh = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE).sheet1
        df = pd.DataFrame(sh.get_all_records())
        if df.empty: return pd.DataFrame(columns=['Date','Ticker','Type','Price','Shares','Total'])
        for c in ['Price', 'Shares', 'Total']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        return df
    except: return pd.DataFrame(columns=['Date','Ticker','Type','Price','Shares','Total'])

def save_trade(d, ticker, t, p, s):
    try:
        sh = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE).sheet1
        sh.append_row([str(d), ticker.upper().strip(), t, float(p), float(s), float(p*s)])
        return True
    except: return False

def sync_nav_history(total_assets):
    try:
        ss = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE)
        try: ws_history = ss.worksheet("History")
        except:
            ws_history = ss.add_worksheet(title="History", rows="1000", cols="2")
            ws_history.append_row(["Date", "Total Assets"])
        today_str = date.today().strftime('%Y-%m-%d')
        existing = ws_history.get_all_records()
        if not (existing and existing[-1].get("Date") == today_str):
            ws_history.append_row([today_str, float(total_assets)])
        return pd.DataFrame(ws_history.get_all_records())
    except: return None

# ===============================
# 3. 自動化掃描與指令生成引擎
# ===============================
def run_auto_scanner(portfolio_list):
    """背景掃描所有持股，偵測是否達到買賣指令觸發價"""
    for item in portfolio_list:
        ticker = item['Ticker']
        current_shares = item['Shares']
        hist = get_unified_analysis(ticker)
        if hist is None: continue
        
        last = hist.iloc[-1]
        curr_p, curr_atr = last['Close'], last['ATR']
        
        # 評分邏輯 (加權計算)
        score = 0
        if curr_p > last['SMA200']: score += 2
        if last['RSI'] < 45: score += 1
        if last['Hist'] > 0: score += 1
        vol_ratio = last['Volume'] / last['Vol_MA20']
        if vol_ratio > 1.5 and last['Close'] > last['Open']: score += 1
        
        buy_target = (last['BB_lower'] + last['SMA20']) / 2
        sell_target = last['BB_upper']
        
        msg = ""
        # 買入指令 (分數高且接近建議價 1%)
        if score >= 3 and curr_p <= buy_target * 1.01:
            msg = (
                f"🔥 **【買入指令建議】**\n📌 標的: `{ticker}`\n💰 現價: `${curr_p:.2f}`\n"
                f"✅ 建議買入價: `${buy_target:.2f}` 以下\n📊 建議操作: **分批買入 10 股**\n"
                f"🛡️ 建議停損: `${(curr_p - 2*curr_atr):.2f}`\n🚀 建議獲利: `${(curr_p + 3*curr_atr):.2f}`\n評分: {score}/5"
            )
        # 賣出指令 (分數低且接近上軌 1%)
        elif score <= 1 and curr_p >= sell_target * 0.99 and current_shares > 0:
            reduce_shares = math.ceil(current_shares * 0.5)
            msg = (
                f"⚠️ **【賣出指令建議】**\n📌 標的: `{ticker}`\n💰 現價: `${curr_p:.2f}`\n"
                f"❌ 建議出場價: `${sell_target:.2f}` 以上\n📊 建議操作: **分批減碼 {reduce_shares} 股**\n"
                f"📉 目前持股: {current_shares} 股\n評分: {score}/5"
            )

        if msg:
            sent_key = f"auto_{ticker}_{date.today()}"
            if sent_key not in st.session_state:
                if send_telegram_msg(msg):
                    st.session_state[sent_key] = True
                    st.toast(f"已發送 {ticker} 交易通知！")

# ===============================
# 4. Sidebar 控制中心
# ===============================
st.sidebar.title("🎮 Command Center")

@st.cache_data(ttl=86400)
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        df = pd.read_html(response.text)[0]
        df['Display'] = df['Symbol'].str.replace('.', '-', regex=False) + " - " + df['Security']
        return sorted(df['Display'].tolist())
    except: return ["NVDA - NVIDIA", "AAPL - Apple", "TSLA - Tesla"]

if st.sidebar.button("發送 TG 測試訊息"):
    res = send_telegram_msg(f"🚀 通訊測試成功！\n時間：{datetime.now().strftime('%H:%M:%S')}")
    if res: st.sidebar.success("✅ 手機已收到！")

st.sidebar.divider()
initial_capital = st.sidebar.number_input("Initial Fund (USD)", value=32000, step=1000)
sp500_list = get_sp500_tickers()
is_manual = st.sidebar.checkbox("Manual Input (Ticker)")

with st.sidebar.form("trade_entry"):
    if is_manual: ticker_input = st.text_input("Enter Ticker").upper().strip()
    else:
        sel_stock = st.selectbox("Search Stock", options=sp500_list)
        ticker_input = sel_stock.split(" - ")[0] if sel_stock else ""
    t_type = st.selectbox("Type", ["買入 (Buy)", "賣出 (Sell)"])
    t_date = st.date_input("Date", date.today())
    t_price = st.number_input("Price", min_value=0.01, format="%.2f")
    t_shares = st.number_input("Shares", min_value=0.01, format="%.2f")
    if st.form_submit_button("Sync to Cloud"):
        if ticker_input and save_trade(t_date, ticker_input, t_type, t_price, t_shares):
            st.cache_data.clear()
            st.rerun()

# ===============================
# 5. 資產運算與持股清單
# ===============================
trades_df = load_trades()
unique_tickers = trades_df['Ticker'].unique().tolist() if not trades_df.empty else []
portfolio_cal, cash, total_realized_pl = [], initial_capital, 0

for ticker in unique_tickers:
    t_df = trades_df[trades_df['Ticker'] == ticker]
    shares_h, cost_b, ticker_realized_pl = 0, 0, 0
    for _, r in t_df.iterrows():
        val = r['Price'] * r['Shares']
        if "買入" in r['Type']:
            shares_h += r['Shares']; cost_b += val; cash -= val
        else:
            if shares_h > 0:
                avg_cost = cost_b / shares_h
                ticker_realized_pl += (r['Price'] - avg_cost) * r['Shares']
                cost_b -= avg_cost * r['Shares']
            shares_h -= r['Shares']; cash += val
    total_realized_pl += ticker_realized_pl
    if shares_h > 0:
        try:
            real_p = yf.Ticker(ticker).fast_info.last_price
            avg_cost_val = cost_b / shares_h
            portfolio_cal.append({
                "Ticker": ticker, "Shares": shares_h, "AvgCost": avg_cost_val, 
                "RealPrice": real_p, "Unrealized": (real_p - avg_cost_val) * shares_h, 
                "PL_Pct": ((real_p / avg_cost_val) - 1) * 100, "MktVal": shares_h*real_p
            })
        except: pass

total_unrealized_pl = sum(p['Unrealized'] for p in portfolio_cal)
total_assets = (sum(p['MktVal'] for p in portfolio_cal)) + cash
total_pl_v = total_assets - initial_capital
history_df = sync_nav_history(total_assets)

# ===============================
# 6. UI 顯示介面
# ===============================
st.title("🏛️ 專業級資產配置管理 V9.95")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("NAV 總值", f"${total_assets:,.1f}") 
c2.metric("Cash 購買力", f"${cash:,.1f}")
c3.metric("Realized 實現", f"${total_realized_pl:,.1f}")
c4.metric("Unrealized 未實現", f"${total_unrealized_pl:,.1f}", delta=f"{total_unrealized_pl:,.1f}")
c5.metric("Total P/L 總損益", f"${total_pl_v:,.1f}", f"{(total_pl_v/initial_capital*100):.2f}%")

# --- 持倉明細顏色邏輯 ---
def color_profit_loss(val):
    color = '#26A69A' if val > 0 else '#EF5350' if val < 0 else 'white'
    return f'color: {color}'
    
if history_df is not None and not history_df.empty:
    with st.expander("📈 績效回測追蹤 & 持倉明細", expanded=True):
        fig_nav = go.Figure()
        fig_nav.add_trace(go.Scatter(x=history_df['Date'], y=history_df['Total Assets'], mode='lines+markers', fill='tozeroy', name='NAV', line=dict(color='#00FFCC')))
        fig_nav.update_layout(template="plotly_dark", height=250, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig_nav, use_container_width=True)
        if portfolio_cal:
            st.markdown("#### 🔍 當前持倉實時明細 (顏色標註盈虧)")
            df_styled = pd.DataFrame(portfolio_cal)
            
            # 使用 Styler 進行顏色美化
            styled_table = df_styled.style.applymap(color_profit_loss, subset=['Unrealized', 'PL_Pct'])\
                .format({
                    'AvgCost': '${:,.2f}', 
                    'RealPrice': '${:,.2f}', 
                    'Unrealized': '${:,.2f}', 
                    'PL_Pct': '{:.2f}%', 
                    'MktVal': '${:,.2f}'
                })
            st.dataframe(styled_table, use_container_width=True)

# ===============================
# 7. 量化策略決策中心
# ===============================
st.divider()
st.subheader("🎯 策略決策中心")

analysis_mode = st.radio("選擇分析對象", ["我的持股", "搜尋全市場標的"], horizontal=True)
if analysis_mode == "我的持股":
    analyze_ticker = st.selectbox("選擇持倉標的", options=unique_tickers if unique_tickers else ["NVDA"])
else:
    selected_s = st.selectbox("從 S&P 500 搜尋", options=sp500_list)
    analyze_ticker = selected_s.split(" - ")[0] if selected_s else "NVDA"

hist = get_unified_analysis(analyze_ticker)
if hist is not None:
    l_col, r_col = st.columns([3, 7])
    last = hist.iloc[-1]
    curr_p, curr_atr = last['Close'], last['ATR']
    
    with l_col:
        st.subheader(f"🛠️ 建議策略 ({analyze_ticker})")
        st.markdown(f'<div class="price-box"><span style="font-size: 0.9rem; color: #888;">Current Market Price</span><br><span style="font-size: 2.2rem; font-weight: bold; color: #17BECF;">${curr_p:.2f}</span></div>', unsafe_allow_html=True)
        
        # 評分與指令 logic 與 run_auto_scanner 保持一致
        score = 0
        if curr_p > last['SMA200']: score += 2 
        if last['RSI'] < 45: score += 1 
        if last['Hist'] > 0: score += 1 
        
        buy_p = (last['BB_lower'] + last['SMA20']) / 2
        sell_p = last['BB_upper']

        if score >= 3:
            st.success(f"🔥 建議：分批買入 (評分: {score}/5)")
            st.markdown(f"📍 建議進場價: :green[${buy_p:.2f}] 以下")
        elif score <= 1:
            st.error(f"⚠️ 建議：分批減碼 (評分: {score}/5)")
            st.markdown(f"📍 建議出場價: :red[${sell_p:.2f}] 以上")
        else:
            st.warning(f"⚖️ 狀態：觀望 (評分: {score}/5)")

        st.divider()
        st.write(f"🛡️ **ATR 風控 (ATR: {curr_atr:.2f})**")
        st.write(f"- 建議停損 (2*ATR): :red[${(curr_p - 2*curr_atr):.2f}]")
        st.write(f"- 建議獲利 (3*ATR): :green[${(curr_p + 3*curr_atr):.2f}]")

    with r_col:
        df_plot = hist.tail(100)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)
        fig.add_trace(go.Candlestick(x=df_plot.index, open=df_plot['Open'], high=df_plot['High'], low=df_plot['Low'], close=df_plot['Close'], name="K線"), 1, 1)
        for ma, color in zip(['SMA20','SMA200', 'BB_upper', 'BB_lower'], ['#17BECF','#D62728', 'rgba(173,216,230,0.1)', 'rgba(173,216,230,0.1)']):
            fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[ma], name=ma, line=dict(width=1, color=color)), 1, 1)
        fig.update_layout(height=500, template="plotly_dark", xaxis_rangeslider_visible=False, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

# --- 核心：執行自動化持股掃描 ---
if portfolio_cal:
    run_auto_scanner(portfolio_cal)
    st.sidebar.success("🤖 自動掃描引擎：運行中")

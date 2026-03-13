# ===============================
# 0. 基礎設定 (V9.95 整合精確指令版)
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

# --- 憑證設定 ---
TG_TOKEN = st.secrets.get("TG_TOKEN", "8252298047:AAHJ_HSd_vrZlAC6RHtNQYaW6nJ1eywdKx4").strip()
TG_CHAT_ID = str(st.secrets.get("TG_CHAT_ID", "6484933731")).strip()
PORTFOLIO_SHEET_TITLE = 'US Stock' 

st.set_page_config(page_title="Pro 量化投資戰情室 V9.97", layout="wide")
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
# 1. 核心函數定義
# ===============================
def send_telegram_msg(message):
    if not TG_TOKEN or not TG_CHAT_ID: return False
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        res = requests.post(url, json=payload, timeout=5)
        return res.json().get("ok")
    except: return False

def get_recent_trade_status(ticker, trades_df):
    if trades_df.empty: return False, False
    COOLDOWN_DAYS = 3
    cutoff_date = date.today() - timedelta(days=COOLDOWN_DAYS)
    temp_df = trades_df.copy()
    temp_df['Date'] = pd.to_datetime(temp_df['Date']).dt.date
    recent = temp_df[(temp_df['Ticker'] == ticker) & (temp_df['Date'] >= cutoff_date)]
    return not recent[recent['Type'].str.contains("買入")].empty, not recent[recent['Type'].str.contains("賣出")].empty

# --- 持倉明細顏色邏輯 ---
def color_profit_loss(val):
    color = '#26A69A' if val > 0 else '#EF5350' if val < 0 else 'white'
    return f'color: {color}'

@st.cache_data(ttl=600)
def get_unified_analysis(symbol):
    try:
        df = yf.Ticker(symbol).history(period="2y")
        if df.empty: return None
        df['SMA20'] = df['Close'].rolling(20).mean(); df['SMA200'] = df['Close'].rolling(200).mean()
        std = df['Close'].rolling(20).std()
        df['BB_upper'] = df['SMA20'] + 2 * std; df['BB_lower'] = df['SMA20'] - 2 * std
        hl, hc, lc = df['High']-df['Low'], (df['High']-df['Close'].shift()).abs(), (df['Low']-df['Close'].shift()).abs()
        df['ATR'] = pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(14).mean()
        delta = df['Close'].diff()
        gain, loss = delta.clip(lower=0).rolling(14).mean(), -delta.clip(upper=0).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        df['Hist'] = df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()
        return df
    except: return None

def get_gsheet_client():
    return gspread.service_account_from_dict(st.secrets["gcp_service_account"])

@st.cache_data(ttl=300)
def load_trades():
    try:
        sh = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE).sheet1
        data = sh.get_all_records()
        if not data: return pd.DataFrame(columns=['Date','Ticker','Type','Price','Shares','Total'])
        df = pd.DataFrame(data)
        for c in ['Price', 'Shares', 'Total']: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
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
        except: ws_history = ss.add_worksheet(title="History", rows="1000", cols="2"); ws_history.append_row(["Date", "Total Assets"])
        today_str = date.today().strftime('%Y-%m-%d')
        existing = ws_history.get_all_records()
        if not (existing and str(existing[-1].get("Date")) == today_str): ws_history.append_row([today_str, float(total_assets)])
        return pd.DataFrame(ws_history.get_all_records())
    except: return None

# ===============================
# 4. Sidebar 控制中心 (UI 優先渲染)
# ===============================
st.sidebar.title("🎮 Command Center")
if st.sidebar.button("發送 TG 測試訊息"):
    if send_telegram_msg("🚀 測試成功！"): st.sidebar.success("✅ 手機已收到！")

st.sidebar.divider()
initial_capital = st.sidebar.number_input("Initial Fund (USD)", value=32000, step=1000)

# --- 這裡將新增交易功能往上提 ---
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
            st.cache_data.clear(); st.rerun()

# ===============================
# 5. 資產運算
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
                "MktVal": shares_h*real_p, "RealPrice": real_p, 
                "Unrealized": (real_p - avg_cost_val) * shares_h, "PL_Pct": ((real_p / avg_cost_val) - 1) * 100
            })
        except: pass

total_assets = (sum(p['MktVal'] for p in portfolio_cal)) + cash
total_pl_v = total_assets - initial_capital
history_df = sync_nav_history(total_assets)

# UI 佈局
st.title("🏛️ 專業級資產配置管理 V9.4")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("NAV 總值", f"${total_assets:,.1f}") 
c2.metric("Cash 購買力", f"${cash:,.1f}")
c3.metric("Realized 實現", f"${total_realized_pl:,.1f}")
c4.metric("Unrealized 未實現", f"${sum(p['Unrealized'] for p in portfolio_cal):,.1f}")
c5.metric("Total P/L 總損益", f"${total_pl_v:,.1f}", f"{(total_pl_v/initial_capital*100):.2f}%")

if history_df is not None and not history_df.empty:
    with st.expander("📈 投資組合績效回測追蹤 (NAV Curve) & 持倉明細", expanded=True):
        fig_nav = go.Figure()
        fig_nav.add_trace(go.Scatter(x=history_df['Date'], y=history_df['Total Assets'], mode='lines+markers', fill='tozeroy', name='NAV', line=dict(color='#00FFCC')))
        fig_nav.update_layout(template="plotly_dark", height=300, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig_nav, use_container_width=True)
        
        if portfolio_cal:
            detail_df = pd.DataFrame(portfolio_cal)
            for col in ['AvgCost', 'RealPrice', 'Unrealized', 'MktVal']:
                detail_df[col] = detail_df[col].map('${:,.2f}'.format)
            detail_df['PL_Pct'] = detail_df['PL_Pct'].map('{:,.2f}%'.format)
            st.dataframe(detail_df[['Ticker', 'Shares', 'AvgCost', 'RealPrice', 'Unrealized', 'PL_Pct', 'MktVal']], use_container_width=True)

total_unrealized_pl = sum(p['Unrealized'] for p in portfolio_cal)
total_assets = (sum(p['MktVal'] for p in portfolio_cal)) + cash
total_pl_v = total_assets - initial_capital
history_df = sync_nav_history(total_assets)
# ===============================
# 7. 量化策略決策中心 (UI 動態股數)
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
    
    # 取得當前持倉資訊
    held_shares = 0
    current_weight = 0
    for item in portfolio_cal:
        if item['Ticker'] == analyze_ticker:
            held_shares = item['Shares']
            current_weight = item['MktVal'] / total_assets
            break

    with l_col:
        st.subheader(f"🛠️ 策略詳情: {analyze_ticker}")
        st.markdown(f'<div class="price-box">現價: <span style="font-size: 1.8rem;">${curr_p:.2f}</span></div>', unsafe_allow_html=True)

        # 執行冷卻偵測
        has_bought, has_sold = get_recent_trade_status(analyze_ticker, trades_df)
        
        # 評分系統
        score = 0
        if curr_p > last['SMA200']: score += 2 
        if last['RSI'] < 40: score += 1.5 
        if last['Hist'] > 0: score += 1 
        if curr_p < last['BB_lower']: score += 1 

        # 輸出邏輯
        if has_sold: 
            st.info(f"⏳ 處於減碼冷卻期 (3天內已有賣出記錄)。")
        elif has_bought: 
            st.info(f"⏳ 處於建倉冷卻期 (3天內已有買入記錄)。")
        elif score >= 3.5:
            buy_price = (last['BB_lower'] + last['SMA20']) / 2
            suggest_qty = math.floor((cash * 0.15) / buy_price)
            st.success(f"🔥 強力建議：分批買入 (評分: {score})")
            st.markdown(f"📍 建議進場價: :green[${buy_price:.2f}]")
            if current_weight >= 0.3: 
                st.warning("⚠️ 警示：單一標的佔比已 > 30%，停止增持。")
            elif suggest_qty >= 1: 
                st.markdown(f"📋 建議買進股數: :orange[{suggest_qty}] 股")
        elif (score <= 1 or last['RSI'] > 75) and held_shares >= 1:
            sell_price = last['BB_upper']
            sell_qty = math.ceil(held_shares * 0.33)
            st.error(f"⚠️ 建議：分批減碼 (評分: {score})")
            st.markdown(f"📍 建議出場價: :red[${sell_price:.2f}]")
            st.markdown(f"📋 建議賣出股數: :orange[{sell_qty}] 股")
        else: 
            st.warning(f"⚖️ 狀態：觀望 (目前評分: {score})")
            
        st.divider()
        st.write(f"- 當前 RSI: `{last['RSI']:.1f}`")
        st.write(f"- 組合權重: `{current_weight*100:.1f}%`")
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

def run_auto_scanner(portfolio_list, trades_df, current_cash, total_assets):
    if not portfolio_list: return
    for item in portfolio_list:
        ticker = item['Ticker']
        held_shares = item['Shares']
        current_weight = item['MktVal'] / total_assets if total_assets > 0 else 0
        hist = get_unified_analysis(ticker)
        if hist is None: continue
        last = hist.iloc[-1]
        curr_p = last['Close']
        has_bought, has_sold = get_recent_trade_status(ticker, trades_df)
        score = 0
        if curr_p > last['SMA200']: score += 2 
        if last['RSI'] < 40: score += 1.5 
        if last['Hist'] > 0: score += 1 
        if curr_p < last['BB_lower']: score += 1 
        
        msg = ""
        if score >= 3.5 and not has_bought and current_weight < 0.3:
            buy_price = (last['BB_lower'] + last['SMA20']) / 2
            qty = math.floor((current_cash * 0.15) / buy_price)
            if qty >= 1: msg = f"🔥 強力買入 {ticker}: {qty} 股"
        elif (score <= 1 or last['RSI'] > 75) and held_shares >= 1 and not has_sold:
            qty = math.ceil(held_shares * 0.33)
            msg = f"⚠️ 建議減碼 {ticker}: {qty} 股"
        
        if msg:
            if f"tg_{ticker}_{date.today()}" not in st.session_state:
                if send_telegram_msg(msg): st.session_state[f"tg_{ticker}_{date.today()}"] = True

if portfolio_cal:
    run_auto_scanner(portfolio_cal, trades_df, cash, total_assets)
    st.sidebar.success("🤖 自動掃描引擎：運行中")

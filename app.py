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
import google.generativeai as genai
from streamlit_autorefresh import st_autorefresh

PORTFOLIO_SHEET_TITLE = 'US Stock' 
st.set_page_config(page_title="Pro 量化投資戰情室 V9.0", layout="wide")
st_autorefresh(interval=15000, limit=None, key="heartbeat")

# 配置 Gemini API (請確保在 st.secrets 中設定 GEMINI_API_KEY)
try:
    if "GEMINI_API_KEY" in st.secrets:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        ai_model = genai.GenerativeModel('gemini-1.5-flash')
    else:
        st.sidebar.warning("⚠️ 未偵測到 GEMINI_API_KEY，AI 功能將受限")
except Exception as e:
    st.sidebar.error(f"AI 配置出錯: {e}")

st.markdown("""
    <style>
    [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        white-space: nowrap !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.9rem !important;
    }
    .stMetric {
        border: 1px solid rgba(128, 128, 128, 0.3);
        padding: 10px !important;
        border-radius: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

# ===============================
# 1. 數據獲取與 AI 邏輯
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

def get_ai_insight(ticker, score, last_price, rsi):
    """詢問 AI 對該標的的展望"""
    prompt = f"""
    你是專業的美股分析師。目前標的 {ticker} 的量化指標如下：
    - 技術面總分: {score}/4 (4分為極強勢)
    - 目前股價: ${last_price:.2f}
    - RSI 指標: {rsi:.1f}
    
    請針對此標的提供：
    1. 近期公司經營現況與亮點。
    2. 未來一季的產業前景與潛在催化劑（Catalysts）。
    3. 針對目前量化得分 {score}，給予風險管理建議。
    請用繁體中文回答，條列式呈現，語氣客觀且具洞察力。
    """
    try:
        response = ai_model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"AI 服務暫時無法回應: {str(e)}"

# ===============================
# 2. 技術指標核心 (邏輯維持原樣)
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
# 5. 資產運算 (包含個股損益明細)
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
        avg_cost_val = cost_b / shares_h
        unrealized_pl = (real_p - avg_cost_val) * shares_h
        unrealized_pct = ((real_p / avg_cost_val) - 1) * 100
        portfolio_cal.append({
            "Ticker": ticker, "Shares": shares_h, "AvgCost": avg_cost_val, 
            "MktVal": shares_h*real_p, "RealPrice": real_p, 
            "Unrealized": unrealized_pl, "PL_Pct": unrealized_pct
        })

total_unrealized_pl = sum(p['Unrealized'] for p in portfolio_cal)
total_assets = (sum(p['MktVal'] for p in portfolio_cal)) + cash
total_pl_v = total_assets - initial_capital

# UI: 頂部總覽
st.title("🏛️ 專業級資產配置管理")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("NAV 總值", f"${total_assets:,.1f}") 
c2.metric("Cash 購買力", f"${cash:,.1f}")
c3.metric("Realized 實現", f"${total_realized_pl:,.1f}")
c4.metric("Unrealized 未實現", f"${total_unrealized_pl:,.1f}")
c5.metric("Total P/L 總損益", f"${total_pl_v:,.1f}", f"{(total_pl_v/initial_capital*100):.2f}%")

# 個股持倉明細表格
if portfolio_cal:
    with st.expander("🔍 查看個股即時損益明細", expanded=True):
        detail_df = pd.DataFrame(portfolio_cal)
        display_df = detail_df.copy()
        display_df['AvgCost'] = display_df['AvgCost'].map('${:,.2f}'.format)
        display_df['RealPrice'] = display_df['RealPrice'].map('${:,.2f}'.format)
        display_df['Unrealized'] = display_df['Unrealized'].map('${:,.2f}'.format)
        display_df['PL_Pct'] = display_df['PL_Pct'].map('{:,.2f}%'.format)
        display_df['MktVal'] = display_df['MktVal'].map('${:,.2f}'.format)
        st.dataframe(display_df[['Ticker', 'Shares', 'AvgCost', 'RealPrice', 'Unrealized', 'PL_Pct', 'MktVal']], use_container_width=True)

# ===============================
# 6. 量化策略引擎 + AI 分析模組
# ===============================
st.divider()
st.subheader("🎯 策略決策與 AI 洞察")

# 分析對象切換
analysis_mode = st.radio("選擇分析對象", ["我的持股", "搜尋全市場標的"], horizontal=True)

if analysis_mode == "我的持股":
    analyze_target = st.selectbox("選擇持倉標的", options=unique_tickers if unique_tickers else ["NVDA"])
else:
    col_s1, col_s2 = st.columns([1, 2])
    with col_s1:
        search_manual = st.checkbox("手動輸入代碼")
    with col_s2:
        if search_manual:
            analyze_target = st.text_input("請輸入代碼 (如: TSLA)", value="NVDA").upper().strip()
        else:
            selected_s = st.selectbox("從 S&P 500 搜尋", options=sp500_list)
            analyze_target = selected_s.split(" - ")[0] if selected_s else "NVDA"

hist = get_analysis(analyze_target)
if hist is not None:
    l_col, r_col = st.columns([3, 7])
    last = hist.iloc[-1]
    curr_p = last['Close']
    
    with l_col:
        st.subheader(f"🛠️ 建議策略 ({analyze_target})")
        target_info = next((item for item in portfolio_cal if item["Ticker"] == analyze_target), None)
        held_shares = target_info['Shares'] if target_info else 0
        current_weight = (target_info['MktVal'] / total_assets) if total_assets > 0 and target_info else 0
        
        # 冷卻期判斷
        COOLDOWN_DAYS = 3
        cutoff_date_str = (date.today() - timedelta(days=COOLDOWN_DAYS)).strftime('%Y-%m-%d')
        recent_trades = trades_df[(trades_df['Ticker'] == analyze_target) & (trades_df['Date'] >= cutoff_date_str)]
        has_sold_recently = not recent_trades[recent_trades['Type'].str.contains("賣出")].empty
        has_bought_recently = not recent_trades[recent_trades['Type'].str.contains("買入")].empty

        # 評分邏輯
        score = 0
        if curr_p > last['SMA200']: score += 2 
        if last['RSI'] < 45: score += 1 
        if last['Hist'] > 0: score += 1 
        
        if has_sold_recently: st.info(f"⏳ 處於減碼冷卻期 ({COOLDOWN_DAYS} 天內已賣出)")
        elif has_bought_recently: st.info(f"⏳ 處於建倉冷卻期 ({COOLDOWN_DAYS} 天內已買入)")
        elif score >= 3:
            buy_price = (last['BB_lower'] + last['SMA20']) / 2
            suggest_qty = math.floor((cash * 0.2) / buy_price)
            st.success(f"🔥 建議：分批買入")
            st.markdown(f"📍 建議進場價: :green[${buy_price:.2f}]")
            if current_weight >= 0.3: st.warning("⚠️ 警示：單一持股佔比 > 30%。")
            elif suggest_qty >= 1: st.markdown(f"📋 建議股數: :orange[{suggest_qty}] 股")
        elif score <= 1 and held_shares > 1:
            sell_price = last['BB_upper']
            sell_qty = math.ceil(held_shares * 0.33)
            st.error(f"⚠️ 建議：分批減碼")
            st.markdown(f"📍 建議出場價: :red[${sell_price:.2f}]")
            st.markdown(f"📋 建議減碼股數: :orange[{sell_qty}] 股")
        else: 
            st.warning("⚖️ 狀態：觀望 (Hold)")

        st.divider()
        # 新增 AI 分析按鈕 (功能擴充)
        if st.button(f"🤖 詢問 AI 對 {analyze_target} 的看法"):
            with st.spinner("Gemini 正在調閱資料並分析..."):
                insight = get_ai_insight(analyze_target, score, curr_p, last['RSI'])
                st.markdown("### 🧠 AI 投資觀點")
                st.info(insight)
        
    with r_col:
        st.subheader(f"📊 {analyze_target} 技術面動態圖表")
        df_plot = hist.tail(100)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)
        fig.add_trace(go.Candlestick(x=df_plot.index, open=df_plot['Open'], high=df_plot['High'], low=df_plot['Low'], close=df_plot['Close'], name="K線"), 1, 1)
        for ma, color in zip(['SMA20','SMA200'], ['#17BECF','#D62728']):
            fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[ma], name=ma, line=dict(width=1, color=color)), 1, 1)
        
        colors = ['red' if val < 0 else 'green' for val in df_plot['Hist']]
        fig.add_trace(go.Bar(x=df_plot.index, y=df_plot['Hist'], name="MACD Hist", marker_color=colors), 2, 1)
        fig.update_layout(height=600, template="plotly_dark", xaxis_rangeslider_visible=False)
        st.plotly_chart(fig, use_container_width=True)

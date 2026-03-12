import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date, timedelta
import math
import gspread
import time
import requests  # 新增：用於處理帶有 Headers 的請求
from streamlit_autorefresh import st_autorefresh

# ===============================
# 0. 基礎設定
# ===============================
PORTFOLIO_SHEET_TITLE = 'Streamlit US Stock' # 建議更名以符合多股需求
st.set_page_config(page_title="多角化美股戰情室 V7.6", layout="wide")
st_autorefresh(interval=10000, limit=None, key="heartbeat")

# ===============================
# 1. 數據獲取 (修正後的 S&P 500 爬蟲)
# ===============================
@st.cache_data(ttl=86400)
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    # 關鍵修正：加入 User-Agent 偽裝成瀏覽器，避免 403 錯誤
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        # 使用 response.text 傳給 pandas
        df = pd.read_html(response.text)[0]
        # 建立 "Ticker - Security" 格式
        df['Display'] = df['Symbol'].str.replace('.', '-', regex=False) + " - " + df['Security']
        return sorted(df['Display'].tolist())
    except Exception as e:
        st.error(f"爬蟲依然受阻，啟用緊急備用名單。錯誤原因: {e}")
        # 這裡放一些核心大盤股作為備用，確保程式不崩潰
        return ["NVDA - NVIDIA", "AAPL - Apple", "TSLA - Tesla", "MSFT - Microsoft", "GOOGL - Alphabet", "AMZN - Amazon", "META - Meta"]

# ===============================
# 2. Google Sheet 整合
# ===============================
def get_gsheet_client():
    # 確保 st.secrets["gcp_service_account"] 已在 Streamlit Cloud 設定
    return gspread.service_account_from_dict(st.secrets["gcp_service_account"])

@st.cache_data(ttl=300)
def load_trades():
    try:
        sh = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE).sheet1
        df = pd.DataFrame(sh.get_all_records())
        if df.empty:
            return pd.DataFrame(columns=['Date','Ticker','Type','Price','Shares','Total'])
        for c in ['Price','Shares','Total']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.error(f"Google Sheets 讀取失敗，請確認標題欄位是否正確。")
        return pd.DataFrame(columns=['Date','Ticker','Type','Price','Shares','Total'])

def save_trade(d, ticker, t, p, s):
    try:
        sh = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE).sheet1
        sh.append_row([str(d), ticker.upper(), t, float(p), float(s), float(p*s)])
        return True
    except Exception as e:
        st.error(f"寫入失敗：{e}")
        return False

# ===============================
# 3. 技術分析與行情
# ===============================
@st.cache_data(ttl=600)
def get_analysis(symbol):
    df = yf.Ticker(symbol).history(period="2y")
    if df.empty: return None
    df['SMA20'] = df['Close'].rolling(20).mean()
    df['SMA60'] = df['Close'].rolling(60).mean()
    df['SMA200'] = df['Close'].rolling(200).mean()
    std = df['Close'].rolling(20).std()
    df['BB_upper'] = df['SMA20'] + 2 * std
    df['BB_lower'] = df['SMA20'] - 2 * std
    df['BB_pos'] = (df['Close'] - df['BB_lower']) / (df['BB_upper'] - df['BB_lower'] + 1e-9) * 100
    delta = df['Close'].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
    ema12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = ema12 - ema26
    df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['Hist'] = df['MACD'] - df['Signal']
    return df

def get_realtime_batch(tickers):
    data = {}
    for t in tickers:
        try:
            fast = yf.Ticker(t).fast_info
            data[t] = {"price": fast.last_price, "prev": fast.previous_close}
        except:
            data[t] = {"price": 0, "prev": 0}
    return data

# ===============================
# 4. Sidebar: 操作面板 (支援手動輸入)
# ===============================
st.sidebar.header("🕹️ 控制中心")
initial_capital = st.sidebar.number_input("帳戶初始資金 (USD)", value=30000, step=1000)

sp500_list = get_sp500_tickers()

# 在 Form 之外先決定輸入模式，因為 st.checkbox 不能放在 Form 裡面改變 Layout
is_manual = st.sidebar.checkbox("找不到標的？開啟手動輸入")

with st.sidebar.form("trade_entry"):
    st.subheader("新增交易紀錄")
    
    if is_manual:
        # 手動輸入模式：輸入任何代碼 (如 AXTI, ONDS, 甚至 2330.TW)
        ticker_clean = st.text_input("請輸入完整代碼 (例如: ONDS)").upper().strip()
    else:
        # 下拉搜尋模式
        selected_stock = st.selectbox("搜尋標的 (S&P 500)", options=sp500_list)
        ticker_clean = selected_stock.split(" - ")[0]
    
    t_type = st.selectbox("類型", ["買入 (Buy)", "賣出 (Sell)"])
    t_date = st.date_input("日期", date.today())
    t_price = st.number_input("單價", min_value=0.01, format="%.2f")
    t_shares = st.number_input("股數", min_value=0.01, format="%.2f")
    
    if st.form_submit_button("存入紀錄"):
        if not ticker_clean:
            st.error("請輸入或選擇有效的股票代碼")
        elif save_trade(t_date, ticker_clean, t_type, t_price, t_shares):
            st.success(f"{ticker_clean} 成功同步至雲端")
            st.cache_data.clear()
            st.rerun()

if st.sidebar.button("🔄 手動刷新所有行情"):
    st.cache_data.clear()
    st.rerun()

# ===============================
# 5. 核心資產運算邏輯
# ===============================
trades_df = load_trades()
unique_tickers = trades_df['Ticker'].unique().tolist() if not trades_df.empty else []
rt_prices = get_realtime_batch(unique_tickers)

portfolio = []
cash = initial_capital

# 移動平均成本法計算
for ticker in unique_tickers:
    t_df = trades_df[trades_df['Ticker'] == ticker]
    shares, cost_basis = 0, 0
    for _, r in t_df.iterrows():
        val = r['Price'] * r['Shares']
        if "買入" in r['Type']:
            shares += r['Shares']
            cost_basis += val
            cash -= val
        else:
            if shares > 0:
                avg_c = cost_basis / shares
                cost_basis -= avg_c * r['Shares']
            shares -= r['Shares']
            cash += val
    
    if shares > 0:
        curr_p = rt_prices.get(ticker, {}).get('price', 0)
        mkt_v = shares * curr_p
        pl = mkt_v - cost_basis
        portfolio.append({
            "Ticker": ticker, "Shares": shares, 
            "AvgCost": cost_basis/shares, "CurrentPrice": curr_p,
            "MktVal": mkt_v, "PL": pl, "PLPct": (pl/cost_basis)*100
        })

total_mkt_val = sum(item['MktVal'] for item in portfolio)
total_assets = total_mkt_val + cash
total_pl_val = total_assets - initial_capital

# ===============================
# 6. Dashboard 視覺化
# ===============================
st.title("📊 全方位美股投資戰情室")

# 頂部指標
c1, c2, c3, c4 = st.columns(4)
c1.metric("資產總值 (NAV)", f"${total_assets:,.2f}")
c2.metric("剩餘購買力", f"${cash:,.2f}")
c3.metric("組合總損益", f"${total_pl_val:,.2f}", f"{(total_pl_val/initial_capital*100):.2f}%")
c4.metric("持倉檔數", f"{len(portfolio)}")

# 持股細節表格
if portfolio:
    st.subheader("📋 目前持股透視")
    p_df = pd.DataFrame(portfolio)
    p_df['權重'] = (p_df['MktVal'] / total_assets * 100).map("{:.1f}%".format)
    st.dataframe(p_df.style.format({
        'AvgCost': '{:.2f}', 'CurrentPrice': '{:.2f}', 'MktVal': '{:.2f}', 'PL': '{:.2f}', 'PLPct': '{:.2f}%'
    }), use_container_width=True)

# 個股分析與建議
st.divider()
analyze_target = st.selectbox("🎯 選擇分析標的", options=unique_tickers if unique_tickers else ["NVDA"])

hist = get_analysis(analyze_target)
if hist is not None:
    l_col, r_col = st.columns([7, 3])
    
    with l_col:
        st.subheader(f"📈 {analyze_target} 技術面動態")
        df_p = hist.tail(120)
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, row_heights=[0.6, 0.2, 0.2], vertical_spacing=0.05)
        fig.add_trace(go.Candlestick(x=df_p.index, open=df_p['Open'], high=df_p['High'], low=df_p['Low'], close=df_p['Close'], name="K線"), 1, 1)
        for ma, color in zip(['SMA20','SMA60','SMA200'], ['#17BECF','#FF7F0E','#D62728']):
            fig.add_trace(go.Scatter(x=df_p.index, y=df_p[ma], name=ma, line=dict(width=1, color=color)), 1, 1)
        fig.add_trace(go.Scatter(x=df_p.index, y=df_p['RSI'], name="RSI", line=dict(color='#E377C2')), 2, 1)
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
        bar_colors = ['#2CA02C' if v >= 0 else '#D62728' for v in df_p['Hist']]
        fig.add_trace(go.Bar(x=df_p.index, y=df_p['Hist'], marker_color=bar_colors, name="MACD"), 3, 1)
        fig.update_layout(height=700, template="plotly_dark", xaxis_rangeslider_visible=False, margin=dict(t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

    with r_col:
        last = hist.iloc[-1]
        curr_p = rt_prices.get(analyze_target, {}).get('price') or last['Close']
        bull = curr_p > last['SMA200']
        macd_up = last['Hist'] > hist['Hist'].iloc[-2]
        
        # 分數邏輯
        score = (2.0 if bull else 0) + (1.5 if macd_up else 0) + (1.0 if last['RSI'] < 40 else 0)
        
        # 權重檢查
        target_row = p_df[p_df['Ticker'] == analyze_target]
        current_weight = (target_row['MktVal'].values[0] / total_assets) if not target_row.empty else 0
        
        st.subheader("🤖 AI 策略評估")
        action, advice_qty = "HOLD", 0
        
        if score >= 3.5 and current_weight < 0.25:
            action = "STRONG BUY"
            advice_qty = math.floor((cash * 0.25) / curr_p)
        elif score >= 2.5 and current_weight < 0.15:
            action = "BUY"
            advice_qty = math.floor((cash * 0.1) / curr_p)
        elif last['RSI'] > 78 or last['BB_pos'] > 95:
            action = "SELL / TAKE PROFIT"
            held_s = target_row['Shares'].values[0] if not target_row.empty else 0
            advice_qty = math.ceil(held_s * 0.25)

        st.metric("當前策略建議", action)
        st.metric("建議操作股數", f"{advice_qty} 股")
        
        st.info(f"""
        **核心指標摘要:**
        - 市場價格: `${curr_p:.2f}`
        - RSI 指數: `{last['RSI']:.1f}`
        - 布林位置: `{last['BB_pos']:.1f}%`
        - 組合權重: `{current_weight*100:.1f}%`
        - 趨勢狀態: `{"✅ 多頭排列" if bull else "❌ 處於年線下方"}`
        """)
        
        if current_weight > 0.3:
            st.warning("⚠️ 警告：該股權重已超過 30%，為了分散風險，不建議再進行買入操作。")

with st.expander("📝 歷史流水帳查詢"):
    st.dataframe(trades_df.sort_index(ascending=False), use_container_width=True)

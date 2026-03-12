import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date, timedelta
import math
import gspread
import time
from streamlit_autorefresh import st_autorefresh

# ===============================
# 0. 基礎設定
# ===============================
PORTFOLIO_SHEET_TITLE = 'Streamlit US Stock' # 建議更名以符合多股需求
st.set_page_config(page_title="多角化美股戰情室 V7.5", layout="wide")
st_autorefresh(interval=10000, limit=None, key="heartbeat")

# ===============================
# 1. 數據獲取 (S&P 500 爬蟲)
# ===============================
@st.cache_data(ttl=86400) # 每天更新一次清單即可
def get_sp500_tickers():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        df = pd.read_html(url)[0]
        # 建立 "Ticker - Security" 格式供下拉選單搜尋
        df['Display'] = df['Symbol'] + " - " + df['Security']
        return df['Display'].tolist()
    except Exception as e:
        st.error(f"無法獲取 S&P 500 清單，切換至備用模式: {e}")
        return ["NVDA - NVIDIA", "AAPL - Apple", "TSLA - Tesla", "MSFT - Microsoft"]

# ===============================
# 2. Google Sheet 整合
# ===============================
def get_gsheet_client():
    return gspread.service_account_from_dict(st.secrets["gcp_service_account"])

@st.cache_data(ttl=300)
def load_trades():
    try:
        sh = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE).sheet1
        df = pd.DataFrame(sh.get_all_records())
        if df.empty:
            return pd.DataFrame(columns=['Date','Ticker','Type','Price','Shares','Total'])
        # 確保格式一致
        for c in ['Price','Shares','Total']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.error(f"讀取失敗，請檢查試算表標題是否包含 [Date, Ticker, Type, Price, Shares, Total] \n錯誤：{e}")
        return pd.DataFrame(columns=['Date','Ticker','Type','Price','Shares','Total'])

def save_trade(d, ticker, t, p, s):
    try:
        sh = get_gsheet_client().open(PORTFOLIO_SHEET_TITLE).sheet1
        sh.append_row([str(d), ticker, t, float(p), float(s), float(p*s)])
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
    # 均線
    df['SMA20'] = df['Close'].rolling(20).mean()
    df['SMA60'] = df['Close'].rolling(60).mean()
    df['SMA200'] = df['Close'].rolling(200).mean()
    # 布林
    std = df['Close'].rolling(20).std()
    df['BB_upper'] = df['SMA20'] + 2 * std
    df['BB_lower'] = df['SMA20'] - 2 * std
    df['BB_pos'] = (df['Close'] - df['BB_lower']) / (df['BB_upper'] - df['BB_lower'] + 1e-9) * 100
    # RSI
    delta = df['Close'].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
    # MACD
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
# 4. Sidebar: 交易錄入與搜尋
# ===============================
st.sidebar.header("🕹️ 操作面板")
initial_capital = st.sidebar.number_input("初始總資金 (USD)", value=32000, step=1000)

sp500_list = get_sp500_tickers()

with st.sidebar.form("trade_entry"):
    st.subheader("新增交易紀錄")
    # 搜尋式下拉選單
    selected_stock = st.selectbox("搜尋代碼或公司名稱", options=sp500_list)
    ticker_clean = selected_stock.split(" - ")[0]
    
    t_type = st.selectbox("交易類型", ["買入 (Buy)", "賣出 (Sell)"])
    t_date = st.date_input("交易日期", date.today())
    t_price = st.number_input("成交單價", min_value=0.01, format="%.2f")
    t_shares = st.number_input("股數", min_value=0.01, format="%.2f")
    
    if st.form_submit_button("確認送出"):
        if save_trade(t_date, ticker_clean, t_type, t_price, t_shares):
            st.success(f"{ticker_clean} 紀錄成功")
            st.cache_data.clear()
            st.rerun()

if st.sidebar.button("🔄 強制刷新行情"):
    st.cache_data.clear()
    st.rerun()

# ===============================
# 5. 帳戶資產計算邏輯
# ===============================
trades_df = load_trades()
unique_tickers = trades_df['Ticker'].unique().tolist() if not trades_df.empty else []
rt_prices = get_realtime_batch(unique_tickers)

portfolio = []
cash = initial_capital

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
            "AvgCost": cost_basis/shares, "MktVal": mkt_v,
            "PL": pl, "PLPct": (pl/cost_basis)*100
        })

total_mkt_val = sum(item['MktVal'] for item in portfolio)
total_assets = total_mkt_val + cash
total_pl_val = total_assets - initial_capital

# ===============================
# 6. Dashboard UI
# ===============================
st.title("🚀 多策略戰情室 V7.5")

c1, c2, c3, c4 = st.columns(4)
c1.metric("總資產淨值", f"${total_assets:,.0f}")
c2.metric("可用現金", f"${cash:,.0f}")
c3.metric("總損益", f"${total_pl_val:,.0f}", f"{(total_pl_val/initial_capital*100):.2f}%")
c4.metric("持倉數量", f"{len(portfolio)} 檔")

if portfolio:
    st.subheader("📋 當前持股狀態")
    p_df = pd.DataFrame(portfolio)
    p_df['權重'] = (p_df['MktVal'] / total_assets * 100).map("{:.1f}%".format)
    st.dataframe(p_df.style.format({
        'AvgCost': '{:.2f}', 'MktVal': '{:.2f}', 'PL': '{:.2f}', 'PLPct': '{:.2f}%'
    }), use_container_width=True)

# ===============================
# 7. 個股策略與分析區
# ===============================
st.divider()
analyze_target = st.selectbox("🎯 選擇要分析的股票", options=unique_tickers if unique_tickers else ["NVDA"])

hist = get_analysis(analyze_target)
if hist is not None:
    l_col, r_col = st.columns([7, 3])
    
    with l_col:
        st.subheader(f"{analyze_target} 技術線圖")
        df_p = hist.tail(120)
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, row_heights=[0.6, 0.2, 0.2])
        fig.add_trace(go.Candlestick(x=df_p.index, open=df_p['Open'], high=df_p['High'], low=df_p['Low'], close=df_p['Close'], name="K線"), 1, 1)
        for ma, color in zip(['SMA20','SMA60','SMA200'], ['yellow','orange','red']):
            fig.add_trace(go.Scatter(x=df_p.index, y=df_p[ma], name=ma, line=dict(width=1, color=color)), 1, 1)
        fig.add_trace(go.Scatter(x=df_p.index, y=df_p['RSI'], name="RSI", line=dict(color='aqua')), 2, 1)
        bar_colors = ['#00ff00' if v >= 0 else '#ff4444' for v in df_p['Hist']]
        fig.add_trace(go.Bar(x=df_p.index, y=df_p['Hist'], marker_color=bar_colors, name="MACD"), 3, 1)
        fig.update_layout(height=650, template="plotly_dark", xaxis_rangeslider_visible=False, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with r_col:
        last = hist.iloc[-1]
        curr_p = rt_prices.get(analyze_target, {}).get('price') or last['Close']
        
        # 核心策略邏輯
        bull = curr_p > last['SMA200']
        macd_up = last['Hist'] > hist['Hist'].iloc[-2]
        score = (2.0 if bull else 0) + (1.5 if macd_up else 0) + (1.0 if last['RSI'] < 40 else 0)
        
        # 獲取當前這檔股票的權重
        current_weight = (p_df[p_df['Ticker'] == analyze_target]['MktVal'].values[0] / total_assets) if not p_df[p_df['Ticker'] == analyze_target].empty else 0
        
        st.subheader("🤖 策略決策")
        action, advice_qty = "HOLD", 0
        
        if score >= 3.5 and current_weight < 0.30: # 分數高且權重未滿 30%
            action = "STRONG BUY"
            advice_qty = math.floor((cash * 0.2) / curr_p)
        elif score >= 2.5 and current_weight < 0.15:
            action = "BUY"
            advice_qty = math.floor((cash * 0.1) / curr_p)
        elif last['RSI'] > 75 or last['BB_pos'] > 95:
            action = "SELL / TAKE PROFIT"
            held_s = p_df[p_df['Ticker'] == analyze_target]['Shares'].values[0] if not p_df[p_df['Ticker'] == analyze_target].empty else 0
            advice_qty = math.ceil(held_s * 0.3)

        st.metric("建議行動", action)
        st.metric("操作股數", f"{advice_qty} 股")
        
        st.info(f"""
        **數據指標:**
        - 目前價格: `${curr_p:.2f}`
        - RSI (14): `{last['RSI']:.1f}`
        - 多頭排列: `{"是" if bull else "否"}`
        - MACD動能: `{"向上" if macd_up else "向下"}`
        - 組合佔比: `{current_weight*100:.1f}%`
        """)
        
        if current_weight > 0.35:
            st.warning("⚠️ 警示：單一標的佔比過高（>35%），不建議再加碼。")

with st.expander("查看原始交易日誌"):
    st.dataframe(trades_df.sort_index(ascending=False), use_container_width=True)

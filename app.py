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
st.set_page_config(page_title="多策略美股戰情室 V7.0", layout="wide")
st_autorefresh(interval=10000, limit=None, key="heartbeat") # 多股查詢較耗時，建議改為 10s

# ===============================
# 1. Google Sheet 整合與處理
# ===============================
def get_gsheet_client():
    return gspread.service_account_from_dict(st.secrets["gcp_service_account"])

@st.cache_data(ttl=600)
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
        st.error(f"交易紀錄讀取失敗：{e}")
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
# 2. 技術指標核心 (共用)
# ===============================
@st.cache_data(ttl=600)
def get_analysis(symbol):
    df = yf.Ticker(symbol).history(period="2y")
    if df.empty: return None

    # 指標計算
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
    rs = gain / (loss + 1e-9)
    df['RSI'] = 100 - (100 / (1 + rs))

    ema12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = ema12 - ema26
    df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['Hist'] = df['MACD'] - df['Signal']
    return df

def get_realtime_prices(tickers):
    data = {}
    for t in tickers:
        try:
            ticker_obj = yf.Ticker(t)
            # 獲取快速報價
            fast = ticker_obj.fast_info
            data[t] = {"price": fast.last_price, "prev": fast.previous_close}
        except:
            data[t] = {"price": None, "prev": None}
    return data

# ===============================
# 3. Sidebar 控制台
# ===============================
st.sidebar.header("🕹️ 帳戶管理")
initial_capital = st.sidebar.number_input("初始總資金", value=32000, step=1000)

st.sidebar.markdown("---")
st.sidebar.header("➕ 新增交易")
with st.sidebar.form("trade_form"):
    new_ticker = st.text_input("股票代碼 (如: NVDA, TSLA)").upper()
    d = st.date_input("交易日期", date.today())
    t = st.selectbox("類型", ["買入 (Buy)", "賣出 (Sell)"])
    p = st.number_input("單價", min_value=0.0, format="%.2f")
    s = st.number_input("股數", min_value=0.0, format="%.2f")
    if st.form_submit_button("提交交易"):
        if new_ticker and p > 0 and s > 0:
            if save_trade(d, new_ticker, t, p, s):
                st.success(f"{new_ticker} 已紀錄")
                st.cache_data.clear()
                st.rerun()

if st.sidebar.button("🔄 刷新數據"):
    st.cache_data.clear()
    st.rerun()

# ===============================
# 4. 投資組合邏輯處理
# ===============================
trades_df = load_trades()
held_tickers = trades_df['Ticker'].unique().tolist() if not trades_df.empty else []

# 計算各別持股狀態
portfolio_status = []
current_cash = initial_capital

for ticker in held_tickers:
    t_df = trades_df[trades_df['Ticker'] == ticker]
    shares = 0
    total_cost = 0
    
    for _, r in t_df.iterrows():
        amt = r['Price'] * r['Shares']
        if "買入" in r['Type']:
            shares += r['Shares']
            total_cost += amt
            current_cash -= amt
        else:
            # 賣出時按比例減少成本 (移動平均成本法)
            if shares > 0:
                avg_cost = total_cost / shares
                total_cost -= avg_cost * r['Shares']
            shares -= r['Shares']
            current_cash += amt
            
    if shares > 0:
        portfolio_status.append({
            "Ticker": ticker,
            "Shares": shares,
            "AvgCost": total_cost / shares,
            "CostBasis": total_cost
        })

# 獲取即時價格
prices_map = get_realtime_prices(held_tickers)

# ===============================
# 5. Dashboard 總覽面版
# ===============================
st.title("📊 多角化投資戰情室")

total_mkt_val = 0
for p in portfolio_status:
    curr_p = prices_map.get(p['Ticker'], {}).get('price')
    if curr_p:
        total_mkt_val += p['Shares'] * curr_p

total_assets = total_mkt_val + current_cash
total_pl = total_assets - initial_capital
total_pl_pct = (total_pl / initial_capital) * 100

m1, m2, m3, m4 = st.columns(4)
m1.metric("總資產淨值", f"${total_assets:,.2f}")
m2.metric("持倉市值", f"${total_mkt_val:,.2f}")
m3.metric("可用現金", f"${current_cash:,.2f}")
m4.metric("投資組合損益", f"${total_pl:,.2f}", f"{total_pl_pct:.2f}%")

# 持股明細表格
if portfolio_status:
    st.subheader("📋 目前持股明細")
    display_df = pd.DataFrame(portfolio_status)
    display_df['目前價格'] = display_df['Ticker'].map(lambda x: prices_map.get(x, {}).get('price'))
    display_df['現值'] = display_df['Shares'] * display_df['目前價格']
    display_df['損益'] = display_df['現值'] - display_df['CostBasis']
    display_df['報酬率'] = (display_df['損益'] / display_df['CostBasis']) * 100
    display_df['佔比'] = (display_df['現值'] / total_assets) * 100
    
    st.dataframe(display_df.style.format({
        'AvgCost': '{:.2f}', '目前價格': '{:.2f}', '現值': '{:.2f}', 
        '損益': '{:.2f}', '報酬率': '{:.2f}%', '佔比': '{:.2f}%'
    }), use_container_width=True)

# ===============================
# 6. 個股策略分析區
# ===============================
st.markdown("---")
target_ticker = st.selectbox("🎯 選擇分析目標", held_tickers if held_tickers else ["NVDA"])

col_left, col_right = st.columns([7, 3])

with col_left:
    hist_data = get_analysis(target_ticker)
    if hist_data is not None:
        st.subheader(f"📈 {target_ticker} 技術分析")
        df_plot = hist_data.tail(126)
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, row_heights=[0.6,0.2,0.2])
        fig.add_trace(go.Candlestick(x=df_plot.index, open=df_plot['Open'], high=df_plot['High'], low=df_plot['Low'], close=df_plot['Close'], name="Price"), 1, 1)
        for ma in ['SMA20','SMA60','SMA200']:
            fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot[ma], name=ma, line=dict(width=1)), 1, 1)
        fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['RSI'], name="RSI", line=dict(color='yellow')), 2, 1)
        colors = ['green' if v >= 0 else 'red' for v in df_plot['Hist']]
        fig.add_trace(go.Bar(x=df_plot.index, y=df_plot['Hist'], marker_color=colors, name="MACD"), 3, 1)
        fig.update_layout(height=600, template="plotly_dark", xaxis_rangeslider_visible=False, margin=dict(l=10,r=10,t=30,b=10))
        st.plotly_chart(fig, use_container_width=True)

with col_right:
    if hist_data is not None:
        last = hist_data.iloc[-1]
        prev_hist = hist_data['Hist'].iloc[-2]
        curr_price = prices_map.get(target_ticker, {}).get('price') or last['Close']
        
        # 策略邏輯優化
        bull = curr_price > last['SMA200']
        macd_up = last['Hist'] > prev_hist and last['MACD'] > 0
        
        score = 0
        score += 2.0 if bull else 0
        score += 1.5 if macd_up else 0
        score += 1.0 if last['RSI'] < 40 else 0
        score += 0.5 if last['BB_pos'] < 20 else 0
        
        st.subheader("🧠 策略 AI 建議")
        
        # 獲取該股當前持倉比例
        target_info = next((item for item in portfolio_status if item["Ticker"] == target_ticker), None)
        current_shares = target_info['Shares'] if target_info else 0
        current_weight = ((current_shares * curr_price) / total_assets) if total_assets > 0 else 0

        action = "HOLD"
        qty = 0
        
        # 風控優化：單一持股不超過總資產 35%
        if score >= 3.5 and current_weight < 0.35:
            action = "STRONG BUY"
            qty = math.floor((current_cash * 0.3) / curr_price)
        elif score >= 2.5 and current_weight < 0.20:
            action = "BUY"
            qty = math.floor((current_cash * 0.15) / curr_price)
        elif last['RSI'] > 80 or (last['BB_pos'] > 90 and not macd_up):
            action = "SELL / TAKE PROFIT"
            qty = math.ceil(current_shares * 0.3)

        st.metric("建議行動", action)
        st.metric("建議股數", f"{qty} 股")
        
        st.markdown(f"""
        **狀態檢查清單:**
        - 趨勢: {"✅ 多頭" if bull else "❌ 空頭"}
        - MACD動能: {"✅ 轉強" if macd_up else "❌ 疲弱"}
        - RSI: `{last['RSI']:.1f}`
        - 當前佈局權重: `{current_weight*100:.1f}%`
        """)
        
        if current_weight > 0.4:
            st.error("⚠️ 警告：單一持股佔比過高，建議減碼以分散風險。")

# ===============================
# 7. 歷史流水帳
# ===============================
with st.expander("📖 查看完整交易流水帳"):
    st.table(trades_df.sort_index(ascending=False))

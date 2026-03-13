# ===============================
# 0. 基礎設定與 AI 核心配置
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
st.set_page_config(page_title="Pro 量化投資戰情室 V9.4", layout="wide")
st_autorefresh(interval=15000, limit=None, key="heartbeat")

# --- AI 全域配置 ---
api_key = st.secrets.get("GEMINI_API_KEY")
try:
    if api_key:
        genai.configure(api_key=api_key)
    else:
        st.sidebar.warning("⚠️ Secrets 中未偵測到 GEMINI_API_KEY")
except Exception as e:
    st.sidebar.error(f"AI 初始配置失敗: {e}")

# ===============================
# 1. 核心功能函式 (新增動態模型探索)
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
    """向 Gemini 請求分析，具備安全性寬鬆設定與強制顯示機制"""
    if not api_key:
        return "❌ AI 模組未就緒，請檢查 Secrets 中的 API Key 設定。"
    
    # 設定安全性過濾器為最低，避免因為財經數據被誤判為有害內容而回傳空白
    safety_settings = [
        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    ]
    
    prompt = f"""
    你是專業美股分析師。請針對標的 {ticker} 分析：
    1. 當前股價 ${last_price:.2f} 且量化得分 {score}/4 情況下的短評。
    2. 該公司近期營運亮點與未來一季展望。
    3. 針對目前分數給予風險管理建議。
    請用繁體中文，條列式回答，確保語氣專業。
    """
    
    try:
        available_models = [
            m.name for m in genai.list_models() 
            if 'generateContent' in m.supported_generation_methods
        ]
        
        # 優先順序：1.5-flash > 1.0-pro > 清單第一個
        target_model_name = next((m for m in available_models if '1.5-flash' in m), 
                                 next((m for m in available_models if 'pro' in m), available_models[0]))
        
        model = genai.GenerativeModel(model_name=target_model_name, safety_settings=safety_settings)
        response = model.generate_content(prompt)
        
        # 檢查是否有內容回傳
        if response and response.text:
            return f"*(✅ 實際使用模型: `{target_model_name}`)*\n\n" + response.text
        else:
            return "⚠️ AI 回傳了空值，可能是內容觸發了 Google 的自動過濾機制。"

    except Exception as e:
        # 捕捉細節錯誤，例如權限問題或配額限制
        error_msg = str(e)
        if "User location is not supported" in error_msg:
            return "❌ 錯誤：您的 API Key 所在的 GCP 專案區域不支援此模型。"
        return f"AI 請求發生例外錯誤: {error_msg}"

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
# 2. Google Sheets 整合 
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
# 3. UI 控制中心
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
# 4. 資產運算與損益顯示
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
        avg_c = cost_b / shares_h
        unreal_pl = (real_p - avg_c) * shares_h
        unreal_pct = ((real_p / avg_c) - 1) * 100
        portfolio_cal.append({
            "Ticker": ticker, "Shares": shares_h, "AvgCost": avg_c, 
            "MktVal": shares_h*real_p, "RealPrice": real_p, 
            "Unrealized": unreal_pl, "PL_Pct": unreal_pct
        })

total_unrealized_pl = sum(p['Unrealized'] for p in portfolio_cal)
total_assets = (sum(p['MktVal'] for p in portfolio_cal)) + cash
total_pl_v = total_assets - initial_capital

st.title("🏛️ 專業級資產配置管理")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("NAV 總值", f"${total_assets:,.1f}") 
c2.metric("Cash 購買力", f"${cash:,.1f}")
c3.metric("Realized 實現", f"${total_realized_pl:,.1f}")
c4.metric("Unrealized 未實現", f"${total_unrealized_pl:,.1f}")
c5.metric("Total P/L 總損益", f"${total_pl_v:,.1f}", f"{(total_pl_v/initial_capital*100):.2f}%")

if portfolio_cal:
    with st.expander("🔍 查看個股即時損益明細", expanded=True):
        df_show = pd.DataFrame(portfolio_cal)
        st.dataframe(df_show.style.format({
            'AvgCost': '${:,.2f}', 'RealPrice': '${:,.2f}', 
            'Unrealized': '${:,.2f}', 'PL_Pct': '{:.2f}%', 'MktVal': '${:,.2f}'
        }), use_container_width=True)

# ===============================
# 5. 策略建議與 AI 分析
# ===============================
st.divider()
analysis_mode = st.radio("選擇分析對象", ["我的持股", "搜尋全市場標的"], horizontal=True)

if analysis_mode == "我的持股":
    analyze_target = st.selectbox("選擇持倉標的", options=unique_tickers if unique_tickers else ["NVDA"])
else:
    search_target = st.text_input("輸入欲分析代碼 (如: TSLA, SOXL)", value="NVDA").upper().strip()
    analyze_target = search_target

hist = get_analysis(analyze_target)
if hist is not None:
    l_col, r_col = st.columns([3, 7])
    last = hist.iloc[-1]
    curr_p = last['Close']
    
    with l_col:
        st.subheader(f"🛠️ 策略建議 ({analyze_target})")
        target_info = next((item for item in portfolio_cal if item["Ticker"] == analyze_target), None)
        held_shares = target_info['Shares'] if target_info else 0
        
        # 冷卻期判斷
        COOLDOWN_DAYS = 3
        cutoff_date_str = (date.today() - timedelta(days=COOLDOWN_DAYS)).strftime('%Y-%m-%d')
        recent_trades = trades_df[(trades_df['Ticker'] == analyze_target) & (trades_df['Date'] >= cutoff_date_str)]
        has_sold_recently = not recent_trades[recent_trades['Type'].str.contains("賣出")].empty
        has_bought_recently = not recent_trades[recent_trades['Type'].str.contains("買入")].empty
        
        score = 0
        if curr_p > last['SMA200']: score += 2 
        if last['RSI'] < 45: score += 1 
        if last['Hist'] > 0: score += 1 

        if has_sold_recently: st.info(f"⏳ 處於減碼冷卻期 ({COOLDOWN_DAYS} 天內已賣出)")
        elif has_bought_recently: st.info(f"⏳ 處於建倉冷卻期 ({COOLDOWN_DAYS} 天內已買入)")
        elif score >= 3:
            st.success("🔥 建議：分批買入")
            buy_p = (last['BB_lower'] + last['SMA20']) / 2
            st.write(f"建議價: ${buy_p:.2f} 以下")
        elif score <= 1 and held_shares > 0:
            st.error("⚠️ 建議：分批減碼")
        else:
            st.warning("⚖️ 狀態：觀望 (Hold)")

        st.divider()
        if st.button(f"🤖 詢問 AI 對 {analyze_target} 的看法"):
            with st.spinner("🚀 AI 正在深度分析中..."):
                result = get_ai_insight(analyze_target, score, curr_p, last['RSI'])
                st.markdown(f"### 🧠 AI 投資觀點 ({analyze_target})")
                st.write(result) # 使用 st.write 或 st.markdown

    with r_col:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3])
        df_p = hist.tail(100)
        fig.add_trace(go.Candlestick(x=df_p.index, open=df_p['Open'], high=df_p['High'], low=df_p['Low'], close=df_p['Close'], name="K線"), 1, 1)
        fig.update_layout(height=500, template="plotly_dark", xaxis_rangeslider_visible=False)
        st.plotly_chart(fig, use_container_width=True)

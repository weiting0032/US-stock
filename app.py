from datetime import date, datetime, time as dtime
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from core import (
    DEFAULT_COMMISSION, DEFAULT_INITIAL_CAPITAL, DEFAULT_SLIPPAGE_PCT,
    build_portfolio, build_trade_preview, calculate_performance_metrics,
    calc_portfolio_heat, clear_market_cache, color_pl, display_divergence,
    display_market_regime, enrich_portfolio_with_weight_and_risk,
    evaluate_strategy, get_market_regime, get_recent_trade_status,
    get_sp500_tickers, get_unified_analysis, load_alerts, load_history,
    load_signals, load_trades, load_watchlist, maybe_log_daily_history,
    normalize_ticker, run_auto_scanner, save_trade, save_watchlist,
    send_telegram_msg, delete_watchlist_ticker, set_watchlist_enabled,
)

st.set_page_config(page_title="量化投資組合 Pro", layout="wide", initial_sidebar_state="collapsed")

# ===============================
# RWD UI CSS 注入 (針對折疊機與手機)
# ===============================
st.markdown(
    """
    <style>
    /* 全域字體與間距微調 */
    [data-testid="stMetricValue"] { font-size: 1.4rem !important; font-weight: 700; white-space: nowrap !important; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem !important; color: #a0a0a0; }
    .stMetric { border: 1px solid rgba(255, 255, 255, 0.1); padding: 12px !important; border-radius: 12px; background: rgba(255,255,255,0.03); }
    
    /* 質感卡片設計 */
    .mobile-card { border: 1px solid rgba(255,255,255,0.15); border-radius: 16px; padding: 16px; margin-bottom: 12px; background: linear-gradient(145deg, rgba(30,30,30,0.6) 0%, rgba(15,15,15,0.9) 100%); box-shadow: 0 4px 12px rgba(0,0,0,0.2); }
    .mobile-card-title { font-size: 1.15rem; font-weight: 800; margin-bottom: 8px; color: #E0E0E0; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 4px; }
    .mobile-card-text { font-size: 0.95rem; line-height: 1.6; color: #C0C0C0; }
    .price-box { background: rgba(23, 190, 207, 0.1); padding: 16px; border-radius: 12px; border-left: 6px solid #17BECF; margin-bottom: 16px; }
    .action-box { margin-top: 8px; padding: 8px; background: rgba(255, 255, 255, 0.05); border-radius: 8px; font-size: 0.9rem; }

    /* RWD 響應式：手機與摺疊外螢幕 */
    @media (max-width: 600px) {
        [data-testid="stMetricValue"] { font-size: 1.15rem !important; }
        .stMetric { padding: 8px !important; }
    }
    
    /* RWD 響應式：摺疊機內螢幕 */
    @media (min-width: 601px) and (max-width: 900px) {
        .mobile-card { padding: 20px; }
        .mobile-card-title { font-size: 1.3rem; }
    }
    </style>
    """, unsafe_allow_html=True
)

def render_signal_card(title: str, light: str, status: str, desc: str, value_text: str):
    st.markdown(
        f"""
        <div style="border:1px solid rgba(255,255,255,0.15); border-radius:14px; padding:16px; margin-bottom:12px; background: rgba(25,25,25,0.6);">
            <div style="font-size:0.9rem; color:#A0A0A0;">{title}</div>
            <div style="font-size:1.5rem; font-weight:700; margin:4px 0;">{light} {value_text}</div>
            <div style="font-size:1.05rem; font-weight:600; color:#E0E0E0;">{status}</div>
            <div style="font-size:0.85rem; color:#A8A8A8; margin-top:4px;">{desc}</div>
        </div>
        """, unsafe_allow_html=True
    )

# session defaults
for k, v in {"trade_ticker": "NVDA", "trade_type": "BUY", "trade_price": 100.0, "trade_shares": 1.0, "trade_note": "", "trade_fee": DEFAULT_COMMISSION}.items():
    if k not in st.session_state: st.session_state[k] = v

# ===============================
# Sidebar & Data Init
# ===============================
st.sidebar.title("🎮 終端控制")
mobile_mode = st.sidebar.toggle("📱 強制切換卡片流佈局", value=True, help="推薦手機與摺疊機使用者開啟")
if st.sidebar.button("🔄 刷新快取 (Sync)"): clear_market_cache(); st.rerun()

initial_capital = st.sidebar.number_input("初始資金 (USD)", min_value=1000.0, value=float(DEFAULT_INITIAL_CAPITAL), step=1000.0)

try:
    trades_df, watchlist_df = load_trades(), load_watchlist()
    history_df, alerts_df = load_history(), load_alerts()
except:
    trades_df, watchlist_df, history_df, alerts_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

portfolio_raw, cash, total_realized_pl = build_portfolio(trades_df, initial_capital)
market_value = sum(x["MarketValue"] for x in portfolio_raw)
total_assets, total_unrealized_pl = cash + market_value, sum(x["Unrealized"] for x in portfolio_raw)
total_pl = total_assets - initial_capital

market_regime = get_market_regime()
portfolio = enrich_portfolio_with_weight_and_risk(portfolio_raw, total_assets, cash, market_regime) if portfolio_raw else []
heat_info, perf = calc_portfolio_heat(portfolio, total_assets), calculate_performance_metrics(history_df)

# ===============================
# Header
# ===============================
st.title("📈 量化組合 Pro")
st.caption(f"Sync: {datetime.now().strftime('%m/%d %H:%M')} | 策略引擎在線")

if mobile_mode:
    st.markdown(f"**⚡ 摘要**：市場 {display_market_regime(market_regime['regime'])} | Heat {heat_info['heat_pct']:.1f}%")
    r1, r2 = st.columns(2)
    r1.metric("NAV", f"${total_assets:,.0f}")
    r2.metric("總損益", f"${total_pl:,.0f}", f"{(total_pl/initial_capital*100):.2f}%")
    r3, r4 = st.columns(2)
    r3.metric("現金部位", f"${cash:,.0f}")
    r4.metric("市場持倉", f"${market_value:,.0f}")
else:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("總資產 NAV", f"${total_assets:,.2f}")
    m2.metric("現金 (購買力)", f"${cash:,.2f}")
    m3.metric("持股市值", f"${market_value:,.2f}")
    m4.metric("總損益", f"${total_pl:,.2f}", f"{(total_pl / initial_capital * 100):.2f}%")

tab1, tab2, tab3 = st.tabs(["📊 組合儀表", "🎯 策略中樞", "📝 交易終端"])

# ===============================
# Tab 1: Dashboard
# ===============================
with tab1:
    if portfolio:
        # 在折疊機上，利用 Expander 節省空間
        with st.expander("📍 資產配置圓餅圖", expanded=not mobile_mode):
            pie_fig = go.Figure(data=[go.Pie(labels=[p["Ticker"] for p in portfolio], values=[p["MarketValue"] for p in portfolio], hole=0.5)])
            pie_fig.update_layout(template="plotly_dark", height=300, margin=dict(l=0, r=0, t=20, b=0), showlegend=not mobile_mode)
            st.plotly_chart(pie_fig, use_container_width=True, config={'displayModeBar': False})
            
        st.subheader("📋 目前持倉")
        for p in sorted(portfolio, key=lambda x: x["MarketValue"], reverse=True):
            pl_color = "#00E676" if p["Unrealized"] > 0 else "#FF1744" if p["Unrealized"] < 0 else "#E0E0E0"
            
            # 【關鍵修正】：安全解析 StopLoss 與 TakeProfit1，避免因 None 產生的格式化錯誤與 '-'
            sl_val = p.get('StopLoss')
            sl_str = f"${sl_val:.2f}" if sl_val and pd.notna(sl_val) else "-"
            tp_val = p.get('TakeProfit1')
            tp_str = f"${tp_val:.2f}" if tp_val and pd.notna(tp_val) else "-"

            # 【新增】：具體操作建議邏輯
            signal = p.get('Signal', 'WATCH')
            if "BUY" in signal:
                buy_qty = p.get('SuggestedBuyQty', 0)
                action_html = f"<div class='action-box'>🛒 <b>策略建議：</b><span style='color:#17BECF;'>建議加碼買進 {buy_qty} 股 (參考價: ${p['LastPrice']:.2f})</span></div>"
            elif "SELL" in signal:
                sell_qty = p.get('SuggestedSellQty', p['Shares'])
                action_html = f"<div class='action-box'>📉 <b>策略建議：</b><span style='color:#FF1744;'>建議減碼/出場 {sell_qty} 股 (參考價: ${p['LastPrice']:.2f})</span></div>"
            else:
                action_html = f"<div class='action-box'>👀 <b>策略建議：</b>目前無強烈訊號，建議持續觀望。</div>"

            st.markdown(
                f"""
                <div class="mobile-card">
                    <div class="mobile-card-title">{p['Ticker']} <span style="float:right; font-size:0.9rem; color:#17BECF;">{signal}</span></div>
                    <div class="mobile-card-text">
                        權重：<b>{p['WeightPct']:.1f}%</b> | 價值：${p['MarketValue']:,.0f}<br>
                        報酬：<span style="color:{pl_color}; font-weight:bold;">{p['PL_Pct']:.2f}% (${p['Unrealized']:,.0f})</span><br>
                        現價：${p['LastPrice']:.2f} | 成本：${p['AvgCost']:.2f}<br>
                        停損/目標：{sl_str} / {tp_str}
                    </div>
                    {action_html}
                </div>
                """, unsafe_allow_html=True
            )
    else:
        st.info("目前無持倉。")

# ===============================
# Tab 2: Strategy (Alpha)
# ===============================
with tab2:
    st.subheader("🎯 策略分析 (含布林擠壓 Squeeze)")
    analyze_ticker = normalize_ticker(st.text_input("輸入股票代碼 (e.g. NVDA)", value="NVDA"))
    
    if st.button("🚀 執行策略運算"):
        hist = get_unified_analysis(analyze_ticker)
        if hist is not None:
            held_shares = next((p["Shares"] for p in portfolio if p["Ticker"] == analyze_ticker), 0)
            score, action, details, note = evaluate_strategy(analyze_ticker, hist, held_shares, 0, total_assets, cash, market_regime, heat_info["heat_pct"], portfolio)
            
            st.markdown(f'<div class="price-box">動能分數: <span style="font-size: 1.5rem; color:#17BECF;">{score:.1f}</span> | {action}</div>', unsafe_allow_html=True)
            st.markdown(f"**分析依據**：{note}")
            
            # 【新增】：具體買賣股數與價位建議提示匡
            if "BUY" in action:
                st.success(f"🛒 **操作建議**：系統建議 **買進 {details['suggested_buy_qty']} 股** (參考現價/突破價: **${details['close']:.2f}**)。")
            elif "SELL" in action:
                st.warning(f"📉 **操作建議**：系統建議 **賣出 {details['suggested_sell_qty']} 股** (參考現價/跌破價: **${details['close']:.2f}**)。")
            else:
                st.info("👀 **操作建議**：目前未達動能突破門檻或已跌破停損，建議 **觀望 (WATCH)**。")
            
            # 手機適配技術圖
            plot_df = hist.tail(100)
            fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)
            fig.add_trace(go.Candlestick(x=plot_df.index, open=plot_df["Open"], high=plot_df["High"], low=plot_df["Low"], close=plot_df["Close"], name="K"), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["BB_upper"], line=dict(color='rgba(255,255,255,0.3)', dash='dot')), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["BB_lower"], fill='tonexty', fillcolor='rgba(23,190,207,0.05)', line=dict(color='rgba(255,255,255,0.3)', dash='dot')), row=1, col=1)
            fig.add_trace(go.Bar(x=plot_df.index, y=plot_df["MACD_Hist"], marker_color=plot_df["MACD_Hist"].apply(lambda x: '#00E676' if x>0 else '#FF1744')), row=2, col=1)
            
            fig.update_layout(template="plotly_dark", height=450, margin=dict(l=0, r=0, t=10, b=0), xaxis_rangeslider_visible=False, showlegend=False)
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False, 'scrollZoom': False})
        else:
            st.error("查無資料")

# ===============================
# Tab 3: Trade Terminal
# ===============================
with tab3:
    st.subheader("⚡ 快速下單終端")
    with st.form("trade_form"):
        t1, t2 = st.columns(2)
        tk = t1.text_input("Ticker", value=st.session_state["trade_ticker"])
        tp = t2.selectbox("方向", ["BUY", "SELL"])
        
        pr = st.number_input("價格", value=float(st.session_state["trade_price"]), format="%.2f")
        sh = st.number_input("股數", value=float(st.session_state["trade_shares"]), format="%.4f")
        
        if st.form_submit_button("🚀 同步至 Google Sheets", use_container_width=True):
            ok, msg = save_trade(datetime.now(), tk, tp, pr, sh)
            if ok: st.success(msg)
            else: st.error(msg)

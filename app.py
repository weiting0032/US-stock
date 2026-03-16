from datetime import date, datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from core import (
    DEFAULT_INITIAL_CAPITAL,
    build_portfolio,
    color_pl,
    display_divergence,
    enrich_portfolio_with_weight_and_risk,
    evaluate_strategy,
    get_market_regime,
    get_recent_trade_status,
    get_sp500_tickers,
    get_unified_analysis,
    load_alerts,
    load_trades,
    normalize_ticker,
    run_auto_scanner,
    save_trade,
    send_telegram_msg,
)

st.set_page_config(page_title="US Stock Portfolio Pro", layout="wide")

st.markdown(
    """
    <style>
    [data-testid="stMetricValue"] {
        font-size: 1.6rem !important;
        white-space: nowrap !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.95rem !important;
    }
    .stMetric {
        border: 1px solid rgba(128, 128, 128, 0.25);
        padding: 10px !important;
        border-radius: 12px;
        background: rgba(255,255,255,0.02);
    }
    .price-box {
        background-color: rgba(128, 128, 128, 0.08);
        padding: 14px;
        border-radius: 12px;
        border-left: 5px solid #17BECF;
        margin-bottom: 14px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# session defaults
if "trade_ticker" not in st.session_state:
    st.session_state["trade_ticker"] = "NVDA"
if "trade_type" not in st.session_state:
    st.session_state["trade_type"] = "BUY"
if "trade_price" not in st.session_state:
    st.session_state["trade_price"] = 100.0
if "trade_shares" not in st.session_state:
    st.session_state["trade_shares"] = 1.0
if "trade_note" not in st.session_state:
    st.session_state["trade_note"] = ""


# ===============================
# Sidebar
# ===============================
st.sidebar.title("🎮 Control Center")

if st.sidebar.button("🔄 Manual Refresh"):
    st.rerun()

if st.sidebar.button("📨 發送 Telegram 測試訊息"):
    if send_telegram_msg("✅ Telegram 連線測試成功"):
        st.sidebar.success("訊息已送出")
    else:
        st.sidebar.warning("送出失敗，請檢查 TG_TOKEN / TG_CHAT_ID")

initial_capital = st.sidebar.number_input(
    "Initial Capital (USD)",
    min_value=1000.0,
    value=float(DEFAULT_INITIAL_CAPITAL),
    step=1000.0
)

# UI 不自動背景掃描，只提供手動觸發
manual_scan = st.sidebar.button("🤖 手動執行一次掃描")


# ===============================
# Load Data
# ===============================
try:
    trades_df = load_trades()
except Exception as e:
    st.error(f"讀取交易紀錄失敗：{str(e)}")
    trades_df = pd.DataFrame(columns=["Date", "Ticker", "Type", "Price", "Shares", "Total", "Note"])

portfolio_raw, cash, total_realized_pl = build_portfolio(trades_df, initial_capital)
market_value = sum(x["MarketValue"] for x in portfolio_raw)
total_assets = cash + market_value
total_pl = total_assets - initial_capital

market_regime = get_market_regime()
portfolio = enrich_portfolio_with_weight_and_risk(
    portfolio_raw, total_assets, cash, market_regime
) if portfolio_raw else []

if manual_scan and portfolio:
    try:
        scan_logs = run_auto_scanner(portfolio, trades_df, cash, total_assets, market_regime)
        st.sidebar.success("掃描完成")
        with st.sidebar.expander("掃描結果"):
            for line in scan_logs:
                st.write(line)
    except Exception as e:
        st.sidebar.error(f"掃描失敗：{str(e)}")


# ===============================
# Main UI
# ===============================
st.title("🏛️ US Stock Portfolio Pro")
st.caption(f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("NAV", f"${total_assets:,.2f}")
m2.metric("Cash", f"${cash:,.2f}")
m3.metric("Market Value", f"${market_value:,.2f}")
m4.metric("Realized P/L", f"${total_realized_pl:,.2f}")
m5.metric("Unrealized P/L", f"${sum(p['Unrealized'] for p in portfolio):,.2f}")
m6.metric("Total P/L", f"${total_pl:,.2f}", f"{(total_pl / initial_capital * 100):.2f}%")

st.info(f"📡 Market Regime: {market_regime['regime']} | Score: {market_regime['score']}")

tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "📝 Trade Center", "🎯 Strategy Center", "⚙️ Monitor"])


# ===============================
# Tab 1 Dashboard
# ===============================
with tab1:
    left, right = st.columns([6, 4])

    with left:
        st.subheader("📈 Portfolio Overview")
        if portfolio:
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                x=[p["Ticker"] for p in portfolio],
                y=[p["MarketValue"] for p in portfolio],
                marker_color="#17BECF"
            ))
            fig_bar.update_layout(
                template="plotly_dark",
                height=320,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_title="Ticker",
                yaxis_title="Market Value"
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("目前無持倉。")

    with right:
        st.subheader("📌 Portfolio Snapshot")
        invested_ratio = (market_value / total_assets * 100) if total_assets > 0 else 0
        cash_ratio = (cash / total_assets * 100) if total_assets > 0 else 0
        max_weight = max([p["WeightPct"] for p in portfolio], default=0)

        s1, s2, s3 = st.columns(3)
        s1.metric("Invested", f"{invested_ratio:.1f}%")
        s2.metric("Cash Ratio", f"{cash_ratio:.1f}%")
        s3.metric("Max Position", f"{max_weight:.1f}%")

        if portfolio:
            pie_fig = go.Figure(data=[
                go.Pie(
                    labels=[p["Ticker"] for p in portfolio],
                    values=[p["MarketValue"] for p in portfolio],
                    hole=0.45
                )
            ])
            pie_fig.update_layout(template="plotly_dark", height=320, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(pie_fig, use_container_width=True)

    st.subheader("📋 Current Holdings")
    if portfolio:
        holdings_df = pd.DataFrame(portfolio)
        display_cols = [
            "Ticker", "Shares", "AvgCost", "FIFOCostBasis", "LastPrice", "MarketValue",
            "Unrealized", "PL_Pct", "WeightPct", "ATR", "StopLoss",
            "TakeProfit", "TrailingStop", "Signal", "Divergence"
        ]
        holdings_df = holdings_df[display_cols].sort_values("MarketValue", ascending=False)

        styled = holdings_df.style.applymap(color_pl, subset=["Unrealized", "PL_Pct"]).format({
            "AvgCost": "${:,.2f}",
            "FIFOCostBasis": "${:,.2f}",
            "LastPrice": "${:,.2f}",
            "MarketValue": "${:,.2f}",
            "Unrealized": "${:,.2f}",
            "PL_Pct": "{:.2f}%",
            "WeightPct": "{:.2f}%",
            "ATR": "{:.2f}",
            "StopLoss": "${:,.2f}",
            "TakeProfit": "${:,.2f}",
            "TrailingStop": "${:,.2f}",
        })
        st.dataframe(styled, use_container_width=True)
    else:
        st.info("尚無持倉資料。")


# ===============================
# Tab 2 Trade Center
# ===============================
with tab2:
    st.subheader("📝 Add New Trade")
    sp500_list = get_sp500_tickers()

    with st.form("trade_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)

        with c1:
            manual_input = st.checkbox("Manual Ticker Input", value=True)
            if manual_input:
                ticker_input = normalize_ticker(
                    st.text_input("Ticker", value=st.session_state.get("trade_ticker", "NVDA"))
                )
            else:
                default_ticker = st.session_state.get("trade_ticker", "NVDA")
                default_index = 0
                for i, item in enumerate(sp500_list):
                    if item.startswith(default_ticker + " -"):
                        default_index = i
                        break
                selected_stock = st.selectbox("Search Stock", options=sp500_list, index=default_index)
                ticker_input = normalize_ticker(selected_stock.split(" - ")[0]) if selected_stock else ""

        with c2:
            default_trade_type = st.session_state.get("trade_type", "BUY")
            trade_type = st.selectbox("Type", ["BUY", "SELL"], index=0 if default_trade_type == "BUY" else 1)
            trade_date = st.date_input("Date", value=date.today())

        with c3:
            trade_price = st.number_input(
                "Price",
                min_value=0.01,
                value=float(st.session_state.get("trade_price", 100.00)),
                format="%.2f"
            )
            trade_shares = st.number_input(
                "Shares",
                min_value=0.0001,
                value=float(st.session_state.get("trade_shares", 1.0)),
                format="%.4f"
            )

        note = st.text_input("Note", value=st.session_state.get("trade_note", ""))
        submitted = st.form_submit_button("☁️ Sync to Cloud")

        if submitted:
            ok, msg = save_trade(
                trade_date=trade_date,
                ticker=ticker_input,
                trade_type=trade_type,
                price=trade_price,
                shares=trade_shares,
                note=note,
            )
            if ok:
                st.success(msg)
                st.session_state["trade_ticker"] = ticker_input
                st.session_state["trade_type"] = trade_type
                st.session_state["trade_price"] = trade_price
                st.session_state["trade_shares"] = trade_shares
                st.session_state["trade_note"] = note
                st.rerun()
            else:
                st.error(msg)

    st.divider()
    st.subheader("📚 Trade Records")
    if not trades_df.empty:
        show_df = trades_df.copy()
        show_df["Date"] = show_df["Date"].dt.strftime("%Y-%m-%d")
        st.dataframe(show_df.sort_values("Date", ascending=False), use_container_width=True)
    else:
        st.info("尚無交易資料。")


# ===============================
# Tab 3 Strategy Center
# ===============================
with tab3:
    st.subheader("🎯 Strategy Decision Center")

    sp500_list = get_sp500_tickers()
    analysis_mode = st.radio("選擇分析對象", ["我的持股", "搜尋全市場標的"], horizontal=True)

    if analysis_mode == "我的持股":
        available = [p["Ticker"] for p in portfolio] if portfolio else ["NVDA"]
        analyze_ticker = st.selectbox("選擇標的", options=available)
    else:
        col_a, col_b = st.columns([1, 2])
        with col_a:
            search_manual = st.checkbox("手動輸入代碼", value=False)
        with col_b:
            if search_manual:
                analyze_ticker = normalize_ticker(st.text_input("請輸入代碼", value="NVDA"))
            else:
                selected_s = st.selectbox("從 S&P 500 搜尋", options=sp500_list)
                analyze_ticker = normalize_ticker(selected_s.split(" - ")[0]) if selected_s else "NVDA"

    hist = get_unified_analysis(analyze_ticker)

    if hist is None or hist.empty:
        st.error("無法取得該股票資料。")
    else:
        held_shares = 0.0
        current_mkt_value = 0.0
        for p in portfolio:
            if p["Ticker"] == analyze_ticker:
                held_shares = p["Shares"]
                current_mkt_value = p["MarketValue"]
                break

        recent_buy, recent_sell = get_recent_trade_status(analyze_ticker, trades_df)
        score, action, details, note = evaluate_strategy(
            ticker=analyze_ticker,
            hist=hist,
            held_shares=held_shares,
            current_mkt_value=current_mkt_value,
            total_assets=total_assets,
            cash=cash,
            market_regime=market_regime
        )

        left, right = st.columns([3, 7])

        with left:
            st.subheader(f"🛠️ {analyze_ticker}")
            st.markdown(
                f'<div class="price-box">現價: <span style="font-size: 1.8rem;">${details["close"]:.2f}</span></div>',
                unsafe_allow_html=True
            )

            st.write(f"**Strategy Score:** `{score:.1f}`")
            st.write(f"**Current Weight:** `{details['current_weight']*100:.2f}%`")
            st.write(f"**Held Shares:** `{held_shares:.4f}`")
            st.write(f"**RSI:** `{details['rsi']:.1f}`")
            st.write(f"**ATR:** `{details['atr']:.2f}`")
            st.write(f"**Market Regime:** `{details['market_regime']}`")
            st.write(f"**Volume Divergence:** `{display_divergence(details['divergence'])}`")

            if recent_buy:
                st.info("⏳ 近期已有買入")
            if recent_sell:
                st.info("⏳ 近期已有賣出")

            quick_trade_type = "BUY"
            quick_trade_qty = max(1, int(details["suggested_buy_qty"])) if details["suggested_buy_qty"] >= 1 else 1
            quick_trade_price = float(details["target_buy_price"])

            if action == "BUY" and not recent_buy:
                st.success("🔥 建議：分批買入")
                st.markdown(f"- 建議買入股數：`{details['suggested_buy_qty']}`")
                st.markdown(f"- 建議進場價：`${details['target_buy_price']:.2f}`")
            elif action == "SELL" and not recent_sell and held_shares > 0:
                st.error("⚠️ 建議：分批減碼")
                st.markdown(f"- 建議賣出股數：`{details['suggested_sell_qty']}`")
                st.markdown(f"- 建議出場價：`${details['target_sell_price']:.2f}`")
                quick_trade_type = "SELL"
                quick_trade_qty = max(1, int(details["suggested_sell_qty"])) if details["suggested_sell_qty"] >= 1 else 1
                quick_trade_price = float(details["target_sell_price"])
            elif action == "BUY_READY":
                st.warning("🟡 接近買入區，可準備掛單")
            elif action == "SELL_READY":
                st.warning("🟠 接近賣出區，可準備減碼")
            else:
                st.warning("⚖️ 觀望")

            st.markdown(f"- Stop Loss：`${details['stop_loss']:.2f}`" if details["stop_loss"] else "- Stop Loss：N/A")
            st.markdown(f"- Take Profit：`${details['take_profit']:.2f}`" if details["take_profit"] else "- Take Profit：N/A")
            st.markdown(f"- Trailing Stop：`${details['trailing_stop']:.2f}`" if details["trailing_stop"] else "- Trailing Stop：N/A")

            st.divider()
            if st.button("帶入交易中心表單", key=f"quick_fill_{analyze_ticker}_{action}"):
                st.session_state["trade_ticker"] = analyze_ticker
                st.session_state["trade_type"] = quick_trade_type
                st.session_state["trade_price"] = round(quick_trade_price, 2)
                st.session_state["trade_shares"] = float(quick_trade_qty)
                st.session_state["trade_note"] = f"Strategy quick order | Score={score:.1f} | {action}"
                st.success("已帶入交易中心表單，請切換到 Tab 2 確認送出。")

        with right:
            plot_df = hist.tail(120)
            fig = make_subplots(
                rows=4, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.04,
                row_heights=[0.50, 0.16, 0.17, 0.17]
            )

            fig.add_trace(go.Candlestick(
                x=plot_df.index,
                open=plot_df["Open"],
                high=plot_df["High"],
                low=plot_df["Low"],
                close=plot_df["Close"],
                name="Candlestick"
            ), row=1, col=1)

            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["SMA20"], name="SMA20"), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["SMA50"], name="SMA50"), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["SMA200"], name="SMA200"), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["BB_upper"], name="BB Upper"), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["BB_lower"], name="BB Lower"), row=1, col=1)
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["TrailingStop"], name="Trailing Stop"), row=1, col=1)

            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["RSI"], name="RSI"), row=2, col=1)
            fig.add_trace(go.Bar(x=plot_df.index, y=plot_df["MACD_Hist"], name="MACD Hist"), row=3, col=1)
            fig.add_trace(go.Bar(x=plot_df.index, y=plot_df["Volume"], name="Volume"), row=4, col=1)

            fig.update_layout(
                template="plotly_dark",
                height=850,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_rangeslider_visible=False
            )
            st.plotly_chart(fig, use_container_width=True)


# ===============================
# Tab 4 Monitor
# ===============================
with tab4:
    st.subheader("⚙️ Monitor")

    st.write(f"- Trades rows: {len(trades_df)}")
    st.write(f"- Portfolio count: {len(portfolio)}")
    st.write(f"- Market Regime: {market_regime['regime']}")

    with st.expander("Alerts Log"):
        try:
            alerts_df = load_alerts()
            if not alerts_df.empty:
                st.dataframe(alerts_df.sort_values("DateTime", ascending=False), use_container_width=True)
            else:
                st.info("目前沒有 Alerts 紀錄。")
        except Exception as e:
            st.warning(f"讀取 Alerts 失敗：{str(e)}")

    with st.expander("Debug - Trades Data"):
        st.dataframe(trades_df, use_container_width=True)

from datetime import date, datetime, time as dtime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from core import (
    DEFAULT_COMMISSION,
    DEFAULT_INITIAL_CAPITAL,
    DEFAULT_SLIPPAGE_PCT,
    MAX_POSITION_WEIGHT,
    build_portfolio,
    build_trade_preview,
    calculate_performance_metrics,
    clear_market_cache,
    color_pl,
    display_divergence,
    display_market_regime,
    enrich_portfolio_with_weight_and_risk,
    evaluate_strategy,
    get_market_regime,
    get_recent_trade_status,
    get_sp500_tickers,
    get_unified_analysis,
    load_alerts,
    load_history,
    load_trades,
    load_watchlist,
    maybe_log_daily_history,
    normalize_ticker,
    run_auto_scanner,
    save_trade,
    save_watchlist,
    send_telegram_msg,
    delete_watchlist_ticker,
    set_watchlist_enabled,
)

st.set_page_config(page_title="美股投資組合專業版", layout="wide")

st.markdown(
    """
    <style>
    [data-testid="stMetricValue"] {
        font-size: 1.5rem !important;
        white-space: nowrap !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.92rem !important;
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
    unsafe_allow_html=True,
)

# session defaults
defaults = {
    "trade_ticker": "NVDA",
    "trade_type": "BUY",
    "trade_price": 100.0,
    "trade_shares": 1.0,
    "trade_note": "",
    "trade_fee": DEFAULT_COMMISSION,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ===============================
# Sidebar
# ===============================
st.sidebar.title("🎮 控制中心")

if st.sidebar.button("🔄 手動重新整理"):
    clear_market_cache()
    st.rerun()

if st.sidebar.button("📨 發送 Telegram 測試訊息"):
    if send_telegram_msg("✅ Telegram 連線測試成功"):
        st.sidebar.success("訊息已送出")
    else:
        st.sidebar.warning("送出失敗，請檢查 TG_TOKEN / TG_CHAT_ID")

initial_capital = st.sidebar.number_input(
    "初始資金 (USD)",
    min_value=1000.0,
    value=float(DEFAULT_INITIAL_CAPITAL),
    step=1000.0,
)

manual_scan = st.sidebar.button("🤖 手動執行一次掃描")
log_nav_now = st.sidebar.button("🧾 寫入今日 NAV")

# ===============================
# Load Data
# ===============================
try:
    trades_df = load_trades()
    watchlist_df = load_watchlist()
except Exception as e:
    st.error(f"資料讀取失敗：{str(e)}")
    trades_df = pd.DataFrame(columns=[
        "TradeDateTime", "CreatedAt", "Ticker", "Type", "Price", "Shares",
        "GrossTotal", "Fee", "Slippage", "NetTotal", "Note", "OrderID"
    ])
    watchlist_df = pd.DataFrame(columns=["Ticker", "Enabled", "Category", "Note"])

try:
    history_df = load_history()
except Exception:
    history_df = pd.DataFrame()

try:
    alerts_df = load_alerts()
except Exception:
    alerts_df = pd.DataFrame()

portfolio_raw, cash, total_realized_pl = build_portfolio(trades_df, initial_capital)
market_value = sum(x["MarketValue"] for x in portfolio_raw)
total_assets = cash + market_value
total_unrealized_pl = sum(x["Unrealized"] for x in portfolio_raw)
total_pl = total_assets - initial_capital

market_regime = get_market_regime()
portfolio = enrich_portfolio_with_weight_and_risk(
    portfolio_raw, total_assets, cash, market_regime
) if portfolio_raw else []

perf = calculate_performance_metrics(history_df)

if log_nav_now:
    ok, msg = maybe_log_daily_history(
        total_assets=total_assets,
        cash=cash,
        market_value=market_value,
        realized_pl=total_realized_pl,
        unrealized_pl=total_unrealized_pl,
    )
    if ok:
        st.sidebar.success(msg)
    else:
        st.sidebar.info(msg)

scan_result = None
if manual_scan:
    try:
        scan_result = run_auto_scanner(
            portfolio=portfolio,
            trades_df=trades_df,
            cash=cash,
            total_assets=total_assets,
            market_regime=market_regime,
            watchlist_df=watchlist_df,
        )
        st.sidebar.success("掃描完成")
        with st.sidebar.expander("掃描結果"):
            for line in scan_result["logs"]:
                st.write(line)
        with st.sidebar.expander("掃描統計"):
            st.json(scan_result["metrics"])
    except Exception as e:
        st.sidebar.error(f"掃描失敗：{str(e)}")


# ===============================
# Header
# ===============================
st.title("🏛️ 美股投資組合專業版")
st.caption(f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("總資產 NAV", f"${total_assets:,.2f}")
m2.metric("現金", f"${cash:,.2f}")
m3.metric("持股市值", f"${market_value:,.2f}")
m4.metric("已實現損益", f"${total_realized_pl:,.2f}")
m5.metric("未實現損益", f"${total_unrealized_pl:,.2f}")
m6.metric("總損益", f"${total_pl:,.2f}", f"{(total_pl / initial_capital * 100):.2f}%")

x1, x2, x3, x4 = st.columns(4)
x1.metric("市場狀態", display_market_regime(market_regime["regime"]))
x2.metric("Regime Score", market_regime["score"])
x3.metric("最大回撤", "-" if perf["max_drawdown_pct"] is None else f"{perf['max_drawdown_pct']:.2f}%")
x4.metric("Sharpe", "-" if perf["sharpe"] is None else f"{perf['sharpe']:.2f}")
if perf.get("history_points", 0) < 2:
    st.warning("NAV 歷史資料不足 2 筆，Sharpe 與最大回撤暫時不具參考性。請先累積每日 NAV。")

st.info(
    f"📡 市場狀態：{display_market_regime(market_regime['regime'])} ｜ "
    f"分數：{market_regime['score']} ｜ "
    f"VIX：{market_regime.get('vix', 'N/A')}"
)

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 儀表板",
    "📝 交易中心",
    "🎯 策略中心",
    "👀 Watchlist",
    "⚙️ 系統監控",
])


# ===============================
# Tab 1 Dashboard
# ===============================
with tab1:
    top_left, top_right = st.columns([6, 4])

    with top_left:
        st.subheader("📈 投資組合總覽")
        if portfolio:
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                x=[p["Ticker"] for p in portfolio],
                y=[p["MarketValue"] for p in portfolio],
                marker_color="#17BECF",
                text=[f"{p['WeightPct']:.1f}%" for p in portfolio],
                textposition="outside",
            ))
            fig_bar.update_layout(
                template="plotly_dark",
                height=320,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_title="股票代碼",
                yaxis_title="市值",
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("目前無持倉。")

    with top_right:
        st.subheader("📌 投資組合快照")
        invested_ratio = (market_value / total_assets * 100) if total_assets > 0 else 0
        cash_ratio = (cash / total_assets * 100) if total_assets > 0 else 0
        max_weight = max([p["WeightPct"] for p in portfolio], default=0)

        s1, s2, s3 = st.columns(3)
        s1.metric("持股比例", f"{invested_ratio:.1f}%")
        s2.metric("現金比例", f"{cash_ratio:.1f}%")
        s3.metric("最大持倉", f"{max_weight:.1f}%")

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

    st.subheader("🧾 淨值與基準表現")
    if history_df.empty:
        try:
            history_df = load_history()
        except Exception:
            history_df = pd.DataFrame()
    if not history_df.empty:
        nav_fig = go.Figure()
        nav_fig.add_trace(go.Scatter(
            x=history_df["Date"], y=history_df["TotalAssets"], mode="lines", name="Portfolio NAV"
        ))
        if "BenchmarkSPY" in history_df.columns and history_df["BenchmarkSPY"].notna().any():
            base_nav = history_df["TotalAssets"].dropna().iloc[0] if history_df["TotalAssets"].notna().any() else None
            base_spy = history_df["BenchmarkSPY"].dropna().iloc[0] if history_df["BenchmarkSPY"].notna().any() else None
            if base_nav and base_spy:
                normalized_spy = history_df["BenchmarkSPY"] / base_spy * base_nav
                nav_fig.add_trace(go.Scatter(
                    x=history_df["Date"], y=normalized_spy, mode="lines", name="SPY (Normalized)"
                ))
        nav_fig.update_layout(template="plotly_dark", height=350, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(nav_fig, use_container_width=True)
    else:
        st.info("尚無 NAV 歷史資料，可按側欄『寫入今日 NAV』。")

    st.subheader("🔔 最近提醒")
    if alerts_df.empty:
        try:
            alerts_df = load_alerts()
        except Exception:
            alerts_df = pd.DataFrame()
    if not alerts_df.empty:
        recent_alerts = alerts_df.sort_values("DateTime", ascending=False).head(10).copy()
        st.dataframe(recent_alerts, use_container_width=True)
    else:
        st.info("目前沒有提醒紀錄。")

    st.subheader("📋 目前持股")
    if portfolio:
        holdings_df = pd.DataFrame(portfolio)
        display_cols = [
            "Ticker", "Shares", "AvgCost", "FIFOCostBasis", "LastPrice", "MarketValue",
            "Unrealized", "PL_Pct", "WeightPct", "SignalScore", "Signal", "StrategyMode",
            "StopLoss", "TakeProfit1", "TakeProfit2", "TrendStop",
            "DistanceToStopPct", "DistanceToTP1Pct", "DistanceToTrendStopPct", "Divergence"
        ]
        holdings_df = holdings_df[display_cols].sort_values("MarketValue", ascending=False)
        holdings_df = holdings_df.rename(columns={
            "Ticker": "代碼",
            "Shares": "股數",
            "AvgCost": "平均成本",
            "FIFOCostBasis": "FIFO 成本",
            "LastPrice": "最新價",
            "MarketValue": "市值",
            "Unrealized": "未實現損益",
            "PL_Pct": "報酬率",
            "WeightPct": "權重",
            "SignalScore": "訊號分數",
            "Signal": "策略訊號",
            "StrategyMode": "策略模式",
            "StopLoss": "停損價",
            "TakeProfit1": "目標1",
            "TakeProfit2": "目標2",
            "TrendStop": "趨勢停損",
            "DistanceToStopPct": "距停損%",
            "DistanceToTP1Pct": "距目標1%",
            "DistanceToTrendStopPct": "距趨勢停損%",
            "Divergence": "量價背離",
        })
        styled = holdings_df.style.applymap(color_pl, subset=["未實現損益", "報酬率"]).format({
            "平均成本": "${:,.2f}",
            "FIFO 成本": "${:,.2f}",
            "最新價": "${:,.2f}",
            "市值": "${:,.2f}",
            "未實現損益": "${:,.2f}",
            "報酬率": "{:.2f}%",
            "權重": "{:.2f}%",
            "訊號分數": "{:.2f}",
            "停損價": "${:,.2f}",
            "目標1": "${:,.2f}",
            "目標2": "${:,.2f}",
            "趨勢停損": "${:,.2f}",
            "距停損%": "{:.2f}%",
            "距目標1%": "{:.2f}%",
            "距趨勢停損%": "{:.2f}%",
        })
        st.dataframe(styled, use_container_width=True)
    else:
        st.info("尚無持倉資料。")


# ===============================
# Tab 2 Trade Center
# ===============================
with tab2:
    st.subheader("📝 新增交易")
    sp500_list = get_sp500_tickers()

    with st.form("trade_form", clear_on_submit=False):
        c1, c2, c3, c4 = st.columns(4)

        with c1:
            manual_input = st.checkbox("手動輸入股票代碼", value=True)
            if manual_input:
                ticker_input = normalize_ticker(
                    st.text_input("股票代碼", value=st.session_state.get("trade_ticker", "NVDA"))
                )
            else:
                default_ticker = st.session_state.get("trade_ticker", "NVDA")
                default_index = 0
                for i, item in enumerate(sp500_list):
                    if item.startswith(default_ticker + " -"):
                        default_index = i
                        break
                selected_stock = st.selectbox("搜尋股票", options=sp500_list, index=default_index)
                ticker_input = normalize_ticker(selected_stock.split(" - ")[0]) if selected_stock else ""

        with c2:
            default_trade_type = st.session_state.get("trade_type", "BUY")
            trade_type = st.selectbox("交易類型", ["BUY", "SELL"], index=0 if default_trade_type == "BUY" else 1)
            trade_date = st.date_input("交易日期", value=date.today())
            trade_time = st.time_input("交易時間", value=dtime(9, 30))

        with c3:
            trade_price = st.number_input(
                "成交價格",
                min_value=0.01,
                value=float(st.session_state.get("trade_price", 100.00)),
                format="%.2f"
            )
            trade_shares = st.number_input(
                "股數",
                min_value=0.0001,
                value=float(st.session_state.get("trade_shares", 1.0)),
                format="%.4f"
            )

        with c4:
            trade_fee = st.number_input(
                "手續費",
                min_value=0.0,
                value=float(st.session_state.get("trade_fee", DEFAULT_COMMISSION)),
                format="%.4f"
            )
            auto_slippage = trade_price * trade_shares * DEFAULT_SLIPPAGE_PCT
            st.metric("自動滑價成本 (0.1%)", f"${auto_slippage:,.4f}")

        note = st.text_input("備註", value=st.session_state.get("trade_note", ""))
        order_id = st.text_input("Order ID（可空白）", value="")
        trade_dt = datetime.combine(trade_date, trade_time)

        preview = build_trade_preview(
            trades_df=trades_df,
            initial_capital=initial_capital,
            ticker=ticker_input,
            trade_type=trade_type,
            price=trade_price,
            shares=trade_shares,
            fee=trade_fee,
        )

        st.markdown("#### 🔍 交易影響預覽")
        p1, p2, p3, p4 = st.columns(4)
        p1.metric("目前現金", f"${preview['current_cash']:,.2f}")
        p2.metric("交易後現金", f"${preview['after_cash']:,.2f}")
        p3.metric("目前權重", f"{preview['current_weight_pct']:.2f}%")
        p4.metric("交易後權重", f"{preview['after_weight_pct']:.2f}%")
        q1, q2, q3 = st.columns(3)
        q1.metric("毛金額", f"${preview['gross_total']:,.2f}")
        q2.metric("手續費", f"${preview['fee']:,.2f}")
        q3.metric("滑價成本(0.1%)", f"${preview['slippage']:,.2f}")

        if preview["exceed_max_weight"]:
            st.warning(f"⚠️ 交易後持倉權重將超過上限 {MAX_POSITION_WEIGHT*100:.1f}%")
        if preview["sell_exceeds_position"]:
            st.error("❌ 賣出股數超過目前持股")

        submitted = st.form_submit_button("☁️ 同步到雲端")

        if submitted:
            ok, msg = save_trade(
                trade_dt=trade_dt,
                ticker=ticker_input,
                trade_type=trade_type,
                price=trade_price,
                shares=trade_shares,
                note=note,
                fee=trade_fee,
                order_id=order_id,
            )
            if ok:
                st.success(msg)
                st.session_state["trade_ticker"] = ticker_input
                st.session_state["trade_type"] = trade_type
                st.session_state["trade_price"] = trade_price
                st.session_state["trade_shares"] = trade_shares
                st.session_state["trade_note"] = note
                st.session_state["trade_fee"] = trade_fee
                st.rerun()
            else:
                st.error(msg)

    st.divider()
    st.subheader("📚 交易紀錄")
    if not trades_df.empty:
        show_df = trades_df.copy()
        show_df["TradeDateTime"] = show_df["TradeDateTime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        show_df = show_df.rename(columns={
            "TradeDateTime": "交易時間",
            "CreatedAt": "建立時間",
            "Ticker": "代碼",
            "Type": "類型",
            "Price": "價格",
            "Shares": "股數",
            "GrossTotal": "毛金額",
            "Fee": "手續費",
            "Slippage": "滑價",
            "NetTotal": "淨金額",
            "Note": "備註",
            "OrderID": "OrderID",
        })
        st.dataframe(show_df.sort_values("交易時間", ascending=False), use_container_width=True)
    else:
        st.info("尚無交易資料。")

# ===============================
# Tab 3 Strategy Center
# ===============================
with tab3:
    st.subheader("🎯 策略決策中心")

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
            market_regime=market_regime,
        )

        left, right = st.columns([3, 7])

        with left:
            st.subheader(f"🛠️ {analyze_ticker}")
            st.markdown(
                f'<div class="price-box">現價: <span style="font-size: 1.8rem;">${details["close"]:.2f}</span></div>',
                unsafe_allow_html=True
            )

            st.write(f"**策略分數：** `{score:.1f}`")
            st.write(f"**策略模式：** `{details['strategy_mode']}`")
            st.write(f"**目前權重：** `{details['current_weight']*100:.2f}%`")
            st.write(f"**持有股數：** `{held_shares:.4f}`")
            st.write(f"**RSI：** `{details['rsi']:.1f}`")
            st.write(f"**ATR：** `{details['atr']:.2f}`")
            st.write(f"**市場狀態：** `{display_market_regime(details['market_regime'])}`")
            st.write(f"**量價背離：** `{display_divergence(details['divergence'])}`")
            st.write(f"**流動性過濾：** `{'通過' if details['liquid_ok'] else '不通過'}`")
            st.write(f"**財報風險封鎖：** `{'是' if details['earnings_blocked'] else '否'}`")
            st.write(f"**20日平均成交額：** `${details['dollar_volume20']:,.0f}`")

            if recent_buy:
                st.info("⏳ 近期已有買入紀錄")
            if recent_sell:
                st.info("⏳ 近期已有賣出紀錄")

            quick_trade_type = "BUY"
            quick_trade_qty = max(1, int(details["suggested_buy_qty"])) if details["suggested_buy_qty"] >= 1 else 1
            quick_trade_price = float(details["target_buy_price"] or details["close"])

            if action == "BUY_NOW" and not recent_buy:
                st.success("🔥 建議：可執行買入")
                st.markdown(f"- 建議股數：`{details['suggested_buy_qty']}`")
                st.markdown(f"- 建議進場價：`${(details['target_buy_price'] or details['close']):.2f}`")
            elif action == "BUY_PULLBACK":
                st.warning("🟡 建議：等待回檔掛單")
                st.markdown(f"- 回檔目標：`${(details['target_buy_price'] or details['close']):.2f}`")
                st.markdown(f"- 預估股數：`{details['suggested_buy_qty']}`")
            elif action == "SELL_PARTIAL" and not recent_sell and held_shares > 0:
                st.warning("🟠 建議：部分減碼")
                st.markdown(f"- 建議減碼股數：`{details['suggested_sell_qty']}`")
                st.markdown(f"- 建議減碼價：`${(details['target_sell_price'] or details['close']):.2f}`")
                quick_trade_type = "SELL"
                quick_trade_qty = max(1, int(details["suggested_sell_qty"]))
                quick_trade_price = float(details["target_sell_price"] or details["close"])
            elif action == "SELL_EXIT" and not recent_sell and held_shares > 0:
                st.error("⚠️ 建議：全部或大部分出場")
                st.markdown(f"- 建議出場股數：`{details['suggested_sell_qty']}`")
                st.markdown(f"- 建議出場價：`${(details['target_sell_price'] or details['close']):.2f}`")
                quick_trade_type = "SELL"
                quick_trade_qty = max(1, int(details["suggested_sell_qty"]))
                quick_trade_price = float(details["target_sell_price"] or details["close"])
            elif action == "SELL_READY":
                st.warning("🟠 接近減碼區，可準備賣出")
            else:
                st.info("⚖️ 觀望")

            st.markdown(f"- 停損價：`${details['stop_loss']:.2f}`")
            st.markdown(f"- 趨勢停損：`${details['trend_stop']:.2f}`")
            st.markdown(f"- 目標 1：`${details['take_profit_1']:.2f}`")
            st.markdown(f"- 目標 2：`${details['take_profit_2']:.2f}`")
            st.markdown(f"- 回檔進場：`${details['pullback_entry']:.2f}`")
            st.markdown(f"- 突破進場：`${details['breakout_entry']:.2f}`")

            st.divider()
            st.markdown("#### 🧠 因子拆解")
            factor_df = pd.DataFrame({
                "Factor": ["Trend", "Momentum", "Pullback", "Volume", "Regime", "Risk"],
                "Score": [
                    details["trend_score"],
                    details["momentum_score"],
                    details["pullback_score"],
                    details["volume_score"],
                    details["regime_score"],
                    details["risk_score"],
                ],
            })
            factor_fig = go.Figure()
            factor_fig.add_trace(go.Bar(
                x=factor_df["Factor"],
                y=factor_df["Score"],
                marker_color=["#00CC96", "#19D3F3", "#AB63FA", "#FFA15A", "#636EFA", "#EF553B"]
            ))
            factor_fig.update_layout(template="plotly_dark", height=280, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(factor_fig, use_container_width=True)

            st.markdown("#### 📌 訊號依據")
            for r in details["reasons"]:
                st.markdown(f"- {r}")

            st.divider()
            if st.button("帶入交易中心表單", key=f"quick_fill_{analyze_ticker}_{action}"):
                st.session_state["trade_ticker"] = analyze_ticker
                st.session_state["trade_type"] = quick_trade_type
                st.session_state["trade_price"] = round(quick_trade_price, 2)
                st.session_state["trade_shares"] = float(quick_trade_qty)
                st.session_state["trade_note"] = f"策略快速下單 | 分數={score:.1f} | {action} | {details['strategy_mode']}"
                st.success("已帶入交易中心表單，請切換到 Tab 2 確認送出。")

            if st.button("加入 Watchlist", key=f"add_watch_{analyze_ticker}"):
                ok, msg = save_watchlist(analyze_ticker, enabled=True, category="Manual", note="From Strategy Center")
                if ok:
                    st.success(msg)
                else:
                    st.warning(msg)

        with right:
            plot_df = hist.tail(140)
            fig = make_subplots(
                rows=4, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.04,
                row_heights=[0.52, 0.14, 0.17, 0.17]
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
            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["TrailingStop"], name="Trend Stop"), row=1, col=1)

            # 交易點標示
            if not trades_df.empty:
                tdf = trades_df[trades_df["Ticker"] == analyze_ticker].copy()
                if not tdf.empty:
                    buys = tdf[tdf["Type"] == "BUY"]
                    sells = tdf[tdf["Type"] == "SELL"]
                    if not buys.empty:
                        fig.add_trace(go.Scatter(
                            x=buys["TradeDateTime"],
                            y=buys["Price"],
                            mode="markers",
                            marker=dict(size=10, color="lime", symbol="triangle-up"),
                            name="BUY"
                        ), row=1, col=1)
                    if not sells.empty:
                        fig.add_trace(go.Scatter(
                            x=sells["TradeDateTime"],
                            y=sells["Price"],
                            mode="markers",
                            marker=dict(size=10, color="red", symbol="triangle-down"),
                            name="SELL"
                        ), row=1, col=1)

            # target lines
            if details["target_buy_price"]:
                fig.add_hline(y=details["target_buy_price"], line_dash="dot", line_color="green", row=1, col=1)
            if details["target_sell_price"]:
                fig.add_hline(y=details["target_sell_price"], line_dash="dot", line_color="orange", row=1, col=1)
            fig.add_hline(y=details["stop_loss"], line_dash="dash", line_color="red", row=1, col=1)
            fig.add_hline(y=details["take_profit_1"], line_dash="dash", line_color="cyan", row=1, col=1)

            fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["RSI"], name="RSI"), row=2, col=1)
            fig.add_hline(y=70, line_dash="dot", line_color="red", row=2, col=1)
            fig.add_hline(y=30, line_dash="dot", line_color="green", row=2, col=1)

            fig.add_trace(go.Bar(x=plot_df.index, y=plot_df["MACD_Hist"], name="MACD Hist"), row=3, col=1)
            fig.add_trace(go.Bar(x=plot_df.index, y=plot_df["Volume"], name="Volume"), row=4, col=1)

            fig.update_layout(
                template="plotly_dark",
                height=900,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_rangeslider_visible=False
            )
            st.plotly_chart(fig, use_container_width=True)

# ===============================
# Tab 4 Watchlist
# ===============================
with tab4:
    st.subheader("👀 Watchlist 管理")

    col1, col2 = st.columns([4, 6])

    with col1:
        st.markdown("#### ➕ 新增 Watchlist")
        add_mode = st.radio("新增方式", ["手動輸入", "從 S&P 500 選擇"], horizontal=True)

        if add_mode == "手動輸入":
            new_watch_ticker = normalize_ticker(st.text_input("Ticker", value="AAPL"))
        else:
            selected_watch = st.selectbox("選擇股票", options=get_sp500_tickers(), key="watch_sp500_select")
            new_watch_ticker = normalize_ticker(selected_watch.split(" - ")[0]) if selected_watch else ""

        watch_category = st.text_input("分類", value="General")
        watch_note = st.text_input("備註", value="")

        if st.button("加入 Watchlist"):
            ok, msg = save_watchlist(
                ticker=new_watch_ticker,
                enabled=True,
                category=watch_category,
                note=watch_note,
            )
            if ok:
                st.success(msg)
                st.info("已寫入 Watchlist，請稍後手動重新整理。")
            else:
                st.error(msg)

    with col2:
        st.markdown("#### 📋 Watchlist 清單")
        if not watchlist_df.empty:
            st.dataframe(watchlist_df, use_container_width=True)

            st.markdown("#### 🛠️ 管理 Watchlist")
            manage_ticker = st.selectbox(
                "選擇要管理的 Ticker",
                options=watchlist_df["Ticker"].tolist(),
                key="manage_watchlist_ticker"
            )

            a1, a2 = st.columns(2)

            with a1:
                if st.button("停用 / 啟用切換"):
                    current_row = watchlist_df[watchlist_df["Ticker"] == manage_ticker].iloc[0]
                    current_enabled = bool(current_row["Enabled"])
                    ok, msg = set_watchlist_enabled(manage_ticker, not current_enabled)
                    if ok:
                        st.success(msg)
                        st.info("請稍後手動重新整理頁面。")
                    else:
                        st.error(msg)

            with a2:
                if st.button("刪除 Watchlist Ticker"):
                    ok, msg = delete_watchlist_ticker(manage_ticker)
                    if ok:
                        st.success(msg)
                        st.info("請稍後手動重新整理頁面。")
                    else:
                        st.error(msg)
        else:
            st.info("目前 Watchlist 為空。")

    st.divider()
    st.markdown("#### 🔍 Watchlist 即時分析")

    if not watchlist_df.empty:
        enabled_watch = watchlist_df[watchlist_df["Enabled"]]["Ticker"].tolist()
        if enabled_watch:
            selected_watch_ticker = st.selectbox("選擇 Watchlist 標的", options=enabled_watch)
            w_hist = get_unified_analysis(selected_watch_ticker)

            if w_hist is not None and not w_hist.empty:
                held_shares = 0.0
                current_mkt_value = 0.0
                for p in portfolio:
                    if p["Ticker"] == selected_watch_ticker:
                        held_shares = p["Shares"]
                        current_mkt_value = p["MarketValue"]
                        break

                w_score, w_action, w_details, w_note = evaluate_strategy(
                    ticker=selected_watch_ticker,
                    hist=w_hist,
                    held_shares=held_shares,
                    current_mkt_value=current_mkt_value,
                    total_assets=total_assets,
                    cash=cash,
                    market_regime=market_regime,
                )

                ww1, ww2, ww3, ww4 = st.columns(4)
                ww1.metric("訊號", w_action)
                ww2.metric("分數", f"{w_score:.2f}")
                ww3.metric("現價", f"${w_details['close']:.2f}")
                ww4.metric("建議股數", int(w_details["suggested_buy_qty"]))

                st.write("**依據：**", w_note)
            else:
                st.warning("無法取得該標的資料。")
        else:
            st.info("目前沒有啟用中的 Watchlist 標的。")

# ===============================
# Tab 5 Monitor
# ===============================
with tab5:
    st.subheader("⚙️ 系統監控")

    st.write(f"- 交易筆數：{len(trades_df)}")
    st.write(f"- 持股數量：{len(portfolio)}")
    st.write(f"- Watchlist 數量：{len(watchlist_df)}")
    st.write(f"- 市場狀態：{market_regime['regime']}")
    st.write(f"- 現金比例：{(cash / total_assets * 100):.2f}%" if total_assets > 0 else "- 現金比例：N/A")

    if scan_result:
        st.markdown("#### 🤖 最近一次掃描統計")
        sm = scan_result["metrics"]
        s1, s2, s3, s4, s5, s6 = st.columns(6)
        s1.metric("Universe", sm["universe_count"])
        s2.metric("已發送", sm["sent_count"])
        s3.metric("休市阻擋", sm["blocked_count"])
        s4.metric("取價失敗", sm["fetch_failed"])
        s5.metric("去重阻擋", sm["dedup_blocked"])
        s6.metric("耗時(秒)", sm["scan_seconds"])

    with st.expander("提醒紀錄"):
        if not alerts_df.empty:
            st.dataframe(alerts_df.sort_values("DateTime", ascending=False), use_container_width=True)
        else:
            st.info("目前沒有提醒紀錄。")

    with st.expander("NAV 歷史"):
        if not history_df.empty:
            st.dataframe(history_df.sort_values("Date", ascending=False), use_container_width=True)
        else:
            st.info("目前沒有 NAV 歷史。")

    with st.expander("除錯 - 交易資料"):
        st.dataframe(trades_df, use_container_width=True)

    with st.expander("除錯 - 持股資料"):
        if portfolio:
            st.dataframe(pd.DataFrame(portfolio), use_container_width=True)
        else:
            st.info("無持股資料")

"""
量化投資組合 Pro  ·  v3.0
Mobile-first · Bloomberg Terminal aesthetic · Dark precision UI
"""
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from core import (
    DEFAULT_COMMISSION, DEFAULT_INITIAL_CAPITAL, DEFAULT_SLIPPAGE_PCT,
    build_portfolio, build_trade_preview, calculate_performance_metrics,
    calc_portfolio_heat, clear_market_cache, color_pl, display_market_regime,
    enrich_portfolio_with_weight_and_risk, evaluate_strategy,
    get_market_regime, get_market_session, get_unified_analysis,
    load_alerts, load_history, load_signals, load_trades, load_watchlist,
    maybe_log_daily_history, normalize_ticker, run_auto_scanner,
    save_trade, save_watchlist, send_telegram_msg,
    delete_watchlist_ticker, set_watchlist_enabled,
    run_us_semi_scanner, format_us_semi_tg_messages, send_us_semi_tg,
    US_SEMI_UNIVERSE, US_SEMI_SCORE_STRONG, US_SEMI_SCORE_BUY, US_SEMI_SCORE_WATCH,
)

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="量化 Pro",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# Global CSS — Bloomberg Terminal × iOS precision
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=DM+Sans:wght@300;400;500;600;700&display=swap');

/* ── Reset & base ───────────────────────────────────────────────────────── */
:root {
  --bg:         #07080D;
  --surface:    #0F1118;
  --surface2:   #161923;
  --border:     rgba(255,255,255,0.07);
  --border2:    rgba(255,255,255,0.12);
  --text:       #E8EAF0;
  --muted:      #636B80;
  --cyan:       #00D4FF;
  --green:      #00E5A0;
  --red:        #FF3366;
  --gold:       #FFB800;
  --purple:     #9B6DFF;
  --mono:       'JetBrains Mono', monospace;
  --sans:       'DM Sans', sans-serif;
}

html, body, [data-testid="stAppViewContainer"] {
  background: var(--bg) !important;
  color: var(--text);
  font-family: var(--sans);
}

/* hide default streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stSidebarNav"] { display: none; }

/* Scrollbar */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 4px; }

/* ── Typography ─────────────────────────────────────────────────────────── */
h1,h2,h3,h4 { font-family: var(--sans); letter-spacing: -0.02em; color: var(--text); }
.mono { font-family: var(--mono); }

/* ── Streamlit overrides ────────────────────────────────────────────────── */
[data-testid="stMetricValue"] {
  font-family: var(--mono) !important;
  font-size: 1.35rem !important;
  font-weight: 700 !important;
  color: var(--text) !important;
  letter-spacing: -0.02em;
}
[data-testid="stMetricLabel"] {
  font-size: 0.72rem !important;
  color: var(--muted) !important;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-family: var(--sans) !important;
}
[data-testid="stMetricDelta"] { font-size: 0.78rem !important; font-family: var(--mono) !important; }

.stMetric {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: 12px !important;
  padding: 14px 16px !important;
}

/* Tab bar */
[data-baseweb="tab-list"] {
  background: var(--surface) !important;
  border-radius: 12px !important;
  padding: 4px !important;
  gap: 2px !important;
  border: 1px solid var(--border) !important;
}
[data-baseweb="tab"] {
  background: transparent !important;
  color: var(--muted) !important;
  border-radius: 8px !important;
  font-size: 0.78rem !important;
  font-weight: 600 !important;
  font-family: var(--sans) !important;
  padding: 6px 12px !important;
  transition: all 0.2s;
}
[aria-selected="true"][data-baseweb="tab"] {
  background: var(--cyan) !important;
  color: #000 !important;
}

/* Buttons */
.stButton > button {
  background: var(--surface2) !important;
  border: 1px solid var(--border2) !important;
  color: var(--text) !important;
  border-radius: 10px !important;
  font-family: var(--sans) !important;
  font-weight: 600 !important;
  font-size: 0.85rem !important;
  transition: all 0.2s;
}
.stButton > button:hover {
  border-color: var(--cyan) !important;
  color: var(--cyan) !important;
}

/* Inputs */
.stTextInput > div > div > input,
.stNumberInput > div > div > input,
.stSelectbox > div > div {
  background: var(--surface2) !important;
  border: 1px solid var(--border2) !important;
  border-radius: 10px !important;
  color: var(--text) !important;
  font-family: var(--mono) !important;
  font-size: 0.9rem !important;
}

/* Expanders */
[data-testid="stExpander"] {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: 12px !important;
}

/* ── Custom components ──────────────────────────────────────────────────── */

/* Top header bar */
.qp-header {
  display: flex; align-items: center; justify-content: space-between;
  padding: 12px 0 20px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 20px;
}
.qp-logo {
  font-family: var(--mono);
  font-size: 1.1rem; font-weight: 700;
  color: var(--cyan);
  letter-spacing: -0.02em;
}
.qp-logo span { color: var(--muted); font-weight: 400; }

/* Regime + session badges */
.badge {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 0.72rem; font-weight: 700;
  font-family: var(--sans);
  letter-spacing: 0.04em;
  text-transform: uppercase;
}
.badge-on  { background: rgba(0,229,160,0.15); color: var(--green); border: 1px solid rgba(0,229,160,0.3); }
.badge-off { background: rgba(255,51,102,0.15); color: var(--red);   border: 1px solid rgba(255,51,102,0.3); }
.badge-neu { background: rgba(255,184,0,0.12);  color: var(--gold);  border: 1px solid rgba(255,184,0,0.25); }
.badge-session { background: rgba(0,212,255,0.1); color: var(--cyan); border: 1px solid rgba(0,212,255,0.25); }
.badge-closed  { background: rgba(99,107,128,0.15); color: var(--muted); border: 1px solid var(--border2); }

/* Portfolio card */
.pc {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 16px;
  padding: 16px;
  margin-bottom: 10px;
  position: relative;
  overflow: hidden;
  transition: border-color 0.2s;
}
.pc:hover { border-color: var(--border2); }
.pc-accent {
  position: absolute; left: 0; top: 0; bottom: 0; width: 3px;
  border-radius: 16px 0 0 16px;
}
.pc-header {
  display: flex; justify-content: space-between; align-items: flex-start;
  margin-bottom: 10px;
}
.pc-ticker {
  font-family: var(--mono); font-size: 1.05rem; font-weight: 700;
  color: var(--text); letter-spacing: -0.01em;
}
.pc-meta { font-size: 0.7rem; color: var(--muted); font-family: var(--sans); }
.pc-signal {
  font-size: 0.65rem; font-weight: 700; font-family: var(--sans);
  padding: 3px 8px; border-radius: 999px; text-transform: uppercase;
  letter-spacing: 0.06em;
}
.sig-buy  { background: rgba(0,229,160,0.15); color: var(--green); border: 1px solid rgba(0,229,160,0.35); }
.sig-sell { background: rgba(255,51,102,0.15); color: var(--red);   border: 1px solid rgba(255,51,102,0.35); }
.sig-add  { background: rgba(0,212,255,0.12); color: var(--cyan);  border: 1px solid rgba(0,212,255,0.3); }
.sig-part { background: rgba(255,184,0,0.12); color: var(--gold);  border: 1px solid rgba(255,184,0,0.3); }
.sig-watch{ background: rgba(99,107,128,0.15); color: var(--muted); border: 1px solid var(--border2); }

.pc-pl-positive { color: var(--green); font-family: var(--mono); font-weight: 700; }
.pc-pl-negative { color: var(--red);   font-family: var(--mono); font-weight: 700; }
.pc-pl-zero     { color: var(--muted); font-family: var(--mono); font-weight: 700; }

.pc-grid {
  display: grid; grid-template-columns: 1fr 1fr;
  gap: 6px 16px; margin-top: 8px;
}
.pc-kv { display: flex; flex-direction: column; }
.pc-kv-label { font-size: 0.65rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; font-family: var(--sans); }
.pc-kv-value { font-size: 0.88rem; font-family: var(--mono); color: var(--text); font-weight: 600; }

/* Weight bar */
.wbar-bg { background: var(--surface2); border-radius: 999px; height: 4px; margin-top: 10px; }
.wbar-fill { height: 4px; border-radius: 999px; transition: width 0.4s; }

/* Action strip */
.action-strip {
  margin-top: 12px; padding: 10px 12px;
  background: var(--surface2); border-radius: 10px;
  font-size: 0.8rem; color: var(--text);
  border-left: 3px solid var(--cyan);
  font-family: var(--sans);
}

/* Scanner card */
.sc-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 14px; padding: 14px 16px; margin-bottom: 8px;
  display: flex; align-items: center; gap: 14px;
}
.sc-rank {
  font-family: var(--mono); font-size: 0.75rem; color: var(--muted);
  min-width: 24px; text-align: center;
}
.sc-ticker {
  font-family: var(--mono); font-size: 1rem; font-weight: 700; color: var(--text);
}
.sc-score {
  font-family: var(--mono); font-size: 0.85rem; color: var(--cyan); font-weight: 600;
}
.sc-reason { font-size: 0.75rem; color: var(--muted); margin-top: 2px; font-family: var(--sans); }

/* Score bar */
.sbar { background: var(--surface2); border-radius: 999px; height: 3px; flex: 1; }
.sbar-fill { height: 3px; border-radius: 999px; background: linear-gradient(90deg, var(--cyan), var(--green)); }

/* Perf stat grid */
.pstat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.pstat {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 12px; padding: 14px;
}
.pstat-label { font-size: 0.65rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; font-family: var(--sans); }
.pstat-value { font-family: var(--mono); font-size: 1.2rem; font-weight: 700; margin-top: 4px; }

/* Divider */
.qdiv { border: none; border-top: 1px solid var(--border); margin: 18px 0; }

/* Section heading */
.qsec { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.1em; color: var(--muted); font-family: var(--sans); font-weight: 700; margin: 18px 0 10px; }

/* VIX meter */
.vix-bar { display: flex; align-items: center; gap: 8px; margin-top: 4px; }
.vix-label { font-family: var(--mono); font-size: 0.8rem; color: var(--muted); min-width: 36px; }
.vix-track { flex: 1; background: var(--surface2); border-radius: 999px; height: 6px; }
.vix-fill { height: 6px; border-radius: 999px; }

/* Mobile breakpoints */
@media (max-width: 600px) {
  [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
  .pc { padding: 14px; }
  .pc-ticker { font-size: 0.95rem; }
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def signal_badge(signal: str) -> str:
    signal = str(signal).upper()
    if "BUY_NOW"    in signal: return f'<span class="pc-signal sig-buy">▲ 買進</span>'
    if "BUY_ADD"    in signal: return f'<span class="pc-signal sig-add">＋ 加碼</span>'
    if "SELL_EXIT"  in signal: return f'<span class="pc-signal sig-sell">▼ 出場</span>'
    if "SELL_PART"  in signal: return f'<span class="pc-signal sig-part">◑ 減碼</span>'
    return f'<span class="pc-signal sig-watch">— 觀望</span>'

def regime_badge(regime: str, vix) -> str:
    label = display_market_regime(regime)
    vix_str = f" VIX {vix:.1f}" if vix else ""
    if regime == "RISK_ON":  return f'<span class="badge badge-on">🟢 {label}{vix_str}</span>'
    if regime == "RISK_OFF": return f'<span class="badge badge-off">🔴 {label}{vix_str}</span>'
    return f'<span class="badge badge-neu">🟡 {label}{vix_str}</span>'

def session_badge(session: str) -> str:
    labels = {"REGULAR": ("正常盤", "badge-session"), "PREMARKET": ("盤前", "badge-session"),
              "AFTERMARKET": ("盤後", "badge-session"), "CLOSED": ("休市", "badge-closed")}
    lbl, cls = labels.get(session, (session, "badge-closed"))
    return f'<span class="badge {cls}">{lbl}</span>'

def pl_class(val: float) -> str:
    if val > 0: return "pc-pl-positive"
    if val < 0: return "pc-pl-negative"
    return "pc-pl-zero"

def fmt_dollar(v: float) -> str:
    return f"${v:,.2f}"

def fmt_pct(v: float) -> str:
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}%"

def weight_bar(pct: float, max_pct: float = 25) -> str:
    fill = min(100, pct / max_pct * 100)
    colour = "#FF3366" if pct > max_pct * 0.9 else "#00D4FF"
    return f'''<div class="wbar-bg"><div class="wbar-fill" style="width:{fill:.0f}%;background:{colour}"></div></div>'''

def score_bar(score: float, max_score: float = 8) -> str:
    fill = min(100, score / max_score * 100)
    return f'''<div class="sbar"><div class="sbar-fill" style="width:{fill:.0f}%"></div></div>'''

def action_tip(p: dict) -> str:
    sig = p.get("Signal", "WATCH")
    if "BUY_NOW" in sig:
        return f'🛒 建議買進 <b>{p.get("SuggestedBuyQty", 0)} 股</b>，參考價 <b>${p["LastPrice"]:.2f}</b>'
    if "BUY_ADD" in sig:
        return f'➕ 建議加碼 <b>{p.get("SuggestedBuyQty", 0)} 股</b>，動能仍強，控制部位權重'
    if "SELL_EXIT" in sig:
        return f'⚠️ 跌破停損，建議出場 <b>{p.get("SuggestedSellQty", 0)} 股</b>，執行紀律'
    if "SELL_PARTIAL" in sig:
        return f'💰 到達 TP1，建議獲利了結 <b>{p.get("SuggestedSellQty", 0)} 股</b> (約 50%)'
    return '👁 無強烈訊號，持續觀察。'


# ─────────────────────────────────────────────────────────────────────────────
# Data init
# ─────────────────────────────────────────────────────────────────────────────
if "init_capital" not in st.session_state:
    st.session_state.init_capital = float(DEFAULT_INITIAL_CAPITAL)

initial_capital = st.session_state.init_capital

try:
    trades_df    = load_trades()
    watchlist_df = load_watchlist()
    history_df   = load_history()
    alerts_df    = load_alerts()
except Exception:
    trades_df = watchlist_df = history_df = alerts_df = pd.DataFrame()

portfolio_raw, cash, total_realized_pl = build_portfolio(trades_df, initial_capital)
market_value         = sum(x["MarketValue"] for x in portfolio_raw)
total_assets         = cash + market_value
total_unrealized_pl  = sum(x["Unrealized"]   for x in portfolio_raw)
total_pl             = total_assets - initial_capital

market_regime = get_market_regime()
portfolio     = enrich_portfolio_with_weight_and_risk(portfolio_raw, total_assets, cash, market_regime) if portfolio_raw else []
heat_info     = calc_portfolio_heat(portfolio, total_assets)
perf          = calculate_performance_metrics(history_df)

session = get_market_session()

# ─────────────────────────────────────────────────────────────────────────────
# Top header
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="qp-header">
  <div class="qp-logo">QUANT<span>PRO</span></div>
  <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;">
    {regime_badge(market_regime.get('regime','UNKNOWN'), market_regime.get('vix'))}
    {session_badge(session)}
  </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# NAV summary bar (4 metrics)
# ─────────────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("NAV 總資產", fmt_dollar(total_assets))
delta_pct = f"{(total_pl / initial_capital * 100):+.2f}%"
c2.metric("總損益", fmt_dollar(total_pl), delta_pct)
c3.metric("現金", fmt_dollar(cash))
c4.metric("Portfolio Heat", f"{heat_info['heat_pct']:.1f}%")

st.markdown("<hr class='qdiv'>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Main tabs
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 持倉", "🔍 掃描器", "📈 策略", "📝 交易", "⚡ 績效", "🔬 美股半導體"
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Portfolio Dashboard
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    if not portfolio:
        st.info("目前無持倉。在「交易」頁籤新增第一筆買進紀錄。")
    else:
        # Pie chart in expander to save space
        with st.expander("資產配置圖", expanded=False):
            labels = [p["Ticker"] for p in portfolio]
            values = [p["MarketValue"] for p in portfolio]
            colours = ["#00D4FF", "#00E5A0", "#FFB800", "#9B6DFF", "#FF3366",
                       "#FF8C42", "#4CC9F0", "#7BFF6A", "#F72585"][:len(labels)]
            pie = go.Figure(go.Pie(
                labels=labels, values=values, hole=0.6,
                marker=dict(colors=colours, line=dict(color="#07080D", width=2)),
                textfont=dict(family="JetBrains Mono", size=11),
            ))
            pie.update_layout(
                template="plotly_dark", height=260,
                margin=dict(l=0, r=0, t=10, b=0),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=True,
                legend=dict(font=dict(family="DM Sans", size=11), orientation="h", yanchor="bottom", y=-0.2),
            )
            st.plotly_chart(pie, use_container_width=True, config={"displayModeBar": False})

        st.markdown('<div class="qsec">持倉明細</div>', unsafe_allow_html=True)

        for p in sorted(portfolio, key=lambda x: x["MarketValue"], reverse=True):
            sig   = p.get("Signal", "WATCH")
            pl    = p.get("Unrealized", 0)
            pl_p  = p.get("PL_Pct", 0)

            sl_val = p.get("StopLoss");  sl_str = f"${sl_val:.2f}" if sl_val and pd.notna(sl_val) else "—"
            tp_val = p.get("TakeProfit1"); tp_str = f"${tp_val:.2f}" if tp_val and pd.notna(tp_val) else "—"
            rs_val = p.get("RS20vsSPY", 0); rs_str = f"{rs_val:+.1f}%" if rs_val else "—"
            sc_val = p.get("SignalScore", 0)

            # Accent colour
            if "BUY_NOW" in sig:  accent = "#00E5A0"
            elif "BUY_ADD" in sig: accent = "#00D4FF"
            elif "SELL" in sig:    accent = "#FF3366"
            else:                  accent = "#636B80"

            bucket_label = "Large" if p.get("Bucket", "LARGE_CAP") == "LARGE_CAP" else "Small"

            st.markdown(f"""
<div class="pc">
  <div class="pc-accent" style="background:{accent}"></div>
  <div class="pc-header">
    <div>
      <div class="pc-ticker">{p['Ticker']} <span style="font-size:0.7rem;color:var(--muted);font-weight:400">{bucket_label}</span></div>
      <div class="pc-meta">{p['Shares']:.4f} 股 · 成本 ${p['AvgCost']:.2f}</div>
    </div>
    <div style="text-align:right">
      {signal_badge(sig)}
      <div style="margin-top:4px;font-family:var(--mono);font-size:0.72rem;color:var(--muted)">分數 {sc_val:.1f}</div>
    </div>
  </div>

  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px;">
    <span style="font-family:var(--mono);font-size:1.15rem;font-weight:700;color:var(--text)">${p['LastPrice']:.2f}</span>
    <span class="{pl_class(pl)}" style="font-size:0.88rem">{fmt_pct(pl_p)}&nbsp;&nbsp;{fmt_dollar(pl)}</span>
  </div>

  {weight_bar(p.get('WeightPct', 0))}

  <div class="pc-grid">
    <div class="pc-kv"><span class="pc-kv-label">停損</span><span class="pc-kv-value">{sl_str}</span></div>
    <div class="pc-kv"><span class="pc-kv-label">目標1</span><span class="pc-kv-value">{tp_str}</span></div>
    <div class="pc-kv"><span class="pc-kv-label">RS vs SPY</span><span class="pc-kv-value" style="color:{'var(--green)' if (rs_val or 0)>0 else 'var(--red)'}">{rs_str}</span></div>
    <div class="pc-kv"><span class="pc-kv-label">部位權重</span><span class="pc-kv-value">{p.get('WeightPct',0):.1f}%</span></div>
  </div>

  <div class="action-strip">{action_tip(p)}</div>
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Auto Scanner
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown('<div class="qsec">自動掃描器</div>', unsafe_allow_html=True)

    col_a, col_b = st.columns([3, 1])
    with col_a:
        add_ticker = st.text_input("新增至 Watchlist", placeholder="AAPL, TSLA …", label_visibility="collapsed")
    with col_b:
        if st.button("＋ 加入", use_container_width=True):
            for t in [x.strip() for x in add_ticker.split(",") if x.strip()]:
                ok, msg = save_watchlist(normalize_ticker(t))
                st.toast(msg, icon="✅" if ok else "❌")

    if st.button("🔄 執行完整掃描", use_container_width=True):
        with st.spinner("掃描中 …"):
            result = run_auto_scanner(
                portfolio=portfolio, trades_df=trades_df,
                cash=cash, total_assets=total_assets,
                market_regime=market_regime, watchlist_df=watchlist_df,
            )
        st.session_state["scan_result"] = result
        st.rerun()

    result = st.session_state.get("scan_result")
    if result:
        m = result["metrics"]
        ma, mb, mc = st.columns(3)
        ma.metric("掃描標的", m.get("universe_count", 0))
        mb.metric("買進訊號", m.get("buy_signals", 0))
        mc.metric("出場訊號", m.get("sell_signals", 0))

        # Exit alerts (priority)
        exits = result.get("top_exits", [])
        if exits:
            st.markdown('<div class="qsec" style="color:var(--red)">⚠️ 出場訊號</div>', unsafe_allow_html=True)
            for c in exits:
                det = c["details"]
                st.markdown(f"""
<div class="sc-card" style="border-color:rgba(255,51,102,0.35)">
  <div class="sc-rank">🔴</div>
  <div style="flex:1">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <div class="sc-ticker">{c['ticker']}</div>
      <div style="font-family:var(--mono);font-size:0.85rem;color:var(--red)">${det['close']:.2f}</div>
    </div>
    <div class="sc-reason">{c['note']}</div>
  </div>
</div>""", unsafe_allow_html=True)

        # Buy signals ranked
        buys = result.get("top_buys", [])
        if buys:
            st.markdown('<div class="qsec">🟢 買進機會 (依強度排序)</div>', unsafe_allow_html=True)
            for i, c in enumerate(buys, 1):
                det = c["details"]
                rs_c = "var(--green)" if det.get("rs20_vs_spy", 0) > 0 else "var(--red)"
                st.markdown(f"""
<div class="sc-card">
  <div class="sc-rank">{i}</div>
  <div style="flex:1">
    <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;">
      <div>
        <div class="sc-ticker">{c['ticker']}</div>
        <div class="sc-reason">{c['note'][:80]}</div>
      </div>
      <div style="text-align:right;min-width:72px;">
        <div class="sc-score">{c['score']:.1f}</div>
        <div style="font-size:0.68rem;color:{rs_c};font-family:var(--mono)">RS {det.get('rs20_vs_spy',0):+.1f}%</div>
      </div>
    </div>
    {score_bar(c['score'])}
  </div>
</div>""", unsafe_allow_html=True)
    else:
        # Watchlist table when no scan run
        if not watchlist_df.empty:
            st.markdown('<div class="qsec">Watchlist</div>', unsafe_allow_html=True)
            wl = watchlist_df[["Ticker", "Enabled", "Category", "Note"]].copy()
            wl["Enabled"] = wl["Enabled"].map({True: "✅", False: "⏸"})
            st.dataframe(wl, use_container_width=True, hide_index=True)
        else:
            st.info("Watchlist 空白。輸入代碼後按「＋ 加入」。")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Strategy Analysis (single stock deep-dive)
# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown('<div class="qsec">單股策略分析</div>', unsafe_allow_html=True)

    analyze_ticker = normalize_ticker(
        st.text_input("股票代碼", value="NVDA", label_visibility="collapsed", placeholder="輸入代碼如 NVDA …")
    )

    if st.button("🚀 執行分析", use_container_width=True):
        with st.spinner(f"分析 {analyze_ticker} …"):
            hist = get_unified_analysis(analyze_ticker)

        if hist is None:
            st.error(f"無法取得 {analyze_ticker} 資料，請確認代碼。")
        else:
            held = next((p["Shares"] for p in portfolio if p["Ticker"] == analyze_ticker), 0)
            mkt_val = next((p["MarketValue"] for p in portfolio if p["Ticker"] == analyze_ticker), 0)
            score, action, det, note = evaluate_strategy(
                analyze_ticker, hist, held, mkt_val,
                total_assets, cash, market_regime,
                heat_info["heat_pct"], portfolio,
            )
            last = hist.iloc[-1]

            # ── Score + action pill ──────────────────────────────────────────
            score_colour = "#00E5A0" if score >= 5 else "#00D4FF" if score >= 3 else "#FFB800" if score >= 1.5 else "#FF3366"
            action_map = {
                "BUY_NOW":      ("🛒 立即買進", "var(--green)"),
                "BUY_ADD":      ("➕ 加碼買進", "var(--cyan)"),
                "SELL_EXIT":    ("⚠️ 立即出場", "var(--red)"),
                "SELL_PARTIAL": ("◑ 部分獲利", "var(--gold)"),
                "WATCH":        ("👁 觀望", "var(--muted)"),
            }
            act_label, act_colour = action_map.get(action, ("— 觀望", "var(--muted)"))

            st.markdown(f"""
<div class="pc" style="margin-bottom:14px;">
  <div style="display:flex;justify-content:space-between;align-items:center;">
    <div>
      <div style="font-family:var(--mono);font-size:1.5rem;font-weight:700;color:{score_colour}">{score:.1f}</div>
      <div style="font-size:0.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.08em">動能分數</div>
    </div>
    <div style="text-align:right;">
      <div style="font-size:1.1rem;font-weight:700;color:{act_colour}">{act_label}</div>
      <div style="font-size:0.72rem;color:var(--muted);margin-top:2px">{det['strategy_mode']}</div>
    </div>
  </div>
  {score_bar(score)}
</div>
""", unsafe_allow_html=True)

            # ── Key stats ──────────────────────────────────────────────────
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("現價",   f"${det['close']:.2f}")
            k2.metric("RSI",   f"{det['rsi']:.1f}")
            k3.metric("ATR",   f"${det['atr']:.2f}")
            k4.metric("ADX",   f"{det.get('adx', 0):.1f}")

            k5, k6, k7, k8 = st.columns(4)
            k5.metric("停損",   f"${det['stop_loss']:.2f}")
            k6.metric("目標 1", f"${det['take_profit_1']:.2f}")
            k7.metric("目標 2", f"${det['take_profit_2']:.2f}")
            k8.metric("RS vs SPY", f"{det.get('rs20_vs_spy', 0):+.1f}%")

            # ── Action recommendation ──────────────────────────────────────
            if "BUY" in action:
                st.success(f"🛒 建議 **{action}** — 參考數量：**{det['suggested_buy_qty']} 股** @ ${det['close']:.2f}")
            elif "SELL" in action:
                st.warning(f"📉 建議 **{action}** — 參考數量：**{det['suggested_sell_qty']} 股** @ ${det['close']:.2f}")
            else:
                st.info(f"👁 {note if note != 'No Signal' else '目前無強烈訊號，建議觀望'}")

            if det.get("is_squeeze"):
                st.warning("📦 **Bollinger Squeeze 偵測到** — 波動率極低，市場即將表態，密切注意突破方向。")

            if det.get("earnings_blocked"):
                st.error("⚠️ **財報封鎖期** — 距離財報 ≤2 天，策略建議暫停操作。")

            # ── Technical chart ────────────────────────────────────────────
            st.markdown('<div class="qsec">技術圖表 (近 90 日)</div>', unsafe_allow_html=True)
            plot_df = hist.tail(90)
            fig = make_subplots(
                rows=3, cols=1, shared_xaxes=True,
                row_heights=[0.55, 0.25, 0.20],
                vertical_spacing=0.04,
                subplot_titles=("", "MACD", "RSI"),
            )

            # Candlestick
            fig.add_trace(go.Candlestick(
                x=plot_df.index, open=plot_df["Open"], high=plot_df["High"],
                low=plot_df["Low"], close=plot_df["Close"], name="K線",
                increasing_fillcolor="#00E5A0", increasing_line_color="#00E5A0",
                decreasing_fillcolor="#FF3366", decreasing_line_color="#FF3366",
            ), row=1, col=1)

            for sma_col, sma_col_name, sma_colour in [
                ("SMA20", "SMA20", "#FFB800"), ("SMA50", "SMA50", "#00D4FF"), ("SMA200", "SMA200", "#9B6DFF")
            ]:
                fig.add_trace(go.Scatter(
                    x=plot_df.index, y=plot_df[sma_col], name=sma_col_name,
                    line=dict(color=sma_colour, width=1.2, dash="dot"), opacity=0.7,
                ), row=1, col=1)

            # Bollinger Bands
            fig.add_trace(go.Scatter(
                x=plot_df.index, y=plot_df["BB_upper"],
                line=dict(color="rgba(255,255,255,0.15)", width=1),
                showlegend=False,
            ), row=1, col=1)
            fig.add_trace(go.Scatter(
                x=plot_df.index, y=plot_df["BB_lower"],
                fill="tonexty", fillcolor="rgba(0,212,255,0.04)",
                line=dict(color="rgba(255,255,255,0.15)", width=1),
                showlegend=False,
            ), row=1, col=1)

            # MACD
            hist_colours = ["#00E5A0" if v > 0 else "#FF3366" for v in plot_df["MACD_Hist"]]
            fig.add_trace(go.Bar(
                x=plot_df.index, y=plot_df["MACD_Hist"],
                marker_color=hist_colours, name="MACD Hist",
            ), row=2, col=1)
            fig.add_trace(go.Scatter(
                x=plot_df.index, y=plot_df["MACD"],
                line=dict(color="#00D4FF", width=1.2), name="MACD",
            ), row=2, col=1)

            # RSI
            fig.add_trace(go.Scatter(
                x=plot_df.index, y=plot_df["RSI"],
                line=dict(color="#9B6DFF", width=1.4), name="RSI",
            ), row=3, col=1)
            for lvl, col in [(70, "rgba(255,51,102,0.3)"), (50, "rgba(255,255,255,0.1)"), (30, "rgba(0,229,160,0.3)")]:
                fig.add_hline(y=lvl, line_color=col, line_dash="dot", row=3, col=1)

            fig.update_layout(
                template="plotly_dark", height=480,
                margin=dict(l=0, r=0, t=20, b=0),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                xaxis_rangeslider_visible=False, showlegend=False,
                font=dict(family="JetBrains Mono", size=10),
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Trade Terminal
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="qsec">快速下單終端</div>', unsafe_allow_html=True)

    with st.form("trade_form_v3", clear_on_submit=False):
        col_tk, col_dir = st.columns([3, 2])
        tk_input = col_tk.text_input("股票代碼", value="NVDA", placeholder="NVDA")
        dir_input = col_dir.selectbox("方向", ["BUY", "SELL"])

        col_pr, col_sh = st.columns(2)
        pr_input = col_pr.number_input("成交價", value=100.0, min_value=0.01, format="%.2f")
        sh_input = col_sh.number_input("股數", value=1.0, min_value=0.0001, format="%.4f")
        
        fee_input  = st.number_input("手續費 (USD)", value=float(DEFAULT_COMMISSION), min_value=0.0, format="%.2f")
        note_input = st.text_input("備註", value="", placeholder="選填")

        # Real-time preview
        gross = pr_input * sh_input
        slip  = gross * DEFAULT_SLIPPAGE_PCT
        net   = gross + fee_input + slip if dir_input == "BUY" else gross - fee_input - slip
        after = cash - net if dir_input == "BUY" else cash + net
        wt    = (pr_input * sh_input / total_assets * 100) if total_assets > 0 else 0

        st.markdown(f"""
<div class="pc" style="margin:10px 0;">
  <div class="pc-grid">
    <div class="pc-kv"><span class="pc-kv-label">毛額</span><span class="pc-kv-value">{fmt_dollar(gross)}</span></div>
    <div class="pc-kv"><span class="pc-kv-label">滑點</span><span class="pc-kv-value">{fmt_dollar(slip)}</span></div>
    <div class="pc-kv"><span class="pc-kv-label">淨額</span><span class="pc-kv-value">{fmt_dollar(net)}</span></div>
    <div class="pc-kv"><span class="pc-kv-label">交後現金</span><span class="pc-kv-value" style="color:{'var(--red)' if after < 0 else 'var(--text)'}">{fmt_dollar(after)}</span></div>
    <div class="pc-kv"><span class="pc-kv-label">佔資產%</span><span class="pc-kv-value">{wt:.1f}%</span></div>
  </div>
</div>
""", unsafe_allow_html=True)

        submitted = st.form_submit_button("🚀 確認寫入 Google Sheets", use_container_width=True)
        if submitted:
            ok, msg = save_trade(datetime.now(), normalize_ticker(tk_input), dir_input, pr_input, sh_input, note_input, fee_input)
            if ok:
                st.success(f"✅ {msg}")
            else:
                st.error(f"❌ {msg}")

    # Recent trades table
    if not trades_df.empty:
        st.markdown('<div class="qsec">最近交易紀錄</div>', unsafe_allow_html=True)
        # 相容 V1 舊資料（NetTotal 等於 GrossTotal）與 V2 新資料
        _rcols = ["TradeDateTime", "Ticker", "Type", "Price", "Shares", "GrossTotal", "Fee", "NetTotal"]
        _rcols_exist = [c for c in _rcols if c in trades_df.columns]
        recent = trades_df.tail(20)[_rcols_exist].copy()
        recent["TradeDateTime"] = pd.to_datetime(recent["TradeDateTime"], errors="coerce").dt.strftime("%m/%d %H:%M")
        _rename = {
            "TradeDateTime": "時間", "Ticker": "代碼", "Type": "方向",
            "Price": "價格", "Shares": "股數",
            "GrossTotal": "毛額", "Fee": "手續費", "NetTotal": "淨額",
        }
        recent = recent.rename(columns={k: v for k, v in _rename.items() if k in recent.columns})
        st.dataframe(recent[::-1], use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Performance & Analytics
# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown('<div class="qsec">績效分析</div>', unsafe_allow_html=True)

    # Perf stats
    max_dd   = perf.get("max_drawdown_pct")
    sharpe   = perf.get("sharpe")
    win_rate = perf.get("win_rate")
    tot_ret  = perf.get("total_return_pct")

    st.markdown(f"""
<div class="pstat-grid">
  <div class="pstat">
    <div class="pstat-label">累積報酬</div>
    <div class="pstat-value" style="color:{'var(--green)' if (tot_ret or 0) >= 0 else 'var(--red)'}">
      {f'{tot_ret:+.2f}%' if tot_ret is not None else '—'}
    </div>
  </div>
  <div class="pstat">
    <div class="pstat-label">最大回撤</div>
    <div class="pstat-value" style="color:var(--red)">
      {f'{max_dd:.2f}%' if max_dd is not None else '—'}
    </div>
  </div>
  <div class="pstat">
    <div class="pstat-label">Sharpe Ratio</div>
    <div class="pstat-value" style="color:{'var(--green)' if (sharpe or 0) >= 1 else 'var(--gold)' if (sharpe or 0) >= 0.5 else 'var(--muted)'}">
      {f'{sharpe:.2f}' if sharpe is not None else '—'}
    </div>
  </div>
  <div class="pstat">
    <div class="pstat-label">勝率</div>
    <div class="pstat-value" style="color:{'var(--green)' if (win_rate or 0) >= 55 else 'var(--muted)'}">
      {f'{win_rate:.1f}%' if win_rate is not None else '—'}
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

    # NAV chart with SPY benchmark
    if not history_df.empty and len(history_df) >= 2:
        st.markdown('<div class="qsec">NAV vs SPY 曲線</div>', unsafe_allow_html=True)
        hdf = history_df.copy()
        base_nav = hdf["TotalAssets"].iloc[0]
        hdf["NAV_idx"] = hdf["TotalAssets"] / base_nav * 100

        fig_nav = go.Figure()
        fig_nav.add_trace(go.Scatter(
            x=hdf["Date"], y=hdf["NAV_idx"], name="Portfolio NAV",
            line=dict(color="#00D4FF", width=2),
            fill="tozeroy", fillcolor="rgba(0,212,255,0.06)",
        ))

        # SPY rebased benchmark
        if "BenchmarkSPY" in hdf.columns and hdf["BenchmarkSPY"].notna().any():
            base_spy = hdf["BenchmarkSPY"].iloc[0]
            hdf["SPY_idx"] = hdf["BenchmarkSPY"] / base_spy * 100
            fig_nav.add_trace(go.Scatter(
                x=hdf["Date"], y=hdf["SPY_idx"], name="SPY",
                line=dict(color="#9B6DFF", width=1.5, dash="dot"),
            ))

        # Drawdown background
        nav_series = hdf["TotalAssets"]
        drawdown_pct = (nav_series / nav_series.cummax() - 1) * 100
        fig_nav.add_trace(go.Scatter(
            x=hdf["Date"], y=drawdown_pct, name="Drawdown",
            line=dict(color="#FF3366", width=0),
            fill="tozeroy", fillcolor="rgba(255,51,102,0.07)",
            yaxis="y2",
        ))

        fig_nav.update_layout(
            template="plotly_dark", height=320,
            margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="JetBrains Mono", size=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.01, font=dict(size=10)),
            yaxis=dict(title="Index (100=Start)", gridcolor="rgba(255,255,255,0.05)"),
            yaxis2=dict(title="DD %", overlaying="y", side="right", gridcolor="rgba(0,0,0,0)", range=[-50, 5]),
            xaxis=dict(gridcolor="rgba(255,255,255,0.04)"),
        )
        st.plotly_chart(fig_nav, use_container_width=True, config={"displayModeBar": False})

        # Drawdown stats
        if "DrawdownPct" in hdf.columns:
            st.markdown('<div class="qsec">每日報酬分佈</div>', unsafe_allow_html=True)
            if "DailyReturnPct" in hdf.columns:
                rets = pd.to_numeric(hdf["DailyReturnPct"], errors="coerce").dropna()
                fig_hist = go.Figure(go.Histogram(
                    x=rets, nbinsx=40,
                    marker_color="#00D4FF", opacity=0.75,
                ))
                fig_hist.update_layout(
                    template="plotly_dark", height=200,
                    margin=dict(l=0, r=0, t=10, b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(title="Daily Return %", gridcolor="rgba(255,255,255,0.05)"),
                    yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                    bargap=0.05,
                )
                st.plotly_chart(fig_hist, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("歷史資料不足，累積每日 NAV 後圖表將自動顯示。")

    # Sidebar-style settings panel at bottom of perf tab
    st.markdown("<hr class='qdiv'>", unsafe_allow_html=True)
    st.markdown('<div class="qsec">快速設定</div>', unsafe_allow_html=True)
    new_cap = st.number_input("初始資金 (USD)", value=st.session_state.init_capital,
                               min_value=1000.0, step=1000.0, format="%.0f")
    if new_cap != st.session_state.init_capital:
        st.session_state.init_capital = new_cap
        st.rerun()

    col_ref, col_sync = st.columns(2)
    if col_ref.button("🔄 刷新快取", use_container_width=True):
        clear_market_cache()
        st.rerun()
    if col_sync.button("📡 記錄今日 NAV", use_container_width=True):
        ok, msg = maybe_log_daily_history(
            total_assets=total_assets, cash=cash, market_value=market_value,
            realized_pl=total_realized_pl, unrealized_pl=total_unrealized_pl,
        )
        st.toast(msg, icon="✅" if ok else "ℹ️")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 — 美股半導體宇宙掃描器
# ═══════════════════════════════════════════════════════════════════════════════
with tab6:
    # ── Session state init ──────────────────────────────────────────────────
    if "semi_result"    not in st.session_state: st.session_state.semi_result    = None
    if "semi_scan_prog" not in st.session_state: st.session_state.semi_scan_prog = ""

    # ── Header status ───────────────────────────────────────────────────────
    st.markdown('<div class="qsec">🔬 美股半導體宇宙掃描器</div>', unsafe_allow_html=True)

    # Quick info bar
    _semi_n = len(set(US_SEMI_UNIVERSE))
    st.markdown(f"""
<div class="pc" style="margin-bottom:14px;">
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;">
    <div style="text-align:center;">
      <div style="font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em">掃描宇宙</div>
      <div style="font-family:var(--mono);font-size:1.3rem;font-weight:700;color:var(--cyan)">{_semi_n} 檔</div>
      <div style="font-size:.65rem;color:var(--muted)">SOX + AI 基礎建設</div>
    </div>
    <div style="text-align:center;">
      <div style="font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em">強力買進門檻</div>
      <div style="font-family:var(--mono);font-size:1.3rem;font-weight:700;color:var(--red)">≥ {US_SEMI_SCORE_STRONG:.1f}</div>
      <div style="font-size:.65rem;color:var(--muted)">/ 10 分</div>
    </div>
    <div style="text-align:center;">
      <div style="font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em">自動執行</div>
      <div style="font-family:var(--mono);font-size:1.1rem;font-weight:700;color:var(--gold)">09:00</div>
      <div style="font-size:.65rem;color:var(--muted)">台灣時間（GitHub Actions）</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

    # ── Strategy explanation ─────────────────────────────────────────────────
    with st.expander("📐 策略過濾器說明", expanded=False):
        st.markdown("""
**8 層多因子評分（滿分 ~10 分）**

| 層 | 因子 | 分值 | 說明 |
|---|---|---|---|
| A | SMA 多頭排列 | +2.5 | SMA20 > SMA50 > SMA200 全線向上 |
| B | MACD 翻多加速 | +0.8 | 柱狀圖翻正且持續放大 |
| B | RSI 健康帶 50–72 | +0.8 | 動能健康，非超買 |
| B | ADX ≥ 18 | +0.8 | 趨勢強度足夠 |
| C | RS vs SPY > +2% | +1.0 | 強於大盤（20日相對強度）|
| C | 優於 SOX 指數 | +0.8 | 領先半導體板塊 |
| D | OBV 法人吸籌 | +0.5 | OBV 斜率向上 + 站上 SMA20 |
| D | 放量上漲日 | +0.5 | 成交量 > 均量 1.5x 且收漲 |
| E | BB 壓縮放量突破 | +1.5 | 低波動率壓縮後量價齊揚 |
| F | 接近 52W 年高 | +0.8 | 距年高 10% 內（強勢領頭羊）|
| G | SOX 趨勢乘數 | ×1.0/0.85/0.65 | BULL/NEUTRAL/BEAR 調整整體得分 |
| H | 流動性門檻 | 必要條件 | 日均成交值 > $20M，非財報封鎖期 |

**訊號門檻**：🔴 強力買進 ≥ {strong} ｜ 🟢 積極買進 ≥ {buy} ｜ 🟡 留意候補 ≥ {watch}
""".format(strong=US_SEMI_SCORE_STRONG, buy=US_SEMI_SCORE_BUY, watch=US_SEMI_SCORE_WATCH))

    st.markdown("<hr class='qdiv'>", unsafe_allow_html=True)

    # ── Scan controls ────────────────────────────────────────────────────────
    st.markdown('<div class="qsec">手動掃描設定</div>', unsafe_allow_html=True)

    _ctl1, _ctl2 = st.columns(2)
    _workers  = _ctl1.slider("平行 Worker 數", min_value=3, max_value=20, value=10,
                              help="越多越快，但易觸發 yfinance 限流")
    _dry_run  = _ctl2.checkbox("Dry Run（掃描但不發 Telegram）", value=True)

    _extra_input = st.text_input(
        "額外加入掃描的標的（逗號分隔）",
        placeholder="SMCI, PLTR, ARM …",
        help="除預設宇宙外額外加入，例如你的持倉中有半導體股",
    )
    _extra_tickers = [t.strip().upper() for t in _extra_input.split(",") if t.strip()]

    if st.button("🚀 立即執行美股半導體掃描", use_container_width=True, type="primary"):
        import concurrent.futures as _cf_ui
        from core import _us_semi_score_one, _get_sox_regime, normalize_ticker as _nt
        import math as _math

        _universe = list(dict.fromkeys(
            [_nt(t) for t in US_SEMI_UNIVERSE] + [_nt(t) for t in _extra_tickers]
        ))

        _prog_bar  = st.progress(0, text="取得 SOX 指數狀態 …")
        _prog_text = st.empty()

        # SOX regime first
        _sox = _get_sox_regime()
        _sox_trend = _sox.get("trend", "NEUTRAL")
        _sox_emoji = {"BULL": "🐂", "NEUTRAL": "➡️", "BEAR": "🐻"}
        _prog_bar.progress(0, text=f"SOX {_sox_emoji.get(_sox_trend)} {_sox_trend}  "
                                    f"vs SPY {_sox.get('rs_vs_spy', 0):+.1f}% ｜ 開始掃描 {len(_universe)} 檔 …")

        _results, _done = [], [0]
        _total = len(_universe)

        def _scan_ticker(tk):
            r = _us_semi_score_one(tk, _sox)
            _done[0] += 1
            pct = _done[0] / _total
            _prog_bar.progress(pct, text=f"掃描 {_done[0]}/{_total} ({pct*100:.0f}%) — "
                                          f"發現 {len(_results)} 個入選標的")
            return r

        with _cf_ui.ThreadPoolExecutor(max_workers=_workers) as _ex:
            for _r in _cf_ui.as_completed({_ex.submit(_scan_ticker, tk): tk for tk in _universe}):
                _res = _r.result()
                if _res: _results.append(_res)

        _prog_bar.empty()
        _prog_text.empty()

        _results.sort(key=lambda x: -x["score"])
        _strong = [r for r in _results if r["signal"] == "STRONG_BUY"]
        _buys   = [r for r in _results if r["signal"] == "BUY"]
        _watch  = [r for r in _results if r["signal"] == "WATCH"]

        st.session_state.semi_result = {
            "strong_buy": _strong, "buy": _buys, "watch": _watch,
            "all_results": _results, "sox_regime": _sox,
            "total_scanned": _total, "total_hits": len(_results),
            "scan_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

        # Telegram
        if not _dry_run and _results:
            from core import format_us_semi_tg_messages, send_us_semi_tg
            _scan_res_for_tg = dict(st.session_state.semi_result)
            _scan_res_for_tg["scan_date"] = datetime.now().strftime("%Y-%m-%d")
            _msgs = format_us_semi_tg_messages(_scan_res_for_tg)
            _ok   = send_us_semi_tg(_msgs)
            st.toast("✅ Telegram 推播成功！" if _ok else "❌ Telegram 推播失敗", icon="📡")
        elif _dry_run:
            st.toast("Dry Run 模式：未發送 Telegram", icon="🔕")

    # ── Results display ──────────────────────────────────────────────────────
    _res_data = st.session_state.get("semi_result")
    if _res_data:
        _sox_r     = _res_data["sox_regime"]
        _all       = _res_data["all_results"]
        _strong    = _res_data["strong_buy"]
        _buys      = _res_data["buy"]
        _watches   = _res_data["watch"]
        _scan_time = _res_data.get("scan_date", "—")
        _sox_trend = _sox_r.get("trend", "NEUTRAL")
        _SOX_BADGE = {"BULL": ("badge-up", "🐂 多頭"), "NEUTRAL": ("badge-flat", "➡️ 中性"), "BEAR": ("badge-down", "🐻 空頭")}
        _sox_cls, _sox_lbl = _SOX_BADGE.get(_sox_trend, ("badge-flat", "—"))

        # Summary bar
        st.markdown(f"""
<div class="pc" style="margin-bottom:12px;">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <div>
      <div style="font-size:.65rem;color:var(--muted);text-transform:uppercase">掃描時間</div>
      <div style="font-family:var(--mono);font-size:.9rem;font-weight:700">{_scan_time}</div>
    </div>
    <span class="badge {_sox_cls}">SOX {_sox_lbl}  vs SPY {_sox_r.get('rs_vs_spy', 0):+.1f}%</span>
    <div style="text-align:right;">
      <div style="font-size:.65rem;color:var(--muted)">掃描 {_res_data['total_scanned']} 檔</div>
      <div style="font-size:.8rem;font-family:var(--mono)">
        <span style="color:var(--red)">🔴{len(_strong)}</span>&nbsp;
        <span style="color:var(--green)">🟢{len(_buys)}</span>&nbsp;
        <span style="color:var(--gold)">🟡{len(_watches)}</span>
      </div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

        if not _all:
            st.info("本次掃描無符合條件標的，市場環境可能偏弱。")
        else:
            # Render cards
            _SIG_LABEL = {
                "STRONG_BUY": ("var(--red)",   "🔴 強力買進"),
                "BUY":        ("var(--green)",  "🟢 積極買進"),
                "WATCH":      ("var(--gold)",   "🟡 留意候補"),
            }
            _rank_emoji = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

            for _i, _r in enumerate(_all[:20]):
                _sig_color, _sig_label = _SIG_LABEL.get(_r["signal"], ("var(--muted)", "—"))
                _stars   = "⭐" * min(5, max(1, round(_r["score"] / 1.5)))
                _reasons = "、".join(_r["reasons"][:3]) if _r["reasons"] else "—"
                _rank    = _rank_emoji[_i] if _i < len(_rank_emoji) else f"{_i+1}."
                _score_w = min(100, _r["score"] / 8 * 100)

                st.markdown(f"""
<div class="pc">
  <div class="pc-accent" style="background:{_sig_color}"></div>
  <div class="pc-header">
    <div>
      <div class="pc-ticker">{_rank} {_r['ticker']} <span style="font-size:.7rem;color:var(--muted);font-weight:400">{_stars}</span></div>
      <div class="pc-meta">{_r['reasons'][0] if _r['reasons'] else ''}</div>
    </div>
    <div style="text-align:right">
      <span class="pc-signal" style="background:rgba(255,255,255,.05);color:{_sig_color};border:1px solid {_sig_color}40">{_sig_label}</span>
      <div style="font-family:var(--mono);font-size:.72rem;color:var(--muted);margin-top:4px">分數 {_r['score']:.1f}/10</div>
    </div>
  </div>
  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px;">
    <span style="font-family:var(--mono);font-size:1.15rem;font-weight:700;color:var(--text)">${_r['close']}</span>
    <span style="font-family:var(--mono);font-size:.8rem;color:var(--cyan)">RS vs SPY {_r['rs20_vs_spy']:+.1f}%</span>
  </div>
  <div class="wbar-bg"><div class="wbar-fill" style="width:{_score_w:.0f}%;background:{_sig_color}"></div></div>
  <div class="pc-grid" style="margin-top:10px;">
    <div class="pc-kv"><span class="pc-kv-label">停損</span><span class="pc-kv-value" style="color:var(--red)">${_r['stop_loss']}</span></div>
    <div class="pc-kv"><span class="pc-kv-label">目標 TP1</span><span class="pc-kv-value" style="color:var(--green)">${_r['tp1']}</span></div>
    <div class="pc-kv"><span class="pc-kv-label">RSI / ADX</span><span class="pc-kv-value">{_r['rsi']:.0f} / {_r['adx']:.0f}</span></div>
    <div class="pc-kv"><span class="pc-kv-label">日均成交</span><span class="pc-kv-value">${_r['dv20_m']:.0f}M</span></div>
    <div class="pc-kv" style="grid-column:span 2;"><span class="pc-kv-label">因子</span><span class="pc-kv-value" style="font-size:.75rem">{_reasons}</span></div>
  </div>
</div>
""", unsafe_allow_html=True)

            # Full results table
            st.markdown("<hr class='qdiv'>", unsafe_allow_html=True)
            st.markdown('<div class="qsec">完整結果表格</div>', unsafe_allow_html=True)
            _df_show = pd.DataFrame([{
                "代碼": r["ticker"], "訊號": r["signal"], "分數": r["score"],
                "現價": r["close"], "停損": r["stop_loss"],
                "TP1": r["tp1"], "TP2": r["tp2"],
                "RSI": r["rsi"], "ADX": r["adx"],
                "RS%SPY": r["rs20_vs_spy"], "日均量$M": r["dv20_m"],
                "建議股數": r["suggested_qty"],
                "因子": "、".join(r["reasons"][:3]),
            } for r in _all])
            st.dataframe(_df_show, use_container_width=True, hide_index=True)

            # TG preview
            with st.expander(f"📨 Telegram 訊息預覽", expanded=False):
                _tg_res = dict(_res_data)
                _tg_res["scan_date"] = datetime.now().strftime("%Y-%m-%d")
                _tg_msgs = format_us_semi_tg_messages(_tg_res)
                for _mi, _m in enumerate(_tg_msgs, 1):
                    st.caption(f"第 {_mi} 則（{len(_m)} 字元）")
                    st.code(_m, language=None)

            # Clear button
            if st.button("✖ 清除掃描結果", use_container_width=True):
                st.session_state.semi_result = None
                st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="text-align:center;padding:20px 0 8px;font-size:0.65rem;color:var(--muted);font-family:var(--mono)">
  QUANTPRO v3.0 · {datetime.now().strftime('%Y/%m/%d %H:%M')} · 本系統僅供輔助參考，不構成投資建議
</div>
""", unsafe_allow_html=True)

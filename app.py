"""
量化投資組合 Pro  ·  v3.0
Mobile-first · Bloomberg Terminal aesthetic · Dark precision UI
"""
from datetime import datetime
import concurrent.futures as _cf_ui
import html
import textwrap

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

from core import (
    DEFAULT_COMMISSION,
    DEFAULT_INITIAL_CAPITAL,
    DEFAULT_SLIPPAGE_PCT,
    MARKET_CACHE_TTL,
    US_SEMI_SCORE_BUY,
    US_SEMI_SCORE_STRONG,
    US_SEMI_SCORE_WATCH,
    US_SEMI_UNIVERSE,
    CATEGORY_MAX_WEIGHT,
    CORR_LOOKBACK_DAYS,
    _get_sox_regime,
    _us_semi_score_one,
    _annotate_semi_candidate,
    _held_tickers_from_trades,
    apply_entry_risk_gates,
    build_portfolio,
    audit_universe,
    build_trade_preview,
    calc_portfolio_heat,
    calc_category_exposure,
    calc_portfolio_correlation,
    calc_realized_trade_stats,
    calculate_performance_metrics,
    evaluate_signal_outcomes,
    summarize_signal_edge,
    clear_market_cache,
    color_pl,
    delete_watchlist_ticker,
    display_market_regime,
    enrich_portfolio_with_weight_and_risk,
    evaluate_strategy,
    format_us_semi_tg_messages,
    get_market_regime,
    get_market_session,
    get_unified_analysis,
    load_alerts,
    load_history,
    load_signals,
    load_trades,
    load_watchlist,
    maybe_log_daily_history,
    migrate_trades_v1_to_v2,
    normalize_ticker,
    run_auto_scanner,
    run_broad_scanner,
    run_us_semi_scanner,
    save_trade,
    save_watchlist,
    send_telegram_msg,
    send_us_semi_tg,
    set_watchlist_enabled,
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
# Global CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=DM+Sans:wght@300;400;500;600;700&display=swap');

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

#MainMenu, footer, header { visibility: hidden; }
[data-testid="stSidebarNav"] { display: none; }

::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 4px; }

h1,h2,h3,h4 { font-family: var(--sans); letter-spacing: -0.02em; color: var(--text); }
.mono { font-family: var(--mono); }

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
[data-testid="stMetricDelta"] {
  font-size: 0.78rem !important;
  font-family: var(--mono) !important;
}

.stMetric {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: 12px !important;
  padding: 14px 16px !important;
}

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

[data-testid="stExpander"] {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: 12px !important;
}

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
.badge-up   { background: rgba(0,229,160,0.15); color: var(--green); border: 1px solid rgba(0,229,160,0.3); }
.badge-down { background: rgba(255,51,102,0.15); color: var(--red); border: 1px solid rgba(255,51,102,0.3); }
.badge-flat { background: rgba(255,184,0,0.12); color: var(--gold); border: 1px solid rgba(255,184,0,0.25); }

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

.wbar-bg { background: var(--surface2); border-radius: 999px; height: 4px; margin-top: 10px; }
.wbar-fill { height: 4px; border-radius: 999px; transition: width 0.4s; }

.action-strip {
  margin-top: 12px; padding: 10px 12px;
  background: var(--surface2); border-radius: 10px;
  font-size: 0.8rem; color: var(--text);
  border-left: 3px solid var(--cyan);
  font-family: var(--sans);
}

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

.sbar { background: var(--surface2); border-radius: 999px; height: 3px; flex: 1; }
.sbar-fill { height: 3px; border-radius: 999px; background: linear-gradient(90deg, var(--cyan), var(--green)); }

.pstat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.pstat {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 12px; padding: 14px;
}
.pstat-label { font-size: 0.65rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; font-family: var(--sans); }
.pstat-value { font-family: var(--mono); font-size: 1.2rem; font-weight: 700; margin-top: 4px; }

.qdiv { border: none; border-top: 1px solid var(--border); margin: 18px 0; }
.qsec { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.1em; color: var(--muted); font-family: var(--sans); font-weight: 700; margin: 18px 0 10px; }

.ts-wrap {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 10px;
  margin-top: 10px;
}

.ts-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 14px;
  padding: 14px;
}

.ts-label {
  font-size: 0.68rem;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  margin-bottom: 6px;
  font-family: var(--sans);
}

.ts-value {
  font-family: var(--mono);
  font-size: 1.35rem;
  font-weight: 700;
  color: var(--text);
  line-height: 1.1;
}

.ts-sub {
  margin-top: 6px;
  font-size: 0.76rem;
  font-family: var(--sans);
  font-weight: 600;
}

.ts-head {
  display:flex;
  justify-content:space-between;
  align-items:center;
  gap:10px;
  padding:12px 14px;
  border:1px solid var(--border);
  border-radius:14px;
  background: linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01));
  margin-bottom: 10px;
}

.ts-badge {
  display:inline-flex;
  align-items:center;
  gap:6px;
  padding:5px 10px;
  border-radius:999px;
  font-size:0.74rem;
  font-weight:700;
  font-family: var(--sans);
  border:1px solid rgba(255,255,255,0.08);
}

.ts-note {
  margin-top: 10px;
  padding: 10px 12px;
  border-radius: 12px;
  background: var(--surface2);
  border-left: 3px solid var(--cyan);
  font-size: 0.8rem;
  color: var(--text);
  font-family: var(--sans);
}

@media (max-width: 900px) {
  .ts-wrap {
    grid-template-columns: repeat(2, 1fr);
  }
}

@media (max-width: 640px) {
  .ts-wrap {
    grid-template-columns: 1fr;
  }
}

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
    if "BUY_NOW" in signal:
        return '<span class="pc-signal sig-buy">▲ 買進</span>'
    if "BUY_ADD" in signal:
        return '<span class="pc-signal sig-add">＋ 加碼</span>'
    if "SELL_EXIT" in signal:
        return '<span class="pc-signal sig-sell">▼ 出場</span>'
    if "SELL_PART" in signal:
        return '<span class="pc-signal sig-part">◑ 減碼</span>'
    return '<span class="pc-signal sig-watch">— 觀望</span>'


def regime_badge(regime: str, vix) -> str:
    label = display_market_regime(regime)
    vix_str = f" VIX {vix:.1f}" if vix else ""
    if regime == "RISK_ON":
        return f'<span class="badge badge-on">🟢 {label}{vix_str}</span>'
    if regime == "RISK_OFF":
        return f'<span class="badge badge-off">🔴 {label}{vix_str}</span>'
    return f'<span class="badge badge-neu">🟡 {label}{vix_str}</span>'


def session_badge(session: str) -> str:
    labels = {
        "REGULAR": ("正常盤", "badge-session"),
        "PREMARKET": ("盤前", "badge-session"),
        "AFTERMARKET": ("盤後", "badge-session"),
        "CLOSED": ("休市", "badge-closed"),
    }
    lbl, cls = labels.get(session, (session, "badge-closed"))
    return f'<span class="badge {cls}">{lbl}</span>'

def classify_kd_status(k: float, d: float) -> Tuple[str, str]:
    if k >= 80 and d >= 80:
        return "高檔偏熱", "#FF3366"
    if k <= 20 and d <= 20:
        return "低檔區", "#00E5A0"
    if k > d:
        return "短線轉強", "#00E5A0"
    if k < d:
        return "短線偏弱", "#FFB800"
    return "中性", "#636B80"


def classify_rsi_status(rsi: float) -> Tuple[str, str]:
    if rsi >= 70:
        return "偏熱", "#FF3366"
    if rsi >= 60:
        return "偏強", "#00E5A0"
    if rsi >= 45:
        return "中性", "#FFB800"
    if rsi >= 30:
        return "偏弱", "#FF8C42"
    return "超賣區", "#00D4FF"


def classify_volume_ratio_status(vol_ratio: float) -> Tuple[str, str]:
    if vol_ratio >= 1.8:
        return "明顯放量", "#00E5A0"
    if vol_ratio >= 1.2:
        return "溫和放量", "#00D4FF"
    if vol_ratio >= 0.8:
        return "正常量", "#FFB800"
    return "量縮", "#636B80"


def classify_rs_status(rs_val: float) -> Tuple[str, str]:
    if rs_val >= 5:
        return "強於大盤", "#00E5A0"
    if rs_val > 0:
        return "略強於大盤", "#00D4FF"
    if rs_val <= -5:
        return "明顯弱於大盤", "#FF3366"
    return "略弱於大盤", "#FFB800"


def classify_adx_status(adx: float) -> Tuple[str, str]:
    if adx >= 30:
        return "強趨勢", "#00E5A0"
    if adx >= 20:
        return "趨勢形成", "#00D4FF"
    if adx >= 15:
        return "弱趨勢", "#FFB800"
    return "盤整為主", "#636B80"


def get_technical_summary_signal(k: float, d: float, rsi: float, rs_vs_spy: float, adx: float, vol_ratio: float) -> Tuple[str, str, str]:
    score = 0

    if k > d:
        score += 1
    elif k < d:
        score -= 1

    if rsi >= 60:
        score += 1
    elif rsi < 40:
        score -= 1

    if rs_vs_spy > 0:
        score += 1
    elif rs_vs_spy < 0:
        score -= 1

    if adx >= 20:
        score += 1

    if vol_ratio >= 1.2:
        score += 0.5

    if score >= 2.5:
        return "🟢 偏多", "#00E5A0", "多項動能指標同步偏強"
    if score <= -1:
        return "🔴 偏弱", "#FF3366", "短線動能與相對強弱偏弱"
    return "🟡 中性", "#FFB800", "動能中性，等待更明確方向"

def get_ticker_brief_technical_signal(ticker: str) -> Tuple[str, str]:
    hist = get_unified_analysis(ticker)
    if hist is None or hist.empty or len(hist) < 30:
        return "⚪ 未知", "#636B80"

    df = hist.tail(30).copy()

    low_n = df["Low"].rolling(9).min()
    high_n = df["High"].rolling(9).max()
    rsv = (df["Close"] - low_n) / (high_n - low_n + 1e-9) * 100
    df["K"] = rsv.ewm(com=2).mean()
    df["D"] = df["K"].ewm(com=2).mean()

    last = df.iloc[-1]

    k = float(last["K"])
    d = float(last["D"])
    rsi = float(last["RSI"])
    adx = float(last.get("ADX", 0) or 0)
    vol_ratio = float(last["Volume"]) / max(float(last.get("VOL_SMA20", 1) or 1), 1)
    rs_vs_spy = float(last.get("RS20_vs_SPY", 0) or 0)

    label, color, _ = get_technical_summary_signal(
        k=k,
        d=d,
        rsi=rsi,
        rs_vs_spy=rs_vs_spy,
        adx=adx,
        vol_ratio=vol_ratio,
    )
    return label, color

def technical_bias_badge(label: str, color: str) -> str:
    safe_label = html.escape(str(label))
    return (
        f'<span style="display:inline-flex;align-items:center;gap:6px;'
        f'padding:4px 10px;border-radius:999px;font-size:0.68rem;'
        f'font-weight:700;font-family:var(--sans);'
        f'color:{color};background:{color}18;border:1px solid {color}55;">'
        f'{safe_label}</span>'
    )

def render_ticker_technical_summary(ticker: str):
    hist = get_unified_analysis(ticker)
    if hist is None or hist.empty or len(hist) < 30:
        st.info("技術摘要資料不足")
        return

    df = hist.tail(30).copy()

    low_n = df["Low"].rolling(9).min()
    high_n = df["High"].rolling(9).max()
    rsv = (df["Close"] - low_n) / (high_n - low_n + 1e-9) * 100
    df["K"] = rsv.ewm(com=2).mean()
    df["D"] = df["K"].ewm(com=2).mean()

    last = df.iloc[-1]

    close = float(last["Close"])
    k = float(last["K"])
    d = float(last["D"])
    rsi = float(last["RSI"])
    adx = float(last.get("ADX", 0) or 0)
    vol_ratio = float(last["Volume"]) / max(float(last.get("VOL_SMA20", 1) or 1), 1)
    rs_vs_spy = float(last.get("RS20_vs_SPY", 0) or 0)

    kd_txt, kd_color = classify_kd_status(k, d)
    rsi_txt, rsi_color = classify_rsi_status(rsi)
    vol_txt, vol_color = classify_volume_ratio_status(vol_ratio)
    rs_txt, rs_color = classify_rs_status(rs_vs_spy)
    adx_txt, adx_color = classify_adx_status(adx)

    head_label, head_color, head_desc = get_technical_summary_signal(
        k=k,
        d=d,
        rsi=rsi,
        rs_vs_spy=rs_vs_spy,
        adx=adx,
        vol_ratio=vol_ratio,
    )

    summary_tags = []
    summary_tags.append("KD偏多" if k > d else "KD偏弱" if k < d else "KD中性")

    if rsi >= 70:
        summary_tags.append("RSI偏熱")
    elif rsi >= 60:
        summary_tags.append("RSI偏強")
    elif rsi < 40:
        summary_tags.append("RSI偏弱")
    else:
        summary_tags.append("RSI中性")

    if vol_ratio >= 1.5:
        summary_tags.append("量能放大")
    elif vol_ratio < 0.8:
        summary_tags.append("量能偏低")
    else:
        summary_tags.append("量能正常")

    summary_tags.append("強於大盤" if rs_vs_spy > 0 else "弱於大盤")
    summary_tags.append("趨勢明確" if adx >= 20 else "趨勢不明")

    summary_html = f"""
<div class="ts-head">
  <div>
    <div style="font-size:0.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.08em;">Technical Summary</div>
    <div style="font-family:var(--mono);font-size:1.1rem;font-weight:700;color:{head_color};margin-top:4px;">{head_label}</div>
    <div style="font-size:0.78rem;color:var(--muted);margin-top:4px;">{head_desc}</div>
  </div>
  <div class="ts-badge" style="color:{head_color};border-color:{head_color}55;background:{head_color}12;">
    {ticker}
  </div>
</div>

<div class="ts-wrap">
  <div class="ts-card">
    <div class="ts-label">現價</div>
    <div class="ts-value">${close:,.2f}</div>
    <div class="ts-sub" style="color:#9AA4BF;">最新收盤</div>
  </div>

  <div class="ts-card">
    <div class="ts-label">K / D</div>
    <div class="ts-value">{k:.0f} / {d:.0f}</div>
    <div class="ts-sub" style="color:{kd_color};">{kd_txt}</div>
  </div>

  <div class="ts-card">
    <div class="ts-label">RSI</div>
    <div class="ts-value">{rsi:.1f}</div>
    <div class="ts-sub" style="color:{rsi_color};">{rsi_txt}</div>
  </div>

  <div class="ts-card">
    <div class="ts-label">量比</div>
    <div class="ts-value">{vol_ratio:.1f}x</div>
    <div class="ts-sub" style="color:{vol_color};">{vol_txt}</div>
  </div>

  <div class="ts-card">
    <div class="ts-label">RS vs SPY</div>
    <div class="ts-value">{rs_vs_spy:+.1f}%</div>
    <div class="ts-sub" style="color:{rs_color};">{rs_txt}</div>
  </div>

  <div class="ts-card">
    <div class="ts-label">ADX</div>
    <div class="ts-value">{adx:.1f}</div>
    <div class="ts-sub" style="color:{adx_color};">{adx_txt}</div>
  </div>
</div>

<div class="ts-note">
  技術摘要：{" ｜ ".join(summary_tags)}
</div>
"""
    st.markdown(summary_html, unsafe_allow_html=True)

    if k >= 85 and d >= 80 and rsi >= 75:
        st.warning(f"⚠️ 技術面高檔過熱（KD {k:.0f}/{d:.0f} · RSI {rsi:.0f}）")
    elif k >= 80 or rsi >= 70:
        st.info(f"⚠️ 偏高檔區，留意追價風險（KD {k:.0f}/{d:.0f} · RSI {rsi:.0f}）")
    elif k <= 20 and d <= 20 and rsi <= 35:
        st.success(f"✅ 進入低檔區，可觀察是否出現反彈訊號（KD {k:.0f}/{d:.0f} · RSI {rsi:.0f}）")

def pl_class(val: float) -> str:
    if val > 0:
        return "pc-pl-positive"
    if val < 0:
        return "pc-pl-negative"
    return "pc-pl-zero"


def fmt_dollar(v: float) -> str:
    return f"${v:,.2f}"


def fmt_pct(v: float) -> str:
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}%"


def weight_bar(pct: float, max_pct: float = 25) -> str:
    fill = min(100, pct / max_pct * 100)
    colour = "#FF3366" if pct > max_pct * 0.9 else "#00D4FF"
    return f'<div class="wbar-bg"><div class="wbar-fill" style="width:{fill:.0f}%;background:{colour}"></div></div>'


def score_bar(score: float, max_score: float = 8) -> str:
    fill = min(100, score / max_score * 100)
    return f'<div class="sbar"><div class="sbar-fill" style="width:{fill:.0f}%"></div></div>'


def action_tip(p: dict) -> str:
    sig = p.get("Signal", "WATCH")
    mode = p.get("StrategyMode", "")
    qty_sell = p.get("SuggestedSellQty", 0)
    pl = p.get("PL_Pct", 0)
    if "BUY_NOW" in sig:
        return f'🛒 建議買進 <b>{p.get("SuggestedBuyQty", 0)} 股</b>，參考價 <b>${p["LastPrice"]:.2f}</b>'
    if "BUY_ADD" in sig:
        return f'➕ 建議加碼 <b>{p.get("SuggestedBuyQty", 0)} 股</b>，動能仍強，控制部位權重'
    if "SELL_EXIT" in sig:
        if mode == "TRAIL_EXIT":
            if pl >= 0:
                return f'📉 回落觸發移動停利（鎖定 {pl:+.1f}%），建議出場 <b>{qty_sell} 股</b>'
            return f'📉 回落跌破移動停損（獲利已回吐至 {pl:+.1f}%），建議出場 <b>{qty_sell} 股</b>，執行紀律'
        if mode == "BREAKEVEN_EXIT":
            if pl >= -0.5:
                return f'🛡 回落至保本線（{pl:+.1f}%），建議出場 <b>{qty_sell} 股</b>，保護獲利不倒賠'
            return f'🛡 跌破保本線（{pl:+.1f}%），建議出場 <b>{qty_sell} 股</b>，執行紀律'
        if mode == "TREND_EXIT":
            return f'📉 趨勢轉弱（虧損中跌破年線 SMA200），建議出場 <b>{qty_sell} 股</b>，執行紀律'
        return f'⚠️ 跌破停損（{pl:+.1f}%），建議出場 <b>{qty_sell} 股</b>，執行紀律'
    if "SELL_PARTIAL" in sig:
        return f'💰 觸及獲利目標（{pl:+.1f}%），建議分批減碼 <b>{qty_sell} 股</b>落袋，其餘續抱讓移動停損接管'
    if p.get("CategoryCapped"):
        return f'🔒 訊號達標，但「{p.get("Category","該產業")}」曝險已達上限（{p.get("CategoryWeight",0):.0f}%），暫不加碼以控制集中度'
    return '👁 無強烈訊號，持續觀察。'

def render_ticker_technical_chart(ticker: str, days: int = 180, chart_key: str = None):
    hist = get_unified_analysis(ticker)
    if hist is None or hist.empty:
        st.warning(f"無法取得 {ticker} 技術圖資料")
        return

    plot_df = hist.tail(days).copy()
    if plot_df.empty:
        st.warning(f"{ticker} 無足夠歷史資料")
        return

    low_n = plot_df["Low"].rolling(9).min()
    high_n = plot_df["High"].rolling(9).max()
    rsv = (plot_df["Close"] - low_n) / (high_n - low_n + 1e-9) * 100
    plot_df["K"] = rsv.ewm(com=2).mean()
    plot_df["D"] = plot_df["K"].ewm(com=2).mean()

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.46, 0.18, 0.18, 0.18],
        subplot_titles=(f"{ticker} — K線 + BB", "成交量", "KDJ 隨機指標", "MACD"),
    )

    fig.add_trace(go.Candlestick(
        x=plot_df.index,
        open=plot_df["Open"],
        high=plot_df["High"],
        low=plot_df["Low"],
        close=plot_df["Close"],
        name="K線",
        increasing_fillcolor="#00E5A0",
        increasing_line_color="#00E5A0",
        decreasing_fillcolor="#FF3366",
        decreasing_line_color="#FF3366",
    ), row=1, col=1)

    sma_specs = [
        ("SMA20", "#F7B500", "SMA20"),
        ("SMA50", "#1E90FF", "SMA50"),
        ("SMA200", "#9B6DFF", "SMA200"),
    ]
    for col_name, color, label in sma_specs:
        if col_name in plot_df.columns:
            fig.add_trace(go.Scatter(
                x=plot_df.index,
                y=plot_df[col_name],
                mode="lines",
                line=dict(color=color, width=1.5, dash="dot"),
                name=label,
            ), row=1, col=1)

    if "BB_upper" in plot_df.columns and "BB_lower" in plot_df.columns:
        fig.add_trace(go.Scatter(
            x=plot_df.index,
            y=plot_df["BB_upper"],
            mode="lines",
            line=dict(color="rgba(255,255,255,0.18)", width=1),
            name="BB Upper",
            showlegend=False,
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=plot_df.index,
            y=plot_df["BB_lower"],
            mode="lines",
            fill="tonexty",
            fillcolor="rgba(0,212,255,0.05)",
            line=dict(color="rgba(255,255,255,0.18)", width=1),
            name="BB Lower",
            showlegend=False,
        ), row=1, col=1)

    last = plot_df.iloc[-1]
    close = float(last["Close"])
    atr = float(last.get("ATR", 0) or 0)
    stop_loss = max(0.01, close - 2.0 * atr) if atr > 0 else close * 0.93
    tp1 = close + 2.0 * atr if atr > 0 else close * 1.08

    fig.add_hline(y=stop_loss, line_color="#00E5A0", line_dash="dot", row=1, col=1)
    fig.add_hline(y=tp1, line_color="#FF3366", line_dash="dot", row=1, col=1)

    vol_colors = ["#00E5A0" if c >= o else "#FF3366" for c, o in zip(plot_df["Close"], plot_df["Open"])]
    fig.add_trace(go.Bar(
        x=plot_df.index,
        y=plot_df["Volume"],
        marker_color=vol_colors,
        name="Volume",
    ), row=2, col=1)

    if "VOL_SMA20" in plot_df.columns:
        fig.add_trace(go.Scatter(
            x=plot_df.index,
            y=plot_df["VOL_SMA20"],
            mode="lines",
            line=dict(color="#F7B500", width=1.2),
            name="VOL SMA20",
        ), row=2, col=1)

    fig.add_trace(go.Scatter(
        x=plot_df.index,
        y=plot_df["K"],
        mode="lines",
        line=dict(color="#1E90FF", width=1.4),
        name="K",
    ), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=plot_df.index,
        y=plot_df["D"],
        mode="lines",
        line=dict(color="#F7B500", width=1.4),
        name="D",
    ), row=3, col=1)
    fig.add_hline(y=80, line_color="rgba(255,51,102,0.35)", line_dash="dot", row=3, col=1)
    fig.add_hline(y=20, line_color="rgba(0,229,160,0.35)", line_dash="dot", row=3, col=1)

    macd_colors = ["#00E5A0" if v >= 0 else "#FF3366" for v in plot_df["MACD_Hist"]]
    fig.add_trace(go.Bar(
        x=plot_df.index,
        y=plot_df["MACD_Hist"],
        marker_color=macd_colors,
        name="MACD Hist",
    ), row=4, col=1)
    fig.add_trace(go.Scatter(
        x=plot_df.index,
        y=plot_df["MACD"],
        mode="lines",
        line=dict(color="#1E90FF", width=1.4),
        name="MACD",
    ), row=4, col=1)
    fig.add_trace(go.Scatter(
        x=plot_df.index,
        y=plot_df["MACD_Signal"],
        mode="lines",
        line=dict(color="#F7B500", width=1.2),
        name="Signal",
    ), row=4, col=1)

    fig.update_layout(
        template="plotly_dark",
        height=760,
        margin=dict(l=10, r=10, t=40, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="JetBrains Mono", size=10),
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0),
    )

    for r in [1, 2, 3, 4]:
        fig.update_xaxes(gridcolor="rgba(255,255,255,0.04)", row=r, col=1)
        fig.update_yaxes(gridcolor="rgba(255,255,255,0.05)", row=r, col=1)

    st.plotly_chart(
        fig,
        use_container_width=True,
        config={"displayModeBar": False},
        key=chart_key or f"tech_chart_{ticker}_{days}"
    )

def render_ticker_technical_panel(
    ticker: str,
    chart_key_prefix: str = "tech",
    days: int = 180,
    show_title: bool = True,
):
    ticker = normalize_ticker(ticker)

    if show_title:
        st.markdown("#### 技術摘要")
    render_ticker_technical_summary(ticker)

    if show_title:
        st.markdown("#### 完整技術圖")
    render_ticker_technical_chart(
        ticker,
        days=days,
        chart_key=f"{chart_key_prefix}_{ticker}_{days}"
    )

def render_ticker_technical_expander(
    ticker: str,
    expander_label: Optional[str] = None,
    chart_key_prefix: str = "tech",
    days: int = 180,
    expanded: bool = False,
):
    ticker = normalize_ticker(ticker)
    label = expander_label or f"📈 查看 {ticker} 技術分析"

    with st.expander(label, expanded=expanded):
        render_ticker_technical_panel(
            ticker=ticker,
            chart_key_prefix=chart_key_prefix,
            days=days,
            show_title=True,
        )

# ─────────────────────────────────────────────────────────────────────────────
# Data init
# ─────────────────────────────────────────────────────────────────────────────
if "init_capital" not in st.session_state:
    st.session_state.init_capital = float(DEFAULT_INITIAL_CAPITAL)

initial_capital = st.session_state.init_capital

try:
    trades_df = load_trades()
    watchlist_df = load_watchlist()
    history_df = load_history()
    alerts_df = load_alerts()
except Exception:
    trades_df = pd.DataFrame()
    watchlist_df = pd.DataFrame()
    history_df = pd.DataFrame()
    alerts_df = pd.DataFrame()

portfolio_raw, cash, total_realized_pl = build_portfolio(trades_df, initial_capital)
market_value = sum(x["MarketValue"] for x in portfolio_raw)
total_assets = cash + market_value
total_unrealized_pl = sum(x["Unrealized"] for x in portfolio_raw)
total_pl = total_realized_pl + total_unrealized_pl
nav_pl = total_assets - initial_capital

market_regime = get_market_regime()
portfolio = enrich_portfolio_with_weight_and_risk(portfolio_raw, total_assets, cash, market_regime) if portfolio_raw else []
heat_info = calc_portfolio_heat(portfolio, total_assets)
perf = calculate_performance_metrics(history_df)

session = get_market_session()

# 盤中（PREMARKET/REGULAR/AFTERMARKET）自動刷新：每 MARKET_CACHE_TTL 秒重跑，
# 配合行情 TTL 快取自然到期重抓，讓 NAV／報價／訊號不再凍結在進程啟動當下。
if st_autorefresh is not None and session != "CLOSED":
    st_autorefresh(interval=MARKET_CACHE_TTL * 1000, key="market_refresh")

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
# NAV summary
# ─────────────────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("NAV 總資產", fmt_dollar(total_assets))
_pl_pct = (total_pl / initial_capital * 100) if initial_capital > 0 else 0.0
c2.metric("總損益", fmt_dollar(total_pl), f"{_pl_pct:+.2f}%")
c3.metric("已實現", fmt_dollar(total_realized_pl))
c4.metric("未實現", fmt_dollar(total_unrealized_pl))
c5.metric("現金", fmt_dollar(cash))
c6.metric("Portfolio Heat", f"{heat_info['heat_pct']:.1f}%")

st.markdown("<hr class='qdiv'>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 持倉", "🔍 掃描器", "📈 策略", "📝 交易", "⚡ 績效", "🔬 美股半導體", "🧪 訊號驗證"
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Portfolio Dashboard
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    if not portfolio:
        st.info("目前無持倉。在「交易」頁籤新增第一筆買進紀錄。")
    else:
        with st.expander("資產配置圖", expanded=False):
            labels = [p["Ticker"] for p in portfolio]
            values = [p["MarketValue"] for p in portfolio]
            colours = ["#00D4FF", "#00E5A0", "#FFB800", "#9B6DFF", "#FF3366",
                       "#FF8C42", "#4CC9F0", "#7BFF6A", "#F72585"][:len(labels)]
            pie = go.Figure(go.Pie(
                labels=labels,
                values=values,
                hole=0.6,
                marker=dict(colors=colours, line=dict(color="#07080D", width=2)),
                textfont=dict(family="JetBrains Mono", size=11),
            ))
            pie.update_layout(
                template="plotly_dark",
                height=260,
                margin=dict(l=0, r=0, t=10, b=0),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=True,
                legend=dict(font=dict(family="DM Sans", size=11), orientation="h", yanchor="bottom", y=-0.2),
            )
            st.plotly_chart(pie, use_container_width=True, config={"displayModeBar": False})

        # ── 產業曝險 / 相關性集中度 ──────────────────────────────────────
        cat_exp = calc_category_exposure(portfolio, total_assets)
        corr_info = calc_portfolio_correlation(portfolio)
        over_cap_cats = [c for c in cat_exp if c.get("over_cap")]
        concentrated = bool(over_cap_cats) or bool(corr_info.get("over_threshold"))
        cap_pct = CATEGORY_MAX_WEIGHT * 100

        if concentrated:
            bits = []
            if over_cap_cats:
                bits.append("、".join(f'{c["category"]} {c["weight_pct"]:.0f}%' for c in over_cap_cats)
                            + f'（單一次產業上限 {cap_pct:.0f}%，已自動禁止再加碼）')
            if corr_info.get("over_threshold"):
                bits.append(f'持倉平均相關性 {corr_info["avg_corr"]}（高度同向，看似多檔實為一注）')
            st.warning("⚠️ 集中度偏高：" + "；".join(bits))

        with st.expander("產業曝險 / 相關性", expanded=concentrated):
            if cat_exp:
                st.markdown('<div class="qsec">半導體次產業曝險</div>', unsafe_allow_html=True)
                for c in cat_exp:
                    colour = "var(--red)" if c.get("over_cap") else "var(--cyan)"
                    bar_w = min(100.0, float(c["weight_pct"]))
                    flag = " ⚠" if c.get("over_cap") else ""
                    st.markdown(
                        f'<div style="margin:5px 0">'
                        f'<div style="display:flex;justify-content:space-between;font-size:0.8rem;margin-bottom:2px">'
                        f'<span>{c["category"]}</span>'
                        f'<span style="color:{colour}">{c["weight_pct"]:.1f}%{flag}</span></div>'
                        f'<div style="height:6px;background:#1a1d27;border-radius:3px;overflow:hidden">'
                        f'<div style="height:100%;width:{bar_w}%;background:{colour}"></div></div></div>',
                        unsafe_allow_html=True,
                    )
            if corr_info.get("avg_corr") is not None:
                mp = corr_info.get("max_pair")
                pair_str = f'{mp[0]}–{mp[1]}（{corr_info["max_corr"]}）' if mp else "—"
                st.caption(
                    f'持倉平均兩兩相關性：{corr_info["avg_corr"]}'
                    f'（{corr_info["n"]} 檔，近 {CORR_LOOKBACK_DAYS} 日）　最相關：{pair_str}'
                )
                st.caption('相關性越高分散效果越差；半導體普遍偏高。新倉/加碼已自動受單一次產業上限約束。')
            else:
                st.caption('持倉不足 2 檔或歷史資料不足，無法計算相關性。')

        st.markdown('<div class="qsec">持倉明細</div>', unsafe_allow_html=True)

        for i, p in enumerate(sorted(portfolio, key=lambda x: x["MarketValue"], reverse=True)):
            sig = str(p.get("Signal", "WATCH")).upper()
            pl = float(p.get("Unrealized", 0) or 0)
            pl_p = float(p.get("PL_Pct", 0) or 0)
        
            sl_val = p.get("StopLoss")
            sl_str = f"${sl_val:.2f}" if sl_val and pd.notna(sl_val) else "—"
            tp_val = p.get("TakeProfit1")
            tp_str = f"${tp_val:.2f}" if tp_val and pd.notna(tp_val) else "—"
            rs_val = float(p.get("RS20vsSPY", 0) or 0)
            rs_str = f"{rs_val:+.1f}%" if rs_val else "—"
            sc_val = float(p.get("SignalScore", 0) or 0)
        
            if "BUY_NOW" in sig:
                accent = "#00E5A0"
            elif "BUY_ADD" in sig:
                accent = "#00D4FF"
            elif "SELL" in sig:
                accent = "#FF3366"
            else:
                accent = "#636B80"
        
            bucket_label = "Large" if p.get("Bucket", "LARGE_CAP") == "LARGE_CAP" else "Small"
        
            ticker_safe = html.escape(str(p.get("Ticker", "")))
            bucket_safe = html.escape(str(bucket_label))
            industry_raw = str(p.get("Industry", "") or "").strip()
            industry_safe = html.escape(industry_raw)
        
            brief_label, brief_color = get_ticker_brief_technical_signal(p["Ticker"])
            brief_badge_html = technical_bias_badge(brief_label, brief_color)
        
            signal_html = signal_badge(sig)
            action_tip_safe = html.escape(
                str(action_tip(p)).replace("<b>", "").replace("</b>", "")
            )
        
            meta_suffix = f" · {industry_safe}" if industry_safe else ""
            rs_color = "var(--green)" if rs_val > 0 else "var(--red)" if rs_val < 0 else "var(--muted)"
            weight_html = weight_bar(float(p.get("WeightPct", 0) or 0))
        
            card_html = "\n".join([
                '<div class="pc">',
                f'  <div class="pc-accent" style="background:{accent}"></div>',
                '',
                '  <div class="pc-header">',
                '    <div>',
                f'      <div class="pc-ticker">{ticker_safe} <span style="font-size:0.7rem;color:var(--muted);font-weight:400">{bucket_safe}</span></div>',
                f'      <div class="pc-meta">{p["Shares"]:.4f} 股 · 成本 ${p["AvgCost"]:.2f}{meta_suffix}</div>',
                f'      <div style="margin-top:8px;">{brief_badge_html}</div>',
                '    </div>',
                f'    <div style="text-align:right">{signal_html}<div style="margin-top:4px;font-family:var(--mono);font-size:0.72rem;color:var(--muted)">分數 {sc_val:.1f}</div></div>',
                '  </div>',
                '',
                '  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px;">',
                f'    <span style="font-family:var(--mono);font-size:1.15rem;font-weight:700;color:var(--text)">${p["LastPrice"]:.2f}</span>',
                f'    <span class="{pl_class(pl)}" style="font-size:0.88rem">{fmt_pct(pl_p)}&nbsp;&nbsp;{fmt_dollar(pl)}</span>',
                '  </div>',
                '',
                weight_html,
                '',
                '  <div class="pc-grid">',
                f'    <div class="pc-kv"><span class="pc-kv-label">未實現</span><span class="pc-kv-value">{fmt_dollar(float(p.get("Unrealized", 0) or 0))}</span></div>',
                f'    <div class="pc-kv"><span class="pc-kv-label">已實現</span><span class="pc-kv-value">{fmt_dollar(float(p.get("RealizedPL", 0) or 0))}</span></div>',
                f'    <div class="pc-kv"><span class="pc-kv-label">停損</span><span class="pc-kv-value">{sl_str}</span></div>',
                f'    <div class="pc-kv"><span class="pc-kv-label">目標1</span><span class="pc-kv-value">{tp_str}</span></div>',
                f'    <div class="pc-kv"><span class="pc-kv-label">RS vs SPY</span><span class="pc-kv-value" style="color:{rs_color}">{rs_str}</span></div>',
                f'    <div class="pc-kv"><span class="pc-kv-label">部位權重</span><span class="pc-kv-value">{float(p.get("WeightPct", 0) or 0):.1f}%</span></div>',
                '  </div>',
                '',
                f'  <div class="action-strip">{action_tip_safe}</div>',
                '</div>',
            ])
        
            st.markdown(card_html, unsafe_allow_html=True)
        
            render_ticker_technical_expander(
                ticker=p["Ticker"],
                expander_label=f"📈 查看 {ticker_safe} 技術分析",
                chart_key_prefix=f"portfolio_chart_{i}",
                days=180,
                expanded=False,
            )

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Scanner
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
                portfolio=portfolio,
                trades_df=trades_df,
                cash=cash,
                total_assets=total_assets,
                market_regime=market_regime,
                watchlist_df=watchlist_df,
            )
        st.session_state["scan_result"] = result
        st.rerun()

    # ── 跨產業廣度發現（P4）：不限半導體，掃全市場趨勢領導股 ──
    with st.expander("🌐 跨產業廣度發現（找全市場新標的）", expanded=False):
        st.caption("對跨產業高流動性領導股套用趨勢動能評分 + 進場品質閘（突破/回檔、不追高、強於大盤）。")
        if st.button("🔭 執行廣度發現掃描", use_container_width=True, key="run_broad"):
            with st.spinner("跨產業掃描中 …"):
                try:
                    st.session_state["broad_result"] = run_broad_scanner()
                except Exception as e:
                    st.session_state["broad_result"] = None
                    st.error(f"掃描失敗：{e}")
            st.rerun()

        _broad = st.session_state.get("broad_result")
        if _broad:
            st.caption(f"市場 {_broad['regime']}｜掃描 {_broad['total_scanned']} 檔｜"
                       f"買進候選 {_broad['total_hits']} 檔｜{_broad['scan_date']}")
            if not _broad.get("allow_new_position"):
                st.warning("⚠️ 目前市場狀態不開放新倉，以下為觀察用候選。")
            _hits = _broad["strong_buy"] + _broad["buy"]
            if not _hits:
                st.info("目前無符合進場品質的買進候選。")
            for r in _hits[:15]:
                _emoji = "🔴" if r["signal"] == "STRONG_BUY" else "🟢"
                st.markdown(f"""
<div class="sc-card">
  <div class="sc-rank">{_emoji}</div>
  <div style="flex:1">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <div class="sc-ticker">{r['ticker']} <span style="font-size:0.7rem;color:var(--muted)">{html.escape(str(r.get('sector','')))}</span></div>
      <div style="font-family:var(--mono);font-size:0.85rem">${r['close']:.2f} · {r['score']:.1f}pt</div>
    </div>
    <div class="sc-reason">[{r['trigger']}] RS{r['rs20_vs_spy']:+.1f}% · 停損 ${r['stop_loss']} · TP1 ${r['tp1']} · {' / '.join(r.get('reasons', []))}</div>
  </div>
</div>""", unsafe_allow_html=True)

    result = st.session_state.get("scan_result")
    if result:
        m = result["metrics"]
        ma, mb, mc = st.columns(3)
        ma.metric("掃描標的", m.get("universe_count", 0))
        mb.metric("買進訊號", m.get("buy_signals", 0))
        mc.metric("出場訊號", m.get("sell_signals", 0))

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
        if not watchlist_df.empty:
            st.markdown('<div class="qsec">Watchlist</div>', unsafe_allow_html=True)
            wl = watchlist_df[["Ticker", "Enabled", "Category", "Note"]].copy()
            wl["Enabled"] = wl["Enabled"].map({True: "✅", False: "⏸"})
            st.dataframe(wl, use_container_width=True, hide_index=True)
        else:
            st.info("Watchlist 空白。輸入代碼後按「＋ 加入」。")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Strategy Analysis
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
                analyze_ticker,
                hist,
                held,
                mkt_val,
                total_assets,
                cash,
                market_regime,
                heat_info["heat_pct"],
                portfolio,
            )

            score_colour = "#00E5A0" if score >= 5 else "#00D4FF" if score >= 3 else "#FFB800" if score >= 1.5 else "#FF3366"
            action_map = {
                "BUY_NOW": ("🛒 立即買進", "var(--green)"),
                "BUY_ADD": ("➕ 加碼買進", "var(--cyan)"),
                "SELL_EXIT": ("⚠️ 立即出場", "var(--red)"),
                "SELL_PARTIAL": ("◑ 部分獲利", "var(--gold)"),
                "WATCH": ("👁 觀望", "var(--muted)"),
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

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("現價", f"${det['close']:.2f}")
            k2.metric("RSI", f"{det['rsi']:.1f}")
            k3.metric("ATR", f"${det['atr']:.2f}")
            k4.metric("ADX", f"{det.get('adx', 0):.1f}")

            k5, k6, k7, k8 = st.columns(4)
            k5.metric("停損", f"${det['stop_loss']:.2f}")
            k6.metric("目標 1", f"${det['take_profit_1']:.2f}")
            k7.metric("目標 2", f"${det['take_profit_2']:.2f}")
            k8.metric("RS vs SPY", f"{det.get('rs20_vs_spy', 0):+.1f}%")

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

            st.markdown('<div class="qsec">技術圖表 (近 90 日)</div>', unsafe_allow_html=True)
            plot_df = hist.tail(90)
            fig = make_subplots(
                rows=3, cols=1, shared_xaxes=True,
                row_heights=[0.55, 0.25, 0.20],
                vertical_spacing=0.04,
                subplot_titles=("", "MACD", "RSI"),
            )

            fig.add_trace(go.Candlestick(
                x=plot_df.index,
                open=plot_df["Open"],
                high=plot_df["High"],
                low=plot_df["Low"],
                close=plot_df["Close"],
                name="K線",
                increasing_fillcolor="#00E5A0",
                increasing_line_color="#00E5A0",
                decreasing_fillcolor="#FF3366",
                decreasing_line_color="#FF3366",
            ), row=1, col=1)

            for sma_col, sma_col_name, sma_colour in [
                ("SMA20", "SMA20", "#FFB800"),
                ("SMA50", "SMA50", "#00D4FF"),
                ("SMA200", "SMA200", "#9B6DFF"),
            ]:
                fig.add_trace(go.Scatter(
                    x=plot_df.index,
                    y=plot_df[sma_col],
                    name=sma_col_name,
                    line=dict(color=sma_colour, width=1.2, dash="dot"),
                    opacity=0.7,
                ), row=1, col=1)

            fig.add_trace(go.Scatter(
                x=plot_df.index,
                y=plot_df["BB_upper"],
                line=dict(color="rgba(255,255,255,0.15)", width=1),
                showlegend=False,
            ), row=1, col=1)
            fig.add_trace(go.Scatter(
                x=plot_df.index,
                y=plot_df["BB_lower"],
                fill="tonexty",
                fillcolor="rgba(0,212,255,0.04)",
                line=dict(color="rgba(255,255,255,0.15)", width=1),
                showlegend=False,
            ), row=1, col=1)

            hist_colours = ["#00E5A0" if v > 0 else "#FF3366" for v in plot_df["MACD_Hist"]]
            fig.add_trace(go.Bar(
                x=plot_df.index,
                y=plot_df["MACD_Hist"],
                marker_color=hist_colours,
                name="MACD Hist",
            ), row=2, col=1)
            fig.add_trace(go.Scatter(
                x=plot_df.index,
                y=plot_df["MACD"],
                line=dict(color="#00D4FF", width=1.2),
                name="MACD",
            ), row=2, col=1)

            fig.add_trace(go.Scatter(
                x=plot_df.index,
                y=plot_df["RSI"],
                line=dict(color="#9B6DFF", width=1.4),
                name="RSI",
            ), row=3, col=1)
            for lvl, col in [(70, "rgba(255,51,102,0.3)"), (50, "rgba(255,255,255,0.1)"), (30, "rgba(0,229,160,0.3)")]:
                fig.add_hline(y=lvl, line_color=col, line_dash="dot", row=3, col=1)

            fig.update_layout(
                template="plotly_dark",
                height=480,
                margin=dict(l=0, r=0, t=20, b=0),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis_rangeslider_visible=False,
                showlegend=False,
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

        fee_input = st.number_input("手續費 (USD)", value=float(DEFAULT_COMMISSION), min_value=0.0, format="%.2f")
        note_input = st.text_input("備註", value="", placeholder="選填")

        gross = pr_input * sh_input
        slip = gross * DEFAULT_SLIPPAGE_PCT
        net = gross + fee_input + slip if dir_input == "BUY" else gross - fee_input - slip
        after = cash - net if dir_input == "BUY" else cash + net
        wt = (pr_input * sh_input / total_assets * 100) if total_assets > 0 else 0

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
                
    with st.expander("🔧 歷史資料修復工具（V1→V2 格式遷移）", expanded=False):
        st.markdown("""
**問題說明**：舊版代碼可能寫入 6 欄或 7 欄格式，與新版 12 欄格式不一致，會造成交易表欄位錯位、最近交易紀錄顯示異常、持倉計算錯誤。

**修復動作**：將 Google Sheets 的 `Trades` 工作表內容統一轉為新版 12 欄格式：

`TradeDateTime / CreatedAt / Ticker / Type / Price / Shares / GrossTotal / Fee / Slippage / NetTotal / Note / OrderID`

> ⚠️ 建議先手動備份 Google Sheets 的 `Trades` 工作表後再執行。
""")
        _migrate_col1, _migrate_col2 = st.columns([3, 1])
        _migrate_col1.caption("點擊右側按鈕開始遷移，執行時間約 10–30 秒。")
        if _migrate_col2.button("🛠️ 立即修復", use_container_width=True):
            with st.spinner("遷移中，請勿關閉頁面 …"):
                _ok, _msg = migrate_trades_v1_to_v2()
            if _ok:
                st.success(_msg)
                clear_market_cache()
                st.rerun()
            else:
                st.error(_msg)

    if not trades_df.empty:
        st.markdown('<div class="qsec">最近交易紀錄</div>', unsafe_allow_html=True)
        _rcols = ["TradeDateTime", "Ticker", "Type", "Price", "Shares", "GrossTotal", "Fee", "NetTotal"]
        _rcols_exist = [c for c in _rcols if c in trades_df.columns]
        recent = trades_df.tail(20)[_rcols_exist].copy()
        recent["TradeDateTime"] = pd.to_datetime(recent["TradeDateTime"], errors="coerce").dt.strftime("%m/%d %H:%M")
        _rename = {
            "TradeDateTime": "時間",
            "Ticker": "代碼",
            "Type": "方向",
            "Price": "價格",
            "Shares": "股數",
            "GrossTotal": "毛額",
            "Fee": "手續費",
            "NetTotal": "淨額",
        }
        recent = recent.rename(columns={k: v for k, v in _rename.items() if k in recent.columns})
        st.dataframe(recent[::-1], use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Performance & Analytics
# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown('<div class="qsec">績效分析</div>', unsafe_allow_html=True)

    max_dd = perf.get("max_drawdown_pct")
    sharpe = perf.get("sharpe")
    win_rate = perf.get("win_rate")
    tot_ret = perf.get("total_return_pct")

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
    <div class="pstat-label">上漲日%</div>
    <div class="pstat-value" style="color:{'var(--green)' if (win_rate or 0) >= 55 else 'var(--muted)'}">
      {f'{win_rate:.1f}%' if win_rate is not None else '—'}
    </div>
  </div>
</div>
""", unsafe_allow_html=True)
    st.caption("Sharpe 以每日 NAV 報酬年化（×√252、未扣無風險利率），NAV 每日僅記一筆故為近似值；"
               "「上漲日%」為 NAV 上漲天數比例，非交易勝率 — 真實已平倉勝率請見「訊號驗證」頁。")

    if not history_df.empty and len(history_df) >= 2:
        st.markdown('<div class="qsec">NAV vs SPY 曲線</div>', unsafe_allow_html=True)
        hdf = history_df.copy()
        base_nav = hdf["TotalAssets"].iloc[0]
        hdf["NAV_idx"] = hdf["TotalAssets"] / base_nav * 100

        fig_nav = go.Figure()
        fig_nav.add_trace(go.Scatter(
            x=hdf["Date"],
            y=hdf["NAV_idx"],
            name="Portfolio NAV",
            line=dict(color="#00D4FF", width=2),
            fill="tozeroy",
            fillcolor="rgba(0,212,255,0.06)",
        ))

        if "BenchmarkSPY" in hdf.columns and hdf["BenchmarkSPY"].notna().any():
            base_spy = hdf["BenchmarkSPY"].iloc[0]
            if base_spy and base_spy > 0:
                hdf["SPY_idx"] = hdf["BenchmarkSPY"] / base_spy * 100
                fig_nav.add_trace(go.Scatter(
                    x=hdf["Date"],
                    y=hdf["SPY_idx"],
                    name="SPY",
                    line=dict(color="#9B6DFF", width=1.5, dash="dot"),
                ))

        nav_series = hdf["TotalAssets"]
        drawdown_pct = (nav_series / nav_series.cummax() - 1) * 100
        fig_nav.add_trace(go.Scatter(
            x=hdf["Date"],
            y=drawdown_pct,
            name="Drawdown",
            line=dict(color="#FF3366", width=0),
            fill="tozeroy",
            fillcolor="rgba(255,51,102,0.07)",
            yaxis="y2",
        ))

        fig_nav.update_layout(
            template="plotly_dark",
            height=320,
            margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="JetBrains Mono", size=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.01, font=dict(size=10)),
            yaxis=dict(title="Index (100=Start)", gridcolor="rgba(255,255,255,0.05)"),
            yaxis2=dict(title="DD %", overlaying="y", side="right", gridcolor="rgba(0,0,0,0)", range=[-50, 5]),
            xaxis=dict(gridcolor="rgba(255,255,255,0.04)"),
        )
        st.plotly_chart(fig_nav, use_container_width=True, config={"displayModeBar": False})

        st.markdown('<div class="qsec">每日報酬分佈</div>', unsafe_allow_html=True)
        if "DailyReturnPct" in hdf.columns:
            rets = pd.to_numeric(hdf["DailyReturnPct"], errors="coerce").dropna()
            if not rets.empty:
                fig_hist = go.Figure(go.Histogram(
                    x=rets,
                    nbinsx=40,
                    marker_color="#00D4FF",
                    opacity=0.75,
                ))
                fig_hist.update_layout(
                    template="plotly_dark",
                    height=200,
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

    st.markdown("<hr class='qdiv'>", unsafe_allow_html=True)
    st.markdown('<div class="qsec">快速設定</div>', unsafe_allow_html=True)

    new_cap = st.number_input(
        "初始資金 (USD)",
        value=st.session_state.init_capital,
        min_value=1000.0,
        step=1000.0,
        format="%.0f"
    )
    if new_cap != st.session_state.init_capital:
        st.session_state.init_capital = new_cap
        st.rerun()

    col_ref, col_sync = st.columns(2)
    if col_ref.button("🔄 刷新快取", use_container_width=True):
        clear_market_cache()
        st.rerun()

    if col_sync.button("📡 記錄今日 NAV", use_container_width=True):
        ok, msg = maybe_log_daily_history(
            total_assets=total_assets,
            cash=cash,
            market_value=market_value,
            realized_pl=total_realized_pl,
            unrealized_pl=total_unrealized_pl,
        )
        st.toast(msg, icon="✅" if ok else "ℹ️")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 — US Semiconductor Scanner
# ═══════════════════════════════════════════════════════════════════════════════
with tab6:
    if "semi_result" not in st.session_state:
        st.session_state.semi_result = None

    st.markdown('<div class="qsec">🔬 美股半導體宇宙掃描器</div>', unsafe_allow_html=True)

    _semi_n = len(set(US_SEMI_UNIVERSE))
    st.markdown(textwrap.dedent(f"""
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
        """).strip(), unsafe_allow_html=True)

    with st.expander("📐 策略過濾器說明", expanded=False):
        st.markdown(f"""
**8 層多因子評分（滿分 ~10 分）**

| 層 | 因子 | 分值 | 說明 |
|---|---|---|---|
| A | SMA 多頭排列 | +2.5 | SMA20 > SMA50 > SMA200 全線向上 |
| B | MACD 翻多加速 | +0.8 | 柱狀圖翻正且持續放大 |
| B | RSI 健康帶 50–72 | +0.8 | 動能健康，非超買 |
| B | ADX ≥ 18 | +0.8 | 趨勢強度足夠 |
| C | RS vs SPY > +2% | +1.0 | 強於大盤（20日相對強度） |
| C | 優於 SOX 指數 | +0.8 | 領先半導體板塊 |
| D | OBV 法人吸籌 | +0.5 | OBV 斜率向上 + 站上 SMA20 |
| D | 放量上漲日 | +0.5 | 成交量 > 均量 1.5x 且收漲 |
| E | BB 壓縮放量突破 | +1.5 | 低波動率壓縮後量價齊揚 |
| F | 接近 52W 年高 | +0.8 | 距年高 10% 內（強勢領頭羊） |
| G | SOX 趨勢乘數 | ×1.0/0.85/0.65 | BULL / NEUTRAL / BEAR 調整整體得分 |
| H | 流動性門檻 | 必要條件 | 日均成交值 > $20M，非財報封鎖期 |

**訊號門檻**：🔴 強力買進 ≥ {US_SEMI_SCORE_STRONG} ｜ 🟢 積極買進 ≥ {US_SEMI_SCORE_BUY} ｜ 🟡 留意候補 ≥ {US_SEMI_SCORE_WATCH}
""")

    st.markdown("<hr class='qdiv'>", unsafe_allow_html=True)
    st.markdown('<div class="qsec">手動掃描設定</div>', unsafe_allow_html=True)

    _ctl1, _ctl2 = st.columns(2)
    _workers = _ctl1.slider("平行 Worker 數", min_value=3, max_value=20, value=10, help="越多越快，但易觸發 yfinance 限流")
    _dry_run = _ctl2.checkbox("Dry Run（掃描但不發 Telegram）", value=True)

    _extra_input = st.text_input(
        "額外加入掃描的標的（逗號分隔）",
        placeholder="SMCI, ARM …",
        help="除預設宇宙外額外加入，例如你的持倉中有半導體股",
    )
    _extra_tickers = [t.strip().upper() for t in _extra_input.split(",") if t.strip()]

    if st.button("🚀 立即執行美股半導體掃描", use_container_width=True, type="primary"):
        _universe = list(dict.fromkeys(
            [normalize_ticker(t) for t in US_SEMI_UNIVERSE] + [normalize_ticker(t) for t in _extra_tickers]
        ))

        _prog_bar = st.progress(0, text="取得 SOX 指數狀態 …")
        _sox = _get_sox_regime()
        _sox_trend = _sox.get("trend", "NEUTRAL")
        _sox_emoji = {"BULL": "🐂", "NEUTRAL": "➡️", "BEAR": "🐻"}

        _prog_bar.progress(
            0,
            text=f"SOX {_sox_emoji.get(_sox_trend)} {_sox_trend} vs SPY {_sox.get('rs_vs_spy', 0):+.1f}% ｜ 開始掃描 {len(_universe)} 檔 …"
        )

        _results = []
        _done = 0
        _total = len(_universe)

        def _scan_ticker(tk):
            return _us_semi_score_one(tk, _sox)

        with _cf_ui.ThreadPoolExecutor(max_workers=_workers) as _ex:
            futures = {_ex.submit(_scan_ticker, tk): tk for tk in _universe}
            for _future in _cf_ui.as_completed(futures):
                _done += 1
                try:
                    _res = _future.result()
                    if _res:
                        _results.append(_res)
                except Exception:
                    pass

                pct = _done / _total if _total else 1.0
                _prog_bar.progress(
                    pct,
                    text=f"掃描 {_done}/{_total} ({pct*100:.0f}%) — 發現 {len(_results)} 個入選標的"
                )

        _prog_bar.empty()

        # 套用與排程引擎 (run_us_semi_scanner) 一致的風控閘：標註持倉/集中度/冷卻後，
        # 用即時權益計算最終可買股數。過去 UI 只跑 _us_semi_score_one，顯示的建議股數
        # 用固定本金、無視 regime/現金/權重/產業/熱度/冷卻，與你的實際進場引擎不一致。
        try:
            _exposure = {c["category"]: c for c in calc_category_exposure(portfolio, total_assets)}
            _held_set = _held_tickers_from_trades(trades_df)
            _cap_pct = CATEGORY_MAX_WEIGHT * 100
            _heat_pct = heat_info.get("heat_pct", 0.0)
            for _r in _results:
                _annotate_semi_candidate(_r, _exposure, _held_set, trades_df, _cap_pct)
                apply_entry_risk_gates(_r, portfolio, total_assets, cash, _heat_pct, market_regime)
        except Exception:
            pass

        _results.sort(key=lambda x: -x["score"])
        _strong = [r for r in _results if r["signal"] == "STRONG_BUY"]
        _buys = [r for r in _results if r["signal"] == "BUY"]
        _watch = [r for r in _results if r["signal"] == "WATCH"]

        st.session_state.semi_result = {
            "strong_buy": _strong,
            "buy": _buys,
            "watch": _watch,
            "all_results": _results,
            "sox_regime": _sox,
            "total_scanned": _total,
            "total_hits": len(_results),
            "scan_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

        if not _dry_run and _results:
            _scan_res_for_tg = dict(st.session_state.semi_result)
            _scan_res_for_tg["scan_date"] = datetime.now().strftime("%Y-%m-%d")
            _msgs = format_us_semi_tg_messages(_scan_res_for_tg)
            _ok = send_us_semi_tg(_msgs)
            st.toast("✅ Telegram 推播成功！" if _ok else "❌ Telegram 推播失敗", icon="📡")
        elif _dry_run:
            st.toast("Dry Run 模式：未發送 Telegram", icon="🔕")

    _res_data = st.session_state.get("semi_result")
    if _res_data:
        _sox_r = _res_data["sox_regime"]
        _all = _res_data["all_results"]
        _strong = _res_data["strong_buy"]
        _buys = _res_data["buy"]
        _watches = _res_data["watch"]
        _scan_time = _res_data.get("scan_date", "—")
        _sox_trend = _sox_r.get("trend", "NEUTRAL")
        _SOX_BADGE = {
            "BULL": ("badge-up", "🐂 多頭"),
            "NEUTRAL": ("badge-flat", "➡️ 中性"),
            "BEAR": ("badge-down", "🐻 空頭"),
        }
        _sox_cls, _sox_lbl = _SOX_BADGE.get(_sox_trend, ("badge-flat", "—"))

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
            _SIG_LABEL = {
                "STRONG_BUY": ("var(--red)", "🔴 強力買進"),
                "BUY": ("var(--green)", "🟢 積極買進"),
                "WATCH": ("var(--gold)", "🟡 留意候補"),
            }
            _rank_emoji = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

            for _i, _r in enumerate(_all):
                _sig_color, _sig_label = _SIG_LABEL.get(_r["signal"], ("var(--muted)", "—"))
                _stars = "⭐" * min(5, max(1, round(_r["score"] / 1.5)))
                _reasons = "、".join(_r["reasons"][:3]) if _r["reasons"] else "—"
                _rank = _rank_emoji[_i] if _i < len(_rank_emoji) else f"{_i+1}."
                _score_w = min(100, _r["score"] / 8 * 100)
                _warns = _r.get("warnings") or []
                _warn_html = ""
                if _warns:
                    _chips = ""
                    for _w in _warns:
                        _wc = "var(--gold)" if _w.startswith("⚠️") else "var(--cyan)"
                        _chips += (f'<span style="font-size:.68rem;padding:2px 7px;border-radius:5px;'
                                   f'background:rgba(255,255,255,.04);color:{_wc};border:1px solid {_wc}40">{_w}</span>')
                    _warn_html = f'<div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:8px">{_chips}</div>'
            
                st.markdown(f"""
            <div class="pc">
              <div class="pc-accent" style="background:{_sig_color}"></div>
              <div class="pc-header">
                <div>
                  <div class="pc-ticker">{_rank} {_r['ticker']} <span style="font-size:.7rem;color:var(--muted);font-weight:400">{_stars}</span></div>
                  <div class="pc-meta">{_r.get('category', 'Semiconductor / Other')} · {_r['reasons'][0] if _r['reasons'] else ''}</div>
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
              {_warn_html}
            </div>
            """, unsafe_allow_html=True)
            
                render_ticker_technical_expander(
                    ticker=_r["ticker"],
                    expander_label=f"📈 查看 {_r['ticker']} 技術分析",
                    chart_key_prefix=f"semi_chart_{_i}",
                    days=180,
                    expanded=False,
                )

            st.markdown("<hr class='qdiv'>", unsafe_allow_html=True)
            st.markdown('<div class="qsec">完整結果表格</div>', unsafe_allow_html=True)
            _df_show = pd.DataFrame([{
                "代碼": r["ticker"],
                "類別": r.get("category", "Semiconductor / Other"),
                "訊號": r["signal"],
                "分數": r["score"],
                "現價": r["close"],
                "停損": r["stop_loss"],
                "TP1": r["tp1"],
                "TP2": r["tp2"],
                "RSI": r["rsi"],
                "ADX": r["adx"],
                "RS%SPY": r["rs20_vs_spy"],
                "日均量$M": r["dv20_m"],
                "建議股數": r["suggested_qty"],
                "因子": "、".join(r["reasons"][:3]),
            } for r in _all])
            st.dataframe(_df_show, use_container_width=True, hide_index=True)

            with st.expander("📨 Telegram 訊息預覽", expanded=False):
                _tg_res = dict(_res_data)
                _tg_res["scan_date"] = datetime.now().strftime("%Y-%m-%d")
                _tg_msgs = format_us_semi_tg_messages(_tg_res)
                for _mi, _m in enumerate(_tg_msgs, 1):
                    st.caption(f"第 {_mi} 則（{len(_m)} 字元）")
                    st.code(_m, language=None)

            if st.button("✖ 清除掃描結果", use_container_width=True):
                st.session_state.semi_result = None
                st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 7 — Signal Validation & Edge（訊號驗證）
# ═══════════════════════════════════════════════════════════════════════════════
with tab7:
    st.markdown('<div class="qsec">真實交易統計（已平倉 · FIFO）</div>', unsafe_allow_html=True)
    st.caption("以 FIFO 配對計算已平倉交易的真實勝率與盈虧比，與「績效」頁的『上漲日%』是不同概念。")

    ts = calc_realized_trade_stats(trades_df)
    if ts["closed_trades"] == 0:
        st.info("尚無已平倉交易，無法計算交易統計。")
    else:
        wr = ts["win_rate"]
        pf = ts["profit_factor"]
        exp = ts["expectancy"]
        payoff = ts["payoff_ratio"]
        st.markdown(f"""
<div class="pstat-grid">
  <div class="pstat">
    <div class="pstat-label">已平倉筆數</div>
    <div class="pstat-value">{ts['closed_trades']}</div>
  </div>
  <div class="pstat">
    <div class="pstat-label">真實勝率</div>
    <div class="pstat-value" style="color:{'var(--green)' if (wr or 0) >= 50 else 'var(--gold)'}">{wr:.1f}%</div>
  </div>
  <div class="pstat">
    <div class="pstat-label">獲利因子 PF</div>
    <div class="pstat-value" style="color:{'var(--green)' if (pf or 0) >= 1.5 else 'var(--gold)' if (pf or 0) >= 1 else 'var(--red)'}">{f'{pf:.2f}' if pf is not None else '∞'}</div>
  </div>
  <div class="pstat">
    <div class="pstat-label">每筆期望值</div>
    <div class="pstat-value" style="color:{'var(--green)' if (exp or 0) >= 0 else 'var(--red)'}">${exp:.2f}</div>
  </div>
</div>
""", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        c1.metric("平均獲利", f"${ts['avg_win']:.2f}")
        c2.metric("平均虧損", f"${ts['avg_loss']:.2f}")
        c3.metric("盈虧比", f"{payoff:.2f}" if payoff is not None else "—")
        st.caption(
            f"毛獲利 ${ts['gross_profit']:,.2f}　|　毛虧損 ${ts['gross_loss']:,.2f}　|　淨已實現 ${ts['net_realized']:,.2f}"
        )

    st.divider()

    # ── 訊號成效（買進訊號 · 分數分箱）─────────────────────────────────────
    st.markdown('<div class="qsec">訊號成效（買進訊號 · 分數分箱）</div>', unsafe_allow_html=True)
    st.caption("回顧 Signals 表中已成熟的訊號，計算前瞻報酬。若高分箱未明顯優於低分箱，代表評分缺乏 edge。")

    _src_label = st.radio(
        "訊號來源", ["半導體 (SEMI)", "持倉引擎 (PORTFOLIO)", "全部"],
        horizontal=True, key="edge_source",
        help="兩套引擎分數尺度不同，建議單一來源分析。你的進場是半導體引擎，故預設 SEMI。",
    )
    _src = {"半導體 (SEMI)": "SEMI", "持倉引擎 (PORTFOLIO)": "PORTFOLIO", "全部": None}[_src_label]

    if st.button("🧪 計算訊號成效", use_container_width=True, key="run_edge"):
        with st.spinner("回測已記錄訊號的前瞻表現…（需抓取歷史價，可能稍久）"):
            try:
                _outcomes = evaluate_signal_outcomes(source=_src)
                st.session_state.sig_outcomes = _outcomes
                st.session_state.sig_edge = summarize_signal_edge(_outcomes)
            except Exception as e:
                st.session_state.sig_outcomes = None
                st.session_state.sig_edge = None
                st.error(f"計算失敗：{e}")

    _edge = st.session_state.get("sig_edge")
    _outcomes = st.session_state.get("sig_outcomes")
    if _outcomes is not None:
        _n_buy = (
            int(_outcomes["Action"].astype(str).str.contains("BUY", na=False).sum())
            if not _outcomes.empty else 0
        )
        st.caption(f"已成熟訊號：{len(_outcomes)} 筆（買進類 {_n_buy} 筆）· 來源：{_src_label}")
        if _edge is not None and not _edge.empty:
            st.dataframe(_edge, use_container_width=True, hide_index=True)
            st.caption("判讀：理想情況下『勝率%』與『平均報酬%』應隨分數區間遞增；若否，請重新檢視評分權重或 BUY 門檻。")
        else:
            st.info("買進類成熟訊號不足，無法分箱彙總（需先累積一段時間的訊號記錄）。")

    st.divider()

    # ── 半導體宇宙資料健檢 ─────────────────────────────────────────────────
    st.markdown('<div class="qsec">半導體宇宙資料健檢</div>', unsafe_allow_html=True)
    st.caption("檢查每檔是否取得到資料；取不到的多半是下市/更名/錯誤代碼，建議從 US_SEMI_UNIVERSE 移除。")

    if st.button("🔍 執行宇宙健檢", use_container_width=True, key="run_audit"):
        with st.spinner(f"檢查 {len(US_SEMI_UNIVERSE)} 檔資料可取得性…"):
            try:
                st.session_state.universe_audit = audit_universe(US_SEMI_UNIVERSE)
            except Exception as e:
                st.session_state.universe_audit = None
                st.error(f"健檢失敗：{e}")

    _audit = st.session_state.get("universe_audit")
    if _audit is not None and not _audit.empty:
        _bad = _audit[~_audit["可取得資料"]]
        if not _bad.empty:
            st.warning(f"⚠️ {len(_bad)} 檔取不到資料：{'、'.join(_bad['Ticker'].tolist())}")
        else:
            st.success("✅ 全部代碼皆可取得資料。")
        st.dataframe(_audit, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="text-align:center;padding:20px 0 8px;font-size:0.65rem;color:var(--muted);font-family:var(--mono)">
  QUANTPRO v3.0 · {datetime.now().strftime('%Y/%m/%d %H:%M')} · 本系統僅供輔助參考，不構成投資建議
</div>
""", unsafe_allow_html=True)

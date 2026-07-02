"""
backtest.py — 事件驅動回測引擎（重放實盤 evaluate_strategy 邏輯）

設計原則
────────────────────────────────────────────────────────────────────────
1. 不另寫策略：逐根 K 棒把 hist「切到當日為止」餵進現行的 core.evaluate_strategy，
   確保回測 = 實盤邏輯，不會兩套漂移。所有指標（SMA/ATR/RSI/MACD/RollingHigh/RS…）
   皆為因果型（只用過去資料），切片 df.iloc[:pos+1] 天然無未來函數。
2. regime 每日以「截至當日」的 SPY/QQQ/VIX 指標列重算（core._regime_from_indicator_rows，
   與實盤同一函式）。
3. 成交在「訊號當根收盤」±滑點成交（EOD 系統標準做法），手續費/滑點沿用 core 設定。
4. 依賴注入：傳入 data / regime_frames / benchmarks 即可離線回測與單元測試，不碰網路。

已知限制（研究工具，非撮合模擬器）
────────────────────────────────────────────────────────────────────────
• yfinance 僅約 2 年日線，樣本有限；更長期需自備資料。
• is_earnings_blocked / classify_symbol_bucket / 產業分類使用「當前」財報日與市值
  （非歷史 as-of），對進場閘有輕微前視；影響小，於報告中揭露。
• 成交量衝擊未建模；滑點為固定百分比近似。

CLI
────────────────────────────────────────────────────────────────────────
  python backtest.py                      # 半導體宇宙、近 2 年、預設本金
  python backtest.py --universe broad     # 跨產業領導股宇宙
  python backtest.py --tickers NVDA,AMD,AVGO --start 2024-01-01 --capital 32000
  python backtest.py --csv equity.csv     # 另存權益曲線 CSV
"""
from __future__ import annotations

import argparse
import math
import sys
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from core import (
    BROAD_UNIVERSE_DEFAULT,
    CASH_RESERVE_PCT,
    COOLDOWN_DAYS,
    DEFAULT_COMMISSION,
    DEFAULT_INITIAL_CAPITAL,
    DEFAULT_SLIPPAGE_PCT,
    US_SEMI_UNIVERSE,
    TRADE_HEADERS_V2,
    _regime_from_indicator_rows,
    calc_realized_trade_stats,
    evaluate_strategy,
    get_unified_analysis,
    normalize_ticker,
    safe_float,
)
import core

TRADING_DAYS_PER_YEAR = 252


# ────────────────────────────────────────────────────────────────────────────
# 索引工具：把（可能帶時區的）DatetimeIndex 正規化成 tz-naive 當日
# ────────────────────────────────────────────────────────────────────────────
def _norm_index(idx: pd.Index) -> pd.DatetimeIndex:
    di = pd.DatetimeIndex(idx)
    if di.tz is not None:
        di = di.tz_localize(None)
    return di.normalize()


class _Position:
    """單一標的的持倉狀態（FIFO lots），鏡射 core.build_portfolio 的計算方式。"""

    __slots__ = ("lots", "run_bought", "last_stop")

    def __init__(self):
        self.lots: List[Dict] = []          # [{shares, price(含成本), date}]
        self.run_bought: float = 0.0        # 本輪（自上次清空後）累計買進股數 → EntryShares
        self.last_stop: float = 0.0         # 上一次評估得到的止損（供 heat 計算）

    @property
    def shares(self) -> float:
        return sum(l["shares"] for l in self.lots)

    @property
    def cost_basis(self) -> float:
        return sum(l["shares"] * l["price"] for l in self.lots)

    @property
    def avg_cost(self) -> float:
        s = self.shares
        return self.cost_basis / s if s > 1e-9 else 0.0

    @property
    def entry_date(self):
        return self.lots[0]["date"] if self.lots else None

    def buy(self, shares: float, fill_price_per_share_incl_costs: float, date):
        self.lots.append({"shares": shares, "price": fill_price_per_share_incl_costs, "date": date})
        self.run_bought += shares

    def sell_fifo(self, shares: float, proceeds_per_share: float) -> float:
        """FIFO 賣出，回傳已實現損益。清空後 run_bought 歸零（下一輪重新計 EntryShares）。"""
        realized = 0.0
        sell_qty = shares
        while sell_qty > 1e-9 and self.lots:
            first = self.lots[0]
            matched = min(sell_qty, first["shares"])
            realized += (proceeds_per_share - first["price"]) * matched
            first["shares"] -= matched
            sell_qty -= matched
            if first["shares"] <= 1e-9:
                self.lots.pop(0)
        if not self.lots:
            self.run_bought = 0.0
        return realized


# ────────────────────────────────────────────────────────────────────────────
# 主回測
# ────────────────────────────────────────────────────────────────────────────
def run_backtest(
    tickers: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    initial_capital: float = DEFAULT_INITIAL_CAPITAL,
    fee: float = DEFAULT_COMMISSION,
    slippage_pct: float = DEFAULT_SLIPPAGE_PCT,
    data: Optional[Dict[str, pd.DataFrame]] = None,
    regime_frames: Optional[Dict[str, pd.DataFrame]] = None,
    benchmarks: Optional[Dict[str, pd.DataFrame]] = None,
    benchmark_symbols: Optional[List[str]] = None,
    progress: bool = False,
    earnings_gate: bool = False,
) -> Dict:
    """回測入口（P1）：預設停用財報封鎖閘。

    core.is_earnings_blocked 以「執行當下」的下一次財報日判斷，對歷史回放是
    非平穩污染源——同參數在不同日期執行，結果會漂移；執行日恰逢某檔財報前
    N 日時，該檔在整段 2 年回測中一筆都不會進場。故回測預設停用此閘，並於
    metrics["earnings_gate"] 註記（live 仍會迴避財報，回測進場面因此略偏樂觀）。
    研究財報效應時可顯式傳 earnings_gate=True。

    實作為「暫時替換 core.is_earnings_blocked、結束必還原」——屬進程級副作用，
    勿與其他執行緒的即時掃描並行執行。
    """
    if earnings_gate:
        result = _run_backtest_impl(tickers, start, end, initial_capital, fee, slippage_pct,
                                    data, regime_frames, benchmarks, benchmark_symbols, progress)
    else:
        _orig_gate = core.is_earnings_blocked
        core.is_earnings_blocked = lambda *_a, **_k: False
        try:
            result = _run_backtest_impl(tickers, start, end, initial_capital, fee, slippage_pct,
                                        data, regime_frames, benchmarks, benchmark_symbols, progress)
        finally:
            core.is_earnings_blocked = _orig_gate
    result["metrics"]["earnings_gate"] = bool(earnings_gate)
    return result


def _run_backtest_impl(
    tickers: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    initial_capital: float = DEFAULT_INITIAL_CAPITAL,
    fee: float = DEFAULT_COMMISSION,
    slippage_pct: float = DEFAULT_SLIPPAGE_PCT,
    data: Optional[Dict[str, pd.DataFrame]] = None,
    regime_frames: Optional[Dict[str, pd.DataFrame]] = None,
    benchmarks: Optional[Dict[str, pd.DataFrame]] = None,
    benchmark_symbols: Optional[List[str]] = None,
    progress: bool = False,
) -> Dict:
    """
    回傳 dict：metrics / equity_curve(DataFrame) / trades(DataFrame) / benchmarks。

    data / regime_frames / benchmarks 若提供則直接使用（離線/測試）；否則以 yfinance 抓取。
      data[ticker]           → core.get_unified_analysis 產出的指標 DataFrame
      regime_frames[sym]     → 同上，sym ∈ {SPY, QQQ, ^VIX}
      benchmarks[sym]        → 至少含 Close 的 DataFrame（買進持有對標）
    """
    # ── 1) 資料 ────────────────────────────────────────────────────────────
    if data is None:
        tickers = [normalize_ticker(t) for t in (tickers or US_SEMI_UNIVERSE)]
        tickers = list(dict.fromkeys(tickers))
        data = {}
        for i, tk in enumerate(tickers):
            if progress and i % 20 == 0:
                print(f"  下載資料 {i}/{len(tickers)} …")
            df = get_unified_analysis(tk)
            if df is not None and not df.empty:
                data[tk] = df
    else:
        data = {normalize_ticker(k): v for k, v in data.items() if v is not None and not v.empty}

    if not data:
        raise ValueError("無任何標的資料可回測")

    if regime_frames is None:
        regime_frames = {s: get_unified_analysis(s) for s in ("SPY", "QQQ", "^VIX")}
    regime_frames = {k: v for k, v in regime_frames.items() if v is not None and not getattr(v, "empty", True)}

    # 對標（買進持有）：預設 SOXX + SPY
    if benchmarks is None:
        benchmark_symbols = benchmark_symbols or ["SOXX", "SPY"]
        benchmarks = {}
        for s in benchmark_symbols:
            bf = regime_frames.get(s)          # 不可用 `or`：DataFrame 的布林值不明確會拋錯
            if bf is None:
                bf = get_unified_analysis(s)
            if bf is not None and not bf.empty:
                benchmarks[s] = bf

    # ── 2) 主交易日曆（以 SPY regime 日曆為準，確保每日有 regime）────────────
    spy = regime_frames.get("SPY")
    if spy is not None and not spy.empty:
        master = _norm_index(spy.index)
    else:
        # 無 SPY 時退回「所有標的日期聯集」
        alld = pd.DatetimeIndex([])
        for df in data.values():
            alld = alld.union(_norm_index(df.index))
        master = alld.sort_values()

    lo = pd.Timestamp(start).normalize() if start else master.min()
    hi = pd.Timestamp(end).normalize() if end else master.max()
    master = master[(master >= lo) & (master <= hi)]
    if len(master) < 2:
        raise ValueError("回測期間資料點不足（<2 日）")

    # ── 3) 每檔預先建立 date→pos 對照與 close 陣列（避免每日 label 切片開銷）──
    frames: Dict[str, Dict] = {}
    for tk, df in data.items():
        nidx = _norm_index(df.index)
        frames[tk] = {
            "df": df,
            "close": df["Close"].to_numpy(dtype="float64"),
            "pos": {d: i for i, d in enumerate(nidx)},
        }

    # regime 對照：把 QQQ/VIX 對齊到 SPY 日曆（as-of ffill），逐日預算 regime
    regime_by_date: Dict[pd.Timestamp, Dict] = {}
    if spy is not None and not spy.empty:
        spy_n = spy.copy()
        spy_n.index = _norm_index(spy.index)
        qqq = regime_frames.get("QQQ")
        vix = regime_frames.get("^VIX")
        qqq_al = None
        vix_al = None
        if qqq is not None and not qqq.empty:
            qn = qqq.copy(); qn.index = _norm_index(qqq.index)
            qqq_al = qn.reindex(spy_n.index).ffill()
        if vix is not None and not vix.empty:
            vn = vix.copy(); vn.index = _norm_index(vix.index)
            vix_al = vn.reindex(spy_n.index).ffill()
        for d in master:
            if d not in spy_n.index:
                continue
            regime_by_date[d] = _regime_from_indicator_rows(
                spy_n.loc[d],
                qqq_al.loc[d] if qqq_al is not None else None,
                vix_al.loc[d] if vix_al is not None else None,
            )
    # 與 core 的 fail-closed 一致（P4）：無 regime 資料的日子不得開新倉/加碼
    default_regime = {"regime": "UNKNOWN", "score": 0, "allow_new_position": False,
                      "allow_add_position": False, "risk_multiplier": 0.25, "vix": None}

    # ── 4) 模擬狀態 ────────────────────────────────────────────────────────
    cash = float(initial_capital)
    positions: Dict[str, _Position] = {}
    trade_log: List[Dict] = []
    nav_rows: List[Dict] = []
    cash_reserve = initial_capital * CASH_RESERVE_PCT  # 與實盤一致：保留現金準備

    def _recent_status(tk: str, today: pd.Timestamp):
        cutoff = today - pd.Timedelta(days=COOLDOWN_DAYS)
        rb = rs = False
        for t in reversed(trade_log):
            if t["Ticker"] != tk:
                continue
            if t["Date"] < cutoff:
                break
            if t["Type"] == "BUY":
                rb = True
            elif t["Type"] == "SELL":
                rs = True
        return rb, rs

    # ── 5) 逐日回放 ────────────────────────────────────────────────────────
    for di, today in enumerate(master):
        if progress and di % 60 == 0:
            print(f"  回放 {di}/{len(master)}  {today.date()}  NAV≈{cash + sum(positions[t].shares * frames[t]['close'][frames[t]['pos'][today]] for t in positions if today in frames[t]['pos']):,.0f}")

        regime = regime_by_date.get(today, default_regime)

        # (a) 目前持倉的今日市值 / 建構給 evaluate_strategy 的 portfolio 列表
        held_today = {}
        port_list: List[Dict] = []
        for tk, pos in positions.items():
            if pos.shares <= 1e-9:
                continue
            fr = frames[tk]
            if today not in fr["pos"]:
                continue  # 該檔今日無報價 → 當日不處理（保留部位）
            px = fr["close"][fr["pos"][today]]
            mv = pos.shares * px
            held_today[tk] = {"px": px, "mv": mv, "pos": pos}
            port_list.append({
                "Ticker": tk,
                "Shares": pos.shares,
                "AvgCost": pos.avg_cost,
                "EntryDate": pos.entry_date,
                "EntryShares": pos.run_bought,
                "MarketValue": mv,
            })

        market_value = sum(h["mv"] for h in held_today.values())
        total_assets = cash + market_value

        # (b) 投組熱度：用「上一輪評估」的止損（新倉當日尚無止損 → 不計入）
        heat = 0.0
        for tk, h in held_today.items():
            stop = h["pos"].last_stop
            if h["px"] > stop > 0:
                heat += (h["px"] - stop) * h["pos"].shares
        heat_pct = heat / total_assets * 100 if total_assets > 0 else 0.0

        # (c) 對宇宙每檔評估（持有與未持有都要，才會有進場訊號）
        decisions = []
        for tk, fr in frames.items():
            if today not in fr["pos"]:
                continue
            pos = fr["pos"][today]
            hist_view = fr["df"].iloc[:pos + 1]     # 截至當日（含）→ 無未來函數
            if len(hist_view) < 60:
                continue
            held_shares = held_today[tk]["pos"].shares if tk in held_today else 0.0
            cur_mv = held_today[tk]["mv"] if tk in held_today else 0.0
            rb, rs = _recent_status(tk, today)
            try:
                sc, act, det, note = evaluate_strategy(
                    tk, hist_view, held_shares, cur_mv, total_assets, cash,
                    regime, heat_pct, port_list, recent_buy=rb, recent_sell=rs,
                )
            except Exception:
                continue
            # 記錄止損供下一日 heat 使用
            if tk in positions:
                positions[tk].last_stop = safe_float(det.get("stop_loss"))
            if act != "WATCH":
                decisions.append((tk, sc, act, det))

        # (d) 執行：先賣後買（釋放現金/熱度），買進依分數高→低
        sells = [d for d in decisions if "SELL" in d[2]]
        buys = [d for d in decisions if "BUY" in d[2]]
        buys.sort(key=lambda x: -x[1])

        for tk, sc, act, det in sells:
            pos = positions.get(tk)
            if pos is None or pos.shares <= 1e-9:
                continue
            px = frames[tk]["close"][frames[tk]["pos"][today]]
            if act == "SELL_EXIT":
                qty = pos.shares
            else:  # SELL_PARTIAL
                qty = min(pos.shares, float(det.get("suggested_sell_qty", 0) or 0))
            if qty <= 1e-9:
                continue
            proceeds_ps = px * (1 - slippage_pct)
            pos.sell_fifo(qty, proceeds_ps)
            cash += px * qty * (1 - slippage_pct) - fee
            trade_log.append({"Date": today, "Ticker": tk, "Type": "SELL",
                              "Price": px, "Shares": qty, "Fee": fee,
                              "Slippage": px * qty * slippage_pct})

        for tk, sc, act, det in buys:
            qty = float(det.get("suggested_buy_qty", 0) or 0)
            if qty <= 0:
                continue
            px = frames[tk]["close"][frames[tk]["pos"][today]]
            cost = px * qty * (1 + slippage_pct) + fee
            # 現金閘（保留現金準備）— 與實盤 avail_cash 一致
            if cost > cash - cash_reserve + 1e-6:
                # 依可用現金縮量
                avail = cash - cash_reserve - fee
                qty = math.floor(avail / (px * (1 + slippage_pct))) if px > 0 else 0
                if qty <= 0:
                    continue
                cost = px * qty * (1 + slippage_pct) + fee
            pos = positions.setdefault(tk, _Position())
            pos.buy(qty, cost / qty, today)
            cash -= cost
            trade_log.append({"Date": today, "Ticker": tk, "Type": "BUY",
                              "Price": px, "Shares": qty, "Fee": fee,
                              "Slippage": px * qty * slippage_pct})

        # (e) 收盤後記錄 NAV
        mv_close = 0.0
        for tk, pos in positions.items():
            if pos.shares <= 1e-9:
                continue
            fr = frames[tk]
            if today in fr["pos"]:
                mv_close += pos.shares * fr["close"][fr["pos"][today]]
            else:
                mv_close += pos.cost_basis  # 無報價 → 以成本估
        nav_rows.append({"Date": today, "NAV": cash + mv_close, "Cash": cash,
                         "MarketValue": mv_close, "Positions": sum(1 for p in positions.values() if p.shares > 1e-9)})

    equity = pd.DataFrame(nav_rows).set_index("Date")
    trades_df = _trades_to_df(trade_log)
    metrics = _compute_metrics(equity, trades_df, initial_capital)
    bench = _benchmark_metrics(benchmarks, master, initial_capital)
    metrics["benchmarks"] = bench

    return {"metrics": metrics, "equity_curve": equity, "trades": trades_df,
            "benchmark_curves": bench.get("_curves", {})}


# ────────────────────────────────────────────────────────────────────────────
# 指標
# ────────────────────────────────────────────────────────────────────────────
def _trades_to_df(trade_log: List[Dict]) -> pd.DataFrame:
    """把回測交易記錄轉成 core.calc_realized_trade_stats 能吃的 V2 交易表格式。"""
    if not trade_log:
        return pd.DataFrame(columns=TRADE_HEADERS_V2)
    rows = []
    for t in trade_log:
        gross = t["Price"] * t["Shares"]
        rows.append({
            "TradeDateTime": t["Date"], "CreatedAt": t["Date"], "Ticker": t["Ticker"],
            "Type": t["Type"], "Price": t["Price"], "Shares": t["Shares"],
            "GrossTotal": gross, "Fee": t.get("Fee", 0.0), "Slippage": t.get("Slippage", 0.0),
            "NetTotal": gross, "Note": "", "OrderID": "",
        })
    return pd.DataFrame(rows, columns=TRADE_HEADERS_V2)


def _compute_metrics(equity: pd.DataFrame, trades_df: pd.DataFrame, initial_capital: float) -> Dict:
    nav = equity["NAV"].astype("float64")
    out = {
        "initial_capital": round(float(initial_capital), 2),
        "final_nav": round(float(nav.iloc[-1]), 2),
        "start": equity.index[0].date().isoformat(),
        "end": equity.index[-1].date().isoformat(),
        "days": int(len(nav)),
        "n_trades": int(len(trades_df)),
    }
    total_ret = (nav.iloc[-1] / nav.iloc[0] - 1) if nav.iloc[0] > 0 else 0.0
    out["total_return_pct"] = round(total_ret * 100, 2)

    years = max(len(nav) / TRADING_DAYS_PER_YEAR, 1e-9)
    out["cagr_pct"] = round(((nav.iloc[-1] / nav.iloc[0]) ** (1 / years) - 1) * 100, 2) if nav.iloc[0] > 0 else None

    dd = (nav / nav.cummax() - 1.0)
    out["max_drawdown_pct"] = round(float(dd.min()) * 100, 2)

    rets = nav.pct_change().dropna()
    if len(rets) > 1 and rets.std() > 0:
        out["sharpe"] = round(float(rets.mean() / rets.std() * math.sqrt(TRADING_DAYS_PER_YEAR)), 2)
        out["volatility_pct"] = round(float(rets.std() * math.sqrt(TRADING_DAYS_PER_YEAR)) * 100, 2)
        downside = rets[rets < 0]
        out["sortino"] = round(float(rets.mean() / downside.std() * math.sqrt(TRADING_DAYS_PER_YEAR)), 2) if len(downside) > 1 and downside.std() > 0 else None
    else:
        out["sharpe"] = out["volatility_pct"] = out["sortino"] = None

    calmar = (out["cagr_pct"] / abs(out["max_drawdown_pct"])) if out.get("cagr_pct") and out["max_drawdown_pct"] < 0 else None
    out["calmar"] = round(calmar, 2) if calmar is not None else None

    if "MarketValue" in equity.columns:
        exposure = (equity["MarketValue"] / nav.replace(0, np.nan)).clip(upper=1.5)
        out["avg_exposure_pct"] = round(float(exposure.mean()) * 100, 1)

    # 已平倉真實統計（重用實盤 FIFO 統計函式，口徑一致）。
    # split_adjust=False：回測交易已成交在 auto_adjust 還原權值價上，不可再二次分割調整。
    out["realized"] = calc_realized_trade_stats(trades_df, split_adjust=False)
    return out


def _benchmark_metrics(benchmarks: Dict[str, pd.DataFrame], master: pd.DatetimeIndex,
                       initial_capital: float) -> Dict:
    res: Dict = {"_curves": {}}
    for sym, bf in (benchmarks or {}).items():
        try:
            s = bf["Close"].copy()
            s.index = _norm_index(bf.index)
            s = s.reindex(master).ffill().dropna()
            if len(s) < 2 or s.iloc[0] <= 0:
                continue
            idxed = s / s.iloc[0] * initial_capital
            dd = (idxed / idxed.cummax() - 1.0)
            years = max(len(s) / TRADING_DAYS_PER_YEAR, 1e-9)
            rets = s.pct_change().dropna()
            res[sym] = {
                "total_return_pct": round((s.iloc[-1] / s.iloc[0] - 1) * 100, 2),
                "cagr_pct": round(((s.iloc[-1] / s.iloc[0]) ** (1 / years) - 1) * 100, 2),
                "max_drawdown_pct": round(float(dd.min()) * 100, 2),
                "sharpe": round(float(rets.mean() / rets.std() * math.sqrt(TRADING_DAYS_PER_YEAR)), 2) if len(rets) > 1 and rets.std() > 0 else None,
            }
            res["_curves"][sym] = idxed
        except Exception:
            continue
    return res


# ────────────────────────────────────────────────────────────────────────────
# 報告輸出
# ────────────────────────────────────────────────────────────────────────────
def format_report(result: Dict) -> str:
    m = result["metrics"]
    r = m.get("realized", {}) or {}
    lines = [
        "═══════════════════════════════════════════════",
        "  回測結果  BACKTEST REPORT",
        "═══════════════════════════════════════════════",
        f"  期間        {m['start']} → {m['end']}  ({m['days']} 交易日)",
        f"  初始本金    ${m['initial_capital']:,.0f}",
        f"  期末 NAV    ${m['final_nav']:,.0f}",
        "  ───────────────────────────────────────────",
        f"  總報酬      {m['total_return_pct']:+.2f}%",
        f"  CAGR        {_fmt(m.get('cagr_pct'))}%",
        f"  最大回撤    {m['max_drawdown_pct']:.2f}%",
        f"  Sharpe      {_fmt(m.get('sharpe'))}",
        f"  Sortino     {_fmt(m.get('sortino'))}",
        f"  Calmar      {_fmt(m.get('calmar'))}",
        f"  年化波動    {_fmt(m.get('volatility_pct'))}%",
        f"  平均曝險    {_fmt(m.get('avg_exposure_pct'))}%",
        "  ───────────────────────────────────────────",
        f"  財報封鎖    {'啟用' if m.get('earnings_gate') else '停用（回測預設：以執行日財報判斷會污染歷史；live 會迴避，故進場面略偏樂觀）'}",
        f"  總成交筆數  {m['n_trades']}",
        f"  已平倉筆數  {r.get('closed_trades', 0)}",
        f"  真實勝率    {_fmt(r.get('win_rate'))}%",
        f"  獲利因子    {_fmt(r.get('profit_factor'))}",
        f"  盈虧比      {_fmt(r.get('payoff_ratio'))}",
        f"  每筆期望值  ${_fmt(r.get('expectancy'))}",
        f"  淨已實現    ${_fmt(r.get('net_realized'))}",
    ]
    bench = m.get("benchmarks", {})
    bench_syms = [k for k in bench.keys() if not k.startswith("_")]
    if bench_syms:
        lines.append("  ───────────────────────────────────────────")
        lines.append("  買進持有對標（同期）")
        strat_cagr = m.get("cagr_pct")
        for sym in bench_syms:
            b = bench[sym]
            edge = ""
            if strat_cagr is not None and b.get("cagr_pct") is not None:
                edge = f"   (策略 CAGR 差 {strat_cagr - b['cagr_pct']:+.2f}pp)"
            lines.append(f"  {sym:6s}  報酬 {b['total_return_pct']:+.2f}%  "
                         f"CAGR {_fmt(b.get('cagr_pct'))}%  MaxDD {b['max_drawdown_pct']:.2f}%  "
                         f"Sharpe {_fmt(b.get('sharpe'))}{edge}")
    lines.append("═══════════════════════════════════════════════")
    return "\n".join(lines)


def _fmt(v) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


# ────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────
def _resolve_universe(name: str) -> List[str]:
    key = (name or "semi").strip().lower()
    if key == "semi":
        return list(US_SEMI_UNIVERSE)
    if key == "broad":
        return list(BROAD_UNIVERSE_DEFAULT)
    raise SystemExit(f"未知宇宙：{name}（可用 semi / broad，或用 --tickers 指定）")


def main():
    # Windows 主控台預設 cp1252，報表含中文/框線字元會 UnicodeEncodeError → 強制 UTF-8
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    ap = argparse.ArgumentParser(description="事件驅動回測（重放 evaluate_strategy）")
    ap.add_argument("--universe", default="semi", help="semi | broad（預設 semi）")
    ap.add_argument("--tickers", default=None, help="逗號分隔，覆蓋 --universe")
    ap.add_argument("--start", default=None, help="YYYY-MM-DD（預設全歷史起點）")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD（預設最新）")
    ap.add_argument("--capital", type=float, default=DEFAULT_INITIAL_CAPITAL)
    ap.add_argument("--csv", default=None, help="另存權益曲線 CSV 路徑")
    ap.add_argument("--earnings-gate", action="store_true",
                    help="啟用財報封鎖閘（預設停用；以執行日財報日判斷會污染歷史回放）")
    args = ap.parse_args()

    if args.tickers:
        universe = [normalize_ticker(t) for t in args.tickers.split(",") if t.strip()]
    else:
        universe = _resolve_universe(args.universe)

    print(f"回測宇宙：{len(universe)} 檔  |  本金 ${args.capital:,.0f}  |  下載並回放中 …")
    result = run_backtest(
        tickers=universe, start=args.start, end=args.end,
        initial_capital=args.capital, progress=True,
        earnings_gate=args.earnings_gate,
    )
    print()
    print(format_report(result))

    if args.csv:
        eq = result["equity_curve"].copy()
        for sym, curve in result.get("benchmark_curves", {}).items():
            eq[f"BH_{sym}"] = curve.reindex(eq.index)
        eq.to_csv(args.csv, encoding="utf-8-sig")
        print(f"\n權益曲線已另存：{args.csv}")


if __name__ == "__main__":
    main()

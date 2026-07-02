"""backtest.py：引擎正確性、因果性（無未來函數）、benchmarks=None 回歸、零網路。"""
import pandas as pd
import pytest

import backtest as bt
import core


def test_engine_runs_and_profits_in_uptrend(offline_core, synth_market):
    data, regime, bench = synth_market
    res = bt.run_backtest(initial_capital=32000.0, data=data,
                          regime_frames=regime, benchmarks=bench)
    m = res["metrics"]
    assert m["days"] > 100
    assert m["n_trades"] > 0, "乾淨上升趨勢應有成交"
    assert m["final_nav"] > m["initial_capital"], "上升趨勢下 NAV 應成長"
    assert m["max_drawdown_pct"] <= 0
    assert "SOXX" in m["benchmarks"] and "SPY" in m["benchmarks"]
    assert "win_rate" in m["realized"]


def test_causality_truncated_equals_full(offline_core, synth_market):
    """把回測截到中段，共同日的 NAV 必須與全期完全一致 → 證明無未來函數洩漏。"""
    data, regime, bench = synth_market
    full = bt.run_backtest(initial_capital=32000.0, data=data,
                           regime_frames=regime, benchmarks=bench)
    eq_full = full["equity_curve"]["NAV"]
    mid = eq_full.index[len(eq_full) // 2]
    trunc = bt.run_backtest(initial_capital=32000.0, end=str(mid.date()), data=data,
                            regime_frames=regime, benchmarks=bench)
    eq_trunc = trunc["equity_curve"]["NAV"]
    common = eq_trunc.index.intersection(eq_full.index)
    assert len(common) > 50
    assert (eq_full.loc[common] - eq_trunc.loc[common]).abs().max() < 1e-6


def test_benchmarks_none_regression(offline_core, synth_market):
    """回歸：benchmarks=None 走抓取分支；regime dict 內含 DataFrame，
    過去 `regime_frames.get(s) or ...` 會拋 truth-value-ambiguous。"""
    data, regime, _ = synth_market
    regime = dict(regime)
    regime["SOXX"] = data["NVDA"]          # 讓 .get("SOXX") 回傳 DataFrame
    res = bt.run_backtest(initial_capital=32000.0, data=data,
                          regime_frames=regime, benchmarks=None)
    assert "SOXX" in res["metrics"]["benchmarks"]
    assert "SPY" in res["metrics"]["benchmarks"]


def test_no_network_when_injected(offline_core, synth_market, monkeypatch):
    """資料注入時，整條回測管線不得觸碰 yfinance（含被 yf_retry 吞掉的呼叫）。"""
    calls = []
    monkeypatch.setattr(core.yf, "Ticker",
                        lambda sym, *a, **k: calls.append(sym) or (_ for _ in ()).throw(RuntimeError))
    data, regime, bench = synth_market
    bt.run_backtest(initial_capital=32000.0, data=data,
                    regime_frames=regime, benchmarks=bench)
    assert calls == [], f"注入回測不應連網，卻呼叫了 yf.Ticker：{calls}"


# ── P1：回測預設停用財報封鎖（以執行日判斷會污染歷史），且結束必還原 ────────
def test_earnings_gate_disabled_by_default_and_restored(offline_core, synth_market, monkeypatch):
    always_blocked = lambda *a, **k: True          # 若閘生效，所有進場都會被擋
    monkeypatch.setattr(core, "is_earnings_blocked", always_blocked)

    data, regime, bench = synth_market
    res = bt.run_backtest(initial_capital=32000.0, data=data,
                          regime_frames=regime, benchmarks=bench)
    assert res["metrics"]["earnings_gate"] is False
    assert res["metrics"]["n_trades"] > 0, "預設應繞過財報閘 → 上升趨勢中必有成交"
    assert core.is_earnings_blocked is always_blocked, "回測結束必須還原原函式"


def test_earnings_gate_true_honors_block(offline_core, synth_market, monkeypatch):
    monkeypatch.setattr(core, "is_earnings_blocked", lambda *a, **k: True)

    data, regime, bench = synth_market
    res = bt.run_backtest(initial_capital=32000.0, data=data,
                          regime_frames=regime, benchmarks=bench, earnings_gate=True)
    assert res["metrics"]["earnings_gate"] is True
    assert res["metrics"]["n_trades"] == 0, "閘啟用且全數封鎖 → 不應有任何進場"


def test_sizing_respects_cash_reserve(offline_core, synth_market):
    """任一時點現金不得低於「初始資金 × 現金準備比例」以下太多（買進閘生效）。"""
    data, regime, bench = synth_market
    res = bt.run_backtest(initial_capital=32000.0, data=data,
                          regime_frames=regime, benchmarks=bench)
    eq = res["equity_curve"]
    reserve = 32000.0 * core.CASH_RESERVE_PCT
    # 允許單筆成交的粒度誤差（一股價格 + 費用）
    assert eq["Cash"].min() > reserve - 1500, "現金準備閘未生效"

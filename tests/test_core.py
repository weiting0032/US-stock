"""core.py：OBV 向量化等價、regime 門檻、split_adjust 旗標、設定解析、yf_retry。"""
import numpy as np
import pandas as pd
import pytest

import core


# ── OBV 向量化：與舊逐日迴圈完全等價 ────────────────────────────────────────
def _obv_reference_loop(close, volume):
    obv = [0.0]
    for i in range(1, len(close)):
        if close[i] > close[i - 1]:
            obv.append(obv[-1] + volume[i])
        elif close[i] < close[i - 1]:
            obv.append(obv[-1] - volume[i])
        else:
            obv.append(obv[-1])
    return obv


def test_obv_vectorized_matches_reference(monkeypatch):
    n = 300
    rng = np.random.default_rng(7)
    idx = pd.bdate_range("2024-01-02", periods=n)
    close = 100 * np.exp(np.cumsum(0.001 + 0.02 * rng.standard_normal(n)))
    close[50] = close[49]                       # 刻意做一根「平盤」驗證不變分支
    vol = rng.integers(1e5, 5e6, n).astype(float)
    ohlcv = pd.DataFrame({"Open": close, "High": close * 1.01, "Low": close * 0.99,
                          "Close": close, "Volume": vol}, index=idx)

    class FakeTicker:
        def __init__(self, sym, *a, **k):
            self.sym = sym

        def history(self, period=None, auto_adjust=True):
            return ohlcv.copy()

    monkeypatch.setattr(core.yf, "Ticker", FakeTicker)
    core.get_unified_analysis.cache_clear()
    core._get_benchmark_close.cache_clear()

    df = core.get_unified_analysis("FAKE")
    assert df is not None and not df.empty
    ref = pd.Series(_obv_reference_loop(close.tolist(), vol.tolist()), index=idx)
    got = df["OBV"]
    aligned = ref.reindex(got.index)
    assert np.allclose(got.to_numpy(), aligned.to_numpy()), "OBV 向量化與迴圈版不等價"


# ── regime 門檻（單一真相來源 _regime_from_indicator_rows）──────────────────
def _row(close, sma50, sma200, macd):
    return pd.Series({"Close": close, "SMA50": sma50, "SMA200": sma200, "MACD_Hist": macd})


def test_regime_risk_on_off_neutral_unknown():
    bull = _row(110, 105, 100, 1.0)
    bear = _row(90, 95, 100, -1.0)
    vix_low = pd.Series({"Close": 15.0})
    vix_high = pd.Series({"Close": 30.0})

    r = core._regime_from_indicator_rows(bull, bull, vix_low)
    assert r["regime"] == "RISK_ON" and r["risk_multiplier"] == 1.0

    r = core._regime_from_indicator_rows(bear, bear, vix_low)
    assert r["regime"] == "RISK_OFF" and not r["allow_new_position"]

    # 分數高但 VIX >= 25 → 不得 RISK_ON
    r = core._regime_from_indicator_rows(bull, bull, vix_high)
    assert r["regime"] != "RISK_ON"

    # P4 fail-closed：資料異常時不得放行新倉/加碼（過去 fail-open 是風控漏洞）
    r = core._regime_from_indicator_rows(None, None, None)
    assert r["regime"] == "UNKNOWN"
    assert r["allow_new_position"] is False
    assert r["allow_add_position"] is False
    assert r["risk_multiplier"] <= 0.25


# ── calc_realized_trade_stats：split_adjust 旗標（回測防二次分割調整）────────
def test_realized_stats_split_adjust_flag(monkeypatch):
    monkeypatch.setattr(core, "_get_splits",
                        lambda tk: ((pd.Timestamp("2024-06-10"), 10.0),))
    trades = pd.DataFrame([
        {"TradeDateTime": pd.Timestamp("2024-06-01"), "Ticker": "NVDA", "Type": "BUY",
         "Price": 1000.0, "Shares": 1.0, "Fee": 0.0, "Slippage": 0.0},
        {"TradeDateTime": pd.Timestamp("2024-06-20"), "Ticker": "NVDA", "Type": "SELL",
         "Price": 110.0, "Shares": 10.0, "Fee": 0.0, "Slippage": 0.0},
    ])
    adj = core.calc_realized_trade_stats(trades, split_adjust=True)
    raw = core.calc_realized_trade_stats(trades, split_adjust=False)
    # 名目交易 + 分割對齊：買 1@1000 → 10@100，賣 10@110 → 賺 100、勝率 100%
    assert adj["net_realized"] == pytest.approx(100.0)
    assert adj["win_rate"] == 100.0
    # 回測交易已在還原權值空間；若強行再調整，結果必然不同（此即禁用旗標的原因）
    assert raw["net_realized"] != adj["net_realized"]


# ── 設定解析：環境變數 → st.secrets → 預設值 ────────────────────────────────
def test_config_resolution_env_first(monkeypatch):
    monkeypatch.setenv("EXIT_INIT_STOP_ATR", "2.7")
    assert core.get_env_float("EXIT_INIT_STOP_ATR", 2.0) == 2.7


def test_config_resolution_secrets_fallback(monkeypatch):
    monkeypatch.delenv("EXIT_INIT_STOP_ATR", raising=False)

    class FakeSt:
        secrets = {"EXIT_INIT_STOP_ATR": "2.5", "EXIT_MIN_HOLD_BARS": "3"}

    monkeypatch.setattr(core, "st", FakeSt())
    core._secrets_probe_ok = None
    assert core.get_env_float("EXIT_INIT_STOP_ATR", 2.0) == 2.5
    assert core.get_env_int("EXIT_MIN_HOLD_BARS", 1) == 3
    assert core.get_env_float("NOT_THERE", 9.9) == 9.9


def test_config_resolution_secrets_exception_memoized(monkeypatch):
    monkeypatch.delenv("EXIT_INIT_STOP_ATR", raising=False)

    class RaisingSecrets:
        def __contains__(self, k):
            raise RuntimeError("no secrets file")

    class RaisingSt:
        secrets = RaisingSecrets()

    monkeypatch.setattr(core, "st", RaisingSt())
    core._secrets_probe_ok = None
    assert core.get_env_float("EXIT_INIT_STOP_ATR", 2.0) == 2.0
    assert core._secrets_probe_ok is False, "探測失敗應被記住，不再重試"


# ── yf_retry：重試後成功清除登記；耗盡則登記 fetch_failures ──────────────────
def test_yf_retry_records_and_clears_failures(monkeypatch):
    monkeypatch.setattr(core, "YF_RETRY_BASE_SLEEP", 0.0)

    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RuntimeError("rate limited")
        return "ok"

    assert core.yf_retry(flaky, "TEST1", retries=3) == "ok"
    assert "TEST1" not in core.get_fetch_failures()

    def always_fail():
        raise RuntimeError("dead")

    assert core.yf_retry(always_fail, "TEST2", retries=2) is None
    assert "TEST2" in core.get_fetch_failures()
    assert "dead" in core.get_fetch_failures()["TEST2"]

    core._clear_fetch_failure("TEST2")
    assert "TEST2" not in core.get_fetch_failures()


# ── P3：訊號成效的超額報酬（vs benchmark）────────────────────────────────────
def _signal_frames():
    """確定性合成：FAST 日漲 0.3%、HUGGER 與對標同速 0.1%、SOXX 對標 0.1%。"""
    from tests.conftest import make_stock
    return {
        "FAST": make_stock(seed=1, drift=0.003, noise=0.0),
        "HUGGER": make_stock(seed=2, drift=0.001, noise=0.0),
        "SOXX": make_stock(seed=3, drift=0.001, noise=0.0),
    }


def _signals_df():
    dt = pd.Timestamp("2024-06-03 09:00:00")     # 遠早於 now-lookahead → 已成熟
    rows = []
    for tk in ("FAST", "HUGGER"):
        rows.append({"DateTime": dt, "Ticker": tk, "Action": "BUY_NOW",
                     "StrategyMode": "TEST", "Source": "SEMI", "Score": 5.0,
                     "Close": 100.0, "StopLoss": 0.0})
    return pd.DataFrame(rows)


def test_signal_outcomes_excess_return_columns(monkeypatch):
    frames = _signal_frames()
    monkeypatch.setattr(core, "load_signals", _signals_df)
    monkeypatch.setattr(core, "get_unified_analysis",
                        lambda s, *a, **k: frames.get(core.normalize_ticker(s)))

    out = core.evaluate_signal_outcomes(lookahead_days=20, benchmark="SOXX")
    assert set(["BenchRetPct", "ExcessRetPct"]) <= set(out.columns)
    assert len(out) == 2

    fast = out[out["Ticker"] == "FAST"].iloc[0]
    hug = out[out["Ticker"] == "HUGGER"].iloc[0]
    # FAST 日漲 0.3% vs 對標 0.1%：20 交易日超額約 +4pp
    assert 2.0 < fast["ExcessRetPct"] < 8.0, f"FAST 超額異常：{fast['ExcessRetPct']}"
    # HUGGER 與對標同速：超額應近 0
    assert abs(hug["ExcessRetPct"]) < 0.8, f"HUGGER 超額應近 0：{hug['ExcessRetPct']}"


def test_summarize_edge_uses_excess_as_primary(monkeypatch):
    frames = _signal_frames()
    monkeypatch.setattr(core, "load_signals", _signals_df)
    monkeypatch.setattr(core, "get_unified_analysis",
                        lambda s, *a, **k: frames.get(core.normalize_ticker(s)))

    tbl = core.summarize_signal_edge(lookahead_days=20, benchmark="SOXX")
    assert not tbl.empty
    assert any("超額" in c for c in tbl.columns), f"缺超額欄位：{list(tbl.columns)}"
    assert any("原始" in c for c in tbl.columns), "原始欄位應保留供對照"
    row = tbl[tbl["分數區間"] == "4.5–5.5"].iloc[0]
    assert row["樣本數"] == 2
    assert row["超額20日報酬%"] > 1.0        # (4pp + 0pp)/2 ≈ +2pp

    # benchmark=None → 無超額欄位，僅原始（優雅退化）
    tbl2 = core.summarize_signal_edge(lookahead_days=20, benchmark=None)
    assert not any("超額" in c for c in tbl2.columns)

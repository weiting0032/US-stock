"""optimize.py：override_config 安全性、grid_search、walk_forward 分割。"""
import pytest

import core
import optimize as opt


def test_override_config_apply_and_restore():
    orig = core.EXIT_INIT_STOP_ATR
    with opt.override_config(EXIT_INIT_STOP_ATR=9.9):
        assert core.EXIT_INIT_STOP_ATR == 9.9
    assert core.EXIT_INIT_STOP_ATR == orig


def test_override_config_unknown_key_raises():
    with pytest.raises(KeyError):
        with opt.override_config(NOT_A_REAL_PARAM=1):
            pass


def test_override_config_restores_on_exception():
    orig = core.EXIT_INIT_STOP_ATR
    with pytest.raises(RuntimeError):
        with opt.override_config(EXIT_INIT_STOP_ATR=5.0):
            raise RuntimeError("boom")
    assert core.EXIT_INIT_STOP_ATR == orig


def test_grid_search_rows_order_and_effect(offline_core, synth_market):
    data, regime, bench = synth_market
    grid = {"EXIT_INIT_STOP_ATR": [1.5, 2.0, 3.0]}
    tbl = opt.grid_search(grid, data, regime, bench,
                          initial_capital=32000.0, rank_by="calmar")
    assert len(tbl) == 3
    assert {"EXIT_INIT_STOP_ATR", "calmar", "vs_SOXX_pp"} <= set(tbl.columns)
    cal = tbl["calmar"].dropna().tolist()
    assert cal == sorted(cal, reverse=True), "應依 rank_by 由優到劣"
    assert tbl["total_return_pct"].nunique() >= 2, "覆蓋參數未影響結果（管線未套用？）"
    assert core.EXIT_INIT_STOP_ATR == 2.0, "掃描後全域未還原"


def test_walk_forward_split_and_champion(offline_core, synth_market):
    data, regime, bench = synth_market
    grid = {"EXIT_INIT_STOP_ATR": [1.5, 3.0], "EXIT_TRAIL_ATR": [2.5, 3.5]}
    wf = opt.walk_forward(grid, data, regime, bench, initial_capital=32000.0,
                          train_frac=0.6, rank_by="calmar")
    (tr0, tr1), (te0, te1) = wf["split"]["train"], wf["split"]["test"]
    assert tr0 <= tr1 < te0 <= te1, "train/test 應不重疊且有序"
    assert len(wf["train_table"]) == 4 and len(wf["test_table"]) == 4
    assert set(wf["best_params"]) == set(grid)
    assert wf["best_test"] is not None, "冠軍應能在測試段找到對應列"
    report = opt.format_walk_forward(wf, list(grid))
    assert "WALK-FORWARD" in report and "樣本外" in report


# ── P10：rolling walk-forward——跨折排名穩定度判準 ────────────────────────────
def test_rolling_walk_forward_folds_and_stability(offline_core, synth_market):
    data, regime, bench = synth_market
    grid = {"EXIT_INIT_STOP_ATR": [1.5, 3.0]}
    res = opt.rolling_walk_forward(grid, data, regime, bench, initial_capital=32000.0,
                                   n_folds=2, rank_by="calmar")
    assert res["n_folds"] == 2 and len(res["folds"]) == 2
    f1, f2 = res["folds"]
    assert f1["test"][1] < f2["test"][0], "各折樣本外窗不得重疊"
    s = res["summary"]
    assert len(s) == 2, "每組參數應有一列彙總"
    assert set(s["折數"]) == {2}, "每組參數在每折都應被評估"
    assert {"排名中位數", "排名最差"} <= set(s.columns)
    assert set(res["best_params"]) == {"EXIT_INIT_STOP_ATR"}
    txt = opt.format_rolling(res, list(grid))
    assert "ROLLING WALK-FORWARD" in txt and "排名穩定度" in txt


def test_prepare_data_benchmarks_none_regression(offline_core, synth_market, monkeypatch):
    """回歸：prepare_data 的 `regime.get(s) or ...` DataFrame 布林值問題。"""
    from tests.conftest import make_regime, make_stock, make_vix
    frames = {"SPY": make_regime(), "QQQ": make_regime(420), "^VIX": make_vix(),
              "SOXX": make_stock(1), "NVDA": make_stock(1), "AMD": make_stock(2)}
    fake = lambda s, *a, **k: frames.get(core.normalize_ticker(s))
    monkeypatch.setattr(core, "get_unified_analysis", fake)
    monkeypatch.setattr(opt, "get_unified_analysis", fake)   # optimize 以 from-import 綁定
    data, regime, bench = opt.prepare_data(["NVDA", "AMD"])
    assert "SOXX" in bench and "SPY" in bench
    assert set(data) == {"NVDA", "AMD"}

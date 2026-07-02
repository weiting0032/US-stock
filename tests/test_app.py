"""app.py（AppTest 無頭執行）：分頁渲染、長任務兩段式＋資料重用、參數套用/還原。"""
import pandas as pd
import pytest

import core
import optimize as opt
from tests.conftest import APP_PATH, make_regime, make_stock, make_vix

from streamlit.testing.v1 import AppTest


def _synth_bundle():
    data = {"NVDA": make_stock(1), "AMD": make_stock(2)}
    regime = {"SPY": make_regime(), "QQQ": make_regime(420), "^VIX": make_vix()}
    bench = {"SOXX": make_stock(1)[["Close"]], "SPY": make_regime()[["Close"]]}
    return data, regime, bench


def test_all_tabs_render_without_exception(offline_app):
    at = AppTest.from_file(APP_PATH, default_timeout=120)
    at.run()
    assert not at.exception, [str(e) for e in at.exception]
    labels = [t.label for t in at.tabs]
    assert len(at.tabs) == 8
    assert any("回測" in (l or "") for l in labels)
    btns = [b.label for b in at.button]
    assert any("執行回測" in (l or "") for l in btns)
    assert any("walk-forward" in (l or "") for l in btns)


def test_backtest_two_phase_and_data_reuse(offline_app, monkeypatch):
    """點「執行回測」→ 自動暫停刷新、兩段式完成、資料快取；
    接著 walk-forward 重用資料（prepare_data 全程只呼叫一次）。"""
    calls = {"n": 0}
    data, regime, bench = _synth_bundle()

    def fake_prepare(universe, progress=False):
        calls["n"] += 1
        return dict(data), dict(regime), dict(bench)

    monkeypatch.setattr(opt, "prepare_data", fake_prepare)

    at = AppTest.from_file(APP_PATH, default_timeout=300)
    at.run()
    at.radio(key="bt_uni_choice").set_value("自訂代碼")
    at.run()
    at.text_input(key="bt_custom").set_value("NVDA,AMD")
    at.run()

    at.button(key="run_bt").click()
    at.run()
    assert not at.exception, [str(e) for e in at.exception]
    assert at.session_state["pause_refresh"] is True, "執行時應自動暫停自動刷新"
    assert at.session_state["bt_result"] is not None
    assert calls["n"] == 1

    at.button(key="run_wf").click()
    at.run()
    assert not at.exception, [str(e) for e in at.exception]
    assert at.session_state["wf_result"] is not None
    assert calls["n"] == 1, "walk-forward 應重用 bt_prepared，不重新下載"


def test_apply_and_reset_session_params(offline_app, monkeypatch):
    """套用冠軍參數 → core 全域改變＋橫幅；還原 → 回到原值。（人工核可流程）"""
    data, regime, bench = _synth_bundle()
    wf = opt.walk_forward({"EXIT_INIT_STOP_ATR": [1.5, 3.0]}, data, regime, bench,
                          initial_capital=32000.0, train_frac=0.6, rank_by="calmar")
    assert wf["best_params"] is not None
    champion = float(wf["best_params"]["EXIT_INIT_STOP_ATR"])
    default_val = core.EXIT_INIT_STOP_ATR
    assert champion != default_val, "測試前提：冠軍值需異於預設，否則驗不出套用"

    at = AppTest.from_file(APP_PATH, default_timeout=120)
    at.session_state["wf_result"] = wf
    at.session_state["wf_keys"] = ["EXIT_INIT_STOP_ATR"]
    at.session_state["wf_rank_by"] = "calmar"
    at.run()
    assert not at.exception, [str(e) for e in at.exception]

    # 套用（僅本工作階段）
    at.button(key="apply_wf").click()
    at.run()
    assert not at.exception, [str(e) for e in at.exception]
    assert at.session_state["applied_params"]["EXIT_INIT_STOP_ATR"] == champion
    assert core.EXIT_INIT_STOP_ATR == champion, "套用後 core 全域應立即反映"

    # 還原預設
    at.button(key="reset_applied").click()
    at.run()
    assert not at.exception, [str(e) for e in at.exception]
    assert "applied_params" not in at.session_state or not at.session_state["applied_params"]
    assert core.EXIT_INIT_STOP_ATR == default_val, "還原後應回到原值"

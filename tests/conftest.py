"""共用測試設施：合成行情、離線 patch、策略參數快照保護。

原則：所有測試 100% 離線（合成資料 + monkeypatch 掉 yfinance / Google Sheets），
CI 上可直接跑，不受限流/網路影響。
"""
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
import pytest

import core

N_BARS = 520
IDX = pd.bdate_range("2023-01-02", periods=N_BARS)

# 保護的策略參數前綴：測試（如 App 套用流程）可能 setattr core 全域，跑完必須還原
_PARAM_PREFIXES = ("EXIT_", "ENTRY_", "SCORE_", "TIME_STOP", "ADD_", "US_SEMI_SCORE",
                   "CASH_RESERVE", "COOLDOWN", "YF_MAX_RETRIES", "YF_RETRY_BASE_SLEEP")


@pytest.fixture(autouse=True)
def _restore_core_params():
    keys = [k for k in dir(core) if k.startswith(_PARAM_PREFIXES)]
    snap = {k: getattr(core, k) for k in keys}
    probe = core._secrets_probe_ok
    yield
    for k, v in snap.items():
        setattr(core, k, v)
    core._secrets_probe_ok = probe


def make_stock(seed: int = 0, drift: float = 0.0015, noise: float = 0.004,
               start: float = 100.0) -> pd.DataFrame:
    """乾淨上升趨勢＋雜訊的合成指標 DataFrame（欄位相容 get_unified_analysis 輸出）。"""
    rng = np.random.default_rng(seed)
    c = start * np.exp(np.cumsum(drift + noise * rng.standard_normal(N_BARS)))
    d = pd.DataFrame(index=IDX)
    for col, val in [("Open", c), ("High", c * 1.008), ("Low", c * 0.992),
                     ("Close", c), ("Volume", 1e6)]:
        d[col] = val
    d["SMA20"] = pd.Series(c, index=IDX).rolling(20).mean()
    d["SMA50"] = pd.Series(c, index=IDX).rolling(50).mean()
    d["SMA200"] = pd.Series(c, index=IDX).rolling(200).mean()
    d["ATR"] = c * 0.02
    d["RSI"] = 60.0
    d["MACD_Hist"] = 0.5
    d["ADX"] = 25.0
    d["BB_Width"] = 0.10
    d["VOL_SMA20"] = 1e6
    d["RollingHigh20"] = d["High"].rolling(20).max()
    d["RollingHigh252"] = d["High"].rolling(252).max()
    d["DollarVolume20"] = (d["Close"] * d["Volume"]).rolling(20).mean()
    d["OBV_Slope20"] = 1.0
    d["RS20_vs_SPY"] = 3.0
    return d.dropna()


def make_regime(start: float = 400.0) -> pd.DataFrame:
    c = np.linspace(start, start * 1.4, N_BARS)
    d = pd.DataFrame(index=IDX)
    d["Close"] = c
    d["SMA50"] = pd.Series(c, index=IDX).rolling(50).mean()
    d["SMA200"] = pd.Series(c, index=IDX).rolling(200).mean()
    d["MACD_Hist"] = 0.5
    return d.dropna()


def make_vix(level: float = 15.0) -> pd.DataFrame:
    d = pd.DataFrame(index=IDX)
    d["Close"] = level
    d["SMA50"] = level
    d["SMA200"] = level
    d["MACD_Hist"] = 0.0
    return d.dropna()


@pytest.fixture()
def offline_core(monkeypatch):
    """切斷 core 的外部相依（earnings / 市值 / profile），供回測/最佳化離線執行。"""
    monkeypatch.setattr(core, "is_earnings_blocked", lambda *a, **k: False)
    monkeypatch.setattr(core, "classify_symbol_bucket", lambda *a, **k: "LARGE_CAP")
    monkeypatch.setattr(core, "get_symbol_market_cap", lambda *a, **k: 5e10)
    monkeypatch.setattr(core, "get_symbol_profile",
                        lambda *a, **k: {"market_cap": 5e10, "sector": "Tech", "industry": "Semis"})
    return core


@pytest.fixture()
def synth_market():
    """(data, regime_frames, benchmarks) 三元組，直接注入 run_backtest / walk_forward。"""
    data = {"NVDA": make_stock(1), "AMD": make_stock(2), "AVGO": make_stock(3)}
    regime = {"SPY": make_regime(), "QQQ": make_regime(420), "^VIX": make_vix()}
    bench = {"SOXX": make_stock(1)[["Close"]], "SPY": make_regime()[["Close"]]}
    return data, regime, bench


@pytest.fixture()
def offline_app(monkeypatch, offline_core):
    """AppTest 用：再切斷 Sheets / 行情 / regime，讓 app.py 空狀態可離線渲染。"""
    monkeypatch.setattr(core, "get_market_regime",
                        lambda: {"regime": "UNKNOWN", "score": 0, "allow_new_position": True,
                                 "allow_add_position": True, "risk_multiplier": 0.5, "vix": None})
    monkeypatch.setattr(core, "get_market_session", lambda: "REGULAR")
    monkeypatch.setattr(core, "get_unified_analysis", lambda *a, **k: None)
    monkeypatch.setattr(core, "get_last_price", lambda *a, **k: None)
    monkeypatch.setattr(core, "load_trades", lambda: pd.DataFrame())
    monkeypatch.setattr(core, "load_watchlist", lambda: pd.DataFrame())
    monkeypatch.setattr(core, "load_history", lambda: pd.DataFrame())
    monkeypatch.setattr(core, "load_alerts", lambda: pd.DataFrame())
    return core


APP_PATH = str(ROOT / "app.py")

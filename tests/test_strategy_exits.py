"""P2/P5/P6：Chandelier ratchet、相關性縮量＋熱度增量閘、保本緩衝。

用可控合成 K 線直接呼叫 evaluate_strategy，驗證出場/部位邏輯的因果行為。
"""
import numpy as np
import pandas as pd
import pytest

import core
from tests.conftest import make_stock

REGIME_ON = {"regime": "RISK_ON", "score": 5, "allow_new_position": True,
             "allow_add_position": True, "risk_multiplier": 1.0, "vix": 15.0}


def _frame(close, high, atr, *, sma20=100.0, sma50=95.0, sma200=80.0):
    """自訂 K 線 → evaluate_strategy/rank_symbol_strength 所需的完整欄位。"""
    n = len(close)
    idx = pd.bdate_range("2024-01-02", periods=n)
    df = pd.DataFrame({"Close": close, "High": high, "ATR": atr}, index=idx)
    df["Open"] = df["Close"]
    df["Low"] = df["Close"] * 0.99
    df["Volume"] = 1e6
    df["SMA20"] = sma20
    df["SMA50"] = sma50
    df["SMA200"] = sma200
    df["RSI"] = 55.0
    df["MACD_Hist"] = 0.1
    df["ADX"] = 15.0
    df["BB_Width"] = 0.10
    df["VOL_SMA20"] = 1e6
    df["DollarVolume20"] = 5e7
    df["OBV_Slope20"] = 0.0
    df["RS20_vs_SPY"] = 1.0
    df["RollingHigh252"] = 500.0            # 遠高於價格 → 不觸發 52 週高加分
    df["RollingHigh20"] = df["High"].rolling(20, min_periods=1).max()
    return df


def _held_portfolio(hist, entry_pos, shares=10.0, avg_cost=100.0):
    entry_ts = hist.index[entry_pos]
    mv = shares * float(hist["Close"].iloc[-1])
    return [{"Ticker": "TESTX", "Shares": shares, "AvgCost": avg_cost,
             "EntryDate": entry_ts, "EntryShares": shares, "MarketValue": mv}], entry_ts, mv


# ── P2：ATR 暴增時 Chandelier 不得跨日下移（ratchet 只升不降）────────────────
def test_chandelier_ratchets_when_atr_expands(offline_core):
    n = 80
    close = np.full(n, 100.0)
    high = np.full(n, 100.0)
    atr = np.full(n, 2.0)
    # 進場於 pos 50；A 段(50..69)：漲到 130、ATR 維持 2 → ratchet 高點 = 131 − 3×2 = 125
    entry_pos = 50
    for i, p in enumerate(np.linspace(101, 130, 20)):
        close[entry_pos + i] = p
        high[entry_pos + i] = p + 1.0
    # B 段(70..79)：急跌到 112、ATR 擴張到 8 → 舊點式 = 131 − 3×8 = 107（比昨天鬆！）
    for i, (p, a) in enumerate(zip(np.linspace(128, 112, 10), np.linspace(3.0, 8.0, 10))):
        close[70 + i] = p
        high[70 + i] = p + 0.5
        atr[70 + i] = a

    hist = _frame(close, high, atr)
    portfolio, entry_ts, mv = _held_portfolio(hist, entry_pos)

    sc, act, det, _ = core.evaluate_strategy(
        "TESTX", hist, 10.0, mv, 32000.0, 20000.0, REGIME_ON, 0.0, portfolio)

    old_point_formula = 131.0 - 3.0 * 8.0          # 107：修正前的（會下移的）停損
    assert det["trend_stop"] >= 124.0, f"ratchet 應鎖在 ~125，得 {det['trend_stop']}"
    assert det["trend_stop"] > old_point_formula + 10, "修正後停損必須顯著高於舊點式"
    assert act == "SELL_EXIT" and det["strategy_mode"] == "TRAIL_EXIT", \
        "收盤 112 已低於 ratchet 停損 → 應觸發移動停損出場"


# ── P5a：相關性縮量係數與 sizing ────────────────────────────────────────────
def test_corr_risk_scale_function():
    assert core.corr_risk_scale(None) == 1.0
    assert core.corr_risk_scale(0.2) == 1.0                       # ≤0.3 不縮
    assert abs(core.corr_risk_scale(0.7) - 0.6) < 1e-9
    assert core.corr_risk_scale(0.95) == core.CORR_RISK_SCALE_FLOOR   # 觸底
    try:
        core.CORR_RISK_SCALE_ENABLE = 0
        assert core.corr_risk_scale(0.9) == 1.0                   # 停用開關
    finally:
        core.CORR_RISK_SCALE_ENABLE = 1


def test_high_corr_scales_down_buy_qty(offline_core):
    hist = make_stock(seed=1, noise=0.0)          # 乾淨上升趨勢 → BUY_NOW 條件成立
    _, act0, det0, _ = core.evaluate_strategy(
        "TESTX", hist, 0.0, 0.0, 32000.0, 32000.0, REGIME_ON, 0.0, [], avg_corr=None)
    _, act9, det9, _ = core.evaluate_strategy(
        "TESTX", hist, 0.0, 0.0, 32000.0, 32000.0, REGIME_ON, 0.0, [], avg_corr=0.9)

    q0, q9 = det0["suggested_buy_qty"], det9["suggested_buy_qty"]
    assert act0 == "BUY_NOW" and q0 > 0
    assert det9["corr_risk_scale"] == 0.4
    assert 0 < q9 < q0, "高相關應縮小建議股數"
    assert q9 <= int(q0 * 0.45) + 1, f"corr 0.9 → 縮至 ~40%（{q0} → {q9}）"


# ── P5b：熱度閘計入本筆增量 ─────────────────────────────────────────────────
def test_heat_gate_counts_new_position_increment(offline_core):
    hist = make_stock(seed=1, noise=0.0)
    # 既有熱度 4.9%：舊式（heat<5%）會放行 → 事後 ~5.9% 超標；新式必須擋下
    _, act_hi, det_hi, _ = core.evaluate_strategy(
        "TESTX", hist, 0.0, 0.0, 32000.0, 32000.0, REGIME_ON, 4.9, [])
    assert act_hi == "WATCH", "熱度 4.9% + 新倉 ~1% 將超過 5% 上限 → 應擋下"

    # 既有熱度 3.5%：加上 ~1% 後 ≤5% → 放行
    _, act_lo, det_lo, _ = core.evaluate_strategy(
        "TESTX", hist, 0.0, 0.0, 32000.0, 32000.0, REGIME_ON, 3.5, [])
    assert act_lo == "BUY_NOW", "熱度 3.5% + 新倉 ~1% 未超限 → 應放行"


# ── P6：保本線緩衝（預設 0 行為不變；0.25R 可容忍回踩）───────────────────────
def _breakeven_scenario():
    """entry=100、ATR=3（R=6、+1R=106）：漲到 107 觸發保本，再回踩到 99.5。"""
    n = 49
    close = np.full(n, 100.0)
    high = np.full(n, 100.0)
    atr = np.full(n, 3.0)
    entry_pos = 40
    rise = [101, 103, 105, 106.5, 107]                 # peak_close 107 ≥ 106 → reached_1R
    for i, p in enumerate(rise):
        close[41 + i] = p
        high[41 + i] = p
    for i, p in enumerate([103, 101, 99.5]):           # 回踩：終值 99.5
        close[46 + i] = p
        high[46 + i] = p + 0.5
    hist = _frame(close, high, atr)
    portfolio, entry_ts, mv = _held_portfolio(hist, entry_pos)
    return hist, portfolio, mv


def test_breakeven_default_exits_at_cost(offline_core):
    hist, portfolio, mv = _breakeven_scenario()
    # 預設 buffer=0：保本線=100，收盤 99.5 < 100 → BREAKEVEN_EXIT（現行行為不變）
    sc, act, det, _ = core.evaluate_strategy(
        "TESTX", hist, 10.0, mv, 32000.0, 20000.0, REGIME_ON, 0.0, portfolio)
    assert act == "SELL_EXIT" and det["strategy_mode"] == "BREAKEVEN_EXIT"


def test_breakeven_buffer_tolerates_retrace(offline_core, monkeypatch):
    hist, portfolio, mv = _breakeven_scenario()
    monkeypatch.setattr(core, "EXIT_BREAKEVEN_BUFFER_R", 0.25)   # 保本線=100−0.25×6=98.5
    sc, act, det, _ = core.evaluate_strategy(
        "TESTX", hist, 10.0, mv, 32000.0, 20000.0, REGIME_ON, 0.0, portfolio)
    assert act != "SELL_EXIT", f"收盤 99.5 > 緩衝保本線 98.5 → 不應出場（act={act}, mode={det['strategy_mode']}）"


# ── P8：回檔閘＝帶狀區＋反轉確認（不再是接刀閘）─────────────────────────────
def test_pullback_gate_floor_and_confirm(monkeypatch):
    # 帶內＋收漲 → 放行
    assert core.pullback_entry_ok(101.0, 100.0, 100.0, 102.0) is True
    # 接刀：單日深跌破地板（88 < 100×0.96）→ 擋（舊式 88 ≤ 104 會放行！）
    assert core.pullback_entry_ok(88.0, 100.0, 95.0, 96.0) is False
    # 帶內但收跌且未收復昨高 → 無反轉跡象 → 擋
    assert core.pullback_entry_ok(99.0, 100.0, 100.0, 101.0) is False
    # 收復昨高（≥prev_high）即使收跌 → 放行
    assert core.pullback_entry_ok(99.5, 100.0, 100.0, 99.5) is True
    # 天花板恆在：乖離過高不算回檔
    assert core.pullback_entry_ok(105.0, 100.0, 100.0, 106.0) is False
    # 開關：關地板後深跌＋收漲可放行；再關確認後收跌也放行（回到舊行為）
    monkeypatch.setattr(core, "ENTRY_PULLBACK_FLOOR_ENABLE", 0)
    assert core.pullback_entry_ok(88.0, 100.0, 87.0, 96.0) is True
    monkeypatch.setattr(core, "ENTRY_PULLBACK_CONFIRM", 0)
    assert core.pullback_entry_ok(99.0, 100.0, 100.0, 101.0) is True


# ── P11：SOX 板塊軟閘——只縮半導體、只在 BEAR、非半導體不受影響 ────────────────
def test_sox_bear_gate_scales_semi_only(offline_core):
    hist = make_stock(seed=1, noise=0.0)

    def _qty(ticker, sox):
        _, act, det, _ = core.evaluate_strategy(
            ticker, hist, 0.0, 0.0, 32000.0, 32000.0, REGIME_ON, 0.0, [], sox_trend=sox)
        return act, det

    act0, det0 = _qty("NVDA", None)          # 基準（無 SOX 資訊）
    q0 = det0["suggested_buy_qty"]
    assert act0 == "BUY_NOW" and q0 > 0

    act_bear, det_bear = _qty("NVDA", "BEAR")    # NVDA 在 US_SEMI_CATEGORY_MAP → 縮至 50%
    assert det_bear["sox_gate_scale"] == 0.5
    assert abs(det_bear["suggested_buy_qty"] - q0 * 0.5) <= 1

    act_bull, det_bull = _qty("NVDA", "BULL")    # BULL 不縮
    assert det_bull["sox_gate_scale"] == 1.0
    assert det_bull["suggested_buy_qty"] == q0

    act_ns, det_ns = _qty("TESTX", "BEAR")       # 非半導體（不在 map）→ 不受影響
    assert det_ns["sox_gate_scale"] == 1.0
    assert det_ns["suggested_buy_qty"] == q0

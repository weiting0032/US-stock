"""
optimize.py — 策略參數掃描 / walk-forward 最佳化（建在 backtest.py 之上）

用途
────────────────────────────────────────────────────────────────────────
用回測「數據」回答分析報告點出的問題，取代手選參數：
  • 止損 ATR 倍數（EXIT_INIT_STOP_ATR）與移動停損倍數（EXIT_TRAIL_ATR）
  • 純移動停損 vs 分批 TP（EXIT_TP1_R / EXIT_SCALE_OUT_PCT）
  • 新倉保護期天數（EXIT_MIN_HOLD_BARS）與保本觸發（EXIT_BREAKEVEN_AT_R）
  • 進場門檻與追高上限（SCORE_BUY_NOW_THRESHOLD / ENTRY_MAX_EXT_ATR）

做法
────────────────────────────────────────────────────────────────────────
evaluate_strategy 每次呼叫都重讀 core 的模組層級參數，故用 override_config
暫時覆蓋這些全域即可讓整條回測管線套用該組參數（結束後保證還原）。
資料只抓一次、以注入方式重複餵給 run_backtest，避免每組參數重抓。

⚠️ 過擬合警告：yfinance 僅約 2 年日線，樣本少、極易過擬合。故預設用
   train/test 分割：只在「訓練段」選最佳，再看它在「測試段（樣本外）」是否仍穩健。
   只有樣本外仍佳的參數才可信；務必別只看訓練段冠軍。

CLI
────────────────────────────────────────────────────────────────────────
  python optimize.py --study stops                 # 掃止損/移動停損倍數
  python optimize.py --study exits --universe semi  # 純移動停損 vs 分批 TP
  python optimize.py --study grace --tickers NVDA,AMD,AVGO,MRVL,AMAT
  python optimize.py --study all --rank-by calmar --csv sweep.csv
"""
from __future__ import annotations

import argparse
import itertools
import sys
from contextlib import contextmanager
from typing import Dict, List, Optional

import pandas as pd

import core
from backtest import run_backtest
from core import (
    BROAD_UNIVERSE_DEFAULT,
    DEFAULT_INITIAL_CAPITAL,
    US_SEMI_UNIVERSE,
    get_unified_analysis,
    normalize_ticker,
)

# ── 預定義掃描研究（對應分析報告 §5/§6 的具體問題）──────────────────────────
STUDIES: Dict[str, Dict[str, List]] = {
    # §5 止損：初始硬止損與 Chandelier 移動停損的 ATR 倍數
    "stops": {
        "EXIT_INIT_STOP_ATR": [1.5, 2.0, 2.5, 3.0],
        "EXIT_TRAIL_ATR": [2.5, 3.0, 3.5],
    },
    # §6 出場：分批 TP 的位置與比例（含「幾乎純移動停損」= TP1 拉到 4R）
    "exits": {
        "EXIT_TP1_R": [2.0, 3.0, 4.0],
        "EXIT_TP1_PCT": [0.20, 0.35],
        "EXIT_SCALE_OUT_PCT": [0.34, 0.50],
    },
    # §5 保護期與保本（P6：含保本緩衝——避開 +1R~+2R「碰成本就死」的洗盤走廊）
    "grace": {
        "EXIT_MIN_HOLD_BARS": [1, 3, 5],
        "EXIT_BREAKEVEN_AT_R": [1.0, 1.5],
        "EXIT_BREAKEVEN_BUFFER_R": [0.0, 0.25],
    },
    # §2/§5 進場門檻與追高上限
    "entry": {
        "SCORE_BUY_NOW_THRESHOLD": [3.0, 3.5, 4.0],
        "ENTRY_MAX_EXT_ATR": [3.0, 4.0, 6.0],
    },
}

# 純移動停損變體（把 TP 有效關閉：+R 與 %上限都拉到極大 → 永不觸發分批）
PURE_TRAIL_OVERRIDE = {
    "EXIT_TP1_R": 99.0, "EXIT_TP1_PCT": 9.9,
    "EXIT_TP2_R": 99.0, "EXIT_TP2_PCT": 9.9,
}

# 掃描結果表要攤平呈現的指標欄
_METRIC_COLS = ["cagr_pct", "max_drawdown_pct", "calmar", "sharpe", "sortino",
                "total_return_pct", "avg_exposure_pct", "n_trades",
                "win_rate", "profit_factor", "expectancy", "vs_SOXX_pp", "vs_SPY_pp"]


@contextmanager
def override_config(**overrides):
    """暫時覆蓋 core 的模組層級策略參數；離開時保證還原（即使拋錯）。

    只允許覆蓋 core 上已存在的名稱，避免打錯字卻靜默無效。
    """
    unknown = [k for k in overrides if not hasattr(core, k)]
    if unknown:
        raise KeyError(f"未知的策略參數（core 上不存在）：{unknown}")
    old = {k: getattr(core, k) for k in overrides}
    try:
        for k, v in overrides.items():
            setattr(core, k, v)
        yield
    finally:
        for k, v in old.items():
            setattr(core, k, v)


def prepare_data(tickers: List[str], progress: bool = False):
    """一次抓齊：標的指標、regime（SPY/QQQ/^VIX）、對標（SOXX/SPY）。之後注入重用。"""
    tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
    data = {}
    for i, tk in enumerate(tickers):
        if progress and i % 20 == 0:
            print(f"  下載 {i}/{len(tickers)} …")
        df = get_unified_analysis(tk)
        if df is not None and not df.empty:
            data[tk] = df
    regime = {s: get_unified_analysis(s) for s in ("SPY", "QQQ", "^VIX")}
    benchmarks = {}
    for s in ("SOXX", "SPY"):
        bf = regime.get(s)                     # 不可用 `or`：DataFrame 的布林值不明確會拋錯
        if bf is None:
            bf = get_unified_analysis(s)
        if bf is not None and not bf.empty:
            benchmarks[s] = bf
    return data, regime, benchmarks


def _flatten_metrics(combo: Dict, result: Dict) -> Dict:
    """把一組回測結果攤平成單列（含 vs 對標的 CAGR 超額）。"""
    m = result["metrics"]
    r = m.get("realized", {}) or {}
    bench = m.get("benchmarks", {}) or {}
    row = dict(combo)
    row.update({
        "cagr_pct": m.get("cagr_pct"),
        "max_drawdown_pct": m.get("max_drawdown_pct"),
        "calmar": m.get("calmar"),
        "sharpe": m.get("sharpe"),
        "sortino": m.get("sortino"),
        "total_return_pct": m.get("total_return_pct"),
        "avg_exposure_pct": m.get("avg_exposure_pct"),
        "n_trades": m.get("n_trades"),
        "win_rate": r.get("win_rate"),
        "profit_factor": r.get("profit_factor"),
        "expectancy": r.get("expectancy"),
    })
    cagr = m.get("cagr_pct")
    for sym, key in (("SOXX", "vs_SOXX_pp"), ("SPY", "vs_SPY_pp")):
        b = bench.get(sym, {})
        row[key] = round(cagr - b["cagr_pct"], 2) if (cagr is not None and b.get("cagr_pct") is not None) else None
    return row


def grid_search(
    param_grid: Dict[str, List],
    data: Dict, regime_frames: Dict, benchmarks: Dict,
    initial_capital: float = DEFAULT_INITIAL_CAPITAL,
    start: Optional[str] = None, end: Optional[str] = None,
    extra_overrides: Optional[Dict] = None,
    rank_by: str = "calmar",
    progress: bool = False,
) -> pd.DataFrame:
    """對 param_grid 的笛卡兒積逐組回測，回傳依 rank_by 由優到劣排序的結果表。"""
    keys = list(param_grid.keys())
    combos = list(itertools.product(*[param_grid[k] for k in keys]))
    rows = []
    for i, values in enumerate(combos):
        combo = dict(zip(keys, values))
        overrides = dict(combo)
        if extra_overrides:
            overrides.update(extra_overrides)
        if progress:
            print(f"  [{i + 1}/{len(combos)}] {combo}")
        try:
            with override_config(**overrides):
                result = run_backtest(
                    initial_capital=initial_capital, start=start, end=end,
                    data=data, regime_frames=regime_frames, benchmarks=benchmarks,
                )
            rows.append(_flatten_metrics(combo, result))
        except Exception as e:
            row = dict(combo)
            row["error"] = str(e)[:80]
            rows.append(row)

    df = pd.DataFrame(rows)
    if rank_by in df.columns:
        df = df.sort_values(rank_by, ascending=False, na_position="last").reset_index(drop=True)
    return df


def _master_dates(regime_frames: Dict) -> pd.DatetimeIndex:
    spy = regime_frames.get("SPY")
    idx = pd.DatetimeIndex(spy.index)
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    return idx.normalize()


def walk_forward(
    param_grid: Dict[str, List],
    data: Dict, regime_frames: Dict, benchmarks: Dict,
    initial_capital: float = DEFAULT_INITIAL_CAPITAL,
    train_frac: float = 0.6,
    extra_overrides: Optional[Dict] = None,
    rank_by: str = "calmar",
    progress: bool = False,
) -> Dict:
    """
    切成訓練段（前 train_frac）與測試段（其餘），在訓練段選 rank_by 冠軍，
    回報該冠軍在測試段（樣本外）的表現，判斷是否過擬合。
    """
    master = _master_dates(regime_frames)
    k = max(1, min(len(master) - 2, int(len(master) * train_frac)))
    tr_start, tr_end = master[0], master[k - 1]
    te_start, te_end = master[k], master[-1]

    if progress:
        print(f"  訓練段 {tr_start.date()}→{tr_end.date()}（{k} 日）｜"
              f"測試段 {te_start.date()}→{te_end.date()}（{len(master) - k} 日）")

    train_tbl = grid_search(param_grid, data, regime_frames, benchmarks, initial_capital,
                            str(tr_start.date()), str(tr_end.date()), extra_overrides, rank_by, progress)
    test_tbl = grid_search(param_grid, data, regime_frames, benchmarks, initial_capital,
                           str(te_start.date()), str(te_end.date()), extra_overrides, rank_by, progress)

    best_params = None
    if not train_tbl.empty and rank_by in train_tbl.columns and train_tbl[rank_by].notna().any():
        keys = list(param_grid.keys())
        best_row = train_tbl.iloc[0]
        best_params = {k_: best_row[k_] for k_ in keys}

    # 冠軍在測試段對應列
    best_test = None
    if best_params is not None and not test_tbl.empty:
        mask = pd.Series(True, index=test_tbl.index)
        for k_, v in best_params.items():
            mask &= (test_tbl[k_] == v)
        if mask.any():
            best_test = test_tbl[mask].iloc[0].to_dict()

    return {
        "train_table": train_tbl,
        "test_table": test_tbl,
        "best_params": best_params,
        "best_train": train_tbl.iloc[0].to_dict() if not train_tbl.empty else None,
        "best_test": best_test,
        "split": {"train": (str(tr_start.date()), str(tr_end.date())),
                  "test": (str(te_start.date()), str(te_end.date()))},
        "rank_by": rank_by,
    }


# ────────────────────────────────────────────────────────────────────────────
# 報表
# ────────────────────────────────────────────────────────────────────────────
def _show_cols(df: pd.DataFrame, param_keys: List[str]) -> pd.DataFrame:
    cols = [c for c in param_keys + _METRIC_COLS if c in df.columns]
    return df[cols]


def format_walk_forward(res: Dict, param_keys: List[str]) -> str:
    rb = res["rank_by"]
    lines = [
        "═══════════════════════════════════════════════════════════",
        f"  WALK-FORWARD 最佳化  (依 {rb} 排序)",
        "═══════════════════════════════════════════════════════════",
        f"  訓練段 {res['split']['train'][0]} → {res['split']['train'][1]}",
        f"  測試段 {res['split']['test'][0]} → {res['split']['test'][1]}  ← 樣本外驗證",
        "  ───────────────────────────────────────────────────────",
        "  訓練段前 5 名：",
        _show_cols(res["train_table"], param_keys).head(5).to_string(index=False),
        "",
    ]
    if res["best_params"] is not None:
        lines.append(f"  訓練段冠軍參數：{res['best_params']}")
        bt = res.get("best_train") or {}
        lines.append(f"    訓練段  {rb}={_g(bt, rb)}  CAGR={_g(bt,'cagr_pct')}%  "
                     f"MaxDD={_g(bt,'max_drawdown_pct')}%  vs SOXX={_g(bt,'vs_SOXX_pp')}pp")
        te = res.get("best_test")
        if te:
            lines.append(f"    測試段  {rb}={_g(te, rb)}  CAGR={_g(te,'cagr_pct')}%  "
                         f"MaxDD={_g(te,'max_drawdown_pct')}%  vs SOXX={_g(te,'vs_SOXX_pp')}pp   ← 樣本外")
            _verdict(lines, bt, te, rb)
    lines.append("═══════════════════════════════════════════════════════════")
    return "\n".join(lines)


def _verdict(lines: List[str], train_row: Dict, test_row: Dict, rb: str):
    tv, ev = train_row.get(rb), test_row.get(rb)
    if tv is None or ev is None:
        return
    if ev >= tv * 0.5 and ev > 0:
        lines.append("    ✅ 樣本外仍穩健（測試段 ≥ 訓練段一半且為正）→ 參數較可信")
    elif ev <= 0:
        lines.append("    ⚠️ 樣本外轉負 → 疑似過擬合，勿直接採用")
    else:
        lines.append("    ⚠️ 樣本外明顯衰退 → 邊際證據，建議擴大樣本或保守採用")


def _g(d: Dict, k: str) -> str:
    v = d.get(k) if d else None
    return f"{v}" if v is not None else "—"


# ────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────
def _resolve_universe(name: str) -> List[str]:
    key = (name or "semi").strip().lower()
    if key == "semi":
        return list(US_SEMI_UNIVERSE)
    if key == "broad":
        return list(BROAD_UNIVERSE_DEFAULT)
    raise SystemExit(f"未知宇宙：{name}（可用 semi / broad，或 --tickers）")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    ap = argparse.ArgumentParser(description="策略參數掃描 / walk-forward 最佳化")
    ap.add_argument("--study", default="stops", help="stops | exits | grace | entry | all")
    ap.add_argument("--universe", default="semi", help="semi | broad")
    ap.add_argument("--tickers", default=None, help="逗號分隔，覆蓋 --universe")
    ap.add_argument("--capital", type=float, default=DEFAULT_INITIAL_CAPITAL)
    ap.add_argument("--train-frac", type=float, default=0.6, help="訓練段比例（其餘為樣本外測試段）")
    ap.add_argument("--rank-by", default="calmar", help="calmar | sharpe | cagr_pct | total_return_pct")
    ap.add_argument("--pure-trail", action="store_true", help="關閉分批 TP，測純移動停損")
    ap.add_argument("--csv", default=None, help="另存完整掃描表 CSV")
    args = ap.parse_args()

    if args.tickers:
        universe = [normalize_ticker(t) for t in args.tickers.split(",") if t.strip()]
    else:
        universe = _resolve_universe(args.universe)

    studies = list(STUDIES.keys()) if args.study == "all" else [args.study]
    for s in studies:
        if s not in STUDIES:
            raise SystemExit(f"未知 study：{s}（可用 {list(STUDIES)} 或 all）")

    print(f"最佳化宇宙：{len(universe)} 檔  |  本金 ${args.capital:,.0f}  |  抓資料中 …")
    data, regime, benchmarks = prepare_data(universe, progress=True)
    if not data:
        raise SystemExit("無資料可最佳化")
    extra = dict(PURE_TRAIL_OVERRIDE) if args.pure_trail else None

    all_tables = []
    for s in studies:
        grid = STUDIES[s]
        print(f"\n### STUDY = {s}  參數格 {grid}"
              f"{'（純移動停損）' if args.pure_trail else ''}")
        res = walk_forward(grid, data, regime, benchmarks, args.capital,
                           train_frac=args.train_frac, extra_overrides=extra,
                           rank_by=args.rank_by, progress=True)
        print()
        print(format_walk_forward(res, list(grid.keys())))
        t = res["test_table"].copy()
        t.insert(0, "study", s)
        t.insert(1, "segment", "test")
        all_tables.append(t)

    if args.csv and all_tables:
        pd.concat(all_tables, ignore_index=True).to_csv(args.csv, index=False, encoding="utf-8-sig")
        print(f"\n完整掃描表已另存：{args.csv}")


if __name__ == "__main__":
    main()

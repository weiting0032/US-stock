"""
scanner.py — 美股投資組合掃描器（GitHub Actions 每日執行）

執行時機：
  - 美東 09:00 前（台灣 21:00）：開盤前掃描持倉 + Watchlist 訊號
  - 美東 17:30 後（台灣 05:30）：收盤後掃描持倉 + Watchlist 訊號
  - 台灣 09:00（UTC 01:00）   ：美股半導體宇宙全掃描 + Telegram 推播

透過 GitHub Actions cron 觸發，不需 Streamlit 在線。
"""
import sys
from core import (
    DEFAULT_INITIAL_CAPITAL,
    build_portfolio,
    enrich_portfolio_with_weight_and_risk,
    format_us_semi_tg_messages,
    get_market_regime,
    load_trades,
    load_watchlist,
    maybe_log_daily_history,
    normalize_ticker,
    run_auto_scanner,
    run_broad_scanner,
    run_us_semi_scanner,
    send_telegram_msg,
    send_us_semi_tg,
)


# ── 執行模式由 CLI 引數控制 ───────────────────────────────────────────────────
#   python scanner.py            → 標準持倉/Watchlist 掃描
#   python scanner.py --semi     → 美股半導體宇宙全掃描（台灣 09:00 執行）
SEMI_MODE = "--semi" in sys.argv
BROAD_MODE = "--broad" in sys.argv


def run_portfolio_scan():
    """標準掃描：持倉 + Watchlist，每日盤前/盤後執行"""
    trades_df    = load_trades()
    watchlist_df = load_watchlist()

    portfolio_raw, cash, total_realized_pl = build_portfolio(trades_df, DEFAULT_INITIAL_CAPITAL)
    market_value       = sum(x["MarketValue"] for x in portfolio_raw)
    total_assets       = cash + market_value
    total_unrealized_pl = sum(x["Unrealized"] for x in portfolio_raw)

    market_regime = get_market_regime()
    portfolio = enrich_portfolio_with_weight_and_risk(
        portfolio_raw, total_assets, cash, market_regime,
    ) if portfolio_raw else []

    nav_ok, nav_msg = maybe_log_daily_history(
        total_assets=total_assets,
        cash=cash,
        market_value=market_value,
        realized_pl=total_realized_pl,
        unrealized_pl=total_unrealized_pl,
    )

    result = run_auto_scanner(
        portfolio=portfolio,
        trades_df=trades_df,
        cash=cash,
        total_assets=total_assets,
        market_regime=market_regime,
        watchlist_df=watchlist_df,
    )

    print("===== Portfolio Scanner =====")
    print(f"Trades: {len(trades_df)} | Portfolio: {len(portfolio)} | Watchlist: {len(watchlist_df)}")
    print(f"Cash: {cash:.2f} | MarketValue: {market_value:.2f} | NAV: {total_assets:.2f}")
    print(f"NAV Log: {nav_msg}")
    for k, v in result["metrics"].items():
        print(f"  {k}: {v}")


def run_semi_scan():
    """
    美股半導體宇宙全掃描（台灣時間 09:00 / UTC 01:00 執行）。
    掃描 SOX 成分 + AI 基礎建設 + 設備材料共 ~45 個半導體標的，
    套用多因子評分策略，強勢訊號透過 Telegram 推播。
    """
    print("===== 美股半導體宇宙掃描 =====")

    # 取得持倉中的半導體個股，一併納入掃描
    trades_df = None
    try:
        trades_df = load_trades()
        portfolio_raw, _, _ = build_portfolio(trades_df, DEFAULT_INITIAL_CAPITAL)
        held_tickers = [p["Ticker"] for p in portfolio_raw]
    except Exception:
        held_tickers = []

    result = run_us_semi_scanner(extra_tickers=held_tickers, log_signals=True, trades_df=trades_df)

    sox = result["sox_regime"]
    print(f"SOX 趨勢: {sox.get('trend')} | RS vs SPY: {sox.get('rs_vs_spy', 0):+.1f}%")
    print(f"掃描: {result['total_scanned']} 檔 | 入選: {result['total_hits']} 檔 | 寫入訊號: {result.get('signals_logged', 0)} 筆")
    print(f"  🔴 強力買進: {len(result['strong_buy'])} 檔")
    print(f"  🟢 積極買進: {len(result['buy'])} 檔")
    print(f"  🟡 留意候補: {len(result['watch'])} 檔")

    for r in result["strong_buy"]:
        print(f"  ★ {r['ticker']:6s}  {r['score']:.1f}pt  ${r['close']}  {', '.join(r['reasons'][:2])}")
    for r in result["buy"]:
        print(f"  ▲ {r['ticker']:6s}  {r['score']:.1f}pt  ${r['close']}  {', '.join(r['reasons'][:2])}")

    # Telegram 推播
    if result["total_hits"] > 0:
        msgs = format_us_semi_tg_messages(result)
        ok   = send_us_semi_tg(msgs)
        print(f"Telegram: {'✅ 已發送 ({len(msgs)} 則)' if ok else '❌ 發送失敗'}")
    else:
        print("本日無強勢半導體標的，略過 Telegram 推播。")


def run_broad_scan():
    """跨產業廣度發現掃描（找全市場趨勢領導股），強勢候選透過 Telegram 推播。"""
    print("===== 跨產業廣度發現掃描 =====")
    result = run_broad_scanner()
    print(f"市場狀態: {result['regime']} | 開放新倉: {result['allow_new_position']}")
    print(f"掃描: {result['total_scanned']} 檔 | 買進候選: {result['total_hits']} 檔")
    for r in result["strong_buy"] + result["buy"]:
        print(f"  {r['signal']:10s} {r['ticker']:6s} {r['score']:.1f}pt ${r['close']} "
              f"[{r['trigger']}] RS{r['rs20_vs_spy']:+.1f}% {r.get('sector','')}")

    hits = result["strong_buy"] + result["buy"]
    if hits and result.get("allow_new_position"):
        lines = ["📡 *跨產業發現掃描*", f"市場 {result['regime']}｜候選 {len(hits)} 檔", ""]
        for r in hits[:15]:
            emoji = "🔴" if r["signal"] == "STRONG_BUY" else "🟢"
            lines.append(f"{emoji} *{r['ticker']}* {r['score']:.1f}pt ${r['close']} "
                         f"[{r['trigger']}] RS{r['rs20_vs_spy']:+.1f}%")
        ok = send_telegram_msg("\n".join(lines))
        print(f"Telegram: {'✅ 已發送' if ok else '❌ 發送失敗'}")
    else:
        print("無符合條件之新倉候選（或市場狀態不開放新倉），略過 Telegram。")


if __name__ == "__main__":
    if SEMI_MODE:
        run_semi_scan()
    elif BROAD_MODE:
        run_broad_scan()
    else:
        run_portfolio_scan()

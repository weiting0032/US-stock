from core import (
    DEFAULT_INITIAL_CAPITAL,
    build_portfolio,
    enrich_portfolio_with_weight_and_risk,
    get_market_regime,
    load_trades,
    load_watchlist,
    maybe_log_daily_history,
    run_auto_scanner,
)


def main():
    trades_df = load_trades()
    watchlist_df = load_watchlist()

    portfolio_raw, cash, total_realized_pl = build_portfolio(trades_df, DEFAULT_INITIAL_CAPITAL)
    market_value = sum(x["MarketValue"] for x in portfolio_raw)
    total_assets = cash + market_value
    total_unrealized = sum(x["Unrealized"] for x in portfolio_raw)
    market_regime = get_market_regime()

    portfolio = enrich_portfolio_with_weight_and_risk(
        portfolio_raw, total_assets, cash, market_regime
    ) if portfolio_raw else []

    nav_ok, nav_msg = maybe_log_daily_history(
        total_assets=total_assets,
        cash=cash,
        market_value=market_value,
        realized_pl=total_realized_pl,
        unrealized_pl=total_unrealized,
    )

    result = run_auto_scanner(
        portfolio=portfolio,
        trades_df=trades_df,
        cash=cash,
        total_assets=total_assets,
        market_regime=market_regime,
        watchlist_df=watchlist_df,
    )

    print("===== Scanner Result =====")
    print(f"Trades: {len(trades_df)}")
    print(f"Portfolio count: {len(portfolio)}")
    print(f"Watchlist count: {len(watchlist_df)}")
    print(f"Cash: {cash:.2f}")
    print(f"Market Value: {market_value:.2f}")
    print(f"Total Assets: {total_assets:.2f}")
    print(f"NAV Log: {nav_msg}")
    print("----- Metrics -----")
    for k, v in result["metrics"].items():
        print(f"{k}: {v}")
    print("----- Logs -----")
    for line in result["logs"]:
        print(line)


if __name__ == "__main__":
    main()

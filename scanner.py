from core import (
    DEFAULT_INITIAL_CAPITAL,
    build_portfolio,
    enrich_portfolio_with_weight_and_risk,
    get_market_regime,
    load_trades,
    run_auto_scanner,
)


def main():
    trades_df = load_trades()
    portfolio_raw, cash, total_realized_pl = build_portfolio(trades_df, DEFAULT_INITIAL_CAPITAL)
    market_value = sum(x["MarketValue"] for x in portfolio_raw)
    total_assets = cash + market_value
    market_regime = get_market_regime()

    portfolio = enrich_portfolio_with_weight_and_risk(
        portfolio_raw, total_assets, cash, market_regime
    ) if portfolio_raw else []

    logs = run_auto_scanner(portfolio, trades_df, cash, total_assets, market_regime)

    print("===== Scanner Result =====")
    print(f"Trades: {len(trades_df)}")
    print(f"Portfolio count: {len(portfolio)}")
    print(f"Cash: {cash:.2f}")
    print(f"Market Value: {market_value:.2f}")
    print(f"Total Assets: {total_assets:.2f}")
    print("----- Logs -----")
    for line in logs:
        print(line)


if __name__ == "__main__":
    main()

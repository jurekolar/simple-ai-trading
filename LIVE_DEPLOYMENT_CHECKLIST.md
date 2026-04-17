# Live Deployment Checklist

Use this checklist before the first live-capital session and before any later scale-up.

## Strategy Approval

- Primary live strategy is `breakout`.
- Parameter set is frozen and archived.
- Symbol universe is frozen and archived.
- Latest paper burn-in window is at least `3-4` clean calendar weeks.
- Latest benchmark artifact set is archived and reviewed.
- Expected live acceptance metrics are reviewed:
  - positive expectancy after fees/slippage assumptions
  - acceptable max drawdown
  - acceptable turnover
  - no unresolved operational incidents in the final burn-in window

## Config Signoff

- Use [.env.live.example](/Users/jurekolar/Code/simple-ai-trading/.env.live.example) as the starting point.
- `ALPACA_PAPER=false`
- `PAPER_ONLY=false`
- `ALLOW_LIVE=true`
- `CONFIG_PROFILE=live`
- `LIVE_CONFIG_PROFILE=live`
- `LIVE_DEPLOYMENT_ACK=I_ACKNOWLEDGE_LIVE_TRADING`
- `DENY_NEW_ENTRIES=true` before the supervised open.
- `SAFE_OPEN_ENABLED=true` and the operator-watched entry window is agreed.
- `DATABASE_URL` points to a dedicated live journal, not the paper burn-in database.
- Alert routing is configured and tested.
- `PRIMARY_LIVE_STRATEGY` matches the latest approved benchmark candidate.
- The latest benchmark artifact is recent enough for `preflight` to pass.

## Operator Readiness

- Assigned operator coverage for market open and the first `1-2` live weeks.
- Restart authority is explicit.
- Flatten authority is explicit.
- Escalation path for stale data, broker outage, and reconciliation mismatch is written down.
- Rollback path is clear:
  - set `DENY_NEW_ENTRIES=true`
  - run `reconcile`
  - flatten if risk or reconciliation requires it

## Session Launch

- Run preflight before enabling live trading:
  ```bash
  python -m app.main --strategy breakout preflight
  ```
- Confirm latest burn-in and daily operator report outputs are clean.
- Confirm `BACKTEST_OUTPUT_DIR/latest.json` still points to the approved candidate and a decision-ready artifact set.
- Run auth checks.
- Run `reconcile` before market open.
- Review startup summary:
  - broker mode
  - account environment
  - strategy
  - symbols
  - max gross exposure
  - max daily loss
  - whether new entries are enabled
- Enable entries only during the supervised safe-open window.

## Post-Session Review

- Run `reconcile` after the session.
- Review the daily operator report before the next session.
- Confirm:
  - no reconciliation drift
  - no duplicate order IDs
  - no unexplained unresolved orders
  - alert delivery worked
  - blocked orders and kill-switch events are understood

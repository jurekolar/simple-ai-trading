# Paper Burn-In Checklist

Use this every trading day during the 3-4 week paper burn-in for the primary live candidate strategy.

## Pre-Market

- Confirm `.env` matches the paper burn-in profile and still has `ALPACA_PAPER=true`, `PAPER_ONLY=true`, `ALLOW_LIVE=false`.
- Confirm `CONFIG_PROFILE=paper` and `PRIMARY_LIVE_STRATEGY=breakout`.
- Confirm `DRY_RUN=false` if you want real paper orders submitted.
- Confirm the database path is correct and writable.
- Confirm alert routing is configured and tested if `ALERT_WEBHOOK_URL` is set.
- Record the code version or git commit in your daily log.
- Run the burn-in summary script for the prior window:
  ```bash
  .venv/bin/python scripts/burnin_report.py --days 7
  ```

## Market Open / Intraday

- Start the trading loop and confirm the first run completes cleanly.
- Watch for:
  - blocked orders
  - kill-switch activations
  - reconciliation events
  - broker error events
  - unexpected position changes
- If doing drills, record the exact time and expected outcome.

## Recommended Drills

- Restart during market hours with unresolved orders.
- Toggle `DENY_NEW_ENTRIES=true` and verify entries stop while exits still run.
- Trigger `EMERGENCY_FLATTEN=true` and verify positions reduce cleanly.
- Simulate stale/unsafe data and verify the run blocks new entries.
- Simulate broker submit failures and verify alerts fire.

## End-Of-Day Review

- Run:
  ```bash
  .venv/bin/python scripts/burnin_report.py --days 1
  ```
- Confirm:
  - zero duplicate orders
  - zero reconciliation drift
  - zero unexplained position changes
  - unresolved orders are understood
  - alerts were delivered for any meaningful event
- Review recent orders, reconciliation events, kill-switch events, and broker error events.
- Record:
  - submitted orders
  - blocked orders
  - unresolved orders at close
  - kill-switch reason(s)
  - reconciliation mismatch count
  - broker failure count
  - realized/unrealized PnL
  - notes

## Exit Criteria For Burn-In

Before first live capital, require:

- A continuous `3-4` week burn-in on the frozen primary live strategy.
- Zero reconciliation drift over the burn-in window.
- Zero duplicate orders.
- Stable alert delivery.
- Known behavior under stale data and broker failures.
- Documented drills for restart, deny-new-entries, emergency flatten, stale data, and broker failure.
- A reviewed operator runbook for restart, flatten, and recovery.

# Operator Runbook

Use this runbook for paper burn-in operations and as the baseline for live-readiness review. Pair it with [LIVE_DEPLOYMENT_CHECKLIST.md](/Users/jurekolar/Code/simple-ai-trading/LIVE_DEPLOYMENT_CHECKLIST.md) before any live-capital session.

## Startup

1. Confirm the intended profile is active:
   ```bash
   cp .env.paper_burnin .env
   ```
2. Confirm Alpaca trading and data auth:
   ```bash
   make burnin-auth
   ```
3. Confirm the latest burn-in summaries if this is not the first run of the day:
   ```bash
   make burnin-report-1d
   make burnin-report-7d
   ```
4. Start the daily workflow:
   ```bash
   make burnin-day
   make burnin-day STRATEGY=breakout
   ```
5. If needed, open the dashboard:
   ```bash
   streamlit run app/monitoring/dashboard.py
   ```

Checks before leaving the bot unattended:
- No auth failures.
- No fallback/synthetic data block.
- No unresolved orders that are unexplained.
- No reconciliation mismatch.

## Restart During Market Hours

Use this when the bot process stops unexpectedly or you need to restart intentionally.

1. Do not edit limits or flags unless that is part of the procedure.
2. Inspect current state first:
   ```bash
   make burnin-report-1d
   .venv/bin/python -m app.main reconcile
   make burnin-report-1d
   ```
3. Check:
- open orders
- unresolved orders
- current broker positions
- recent reconciliation events
4. If unresolved orders or open positions exist, restart without changing risk config:
   ```bash
   .venv/bin/python -m app.main paper
   ```
5. Confirm after restart:
- no duplicate orders
- no reconciliation events
- unresolved order count is stable or decreasing

Escalate and stop new restarts if:
- unresolved orders increase unexpectedly
- duplicate order IDs appear
- reconciliation mismatch appears after restart

## Stale Data

Symptoms:
- `unsafe market data source=fallback blocked in trading mode`
- kill-switch reason includes `stale_data`
- alerts mention stale data or unsafe data source

Action:
1. Do not override with `ALLOW_UNSAFE_DATA_FALLBACK=true` during burn-in.
2. Check auth and market-data access:
   ```bash
   make burnin-auth
   ```
3. Re-run reconcile only:
   ```bash
   .venv/bin/python -m app.main reconcile
   ```
4. If positions exist, confirm whether exits are needed and whether the bot is still in reduce-only behavior.
5. Record the incident in the daily log.

Do not resume normal paper runs until:
- Alpaca data auth passes
- the data source is back to `alpaca`
- no stale-data alert remains unexplained

## Broker Outage

Symptoms:
- broker submit errors
- account/order/position calls failing
- repeated broker error events

Action:
1. Stop issuing repeated manual reruns.
2. Check recent state:
   ```bash
   make burnin-report-1d
   .venv/bin/python -m app.main reconcile
   ```
3. Review:
- broker error events
- unresolved orders
- open positions
- kill-switch state
4. If the outage is transient, wait for broker recovery before the next paper cycle.
5. If positions are open and the broker is degraded, do not assume flattening will succeed immediately.

Escalate if:
- broker error events continue after recovery
- order state is ambiguous
- reconciliation changes unexpectedly after the outage

## Emergency Flatten

Use only when you intentionally want all positions exited as soon as the system can do so.

1. Set in `.env`:
   ```bash
   EMERGENCY_FLATTEN=true
   ```
2. Run:
   ```bash
   .venv/bin/python -m app.main paper
   .venv/bin/python -m app.main reconcile
   make burnin-report-1d
   ```
3. Confirm:
- positions are reduced or closed
- no new entries were submitted
- no unresolved orders remain unexpectedly
- alerts were generated if flattening encountered problems
4. After the event is complete, reset:
   ```bash
   EMERGENCY_FLATTEN=false
   ```
   Apply it back into the active `.env`.

Do not leave `EMERGENCY_FLATTEN=true` enabled by accident across normal sessions.

## End-Of-Day Signoff

Run:
```bash
make burnin-review
```

Review and record:
- paper runs completed
- blocked orders
- error orders
- unresolved orders
- duplicate order IDs
- reconciliation events
- kill-switch events
- broker error events
- latest equity, buying power, cash
- gross exposure and unrealized PnL

Daily signoff is complete only if:
- unresolved orders are zero or explicitly understood
- no reconciliation drift exists
- no duplicate order IDs exist
- any blocked/error orders are explained
- any kill-switch or broker-error event has a written note

## Incident Notes Template

Record incidents with:
- Date and time
- Trigger
- Observed behavior
- Commands run
- Current broker positions/orders
- Resolution
- Follow-up fix needed

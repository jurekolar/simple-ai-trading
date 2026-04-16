# Manual Position Interventions

This document explains what to expect if you manually change positions in Alpaca while the bot is operating in paper burn-in mode.

## General Rule

Manual broker actions are treated as out-of-band state changes.

The bot does not know about them immediately. It learns about them on the next:

- `python -m app.main paper`
- `python -m app.main reconcile`

Until that happens, local journal state may temporarily lag broker truth.

## Recommended Follow-Up After Any Manual Action

Run:

```bash
.venv/bin/python -m app.main reconcile
.venv/bin/python scripts/burnin_report.py --days 1
```

Then check:

- unresolved orders
- reconciliation events
- kill-switch events
- current broker positions
- current local position snapshots

Record the event using [logs/burnin/INCIDENT_TEMPLATE.md](/Users/jurekolar/Code/simple-ai-trading/logs/burnin/INCIDENT_TEMPLATE.md).

## Case 1: Manual Full Close With No Open Orders

Example:
- The account holds `SPY`
- There are no open exit orders for `SPY`
- You manually close `SPY` in Alpaca

Expected sequence:

1. Alpaca position becomes zero immediately.
2. The local journal still shows the old position until the next reconcile or paper run.
3. On the next reconcile:
   - local position snapshots are replaced with broker truth
   - the manual close fill/order should appear in synced broker activity
   - realized PnL should update from execution fills
4. After reconciliation, the bot should no longer try to exit that position.
5. On later runs, `SPY` may become eligible for a new entry again if signals and limits allow.

What to watch:

- any temporary reconciliation event before sync completes
- whether realized PnL updates correctly
- whether `SPY` is absent from both broker and local positions after reconcile

## Case 2: Manual Full Close While The Bot Already Has An Unresolved Exit Order

Example:
- The account holds `SPY`
- The bot already submitted an exit for `SPY`, but that order is still unresolved
- You manually close `SPY` in Alpaca anyway

Expected sequence:

1. Alpaca may now show:
   - the original bot exit order still open or pending, and/or
   - your manual close already filled
2. This creates temporary ambiguity until broker order state settles.
3. On the next reconcile:
   - broker orders and fills are synced
   - local unresolved order state should update from broker status
   - the position snapshot should move to zero if the manual close fully flattened it
4. If the old exit order is still unresolved at the broker, the bot should continue to treat the symbol as unresolved and avoid submitting another order for it.
5. Once the broker resolves or cancels the old exit order, unresolved state should clear on a later reconcile.

Risks:

- temporary reconciliation mismatch
- unresolved order state that persists longer than expected
- duplicate-exit risk if broker state is not reconciled before another decision cycle

Required response:

```bash
.venv/bin/python -m app.main reconcile
.venv/bin/python scripts/burnin_report.py --days 1
```

Do not manually restart repeated paper cycles until the unresolved state is understood.

## Case 3: Manual Partial Close

Example:
- The account holds `SPY` 100 shares
- You manually sell 40 shares in Alpaca
- 60 shares remain

Expected sequence:

1. Alpaca updates the live position quantity immediately.
2. Local journal quantity remains stale until reconcile.
3. On the next reconcile:
   - local position snapshot should move from 100 to 60
   - fill history should include the partial close
   - realized PnL should update only for the closed quantity
4. If the bot later generates an exit, it should size the exit using the reconciled remaining broker quantity, not the pre-intervention quantity.
5. If there was an unresolved order already attached to that symbol, the bot should still treat the symbol as unresolved until broker order state settles.

What to watch:

- position quantity after reconcile matches Alpaca
- no oversize exit is submitted against the old quantity
- realized PnL updates only for the closed portion
- unresolved state is consistent with any remaining broker orders

## Operational Guidance

- Prefer reconciling immediately after any manual intervention.
- Avoid manual intervention during unresolved broker order flow unless the drill explicitly requires it.
- Never assume the bot has seen the change until a reconcile has completed.
- Treat manual actions as test incidents and record them.

## Minimum Post-Intervention Checks

After any manual close or partial close, confirm:

- `open_orders` makes sense
- `Unresolved orders now` is expected
- no duplicate order IDs appear
- no unexplained reconciliation event appears
- position quantity and realized PnL match broker reality

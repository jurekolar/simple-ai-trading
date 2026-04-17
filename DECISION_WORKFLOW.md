# Decision Workflow

This is the intended end-to-end flow from research to paper burn-in to live approval.

## 1. Run Benchmark Research

```bash
.venv/bin/python -m app.main compare
```

Review:

- `recommendation`
- `benchmark_valid`
- `selection_score`
- artifact directory path

Only treat a candidate as live-eligible if the benchmark is valid and the recommended candidate is clearly identified in the latest artifact set.

## 2. Review Benchmark Artifacts

Open the latest files under `BACKTEST_OUTPUT_DIR`:

- `strategy_comparison.csv`
- `approval_summary.json`
- `approval_summary.md`
- strategy trade logs
- strategy equity curves
- regime / quarter / sensitivity summaries

Confirm:

- the winner is stable
- invalid benchmark reasons are empty
- baselines were beaten
- the candidate is not obviously fragile in sensitivity checks

## 3. Freeze The Candidate

Set:

- `PRIMARY_LIVE_STRATEGY`
- strategy parameters
- symbol universe

Archive the benchmark artifact set that justified the decision.

## 4. Run Paper Burn-In

Use the frozen candidate in paper mode and follow:

- [PAPER_BURNIN_CHECKLIST.md](/Users/jurekolar/Code/simple-ai-trading/PAPER_BURNIN_CHECKLIST.md)
- [OPERATOR_RUNBOOK.md](/Users/jurekolar/Code/simple-ai-trading/OPERATOR_RUNBOOK.md)

Record drills and incidents during the burn-in window.

## 5. Run Preflight Before Live

```bash
.venv/bin/python -m app.main --strategy breakout preflight
```

Preflight should confirm:

- config is correct
- alert routing is configured
- benchmark artifact exists and is recent
- approved candidate matches the requested live strategy
- database separation is safe
- snapshots and reconcile state are acceptable

## 6. Live Checklist Signoff

Use:

- [LIVE_DEPLOYMENT_CHECKLIST.md](/Users/jurekolar/Code/simple-ai-trading/LIVE_DEPLOYMENT_CHECKLIST.md)

The benchmark artifact and preflight result should be part of the signoff, not a separate informal step.

## 7. Recovery Path

If unresolved state appears:

```bash
make recover-unresolved
```

Then use the runbook to decide whether to restart, deny new entries, or flatten.

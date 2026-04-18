.PHONY: burnin-day burnin-review burnin-auth paper reconcile report preflight burnin-report-1d burnin-report-7d stress-drill recover-unresolved

STRATEGY ?= momentum
SCENARIO ?= stale_data_block

burnin-auth:
	.venv/bin/python scripts/check_alpaca_auth.py --symbol SPY

paper:
	.venv/bin/python -m app.main paper

reconcile:
	.venv/bin/python -m app.main reconcile

report:
	.venv/bin/python -m app.main report

preflight:
	.venv/bin/python -m app.main --strategy $(STRATEGY) preflight

burnin-report-1d:
	.venv/bin/python scripts/burnin_report.py --days 1

burnin-report-7d:
	.venv/bin/python scripts/burnin_report.py --days 7

burnin-day:
	STRATEGY=$(STRATEGY) bash ./scripts/run_burnin_day.sh

burnin-review:
	bash ./scripts/review_burnin_day.sh

stress-drill:
	.venv/bin/python scripts/stress_drill.py --scenario $(SCENARIO)

recover-unresolved:
	bash ./scripts/recover_unresolved_state.sh

.PHONY: burnin-day burnin-review burnin-auth paper reconcile report burnin-report-1d burnin-report-7d

STRATEGY ?= momentum

burnin-auth:
	.venv/bin/python scripts/check_alpaca_auth.py --symbol SPY

paper:
	.venv/bin/python -m app.main paper

reconcile:
	.venv/bin/python -m app.main reconcile

report:
	.venv/bin/python -m app.main report

burnin-report-1d:
	.venv/bin/python scripts/burnin_report.py --days 1

burnin-report-7d:
	.venv/bin/python scripts/burnin_report.py --days 7

burnin-day:
	STRATEGY=$(STRATEGY) ./scripts/run_burnin_day.sh

burnin-review:
	./scripts/review_burnin_day.sh

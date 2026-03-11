# token-usage-dashboard

Interactive local dashboard for CodexBar usage/cost data.

## What it does (this PR scope)

- Builds a self-contained HTML dashboard from CodexBar JSON
- Shows model cost breakdown and top 7-day movers
- Detects daily spikes (configurable threshold)
- Provides cost forecast (7/30 days), anomaly detection (z-score), and alert rule evaluation

> Note: notification channels in config are **currently rule-evaluation metadata only**. This project does not dispatch real notifications yet.

## Quick start

```bash
git clone https://github.com/bkes994408-cmd/token-usage-dashboard.git
cd token-usage-dashboard
./run_dashboard.sh
```

## CLI usage

```bash
python3 scripts/token_usage_dashboard.py \
  --provider codex \
  --days 30 \
  --spike-lookback-days 7 \
  --spike-threshold-mult 2.0 \
  --alert-config docs/ALERT_CONFIG_EXAMPLE.json \
  --output /tmp/token_usage_dashboard.html \
  --summary-json /tmp/token_usage_summary.json \
  --open
```

## Output files

- HTML dashboard: `/tmp/token_usage_dashboard.html` (default)
- Summary JSON: `/tmp/token_usage_summary.json` (default)

## Notes

- Requires `codexbar` CLI in PATH for live pull mode.
- You can also pass pre-exported JSON via `--input`.

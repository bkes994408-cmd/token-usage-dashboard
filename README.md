# token-usage-dashboard

Interactive local dashboard for CodexBar usage/cost data.

## What it does

- Builds a self-contained HTML dashboard from CodexBar JSON
- Optimized for larger datasets (throttled tooltip rendering, cached range/day summaries, capped initial model table rows)
- Shows model cost breakdown and top 7-day movers
- Detects daily spikes (configurable threshold)
- Supports deep-link state (`#date=...&spikeOnly=1`)
- Keyboard-driven navigation and spike focus
- New: Custom Report Builder (choose metrics, model filters, daily/weekly/monthly granularity, export CSV)
- New: Multi-tenant organization isolation + org-level users/roles/dashboard-view assignment
- New: Cost forecasting (next N days) + anomaly consumption alerts (z-score based)

## Quick start

```bash
# clone
 git clone https://github.com/bkes994408-cmd/token-usage-dashboard.git
 cd token-usage-dashboard

# run with defaults (codex, last 30 days, open browser)
./run_dashboard.sh
```

## CLI usage

### One-shot script

```bash
./run_dashboard.sh --provider codex --days 30
./run_dashboard.sh --provider claude --days 14 --no-open
./run_dashboard.sh --input /tmp/cost.json --spike-threshold-mult 1.8 --forecast-days 14 --anomaly-z-threshold 2.3 --max-table-rows 150 --chart-max-points 1000
```

### Direct Python command

```bash
python3 scripts/token_usage_dashboard.py \
  --provider codex \
  --days 30 \
  --spike-lookback-days 7 \
  --spike-threshold-mult 2.0 \
  --output /tmp/token_usage_dashboard.html \
  --summary-json /tmp/token_usage_summary.json \
  --custom-report-json /tmp/custom_report.json \
  --report-metrics total_cost,active_models,avg_cost_per_model \
  --report-models gpt-5,o3 \
  --report-granularity weekly \
  --open
```

### Multi-tenant / organization mode

```bash
python3 scripts/token_usage_dashboard.py \
  --provider codex \
  --input /tmp/tenant_usage_payload.json \
  --tenant-config /tmp/tenant_config.json \
  --org-id acme \
  --user alice \
  --dashboard-view eng-core \
  --output /tmp/token_usage_dashboard_acme.html
```

User management (create/update/delete/list):

```bash
python3 scripts/token_usage_dashboard.py --tenant-config /tmp/tenant_config.json --org-id acme \
  --manage-users create --target-user bob --target-role analyst --target-group analytics
```

Tenant payload format should include per-org daily data, e.g.:

```json
{
  "provider": "codex",
  "organizations": {
    "acme": { "daily": [ ... ] },
    "globex": { "daily": [ ... ] }
  }
}
```

See `docs/TENANT_CONFIG_EXAMPLE.json` for org/user/group/role/view config schema.

Dashboard view management (create/update/delete/list/assign/unassign):

```bash
python3 scripts/token_usage_dashboard.py --tenant-config /tmp/tenant_config.json --org-id acme \
  --manage-views create --view-id analytics-view --view-models gpt-5,o3 --view-max-days 30
python3 scripts/token_usage_dashboard.py --tenant-config /tmp/tenant_config.json --org-id acme \
  --manage-views assign --view-id analytics-view --view-group analytics
```

## Keyboard shortcuts

- `←/→` or `j/k`: previous/next day
- `n/p`: next/previous spike day
- `s`: toggle spike-only navigation
- `r`: reset to latest day
- `?`: show/hide keyboard help
- `Esc`: close keyboard help

## Output files

- HTML dashboard: `/tmp/token_usage_dashboard.html` (default)
- Summary JSON: `/tmp/token_usage_summary.json` (default)

## Notes

- Requires `codexbar` CLI in PATH for live pull mode.
- You can also pass pre-exported JSON via `--input`.

# token-usage-dashboard

Interactive local dashboard for CodexBar usage/cost data.

## What it does

- Builds a self-contained HTML dashboard from CodexBar JSON
- Optimized for larger datasets (throttled tooltip rendering, cached range/day summaries, capped initial model table rows)
- Shows model cost breakdown and top 7-day movers
- Detects daily spikes (configurable threshold)
- Supports deep-link state (`#date=...&spikeOnly=1`)
- Keyboard-driven navigation and spike focus

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
./run_dashboard.sh --input /tmp/cost.json --spike-threshold-mult 1.8 --max-table-rows 150 --chart-max-points 1000
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
  --open
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

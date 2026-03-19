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

- New: LLM 使用模式深入分析（prompt/completion 分布、高消耗 hotspot、模型效率、匿名關鍵詞）
- New: 成本預測與異常消耗預警（7/30 天 forecast、z-score anomaly、可配置預警規則與通知通道）
- New: 成本歸因與優化建議（project/department/user/application/business line attribution + 規則式節流建議）
- New: Prompt 優化建議引擎（正式化輸出：engineVersion/spec、可配置高消耗 family 排序與 A/B success criteria）
- New: Quota Policies（預算分配 + 角色/使用者權限矩陣 + 自動化超額處置彙總）
- New: Dashboard Policy View（quota 摘要、allocation policy 狀態、auto-enforcement actions）
- New: 報表自動化排程（daily/weekly/monthly/quarterly jobs）、JSON/CSV 自動產出、report history/download center（`report_history.json`）
- New: 報表分發權限守門（recipient role guardrail，未授權收件者自動 block 並留下審計記錄）
- New: 實時成本控制策略（multi-layer budget policy，可觸發 degrade / switch_model / stop_calls 動作）
- New: 多雲/多模型成本聚合（`--aggregate-providers codex,claude`）：統一聚合 provider/model/day 成本並在 dashboard 顯示 Unified View
- New: Notification Dispatch 系統（Slack/Discord Webhook）已整合於 Scheduler 與 Event Monitor；支援 timeout/retry 設定。
- New: Cloud Cost Management 整合（AWS Cost Explorer / GCP Billing 匯入）與 Unified Cloud Cost View（LLM + Cloud Infra 成本統一視圖）。

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
./run_dashboard.sh --provider codex --input /tmp/cost_multi_provider.json --aggregate-providers codex,claude
```

### Direct Python command

```bash
python3 scripts/token_usage_dashboard.py \
  --provider codex \
  --aggregate-providers codex,claude \
  --days 30 \
  --spike-lookback-days 7 \
  --spike-threshold-mult 2.0 \
  --alert-config docs/ALERT_CONFIG_EXAMPLE.json \
  --cost-control-config docs/COST_CONTROL_CONFIG_EXAMPLE.json \
  --budget-config docs/BUDGET_CONFIG_EXAMPLE.json \
  --prompt-optimization-config docs/PROMPT_OPTIMIZATION_CONFIG_EXAMPLE.json \
  --cloud-cost-input docs/CLOUD_COST_INPUT_EXAMPLE.json \
  --output /tmp/token_usage_dashboard.html \
  --summary-json /tmp/token_usage_summary.json \
  --custom-report-json /tmp/custom_report.json \
  --report-metrics total_cost,active_models,avg_cost_per_model \
  --report-models gpt-5,o3 \
  --report-granularity weekly \
  --open
```

### Scheduled report automation

```bash
python3 scripts/token_usage_dashboard.py \
  --provider codex \
  --input /tmp/cost.json \
  --run-report-scheduler \
  --report-scheduler-config /tmp/report_scheduler.json \
  --report-output-dir /tmp/report_center
```

`/tmp/report_center/report_history.json` 會保存歷史版本與下載檔案路徑（JSON/CSV），並記錄每個收件 webhook 的 dispatch 結果（sent/failed/blocked）。
可參考 `docs/REPORT_SCHEDULER_EXAMPLE.json`。

### Event monitor + alert dispatch

```bash
python3 scripts/token_usage_dashboard.py \
  --provider codex \
  --input /tmp/cost.json \
  --alert-config docs/ALERT_CONFIG_EXAMPLE.json \
  --cost-control-config docs/COST_CONTROL_CONFIG_EXAMPLE.json \
  --budget-config docs/BUDGET_CONFIG_EXAMPLE.json \
  --run-event-monitor \
  --event-output-json /tmp/event_monitor_result.json
```

Event monitor 會整合：
- alert rules triggered
- real-time cost control triggered actions
- overage behaviors

並將彙整告警透過 `notificationChannels` 的 webhook 發送（Slack/Discord）。

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

Real-time cost control policy example: `docs/COST_CONTROL_CONFIG_EXAMPLE.json`.
Prompt optimization engine config example: `docs/PROMPT_OPTIMIZATION_CONFIG_EXAMPLE.json`.
Cloud cost input example (AWS/GCP normalized records): `docs/CLOUD_COST_INPUT_EXAMPLE.json`.

`--cloud-cost-input` 支援三種資料形狀：
- normalized records (`records` 或 list)
- AWS Cost Explorer `ResultsByTime`
- GCP billing-like `daily` rows

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

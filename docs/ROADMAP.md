# Roadmap / MVP Checklist (token-usage-dashboard)

## Current focus: Forecast / Anomaly / Alert

- [x] 7/30 day cost forecast (moving-average + trend)
- [x] Cost anomaly detection (z-score)
- [x] Alert rule evaluation from config
- [x] Dashboard visualization for forecast/anomaly/triggered alerts

## Important implementation note

- `notificationChannels` currently represent **rule evaluation output only**.
- Real notification dispatch (email/webhook/slack/discord delivery) is **not implemented yet**.

## Out of scope for this PR

- Multi-tenant org/user/view management
- LLM deep usage/prompt pattern analysis
- Other platform-level enterprise features

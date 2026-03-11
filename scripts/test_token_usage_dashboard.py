#!/usr/bin/env python3

from unittest import TestCase, main

from token_usage_dashboard import (
    build_dashboard_html,
    build_summary,
    detect_cost_anomalies,
    evaluate_alert_rules,
    forecast_cost,
)


class TestTokenDashboard(TestCase):
    def test_summary_includes_forecast_anomaly_alert(self):
        rows = [
            {"date": f"2026-03-{d:02d}", "modelBreakdowns": [{"modelName": "gpt-5", "cost": float(d)}]}
            for d in range(1, 16)
        ]
        summary = build_summary("codex", rows, alert_config={"rules": {"budgetThresholdUSD": 20, "budgetForecastPct": 80}})
        self.assertIn("forecast", summary)
        self.assertIn("costAnomalies", summary)
        self.assertIn("alerts", summary)
        self.assertIn("next7Days", summary["forecast"])

    def test_alert_config_invalid_anomaly_threshold_is_robust(self):
        rows = [
            {"date": f"2026-03-{d:02d}", "modelBreakdowns": [{"modelName": "gpt-5", "cost": float(d)}]}
            for d in range(1, 16)
        ]
        f7 = forecast_cost(rows, horizon_days=7, lookback_days=14)
        anomalies = detect_cost_anomalies(
            rows + [{"date": "2026-03-16", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 200.0}]}],
            lookback_days=7,
            z_threshold=1.5,
        )
        alerts = evaluate_alert_rules(
            rows,
            f7,
            anomalies,
            config={"rules": {"anomalyCountThreshold": "abc"}, "notificationChannels": ["discord"]},
        )
        self.assertIsInstance(alerts, dict)
        self.assertIn("triggered", alerts)

    def test_dashboard_html_escapes_xss_dynamic_strings(self):
        rows = [
            {
                "date": "2026-03-01\"><img src=x onerror=alert(1)>",
                "modelBreakdowns": [
                    {"modelName": "<script>alert(1)</script>", "cost": 10.0},
                ],
            },
            {
                "date": "2026-03-02",
                "modelBreakdowns": [
                    {"modelName": "safe-model", "cost": 200.0},
                ],
            },
        ]
        summary = build_summary(
            "codex",
            rows,
            alert_config={
                "rules": {"budgetThresholdUSD": 1, "budgetForecastPct": 1, "anomalyCountThreshold": 1},
                "notificationChannels": ["discord\"><script>alert(9)</script>"],
            },
        )
        self.assertTrue(summary["alerts"]["triggered"])
        html = build_dashboard_html(
            "codex\"><script>alert('p')</script>",
            rows,
            top_models=5,
            alert_config={
                "rules": {"budgetThresholdUSD": 1, "budgetForecastPct": 1, "anomalyCountThreshold": 1},
                "notificationChannels": ["discord\"><script>alert(9)</script>"],
            },
        )

        self.assertNotIn("<script>alert(1)</script>", html)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html)
        self.assertIn("data-date=", html)
        self.assertIn("\\u003cimg src=x onerror=alert(1)>", html)
        self.assertIn("&lt;script&gt;alert(&#x27;p&#x27;)&lt;/script&gt;", html)


if __name__ == "__main__":
    main()

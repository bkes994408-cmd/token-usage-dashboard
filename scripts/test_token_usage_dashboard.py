#!/usr/bin/env python3

from unittest import TestCase, main

from token_usage_dashboard import (
    build_dashboard_html,
    build_model_table_rows,
    build_summary,
    detect_spikes,
    model_totals,
    prepare_chart_series,
)


class TestTokenDashboard(TestCase):
    def test_build_summary(self):
        rows = [
            {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.2}]},
            {"date": "2026-03-02", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 0.8}, {"modelName": "o3", "cost": 0.5}]},
        ]
        summary = build_summary("codex", rows)
        self.assertEqual(summary["provider"], "codex")
        self.assertEqual(summary["rows"], 2)
        self.assertAlmostEqual(summary["totalCostUSD"], 2.5)
        self.assertEqual(summary["models"][0]["model"], "gpt-5")

    def test_model_totals_aggregates(self):
        rows = [
            {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.2}]},
            {"date": "2026-03-02", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 0.8}, {"modelName": "o3", "cost": 0.5}]},
        ]
        totals = model_totals(rows)
        self.assertAlmostEqual(totals["gpt-5"], 2.0)
        self.assertAlmostEqual(totals["o3"], 0.5)

    def test_summary_includes_7d_delta(self):
        rows = [
            {
                "date": f"2026-03-{d:02d}",
                "modelBreakdowns": [
                    {"modelName": "gpt-5", "cost": float(d)},
                    {"modelName": "o3", "cost": 1.0 if d >= 8 else 0.0},
                ],
            }
            for d in range(1, 15)
        ]
        summary = build_summary("codex", rows)
        self.assertAlmostEqual(summary["last7dCostUSD"], sum(float(d) + 1.0 for d in range(8, 15)))
        self.assertAlmostEqual(summary["prev7dCostUSD"], sum(float(d) for d in range(1, 8)))
        self.assertIsInstance(summary["last7dDeltaPct"], float)
        self.assertTrue(len(summary["movers"]) >= 2)
        self.assertEqual(summary["movers"][0]["model"], "gpt-5")

    def test_detect_spikes(self):
        rows = [
            {"date": f"2026-03-{d:02d}", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 10.0}]}
            for d in range(1, 9)
        ]
        rows.append({"date": "2026-03-09", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 30.0}]})

        spikes = detect_spikes(rows, lookback_days=7, threshold_mult=2.0)
        self.assertEqual(len(spikes), 1)
        self.assertEqual(spikes[0]["date"], "2026-03-09")

    def test_build_summary_respects_spike_params(self):
        rows = [
            {"date": f"2026-03-{d:02d}", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 10.0}]}
            for d in range(1, 9)
        ]
        rows.append({"date": "2026-03-09", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 15.0}]})

        summary_default = build_summary("codex", rows)
        summary_sensitive = build_summary("codex", rows, spike_lookback_days=7, spike_threshold_mult=1.2)
        self.assertEqual(len(summary_default["spikes"]), 0)
        self.assertEqual(len(summary_sensitive["spikes"]), 1)

    def test_dashboard_html_contains_spike_visuals(self):
        rows = [
            {"date": f"2026-03-{d:02d}", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 10.0}]}
            for d in range(1, 9)
        ]
        rows.append({"date": "2026-03-09", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 30.0}]})
        html = build_dashboard_html("codex", rows, top_models=3, spike_lookback_days=7, spike_threshold_mult=2.0)
        self.assertIn("const spikeByDate", html)
        self.assertIn("⚠ Spike", html)
        self.assertIn("Daily Cost Spikes", html)
        self.assertIn("spike-row-2026-03-09", html)
        self.assertIn("scrollIntoView", html)
        self.assertIn("const dayBreakdownByDate", html)
        self.assertIn("Selected Day Model Breakdown", html)
        self.assertIn("DoD Δ", html)
        self.assertIn("renderSelectedDay", html)
        self.assertIn("id=\"spikesBody\"", html)
        self.assertIn("focusSpikeDate", html)
        self.assertIn("focusDate", html)
        self.assertIn("stepDate", html)
        self.assertIn("jumpSpike", html)
        self.assertIn("spikeOnlyToggle", html)
        self.assertIn("toggleSpikeOnlyMode", html)
        self.assertIn("toggleKeyboardHelp", html)
        self.assertIn("resetToLatestDay", html)
        self.assertIn("copyDeepLink", html)
        self.assertIn("id=\"copyLinkBtn\"", html)
        self.assertIn("id=\"selectedDayMeta\"", html)
        self.assertIn("id=\"sortByDodToggle\"", html)
        self.assertIn("id=\"showOnlyChangesToggle\"", html)
        self.assertIn("sortByDodMode", html)
        self.assertIn("showOnlyChangesMode", html)
        self.assertIn("toggleDodSortMode", html)
        self.assertIn("toggleChangesOnlyMode", html)
        self.assertIn("ev.key === 'd'", html)
        self.assertIn("ev.key === 'x'", html)
        self.assertIn("sortDod", html)
        self.assertIn("changesOnly", html)
        self.assertIn("ev.key === 'c'", html)
        self.assertIn("ev.key === 'Home'", html)
        self.assertIn("ev.key === 'End'", html)
        self.assertIn("dod-pos", html)
        self.assertIn("dod-neg", html)
        self.assertIn("model-top", html)
        self.assertIn("Escape", html)
        self.assertIn("id=\"kbdHelp\"", html)
        self.assertIn("getInitialStateFromHash", html)
        self.assertIn("history.replaceState", html)
        self.assertIn("window.addEventListener('keydown'", html)
        self.assertIn("isEditable", html)
        self.assertIn("selectedSpikeDate", html)

    def test_prepare_chart_series_groups_other(self):
        rows = [
            {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "a", "cost": 3}, {"modelName": "b", "cost": 1}]},
            {"date": "2026-03-02", "modelBreakdowns": [{"modelName": "a", "cost": 2}, {"modelName": "c", "cost": 4}]},
        ]
        labels, series, totals = prepare_chart_series(rows, top_models=1)
        self.assertEqual(labels, ["2026-03-01", "2026-03-02"])
        self.assertEqual(series["a"], [3.0, 2.0])
        self.assertEqual(series["Other"], [1.0, 4.0])
        self.assertEqual(totals, [4.0, 6.0])

    def test_build_model_table_rows_collapses_tail(self):
        ranked = [("a", 50.0), ("b", 30.0), ("c", 20.0)]
        html = build_model_table_rows(ranked, grand_total=100.0, max_rows=2)
        self.assertIn("<td>1</td><td>a</td>", html)
        self.assertIn("<td>2</td><td>b</td>", html)
        self.assertIn("Remaining 1 models", html)

    def test_dashboard_html_respects_max_table_rows(self):
        rows = [
            {
                "date": "2026-03-01",
                "modelBreakdowns": [
                    {"modelName": "m1", "cost": 3},
                    {"modelName": "m2", "cost": 2},
                    {"modelName": "m3", "cost": 1},
                ],
            }
        ]
        html = build_dashboard_html("codex", rows, top_models=3, max_table_rows=2)
        self.assertIn("Showing up to top 2 models", html)
        self.assertIn("Remaining 1 models", html)


if __name__ == "__main__":
    main()

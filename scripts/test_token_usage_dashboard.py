#!/usr/bin/env python3

from pathlib import Path
from unittest import TestCase, main

import token_usage_dashboard as dashboard_module

from token_usage_dashboard import (
    apply_access_policy,
    build_dashboard_html,
    build_model_table_rows,
    build_summary,
    build_llm_pattern_analysis,
    build_cost_attribution,
    build_optimization_recommendations,
    build_prompt_optimization_engine,
    detect_spikes,
    detect_cost_anomalies,
    evaluate_alert_rules,
    evaluate_realtime_cost_controls,
    evaluate_budget_allocation_and_permissions,
    evaluate_overage_behaviors,
    build_quota_policies,
    forecast_cost,
    parse_provider_list,
    build_multi_provider_aggregation,
    build_unified_cloud_cost_view,
    evaluate_unified_budget_alerts,
    downsample_rows,
    generate_custom_report,
    main as dashboard_main,
    manage_tenant_config,
    model_totals,
    prepare_chart_series,
    resolve_access_policy,
    resolve_multi_tenant_context,
    run_report_scheduler,
    dispatch_report_delivery,
    dispatch_event_alerts,
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

    def test_parse_provider_list_dedup_and_validation(self):
        self.assertEqual(parse_provider_list("codex,claude,codex"), ["codex", "claude"])
        with self.assertRaises(RuntimeError):
            parse_provider_list("codex,unknown")

    def test_build_multi_provider_aggregation_normalized_unified_totals(self):
        provider_rows = {
            "codex": [
                {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 2.0}]},
                {"date": "2026-03-02", "modelBreakdowns": [{"modelName": "o3", "cost": 1.0}]},
            ],
            "claude": [
                {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "sonnet", "cost": 3.0}]},
                {"date": "2026-03-02", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.5}]},
            ],
        }
        agg = build_multi_provider_aggregation(provider_rows)
        self.assertTrue(agg["available"])
        self.assertAlmostEqual(agg["totals"]["grandTotalCostUSD"], 7.5)
        self.assertEqual(agg["providers"][0]["provider"], "claude")
        self.assertAlmostEqual(agg["totals"]["models"]["gpt-5"], 3.5)
        self.assertEqual(len(agg["daily"]), 2)

    def test_dashboard_html_contains_multi_provider_unified_section(self):
        rows = [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}]}]
        agg = {
            "available": True,
            "providers": [{"provider": "codex", "totalCostUSD": 4.0}, {"provider": "claude", "totalCostUSD": 3.0}],
            "totals": {"grandTotalCostUSD": 7.0},
            "topModels": [{"model": "gpt-5", "totalCostUSD": 2.5}],
        }
        html = build_dashboard_html("codex", rows, top_models=2, multi_provider_agg=agg)
        self.assertIn("Multi-Provider Unified View", html)
        self.assertIn("Unified Top Models (cross-provider)", html)
        self.assertIn("Aggregated providers total", html)

    def test_unified_cloud_cost_view_merges_llm_and_cloud(self):
        rows = [
            {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 2.0}]},
            {"date": "2026-03-02", "modelBreakdowns": [{"modelName": "o3", "cost": 3.0}]},
        ]
        cloud_rows = [
            {"date": "2026-03-01", "provider": "aws", "service": "ec2", "costUSD": 5.0},
            {"date": "2026-03-02", "provider": "gcp", "service": "bigquery", "costUSD": 7.0},
        ]
        view = build_unified_cloud_cost_view(rows, cloud_rows)
        self.assertTrue(view["available"])
        self.assertTrue(view["cloudIntegrationEnabled"])
        self.assertAlmostEqual(view["totals"]["llmCostUSD"], 5.0)
        self.assertAlmostEqual(view["totals"]["cloudInfraCostUSD"], 12.0)
        self.assertAlmostEqual(view["totals"]["totalUnifiedCostUSD"], 17.0)
        self.assertEqual(len(view["daily"]), 2)

    def test_summary_cloud_integration_updates_hook_status(self):
        rows = [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}]}]
        cloud_rows = [{"date": "2026-03-01", "provider": "aws", "service": "ec2", "costUSD": 2.0}]
        summary = build_summary("codex", rows, cloud_cost_rows=cloud_rows)
        self.assertIn("unifiedCloudCostView", summary)
        self.assertTrue(summary["unifiedCloudCostView"]["cloudIntegrationEnabled"])
        hooks = summary["optimizationRecommendations"]["integrationHooks"]["cloudCostManagement"]
        self.assertEqual(hooks["awsCostExplorer"]["status"], "connected")
        self.assertEqual(hooks["gcpBilling"]["status"], "connected")

        html = build_dashboard_html("codex", rows, top_models=2, cloud_cost_rows=cloud_rows)
        self.assertIn("Unified Cloud Cost View (LLM + AWS/GCP)", html)
        self.assertIn("Cloud Provider", html)
        self.assertIn("Top Cloud Services", html)

    def test_evaluate_unified_budget_alerts_supports_total_provider_service_scope(self):
        rows = [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 5.0}]}]
        cloud_rows = [
            {"date": "2026-03-01", "provider": "aws", "service": "ec2", "costUSD": 7.0},
            {"date": "2026-03-01", "provider": "gcp", "service": "bigquery", "costUSD": 2.0},
        ]
        alerts = evaluate_unified_budget_alerts(
            rows,
            cloud_rows,
            config={
                "unifiedBudgetAlerts": [
                    {"id": "all", "scope": "total", "thresholdUSD": 10},
                    {"id": "aws", "scope": "provider", "provider": "aws", "thresholdUSD": 6},
                    {"id": "ec2", "scope": "service", "provider": "aws", "service": "ec2", "thresholdUSD": 6.5},
                ]
            },
        )
        self.assertTrue(alerts["available"])
        self.assertEqual(len(alerts["events"]), 3)
        scopes = {x["scope"] for x in alerts["events"]}
        self.assertIn("total", scopes)
        self.assertIn("provider:aws", scopes)
        self.assertIn("service:aws:ec2", scopes)

    def test_summary_dashboard_and_event_dispatch_include_unified_budget_alerts(self):
        from unittest.mock import patch

        rows = [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 4.0}]}]
        cloud_rows = [{"date": "2026-03-01", "provider": "aws", "service": "ec2", "costUSD": 4.0}]
        alert_cfg = {"unifiedBudgetAlerts": [{"id": "u1", "scope": "total", "thresholdUSD": 5}], "notificationChannels": ["slack:webhook:https://hooks.slack.com/services/x/y/z"]}
        summary = build_summary("codex", rows, alert_config=alert_cfg, cloud_cost_rows=cloud_rows)
        self.assertIn("unifiedBudgetAlerts", summary)
        self.assertGreaterEqual(len(summary["unifiedBudgetAlerts"]["events"]), 1)

        html = build_dashboard_html("codex", rows, top_models=2, alert_config=alert_cfg, cloud_cost_rows=cloud_rows)
        self.assertIn("Cross-platform Unified Budget Alerts", html)

        with patch("token_usage_dashboard._dispatch_webhook", return_value={"status": "sent", "httpStatus": 200}) as mocked:
            result = dispatch_event_alerts(summary, alert_config=alert_cfg)
        self.assertEqual(result["sent"], 1)
        self.assertEqual(mocked.call_count, 1)

    def test_normalize_cloud_cost_payload_supports_aws_and_gcp_shapes(self):
        aws_payload = {
            "ResultsByTime": [
                {
                    "TimePeriod": {"Start": "2026-03-01"},
                    "Groups": [
                        {"Keys": ["AmazonEC2"], "Metrics": {"UnblendedCost": {"Amount": "3.5", "Unit": "USD"}}}
                    ],
                }
            ]
        }
        gcp_payload = {
            "daily": [
                {"date": "2026-03-01", "service": "BigQuery", "cost": 2.25, "projectId": "acme-prod"}
            ]
        }

        aws_rows = dashboard_module._normalize_cloud_cost_payload(aws_payload)
        gcp_rows = dashboard_module._normalize_cloud_cost_payload(gcp_payload)
        self.assertEqual(aws_rows[0]["provider"], "aws")
        self.assertEqual(aws_rows[0]["source"], "aws_cost_explorer")
        self.assertEqual(gcp_rows[0]["provider"], "gcp")
        self.assertEqual(gcp_rows[0]["source"], "gcp_billing_export")

    def test_summary_includes_pattern_analysis(self):
        rows = [
            {
                "date": "2026-03-01",
                "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}],
                "llmCalls": [{"modelName": "gpt-5", "promptTokens": 10, "completionTokens": 20, "totalTokens": 30, "cost": 0.1, "prompt": "summarize logs"}],
            }
        ]
        summary = build_summary("codex", rows)
        self.assertIn("llmPatternAnalysis", summary)
        self.assertTrue(summary["llmPatternAnalysis"]["available"])
        self.assertIn("costAttribution", summary)
        self.assertTrue(summary["costAttribution"]["available"])
        self.assertIn("optimizationRecommendations", summary)
        self.assertTrue(summary["optimizationRecommendations"]["available"])

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

    def test_build_llm_pattern_analysis(self):
        rows = [
            {
                "date": "2026-03-01",
                "llmCalls": [
                    {
                        "modelName": "gpt-5",
                        "modelType": "chat",
                        "useCase": "support",
                        "userId": "alice",
                        "projectId": "proj-a",
                        "sessionId": "s1",
                        "promptTokens": 100,
                        "completionTokens": 200,
                        "totalTokens": 300,
                        "cost": 0.9,
                        "latencyMs": 500,
                        "prompt": "Please summarize Taiwan market outlook and risk factors",
                    },
                    {
                        "modelName": "o3",
                        "modelType": "reasoning",
                        "useCase": "analysis",
                        "userId": "bob",
                        "projectId": "proj-b",
                        "sessionId": "s2",
                        "promptTokens": 400,
                        "completionTokens": 120,
                        "totalTokens": 520,
                        "cost": 1.5,
                        "latencyMs": 900,
                        "prompt": "Analyze quarterly trend and optimization plan",
                    },
                ],
            }
        ]
        analysis = build_llm_pattern_analysis(rows)
        self.assertTrue(analysis["available"])
        self.assertEqual(analysis["calls"], 2)
        self.assertIn("dimensions", analysis)
        self.assertTrue(len(analysis["efficiency"]) >= 2)
        self.assertTrue(len(analysis["anonymizedPromptKeywords"]) > 0)

    def test_build_cost_attribution_unallocated_and_dimensions(self):
        rows = [
            {
                "date": "2026-03-01",
                "modelBreakdowns": [{"modelName": "gpt-5", "cost": 3.0}],
                "llmCalls": [
                    {
                        "modelName": "gpt-5",
                        "projectId": "proj-a",
                        "userId": "alice",
                        "department": "eng",
                        "application": "api-gateway",
                        "businessLine": "consumer",
                        "totalTokens": 100,
                        "cost": 1.2,
                    },
                    {
                        "modelName": "o3",
                        "projectId": "proj-b",
                        "userId": "bob",
                        "department": "sales",
                        "application": "crm",
                        "businessLine": "enterprise",
                        "totalTokens": 80,
                        "cost": 0.8,
                    },
                ],
            }
        ]
        attribution = build_cost_attribution(rows)
        self.assertTrue(attribution["available"])
        self.assertAlmostEqual(attribution["totalAttributedCostUSD"], 2.0)
        self.assertAlmostEqual(attribution["unallocatedCostUSD"], 1.0)
        self.assertIn("businessLine", attribution["dimensions"])
        self.assertEqual(attribution["dimensions"]["businessLine"][0]["key"], "consumer")

    def test_build_cost_attribution_share_sort_topn_and_unknown_fallback(self):
        rows = [
            {
                "date": "2026-03-01",
                "llmCalls": [
                    {"projectId": "proj-a", "cost": 3.0, "totalTokens": 30},
                    {"projectId": "proj-b", "cost": 2.0, "totalTokens": 20},
                    {"projectId": "proj-c", "cost": 1.0, "totalTokens": 10},
                    {"projectId": "proj-d", "cost": 0.5, "totalTokens": 5},
                    {"cost": 0.5, "totalTokens": 5},
                ],
            }
        ]
        attribution = build_cost_attribution(rows, top_n=3)
        projects = attribution["dimensions"]["project"]
        self.assertEqual([p["key"] for p in projects], ["proj-a", "proj-b", "proj-c"])
        share_sum = sum(p["sharePct"] for p in attribution["dimensions"]["project"])
        self.assertAlmostEqual(share_sum, 85.71428571428571)

        attribution_all = build_cost_attribution(rows, top_n=10)
        all_projects = attribution_all["dimensions"]["project"]
        self.assertIn("unknown", [p["key"] for p in all_projects])
        self.assertAlmostEqual(sum(p["sharePct"] for p in all_projects), 100.0)

    def test_build_optimization_recommendations_trigger_conditions(self):
        rows = [
            {
                "date": "2026-03-01",
                "llmCalls": [
                    {"projectId": "proj-a", "totalTokens": 100, "cost": 0.5},
                    {"projectId": "proj-a", "totalTokens": 110, "cost": 0.6},
                ] + [{"projectId": "proj-a", "totalTokens": 100, "cost": 0.2} for _ in range(25)],
            }
        ]
        pattern_analysis = {
            "efficiency": [
                {"model": "cheap", "costPer1kTokensUSD": 0.01, "totalTokens": 1000},
                {"model": "expensive", "costPer1kTokensUSD": 0.03, "totalTokens": 1000},
            ],
            "promptTokens": {"p50": 100, "p95": 250},
        }
        attribution = {
            "dimensions": {
                "project": [
                    {"key": "proj-a", "sharePct": 55.0},
                    {"key": "proj-b", "sharePct": 20.0},
                ]
            }
        }
        recs = build_optimization_recommendations(rows, pattern_analysis, attribution)
        self.assertTrue(recs["available"])
        rec_types = {r["type"] for r in recs["recommendations"]}
        self.assertIn("model_rightsizing", rec_types)
        self.assertIn("prompt_optimization", rec_types)
        self.assertIn("batching", rec_types)
        self.assertIn("budget_guardrail", rec_types)

    def test_budget_allocation_and_overage_behaviors(self):
        rows = [
            {
                "date": "2026-03-01",
                "llmCalls": [
                    {"projectId": "proj-a", "department": "eng", "userId": "alice", "modelName": "gpt-5", "cost": 1.4, "totalTokens": 100},
                    {"projectId": "proj-a", "department": "eng", "userId": "alice", "modelName": "gpt-5", "cost": 1.1, "totalTokens": 120},
                ],
            }
        ]
        cfg = {
            "allocations": [
                {"id": "eng-proj-a", "dimension": "project", "key": "proj-a", "budgetUSD": 2.0},
            ],
            "permissions": {
                "roles": {"analyst": {"allowedModels": ["gpt-5"]}},
                "users": {"alice": {"allowedModels": ["gpt-5"]}},
            },
            "overagePolicies": [
                {"thresholdPct": 100, "action": "degrade", "message": "project over budget"},
                {"thresholdPct": 120, "action": "switch_model", "routeToModel": "o3-mini", "message": "switch to cheaper model"},
            ],
        }
        budget = evaluate_budget_allocation_and_permissions(rows, config=cfg)
        self.assertTrue(budget["available"])
        self.assertEqual(budget["allocations"][0]["id"], "eng-proj-a")
        self.assertGreater(budget["allocations"][0]["usagePct"], 100.0)

        overage = evaluate_overage_behaviors(budget)
        self.assertTrue(overage["available"])
        self.assertEqual(overage["events"][0]["action"], "switch_model")
        self.assertEqual(overage["events"][0]["routeToModel"], "o3-mini")

    def test_build_quota_policies_and_auto_enforcement_summary(self):
        rows = [{
            "date": "2026-03-01",
            "llmCalls": [{"projectId": "proj-a", "userId": "alice", "modelName": "gpt-5", "cost": 2.5, "totalTokens": 300}],
        }]
        cfg = {
            "allocations": [{"id": "alloc-a", "dimension": "project", "key": "proj-a", "budgetUSD": 2.0}],
            "permissions": {"roles": {"viewer": {"allowedModels": ["o3-mini"]}}, "users": {"alice": {"role": "viewer"}}},
            "overagePolicies": [{"thresholdPct": 100, "action": "stop_calls", "message": "hard stop"}],
        }
        budget = evaluate_budget_allocation_and_permissions(rows, config=cfg)
        overage = evaluate_overage_behaviors(budget)
        quotas = build_quota_policies(budget, overage)

        self.assertTrue(quotas["available"])
        self.assertEqual(quotas["summary"]["allocationPolicies"], 1)
        self.assertEqual(quotas["summary"]["autoHandledEvents"], 1)
        self.assertEqual(quotas["enforcements"][0]["action"], "stop_calls")

    def test_build_prompt_optimization_engine_and_ab_plan(self):
        rows = [
            {
                "date": "2026-03-01",
                "llmCalls": [
                    {
                        "modelName": "gpt-5",
                        "projectId": "proj-a",
                        "promptTokens": 1000,
                        "completionTokens": 120,
                        "totalTokens": 1120,
                        "cost": 1.3,
                        "prompt": "請幫我分析這一整段歷史 log 並整理出重點與風險，附上完整上下文。",
                    },
                    {
                        "modelName": "gpt-5",
                        "projectId": "proj-a",
                        "promptTokens": 900,
                        "completionTokens": 130,
                        "totalTokens": 1030,
                        "cost": 1.1,
                        "prompt": "請幫我分析這一整段歷史 log 並整理出重點與風險，附上完整上下文。",
                    },
                ],
            }
        ]
        pattern = build_llm_pattern_analysis(rows)
        engine = build_prompt_optimization_engine(rows, pattern)
        self.assertTrue(engine["available"])
        self.assertEqual(engine["engineVersion"], "1.1")
        self.assertGreaterEqual(len(engine["highConsumptionPrompts"]), 1)
        self.assertGreaterEqual(len(engine["abTests"]), 1)
        first = engine["highConsumptionPrompts"][0]
        self.assertIn("rankScore", first)
        self.assertIn("suggestions", first)
        self.assertTrue(any(s["type"] == "model_rightsizing" for s in first["suggestions"]))

    def test_prompt_optimization_engine_respects_config(self):
        rows = [{
            "date": "2026-03-01",
            "llmCalls": [{
                "modelName": "gpt-5",
                "projectId": "proj-a",
                "promptTokens": 820,
                "completionTokens": 100,
                "totalTokens": 920,
                "cost": 1.0,
                "prompt": "長提示詞 A",
            }]
        }]
        pattern = build_llm_pattern_analysis(rows)
        cfg = {
            "maxPromptFamilies": 1,
            "minFamilyCalls": 1,
            "rankingWeights": {"costUSD": 1.0, "promptTokens": 0.2, "calls": 0.1, "promptToCompletionRatio": 0.5},
            "abTesting": {
                "trafficSplitB": 0.25,
                "costReductionPctMin": 12,
                "qualityDropPctMax": 1.5,
                "latencyIncreasePctMax": 8,
                "evaluationDays": 5,
                "minimumSampleSize": 60,
                "rolloutStages": [0.1, 0.2, 0.4],
            }
        }
        engine = build_prompt_optimization_engine(rows, pattern, config=cfg)
        self.assertEqual(engine["config"]["maxPromptFamilies"], 1)
        self.assertEqual(engine["abTests"][0]["trafficSplit"]["B"], 0.25)
        self.assertEqual(engine["abTests"][0]["successCriteria"]["costReductionPctMin"], 12.0)
        self.assertEqual(engine["abTests"][0]["executionPlan"]["evaluationDays"], 5)
        self.assertEqual(engine["abTests"][0]["executionPlan"]["minimumSampleSize"], 60)

    def test_budget_permissions_detect_violations(self):
        rows = [{
            "date": "2026-03-01",
            "llmCalls": [
                {"userId": "alice", "modelName": "gpt-5", "cost": 1.2, "totalTokens": 100},
                {"userId": "alice", "modelName": "gpt-4.1", "cost": 0.4, "totalTokens": 60},
            ],
        }]
        cfg = {
            "permissions": {
                "defaultRole": "viewer",
                "roles": {
                    "viewer": {"allowedModels": ["gpt-4.1"], "maxCostPerCallUSD": 0.5},
                },
                "users": {
                    "alice": {"role": "viewer"},
                },
            }
        }
        budget = evaluate_budget_allocation_and_permissions(rows, config=cfg)
        violations = budget["permissions"].get("violations", [])
        self.assertGreaterEqual(len(violations), 1)
        violation_types = {v["violation"] for v in violations}
        self.assertIn("model_not_allowed", violation_types)
        self.assertIn("call_cost_exceeded", violation_types)

    def test_build_summary_normalizes_call_records_once(self):
        from unittest.mock import patch

        rows = [{
            "date": "2026-03-01",
            "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}],
            "llmCalls": [{"modelName": "gpt-5", "totalTokens": 10, "cost": 0.2}],
        }]

        with patch("token_usage_dashboard._normalize_call_records", wraps=dashboard_module._normalize_call_records) as normalize_spy:
            summary = build_summary("codex", rows)
        self.assertIn("costAttribution", summary)
        self.assertIn("promptOptimizationEngine", summary)
        self.assertEqual(normalize_spy.call_count, 1)

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

    def test_forecast_and_alerts(self):
        rows = [
            {"date": f"2026-03-{d:02d}", "modelBreakdowns": [{"modelName": "gpt-5", "cost": float(d)}]}
            for d in range(1, 16)
        ]
        f7 = forecast_cost(rows, horizon_days=7, lookback_days=14)
        self.assertEqual(f7["horizonDays"], 7)
        self.assertGreater(f7["predictedTotalCostUSD"], 0.0)

        anomalies = detect_cost_anomalies(rows + [{"date": "2026-03-16", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 100.0}]}], lookback_days=7, z_threshold=1.5)
        self.assertGreaterEqual(len(anomalies), 1)

        alerts = evaluate_alert_rules(
            rows,
            f7,
            anomalies,
            config={
                "rules": {"budgetThresholdUSD": 10, "budgetForecastPct": 50, "anomalyCountThreshold": 1},
                "notificationChannels": ["email", "discord:webhook"],
            },
        )
        self.assertTrue(len(alerts["triggered"]) >= 1)
        self.assertIn("email", alerts["notificationChannels"])

    def test_build_summary_includes_forecast_and_alerts(self):
        rows = [
            {"date": f"2026-03-{d:02d}", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 5.0}]}
            for d in range(1, 15)
        ]
        summary = build_summary("codex", rows, alert_config={"rules": {"budgetThresholdUSD": 20, "budgetForecastPct": 80}})
        self.assertIn("forecast", summary)
        self.assertIn("costAnomalies", summary)
        self.assertIn("alerts", summary)
        self.assertIn("next7Days", summary["forecast"])

    def test_evaluate_realtime_cost_controls_triggers_multi_layers(self):
        rows = [
            {
                "date": "2026-03-01",
                "modelBreakdowns": [{"modelName": "gpt-5", "cost": 6.0}],
                "llmCalls": [
                    {"projectId": "proj-a", "cost": 4.0, "totalTokens": 100},
                    {"projectId": "proj-b", "cost": 1.0, "totalTokens": 30},
                ],
            }
        ]
        controls = evaluate_realtime_cost_controls(
            rows,
            forecast_7d={"predictedTotalCostUSD": 20.0},
            anomalies=[{"date": "2026-03-01", "zScore": 3.1}],
            config={
                "layers": [
                    {"id": "global-forecast", "metric": "forecast_7d_total_cost", "threshold": 10, "action": "degrade", "routeToModel": "o3-mini"},
                    {"id": "project-cap", "metric": "dimension_cost", "dimension": "project", "key": "proj-a", "threshold": 3.0, "action": "switch_model", "routeToModel": "gpt-4.1-mini"},
                    {"id": "anomaly-circuit", "metric": "anomaly_count", "threshold": 1, "action": "stop_calls", "stopReason": "anomaly_spike"},
                ]
            },
        )
        self.assertTrue(controls["available"])
        self.assertEqual(len(controls["layers"]), 3)
        self.assertEqual(len(controls["triggeredActions"]), 3)
        self.assertIn("project:proj-a", [x["scope"] for x in controls["triggeredActions"]])

    def test_summary_and_dashboard_include_realtime_cost_control_section(self):
        rows = [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 3.0}], "llmCalls": [{"projectId": "proj-a", "cost": 2.0, "totalTokens": 50}]}]
        summary = build_summary(
            "codex",
            rows,
            cost_control_config={
                "layers": [
                    {"id": "global-cap", "metric": "actual_total_cost", "threshold": 2.5, "action": "degrade", "routeToModel": "o3-mini"}
                ]
            },
        )
        self.assertIn("realTimeCostControls", summary)
        self.assertTrue(summary["realTimeCostControls"]["available"])

        html = build_dashboard_html("codex", rows, top_models=2, cost_control_config={"layers": [{"id": "global-cap", "metric": "actual_total_cost", "threshold": 2.5, "action": "degrade"}]})
        self.assertIn("Real-time Cost Control Strategy", html)
        self.assertIn("Triggered Control Actions", html)


    def test_dashboard_html_contains_all_pattern_sections(self):
        rows = [{
            "date": "2026-03-01",
            "modelBreakdowns": [{"modelName": "gpt-5", "cost": 3.0}],
            "llmCalls": [{
                "modelName": "gpt-5", "modelType": "chat", "projectId": "proj-a", "useCase": "qa", "userId": "alice", "sessionId": "sess-1", "workflowId": "wf-1",
                "promptTokens": 10, "completionTokens": 20, "totalTokens": 30, "cost": 0.2, "latencyMs": 200,
                "prompt": "contact me at user@example.com and trace 1234567890 id 550e8400-e29b-41d4-a716-446655440000"
            }]
        }]
        html = build_dashboard_html("codex", rows, top_models=2)
        for text in [
            "Prompt tokens", "Completion tokens", "By Model Type", "By Project",
            "Hotspots · Top API Calls", "Hotspots · Top Sessions", "Hotspots · Top Workflows",
            "Anonymized Prompt Keywords", "Cost Attribution & Optimization Recommendations",
            "Optimization Recommendations", "Attribution by Department", "Attribution by Business Line",
            "Prompt 優化建議引擎 · High-Consumption Prompt Families", "Prompt 優化建議引擎 · A/B Testing Plans"
        ]:
            self.assertIn(text, html)

    def test_anonymization_masks_sensitive_strings(self):
        rows = [{
            "date": "2026-03-01",
            "llmCalls": [{
                "modelName": "gpt-5", "promptTokens": 1, "completionTokens": 1, "totalTokens": 2, "cost": 0.01,
                "prompt": "Email user@example.com uuid 550e8400-e29b-41d4-a716-446655440000 number 987654321012"
            }]
        }]
        analysis = build_llm_pattern_analysis(rows)
        kws = {x["keyword"] for x in analysis["anonymizedPromptKeywords"]}
        self.assertIn("<email>", kws)
        self.assertIn("<uuid>", kws)
        self.assertIn("<number>", kws)
        for forbidden in ["user", "example", "550e8400", "987654321012"]:
            self.assertFalse(any(forbidden in k for k in kws))

    def test_large_dataset_performance_smoke(self):
        import time
        rows = []
        for d in range(1, 2001):
            rows.append({
                "date": f"2026-01-{((d - 1) % 28) + 1:02d}",
                "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}],
                "llmCalls": [{"modelName": "gpt-5", "promptTokens": 100, "completionTokens": 200, "totalTokens": 300, "cost": 0.1, "sessionId": f"s{d}", "workflowId": f"w{d}", "prompt": f"task {d}"}],
            })
        t0 = time.perf_counter()
        html = build_dashboard_html("codex", rows, top_models=3, chart_max_points=300)
        elapsed = time.perf_counter() - t0
        self.assertIn("LLM Usage Pattern Deep Analysis", html)
        self.assertLess(elapsed, 6.0)

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
        self.assertIn("Cost Forecast & Anomaly Alerts", html)
        self.assertIn("Detected Cost Anomalies", html)
        self.assertIn("Triggered Alerts", html)
        self.assertIn("LLM Usage Pattern Deep Analysis", html)
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

    def test_downsample_rows_keeps_bounds(self):
        rows = [{"date": f"2026-01-{d:02d}", "modelBreakdowns": []} for d in range(1, 32)]
        sampled = downsample_rows(rows, max_points=10)
        self.assertEqual(len(sampled), 10)
        self.assertEqual(sampled[0]["date"], rows[0]["date"])
        self.assertEqual(sampled[-1]["date"], rows[-1]["date"])

    def test_dashboard_html_shows_downsample_hint(self):
        rows = [
            {"date": f"2026-03-{d:02d}", "modelBreakdowns": [{"modelName": "m1", "cost": float(d)}]}
            for d in range(1, 21)
        ]
        html = build_dashboard_html("codex", rows, top_models=3, chart_max_points=8)
        self.assertIn("Chart points: 8/20", html)

    def test_custom_report_generation_metrics_models_granularity(self):
        rows = [
            {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 3.0}, {"modelName": "o3", "cost": 1.0}]},
            {"date": "2026-03-02", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 2.0}]},
            {"date": "2026-03-08", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 5.0}]},
        ]
        report = generate_custom_report(rows, metrics=["total_cost", "active_models", "avg_cost_per_model"], models=["gpt-5"], granularity="monthly")
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0]["period"], "2026-03")
        self.assertAlmostEqual(report[0]["totalCostUSD"], 10.0)

    def test_dashboard_html_contains_custom_report_builder(self):
        rows = [
            {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}]},
            {"date": "2026-03-02", "modelBreakdowns": [{"modelName": "o3", "cost": 2.0}]},
        ]
        html = build_dashboard_html("codex", rows, top_models=2)
        self.assertIn("Custom Report Builder", html)
        self.assertIn("id=\"reportGranularity\"", html)
        self.assertIn("id=\"generateReportBtn\"", html)
        self.assertIn("id=\"downloadReportCsvBtn\"", html)
        self.assertIn("id=\"customReportBody\"", html)
        self.assertIn("generateCustomReportRows", html)
        self.assertIn("initCustomReportBuilder", html)

    def test_apply_access_policy_viewer_hides_breakdown(self):
        rows = [
            {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 3.0}, {"modelName": "o3", "cost": 1.0}]},
        ]
        _, policy = resolve_access_policy("viewer", None, None)
        filtered = apply_access_policy(rows, policy)
        self.assertEqual(filtered[0]["modelBreakdowns"], [])
        self.assertAlmostEqual(filtered[0]["totalCost"], 4.0)

    def test_apply_access_policy_allowed_models(self):
        rows = [
            {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 3.0}, {"modelName": "o3", "cost": 1.0}]},
        ]
        policy = {
            "canViewModelBreakdown": True,
            "canViewModelNames": True,
            "allowedModels": ["o3"],
        }
        filtered = apply_access_policy(rows, policy)
        self.assertEqual(len(filtered[0]["modelBreakdowns"]), 1)
        self.assertEqual(filtered[0]["modelBreakdowns"][0]["modelName"], "o3")
        self.assertAlmostEqual(filtered[0]["totalCost"], 1.0)


    def test_resolve_multi_tenant_context_isolates_org_data(self):
        payload = {
            "organizations": {
                "org-a": {"daily": [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}]}]},
                "org-b": {"daily": [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "o3", "cost": 9.0}]}]},
            }
        }
        import json, tempfile, os
        cfg = {
            "organizations": {
                "org-a": {
                    "defaultRole": "viewer",
                    "users": {"alice": {"role": "admin", "group": "eng"}},
                    "groups": {"eng": {"dashboardViews": ["eng-core"]}},
                    "dashboardViews": {"eng-core": {"allowedModels": ["gpt-5"]}},
                },
                "org-b": {"users": {}, "groups": {}, "dashboardViews": {}},
            }
        }
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.json') as f:
            f.write(json.dumps(cfg))
            path = f.name
        try:
            rows, role, policy, meta = resolve_multi_tenant_context(payload, path, 'org-a', None, 'alice', 'eng-core')
            self.assertEqual(role, 'admin')
            self.assertEqual(meta['organizationId'], 'org-a')
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['modelBreakdowns'][0]['modelName'], 'gpt-5')
            self.assertEqual(policy.get('allowedModels'), ['gpt-5'])
        finally:
            os.unlink(path)

    def test_manage_tenant_config_user_and_view(self):
        import json, tempfile, os
        cfg = {
            "organizations": {
                "org-a": {"users": {}, "groups": {"analytics": {"dashboardViews": []}}, "dashboardViews": {}}
            }
        }
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.json') as f:
            f.write(json.dumps(cfg))
            path = f.name
        try:
            manage_tenant_config(path, 'org-a', 'create', 'bob', 'analyst', 'analytics', None, None, None, None, None)
            manage_tenant_config(path, 'org-a', None, None, None, None, 'create', 'ops-view', 'gpt-5,o3', 30, None)
            result = manage_tenant_config(path, 'org-a', None, None, None, None, 'assign', 'ops-view', None, None, 'analytics')
            self.assertIn('bob', result['users'])
            self.assertIn('ops-view', result['views'])
            data = json.loads(Path(path).read_text())
            self.assertIn('ops-view', data['organizations']['org-a']['groups']['analytics']['dashboardViews'])
        finally:
            os.unlink(path)

    def test_manage_mode_does_not_require_input_payload(self):
        import json, os, sys, tempfile
        from unittest.mock import patch

        cfg = {"organizations": {"org-a": {"users": {}, "groups": {}, "dashboardViews": {}}}}
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.json') as f:
            f.write(json.dumps(cfg))
            path = f.name
        try:
            argv = [
                "token_usage_dashboard.py",
                "--tenant-config", path,
                "--org-id", "org-a",
                "--manage-users", "list",
            ]
            with patch.object(sys, "argv", argv), patch("token_usage_dashboard.load_payload", side_effect=RuntimeError("should not load payload")):
                rc = dashboard_main()
            self.assertEqual(rc, 0)
        finally:
            os.unlink(path)

    def test_multi_tenant_user_without_group_is_deny_by_default(self):
        import json, os, tempfile

        payload = {"organizations": {"org-a": {"daily": [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 2.0}]}]}}}
        cfg = {
            "organizations": {
                "org-a": {
                    "users": {"alice": {"role": "analyst"}},
                    "groups": {},
                    "dashboardViews": {"team": {"allowedModels": ["gpt-5"]}},
                }
            }
        }
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.json') as f:
            f.write(json.dumps(cfg))
            path = f.name
        try:
            with self.assertRaises(RuntimeError):
                resolve_multi_tenant_context(payload, path, "org-a", None, "alice", "team")
            rows, role, policy, _ = resolve_multi_tenant_context(payload, path, "org-a", None, "alice", None)
            self.assertEqual(role, "analyst")
            self.assertEqual(policy.get("allowedModels"), [])
            filtered = apply_access_policy(rows, policy)
            self.assertAlmostEqual(filtered[0]["totalCost"], 0.0)
        finally:
            os.unlink(path)

    def test_multi_tenant_role_override_requires_allow_flag(self):
        import json, os, tempfile

        payload = {"organizations": {"org-a": {"daily": [{"date": "2026-03-01", "modelBreakdowns": []}]}}}
        cfg = {
            "organizations": {
                "org-a": {
                    "users": {"alice": {"role": "viewer", "defaultDashboard": "team"}},
                    "groups": {},
                    "dashboardViews": {"team": {"allowedModels": ["gpt-5"]}},
                }
            }
        }
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.json') as f:
            f.write(json.dumps(cfg))
            path = f.name
        try:
            _, role_ignored, _, _ = resolve_multi_tenant_context(payload, path, "org-a", "admin", "alice", None)
            _, role_override, _, _ = resolve_multi_tenant_context(payload, path, "org-a", "admin", "alice", None, allow_role_override=True)
            self.assertEqual(role_ignored, "viewer")
            self.assertEqual(role_override, "admin")
        finally:
            os.unlink(path)

    def test_assign_nonexistent_view_fails(self):
        import json, os, tempfile

        cfg = {
            "organizations": {
                "org-a": {"users": {}, "groups": {"analytics": {"dashboardViews": []}}, "dashboardViews": {}}
            }
        }
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.json') as f:
            f.write(json.dumps(cfg))
            path = f.name
        try:
            with self.assertRaises(RuntimeError):
                manage_tenant_config(path, "org-a", None, None, None, None, "assign", "missing-view", None, None, "analytics")
        finally:
            os.unlink(path)

    def test_run_report_scheduler_generates_artifacts_and_history(self):
        import json, os, tempfile
        from datetime import datetime
        from zoneinfo import ZoneInfo

        payload = {
            "provider": "codex",
            "daily": [
                {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.5}]},
                {"date": "2026-03-02", "modelBreakdowns": [{"modelName": "o3", "cost": 2.0}]},
            ],
        }
        config = {
            "jobs": [
                {
                    "id": "finance-weekly",
                    "name": "Finance Weekly",
                    "frequency": "weekly",
                    "granularity": "weekly",
                    "metrics": ["total_cost", "active_models"],
                    "formats": ["json", "csv"],
                    "recipients": [{"channel": "email", "target": "finance@example.com"}],
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_report_scheduler(
                payload=payload,
                provider="codex",
                config=config,
                output_dir=Path(tmpdir),
                now=datetime(2026, 3, 12, 12, 0, 0, tzinfo=ZoneInfo("UTC")),
            )
            self.assertEqual(result["generated"], 1)
            history_path = Path(tmpdir) / "report_history.json"
            self.assertTrue(history_path.exists())
            history = json.loads(history_path.read_text())
            self.assertEqual(len(history["reports"]), 1)
            artifacts = history["reports"][0]["artifacts"]
            self.assertTrue(Path(artifacts["json"]).exists())
            self.assertTrue(Path(artifacts["csv"]).exists())

    def test_run_report_scheduler_blocks_unauthorized_recipient(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        import tempfile

        payload = {"provider": "codex", "daily": [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}]}]}
        config = {
            "jobs": [
                {
                    "id": "viewer-only",
                    "frequency": "daily",
                    "role": "viewer",
                    "allowedRoles": ["admin"],
                    "recipients": [{"channel": "email", "target": "ops@example.com"}],
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_report_scheduler(
                payload=payload,
                provider="codex",
                config=config,
                output_dir=Path(tmpdir),
                now=datetime(2026, 3, 12, 12, 0, 0, tzinfo=ZoneInfo("UTC")),
            )
            deliveries = result["jobs"][0]["deliveries"]
            self.assertEqual(deliveries[0]["status"], "blocked")
            self.assertEqual(deliveries[0]["reason"], "role_not_allowed")

    def test_dispatch_report_delivery_supports_slack_and_discord(self):
        from unittest.mock import patch

        payload = {
            "provider": "codex",
            "generatedAt": "2026-03-19T10:00:00+00:00",
            "job": {"id": "daily", "name": "Daily"},
            "summary": {"startDate": "2026-03-01", "endDate": "2026-03-19", "totalCostUSD": 12.3, "last7dCostUSD": 4.5},
        }

        with patch("token_usage_dashboard._dispatch_webhook", return_value={"status": "sent", "httpStatus": 200}) as mocked:
            slack = dispatch_report_delivery(payload, {"channel": "slack", "target": "https://hooks.slack.com/services/x/y/z"})
            discord = dispatch_report_delivery(payload, {"channel": "discord", "target": "https://discord.com/api/webhooks/x"})

        self.assertEqual(slack["status"], "sent")
        self.assertEqual(discord["status"], "sent")
        self.assertEqual(mocked.call_count, 2)

    def test_dispatch_event_alerts_aggregates_alerts_controls_overage(self):
        from unittest.mock import patch

        summary = {
            "provider": "codex",
            "startDate": "2026-03-01",
            "endDate": "2026-03-19",
            "totalCostUSD": 123.0,
            "alerts": {"triggered": [{"severity": "high", "message": "budget exceeded"}]},
            "realTimeCostControls": {"triggeredActions": [{"action": "degrade", "message": "route to cheaper model"}]},
            "overageBehaviors": {"events": [{"action": "switch_model", "dimension": "project", "key": "proj-a", "usagePct": 133.0}]},
        }
        cfg = {"notificationChannels": ["slack:webhook:https://hooks.slack.com/services/x/y/z"]}

        with patch("token_usage_dashboard._dispatch_webhook", return_value={"status": "sent", "httpStatus": 200}) as mocked:
            result = dispatch_event_alerts(summary, alert_config=cfg)

        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertGreaterEqual(result["events"], 3)
        self.assertEqual(mocked.call_count, 1)

    def test_scheduler_cli_requires_config(self):
        import sys
        from unittest.mock import patch

        argv = ["token_usage_dashboard.py", "--run-report-scheduler"]
        with patch.object(sys, "argv", argv), patch("token_usage_dashboard.load_payload", return_value={"daily": []}):
            rc = dashboard_main()
        self.assertEqual(rc, 8)

    def test_alert_config_parsing_tolerates_invalid_numbers(self):
        rows = [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}]}]
        alerts = evaluate_alert_rules(
            rows,
            {"predictedTotalCostUSD": 100.0},
            [{"date": "2026-03-01", "zScore": "not-a-number"}],
            config={
                "rules": {
                    "budgetThresholdUSD": "bad-threshold",
                    "budgetForecastPct": "85.5",
                    "anomalyCountThreshold": "abc",
                },
                "notificationChannels": ["email", 123],
            },
        )
        self.assertEqual(alerts["notificationChannels"], ["email", "123"])
        self.assertTrue(any(a["rule"] == "anomaly_count_threshold" for a in alerts["triggered"]))

    def test_dashboard_html_escapes_xss_dynamic_strings(self):
        evil = "<img src=x onerror=alert(1)>"
        rows = [{
            "date": "2026-03-01",
            "modelBreakdowns": [{"modelName": evil, "cost": 3.0}],
            "llmCalls": [{"modelName": evil, "task": evil, "promptTokens": 1, "completionTokens": 1, "totalTokens": 2, "cost": 0.1}],
        }]
        html = build_dashboard_html(
            f"codex</title><script>alert(9)</script>",
            rows,
            top_models=3,
            role_name="viewer<script>alert(8)</script>",
            alert_config={
                "rules": {"budgetThresholdUSD": 1, "budgetForecastPct": 1, "anomalyCountThreshold": 1},
                "notificationChannels": ["slack<script>alert(7)</script>", evil],
            },
        )
        self.assertNotIn("<script>alert(9)</script>", html)
        self.assertNotIn("<td><img src=x onerror=alert(1)>", html)
        self.assertIn("&lt;img src=x onerror=alert(1)&gt;", html)
        self.assertIn("escapeHtml", html)

    def test_summary_and_dashboard_include_budget_and_overage_sections(self):
        rows = [{
            "date": "2026-03-01",
            "modelBreakdowns": [{"modelName": "gpt-5", "cost": 2.5}],
            "llmCalls": [
                {"projectId": "proj-a", "department": "eng", "userId": "alice", "modelName": "gpt-5", "cost": 2.5, "totalTokens": 300}
            ],
        }]
        budget_cfg = {
            "allocations": [{"id": "alloc-a", "dimension": "project", "key": "proj-a", "budgetUSD": 2.0}],
            "permissions": {
                "roles": {"viewer": {"allowedModels": ["gpt-4.1"], "maxCostPerCallUSD": 1.0}},
                "users": {"alice": {"role": "viewer"}},
            },
            "overagePolicies": [{"thresholdPct": 100, "action": "stop_calls", "message": "hard stop"}],
        }
        summary = build_summary("codex", rows, budget_config=budget_cfg)
        self.assertIn("budgetAllocation", summary)
        self.assertIn("quotaPolicies", summary)
        self.assertIn("overageBehaviors", summary)
        self.assertEqual(summary["overageBehaviors"]["events"][0]["action"], "stop_calls")

        html = build_dashboard_html("codex", rows, top_models=2, budget_config=budget_cfg)
        self.assertIn("Budget Allocation & Permission Management", html)
        self.assertIn("Role Permission Matrix", html)
        self.assertIn("Permission Violations (Detected from call logs)", html)
        self.assertIn("Overage Handling", html)
        self.assertIn("Dashboard Policy View", html)
        self.assertIn("Auto Enforcement Actions", html)
        self.assertIn("proj-a", html)

    def test_dashboard_html_escapes_alert_fields(self):
        rows = [{"date": f"2026-03-{d:02d}", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 10.0}]} for d in range(1, 10)]
        anomalies = [{"date": "2026-03-09", "costUSD": 99.0, "zScore": 9.1, "severity": "<b>high</b>"}]
        alerts = evaluate_alert_rules(
            rows,
            {"predictedTotalCostUSD": 100.0},
            anomalies,
            config={"rules": {"budgetThresholdUSD": 1, "budgetForecastPct": 1, "anomalyCountThreshold": 1}, "notificationChannels": ["discord</td><script>alert(4)</script>"]},
        )
        html = build_dashboard_html("codex", rows, top_models=2, alert_config={"rules": alerts["rules"], "notificationChannels": alerts["notificationChannels"]})
        self.assertIn("&lt;/td&gt;&lt;script&gt;alert(4)&lt;/script&gt;", html)

    def test_load_budget_config_normalizes_and_filters_invalid_entries(self):
        import json
        import tempfile

        cfg = {
            "allocations": [
                {"id": "ok", "dimension": "project", "key": "proj-a", "budgetUSD": 20},
                {"id": "bad-dim", "dimension": "bad", "key": "proj-x", "budgetUSD": 20},
                {"id": "bad-budget", "dimension": "project", "key": "proj-y", "budgetUSD": 0},
            ],
            "permissions": {
                "defaultRole": "viewer",
                "roles": {
                    "analyst": {"allowedModels": ["gpt-5", "", "gpt-5"], "maxCostPerCallUSD": -1},
                },
                "users": {
                    "alice": {"role": "analyst", "allowedModels": ["o3", "o3"], "maxCostPerCallUSD": 0.8},
                },
            },
            "overagePolicies": [
                {"thresholdPct": 150, "action": "stop_calls"},
                {"thresholdPct": 120, "action": "switch_model", "routeToModel": "o3-mini"},
                {"thresholdPct": 100, "action": "unknown-action"},
            ],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir) / "budget.json"
            p.write_text(json.dumps(cfg), encoding="utf-8")
            parsed = dashboard_module._load_budget_config(str(p))

        self.assertEqual(len(parsed["allocations"]), 1)
        self.assertEqual(parsed["allocations"][0]["id"], "ok")
        self.assertEqual(parsed["permissions"]["roles"]["analyst"]["allowedModels"], ["gpt-5"])
        self.assertEqual(parsed["permissions"]["roles"]["analyst"]["maxCostPerCallUSD"], 0.0)
        self.assertEqual(parsed["permissions"]["users"]["alice"]["allowedModels"], ["o3"])
        self.assertEqual(parsed["overagePolicies"][0]["thresholdPct"], 100.0)
        self.assertEqual(parsed["overagePolicies"][0]["action"], "warn")

    def test_load_prompt_optimization_config_normalizes_ranges(self):
        import json
        import tempfile

        cfg = {
            "maxPromptFamilies": 0,
            "compressionThresholdPromptTokens": 700,
            "abTesting": {
                "trafficSplitB": 3,
                "costReductionPctMin": 11,
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir) / "prompt.json"
            p.write_text(json.dumps(cfg), encoding="utf-8")
            parsed = dashboard_module._load_prompt_optimization_config(str(p))

        self.assertEqual(parsed["maxPromptFamilies"], 1)
        self.assertEqual(parsed["compressionThresholdPromptTokens"], 700.0)
        self.assertEqual(parsed["abTesting"]["trafficSplitB"], 0.9)
        self.assertEqual(parsed["abTesting"]["costReductionPctMin"], 11.0)

    def test_prompt_optimization_engine_filters_min_family_calls(self):
        rows = [{
            "date": "2026-03-01",
            "llmCalls": [
                {"modelName": "gpt-5", "projectId": "proj-a", "promptTokens": 1000, "completionTokens": 100, "cost": 1.2, "prompt": "family alpha prompt"},
                {"modelName": "gpt-5", "projectId": "proj-a", "promptTokens": 1000, "completionTokens": 100, "cost": 1.0, "prompt": "family alpha prompt"},
                {"modelName": "gpt-5", "projectId": "proj-b", "promptTokens": 900, "completionTokens": 120, "cost": 0.9, "prompt": "family beta prompt"},
            ],
        }]
        pattern = build_llm_pattern_analysis(rows)
        engine = build_prompt_optimization_engine(rows, pattern, config={"minFamilyCalls": 2})

        self.assertTrue(engine["available"])
        self.assertEqual(len(engine["highConsumptionPrompts"]), 1)
        self.assertEqual(engine["highConsumptionPrompts"][0]["calls"], 2)

    def test_cloud_tag_mapping_and_unified_tag_views(self):
        rows = [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 3.0}]}]
        cloud_rows = [
            {"date": "2026-03-01", "provider": "aws", "service": "AmazonEC2", "costUSD": 8.0, "tags": {"cost_center": "finops", "env": "prod"}},
            {"date": "2026-03-01", "provider": "aws", "service": "AmazonS3", "costUSD": 2.0, "tags": {"cost_center": "finops"}},
        ]
        mapped = dashboard_module._apply_cloud_tag_mapping(cloud_rows, {"cost_center": "businessLine"})
        self.assertEqual(mapped[0]["businessLine"], "finops")

        view = build_unified_cloud_cost_view(rows, mapped)
        self.assertTrue(any(x["tag"] == "cost_center=finops" for x in view["cloudTags"]))

        alerts = evaluate_unified_budget_alerts(
            rows,
            mapped,
            config={"unifiedBudgetAlerts": [{"id": "tag-finops", "scope": "tag", "tagKey": "cost_center", "tagValue": "finops", "thresholdUSD": 9}]},
        )
        self.assertEqual(alerts["events"][0]["scope"], "tag:cost_center=finops")

    def test_detailed_attribution_includes_fine_grained_and_cloud_dimensions(self):
        rows = [{
            "date": "2026-03-01",
            "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}],
            "llmCalls": [{"modelName": "gpt-5", "workflowId": "wf-1", "sessionId": "s-1", "projectId": "proj-a", "cost": 1.0, "totalTokens": 100}],
        }]
        cloud_rows = [{"date": "2026-03-01", "provider": "aws", "service": "AmazonEC2", "costUSD": 3.0, "project": "infra-core", "source": "aws_cost_explorer", "environment": "prod", "tags": {"env": "prod"}}]

        attr = dashboard_module.build_cost_attribution(rows, cloud_rows=cloud_rows, granularity="detailed")
        self.assertIn("workflow", attr["dimensions"])
        self.assertIn("cloudProvider", attr["dimensions"])
        self.assertIn("cloudTag", attr["dimensions"])
        self.assertIn("cloudProject", attr["dimensions"])
        self.assertIn("cloudSource", attr["dimensions"])
        self.assertIn("cloudEnvironment", attr["dimensions"])

        html = build_dashboard_html("codex", rows, top_models=2, cloud_cost_rows=cloud_rows, attribution_granularity="detailed")
        self.assertIn("Cloud Attribution by Tag", html)
        self.assertIn("Cloud Attribution by Project", html)
        self.assertIn("Cloud Attribution by Source", html)
        self.assertIn("Cloud Attribution by Environment", html)
        self.assertIn("Attribution by Workflow (Detailed)", html)

    def test_cloud_tag_mapping_rules_support_alias_valuemap_and_default(self):
        cloud_rows = [
            {"date": "2026-03-01", "provider": "aws", "service": "AmazonEC2", "costUSD": 3.0, "tags": {"owner_team": "plat", "env": "prod"}},
            {"date": "2026-03-02", "provider": "gcp", "service": "BigQuery", "costUSD": 2.0, "tags": {"cost-center": "finops-core"}},
        ]
        mapping_cfg = {
            "mapping": {"env": "environment"},
            "rules": [
                {"target": "department", "from": ["owner_team", "team"], "valueMap": {"plat": "platform"}},
                {"target": "businessLine", "tag": "cost_centre", "aliases": ["cost-center"], "valueMap": {"finops-core": "finops"}},
                {"target": "environment", "from": ["environment", "env"], "default": "shared"},
            ],
        }
        mapped = dashboard_module._apply_cloud_tag_mapping(cloud_rows, mapping_cfg)
        self.assertEqual(mapped[0]["department"], "platform")
        self.assertEqual(mapped[0]["environment"], "prod")
        self.assertEqual(mapped[1]["businessLine"], "finops")
        self.assertEqual(mapped[1]["environment"], "shared")

    def test_run_report_scheduler_only_on_change_and_history_retention(self):
        import json
        import tempfile
        from datetime import datetime
        from zoneinfo import ZoneInfo

        payload = {
            "provider": "codex",
            "daily": [
                {"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.5}]},
                {"date": "2026-03-02", "modelBreakdowns": [{"modelName": "o3", "cost": 2.0}]},
            ],
        }
        config = {
            "history": {"maxReportsPerJob": 1},
            "jobs": [{"id": "finance", "frequency": "daily", "onlyOnChange": True, "formats": ["json"]}],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            first = run_report_scheduler(
                payload=payload,
                provider="codex",
                config=config,
                output_dir=Path(tmpdir),
                now=datetime(2026, 3, 12, 12, 0, 0, tzinfo=ZoneInfo("UTC")),
            )
            self.assertEqual(first["generated"], 1)

            second = run_report_scheduler(
                payload=payload,
                provider="codex",
                config=config,
                output_dir=Path(tmpdir),
                now=datetime(2026, 3, 13, 12, 0, 0, tzinfo=ZoneInfo("UTC")),
            )
            self.assertEqual(second["generated"], 0)
            self.assertEqual(second["jobs"][0]["reason"], "no_change")

            payload_changed = {
                "provider": "codex",
                "daily": payload["daily"] + [{"date": "2026-03-03", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 5.0}]}],
            }
            third = run_report_scheduler(
                payload=payload_changed,
                provider="codex",
                config=config,
                output_dir=Path(tmpdir),
                now=datetime(2026, 3, 14, 12, 0, 0, tzinfo=ZoneInfo("UTC")),
            )
            self.assertEqual(third["generated"], 1)

            history = json.loads((Path(tmpdir) / "report_history.json").read_text())
            self.assertEqual(len(history["reports"]), 1)
            self.assertTrue(history["latestByJob"]["finance"]["fingerprint"])


    def test_cost_attribution_includes_dynamic_cloud_mapped_dimensions(self):
        rows = [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}], "llmCalls": [{"modelName": "gpt-5", "cost": 1.0, "totalTokens": 100}]}]
        cloud_rows = [
            {"date": "2026-03-01", "provider": "aws", "service": "ec2", "costUSD": 3.0, "businessUnit": "retail", "ownerTeam": "platform"},
            {"date": "2026-03-01", "provider": "aws", "service": "s3", "costUSD": 2.0, "businessUnit": "retail", "ownerTeam": "data"},
        ]
        attr = build_cost_attribution(rows, cloud_rows=cloud_rows, granularity="detailed")
        self.assertIn("cloudMapped:businessUnit", attr["dimensions"])
        self.assertIn("cloudMapped:ownerTeam", attr["dimensions"])
        html = build_dashboard_html("codex", rows, top_models=2, cloud_cost_rows=cloud_rows, attribution_granularity="detailed")
        self.assertIn("Cloud Attribution by Mapped Dimension · businessUnit", html)
        self.assertIn("Cloud Attribution by Mapped Dimension · ownerTeam", html)

    def test_scheduler_deduplicates_duplicate_recipients(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        import tempfile

        payload = {"provider": "codex", "daily": [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 1.0}]}]}
        config = {
            "dispatch": {"enabled": False},
            "jobs": [
                {
                    "id": "dup-recipients",
                    "frequency": "daily",
                    "recipients": [
                        {"channel": "slack", "target": "https://hooks.slack.com/services/x/y/z"},
                        {"channel": "slack", "target": "https://hooks.slack.com/services/x/y/z"},
                    ],
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_report_scheduler(
                payload=payload,
                provider="codex",
                config=config,
                output_dir=Path(tmpdir),
                now=datetime(2026, 3, 12, 12, 0, 0, tzinfo=ZoneInfo("UTC")),
            )
        deliveries = result["jobs"][0]["deliveries"]
        self.assertEqual(deliveries[0]["status"], "queued")
        self.assertEqual(deliveries[1]["status"], "skipped")
        self.assertEqual(deliveries[1]["reason"], "duplicate_recipient")

    def test_scheduler_min_total_cost_change_pct_skips_small_changes(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        import tempfile

        payload = {"provider": "codex", "daily": [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 10.0}]}]}
        config = {
            "jobs": [
                {
                    "id": "change-threshold",
                    "frequency": "daily",
                    "formats": ["json"],
                    "onlyOnChange": False,
                    "minTotalCostChangePct": 5,
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            first = run_report_scheduler(
                payload=payload,
                provider="codex",
                config=config,
                output_dir=Path(tmpdir),
                now=datetime(2026, 3, 12, 12, 0, 0, tzinfo=ZoneInfo("UTC")),
            )
            self.assertEqual(first["generated"], 1)

            payload_small_change = {"provider": "codex", "daily": [{"date": "2026-03-01", "modelBreakdowns": [{"modelName": "gpt-5", "cost": 10.2}]}]}
            second = run_report_scheduler(
                payload=payload_small_change,
                provider="codex",
                config=config,
                output_dir=Path(tmpdir),
                now=datetime(2026, 3, 13, 12, 0, 0, tzinfo=ZoneInfo("UTC")),
            )
            self.assertEqual(second["generated"], 0)
            self.assertEqual(second["jobs"][0]["reason"], "change_below_threshold")



if __name__ == "__main__":
    main()

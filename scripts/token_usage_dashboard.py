#!/usr/bin/env python3
"""
Generate a local HTML dashboard for CodexBar model usage/cost data.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import webbrowser
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_ROLE_POLICIES: Dict[str, Dict[str, Any]] = {
    "admin": {
        "canViewTotals": True,
        "canViewModelBreakdown": True,
        "canViewModelNames": True,
        "canViewSpikes": True,
        "canViewMovers": True,
        "canUseCustomReportBuilder": True,
    },
    "analyst": {
        "canViewTotals": True,
        "canViewModelBreakdown": True,
        "canViewModelNames": True,
        "canViewSpikes": True,
        "canViewMovers": True,
        "canUseCustomReportBuilder": True,
    },
    "viewer": {
        "canViewTotals": True,
        "canViewModelBreakdown": False,
        "canViewModelNames": False,
        "canViewSpikes": False,
        "canViewMovers": False,
        "canUseCustomReportBuilder": False,
    },
}


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def run_codexbar_cost(provider: str) -> Dict[str, Any]:
    cmd = ["codexbar", "cost", "--format", "json", "--provider", provider]
    try:
        output = subprocess.check_output(cmd, text=True)
    except FileNotFoundError:
        raise RuntimeError("codexbar not found on PATH. Install CodexBar CLI first.")
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"codexbar cost failed (exit {exc.returncode}).")

    try:
        raw = json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse codexbar JSON output: {exc}")

    return normalize_provider_payload(raw, provider)


def normalize_provider_payload(raw: Any, provider: str) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        for entry in raw:
            if isinstance(entry, dict) and entry.get("provider") == provider:
                return entry
        raise RuntimeError(f"Provider '{provider}' not found in codexbar payload.")
    raise RuntimeError("Unsupported JSON input format.")


def load_payload(input_path: Optional[str], provider: str) -> Dict[str, Any]:
    if not input_path:
        return run_codexbar_cost(provider)

    if input_path == "-":
        raw = sys.stdin.read()
    else:
        raw = Path(input_path).read_text(encoding="utf-8")

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse input JSON: {exc}")

    return normalize_provider_payload(parsed, provider)


def parse_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return parsed


def positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return parsed


def parse_daily(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    daily = payload.get("daily")
    if not isinstance(daily, list):
        return []
    rows = [x for x in daily if isinstance(x, dict) and isinstance(x.get("date"), str)]
    return sorted(rows, key=lambda r: r.get("date") or "")


def filter_days(rows: List[Dict[str, Any]], days: Optional[int]) -> List[Dict[str, Any]]:
    if not days or not rows:
        return rows
    cutoff = date.today() - timedelta(days=days - 1)
    out: List[Dict[str, Any]] = []
    for row in rows:
        d = parse_date(row.get("date", ""))
        if d and d >= cutoff:
            out.append(row)
    return out


def downsample_rows(rows: List[Dict[str, Any]], max_points: int) -> List[Dict[str, Any]]:
    """Keep chart points bounded for large datasets while preserving trend shape."""
    if max_points < 2 or len(rows) <= max_points:
        return rows

    step = (len(rows) - 1) / (max_points - 1)
    out: List[Dict[str, Any]] = []
    last_idx = -1
    for i in range(max_points):
        idx = int(round(i * step))
        idx = min(len(rows) - 1, max(0, idx))
        if idx != last_idx:
            out.append(rows[idx])
            last_idx = idx
    if out[-1] is not rows[-1]:
        out[-1] = rows[-1]
    return out


def model_totals(rows: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    totals: Dict[str, float] = defaultdict(float)
    for row in rows:
        breakdowns = row.get("modelBreakdowns")
        if not isinstance(breakdowns, list):
            continue
        for b in breakdowns:
            if not isinstance(b, dict):
                continue
            name = b.get("modelName")
            cost = b.get("cost")
            if isinstance(name, str) and isinstance(cost, (int, float)):
                totals[name] += float(cost)
    return dict(totals)


def day_total_cost(row: Dict[str, Any]) -> float:
    if isinstance(row.get("totalCost"), (int, float)):
        return float(row["totalCost"])
    breakdowns = row.get("modelBreakdowns")
    if not isinstance(breakdowns, list):
        return 0.0
    total = 0.0
    for b in breakdowns:
        if isinstance(b, dict) and isinstance(b.get("cost"), (int, float)):
            total += float(b["cost"])
    return total


def _load_rbac_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    raw = Path(path).read_text(encoding="utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError("RBAC config must be a JSON object.")
    return parsed


def resolve_access_policy(role: Optional[str], user: Optional[str], rbac_config_path: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    cfg = _load_rbac_config(rbac_config_path)
    users = cfg.get("users") if isinstance(cfg.get("users"), dict) else {}
    roles = cfg.get("roles") if isinstance(cfg.get("roles"), dict) else {}

    selected_role = role or ""
    if not selected_role and user and isinstance(users.get(user), str):
        selected_role = users[user]
    if not selected_role:
        selected_role = str(cfg.get("defaultRole") or "admin")

    base_policy = DEFAULT_ROLE_POLICIES.get(selected_role, DEFAULT_ROLE_POLICIES["viewer"]).copy()
    override = roles.get(selected_role)
    if isinstance(override, dict):
        base_policy.update(override)
    return selected_role, base_policy


def apply_access_policy(rows: List[Dict[str, Any]], policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    can_view_breakdown = bool(policy.get("canViewModelBreakdown", True))
    can_view_model_names = bool(policy.get("canViewModelNames", True))
    allowed_models = policy.get("allowedModels")
    has_allowed_models = isinstance(allowed_models, list)
    allowed_set = {str(x) for x in allowed_models} if has_allowed_models else set()
    max_days = policy.get("maxDays")

    transformed: List[Dict[str, Any]] = []
    for row in rows:
        breakdowns = row.get("modelBreakdowns") if isinstance(row.get("modelBreakdowns"), list) else []
        kept: List[Dict[str, Any]] = []
        total = 0.0
        for b in breakdowns:
            if not isinstance(b, dict):
                continue
            name = b.get("modelName")
            cost = b.get("cost")
            if not isinstance(name, str) or not isinstance(cost, (int, float)):
                continue
            if has_allowed_models and name not in allowed_set:
                continue
            c = float(cost)
            total += c
            if can_view_breakdown:
                kept.append({"modelName": name if can_view_model_names else "Restricted", "cost": c})
        nr = dict(row)
        nr["totalCost"] = total
        nr["modelBreakdowns"] = kept if can_view_breakdown else []
        transformed.append(nr)

    if isinstance(max_days, int) and max_days > 0:
        transformed = transformed[-max_days:]

    return transformed





def _load_tenant_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    raw = Path(path).read_text(encoding="utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError("Tenant config must be a JSON object.")
    return parsed


def _get_org_node(cfg: Dict[str, Any], org_id: Optional[str]) -> Tuple[Optional[str], Dict[str, Any]]:
    orgs = cfg.get("organizations") if isinstance(cfg.get("organizations"), dict) else {}
    if not orgs:
        return None, {}

    target = org_id or str(cfg.get("defaultOrganization") or "")
    if not target:
        target = next(iter(orgs.keys()))
    node = orgs.get(target)
    if not isinstance(node, dict):
        raise RuntimeError(f"Organization '{target}' not found in tenant config.")
    return target, node


def _resolve_role_policies(org_node: Dict[str, Any], global_roles: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    merged = {k: dict(v) for k, v in DEFAULT_ROLE_POLICIES.items()}
    if isinstance(global_roles, dict):
        for role_name, role_policy in global_roles.items():
            if isinstance(role_policy, dict):
                merged.setdefault(str(role_name), {})
                merged[str(role_name)].update(role_policy)

    org_roles = org_node.get("roles") if isinstance(org_node.get("roles"), dict) else {}
    for role_name, role_policy in org_roles.items():
        if isinstance(role_policy, dict):
            merged.setdefault(str(role_name), {})
            merged[str(role_name)].update(role_policy)
    return merged


def resolve_multi_tenant_context(
    payload: Dict[str, Any],
    tenant_config_path: Optional[str],
    org_id: Optional[str],
    role: Optional[str],
    user: Optional[str],
    requested_dashboard: Optional[str],
    allow_role_override: bool = False,
) -> Tuple[List[Dict[str, Any]], str, Dict[str, Any], Dict[str, Any]]:
    cfg = _load_tenant_config(tenant_config_path)
    resolved_org_id, org_node = _get_org_node(cfg, org_id)

    if resolved_org_id:
        org_payloads = payload.get("organizations") if isinstance(payload.get("organizations"), dict) else {}
        org_payload = org_payloads.get(resolved_org_id)
        if not isinstance(org_payload, dict):
            raise RuntimeError(f"Organization '{resolved_org_id}' has no usage payload data.")
        rows = parse_daily(org_payload)
    else:
        rows = parse_daily(payload)

    users = org_node.get("users") if isinstance(org_node.get("users"), dict) else {}
    groups = org_node.get("groups") if isinstance(org_node.get("groups"), dict) else {}
    global_roles = cfg.get("roles") if isinstance(cfg.get("roles"), dict) else {}
    role_policies = _resolve_role_policies(org_node, global_roles)

    selected_role = ""
    user_record = users.get(user) if user and isinstance(users.get(user), dict) else {}
    if isinstance(user_record.get("role"), str):
        selected_role = str(user_record.get("role"))
    elif role and (allow_role_override or not user):
        selected_role = role
    if not selected_role:
        selected_role = str(org_node.get("defaultRole") or cfg.get("defaultRole") or "admin")

    base_policy = role_policies.get(selected_role, role_policies.get("viewer", DEFAULT_ROLE_POLICIES["viewer"]))
    policy = dict(base_policy)

    dashboard_views = org_node.get("dashboardViews") if isinstance(org_node.get("dashboardViews"), dict) else {}
    group_name = str(user_record.get("group") or "") if isinstance(user_record, dict) else ""
    allowed_view_ids: List[str] = []
    if group_name and isinstance(groups.get(group_name), dict):
        view_ids = groups[group_name].get("dashboardViews")
        if isinstance(view_ids, list):
            allowed_view_ids = [str(v) for v in view_ids]

    default_dashboard = str(user_record.get("defaultDashboard")) if isinstance(user_record.get("defaultDashboard"), str) else ""
    explicit_allowlist: Optional[List[str]] = allowed_view_ids if allowed_view_ids else None
    if user and explicit_allowlist is None and default_dashboard:
        explicit_allowlist = [default_dashboard]
    elif user and explicit_allowlist is None:
        explicit_allowlist = []

    selected_dashboard = requested_dashboard
    if not selected_dashboard:
        if default_dashboard:
            selected_dashboard = default_dashboard
        elif allowed_view_ids:
            selected_dashboard = allowed_view_ids[0]

    selected_view = {}
    if selected_dashboard:
        raw_view = dashboard_views.get(selected_dashboard)
        if not isinstance(raw_view, dict):
            raise RuntimeError(f"Dashboard view '{selected_dashboard}' not found in organization '{resolved_org_id}'.")
        if explicit_allowlist is not None and selected_dashboard not in explicit_allowlist:
            raise RuntimeError(f"User '{user}' cannot access dashboard view '{selected_dashboard}'.")
        selected_view = raw_view
        if isinstance(raw_view.get("allowedModels"), list):
            policy["allowedModels"] = [str(x) for x in raw_view.get("allowedModels", [])]
        if isinstance(raw_view.get("maxDays"), int):
            policy["maxDays"] = int(raw_view["maxDays"])
    elif user and not explicit_allowlist:
        policy["allowedModels"] = []

    tenant_meta = {
        "organizationId": resolved_org_id,
        "user": user,
        "group": group_name or None,
        "dashboardView": selected_dashboard,
        "dashboardViewConfig": selected_view,
    }
    return rows, selected_role, policy, tenant_meta


def manage_tenant_config(
    tenant_config_path: str,
    org_id: str,
    user_action: Optional[str],
    target_user: Optional[str],
    target_role: Optional[str],
    target_group: Optional[str],
    view_action: Optional[str],
    view_id: Optional[str],
    view_models: Optional[str],
    view_max_days: Optional[int],
    view_group: Optional[str],
) -> Dict[str, Any]:
    cfg = _load_tenant_config(tenant_config_path)
    _, org_node = _get_org_node(cfg, org_id)

    org_node.setdefault("users", {})
    org_node.setdefault("groups", {})
    org_node.setdefault("dashboardViews", {})

    users = org_node["users"]
    groups = org_node["groups"]
    views = org_node["dashboardViews"]

    actions: List[str] = []

    if user_action:
        if user_action == "list":
            actions.append(f"users={list(users.keys())}")
        elif user_action in {"create", "update"}:
            if not target_user:
                raise RuntimeError("--target-user is required for user create/update.")
            role_name = target_role or str(org_node.get("defaultRole") or cfg.get("defaultRole") or "viewer")
            users[target_user] = {
                "role": role_name,
                "group": target_group,
            }
            actions.append(f"{user_action}_user={target_user}")
        elif user_action == "delete":
            if not target_user:
                raise RuntimeError("--target-user is required for user delete.")
            users.pop(target_user, None)
            actions.append(f"delete_user={target_user}")

    if view_action:
        if view_action == "list":
            actions.append(f"views={list(views.keys())}")
        elif view_action in {"create", "update"}:
            if not view_id:
                raise RuntimeError("--view-id is required for view create/update.")
            view_node = views.get(view_id) if isinstance(views.get(view_id), dict) else {}
            if view_models is not None:
                view_node["allowedModels"] = [x.strip() for x in view_models.split(',') if x.strip()]
            if isinstance(view_max_days, int) and view_max_days > 0:
                view_node["maxDays"] = view_max_days
            views[view_id] = view_node
            actions.append(f"{view_action}_view={view_id}")
        elif view_action == "delete":
            if not view_id:
                raise RuntimeError("--view-id is required for view delete.")
            views.pop(view_id, None)
            for g in groups.values():
                if isinstance(g, dict) and isinstance(g.get("dashboardViews"), list):
                    g["dashboardViews"] = [v for v in g["dashboardViews"] if v != view_id]
            actions.append(f"delete_view={view_id}")
        elif view_action in {"assign", "unassign"}:
            if not view_id or not view_group:
                raise RuntimeError("--view-id and --view-group are required for assign/unassign.")
            if view_action == "assign" and view_id not in views:
                raise RuntimeError(f"Dashboard view '{view_id}' does not exist.")
            g = groups.get(view_group) if isinstance(groups.get(view_group), dict) else {"dashboardViews": []}
            ids = g.get("dashboardViews") if isinstance(g.get("dashboardViews"), list) else []
            if view_action == "assign" and view_id not in ids:
                ids.append(view_id)
            if view_action == "unassign":
                ids = [v for v in ids if v != view_id]
            g["dashboardViews"] = ids
            groups[view_group] = g
            actions.append(f"{view_action}_view={view_id}_group={view_group}")

    Path(tenant_config_path).write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "organizationId": org_id,
        "actions": actions,
        "users": sorted(users.keys()),
        "views": sorted(views.keys()),
        "groups": sorted(groups.keys()),
    }
def detect_spikes(rows: List[Dict[str, Any]], lookback_days: int = 7, threshold_mult: float = 2.0) -> List[Dict[str, Any]]:
    spikes: List[Dict[str, Any]] = []
    daily = [day_total_cost(r) for r in rows]
    for i in range(len(rows)):
        if i < lookback_days:
            continue
        baseline_window = daily[i - lookback_days : i]
        baseline = sum(baseline_window) / len(baseline_window) if baseline_window else 0.0
        if baseline <= 0:
            continue
        cost = daily[i]
        if cost >= baseline * threshold_mult:
            spikes.append(
                {
                    "date": rows[i].get("date"),
                    "costUSD": cost,
                    "baselineUSD": baseline,
                    "ratio": (cost / baseline),
                }
            )
    return spikes


def prepare_chart_series(rows: List[Dict[str, Any]], top_models: int) -> Tuple[List[str], Dict[str, List[float]], List[float]]:
    totals = model_totals(rows)
    top = [m for m, _ in sorted(totals.items(), key=lambda x: x[1], reverse=True)[:top_models]]
    labels = [r["date"] for r in rows]
    per_model: Dict[str, List[float]] = {m: [] for m in top}
    other: List[float] = []

    for row in rows:
        breakdowns = row.get("modelBreakdowns") if isinstance(row.get("modelBreakdowns"), list) else []
        this_row = {m: 0.0 for m in top}
        row_other = 0.0
        for b in breakdowns:
            if not isinstance(b, dict):
                continue
            model = b.get("modelName")
            cost = b.get("cost")
            if not isinstance(model, str) or not isinstance(cost, (int, float)):
                continue
            value = float(cost)
            if model in this_row:
                this_row[model] += value
            else:
                row_other += value
        for m in top:
            per_model[m].append(this_row[m])
        other.append(row_other)

    if any(v > 0 for v in other):
        per_model["Other"] = other

    return labels, per_model, [day_total_cost(r) for r in rows]


def usd(v: float) -> str:
    return f"${v:,.2f}"

def build_model_table_rows(models_ranked: List[Tuple[str, float]], grand_total: float, max_rows: int) -> str:
    if max_rows < 1:
        max_rows = 1
    visible = models_ranked[:max_rows]
    hidden = models_ranked[max_rows:]

    rows = [
        f"<tr><td>{idx}</td><td>{m}</td><td>{usd(c)}</td><td>{(c / grand_total * 100 if grand_total else 0):.1f}%</td></tr>"
        for idx, (m, c) in enumerate(visible, start=1)
    ]

    if hidden:
        hidden_total = sum(c for _, c in hidden)
        rows.append(
            f"<tr><td>…</td><td>Remaining {len(hidden)} models</td><td>{usd(hidden_total)}</td><td>{(hidden_total / grand_total * 100 if grand_total else 0):.1f}%</td></tr>"
        )

    return "\n".join(rows)


def _window_model_totals(rows: List[Dict[str, Any]]) -> Tuple[Dict[str, float], Dict[str, float]]:
    last_rows = rows[-7:] if rows else []
    prev_rows = rows[-14:-7] if len(rows) > 7 else []

    last_totals: Dict[str, float] = defaultdict(float)
    prev_totals: Dict[str, float] = defaultdict(float)

    for row in last_rows:
        for b in row.get("modelBreakdowns") or []:
            if isinstance(b, dict) and isinstance(b.get("modelName"), str) and isinstance(b.get("cost"), (int, float)):
                last_totals[b["modelName"]] += float(b["cost"])

    for row in prev_rows:
        for b in row.get("modelBreakdowns") or []:
            if isinstance(b, dict) and isinstance(b.get("modelName"), str) and isinstance(b.get("cost"), (int, float)):
                prev_totals[b["modelName"]] += float(b["cost"])

    return dict(last_totals), dict(prev_totals)




def forecast_daily_costs(rows: List[Dict[str, Any]], forecast_days: int = 7, lookback_days: int = 30) -> Dict[str, Any]:
    if forecast_days < 1:
        forecast_days = 1
    if lookback_days < 2:
        lookback_days = 2

    dated_costs: List[Tuple[date, float]] = []
    for row in rows:
        d = parse_date(str(row.get("date", "")))
        if not d:
            continue
        dated_costs.append((d, day_total_cost(row)))
    if len(dated_costs) < 2:
        return {
            "method": "linear-regression",
            "lookbackDays": lookback_days,
            "forecastDays": forecast_days,
            "predictions": [],
            "totalForecastCostUSD": 0.0,
        }

    window = dated_costs[-lookback_days:]
    base_day = window[0][0]
    xs = [(d - base_day).days for d, _ in window]
    ys = [c for _, c in window]

    n = len(xs)
    sx = sum(xs)
    sy = sum(ys)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, ys))
    denom = n * sxx - sx * sx
    slope = ((n * sxy - sx * sy) / denom) if denom != 0 else 0.0
    intercept = (sy - slope * sx) / n if n else 0.0

    last_day = window[-1][0]
    predictions: List[Dict[str, Any]] = []
    total = 0.0
    for i in range(1, forecast_days + 1):
        d = last_day + timedelta(days=i)
        x = (d - base_day).days
        y = max(0.0, intercept + slope * x)
        y = round(y, 6)
        total += y
        predictions.append({"date": d.strftime("%Y-%m-%d"), "costUSD": y})

    return {
        "method": "linear-regression",
        "lookbackDays": lookback_days,
        "forecastDays": forecast_days,
        "slope": round(slope, 6),
        "predictions": predictions,
        "totalForecastCostUSD": round(total, 6),
    }


def detect_anomaly_alerts(rows: List[Dict[str, Any]], lookback_days: int = 14, z_threshold: float = 2.5, min_cost: float = 0.0) -> List[Dict[str, Any]]:
    if lookback_days < 2:
        lookback_days = 2
    daily = [day_total_cost(r) for r in rows]
    alerts: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        if i < lookback_days:
            continue
        cost = daily[i]
        if cost < min_cost:
            continue
        baseline = daily[i - lookback_days:i]
        mean = sum(baseline) / len(baseline) if baseline else 0.0
        variance = sum((x - mean) ** 2 for x in baseline) / len(baseline) if baseline else 0.0
        std = variance ** 0.5
        if std <= 0:
            if mean <= 0:
                continue
            if cost >= mean * max(1.5, z_threshold):
                alerts.append({
                    "date": row.get("date"),
                    "costUSD": round(cost, 6),
                    "baselineMeanUSD": round(mean, 6),
                    "baselineStdUSD": 0.0,
                    "zScore": round(z_threshold, 4),
                    "severity": "high",
                })
            continue
        z = (cost - mean) / std
        if z >= z_threshold:
            severity = "high" if z >= (z_threshold + 1.5) else ("medium" if z >= (z_threshold + 0.5) else "low")
            alerts.append({
                "date": row.get("date"),
                "costUSD": round(cost, 6),
                "baselineMeanUSD": round(mean, 6),
                "baselineStdUSD": round(std, 6),
                "zScore": round(z, 4),
                "severity": severity,
            })
    return alerts
def build_summary(
    provider: str,
    rows: List[Dict[str, Any]],
    spike_lookback_days: int = 7,
    spike_threshold_mult: float = 2.0,
    forecast_days: int = 7,
    forecast_lookback_days: int = 30,
    anomaly_lookback_days: int = 14,
    anomaly_z_threshold: float = 2.5,
    anomaly_min_cost: float = 0.0,
) -> Dict[str, Any]:
    totals = model_totals(rows)
    daily_costs = [day_total_cost(r) for r in rows]
    grand_total = sum(daily_costs)
    ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)

    last_7d = sum(daily_costs[-7:]) if daily_costs else 0.0
    prev_7d = sum(daily_costs[-14:-7]) if len(daily_costs) > 7 else 0.0
    delta_pct = ((last_7d - prev_7d) / prev_7d * 100.0) if prev_7d > 0 else None

    last_by_model, prev_by_model = _window_model_totals(rows)
    movers: List[Dict[str, Any]] = []
    for model in set(last_by_model) | set(prev_by_model):
        last_cost = last_by_model.get(model, 0.0)
        prev_cost = prev_by_model.get(model, 0.0)
        delta_cost = last_cost - prev_cost
        movers.append(
            {
                "model": model,
                "last7dCostUSD": last_cost,
                "prev7dCostUSD": prev_cost,
                "deltaCostUSD": delta_cost,
                "deltaPct": ((delta_cost / prev_cost) * 100.0) if prev_cost > 0 else None,
            }
        )
    movers.sort(key=lambda x: x["deltaCostUSD"], reverse=True)

    spikes = detect_spikes(rows, lookback_days=spike_lookback_days, threshold_mult=spike_threshold_mult)
    forecast = forecast_daily_costs(rows, forecast_days=forecast_days, lookback_days=forecast_lookback_days)
    anomaly_alerts = detect_anomaly_alerts(
        rows,
        lookback_days=anomaly_lookback_days,
        z_threshold=anomaly_z_threshold,
        min_cost=anomaly_min_cost,
    )

    return {
        "provider": provider,
        "rows": len(rows),
        "startDate": rows[0]["date"] if rows else None,
        "endDate": rows[-1]["date"] if rows else None,
        "totalCostUSD": grand_total,
        "lastDayCostUSD": daily_costs[-1] if daily_costs else 0.0,
        "last7dCostUSD": last_7d,
        "prev7dCostUSD": prev_7d,
        "last7dDeltaPct": delta_pct,
        "modelsSeen": len(totals),
        "models": [{"model": m, "totalCostUSD": c} for m, c in ranked],
        "movers": movers,
        "spikes": spikes,
        "forecast": forecast,
        "anomalyAlerts": anomaly_alerts,
    }


def _bucket_for_granularity(day: date, granularity: str) -> str:
    g = (granularity or "daily").lower()
    if g == "weekly":
        y, w, _ = day.isocalendar()
        return f"{y}-W{w:02d}"
    if g == "monthly":
        return day.strftime("%Y-%m")
    return day.strftime("%Y-%m-%d")


def generate_custom_report(
    rows: List[Dict[str, Any]],
    metrics: List[str],
    models: Optional[List[str]] = None,
    granularity: str = "daily",
) -> List[Dict[str, Any]]:
    selected_metrics = [m for m in metrics if m in {"total_cost", "active_models", "avg_cost_per_model"}]
    if not selected_metrics:
        selected_metrics = ["total_cost"]

    model_filter = {m for m in (models or []) if isinstance(m, str) and m.strip()}
    buckets: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for row in rows:
        d = parse_date(str(row.get("date", "")))
        if not d:
            continue
        bucket = _bucket_for_granularity(d, granularity)
        breakdowns = row.get("modelBreakdowns") if isinstance(row.get("modelBreakdowns"), list) else []
        model_map: Dict[str, float] = defaultdict(float)
        for b in breakdowns:
            if not isinstance(b, dict):
                continue
            name = b.get("modelName")
            cost = b.get("cost")
            if not isinstance(name, str) or not isinstance(cost, (int, float)):
                continue
            if model_filter and name not in model_filter:
                continue
            model_map[name] += float(cost)

        buckets[bucket]["total_cost"] += sum(model_map.values())
        buckets[bucket]["active_models"] += float(len(model_map))
        buckets[bucket]["days"] += 1.0

    out: List[Dict[str, Any]] = []
    for bucket in sorted(buckets.keys()):
        item: Dict[str, Any] = {"period": bucket}
        days = buckets[bucket].get("days", 1.0) or 1.0
        total_cost = buckets[bucket].get("total_cost", 0.0)
        active_models = buckets[bucket].get("active_models", 0.0) / days
        if "total_cost" in selected_metrics:
            item["totalCostUSD"] = round(total_cost, 6)
        if "active_models" in selected_metrics:
            item["avgActiveModels"] = round(active_models, 6)
        if "avg_cost_per_model" in selected_metrics:
            item["avgCostPerActiveModelUSD"] = round((total_cost / active_models) if active_models > 0 else 0.0, 6)
        out.append(item)

    return out


def build_dashboard_html(
    provider: str,
    rows: List[Dict[str, Any]],
    top_models: int,
    spike_lookback_days: int = 7,
    spike_threshold_mult: float = 2.0,
    forecast_days: int = 7,
    forecast_lookback_days: int = 30,
    anomaly_lookback_days: int = 14,
    anomaly_z_threshold: float = 2.5,
    anomaly_min_cost: float = 0.0,
    max_table_rows: int = 120,
    chart_max_points: int = 1200,
    role_name: str = "admin",
    policy: Optional[Dict[str, Any]] = None,
) -> str:
    policy = policy or DEFAULT_ROLE_POLICIES["admin"]
    totals = model_totals(rows)
    grand_total = sum(day_total_cost(r) for r in rows)
    chart_rows = downsample_rows(rows, chart_max_points)
    labels, series, day_totals = prepare_chart_series(chart_rows, top_models=top_models)
    latest_day = rows[-1]["date"] if rows else "—"
    last_7d = sum(day_totals[-7:]) if day_totals else 0.0
    prev_7d = sum(day_totals[-14:-7]) if len(day_totals) > 7 else 0.0
    if prev_7d > 0:
        trend = f"{((last_7d - prev_7d) / prev_7d * 100.0):+.1f}%"
    else:
        trend = "N/A"

    models_ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    table_rows = build_model_table_rows(models_ranked, grand_total, max_rows=max_table_rows)

    summary = build_summary(
        provider,
        rows,
        spike_lookback_days=spike_lookback_days,
        spike_threshold_mult=spike_threshold_mult,
        forecast_days=forecast_days,
        forecast_lookback_days=forecast_lookback_days,
        anomaly_lookback_days=anomaly_lookback_days,
        anomaly_z_threshold=anomaly_z_threshold,
        anomaly_min_cost=anomaly_min_cost,
    )
    spike_count = len(summary.get("spikes", []))
    forecast_total = float((summary.get("forecast") or {}).get("totalForecastCostUSD", 0.0))
    anomaly_count = len(summary.get("anomalyAlerts", []))

    movers_rows_parts: List[str] = []
    for idx, x in enumerate(summary.get("movers", [])[:8], start=1):
        pct = f"{x['deltaPct']:+.1f}%" if isinstance(x.get("deltaPct"), (int, float)) else "N/A"
        movers_rows_parts.append(
            f"<tr><td>{idx}</td><td>{x['model']}</td><td>{usd(float(x['last7dCostUSD']))}</td><td>{usd(float(x['prev7dCostUSD']))}</td><td>{usd(float(x['deltaCostUSD']))}</td><td>{pct}</td></tr>"
        )
    movers_rows = "\n".join(movers_rows_parts)

    visible_spikes = summary.get("spikes", []) if policy.get("canViewSpikes", True) else []
    spikes_rows_parts: List[str] = []
    for idx, x in enumerate(visible_spikes[:10], start=1):
        date_key = str(x["date"])
        spikes_rows_parts.append(
            f"<tr id='spike-row-{date_key}'><td>{idx}</td><td>{x['date']}</td><td>{usd(float(x['costUSD']))}</td><td>{usd(float(x['baselineUSD']))}</td><td>{x['ratio']:.2f}x</td></tr>"
        )
    spikes_rows = "\n".join(spikes_rows_parts) if spikes_rows_parts else "<tr><td colspan='5'>No spikes detected</td></tr>"

    forecast_rows_parts: List[str] = []
    for idx, x in enumerate((summary.get("forecast") or {}).get("predictions", [])[:14], start=1):
        forecast_rows_parts.append(
            f"<tr><td>{idx}</td><td>{x['date']}</td><td>{usd(float(x['costUSD']))}</td></tr>"
        )
    forecast_rows = "\n".join(forecast_rows_parts) if forecast_rows_parts else "<tr><td colspan='3'>Forecast unavailable (need at least 2 data points)</td></tr>"

    anomaly_rows_parts: List[str] = []
    for idx, x in enumerate(summary.get("anomalyAlerts", [])[-12:][::-1], start=1):
        anomaly_rows_parts.append(
            f"<tr><td>{idx}</td><td>{x['date']}</td><td>{usd(float(x['costUSD']))}</td><td>{x['zScore']:.2f}</td><td>{x['severity']}</td></tr>"
        )
    anomaly_rows = "\n".join(anomaly_rows_parts) if anomaly_rows_parts else "<tr><td colspan='5'>No anomaly alerts</td></tr>"

    spike_by_date: Dict[str, Dict[str, float]] = {}
    for s in visible_spikes:
        d = s.get("date")
        if isinstance(d, str):
            spike_by_date[d] = {
                "costUSD": float(s.get("costUSD", 0.0)),
                "baselineUSD": float(s.get("baselineUSD", 0.0)),
                "ratio": float(s.get("ratio", 0.0)),
            }

    day_breakdown_by_date: Dict[str, List[Dict[str, float | str]]] = {}
    day_total_by_date: Dict[str, float] = {}
    for row in rows:
        d = row.get("date")
        if not isinstance(d, str):
            continue
        breakdowns = row.get("modelBreakdowns")
        if not isinstance(breakdowns, list):
            day_breakdown_by_date[d] = []
            continue
        model_map: Dict[str, float] = defaultdict(float)
        for b in breakdowns:
            if not isinstance(b, dict):
                continue
            name = b.get("modelName")
            cost = b.get("cost")
            if isinstance(name, str) and isinstance(cost, (int, float)):
                model_map[name] += float(cost)
        ranked = sorted(model_map.items(), key=lambda x: x[1], reverse=True)
        day_breakdown_by_date[d] = [{"model": n, "costUSD": c} for n, c in ranked]
        day_total_by_date[d] = sum(model_map.values())

    json_labels = json.dumps(labels)
    json_series = json.dumps(series)
    json_day_totals = json.dumps(day_totals)
    all_models = sorted({str(x["model"]) for day in day_breakdown_by_date.values() for x in day if isinstance(x, dict) and isinstance(x.get("model"), str)})

    json_spike_by_date = json.dumps(spike_by_date)
    json_day_breakdown_by_date = json.dumps(day_breakdown_by_date)
    json_day_total_by_date = json.dumps(day_total_by_date)
    json_all_models = json.dumps(all_models)
    json_policy = json.dumps(policy)

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>Token Usage Dashboard · {provider}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; margin: 24px; color: #1f2937; }}
    .grid {{ display: grid; grid-template-columns: repeat(7, minmax(160px, 1fr)); gap: 12px; margin-bottom: 18px; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px 14px; background: #fff; }}
    .label {{ color: #6b7280; font-size: 12px; }}
    .value {{ font-size: 24px; font-weight: 700; margin-top: 4px; }}
    .chart-wrap {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px; margin: 12px 0 18px; position: relative; }}
    canvas {{ width: 100%; height: 360px; }}
    #tooltip {{ position: absolute; pointer-events: none; background: #111827; color: #fff; padding: 8px 10px; border-radius: 8px; font-size: 12px; line-height: 1.4; opacity: 0; transform: translate(-50%, -110%); white-space: normal; max-width: 320px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; text-align: left; padding: 8px; font-size: 13px; }}
    th {{ color: #374151; background: #f9fafb; }}
    tr.model-top {{ background: #eff6ff; }}
    .dod-pos {{ color: #166534; font-weight: 600; }}
    .dod-neg {{ color: #991b1b; font-weight: 600; }}
    .dod-neutral {{ color: #6b7280; }}
    tr.spike-focus {{ background: #fef2f2; }}
    .kbd-help {{ position: fixed; right: 18px; bottom: 18px; background: #111827; color: #f9fafb; border-radius: 10px; padding: 10px 12px; font-size: 12px; line-height: 1.6; max-width: 280px; box-shadow: 0 8px 24px rgba(0,0,0,.25); display: none; z-index: 20; }}
    .kbd-help.visible {{ display: block; }}
    .kbd-help code {{ background: #374151; color: #fff; padding: 1px 5px; border-radius: 6px; }}
  </style>
</head>
<body>
  <h2>Token Usage Dashboard · {provider}</h2>
  <div style="color:#6b7280;font-size:12px;margin-bottom:6px;">Role: <strong>{role_name}</strong></div>
  <div style="color:#6b7280;font-size:12px;margin-bottom:6px;">Tips: click chart points/spikes to focus a day · use ←/→ or j/k to step dates · n/p jump next/prev spike · Home/End first/last day · s toggle spike-only · d sort DoD · x changes-only · r reset to latest · c copy link · ? help</div>
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap;">
    <label style="display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#374151;">
      <input type="checkbox" id="spikeOnlyToggle" />
      Spike-only navigation (shareable via URL hash)
    </label>
    <label style="display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#374151;">
      <input type="checkbox" id="showOnlyChangesToggle" />
      Selected day: show only DoD changes
    </label>
    <button id="copyLinkBtn" type="button" style="font-size:12px;border:1px solid #d1d5db;background:#fff;border-radius:8px;padding:4px 8px;cursor:pointer;">Copy deep-link</button>
    <span id="copyLinkHint" style="font-size:12px;color:#6b7280;"></span>
  </div>
  <div class=\"grid\">
    <div class=\"card\"><div class=\"label\">Date range rows</div><div class=\"value\">{len(rows)}</div></div>
    <div class=\"card\"><div class=\"label\">Latest day</div><div class=\"value\">{latest_day}</div></div>
    <div class=\"card\"><div class=\"label\">Total cost</div><div class=\"value\">{usd(grand_total)}</div></div>
    <div class=\"card\"><div class=\"label\">7d trend vs prev 7d</div><div class=\"value\">{trend}</div></div>
    <div class=\"card\"><div class=\"label\">Spikes detected</div><div class=\"value\">{spike_count}</div></div>
    <div class=\"card\"><div class=\"label\">Forecast next {forecast_days}d</div><div class=\"value\">{usd(forecast_total)}</div></div>
    <div class=\"card\"><div class=\"label\">Anomaly alerts</div><div class=\"value\">{anomaly_count}</div></div>
  </div>

  <div class=\"chart-wrap\" id=\"chartWrap\">
    <canvas id=\"costChart\"></canvas>
    <div id=\"tooltip\"></div>
  </div>

  <h3>Model Breakdown</h3>
  <div style="font-size:12px;color:#6b7280;margin:-6px 0 8px;">Showing up to top {max_table_rows} models for faster rendering. Chart points: {len(chart_rows)}/{len(rows)}.</div>
  <table>
    <thead><tr><th>#</th><th>Model</th><th>Total Cost</th><th>Share</th></tr></thead>
    <tbody>{table_rows}</tbody>
  </table>

  <h3>Top 7d Movers</h3>
  <table>
    <thead><tr><th>#</th><th>Model</th><th>Last 7d</th><th>Prev 7d</th><th>Δ Cost</th><th>Δ %</th></tr></thead>
    <tbody>{movers_rows}</tbody>
  </table>

  <h3>Daily Cost Spikes</h3>
  <table>
    <thead><tr><th>#</th><th>Date</th><th>Cost</th><th>7d Baseline</th><th>Ratio</th></tr></thead>
    <tbody id="spikesBody">{spikes_rows}</tbody>
  </table>

  <h3>Cost Forecast (Next {forecast_days} Days)</h3>
  <table>
    <thead><tr><th>#</th><th>Date</th><th>Forecast Cost</th></tr></thead>
    <tbody id="forecastBody">{forecast_rows}</tbody>
  </table>

  <h3>Anomaly Consumption Alerts</h3>
  <div style="font-size:12px;color:#6b7280;margin:-6px 0 8px;">Triggered when daily cost z-score ≥ {anomaly_z_threshold} over previous {anomaly_lookback_days} days.</div>
  <table>
    <thead><tr><th>#</th><th>Date</th><th>Cost</th><th>Z-score</th><th>Severity</th></tr></thead>
    <tbody id="anomalyBody">{anomaly_rows}</tbody>
  </table>

  <h3 id="selectedDayTitle">Selected Day Model Breakdown</h3>
  <div id="selectedDayMeta" style="font-size:12px;color:#4b5563;margin:-4px 0 8px;"></div>
  <label style="display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#374151;margin:0 0 8px;">
    <input type="checkbox" id="sortByDodToggle" />
    Sort selected day by DoD Δ (desc)
  </label>
  <table>
    <thead><tr><th>#</th><th>Model</th><th>Cost</th><th>Share</th><th>DoD Δ</th><th>DoD Δ%</th></tr></thead>
    <tbody id="selectedDayBody"><tr><td colspan="6">Click a spike marker to focus a day</td></tr></tbody>
  </table>

  <h3>Custom Report Builder</h3>
  <div style="display:flex;gap:18px;flex-wrap:wrap;margin-bottom:8px;align-items:flex-start;">
    <fieldset style="border:1px solid #e5e7eb;border-radius:10px;padding:8px 10px;min-width:220px;">
      <legend style="font-size:12px;color:#6b7280;">Metrics</legend>
      <label style="display:block;font-size:12px;"><input type="checkbox" class="metricOpt" value="total_cost" checked/> Total Cost</label>
      <label style="display:block;font-size:12px;"><input type="checkbox" class="metricOpt" value="active_models"/> Avg Active Models</label>
      <label style="display:block;font-size:12px;"><input type="checkbox" class="metricOpt" value="avg_cost_per_model"/> Avg Cost / Active Model</label>
    </fieldset>
    <fieldset style="border:1px solid #e5e7eb;border-radius:10px;padding:8px 10px;min-width:220px;max-height:170px;overflow:auto;">
      <legend style="font-size:12px;color:#6b7280;">Models (blank = all)</legend>
      <div id="modelFilters"></div>
    </fieldset>
    <div>
      <label style="font-size:12px;color:#6b7280;">Time Granularity</label><br/>
      <select id="reportGranularity" style="margin-top:4px;padding:4px 6px;border-radius:8px;border:1px solid #d1d5db;">
        <option value="daily">Daily</option>
        <option value="weekly">Weekly</option>
        <option value="monthly">Monthly</option>
      </select>
      <div style="margin-top:8px;display:flex;gap:6px;">
        <button id="generateReportBtn" type="button" style="font-size:12px;border:1px solid #2563eb;background:#2563eb;color:#fff;border-radius:8px;padding:5px 10px;cursor:pointer;">Generate report</button>
        <button id="downloadReportCsvBtn" type="button" style="font-size:12px;border:1px solid #d1d5db;background:#fff;border-radius:8px;padding:5px 10px;cursor:pointer;">Download CSV</button>
      </div>
    </div>
  </div>
  <table>
    <thead><tr><th>Period</th><th>Total Cost</th><th>Avg Active Models</th><th>Avg Cost / Active Model</th></tr></thead>
    <tbody id="customReportBody"><tr><td colspan="4">Select filters then click Generate report</td></tr></tbody>
  </table>

  <div id="kbdHelp" class="kbd-help">
    <div><strong>Keyboard shortcuts</strong></div>
    <div><code>←/→</code> or <code>j/k</code>: step day</div>
    <div><code>n/p</code>: next/prev spike</div>
    <div><code>Home/End</code>: first/last day</div>
    <div><code>s</code>: toggle spike-only mode</div>
    <div><code>d</code>: toggle DoD sort</div>
    <div><code>x</code>: toggle changes-only filter</div>
    <div><code>r</code>: reset to latest day</div>
    <div><code>c</code>: copy deep-link</div>
    <div><code>?</code>: toggle this help</div>
    <div><code>Esc</code>: close help</div>
  </div>

  <script>
    const labels = {json_labels};
    const series = {json_series};
    const dayTotals = {json_day_totals};
    const spikeByDate = {json_spike_by_date};
    const dayBreakdownByDate = {json_day_breakdown_by_date};
    const dayTotalByDate = {json_day_total_by_date};
    const allModels = {json_all_models};
    const accessPolicy = {json_policy};

    const colors = ["#2563eb", "#16a34a", "#dc2626", "#7c3aed", "#ea580c", "#0891b2", "#4f46e5", "#65a30d", "#be123c"];
    const canvas = document.getElementById('costChart');
    const ctx = canvas.getContext('2d');
    const tip = document.getElementById('tooltip');
    const wrap = document.getElementById('chartWrap');
    const selectedDayTitle = document.getElementById('selectedDayTitle');
    const selectedDayMeta = document.getElementById('selectedDayMeta');
    const sortByDodToggle = document.getElementById('sortByDodToggle');
    const selectedDayBody = document.getElementById('selectedDayBody');
    const spikesBody = document.getElementById('spikesBody');
    const spikeOnlyToggle = document.getElementById('spikeOnlyToggle');
    const showOnlyChangesToggle = document.getElementById('showOnlyChangesToggle');
    const copyLinkBtn = document.getElementById('copyLinkBtn');
    const copyLinkHint = document.getElementById('copyLinkHint');
    const kbdHelp = document.getElementById('kbdHelp');
    const modelFilters = document.getElementById('modelFilters');
    const reportGranularity = document.getElementById('reportGranularity');
    const generateReportBtn = document.getElementById('generateReportBtn');
    const downloadReportCsvBtn = document.getElementById('downloadReportCsvBtn');
    const customReportBody = document.getElementById('customReportBody');
    let selectedSpikeDate = null;
    let selectedDate = null;
    let spikeOnlyMode = false;

    function hideSectionByHeading(titleText, message) {{
      const h = Array.from(document.querySelectorAll('h3')).find(x => x.textContent.trim() === titleText);
      if (!h) return;
      let n = h.nextElementSibling;
      while (n && n.tagName !== 'H3') {{
        n.style.display = 'none';
        n = n.nextElementSibling;
      }}
      const note = document.createElement('div');
      note.style.fontSize = '12px';
      note.style.color = '#6b7280';
      note.style.marginBottom = '8px';
      note.textContent = message;
      h.insertAdjacentElement('afterend', note);
    }}
    let sortByDodMode = false;
    let showOnlyChangesMode = false;

    const labelIndexByDate = Object.fromEntries(labels.map((d, i) => [d, i]));
    const dayViewCache = new Map();

    function getDayView(date) {{
      if (dayViewCache.has(date)) return dayViewCache.get(date);
      const baseRows = dayBreakdownByDate[date] || [];
      const idx = labelIndexByDate[date] ?? -1;
      const prevDate = idx > 0 ? labels[idx - 1] : null;
      const prevRows = prevDate ? (dayBreakdownByDate[prevDate] || []) : [];
      const prevMap = Object.fromEntries(prevRows.map(r => [r.model, r.costUSD || 0]));
      const currTotal = dayTotalByDate[date] || 0;
      const prevTotal = prevDate ? (dayTotalByDate[prevDate] || 0) : 0;
      const rows = baseRows.map(r => ({{
        ...r,
        dod: (r.costUSD || 0) - (prevMap[r.model] || 0),
        prevCost: prevMap[r.model] || 0,
      }}));
      const view = {{ rows, currTotal, prevTotal }};
      dayViewCache.set(date, view);
      return view;
    }}

    function renderSelectedDay(date) {{
      const view = getDayView(date);
      const total = view.currTotal || 0;
      const prevTotal = view.prevTotal || 0;
      let rows = view.rows.slice();

      if (showOnlyChangesMode) rows = rows.filter(r => Math.abs(r.dod || 0) > 1e-9);
      if (sortByDodMode) rows.sort((a, b) => (b.dod || 0) - (a.dod || 0));

      const totalDelta = total - prevTotal;
      const totalDeltaPct = prevTotal > 0 ? ((totalDelta / prevTotal) * 100) : null;
      const totalDeltaText = `${{totalDelta >= 0 ? '+' : ''}}$${{totalDelta.toFixed(2)}}`;
      const totalDeltaPctText = totalDeltaPct === null ? 'N/A' : `${{totalDeltaPct >= 0 ? '+' : ''}}${{totalDeltaPct.toFixed(1)}}%`;
      selectedDayTitle.textContent = `Selected Day Model Breakdown · ${{date}} · DoD ${{totalDeltaText}} (${{totalDeltaPctText}})`;
      if (selectedDayMeta) selectedDayMeta.textContent = `Day total: $${{total.toFixed(2)}} · Previous day total: $${{prevTotal.toFixed(2)}}`;

      if (!rows.length) {{
        selectedDayBody.innerHTML = '<tr><td colspan="6">No model breakdown rows for current filter</td></tr>';
        return;
      }}
      selectedDayBody.innerHTML = rows.map((r, i) => {{
        const share = total > 0 ? (((r.costUSD || 0) / total) * 100).toFixed(1) : '0.0';
        const prev = r.prevCost || 0;
        const dod = r.dod || 0;
        const dodPct = prev > 0 ? (dod / prev) * 100 : null;
        const dodText = `${{dod >= 0 ? '+' : ''}}$${{dod.toFixed(2)}}`;
        const dodPctText = dodPct === null ? 'N/A' : `${{dodPct >= 0 ? '+' : ''}}${{dodPct.toFixed(1)}}%`;
        const dodClass = dod > 0 ? 'dod-pos' : (dod < 0 ? 'dod-neg' : 'dod-neutral');
        const dodPctClass = dodPct === null ? 'dod-neutral' : (dodPct > 0 ? 'dod-pos' : (dodPct < 0 ? 'dod-neg' : 'dod-neutral'));
        const rowClass = i === 0 ? 'model-top' : '';
        return `<tr class="${{rowClass}}"><td>${{i + 1}}</td><td>${{r.model}}</td><td>$${{(r.costUSD || 0).toFixed(2)}}</td><td>${{share}}%</td><td class="${{dodClass}}">${{dodText}}</td><td class="${{dodPctClass}}">${{dodPctText}}</td></tr>`;
      }}).join('');
    }}

    function selectedMetricKeys() {{
      const checked = Array.from(document.querySelectorAll('.metricOpt:checked')).map(x => x.value);
      return checked.length ? checked : ['total_cost'];
    }}

    function selectedModels() {{
      return Array.from(document.querySelectorAll('.modelOpt:checked')).map(x => x.value);
    }}

    function bucketFor(dateStr, granularity) {{
      if (granularity === 'monthly') return dateStr.slice(0, 7);
      if (granularity === 'weekly') {{
        const dt = new Date(dateStr + 'T00:00:00Z');
        const tmp = new Date(Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate()));
        const day = (tmp.getUTCDay() + 6) % 7;
        tmp.setUTCDate(tmp.getUTCDate() - day + 3);
        const firstThursday = new Date(Date.UTC(tmp.getUTCFullYear(), 0, 4));
        const week = 1 + Math.round(((tmp - firstThursday) / 86400000 - 3 + ((firstThursday.getUTCDay() + 6) % 7)) / 7);
        return `${{tmp.getUTCFullYear()}}-W${{String(week).padStart(2, '0')}}`;
      }}
      return dateStr;
    }}

    function generateCustomReportRows(metrics, models, granularity) {{
      const modelSet = new Set(models || []);
      const hasModelFilter = modelSet.size > 0;
      const rows = [];
      labels.forEach((d) => {{
        const breakdown = dayBreakdownByDate[d] || [];
        const filtered = breakdown.filter(x => !hasModelFilter || modelSet.has(x.model));
        const totalCost = filtered.reduce((s, x) => s + (x.costUSD || 0), 0);
        rows.push({{ date: d, totalCost, activeModels: filtered.length }});
      }});

      const grouped = new Map();
      rows.forEach((r) => {{
        const period = bucketFor(r.date, granularity);
        if (!grouped.has(period)) grouped.set(period, {{ period, totalCost: 0, activeModels: 0, days: 0 }});
        const g = grouped.get(period);
        g.totalCost += r.totalCost;
        g.activeModels += r.activeModels;
        g.days += 1;
      }});

      return Array.from(grouped.values()).sort((a, b) => a.period.localeCompare(b.period)).map((g) => {{
        const avgActiveModels = g.days > 0 ? g.activeModels / g.days : 0;
        return {{
          period: g.period,
          totalCost: g.totalCost,
          avgActiveModels,
          avgCostPerModel: avgActiveModels > 0 ? (g.totalCost / avgActiveModels) : 0,
          metrics
        }};
      }});
    }}

    function renderCustomReport() {{
      const metrics = selectedMetricKeys();
      const models = selectedModels();
      const granularity = reportGranularity?.value || 'daily';
      const rows = generateCustomReportRows(metrics, models, granularity);
      if (!rows.length) {{
        customReportBody.innerHTML = '<tr><td colspan="4">No rows</td></tr>';
        return [];
      }}
      customReportBody.innerHTML = rows.map(r => {{
        const total = metrics.includes('total_cost') ? `$${{r.totalCost.toFixed(2)}}` : '-';
        const active = metrics.includes('active_models') ? r.avgActiveModels.toFixed(2) : '-';
        const avg = metrics.includes('avg_cost_per_model') ? `$${{r.avgCostPerModel.toFixed(2)}}` : '-';
        return `<tr><td>${{r.period}}</td><td>${{total}}</td><td>${{active}}</td><td>${{avg}}</td></tr>`;
      }}).join('');
      return rows;
    }}

    function downloadCustomReportCsv(rows) {{
      if (!rows || !rows.length) return;
      const header = ['period','totalCostUSD','avgActiveModels','avgCostPerActiveModelUSD'];
      const lines = [header.join(',')].concat(rows.map(r => [r.period, r.totalCost.toFixed(6), r.avgActiveModels.toFixed(6), r.avgCostPerModel.toFixed(6)].join(',')));
      const blob = new Blob([lines.join('\\n')], {{ type: 'text/csv;charset=utf-8;' }});
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'custom_usage_report.csv';
      document.body.appendChild(a);
      a.click();
      a.remove();
    }}

    function initCustomReportBuilder() {{
      if (modelFilters) {{
        modelFilters.innerHTML = allModels.map((m) => `<label style="display:block;font-size:12px;"><input type="checkbox" class="modelOpt" value="${{m}}"/> ${{m}}</label>`).join('');
      }}
      let latestRows = renderCustomReport();
      generateReportBtn?.addEventListener('click', () => {{
        latestRows = renderCustomReport();
      }});
      downloadReportCsvBtn?.addEventListener('click', () => {{
        latestRows = latestRows && latestRows.length ? latestRows : renderCustomReport();
        downloadCustomReportCsv(latestRows);
      }});
    }}

    function resize() {{
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();
      canvas.width = Math.floor(rect.width * dpr);
      canvas.height = Math.floor(rect.height * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      draw();
    }}

    function draw() {{
      const w = canvas.clientWidth;
      const h = canvas.clientHeight;
      ctx.clearRect(0, 0, w, h);
      if (!labels.length) return;

      const pad = {{ l: 46, r: 12, t: 12, b: 40 }};
      const iw = w - pad.l - pad.r;
      const ih = h - pad.t - pad.b;
      const maxY = Math.max(...dayTotals, 0.01);

      ctx.strokeStyle = '#e5e7eb';
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i++) {{
        const y = pad.t + (ih * i / 4);
        ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(w - pad.r, y); ctx.stroke();
      }}

      const names = Object.keys(series);
      const cumulative = new Array(labels.length).fill(0);

      names.forEach((name, idx) => {{
        const vals = series[name];
        ctx.fillStyle = colors[idx % colors.length] + '66';
        ctx.strokeStyle = colors[idx % colors.length];

        ctx.beginPath();
        vals.forEach((v, i) => {{
          const x = pad.l + (iw * i / Math.max(1, labels.length - 1));
          const y = pad.t + ih - ((cumulative[i] + v) / maxY) * ih;
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        }});

        for (let i = vals.length - 1; i >= 0; i--) {{
          const x = pad.l + (iw * i / Math.max(1, labels.length - 1));
          const y = pad.t + ih - ((cumulative[i]) / maxY) * ih;
          ctx.lineTo(x, y);
        }}
        ctx.closePath();
        ctx.fill();

        for (let i = 0; i < vals.length; i++) cumulative[i] += vals[i];
      }});

      labels.forEach((d, i) => {{
        const spike = spikeByDate[d];
        if (!spike) return;
        const x = pad.l + (iw * i / Math.max(1, labels.length - 1));
        const y = pad.t + ih - ((dayTotals[i] || 0) / maxY) * ih;
        const isSelected = selectedSpikeDate === d;

        ctx.strokeStyle = '#dc2626';
        ctx.lineWidth = isSelected ? 2 : 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(x, pad.t);
        ctx.lineTo(x, pad.t + ih);
        ctx.stroke();
        ctx.setLineDash([]);

        ctx.fillStyle = '#dc2626';
        ctx.beginPath();
        ctx.arc(x, y, isSelected ? 6 : 4, 0, Math.PI * 2);
        ctx.fill();

        if (isSelected) {{
          ctx.strokeStyle = '#991b1b';
          ctx.lineWidth = 2;
          ctx.beginPath();
          ctx.arc(x, y, 8, 0, Math.PI * 2);
          ctx.stroke();
        }}
      }});

      ctx.fillStyle = '#6b7280';
      ctx.font = '11px sans-serif';
      for (let i = 0; i <= 4; i++) {{
        const v = (maxY * (1 - i / 4)).toFixed(2);
        const y = pad.t + (ih * i / 4);
        ctx.fillText(`$${{v}}`, 4, y + 3);
      }}

      const step = Math.ceil(labels.length / 8);
      for (let i = 0; i < labels.length; i += step) {{
        const x = pad.l + (iw * i / Math.max(1, labels.length - 1));
        ctx.fillText(labels[i].slice(5), x - 14, h - 12);
      }}

      let lx = pad.l, ly = 16;
      names.forEach((name, idx) => {{
        ctx.fillStyle = colors[idx % colors.length];
        ctx.fillRect(lx, ly - 8, 12, 12);
        ctx.fillStyle = '#111827';
        ctx.fillText(name, lx + 16, ly + 2);
        lx += 90;
      }});
    }}

    function nearestIndex(clientX) {{
      const rect = canvas.getBoundingClientRect();
      const padL = 46, padR = 12;
      const iw = rect.width - padL - padR;
      const x = Math.min(rect.width - padR, Math.max(padL, clientX - rect.left));
      return Math.round(((x - padL) / Math.max(1, iw)) * Math.max(1, labels.length - 1));
    }}

    function focusSpikeDate(d, scrollRow = false) {{
      if (!spikeByDate[d]) return;
      focusDate(d);
      const row = document.getElementById(`spike-row-${{d}}`);
      if (!row) return;
      if (scrollRow) row.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
      row.classList.add('spike-focus');
      setTimeout(() => row.classList.remove('spike-focus'), 1200);
    }}

    function updateHash() {{
      try {{
        const params = new URLSearchParams();
        if (selectedDate) params.set('date', selectedDate);
        if (spikeOnlyMode) params.set('spikeOnly', '1');
        if (sortByDodMode) params.set('sortDod', '1');
        if (showOnlyChangesMode) params.set('changesOnly', '1');
        const hash = params.toString();
        history.replaceState(null, '', hash ? `#${{hash}}` : '#');
      }} catch (e) {{}}
    }}

    function focusDate(d) {{
      if (!labels.includes(d)) return;
      selectedDate = d;
      renderSelectedDay(d);
      if (spikeByDate[d]) {{
        selectedSpikeDate = d;
      }} else {{
        selectedSpikeDate = null;
      }}
      updateHash();
      draw();
    }}

    function getInitialStateFromHash() {{
      const raw = window.location.hash.startsWith('#') ? window.location.hash.slice(1) : window.location.hash;
      if (!raw) return {{ date: null, spikeOnly: false, sortDod: false, changesOnly: false }};
      const p = new URLSearchParams(raw);
      const date = p.get('date');
      const spikeOnly = p.get('spikeOnly') === '1';
      const sortDod = p.get('sortDod') === '1';
      const changesOnly = p.get('changesOnly') === '1';
      return {{
        date: date && labels.includes(date) ? date : null,
        spikeOnly,
        sortDod,
        changesOnly,
      }};
    }}

    function stepDate(offset) {{
      if (!labels.length) return;
      const baseDates = spikeOnlyMode ? labels.filter(d => !!spikeByDate[d]) : labels;
      if (!baseDates.length) return;
      const current = selectedDate && baseDates.includes(selectedDate)
        ? selectedDate
        : baseDates[baseDates.length - 1];
      const idx = baseDates.indexOf(current);
      const next = Math.max(0, Math.min(baseDates.length - 1, idx + offset));
      focusDate(baseDates[next]);
    }}

    function jumpSpike(offset) {{
      const spikeDates = labels.filter(d => !!spikeByDate[d]);
      if (!spikeDates.length) return;
      const current = selectedDate && labels.includes(selectedDate)
        ? selectedDate
        : labels[labels.length - 1];
      let idx = spikeDates.indexOf(current);
      if (idx < 0) {{
        idx = offset >= 0 ? -1 : spikeDates.length;
      }}
      const next = Math.max(0, Math.min(spikeDates.length - 1, idx + offset));
      focusSpikeDate(spikeDates[next], true);
    }}

    function escapeHtml(text) {{
      return String(text)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }}

    canvas.addEventListener('mousemove', (ev) => {{
      if (!labels.length) return;
      const i = nearestIndex(ev.clientX);
      const date = labels[i];
      const dailyRows = (dayBreakdownByDate[date] || []).slice(0, 10);
      const overflowCount = Math.max(0, (dayBreakdownByDate[date] || []).length - dailyRows.length);
      const spike = spikeByDate[date];

      const modelRowsHtml = dailyRows.length
        ? dailyRows.map((r) => `<div>${{escapeHtml(r.model)}}: <strong>$${{(r.costUSD || 0).toFixed(2)}}</strong></div>`).join('')
        : '<div style="color:#d1d5db;">No model cost rows</div>';
      const overflowHtml = overflowCount > 0
        ? `<div style="color:#d1d5db;">+${{overflowCount}} more models…</div>`
        : '';
      const spikeHtml = spike
        ? `<div style="margin-top:6px;color:#fca5a5;">⚠ Spike: $${{(spike.costUSD || 0).toFixed(2)}} / baseline $${{(spike.baselineUSD || 0).toFixed(2)}} (${{(spike.ratio || 0).toFixed(2)}}x)</div>`
        : '';

      tip.innerHTML = `
        <div style="font-weight:700;">${{date}}</div>
        <div style="margin:2px 0 6px;">Total: <strong>$${{(dayTotals[i] || 0).toFixed(2)}}</strong></div>
        <div style="color:#d1d5db;margin-bottom:4px;">Daily model costs</div>
        ${{modelRowsHtml}}
        ${{overflowHtml}}
        ${{spikeHtml}}
      `;

      const wr = wrap.getBoundingClientRect();
      tip.style.left = `${{ev.clientX - wr.left}}px`;
      tip.style.top = `${{ev.clientY - wr.top}}px`;
      tip.style.opacity = '0.95';
    }});
    canvas.addEventListener('mouseleave', () => {{ tip.style.opacity = '0'; }});

    canvas.addEventListener('click', (ev) => {{
      if (!labels.length) return;
      const i = nearestIndex(ev.clientX);
      const d = labels[i];
      focusDate(d);
      if (spikeByDate[d]) focusSpikeDate(d, true);
    }});

    spikesBody?.addEventListener('click', (ev) => {{
      const tr = ev.target?.closest?.("tr[id^='spike-row-']");
      if (!tr) return;
      const d = tr.id.replace('spike-row-', '');
      focusSpikeDate(d, false);
    }});

    const initialState = getInitialStateFromHash();
    spikeOnlyMode = !!initialState.spikeOnly;
    sortByDodMode = !!initialState.sortDod;
    showOnlyChangesMode = !!initialState.changesOnly;
    if (spikeOnlyToggle) spikeOnlyToggle.checked = spikeOnlyMode;
    if (sortByDodToggle) sortByDodToggle.checked = sortByDodMode;
    if (showOnlyChangesToggle) showOnlyChangesToggle.checked = showOnlyChangesMode;

    if (labels.length) {{
      const initial = initialState.date || labels[labels.length - 1];
      focusDate(initial);
    }}

    spikeOnlyToggle?.addEventListener('change', () => {{
      spikeOnlyMode = !!spikeOnlyToggle.checked;
      updateHash();
    }});

    sortByDodToggle?.addEventListener('change', () => {{
      sortByDodMode = !!sortByDodToggle.checked;
      if (selectedDate) renderSelectedDay(selectedDate);
      updateHash();
    }});

    showOnlyChangesToggle?.addEventListener('change', () => {{
      showOnlyChangesMode = !!showOnlyChangesToggle.checked;
      if (selectedDate) renderSelectedDay(selectedDate);
      updateHash();
    }});

    copyLinkBtn?.addEventListener('click', () => {{
      copyDeepLink();
    }});

    function toggleSpikeOnlyMode() {{
      spikeOnlyMode = !spikeOnlyMode;
      if (spikeOnlyToggle) spikeOnlyToggle.checked = spikeOnlyMode;
      if (spikeOnlyMode && selectedDate && !spikeByDate[selectedDate]) {{
        jumpSpike(1);
      }} else {{
        updateHash();
      }}
    }}

    function toggleDodSortMode() {{
      sortByDodMode = !sortByDodMode;
      if (sortByDodToggle) sortByDodToggle.checked = sortByDodMode;
      if (selectedDate) renderSelectedDay(selectedDate);
      updateHash();
    }}

    function toggleChangesOnlyMode() {{
      showOnlyChangesMode = !showOnlyChangesMode;
      if (showOnlyChangesToggle) showOnlyChangesToggle.checked = showOnlyChangesMode;
      if (selectedDate) renderSelectedDay(selectedDate);
      updateHash();
    }}

    function toggleKeyboardHelp(force = null) {{
      if (!kbdHelp) return;
      const next = force === null ? !kbdHelp.classList.contains('visible') : !!force;
      kbdHelp.classList.toggle('visible', next);
    }}

    function currentDeepLink() {{
      const base = `${{window.location.origin}}${{window.location.pathname}}`;
      const hash = window.location.hash || '';
      return `${{base}}${{hash}}`;
    }}

    async function copyDeepLink() {{
      const link = currentDeepLink();
      try {{
        if (navigator.clipboard?.writeText) {{
          await navigator.clipboard.writeText(link);
          if (copyLinkHint) copyLinkHint.textContent = 'Copied';
        }} else {{
          window.prompt('Copy this link:', link);
          if (copyLinkHint) copyLinkHint.textContent = 'Shown in prompt';
        }}
      }} catch (e) {{
        window.prompt('Copy this link:', link);
        if (copyLinkHint) copyLinkHint.textContent = 'Shown in prompt';
      }}
      setTimeout(() => {{ if (copyLinkHint) copyLinkHint.textContent = ''; }}, 1200);
    }}

    function resetToLatestDay() {{
      if (!labels.length) return;
      spikeOnlyMode = false;
      sortByDodMode = false;
      showOnlyChangesMode = false;
      if (spikeOnlyToggle) spikeOnlyToggle.checked = false;
      if (sortByDodToggle) sortByDodToggle.checked = false;
      if (showOnlyChangesToggle) showOnlyChangesToggle.checked = false;
      focusDate(labels[labels.length - 1]);
    }}

    window.addEventListener('keydown', (ev) => {{
      const tag = (ev.target && ev.target.tagName) ? ev.target.tagName.toUpperCase() : '';
      const isEditable = tag === 'INPUT' || tag === 'TEXTAREA' || (ev.target && ev.target.isContentEditable);
      if (isEditable) return;
      if (ev.metaKey || ev.ctrlKey || ev.altKey) return;
      if (ev.key === 'ArrowLeft' || ev.key === 'j') {{
        ev.preventDefault();
        stepDate(-1);
      }}
      if (ev.key === 'ArrowRight' || ev.key === 'k') {{
        ev.preventDefault();
        stepDate(1);
      }}
      if (ev.key === 'n') {{
        ev.preventDefault();
        jumpSpike(1);
      }}
      if (ev.key === 'p') {{
        ev.preventDefault();
        jumpSpike(-1);
      }}
      if (ev.key === 'Home') {{
        ev.preventDefault();
        if (labels.length) focusDate(labels[0]);
      }}
      if (ev.key === 'End') {{
        ev.preventDefault();
        if (labels.length) focusDate(labels[labels.length - 1]);
      }}
      if (ev.key === 's') {{
        ev.preventDefault();
        toggleSpikeOnlyMode();
      }}
      if (ev.key === 'd') {{
        ev.preventDefault();
        toggleDodSortMode();
      }}
      if (ev.key === 'x') {{
        ev.preventDefault();
        toggleChangesOnlyMode();
      }}
      if (ev.key === 'r') {{
        ev.preventDefault();
        resetToLatestDay();
      }}
      if (ev.key === 'c') {{
        ev.preventDefault();
        copyDeepLink();
      }}
      if (ev.key === '?' || (ev.key === '/' && ev.shiftKey)) {{
        ev.preventDefault();
        toggleKeyboardHelp();
      }}
      if (ev.key === 'Escape') {{
        toggleKeyboardHelp(false);
      }}
    }});

    if (!accessPolicy.canViewMovers) {{
      hideSectionByHeading('Top 7d Movers', 'You do not have permission to view movers.');
    }}
    if (!accessPolicy.canViewSpikes) {{
      hideSectionByHeading('Daily Cost Spikes', 'You do not have permission to view spikes.');
    }}
    if (!accessPolicy.canUseCustomReportBuilder) {{
      hideSectionByHeading('Custom Report Builder', 'You do not have permission to use custom reports.');
    }} else {{
      initCustomReportBuilder();
    }}
    window.addEventListener('resize', resize);
    resize();
  </script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate local token usage dashboard from CodexBar JSON.")
    parser.add_argument("--provider", choices=["codex", "claude"], default="codex")
    parser.add_argument("--input", help="Path to codexbar JSON (or '-' for stdin)")
    parser.add_argument("--days", type=positive_int, help="Limit to last N days")
    parser.add_argument("--top-models", type=positive_int, default=6, help="Top models to chart")
    parser.add_argument("--spike-lookback-days", type=positive_int, default=7, help="Lookback window (days) for spike baseline")
    parser.add_argument("--spike-threshold-mult", type=positive_float, default=2.0, help="Spike threshold multiplier vs baseline")
    parser.add_argument("--forecast-days", type=positive_int, default=7, help="Forecast N future days")
    parser.add_argument("--forecast-lookback-days", type=positive_int, default=30, help="History window used for forecast regression")
    parser.add_argument("--anomaly-lookback-days", type=positive_int, default=14, help="Lookback days used for anomaly z-score baseline")
    parser.add_argument("--anomaly-z-threshold", type=positive_float, default=2.5, help="Alert when z-score is above this threshold")
    parser.add_argument("--anomaly-min-cost", type=positive_float, default=0.01, help="Minimum daily cost required to emit anomaly alert")
    parser.add_argument("--output", default="token_usage_dashboard.html", help="Output HTML file path")
    parser.add_argument("--summary-json", help="Also write summary JSON to this path")
    parser.add_argument("--custom-report-json", help="Write custom report JSON to this path")
    parser.add_argument("--report-metrics", default="total_cost", help="Comma-separated: total_cost,active_models,avg_cost_per_model")
    parser.add_argument("--report-models", help="Comma-separated model filter (blank means all models)")
    parser.add_argument("--report-granularity", choices=["daily", "weekly", "monthly"], default="daily", help="Custom report time granularity")
    parser.add_argument("--max-table-rows", type=positive_int, default=120, help="Render at most N model rows in summary tables")
    parser.add_argument("--chart-max-points", type=positive_int, default=1200, help="Downsample chart to at most N points for large datasets")
    parser.add_argument("--role", help="RBAC role used for data access (supports custom roles)")
    parser.add_argument("--user", help="Username for role mapping (used with --rbac-config / --tenant-config)")
    parser.add_argument("--rbac-config", help="Path to RBAC JSON config with users/roles/policies")
    parser.add_argument("--tenant-config", help="Path to multi-tenant organization config JSON")
    parser.add_argument("--org-id", help="Organization ID for strict tenant isolation")
    parser.add_argument("--dashboard-view", help="Dashboard view ID scoped to selected organization")
    parser.add_argument("--allow-role-override", action="store_true", help="Allow --role to override user role in tenant mode")
    parser.add_argument("--manage-users", choices=["list", "create", "update", "delete"], help="Manage users in tenant config then exit")
    parser.add_argument("--target-user", help="Target username for --manage-users")
    parser.add_argument("--target-role", help="Target role for --manage-users create/update")
    parser.add_argument("--target-group", help="Target group for --manage-users create/update")
    parser.add_argument("--manage-views", choices=["list", "create", "update", "delete", "assign", "unassign"], help="Manage dashboard views in tenant config then exit")
    parser.add_argument("--view-id", help="Dashboard view ID for --manage-views")
    parser.add_argument("--view-models", help="Comma-separated allowed model names for view create/update")
    parser.add_argument("--view-max-days", type=positive_int, help="Max days filter for view create/update")
    parser.add_argument("--view-group", help="Group name for view assign/unassign")
    parser.add_argument("--open", action="store_true", help="Open dashboard in default browser")
    args = parser.parse_args()

    if args.manage_users or args.manage_views:
        if not args.tenant_config or not args.org_id:
            eprint("--tenant-config and --org-id are required for tenant management operations.")
            return 5
        try:
            result = manage_tenant_config(
                tenant_config_path=args.tenant_config,
                org_id=args.org_id,
                user_action=args.manage_users,
                target_user=args.target_user,
                target_role=args.target_role,
                target_group=args.target_group,
                view_action=args.manage_views,
                view_id=args.view_id,
                view_models=args.view_models,
                view_max_days=args.view_max_days,
                view_group=args.view_group,
            )
        except Exception as exc:
            eprint(f"Failed to manage tenant config: {exc}")
            return 6
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    try:
        payload = load_payload(args.input, args.provider)
    except Exception as exc:
        eprint(str(exc))
        return 1

    tenant_meta: Dict[str, Any] = {}
    if args.tenant_config:
        try:
            tenant_rows, role_name, policy, tenant_meta = resolve_multi_tenant_context(
                payload=payload,
                tenant_config_path=args.tenant_config,
                org_id=args.org_id,
                role=args.role,
                user=args.user,
                requested_dashboard=args.dashboard_view,
            )
        except Exception as exc:
            eprint(f"Failed to resolve tenant context: {exc}")
            return 3
        rows = filter_days(tenant_rows, args.days)
    else:
        rows = filter_days(parse_daily(payload), args.days)
        try:
            role_name, policy = resolve_access_policy(args.role, args.user, args.rbac_config)
        except Exception as exc:
            eprint(f"Failed to resolve RBAC policy: {exc}")
            return 3

    if not rows:
        eprint("No daily rows found in payload.")
        return 2

    rows = apply_access_policy(rows, policy)

    html = build_dashboard_html(
        args.provider,
        rows,
        top_models=args.top_models,
        spike_lookback_days=args.spike_lookback_days,
        spike_threshold_mult=args.spike_threshold_mult,
        forecast_days=args.forecast_days,
        forecast_lookback_days=args.forecast_lookback_days,
        anomaly_lookback_days=args.anomaly_lookback_days,
        anomaly_z_threshold=args.anomaly_z_threshold,
        anomaly_min_cost=args.anomaly_min_cost,
        max_table_rows=args.max_table_rows,
        chart_max_points=args.chart_max_points,
        role_name=role_name,
        policy=policy,
    )
    out = Path(args.output)
    out.write_text(html, encoding="utf-8")

    if args.summary_json:
        summary = build_summary(
            args.provider,
            rows,
            spike_lookback_days=args.spike_lookback_days,
            spike_threshold_mult=args.spike_threshold_mult,
            forecast_days=args.forecast_days,
            forecast_lookback_days=args.forecast_lookback_days,
            anomaly_lookback_days=args.anomaly_lookback_days,
            anomaly_z_threshold=args.anomaly_z_threshold,
            anomaly_min_cost=args.anomaly_min_cost,
        )
        summary["role"] = role_name
        summary["policy"] = policy
        if tenant_meta:
            summary["tenant"] = tenant_meta
        Path(args.summary_json).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.custom_report_json:
        if not policy.get("canUseCustomReportBuilder", True):
            eprint(f"Role '{role_name}' does not have permission to export custom report JSON.")
            return 4
        metrics = [x.strip() for x in (args.report_metrics or '').split(',') if x.strip()]
        models = [x.strip() for x in (args.report_models or '').split(',') if x.strip()] if args.report_models else []
        report_rows = generate_custom_report(rows, metrics=metrics, models=models, granularity=args.report_granularity)
        report_payload = {
            "provider": args.provider,
            "role": role_name,
            "granularity": args.report_granularity,
            "metrics": metrics,
            "models": models,
            "rows": report_rows,
            "tenant": tenant_meta or None,
        }
        Path(args.custom_report_json).write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    resolved = str(out.resolve())
    print(resolved)

    if args.open:
        webbrowser.open(out.resolve().as_uri())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

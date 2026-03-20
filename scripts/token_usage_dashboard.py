#!/usr/bin/env python3
"""
Generate a local HTML dashboard for CodexBar model usage/cost data.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from collections import defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
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
        "canViewPatternAnalysis": True,
    },
    "analyst": {
        "canViewTotals": True,
        "canViewModelBreakdown": True,
        "canViewModelNames": True,
        "canViewSpikes": True,
        "canViewMovers": True,
        "canUseCustomReportBuilder": True,
        "canViewPatternAnalysis": True,
    },
    "viewer": {
        "canViewTotals": True,
        "canViewModelBreakdown": False,
        "canViewModelNames": False,
        "canViewSpikes": False,
        "canViewMovers": False,
        "canUseCustomReportBuilder": False,
        "canViewPatternAnalysis": False,
    },
}


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def json_for_script(value: Any) -> str:
    # Prevent inline-script breakouts (`</script>`) and JS line-separator hazards.
    return (
        json.dumps(value, ensure_ascii=False)
        .replace("</", "<\\/")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


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


def parse_provider_list(value: Optional[str], default: Optional[List[str]] = None) -> List[str]:
    if value is None:
        return list(default or [])
    raw = [x.strip().lower() for x in value.split(",") if x.strip()]
    allowed = {"codex", "claude"}
    out: List[str] = []
    for p in raw:
        if p not in allowed:
            raise RuntimeError(f"Unsupported provider '{p}'. Allowed: codex, claude")
        if p not in out:
            out.append(p)
    return out


def load_payload_bundle(input_path: Optional[str], providers: List[str]) -> Dict[str, Dict[str, Any]]:
    if not providers:
        return {}

    if not input_path:
        return {p: run_codexbar_cost(p) for p in providers}

    if input_path == "-":
        raw = sys.stdin.read()
    else:
        raw = Path(input_path).read_text(encoding="utf-8")

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse input JSON: {exc}")

    return {p: normalize_provider_payload(parsed, p) for p in providers}


def _normalize_cloud_cost_payload(parsed: Any) -> List[Dict[str, Any]]:
    """Normalize cloud-cost records from simple normalized JSON / AWS CE / GCP billing-like payloads.

    Normalized output item:
    {
      "date": "YYYY-MM-DD",
      "provider": "aws|gcp|...",
      "service": "ec2|bigquery|...",
      "project": "optional project/account",
      "costUSD": 12.34,
      "currency": "USD",
      "source": "aws_cost_explorer|gcp_billing_export|normalized"
    }
    """
    out: List[Dict[str, Any]] = []

    def push(
        d: Any,
        provider: Any,
        service: Any,
        cost: Any,
        *,
        project: Any = None,
        currency: Any = "USD",
        source: str = "normalized",
        tags: Any = None,
    ) -> None:
        ds = str(d or "").strip()
        if not parse_date(ds):
            return
        c = _safe_float(cost, default=-1.0)
        if c < 0:
            return
        out.append(
            {
                "date": ds,
                "provider": str(provider or "unknown").lower(),
                "service": str(service or "all"),
                "project": str(project or ""),
                "costUSD": c,
                "currency": str(currency or "USD"),
                "source": source,
                "tags": _normalize_cloud_tags(tags),
            }
        )

    # Format A: already normalized list
    if isinstance(parsed, list):
        for row in parsed:
            if not isinstance(row, dict):
                continue
            push(
                row.get("date"),
                row.get("provider"),
                row.get("service"),
                row.get("costUSD") if row.get("costUSD") is not None else row.get("cost"),
                project=row.get("project"),
                currency=row.get("currency", "USD"),
                source=str(row.get("source") or "normalized"),
                tags=row.get("tags") or row.get("labels"),
            )
        return sorted(out, key=lambda x: (x["date"], x["provider"], x["service"], x["project"]))

    if not isinstance(parsed, dict):
        return []

    # Format B: wrapper with records
    if isinstance(parsed.get("records"), list):
        return _normalize_cloud_cost_payload(parsed.get("records"))

    # Format C: AWS Cost Explorer-like payload
    if isinstance(parsed.get("ResultsByTime"), list):
        for bucket in parsed.get("ResultsByTime", []):
            if not isinstance(bucket, dict):
                continue
            d = (bucket.get("TimePeriod") or {}).get("Start") if isinstance(bucket.get("TimePeriod"), dict) else bucket.get("date")
            groups = bucket.get("Groups") if isinstance(bucket.get("Groups"), list) else []
            if groups:
                for g in groups:
                    if not isinstance(g, dict):
                        continue
                    keys = g.get("Keys") if isinstance(g.get("Keys"), list) else []
                    service = str(keys[0]) if keys else "all"
                    tag_items = []
                    for raw_key in keys[1:]:
                        txt = str(raw_key)
                        if '$' in txt:
                            tk, tv = txt.split('$', 1)
                            tag_items.append(f"{tk}={tv}")
                        elif '=' in txt or ':' in txt:
                            tag_items.append(txt)
                    metrics = g.get("Metrics") if isinstance(g.get("Metrics"), dict) else {}
                    amount_node = (metrics.get("UnblendedCost") if isinstance(metrics.get("UnblendedCost"), dict) else {})
                    push(
                        d,
                        "aws",
                        service,
                        amount_node.get("Amount"),
                        currency=amount_node.get("Unit") or "USD",
                        source="aws_cost_explorer",
                        tags=tag_items,
                    )
            else:
                total = bucket.get("Total") if isinstance(bucket.get("Total"), dict) else {}
                amount_node = total.get("UnblendedCost") if isinstance(total.get("UnblendedCost"), dict) else {}
                push(
                    d,
                    "aws",
                    "all",
                    amount_node.get("Amount"),
                    currency=amount_node.get("Unit") or "USD",
                    source="aws_cost_explorer",
                )

    # Format D: GCP billing export-like daily rows
    daily = parsed.get("daily") if isinstance(parsed.get("daily"), list) else []
    for row in daily:
        if not isinstance(row, dict):
            continue
        push(
            row.get("date"),
            row.get("provider") or "gcp",
            row.get("service") or row.get("serviceName") or row.get("sku") or "all",
            row.get("costUSD") if row.get("costUSD") is not None else row.get("cost"),
            project=row.get("project") or row.get("projectId"),
            currency=row.get("currency") or "USD",
            source=str(row.get("source") or "gcp_billing_export"),
            tags=row.get("tags") or row.get("labels"),
        )

    return sorted(out, key=lambda x: (x["date"], x["provider"], x["service"], x["project"]))


def _normalize_cloud_tag_mapping_rules(mapping: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not mapping:
        return []

    # Backward-compatible: {"cost_center": "businessLine"}
    if all(isinstance(v, str) for v in mapping.values()):
        return [
            {
                "from": [str(k).strip().lower()],
                "target": str(v).strip(),
            }
            for k, v in mapping.items()
            if str(k).strip() and str(v).strip()
        ]

    base = mapping.get("mapping") if isinstance(mapping.get("mapping"), dict) else {}
    explicit_rules = mapping.get("rules") if isinstance(mapping.get("rules"), list) else []
    out: List[Dict[str, Any]] = []

    for k, v in base.items():
        kk = str(k or "").strip().lower()
        vv = str(v or "").strip()
        if kk and vv:
            out.append({"from": [kk], "target": vv})

    for r in explicit_rules:
        if not isinstance(r, dict):
            continue
        target = str(r.get("target") or "").strip()
        if not target:
            continue
        sources_raw = r.get("from") if isinstance(r.get("from"), list) else []
        if not sources_raw and r.get("tag"):
            sources_raw = [r.get("tag")]
        aliases_raw = r.get("aliases") if isinstance(r.get("aliases"), list) else []
        sources = [str(x).strip().lower() for x in [*sources_raw, *aliases_raw] if str(x).strip()]
        if not sources:
            continue
        value_map_raw = r.get("valueMap") if isinstance(r.get("valueMap"), dict) else {}
        value_map = {
            str(k).strip().lower(): str(v).strip()
            for k, v in value_map_raw.items()
            if str(k).strip() and str(v).strip()
        }
        out.append({
            "from": sources,
            "target": target,
            "default": str(r.get("default") or "").strip() or None,
            "valueMap": value_map,
        })
    return out


def _apply_cloud_tag_mapping(rows: List[Dict[str, Any]], mapping: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    rules = _normalize_cloud_tag_mapping_rules(mapping)
    if not rules:
        return rows
    out: List[Dict[str, Any]] = []
    for row in rows:
        tags = _normalize_cloud_tags(row.get("tags"))
        mapped = dict(row)
        mapped["tags"] = tags
        for rule in rules:
            target = str(rule.get("target") or "").strip()
            if not target:
                continue
            value: Optional[str] = None
            for src in rule.get("from") or []:
                if src in tags and tags[src]:
                    value = tags[src]
                    break
            if value is None:
                value = rule.get("default")
            if not value:
                continue
            value_map = rule.get("valueMap") if isinstance(rule.get("valueMap"), dict) else {}
            transformed = value_map.get(str(value).strip().lower()) if value_map else None
            mapped[target] = transformed if transformed else value
        out.append(mapped)
    return out


def load_cloud_cost_rows(path: Optional[str], tag_mapping: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    if not path:
        return []
    raw = Path(path).read_text(encoding="utf-8")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse cloud cost JSON: {exc}")
    rows = _normalize_cloud_cost_payload(parsed)
    return _apply_cloud_tag_mapping(rows, tag_mapping)




def _normalize_cloud_tags(raw_tags: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if isinstance(raw_tags, dict):
        for k, v in raw_tags.items():
            key = str(k or '').strip().lower()
            if not key:
                continue
            val = str(v or '').strip()
            if val:
                out[key] = val
    elif isinstance(raw_tags, list):
        for item in raw_tags:
            if isinstance(item, dict):
                key = str(item.get('key') or item.get('name') or '').strip().lower()
                val = str(item.get('value') or '').strip()
                if key and val:
                    out[key] = val
            elif isinstance(item, str):
                if '=' in item:
                    k, v = item.split('=', 1)
                elif ':' in item:
                    k, v = item.split(':', 1)
                else:
                    continue
                key = k.strip().lower()
                val = v.strip()
                if key and val:
                    out[key] = val
    return out


def _load_cloud_tag_mapping_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    raw = Path(path).read_text(encoding='utf-8')
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError('Cloud tag mapping config must be a JSON object.')
    rules = _normalize_cloud_tag_mapping_rules(parsed)
    if not rules:
        raise RuntimeError('Cloud tag mapping config must include valid mapping rules.')
    return parsed

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


def _safe_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except (TypeError, ValueError):
            return default
    return default


def _safe_int(value: Any, default: int = 0, minimum: Optional[int] = None) -> int:
    if isinstance(value, bool):
        out = default
    elif isinstance(value, int):
        out = value
    elif isinstance(value, float):
        out = int(value)
    elif isinstance(value, str):
        try:
            out = int(float(value.strip()))
        except (TypeError, ValueError):
            out = default
    else:
        out = default
    if minimum is not None:
        return max(minimum, out)
    return out


def _normalize_call_records(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build normalized per-call records from row-level payloads.

    Supported nested keys (per day): llmCalls/apiCalls/requests/events.
    """
    out: List[Dict[str, Any]] = []
    for row in rows:
        day = str(row.get("date") or "")
        for key in ("llmCalls", "apiCalls", "requests", "events"):
            raw_calls = row.get(key)
            if not isinstance(raw_calls, list):
                continue
            for c in raw_calls:
                if not isinstance(c, dict):
                    continue
                prompt_tokens = _safe_float(c.get("promptTokens") or c.get("inputTokens"))
                completion_tokens = _safe_float(c.get("completionTokens") or c.get("outputTokens"))
                total_tokens = _safe_float(c.get("totalTokens"), prompt_tokens + completion_tokens)
                cost = _safe_float(c.get("cost") or c.get("costUSD"))
                latency_ms = _safe_float(c.get("latencyMs") or c.get("responseMs") or c.get("durationMs"), default=-1.0)
                model = c.get("modelName") or c.get("model") or "unknown"
                model_type = c.get("modelType") or str(model).split("-")[0]
                task = c.get("useCase") or c.get("task") or c.get("scenario") or "unspecified"
                out.append(
                    {
                        "date": day,
                        "model": str(model),
                        "modelType": str(model_type),
                        "task": str(task),
                        "user": str(c.get("userId") or c.get("user") or "unknown"),
                        "project": str(c.get("projectId") or c.get("project") or "unknown"),
                        "session": str(c.get("sessionId") or c.get("conversationId") or "unknown"),
                        "workflow": str(c.get("workflowId") or c.get("flowId") or task),
                        "department": str(c.get("department") or c.get("dept") or c.get("team") or "unknown"),
                        "application": str(c.get("application") or c.get("app") or c.get("service") or "unknown"),
                        "businessLine": str(c.get("businessLine") or c.get("bizLine") or c.get("costCenter") or "unknown"),
                        "promptTokens": prompt_tokens,
                        "completionTokens": completion_tokens,
                        "totalTokens": total_tokens,
                        "costUSD": cost,
                        "latencyMs": latency_ms if latency_ms >= 0 else None,
                        "prompt": str(c.get("prompt") or ""),
                    }
                )
    return out


def _quantile(sorted_values: List[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    idx = (len(sorted_values) - 1) * q
    lo = int(idx)
    hi = min(len(sorted_values) - 1, lo + 1)
    frac = idx - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def _top_dimension(records: List[Dict[str, Any]], key: str, limit: int = 8) -> List[Dict[str, Any]]:
    agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"costUSD": 0.0, "totalTokens": 0.0, "promptTokens": 0.0, "completionTokens": 0.0, "count": 0.0})
    for r in records:
        dim = str(r.get(key) or "unknown")
        agg[dim]["costUSD"] += _safe_float(r.get("costUSD"))
        agg[dim]["totalTokens"] += _safe_float(r.get("totalTokens"))
        agg[dim]["promptTokens"] += _safe_float(r.get("promptTokens"))
        agg[dim]["completionTokens"] += _safe_float(r.get("completionTokens"))
        agg[dim]["count"] += 1.0
    ranked = sorted(agg.items(), key=lambda kv: (kv[1]["costUSD"], kv[1]["totalTokens"]), reverse=True)
    return [{"key": k, **v} for k, v in ranked[:limit]]


def _tokenize_prompt_anonymized(text: str) -> List[str]:
    masked = (text or "").lower()
    # Mask sensitive patterns before tokenization
    masked = re.sub(r"\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b", " <email> ", masked)
    masked = re.sub(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b", " <uuid> ", masked)
    masked = re.sub(r"\b\d{6,}\b", " <number> ", masked)

    cleaned = re.findall(r"[\w<>-]+", masked)
    stop = {"the", "and", "for", "that", "this", "with", "from", "are", "was", "you", "your", "請", "幫", "一下", "一個", "我們", "需要", "分析"}
    out: List[str] = []
    for t in cleaned:
        if t in {"<email>", "<uuid>", "<number>"}:
            out.append(t)
            continue
        if len(t) < 3 or t in stop:
            continue
        # Mask probable long IDs with mixed alnum (session / trace IDs)
        if len(t) >= 12 and any(ch.isdigit() for ch in t) and any(ch.isalpha() for ch in t):
            out.append("<id>")
            continue
        out.append(t)
    return out


def build_llm_pattern_analysis(
    rows: List[Dict[str, Any]],
    normalized_records: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    records = normalized_records if normalized_records is not None else _normalize_call_records(rows)
    if not records:
        return {"available": False, "reason": "No call-level payload (llmCalls/apiCalls/requests/events)."}

    prompt_values = sorted([_safe_float(r.get("promptTokens")) for r in records])
    completion_values = sorted([_safe_float(r.get("completionTokens")) for r in records])

    by_model = _top_dimension(records, "model")
    by_model_type = _top_dimension(records, "modelType")
    by_task = _top_dimension(records, "task")
    by_user = _top_dimension(records, "user")
    by_project = _top_dimension(records, "project")

    # high-consumption hotspots
    top_calls = sorted(records, key=lambda r: (_safe_float(r.get("costUSD")), _safe_float(r.get("totalTokens"))), reverse=True)[:10]
    by_session = _top_dimension(records, "session", limit=10)
    by_workflow = _top_dimension(records, "workflow", limit=10)

    # model efficiency
    efficiency: List[Dict[str, Any]] = []
    for m in by_model:
        total_tokens = _safe_float(m.get("totalTokens"))
        total_cost = _safe_float(m.get("costUSD"))
        matching = [r for r in records if r.get("model") == m.get("key") and isinstance(r.get("latencyMs"), (int, float))]
        avg_latency = (sum(float(r["latencyMs"]) for r in matching) / len(matching)) if matching else None
        efficiency.append(
            {
                "model": m.get("key"),
                "costUSD": total_cost,
                "totalTokens": total_tokens,
                "costPer1kTokensUSD": (total_cost / total_tokens * 1000.0) if total_tokens > 0 else None,
                "avgLatencyMs": avg_latency,
                "sampleCount": int(m.get("count", 0)),
            }
        )

    keyword_counts: Dict[str, int] = defaultdict(int)
    for r in records:
        for tk in _tokenize_prompt_anonymized(str(r.get("prompt") or "")):
            keyword_counts[tk] += 1
    top_keywords = sorted(keyword_counts.items(), key=lambda kv: kv[1], reverse=True)[:20]

    return {
        "available": True,
        "calls": len(records),
        "promptTokens": {
            "avg": sum(prompt_values) / len(prompt_values),
            "p50": _quantile(prompt_values, 0.5),
            "p95": _quantile(prompt_values, 0.95),
        },
        "completionTokens": {
            "avg": sum(completion_values) / len(completion_values),
            "p50": _quantile(completion_values, 0.5),
            "p95": _quantile(completion_values, 0.95),
        },
        "dimensions": {
            "byModel": by_model,
            "byModelType": by_model_type,
            "byTask": by_task,
            "byUser": by_user,
            "byProject": by_project,
        },
        "hotspots": {
            "topApiCalls": top_calls,
            "topSessions": by_session,
            "topWorkflows": by_workflow,
        },
        "efficiency": efficiency,
        "anonymizedPromptKeywords": [{"keyword": k, "count": v} for k, v in top_keywords],
    }


def build_cost_attribution(
    rows: List[Dict[str, Any]],
    top_n: int = 8,
    normalized_records: Optional[List[Dict[str, Any]]] = None,
    cloud_rows: Optional[List[Dict[str, Any]]] = None,
    granularity: str = "standard",
) -> Dict[str, Any]:
    records = normalized_records if normalized_records is not None else _normalize_call_records(rows)
    day_total = sum(day_total_cost(r) for r in rows)
    if not records:
        return {
            "available": False,
            "reason": "No call-level payload (llmCalls/apiCalls/requests/events).",
            "dimensions": {},
            "unallocatedCostUSD": day_total,
            "totalAttributedCostUSD": 0.0,
        }

    total_cost = sum(_safe_float(r.get("costUSD")) for r in records)

    def _aggregate(key: str) -> List[Dict[str, Any]]:
        agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"costUSD": 0.0, "totalTokens": 0.0, "count": 0.0})
        for r in records:
            k = str(r.get(key) or "unknown")
            agg[k]["costUSD"] += _safe_float(r.get("costUSD"))
            agg[k]["totalTokens"] += _safe_float(r.get("totalTokens"))
            agg[k]["count"] += 1.0
        ranked = sorted(agg.items(), key=lambda kv: kv[1]["costUSD"], reverse=True)[:top_n]
        return [
            {
                "key": k,
                "costUSD": v["costUSD"],
                "totalTokens": v["totalTokens"],
                "count": int(v["count"]),
                "sharePct": (v["costUSD"] / total_cost * 100.0) if total_cost > 0 else 0.0,
            }
            for k, v in ranked
        ]

    dimensions = {
        "project": _aggregate("project"),
        "user": _aggregate("user"),
        "department": _aggregate("department"),
        "application": _aggregate("application"),
        "businessLine": _aggregate("businessLine"),
    }

    mode = (granularity or "standard").lower()
    if mode in {"detailed", "fine", "fine_grained"}:
        dimensions["model"] = _aggregate("model")
        dimensions["task"] = _aggregate("task")
        dimensions["workflow"] = _aggregate("workflow")
        dimensions["session"] = _aggregate("session")

    cloud = cloud_rows or []
    if cloud:
        cloud_total = sum(_safe_float(x.get("costUSD")) for x in cloud)

        def _rank_cloud(agg: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
            return [
                {
                    "key": k,
                    "costUSD": v["costUSD"],
                    "totalTokens": 0.0,
                    "count": int(v["count"]),
                    "sharePct": (v["costUSD"] / cloud_total * 100.0) if cloud_total > 0 else 0.0,
                }
                for k, v in sorted(agg.items(), key=lambda kv: kv[1]["costUSD"], reverse=True)[:top_n]
            ]

        cloud_agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"costUSD": 0.0, "count": 0.0})
        cloud_service_agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"costUSD": 0.0, "count": 0.0})
        cloud_tag_agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"costUSD": 0.0, "count": 0.0})
        cloud_project_agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"costUSD": 0.0, "count": 0.0})
        cloud_source_agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"costUSD": 0.0, "count": 0.0})
        cloud_env_agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"costUSD": 0.0, "count": 0.0})
        for r in cloud:
            cost = _safe_float(r.get("costUSD"))
            p = str(r.get("provider") or "unknown").lower()
            s = str(r.get("service") or "all")
            cloud_agg[p]["costUSD"] += cost
            cloud_agg[p]["count"] += 1
            key = f"{p}:{s}"
            cloud_service_agg[key]["costUSD"] += cost
            cloud_service_agg[key]["count"] += 1
            cproj = str(r.get("project") or "unknown")
            cloud_project_agg[cproj]["costUSD"] += cost
            cloud_project_agg[cproj]["count"] += 1
            csrc = str(r.get("source") or "unknown")
            cloud_source_agg[csrc]["costUSD"] += cost
            cloud_source_agg[csrc]["count"] += 1
            if r.get("environment"):
                env = str(r.get("environment"))
                cloud_env_agg[env]["costUSD"] += cost
                cloud_env_agg[env]["count"] += 1
            tags = _normalize_cloud_tags(r.get("tags"))
            for tk, tv in tags.items():
                tkey = f"{tk}={tv}"
                cloud_tag_agg[tkey]["costUSD"] += cost
                cloud_tag_agg[tkey]["count"] += 1

        dimensions["cloudProvider"] = _rank_cloud(cloud_agg)
        dimensions["cloudService"] = _rank_cloud(cloud_service_agg)
        dimensions["cloudTag"] = _rank_cloud(cloud_tag_agg)
        dimensions["cloudProject"] = _rank_cloud(cloud_project_agg)
        dimensions["cloudSource"] = _rank_cloud(cloud_source_agg)
        if cloud_env_agg:
            dimensions["cloudEnvironment"] = _rank_cloud(cloud_env_agg)

    return {
        "available": True,
        "granularity": mode,
        "totalAttributedCostUSD": total_cost,
        "unallocatedCostUSD": max(0.0, day_total - total_cost),
        "dimensions": dimensions,
    }


def _prompt_template_signature(prompt: str) -> str:
    tokens = _tokenize_prompt_anonymized(prompt)
    if not tokens:
        return "empty_prompt"
    return " ".join(tokens[:24])


def _prompt_suggestion_actions(sample_prompt_tokens: float, sample_completion_tokens: float, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    cfg = config if isinstance(config, dict) else {}
    compression_threshold = _safe_float(cfg.get("compressionThresholdPromptTokens"), default=700.0)
    context_refactor_ratio = _safe_float(cfg.get("contextRefactorRatio"), default=1.5)
    target_reduction_pct = _safe_float(cfg.get("targetPromptTokenReductionPct"), default=20.0)
    expected_cost_reduction_pct = _safe_float(cfg.get("expectedCostReductionPct"), default=10.0)
    ab_cfg = cfg.get("abTesting") if isinstance(cfg.get("abTesting"), dict) else {}
    variant_b_ratio = _safe_float(ab_cfg.get("trafficSplitB"), default=0.2)
    variant_b_ratio = min(0.9, max(0.05, variant_b_ratio))

    actions: List[Dict[str, Any]] = []
    if sample_prompt_tokens >= compression_threshold:
        actions.append(
            {
                "type": "compression",
                "title": "Compress long instructions and remove repeated context",
                "expectedPromptTokenReductionPct": target_reduction_pct,
                "actions": [
                    "Extract stable policy text into a reusable system preset.",
                    "Summarize long conversation history before sending to model.",
                ],
            }
        )
    if sample_prompt_tokens >= max(1.0, sample_completion_tokens) * context_refactor_ratio:
        actions.append(
            {
                "type": "context_refactor",
                "title": "Move verbose context into retrieval/cache",
                "expectedPromptTokenReductionPct": max(8.0, target_reduction_pct * 0.75),
                "actions": [
                    "Replace full inline docs with retrieval IDs or snippets.",
                    "Use semantic cache for repeated context blocks.",
                ],
            }
        )
    actions.append(
        {
            "type": "model_rightsizing",
            "title": "A/B test lower-cost model on this prompt family",
            "expectedCostReductionPct": expected_cost_reduction_pct,
            "actions": [
                f"Route {int(variant_b_ratio * 100)}% traffic to candidate model and track quality score.",
                "Promote candidate only if quality drop <= 2%.",
            ],
        }
    )
    return actions


def build_prompt_optimization_engine(
    rows: List[Dict[str, Any]],
    pattern_analysis: Dict[str, Any],
    *,
    normalized_records: Optional[List[Dict[str, Any]]] = None,
    max_prompt_families: int = 8,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = config if isinstance(config, dict) else {}
    family_limit = int(cfg.get("maxPromptFamilies") or max_prompt_families or 8)
    family_limit = max(1, family_limit)
    ab_cfg = cfg.get("abTesting") if isinstance(cfg.get("abTesting"), dict) else {}
    traffic_split_b = min(0.9, max(0.05, _safe_float(ab_cfg.get("trafficSplitB"), default=0.5)))
    criteria = {
        "costReductionPctMin": _safe_float(ab_cfg.get("costReductionPctMin"), default=10.0),
        "qualityDropPctMax": _safe_float(ab_cfg.get("qualityDropPctMax"), default=2.0),
        "latencyIncreasePctMax": _safe_float(ab_cfg.get("latencyIncreasePctMax"), default=10.0),
    }

    records = normalized_records if normalized_records is not None else _normalize_call_records(rows)
    if not records:
        return {"available": False, "highConsumptionPrompts": [], "abTests": [], "config": {"maxPromptFamilies": family_limit, "abTesting": criteria}}

    families: Dict[str, Dict[str, Any]] = {}
    for r in records:
        prompt = str(r.get("prompt") or "")
        sig = _prompt_template_signature(prompt)
        node = families.setdefault(
            sig,
            {
                "templateSignature": sig,
                "samplePrompt": prompt[:240],
                "calls": 0,
                "totalCostUSD": 0.0,
                "totalPromptTokens": 0.0,
                "totalCompletionTokens": 0.0,
                "models": defaultdict(int),
                "projects": defaultdict(int),
            },
        )
        node["calls"] += 1
        node["totalCostUSD"] += _safe_float(r.get("costUSD"))
        node["totalPromptTokens"] += _safe_float(r.get("promptTokens"))
        node["totalCompletionTokens"] += _safe_float(r.get("completionTokens"))
        node["models"][str(r.get("model") or "unknown")] += 1
        node["projects"][str(r.get("project") or "unknown")] += 1

    ranked = sorted(
        families.values(),
        key=lambda x: (x["totalCostUSD"], x["totalPromptTokens"], x["calls"]),
        reverse=True,
    )[:family_limit]

    by_model = pattern_analysis.get("dimensions", {}).get("byModel", []) if isinstance(pattern_analysis, dict) else []
    cheap_model = None
    if isinstance(by_model, list) and by_model:
        by_cost_per_token = sorted(
            [x for x in by_model if _safe_float(x.get("totalTokens")) > 0],
            key=lambda x: (_safe_float(x.get("costUSD")) / max(1.0, _safe_float(x.get("totalTokens")))),
        )
        if by_cost_per_token:
            cheap_model = str(by_cost_per_token[0].get("key") or "")

    prompt_families: List[Dict[str, Any]] = []
    ab_tests: List[Dict[str, Any]] = []
    for idx, item in enumerate(ranked, start=1):
        calls = int(item["calls"])
        avg_prompt_tokens = item["totalPromptTokens"] / max(1, calls)
        avg_completion_tokens = item["totalCompletionTokens"] / max(1, calls)
        top_model = sorted(item["models"].items(), key=lambda kv: kv[1], reverse=True)[0][0] if item["models"] else "unknown"
        top_project = sorted(item["projects"].items(), key=lambda kv: kv[1], reverse=True)[0][0] if item["projects"] else "unknown"
        suggestions = _prompt_suggestion_actions(avg_prompt_tokens, avg_completion_tokens, config=cfg)
        prompt_families.append(
            {
                "rank": idx,
                "templateSignature": item["templateSignature"],
                "samplePrompt": item["samplePrompt"],
                "calls": calls,
                "totalCostUSD": item["totalCostUSD"],
                "avgPromptTokens": avg_prompt_tokens,
                "avgCompletionTokens": avg_completion_tokens,
                "promptToCompletionRatio": (avg_prompt_tokens / avg_completion_tokens) if avg_completion_tokens > 0 else None,
                "topModel": top_model,
                "topProject": top_project,
                "suggestions": suggestions,
            }
        )

        variant_b = {
            "name": "B-compressed",
            "strategy": "compress_prompt",
            "targetPromptTokenReductionPct": 20.0 if avg_prompt_tokens >= 700 else 10.0,
        }
        if cheap_model and cheap_model != top_model:
            variant_b["candidateModel"] = cheap_model

        ab_tests.append(
            {
                "testId": f"prompt-ab-{idx:02d}",
                "scope": item["templateSignature"],
                "goal": "reduce_cost_with_quality_guardrail",
                "trafficSplit": {"A": round(1.0 - traffic_split_b, 3), "B": round(traffic_split_b, 3)},
                "variants": [
                    {"name": "A-control", "strategy": "status_quo", "model": top_model},
                    variant_b,
                ],
                "successCriteria": criteria,
                "metrics": ["avgCostPerCallUSD", "qualityScore", "avgLatencyMs", "promptTokens"],
            }
        )

    return {
        "available": True,
        "engineVersion": "1.0",
        "spec": {
            "recommendationTypes": ["compression", "context_refactor", "model_rightsizing"],
            "ranking": "totalCostUSD,totalPromptTokens,calls",
        },
        "config": {
            "maxPromptFamilies": family_limit,
            "abTesting": criteria,
        },
        "highConsumptionPrompts": prompt_families,
        "abTests": ab_tests,
    }


def build_optimization_recommendations(
    rows: List[Dict[str, Any]],
    pattern_analysis: Dict[str, Any],
    attribution: Dict[str, Any],
    max_items: int = 6,
    normalized_records: Optional[List[Dict[str, Any]]] = None,
    cloud_cost_view: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    records = normalized_records if normalized_records is not None else _normalize_call_records(rows)
    cloud_enabled = bool(isinstance(cloud_cost_view, dict) and cloud_cost_view.get("cloudIntegrationEnabled"))
    hooks = {
        "cloudCostManagement": {
            "awsCostExplorer": {"supported": True, "status": "connected" if cloud_enabled else "ready"},
            "gcpBilling": {"supported": True, "status": "connected" if cloud_enabled else "ready"},
        }
    }
    if not records:
        return {"available": False, "recommendations": [], "integrationHooks": hooks}

    recs: List[Dict[str, Any]] = []

    efficiency = pattern_analysis.get("efficiency") if isinstance(pattern_analysis.get("efficiency"), list) else []
    priced = [x for x in efficiency if isinstance(x.get("costPer1kTokensUSD"), (int, float)) and _safe_float(x.get("totalTokens")) > 0]
    priced_sorted = sorted(priced, key=lambda x: _safe_float(x.get("costPer1kTokensUSD")))
    if len(priced_sorted) >= 2:
        cheapest = priced_sorted[0]
        expensive = priced_sorted[-1]
        expensive_cpk = _safe_float(expensive.get("costPer1kTokensUSD"))
        cheapest_cpk = _safe_float(cheapest.get("costPer1kTokensUSD"))
        if cheapest_cpk > 0 and expensive_cpk >= cheapest_cpk * 1.5:
            recs.append(
                {
                    "type": "model_rightsizing",
                    "priority": "high",
                    "title": f"Evaluate replacing costly model '{expensive.get('model')}' for non-critical traffic.",
                    "rationale": f"Cost/1K tokens = {expensive_cpk:.4f} vs {cheapest_cpk:.4f} on '{cheapest.get('model')}'.",
                    "estimatedSavingsPct": min(50.0, max(8.0, (1 - cheapest_cpk / expensive_cpk) * 100.0)),
                    "actions": [
                        "Route low-risk workflows to lower-cost model via policy/router.",
                        "Run A/B quality tests before full migration.",
                    ],
                }
            )

    p = pattern_analysis.get("promptTokens") if isinstance(pattern_analysis.get("promptTokens"), dict) else {}
    p95_prompt = _safe_float(p.get("p95"))
    p50_prompt = _safe_float(p.get("p50"))
    if p50_prompt > 0 and p95_prompt >= p50_prompt * 2.0:
        recs.append(
            {
                "type": "prompt_optimization",
                "priority": "medium",
                "title": "Prompt length variance is high; standardize prompt templates.",
                "rationale": f"Prompt p95={p95_prompt:.0f} tokens, p50={p50_prompt:.0f} tokens.",
                "estimatedSavingsPct": 10.0,
                "actions": [
                    "Add prompt linting and max-token guards.",
                    "Move verbose context to retrieval/cache instead of inline prompt.",
                ],
            }
        )

    tiny_calls = [r for r in records if _safe_float(r.get("totalTokens")) <= 300]
    if len(tiny_calls) >= max(20, int(len(records) * 0.25)):
        recs.append(
            {
                "type": "batching",
                "priority": "medium",
                "title": "High volume of tiny calls detected; batch requests where possible.",
                "rationale": f"{len(tiny_calls)}/{len(records)} calls are <=300 tokens.",
                "estimatedSavingsPct": 6.0,
                "actions": [
                    "Merge adjacent short prompts into batched calls.",
                    "Enable response caching for repeated prompts.",
                ],
            }
        )

    project_top = attribution.get("dimensions", {}).get("project", []) if isinstance(attribution.get("dimensions"), dict) else []
    if project_top and _safe_float(project_top[0].get("sharePct")) >= 40.0:
        recs.append(
            {
                "type": "budget_guardrail",
                "priority": "high",
                "title": f"Top project '{project_top[0].get('key')}' dominates spend; enforce budget guardrails.",
                "rationale": f"Project share = {_safe_float(project_top[0].get('sharePct')):.1f}% of attributed cost.",
                "estimatedSavingsPct": 12.0,
                "actions": [
                    "Set project-level budget threshold alerts.",
                    "Require approval for high-cost model usage in this project.",
                ],
            }
        )

    return {
        "available": True,
        "recommendations": recs[:max_items],
        "integrationHooks": hooks,
    }


def _load_rbac_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    raw = Path(path).read_text(encoding="utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError("RBAC config must be a JSON object.")
    return parsed


def _load_alert_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    raw = Path(path).read_text(encoding="utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError("Alert config must be a JSON object.")
    return parsed


def _load_cost_control_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    raw = Path(path).read_text(encoding="utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError("Cost control config must be a JSON object.")
    return parsed


def _normalize_budget_config(parsed: Dict[str, Any]) -> Dict[str, Any]:
    allowed_dimensions = {"project", "department", "user", "application", "businessLine", "model"}
    allowed_actions = {"warn", "degrade", "switch_model", "stop_calls"}

    allocations: List[Dict[str, Any]] = []
    for idx, item in enumerate(parsed.get("allocations") if isinstance(parsed.get("allocations"), list) else [], start=1):
        if not isinstance(item, dict):
            continue
        dimension = str(item.get("dimension") or "project")
        if dimension not in allowed_dimensions:
            continue
        key = str(item.get("key") or "unknown")
        budget = _safe_float(item.get("budgetUSD"), default=0.0)
        if budget <= 0:
            continue
        allocations.append({
            "id": str(item.get("id") or f"alloc-{idx}"),
            "dimension": dimension,
            "key": key,
            "budgetUSD": budget,
        })

    permissions_in = parsed.get("permissions") if isinstance(parsed.get("permissions"), dict) else {}
    default_role = str(permissions_in.get("defaultRole") or "viewer")

    roles: Dict[str, Dict[str, Any]] = {}
    for role, policy in (permissions_in.get("roles") if isinstance(permissions_in.get("roles"), dict) else {}).items():
        if not isinstance(policy, dict):
            continue
        node: Dict[str, Any] = {}
        allowed_models = policy.get("allowedModels") if isinstance(policy.get("allowedModels"), list) else None
        if isinstance(allowed_models, list):
            node["allowedModels"] = sorted({str(x) for x in allowed_models if str(x).strip()})
        if isinstance(policy.get("maxCostPerCallUSD"), (int, float)):
            node["maxCostPerCallUSD"] = max(0.0, float(policy.get("maxCostPerCallUSD")))
        roles[str(role)] = node

    users: Dict[str, Dict[str, Any]] = {}
    for user, policy in (permissions_in.get("users") if isinstance(permissions_in.get("users"), dict) else {}).items():
        if not isinstance(policy, dict):
            continue
        node: Dict[str, Any] = {}
        if policy.get("role") is not None:
            node["role"] = str(policy.get("role"))
        allowed_models = policy.get("allowedModels") if isinstance(policy.get("allowedModels"), list) else None
        if isinstance(allowed_models, list):
            node["allowedModels"] = sorted({str(x) for x in allowed_models if str(x).strip()})
        if isinstance(policy.get("maxCostPerCallUSD"), (int, float)):
            node["maxCostPerCallUSD"] = max(0.0, float(policy.get("maxCostPerCallUSD")))
        users[str(user)] = node

    overage_policies: List[Dict[str, Any]] = []
    for item in parsed.get("overagePolicies") if isinstance(parsed.get("overagePolicies"), list) else []:
        if not isinstance(item, dict):
            continue
        threshold = max(0.0, _safe_float(item.get("thresholdPct"), default=100.0))
        action = str(item.get("action") or "warn")
        if action not in allowed_actions:
            action = "warn"
        overage_policies.append({
            "thresholdPct": threshold,
            "action": action,
            "routeToModel": item.get("routeToModel"),
            "message": str(item.get("message") or "").strip(),
        })
    overage_policies.sort(key=lambda x: _safe_float(x.get("thresholdPct")))

    return {
        "allocations": allocations,
        "permissions": {
            "defaultRole": default_role,
            "roles": roles,
            "users": users,
        },
        "overagePolicies": overage_policies,
    }


def _load_budget_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    raw = Path(path).read_text(encoding="utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError("Budget config must be a JSON object.")
    return _normalize_budget_config(parsed)


def _normalize_prompt_optimization_config(parsed: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if parsed.get("maxPromptFamilies") is not None:
        out["maxPromptFamilies"] = max(1, int(_safe_float(parsed.get("maxPromptFamilies"), default=8.0)))

    for key in ("compressionThresholdPromptTokens", "contextRefactorRatio", "targetPromptTokenReductionPct", "expectedCostReductionPct"):
        if isinstance(parsed.get(key), (int, float)):
            out[key] = float(parsed.get(key))

    ab_in = parsed.get("abTesting") if isinstance(parsed.get("abTesting"), dict) else {}
    if ab_in:
        ab_out: Dict[str, Any] = {}
        if isinstance(ab_in.get("trafficSplitB"), (int, float)):
            ab_out["trafficSplitB"] = min(0.9, max(0.05, float(ab_in.get("trafficSplitB"))))
        for key in ("costReductionPctMin", "qualityDropPctMax", "latencyIncreasePctMax"):
            if isinstance(ab_in.get(key), (int, float)):
                ab_out[key] = float(ab_in.get(key))
        out["abTesting"] = ab_out

    return out


def _load_prompt_optimization_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    raw = Path(path).read_text(encoding="utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError("Prompt optimization config must be a JSON object.")
    return _normalize_prompt_optimization_config(parsed)


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
    if role and (allow_role_override or not user):
        selected_role = role
    elif isinstance(user_record.get("role"), str):
        selected_role = str(user_record.get("role"))
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


def forecast_cost(rows: List[Dict[str, Any]], horizon_days: int = 7, lookback_days: int = 14) -> Dict[str, Any]:
    daily = [day_total_cost(r) for r in rows]
    if not daily:
        return {"horizonDays": horizon_days, "predictedTotalCostUSD": 0.0, "method": "moving_average"}

    window = daily[-max(1, lookback_days):]
    avg = sum(window) / len(window)
    trend = 0.0
    if len(window) >= 2:
        trend = (window[-1] - window[0]) / float(len(window) - 1)

    predicted_days: List[float] = []
    for i in range(1, horizon_days + 1):
        val = max(0.0, avg + trend * i)
        predicted_days.append(val)

    return {
        "horizonDays": horizon_days,
        "predictedTotalCostUSD": sum(predicted_days),
        "predictedAvgDailyCostUSD": (sum(predicted_days) / horizon_days) if horizon_days > 0 else 0.0,
        "baselineAvgDailyCostUSD": avg,
        "dailyTrendUSD": trend,
        "method": "moving_average_plus_linear_trend",
    }


def detect_cost_anomalies(rows: List[Dict[str, Any]], lookback_days: int = 7, z_threshold: float = 2.5) -> List[Dict[str, Any]]:
    daily = [day_total_cost(r) for r in rows]
    out: List[Dict[str, Any]] = []
    for i in range(len(daily)):
        if i < lookback_days:
            continue
        w = daily[i - lookback_days:i]
        if not w:
            continue
        mean = sum(w) / len(w)
        var = sum((x - mean) ** 2 for x in w) / len(w)
        std = var ** 0.5
        curr = daily[i]
        z = ((curr - mean) / std) if std > 1e-12 else 0.0
        if z >= z_threshold:
            out.append({
                "date": rows[i].get("date"),
                "costUSD": curr,
                "baselineMeanUSD": mean,
                "zScore": z,
                "severity": "high" if z >= (z_threshold + 1.0) else "medium",
            })
    return out


def evaluate_alert_rules(
    rows: List[Dict[str, Any]],
    forecast_7d: Dict[str, Any],
    anomalies: List[Dict[str, Any]],
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = config or {}
    rules = cfg.get("rules") if isinstance(cfg.get("rules"), dict) else {}
    channels = cfg.get("notificationChannels") if isinstance(cfg.get("notificationChannels"), list) else []

    budget_threshold = _safe_float(rules.get("budgetThresholdUSD"), default=0.0)
    budget_forecast_pct = _safe_float(rules.get("budgetForecastPct"), default=100.0)
    anomaly_count_threshold = _safe_int(rules.get("anomalyCountThreshold", 1), default=1, minimum=1)

    triggered: List[Dict[str, Any]] = []
    if budget_threshold > 0:
        predicted = _safe_float(forecast_7d.get("predictedTotalCostUSD"))
        ratio = (predicted / budget_threshold * 100.0) if budget_threshold > 0 else 0.0
        if ratio >= budget_forecast_pct:
            triggered.append({
                "rule": "budget_forecast_threshold",
                "severity": "high" if ratio >= 120 else "medium",
                "message": f"7-day forecast {predicted:.2f} USD reached {ratio:.1f}% of budget ({budget_threshold:.2f} USD)",
            })

    if len(anomalies) >= anomaly_count_threshold:
        last = anomalies[-1]
        triggered.append({
            "rule": "anomaly_count_threshold",
            "severity": "high" if len(anomalies) >= (anomaly_count_threshold + 2) else "medium",
            "message": f"Detected {len(anomalies)} anomalies in recent window. Latest: {last.get('date')} z={_safe_float(last.get('zScore', 0.0)):.2f}",
        })

    return {
        "rules": rules,
        "notificationChannels": [str(x) for x in channels],
        "triggered": triggered,
    }




def evaluate_unified_budget_alerts(
    rows: List[Dict[str, Any]],
    cloud_rows: Optional[List[Dict[str, Any]]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = config if isinstance(config, dict) else {}
    rules = cfg.get("unifiedBudgetAlerts") if isinstance(cfg.get("unifiedBudgetAlerts"), list) else []
    cloud = cloud_rows or []

    llm_total = sum(day_total_cost(r) for r in rows)
    cloud_total = sum(_safe_float(r.get("costUSD")) for r in cloud)
    unified_total = llm_total + cloud_total

    provider_totals: Dict[str, float] = defaultdict(float)
    service_totals: Dict[str, float] = defaultdict(float)
    tag_totals: Dict[str, float] = defaultdict(float)
    for r in cloud:
        pvd = str(r.get("provider") or "unknown").lower()
        svc = str(r.get("service") or "all")
        c = _safe_float(r.get("costUSD"))
        provider_totals[pvd] += c
        service_totals[f"{pvd}:{svc}"] += c
        for tk, tv in _normalize_cloud_tags(r.get("tags")).items():
            tag_totals[f"{tk}={tv}"] += c

    events: List[Dict[str, Any]] = []
    for idx, rule in enumerate(rules, start=1):
        if not isinstance(rule, dict):
            continue
        scope = str(rule.get("scope") or "total").lower()
        threshold = _safe_float(rule.get("thresholdUSD"), default=0.0)
        if threshold <= 0:
            continue

        value = 0.0
        scope_label = scope
        if scope == "total":
            value = unified_total
        elif scope == "llm":
            value = llm_total
        elif scope == "cloud":
            value = cloud_total
        elif scope == "provider":
            provider = str(rule.get("provider") or "").lower()
            value = _safe_float(provider_totals.get(provider, 0.0))
            scope_label = f"provider:{provider or 'unknown'}"
        elif scope == "service":
            provider = str(rule.get("provider") or "").lower()
            service = str(rule.get("service") or "all")
            key = f"{provider}:{service}"
            value = _safe_float(service_totals.get(key, 0.0))
            scope_label = f"service:{key}"
        elif scope == "tag":
            tag_key = str(rule.get("tagKey") or "").strip().lower()
            tag_val = str(rule.get("tagValue") or "").strip()
            key = f"{tag_key}={tag_val}"
            value = _safe_float(tag_totals.get(key, 0.0))
            scope_label = f"tag:{key}"
        else:
            continue

        if value >= threshold:
            events.append({
                "id": str(rule.get("id") or f"unified-budget-{idx}"),
                "scope": scope_label,
                "valueUSD": value,
                "thresholdUSD": threshold,
                "usagePct": (value / threshold * 100.0) if threshold > 0 else 0.0,
                "severity": str(rule.get("severity") or ("high" if value >= threshold * 1.2 else "medium")),
                "message": str(rule.get("message") or f"{scope_label} reached {value:.2f} USD ({value/threshold*100:.1f}% of budget)"),
            })

    events.sort(key=lambda x: (_safe_float(x.get("usagePct")), _safe_float(x.get("valueUSD"))), reverse=True)
    by_severity: Dict[str, int] = defaultdict(int)
    by_scope: Dict[str, int] = defaultdict(int)
    for e in events:
        by_severity[str(e.get("severity") or "unknown").lower()] += 1
        by_scope[str(e.get("scope") or "unknown")] += 1

    return {
        "available": bool(rules),
        "totals": {
            "llmCostUSD": llm_total,
            "cloudInfraCostUSD": cloud_total,
            "totalUnifiedCostUSD": unified_total,
        },
        "rules": rules,
        "summary": {
            "triggered": len(events),
            "bySeverity": dict(by_severity),
            "byScope": dict(by_scope),
        },
        "events": events,
    }
def evaluate_realtime_cost_controls(
    rows: List[Dict[str, Any]],
    forecast_7d: Dict[str, Any],
    anomalies: List[Dict[str, Any]],
    config: Optional[Dict[str, Any]] = None,
    normalized_records: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    cfg = config or {}
    layers = cfg.get("layers") if isinstance(cfg.get("layers"), list) else []
    if not layers:
        return {"available": False, "layers": [], "triggeredActions": []}

    records = normalized_records if normalized_records is not None else _normalize_call_records(rows)
    total_cost = sum(day_total_cost(r) for r in rows)
    anomaly_count = len(anomalies)

    dim_cost: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in records:
        c = _safe_float(r.get("costUSD"))
        for dim in ("project", "department", "user", "application", "businessLine", "model"):
            dim_cost[dim][str(r.get(dim) or "unknown")] += c

    evaluated_layers: List[Dict[str, Any]] = []
    actions: List[Dict[str, Any]] = []

    for idx, layer in enumerate(layers, start=1):
        if not isinstance(layer, dict):
            continue
        layer_id = str(layer.get("id") or f"layer-{idx}")
        metric = str(layer.get("metric") or "forecast_7d_total_cost")
        action = str(layer.get("action") or "degrade")
        threshold = _safe_float(layer.get("threshold"), default=0.0)
        if threshold <= 0:
            evaluated_layers.append({"id": layer_id, "triggered": False, "reason": "invalid_threshold"})
            continue

        value = 0.0
        scope_label = "global"
        if metric == "forecast_7d_total_cost":
            value = _safe_float(forecast_7d.get("predictedTotalCostUSD"))
        elif metric == "actual_total_cost":
            value = total_cost
        elif metric == "anomaly_count":
            value = float(anomaly_count)
        elif metric == "dimension_cost":
            dimension = str(layer.get("dimension") or "project")
            key = str(layer.get("key") or "")
            if key:
                value = _safe_float(dim_cost.get(dimension, {}).get(key, 0.0))
                scope_label = f"{dimension}:{key}"
            else:
                bucket = dim_cost.get(dimension, {})
                if bucket:
                    top_key, top_value = sorted(bucket.items(), key=lambda kv: kv[1], reverse=True)[0]
                    value = _safe_float(top_value)
                    scope_label = f"{dimension}:{top_key}"
        else:
            evaluated_layers.append({"id": layer_id, "triggered": False, "reason": f"unsupported_metric:{metric}"})
            continue

        triggered = value >= threshold
        layer_result = {
            "id": layer_id,
            "metric": metric,
            "value": value,
            "threshold": threshold,
            "triggered": triggered,
            "scope": scope_label,
            "action": action,
        }
        evaluated_layers.append(layer_result)
        if triggered:
            actions.append(
                {
                    "layerId": layer_id,
                    "scope": scope_label,
                    "action": action,
                    "message": str(layer.get("message") or f"{action} triggered on {scope_label}: {value:.2f} >= {threshold:.2f}"),
                    "routeToModel": layer.get("routeToModel"),
                    "stopReason": layer.get("stopReason"),
                }
            )

    return {
        "available": True,
        "layers": evaluated_layers,
        "triggeredActions": actions,
    }


def evaluate_budget_allocation_and_permissions(
    rows: List[Dict[str, Any]],
    config: Optional[Dict[str, Any]] = None,
    normalized_records: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    cfg = config or {}
    allocations_cfg = cfg.get("allocations") if isinstance(cfg.get("allocations"), list) else []
    permission_cfg = cfg.get("permissions") if isinstance(cfg.get("permissions"), dict) else {}
    overage_cfg = cfg.get("overagePolicies") if isinstance(cfg.get("overagePolicies"), list) else []

    records = normalized_records if normalized_records is not None else _normalize_call_records(rows)
    dim_cost: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in records:
        c = _safe_float(r.get("costUSD"))
        for dim in ("project", "department", "user", "application", "businessLine", "model"):
            dim_cost[dim][str(r.get(dim) or "unknown")] += c

    allocations: List[Dict[str, Any]] = []
    for idx, item in enumerate(allocations_cfg, start=1):
        if not isinstance(item, dict):
            continue
        dimension = str(item.get("dimension") or "project")
        key = str(item.get("key") or "unknown")
        budget = _safe_float(item.get("budgetUSD"), default=0.0)
        if budget <= 0:
            continue
        actual = _safe_float(dim_cost.get(dimension, {}).get(key, 0.0))
        allocations.append({
            "id": str(item.get("id") or f"alloc-{idx}"),
            "dimension": dimension,
            "key": key,
            "budgetUSD": budget,
            "actualCostUSD": actual,
            "usagePct": (actual / budget * 100.0) if budget > 0 else 0.0,
            "remainingUSD": budget - actual,
            "status": "over" if actual > budget else ("warning" if actual >= budget * 0.8 else "healthy"),
        })

    role_permissions = permission_cfg.get("roles") if isinstance(permission_cfg.get("roles"), dict) else {}
    user_permissions = permission_cfg.get("users") if isinstance(permission_cfg.get("users"), dict) else {}
    default_role = str(permission_cfg.get("defaultRole") or "viewer")

    violations: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for r in records:
        user = str(r.get("user") or "unknown")
        model = str(r.get("model") or "unknown")
        cost = _safe_float(r.get("costUSD"))

        raw_user_policy = user_permissions.get(user) if isinstance(user_permissions.get(user), dict) else {}
        assigned_role = str(raw_user_policy.get("role") or default_role)
        role_policy = role_permissions.get(assigned_role) if isinstance(role_permissions.get(assigned_role), dict) else {}

        allowed_models = raw_user_policy.get("allowedModels") if isinstance(raw_user_policy.get("allowedModels"), list) else role_policy.get("allowedModels")
        max_cost = raw_user_policy.get("maxCostPerCallUSD") if isinstance(raw_user_policy.get("maxCostPerCallUSD"), (int, float)) else role_policy.get("maxCostPerCallUSD")

        if isinstance(allowed_models, list) and allowed_models and model not in {str(x) for x in allowed_models}:
            key = (user, model, "model_not_allowed")
            node = violations.setdefault(key, {"user": user, "role": assigned_role, "model": model, "violation": "model_not_allowed", "calls": 0, "costUSD": 0.0, "message": f"Model '{model}' not allowed for role '{assigned_role}'"})
            node["calls"] += 1
            node["costUSD"] += cost

        if isinstance(max_cost, (int, float)) and cost > float(max_cost):
            key = (user, model, "call_cost_exceeded")
            node = violations.setdefault(key, {"user": user, "role": assigned_role, "model": model, "violation": "call_cost_exceeded", "calls": 0, "costUSD": 0.0, "message": f"Per-call cost {cost:.4f} > max {float(max_cost):.4f}"})
            node["calls"] += 1
            node["costUSD"] += cost

    violation_list = sorted(violations.values(), key=lambda x: (x["costUSD"], x["calls"]), reverse=True)

    return {
        "available": bool(allocations or role_permissions or user_permissions or overage_cfg),
        "allocations": allocations,
        "permissions": {
            "defaultRole": default_role,
            "roles": role_permissions,
            "users": user_permissions,
            "violations": violation_list,
        },
        "overagePolicies": overage_cfg,
    }


def evaluate_overage_behaviors(
    budget_eval: Dict[str, Any],
) -> Dict[str, Any]:
    allocations = budget_eval.get("allocations") if isinstance(budget_eval.get("allocations"), list) else []
    policies = budget_eval.get("overagePolicies") if isinstance(budget_eval.get("overagePolicies"), list) else []
    if not allocations and not policies:
        return {"available": False, "events": []}

    events: List[Dict[str, Any]] = []
    for alloc in allocations:
        if not isinstance(alloc, dict):
            continue
        usage_pct = _safe_float(alloc.get("usagePct"))
        if usage_pct <= 100.0:
            continue

        matched = None
        for p in policies:
            if not isinstance(p, dict):
                continue
            threshold = _safe_float(p.get("thresholdPct"), default=100.0)
            if usage_pct >= threshold and (matched is None or threshold > _safe_float(matched.get("thresholdPct"), default=100.0)):
                matched = p

        action = str((matched or {}).get("action") or "warn")
        events.append({
            "allocationId": str(alloc.get("id") or ""),
            "dimension": str(alloc.get("dimension") or ""),
            "key": str(alloc.get("key") or ""),
            "usagePct": usage_pct,
            "action": action,
            "message": str((matched or {}).get("message") or f"{alloc.get('dimension')}:{alloc.get('key')} exceeded budget at {usage_pct:.1f}%"),
            "routeToModel": (matched or {}).get("routeToModel"),
            "autoHandled": action in {"degrade", "switch_model", "stop_calls"},
        })

    return {
        "available": True,
        "events": events,
    }


def build_quota_policies(
    budget_eval: Dict[str, Any],
    overage_eval: Dict[str, Any],
) -> Dict[str, Any]:
    allocations = budget_eval.get("allocations") if isinstance(budget_eval.get("allocations"), list) else []
    permissions = budget_eval.get("permissions") if isinstance(budget_eval.get("permissions"), dict) else {}
    overage_policies = budget_eval.get("overagePolicies") if isinstance(budget_eval.get("overagePolicies"), list) else []
    overage_events = overage_eval.get("events") if isinstance(overage_eval.get("events"), list) else []

    enforced = [x for x in overage_events if isinstance(x, dict) and bool(x.get("autoHandled"))]

    allocation_policies: List[Dict[str, Any]] = []
    for item in allocations:
        if not isinstance(item, dict):
            continue
        allocation_policies.append({
            "policyId": str(item.get("id") or ""),
            "scope": f"{item.get('dimension')}:{item.get('key')}",
            "budgetUSD": _safe_float(item.get("budgetUSD")),
            "usedUSD": _safe_float(item.get("actualCostUSD")),
            "remainingUSD": _safe_float(item.get("remainingUSD")),
            "usagePct": _safe_float(item.get("usagePct")),
            "status": str(item.get("status") or "unknown"),
        })

    default_role = str(permissions.get("defaultRole") or "viewer")
    role_count = len(permissions.get("roles", {})) if isinstance(permissions.get("roles"), dict) else 0
    user_override_count = len(permissions.get("users", {})) if isinstance(permissions.get("users"), dict) else 0
    violation_count = len(permissions.get("violations", [])) if isinstance(permissions.get("violations"), list) else 0

    return {
        "available": bool(allocation_policies or overage_policies or role_count or user_override_count),
        "summary": {
            "allocationPolicies": len(allocation_policies),
            "overagePolicies": len(overage_policies),
            "autoHandledEvents": len(enforced),
            "permissionViolations": violation_count,
        },
        "allocations": allocation_policies,
        "permissions": {
            "defaultRole": default_role,
            "roleCount": role_count,
            "userOverrideCount": user_override_count,
            "violationCount": violation_count,
        },
        "overagePolicies": overage_policies,
        "enforcements": enforced,
    }


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
        f"<tr><td>{idx}</td><td>{esc(m)}</td><td>{usd(c)}</td><td>{(c / grand_total * 100 if grand_total else 0):.1f}%</td></tr>"
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


def build_summary(
    provider: str,
    rows: List[Dict[str, Any]],
    spike_lookback_days: int = 7,
    spike_threshold_mult: float = 2.0,
    alert_config: Optional[Dict[str, Any]] = None,
    cost_control_config: Optional[Dict[str, Any]] = None,
    budget_config: Optional[Dict[str, Any]] = None,
    prompt_optimization_config: Optional[Dict[str, Any]] = None,
    cloud_cost_rows: Optional[List[Dict[str, Any]]] = None,
    attribution_granularity: str = "standard",
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
    normalized_records = _normalize_call_records(rows)
    pattern_analysis = build_llm_pattern_analysis(rows, normalized_records=normalized_records)
    attribution = build_cost_attribution(
        rows,
        normalized_records=normalized_records,
        cloud_rows=cloud_cost_rows,
        granularity=attribution_granularity,
    )
    unified_cloud_cost = build_unified_cloud_cost_view(rows, cloud_rows=cloud_cost_rows)
    unified_budget_alerts = evaluate_unified_budget_alerts(rows, cloud_rows=cloud_cost_rows, config=alert_config)
    recommendations = build_optimization_recommendations(
        rows,
        pattern_analysis,
        attribution,
        normalized_records=normalized_records,
        cloud_cost_view=unified_cloud_cost,
    )
    prompt_optimization_engine = build_prompt_optimization_engine(
        rows,
        pattern_analysis,
        normalized_records=normalized_records,
        config=prompt_optimization_config,
    )
    forecast7 = forecast_cost(rows, horizon_days=7, lookback_days=14)
    forecast30 = forecast_cost(rows, horizon_days=30, lookback_days=30)
    anomalies = detect_cost_anomalies(rows, lookback_days=spike_lookback_days)
    alerts = evaluate_alert_rules(rows, forecast7, anomalies, config=alert_config)
    cost_controls = evaluate_realtime_cost_controls(
        rows,
        forecast7,
        anomalies,
        config=cost_control_config,
        normalized_records=normalized_records,
    )
    budget_eval = evaluate_budget_allocation_and_permissions(rows, config=budget_config, normalized_records=normalized_records)
    overage = evaluate_overage_behaviors(budget_eval)
    quota_policies = build_quota_policies(budget_eval, overage)

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
        "forecast": {
            "next7Days": forecast7,
            "next30Days": forecast30,
        },
        "costAnomalies": anomalies,
        "alerts": alerts,
        "realTimeCostControls": cost_controls,
        "llmPatternAnalysis": pattern_analysis,
        "costAttribution": attribution,
        "optimizationRecommendations": recommendations,
        "unifiedCloudCostView": unified_cloud_cost,
        "unifiedBudgetAlerts": unified_budget_alerts,
        "promptOptimizationEngine": prompt_optimization_engine,
        "budgetAllocation": budget_eval,
        "quotaPolicies": quota_policies,
        "overageBehaviors": overage,
    }


def build_unified_cloud_cost_view(rows: List[Dict[str, Any]], cloud_rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    cloud = cloud_rows or []
    llm_daily: Dict[str, float] = defaultdict(float)
    for r in rows:
        d = str(r.get("date") or "")
        if d:
            llm_daily[d] += day_total_cost(r)

    cloud_daily: Dict[str, float] = defaultdict(float)
    cloud_provider_totals: Dict[str, float] = defaultdict(float)
    cloud_service_totals: Dict[str, float] = defaultdict(float)
    cloud_tag_totals: Dict[str, float] = defaultdict(float)
    for r in cloud:
        d = str(r.get("date") or "")
        if not d:
            continue
        c = _safe_float(r.get("costUSD"))
        p = str(r.get("provider") or "unknown").lower()
        s = str(r.get("service") or "all")
        cloud_daily[d] += c
        cloud_provider_totals[p] += c
        cloud_service_totals[f"{p}:{s}"] += c
        for tk, tv in _normalize_cloud_tags(r.get("tags")).items():
            cloud_tag_totals[f"{tk}={tv}"] += c

    all_days = sorted(set(llm_daily.keys()) | set(cloud_daily.keys()))
    daily = [
        {
            "date": d,
            "llmCostUSD": llm_daily.get(d, 0.0),
            "cloudInfraCostUSD": cloud_daily.get(d, 0.0),
            "totalUnifiedCostUSD": llm_daily.get(d, 0.0) + cloud_daily.get(d, 0.0),
        }
        for d in all_days
    ]

    return {
        "available": True,
        "cloudIntegrationEnabled": bool(cloud),
        "totals": {
            "llmCostUSD": sum(llm_daily.values()),
            "cloudInfraCostUSD": sum(cloud_daily.values()),
            "totalUnifiedCostUSD": sum(llm_daily.values()) + sum(cloud_daily.values()),
        },
        "cloudProviders": [
            {"provider": k, "totalCostUSD": v}
            for k, v in sorted(cloud_provider_totals.items(), key=lambda kv: kv[1], reverse=True)
        ],
        "cloudServices": [
            {"providerService": k, "totalCostUSD": v}
            for k, v in sorted(cloud_service_totals.items(), key=lambda kv: kv[1], reverse=True)[:15]
        ],
        "cloudTags": [
            {"tag": k, "totalCostUSD": v}
            for k, v in sorted(cloud_tag_totals.items(), key=lambda kv: kv[1], reverse=True)[:20]
        ],
        "daily": daily,
    }


def build_multi_provider_aggregation(provider_rows: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    if not provider_rows:
        return {"available": False, "providers": [], "daily": [], "totals": {}}

    provider_totals: Dict[str, float] = {}
    provider_models: Dict[str, Dict[str, float]] = {}
    combined_model_totals: Dict[str, float] = defaultdict(float)
    by_provider_model_totals: Dict[str, float] = defaultdict(float)
    daily_map: Dict[str, Dict[str, Any]] = {}

    for provider, rows in provider_rows.items():
        pt = model_totals(rows)
        provider_models[provider] = pt
        provider_totals[provider] = sum(day_total_cost(r) for r in rows)
        for model_name, cost in pt.items():
            combined_model_totals[model_name] += float(cost)
            by_provider_model_totals[f"{provider}:{model_name}"] += float(cost)

        for row in rows:
            day = str(row.get("date") or "")
            if not day:
                continue
            node = daily_map.setdefault(day, {"date": day, "totalCostUSD": 0.0, "providers": {}, "models": defaultdict(float)})
            row_total = day_total_cost(row)
            node["providers"][provider] = node["providers"].get(provider, 0.0) + row_total
            node["totalCostUSD"] += row_total

            for b in row.get("modelBreakdowns") or []:
                if isinstance(b, dict) and isinstance(b.get("modelName"), str) and isinstance(b.get("cost"), (int, float)):
                    node["models"][b["modelName"]] += float(b["cost"])

    daily = []
    for day in sorted(daily_map.keys()):
        node = daily_map[day]
        models_sorted = sorted(node["models"].items(), key=lambda kv: kv[1], reverse=True)
        daily.append(
            {
                "date": day,
                "totalCostUSD": node["totalCostUSD"],
                "providers": node["providers"],
                "models": [{"model": m, "costUSD": c} for m, c in models_sorted],
            }
        )

    provider_ranked = sorted(provider_totals.items(), key=lambda kv: kv[1], reverse=True)
    model_ranked = sorted(combined_model_totals.items(), key=lambda kv: kv[1], reverse=True)
    provider_model_ranked = sorted(by_provider_model_totals.items(), key=lambda kv: kv[1], reverse=True)

    return {
        "available": True,
        "providers": [{"provider": p, "totalCostUSD": c} for p, c in provider_ranked],
        "totals": {
            "grandTotalCostUSD": sum(provider_totals.values()),
            "providers": provider_totals,
            "models": dict(combined_model_totals),
            "providerModels": dict(by_provider_model_totals),
        },
        "topModels": [{"model": m, "totalCostUSD": c} for m, c in model_ranked[:12]],
        "topProviderModels": [{"providerModel": pm, "totalCostUSD": c} for pm, c in provider_model_ranked[:12]],
        "daily": daily,
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


def _build_notification_text(title: str, lines: List[str]) -> str:
    body = "\n".join([x for x in lines if x])
    return f"{title}\n{body}" if body else title


def _dispatch_webhook(channel: str, webhook_url: str, text: str, timeout_seconds: float = 8.0) -> Dict[str, Any]:
    if not isinstance(webhook_url, str) or not webhook_url.startswith(("https://", "http://")):
        return {"status": "failed", "reason": "invalid_webhook_url", "channel": channel}

    payload: Dict[str, Any]
    ch = channel.lower()
    if ch == "discord":
        payload = {"content": text}
    elif ch == "slack":
        payload = {"text": text}
    else:
        payload = {"text": text}

    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            status = int(getattr(resp, "status", 200) or 200)
        if 200 <= status < 300:
            return {"status": "sent", "channel": channel, "httpStatus": status}
        return {"status": "failed", "channel": channel, "httpStatus": status}
    except urllib.error.HTTPError as exc:
        return {"status": "failed", "channel": channel, "httpStatus": int(exc.code), "reason": str(exc)}
    except Exception as exc:
        return {"status": "failed", "channel": channel, "reason": str(exc)}


def _parse_channel_target(channel: Optional[str], target: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    ch = (channel or "").strip().lower()
    tg = (target or "").strip()

    # 支援 alert config 既有格式: slack:webhook:https://..., discord:webhook:https://...
    if not ch and tg:
        parts = tg.split(":", 2)
        if len(parts) == 3 and parts[1] == "webhook":
            return parts[0].lower(), parts[2]

    if ch in {"slack", "discord"}:
        return ch, tg

    if ch.startswith("slack:webhook:"):
        return "slack", ch.split(":", 2)[2]
    if ch.startswith("discord:webhook:"):
        return "discord", ch.split(":", 2)[2]

    if ch and tg.startswith(("http://", "https://")):
        return ch, tg

    return None, None


def dispatch_report_delivery(
    report_payload: Dict[str, Any],
    recipient: Dict[str, Any],
    timeout_seconds: float = 8.0,
    retries: int = 0,
) -> Dict[str, Any]:
    channel, webhook_url = _parse_channel_target(recipient.get("channel"), recipient.get("target"))
    if not channel or not webhook_url:
        return {
            "target": recipient.get("target"),
            "channel": recipient.get("channel"),
            "status": "blocked",
            "reason": "unsupported_or_missing_webhook",
        }

    job = report_payload.get("job", {}) if isinstance(report_payload.get("job"), dict) else {}
    summary = report_payload.get("summary", {}) if isinstance(report_payload.get("summary"), dict) else {}
    text = _build_notification_text(
        f"[token-usage-dashboard] Report Ready: {job.get('name') or job.get('id')}",
        [
            f"provider={report_payload.get('provider')}",
            f"generatedAt={report_payload.get('generatedAt')}",
            f"range={summary.get('startDate')} ~ {summary.get('endDate')}",
            f"totalCostUSD={_safe_float(summary.get('totalCostUSD')):.2f}",
            f"last7dCostUSD={_safe_float(summary.get('last7dCostUSD')):.2f}",
        ],
    )

    last_result: Dict[str, Any] = {"status": "failed", "reason": "unknown"}
    attempts = max(0, retries) + 1
    for i in range(attempts):
        last_result = _dispatch_webhook(channel, webhook_url, text, timeout_seconds=timeout_seconds)
        if last_result.get("status") == "sent":
            return {
                "target": recipient.get("target"),
                "channel": channel,
                **last_result,
                "attempt": i + 1,
            }
        if i < attempts - 1:
            time.sleep(0.4 * (i + 1))

    return {
        "target": recipient.get("target"),
        "channel": channel,
        **last_result,
        "attempt": attempts,
    }


def dispatch_event_alerts(
    summary: Dict[str, Any],
    alert_config: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 8.0,
    retries: int = 0,
) -> Dict[str, Any]:
    cfg = alert_config if isinstance(alert_config, dict) else {}
    channels_raw = cfg.get("notificationChannels") if isinstance(cfg.get("notificationChannels"), list) else []
    alerts = summary.get("alerts") if isinstance(summary.get("alerts"), dict) else {}
    triggered = alerts.get("triggered") if isinstance(alerts.get("triggered"), list) else []
    controls = summary.get("realTimeCostControls") if isinstance(summary.get("realTimeCostControls"), dict) else {}
    control_actions = controls.get("triggeredActions") if isinstance(controls.get("triggeredActions"), list) else []
    overage = summary.get("overageBehaviors") if isinstance(summary.get("overageBehaviors"), dict) else {}
    overage_events = overage.get("events") if isinstance(overage.get("events"), list) else []
    unified_budget = summary.get("unifiedBudgetAlerts") if isinstance(summary.get("unifiedBudgetAlerts"), dict) else {}
    unified_budget_events = unified_budget.get("events") if isinstance(unified_budget.get("events"), list) else []

    if not (triggered or control_actions or overage_events or unified_budget_events):
        return {"sent": 0, "failed": 0, "events": 0, "results": [], "reason": "no_triggered_events"}

    lines: List[str] = [
        f"provider={summary.get('provider')}",
        f"window={summary.get('startDate')} ~ {summary.get('endDate')}",
        f"totalCostUSD={_safe_float(summary.get('totalCostUSD')):.2f}",
    ]
    for a in triggered[:5]:
        lines.append(f"ALERT[{a.get('severity')}]: {a.get('message')}")
    for a in control_actions[:5]:
        lines.append(f"CONTROL[{a.get('action')}]: {a.get('message')}")
    for a in overage_events[:5]:
        lines.append(f"OVERAGE[{a.get('action')}]: {a.get('dimension')}:{a.get('key')} { _safe_float(a.get('usagePct')):.1f}%")
    for a in unified_budget_events[:5]:
        lines.append(f"UNIFIED_BUDGET[{a.get('severity')}]: {a.get('message')}")

    text = _build_notification_text("[token-usage-dashboard] Event Monitor Alert", lines)

    results: List[Dict[str, Any]] = []
    for entry in channels_raw:
        if not isinstance(entry, str):
            continue
        channel, webhook_url = _parse_channel_target(None, entry)
        if not channel or not webhook_url:
            results.append({"channel": entry, "status": "blocked", "reason": "unsupported_or_missing_webhook"})
            continue

        attempts = max(0, retries) + 1
        last: Dict[str, Any] = {"status": "failed"}
        for i in range(attempts):
            last = _dispatch_webhook(channel, webhook_url, text, timeout_seconds=timeout_seconds)
            if last.get("status") == "sent":
                results.append({"channel": channel, **last, "attempt": i + 1})
                break
            if i < attempts - 1:
                time.sleep(0.4 * (i + 1))
        else:
            results.append({"channel": channel, **last, "attempt": attempts})

    sent = len([x for x in results if x.get("status") == "sent"])
    failed = len(results) - sent
    return {
        "sent": sent,
        "failed": failed,
        "events": len(triggered) + len(control_actions) + len(overage_events) + len(unified_budget_events),
        "results": results,
    }


def _safe_report_job_id(value: Any, fallback: str) -> str:
    raw = str(value or "").strip().lower()
    sanitized = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-")
    return sanitized or fallback


def _normalize_report_jobs(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    jobs_raw = config.get("jobs")
    if not isinstance(jobs_raw, list):
        return []

    out: List[Dict[str, Any]] = []
    for idx, job in enumerate(jobs_raw, start=1):
        if not isinstance(job, dict):
            continue
        metrics_raw = job.get("metrics")
        metrics = [str(x).strip() for x in metrics_raw] if isinstance(metrics_raw, list) else ["total_cost"]
        models_raw = job.get("models")
        models = [str(x).strip() for x in models_raw if str(x).strip()] if isinstance(models_raw, list) else []
        recipients_raw = job.get("recipients")
        recipients = [x for x in recipients_raw if isinstance(x, dict)] if isinstance(recipients_raw, list) else []
        out.append({
            "id": _safe_report_job_id(job.get("id"), f"job-{idx}"),
            "name": str(job.get("name") or f"Scheduled Report {idx}"),
            "enabled": bool(job.get("enabled", True)),
            "frequency": str(job.get("frequency") or "daily").lower(),
            "granularity": str(job.get("granularity") or "daily").lower(),
            "metrics": metrics,
            "models": models,
            "layout": job.get("layout") if isinstance(job.get("layout"), dict) else {},
            "orgId": str(job.get("orgId") or "").strip() or None,
            "user": str(job.get("user") or "").strip() or None,
            "role": str(job.get("role") or "").strip() or None,
            "dashboardView": str(job.get("dashboardView") or "").strip() or None,
            "allowedRoles": [str(x).strip() for x in (job.get("allowedRoles") or []) if str(x).strip()],
            "recipients": recipients,
            "formats": [str(x).strip().lower() for x in (job.get("formats") or ["json"]) if str(x).strip()],
            "onlyOnChange": bool(job.get("onlyOnChange", False)),
        })
    return out


def _frequency_due(frequency: str, now: datetime, history: Optional[Dict[str, Any]]) -> bool:
    last_ts = history.get("generatedAt") if isinstance(history, dict) else None
    if not isinstance(last_ts, str):
        return True
    try:
        last = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
    except Exception:
        return True

    freq = (frequency or "daily").lower()
    if freq == "daily":
        return now.date() > last.date()
    if freq == "weekly":
        return now.isocalendar()[:2] != last.isocalendar()[:2]
    if freq == "monthly":
        return (now.year, now.month) != (last.year, last.month)
    if freq == "quarterly":
        return (now.year, (now.month - 1) // 3) != (last.year, (last.month - 1) // 3)
    return True


def _history_path(output_dir: Path) -> Path:
    return output_dir / "report_history.json"


def _load_report_history(output_dir: Path) -> Dict[str, Any]:
    path = _history_path(output_dir)
    if not path.exists():
        return {"reports": [], "latestByJob": {}}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"reports": [], "latestByJob": {}}
    if not isinstance(raw, dict):
        return {"reports": [], "latestByJob": {}}
    if not isinstance(raw.get("reports"), list):
        raw["reports"] = []
    if not isinstance(raw.get("latestByJob"), dict):
        raw["latestByJob"] = {}
    return raw


def _write_report_history(output_dir: Path, history: Dict[str, Any]) -> None:
    _history_path(output_dir).write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")


def _export_report_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    if not rows:
        path.write_text("period\n", encoding="utf-8")
        return
    keys = ["period"] + [k for k in rows[0].keys() if k != "period"]
    lines = [",".join(keys)]
    for row in rows:
        vals = []
        for k in keys:
            v = row.get(k, "")
            text = str(v)
            if any(ch in text for ch in [",", '"', "\n"]):
                text = '"' + text.replace('"', '""') + '"'
            vals.append(text)
        lines.append(",".join(vals))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _report_payload_fingerprint(report_payload: Dict[str, Any]) -> str:
    compact = {
        "job": report_payload.get("job"),
        "provider": report_payload.get("provider"),
        "role": report_payload.get("role"),
        "tenant": report_payload.get("tenant"),
        "summary": {
            "totalCostUSD": ((report_payload.get("summary") or {}).get("totalCostUSD") if isinstance(report_payload.get("summary"), dict) else None),
            "startDate": ((report_payload.get("summary") or {}).get("startDate") if isinstance(report_payload.get("summary"), dict) else None),
            "endDate": ((report_payload.get("summary") or {}).get("endDate") if isinstance(report_payload.get("summary"), dict) else None),
        },
        "reportRows": report_payload.get("reportRows") or [],
    }
    raw = json.dumps(compact, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _recipient_allowed(recipient: Dict[str, Any], role_name: str, job_allowed_roles: List[str]) -> bool:
    allowed_roles = [str(x).strip() for x in recipient.get("allowedRoles", []) if str(x).strip()] if isinstance(recipient.get("allowedRoles"), list) else []
    effective_allowed = allowed_roles or job_allowed_roles
    if not effective_allowed:
        return True
    return role_name in effective_allowed


def run_report_scheduler(
    payload: Dict[str, Any],
    provider: str,
    config: Dict[str, Any],
    output_dir: Path,
    *,
    tenant_config_path: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    now_dt = now or datetime.now(ZoneInfo("UTC"))
    jobs = _normalize_report_jobs(config)
    history = _load_report_history(output_dir)
    latest_by_job = history.get("latestByJob") if isinstance(history.get("latestByJob"), dict) else {}
    dispatch_cfg = config.get("dispatch") if isinstance(config.get("dispatch"), dict) else {}
    dispatch_enabled = bool(dispatch_cfg.get("enabled", True))
    dispatch_timeout = max(1.0, _safe_float(dispatch_cfg.get("timeoutSeconds"), default=8.0))
    dispatch_retries = _safe_int(dispatch_cfg.get("retries"), default=0, minimum=0)
    history_cfg = config.get("history") if isinstance(config.get("history"), dict) else {}
    max_reports_per_job = _safe_int(history_cfg.get("maxReportsPerJob"), default=0, minimum=0)

    result = {"now": now_dt.isoformat(), "jobs": [], "generated": 0, "skipped": 0, "sent": 0, "failed": 0}

    for job in jobs:
        job_id = str(job["id"])
        if not job.get("enabled", True):
            result["jobs"].append({"jobId": job_id, "status": "skipped", "reason": "disabled"})
            result["skipped"] += 1
            continue
        if not _frequency_due(str(job.get("frequency", "daily")), now_dt, latest_by_job.get(job_id)):
            result["jobs"].append({"jobId": job_id, "status": "skipped", "reason": "not_due"})
            result["skipped"] += 1
            continue

        if tenant_config_path and job.get("orgId"):
            rows, role_name, _, tenant_meta = resolve_multi_tenant_context(
                payload=payload,
                tenant_config_path=tenant_config_path,
                org_id=str(job.get("orgId")),
                role=job.get("role"),
                user=job.get("user"),
                requested_dashboard=job.get("dashboardView"),
                allow_role_override=False,
            )
        else:
            rows = parse_daily(payload)
            role_name = str(job.get("role") or "admin")
            tenant_meta = {}

        report_rows = generate_custom_report(
            rows,
            metrics=job.get("metrics") or ["total_cost"],
            models=job.get("models") or [],
            granularity=str(job.get("granularity") or "daily"),
        )
        ts = now_dt.strftime("%Y%m%dT%H%M%SZ")
        job_dir = output_dir / job_id
        job_dir.mkdir(parents=True, exist_ok=True)

        base = f"{job_id}_{ts}"
        artifact_paths: Dict[str, str] = {}

        summary = build_summary(provider, rows)
        report_payload = {
            "job": {
                "id": job_id,
                "name": job.get("name"),
                "frequency": job.get("frequency"),
                "granularity": job.get("granularity"),
                "layout": job.get("layout") or {},
            },
            "generatedAt": now_dt.isoformat(),
            "provider": provider,
            "role": role_name,
            "tenant": tenant_meta or None,
            "summary": summary,
            "reportRows": report_rows,
        }

        fingerprint = _report_payload_fingerprint(report_payload)
        previous = latest_by_job.get(job_id) if isinstance(latest_by_job.get(job_id), dict) else {}
        only_on_change = bool(job.get("onlyOnChange", False))
        if only_on_change and previous.get("fingerprint") == fingerprint:
            result["jobs"].append({"jobId": job_id, "status": "skipped", "reason": "no_change"})
            result["skipped"] += 1
            continue

        formats = job.get("formats") if isinstance(job.get("formats"), list) else ["json"]
        if "json" in formats:
            json_path = job_dir / f"{base}.json"
            json_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            artifact_paths["json"] = str(json_path)
        if "csv" in formats:
            csv_path = job_dir / f"{base}.csv"
            _export_report_csv(report_rows, csv_path)
            artifact_paths["csv"] = str(csv_path)

        deliveries = []
        for recipient in job.get("recipients") or []:
            if not isinstance(recipient, dict):
                continue
            if not _recipient_allowed(recipient, role_name, job.get("allowedRoles") or []):
                deliveries.append({
                    "target": recipient.get("target"),
                    "channel": recipient.get("channel"),
                    "status": "blocked",
                    "reason": "role_not_allowed",
                })
                continue
            if dispatch_enabled:
                dispatch_result = dispatch_report_delivery(
                    report_payload,
                    recipient,
                    timeout_seconds=dispatch_timeout,
                    retries=dispatch_retries,
                )
                deliveries.append(dispatch_result)
                if dispatch_result.get("status") == "sent":
                    result["sent"] += 1
                elif dispatch_result.get("status") != "blocked":
                    result["failed"] += 1
            else:
                deliveries.append({
                    "target": recipient.get("target"),
                    "channel": recipient.get("channel"),
                    "status": "queued",
                })

        history_item = {
            "jobId": job_id,
            "jobName": job.get("name"),
            "generatedAt": now_dt.isoformat(),
            "frequency": job.get("frequency"),
            "granularity": job.get("granularity"),
            "artifacts": artifact_paths,
            "deliveries": deliveries,
            "layout": job.get("layout") or {},
            "tenant": tenant_meta or None,
            "fingerprint": fingerprint,
        }
        history["reports"].append(history_item)
        latest_by_job[job_id] = {"generatedAt": now_dt.isoformat(), "artifacts": artifact_paths, "fingerprint": fingerprint}

        result["jobs"].append({"jobId": job_id, "status": "generated", "artifacts": artifact_paths, "deliveries": deliveries})
        result["generated"] += 1

    history["latestByJob"] = latest_by_job
    if max_reports_per_job > 0:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        passthrough: List[Dict[str, Any]] = []
        for item in history.get("reports", []):
            if not isinstance(item, dict):
                continue
            jid = str(item.get("jobId") or "").strip()
            if not jid:
                passthrough.append(item)
                continue
            grouped[jid].append(item)
        pruned: List[Dict[str, Any]] = list(passthrough)
        for _, items in grouped.items():
            ranked = sorted(items, key=lambda x: str(x.get("generatedAt") or ""), reverse=True)
            pruned.extend(ranked[:max_reports_per_job])
        history["reports"] = sorted(pruned, key=lambda x: str(x.get("generatedAt") or ""))
    _write_report_history(output_dir, history)
    return result


def build_dashboard_html(
    provider: str,
    rows: List[Dict[str, Any]],
    top_models: int,
    spike_lookback_days: int = 7,
    spike_threshold_mult: float = 2.0,
    max_table_rows: int = 120,
    chart_max_points: int = 1200,
    role_name: str = "admin",
    policy: Optional[Dict[str, Any]] = None,
    alert_config: Optional[Dict[str, Any]] = None,
    cost_control_config: Optional[Dict[str, Any]] = None,
    multi_provider_agg: Optional[Dict[str, Any]] = None,
    budget_config: Optional[Dict[str, Any]] = None,
    prompt_optimization_config: Optional[Dict[str, Any]] = None,
    cloud_cost_rows: Optional[List[Dict[str, Any]]] = None,
    attribution_granularity: str = "standard",
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
        alert_config=alert_config,
        cost_control_config=cost_control_config,
        budget_config=budget_config,
        prompt_optimization_config=prompt_optimization_config,
        cloud_cost_rows=cloud_cost_rows,
        attribution_granularity=attribution_granularity,
    )
    spike_count = len(summary.get("spikes", []))

    movers_rows_parts: List[str] = []
    for idx, x in enumerate(summary.get("movers", [])[:8], start=1):
        pct = f"{x['deltaPct']:+.1f}%" if isinstance(x.get("deltaPct"), (int, float)) else "N/A"
        movers_rows_parts.append(
            f"<tr><td>{idx}</td><td>{esc(x['model'])}</td><td>{usd(float(x['last7dCostUSD']))}</td><td>{usd(float(x['prev7dCostUSD']))}</td><td>{usd(float(x['deltaCostUSD']))}</td><td>{pct}</td></tr>"
        )
    movers_rows = "\n".join(movers_rows_parts)

    visible_spikes = summary.get("spikes", []) if policy.get("canViewSpikes", True) else []
    spikes_rows_parts: List[str] = []
    for idx, x in enumerate(visible_spikes[:10], start=1):
        date_key = str(x["date"])
        spikes_rows_parts.append(
            f"<tr id='spike-row-{esc(date_key)}'><td>{idx}</td><td>{esc(x['date'])}</td><td>{usd(float(x['costUSD']))}</td><td>{usd(float(x['baselineUSD']))}</td><td>{x['ratio']:.2f}x</td></tr>"
        )
    spikes_rows = "\n".join(spikes_rows_parts) if spikes_rows_parts else "<tr><td colspan='5'>No spikes detected</td></tr>"

    multi_provider_rows = "<tr><td colspan='3'>Single-provider mode (no aggregation enabled)</td></tr>"
    multi_provider_models_rows = "<tr><td colspan='3'>Single-provider mode (no aggregation enabled)</td></tr>"
    multi_provider_header = ""
    if isinstance(multi_provider_agg, dict) and multi_provider_agg.get("available"):
        mp_grand_total = _safe_float(multi_provider_agg.get("totals", {}).get("grandTotalCostUSD"))
        multi_provider_header = f"<div style=\"font-size:12px;color:#6b7280;margin:-6px 0 8px;\">Aggregated providers total: {usd(mp_grand_total)}</div>"
        p_rows = []
        for idx, p in enumerate(multi_provider_agg.get("providers", [])[:8], start=1):
            p_rows.append(f"<tr><td>{idx}</td><td>{esc(p.get('provider'))}</td><td>{usd(float(p.get('totalCostUSD', 0.0)))}</td></tr>")
        multi_provider_rows = "\n".join(p_rows) or "<tr><td colspan='3'>No provider aggregation data</td></tr>"

        m_rows = []
        for idx, m in enumerate(multi_provider_agg.get("topModels", [])[:10], start=1):
            m_rows.append(f"<tr><td>{idx}</td><td>{esc(m.get('model'))}</td><td>{usd(float(m.get('totalCostUSD', 0.0)))}</td></tr>")
        multi_provider_models_rows = "\n".join(m_rows) or "<tr><td colspan='3'>No model aggregation data</td></tr>"

    forecast = summary.get("forecast") if isinstance(summary.get("forecast"), dict) else {}
    f7 = forecast.get("next7Days") if isinstance(forecast.get("next7Days"), dict) else {}
    f30 = forecast.get("next30Days") if isinstance(forecast.get("next30Days"), dict) else {}
    anomalies = summary.get("costAnomalies") if isinstance(summary.get("costAnomalies"), list) else []
    alerts = summary.get("alerts") if isinstance(summary.get("alerts"), dict) else {}
    alert_rows = "\n".join([
        f"<tr><td>{i+1}</td><td>{esc(a.get('rule'))}</td><td>{esc(a.get('severity'))}</td><td>{esc(a.get('message'))}</td></tr>"
        for i, a in enumerate(alerts.get("triggered", [])[:8])
    ]) or "<tr><td colspan='4'>No alert triggered</td></tr>"
    anomaly_rows = "\n".join([
        f"<tr><td>{i+1}</td><td>{esc(a.get('date'))}</td><td>{usd(float(a.get('costUSD', 0.0)))}</td><td>{_safe_float(a.get('zScore', 0.0)):.2f}</td><td>{esc(a.get('severity'))}</td></tr>"
        for i, a in enumerate(anomalies[:10])
    ]) or "<tr><td colspan='5'>No anomaly detected</td></tr>"

    cost_controls = summary.get("realTimeCostControls") if isinstance(summary.get("realTimeCostControls"), dict) else {"available": False}
    cost_control_layer_rows = "<tr><td colspan='7'>No real-time cost control policy configured</td></tr>"
    if cost_controls.get("available") and isinstance(cost_controls.get("layers"), list):
        items = cost_controls.get("layers", [])
        if items:
            cost_control_layer_rows = "\n".join([
                f"<tr><td>{i+1}</td><td>{esc(x.get('id'))}</td><td>{esc(x.get('scope'))}</td><td>{esc(x.get('metric'))}</td><td>{float(x.get('value', 0.0)):.2f}</td><td>{float(x.get('threshold', 0.0)):.2f}</td><td>{'yes' if x.get('triggered') else 'no'}</td></tr>"
                for i, x in enumerate(items[:12])
            ])

    cost_control_action_rows = "<tr><td colspan='6'>No control action triggered</td></tr>"
    if cost_controls.get("available") and isinstance(cost_controls.get("triggeredActions"), list) and cost_controls.get("triggeredActions"):
        cost_control_action_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('layerId'))}</td><td>{esc(x.get('scope'))}</td><td>{esc(x.get('action'))}</td><td>{esc(x.get('routeToModel') or '—')}</td><td>{esc(x.get('message'))}</td></tr>"
            for i, x in enumerate(cost_controls.get("triggeredActions", [])[:12])
        ])

    pattern = summary.get("llmPatternAnalysis") if isinstance(summary.get("llmPatternAnalysis"), dict) else {"available": False}
    def _render_pattern_rows(items: List[Dict[str, Any]], key_name: str = "key") -> str:
        if not items:
            return "<tr><td colspan='5'>No data</td></tr>"
        out_rows: List[str] = []
        for idx, item in enumerate(items[:8], start=1):
            out_rows.append(
                f"<tr><td>{idx}</td><td>{esc(item.get(key_name, 'unknown'))}</td><td>{usd(float(item.get('costUSD', 0.0)))}</td><td>{float(item.get('totalTokens', 0.0)):,.0f}</td><td>{int(float(item.get('count', 0.0)))}</td></tr>"
            )
        return "\n".join(out_rows)

    pattern_model_rows = _render_pattern_rows(pattern.get("dimensions", {}).get("byModel", [])) if pattern.get("available") else "<tr><td colspan='5'>No call-level analysis data</td></tr>"
    pattern_model_type_rows = _render_pattern_rows(pattern.get("dimensions", {}).get("byModelType", [])) if pattern.get("available") else "<tr><td colspan='5'>No call-level analysis data</td></tr>"
    pattern_project_rows = _render_pattern_rows(pattern.get("dimensions", {}).get("byProject", [])) if pattern.get("available") else "<tr><td colspan='5'>No call-level analysis data</td></tr>"
    pattern_task_rows = _render_pattern_rows(pattern.get("dimensions", {}).get("byTask", [])) if pattern.get("available") else "<tr><td colspan='5'>No call-level analysis data</td></tr>"
    pattern_user_rows = _render_pattern_rows(pattern.get("dimensions", {}).get("byUser", [])) if pattern.get("available") else "<tr><td colspan='5'>No call-level analysis data</td></tr>"
    pattern_eff_rows = "<tr><td colspan='6'>No efficiency data</td></tr>"
    if pattern.get("available") and isinstance(pattern.get("efficiency"), list) and pattern.get("efficiency"):
        buf: List[str] = []
        for idx, item in enumerate(pattern["efficiency"][:8], start=1):
            cpk = item.get("costPer1kTokensUSD")
            lat = item.get("avgLatencyMs")
            buf.append(f"<tr><td>{idx}</td><td>{esc(item.get('model', 'unknown'))}</td><td>{usd(float(item.get('costUSD', 0.0)))}</td><td>{float(item.get('totalTokens', 0.0)):,.0f}</td><td>{(f'{cpk:.4f}' if isinstance(cpk, (int, float)) else 'N/A')}</td><td>{(f'{lat:.1f}' if isinstance(lat, (int, float)) else 'N/A')}</td></tr>")
        pattern_eff_rows = "\n".join(buf)

    keyword_rows = "<tr><td colspan='3'>No keyword data</td></tr>"
    if pattern.get("available") and isinstance(pattern.get("anonymizedPromptKeywords"), list) and pattern.get("anonymizedPromptKeywords"):
        keyword_rows = "\n".join([f"<tr><td>{i+1}</td><td>{esc(k.get('keyword'))}</td><td>{k.get('count')}</td></tr>" for i, k in enumerate(pattern.get("anonymizedPromptKeywords", [])[:12])])

    def _render_hotspot_rows(items: List[Dict[str, Any]], key_name: str) -> str:
        if not items:
            return "<tr><td colspan='4'>No data</td></tr>"
        return "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get(key_name, 'unknown'))}</td><td>{usd(float(x.get('costUSD', 0.0)))}</td><td>{float(x.get('totalTokens', 0.0)):,.0f}</td></tr>"
            for i, x in enumerate(items[:10])
        ])

    top_api_rows = _render_hotspot_rows(pattern.get("hotspots", {}).get("topApiCalls", []), "model") if pattern.get("available") else "<tr><td colspan='4'>No call-level analysis data</td></tr>"
    top_session_rows = _render_hotspot_rows(pattern.get("hotspots", {}).get("topSessions", []), "key") if pattern.get("available") else "<tr><td colspan='4'>No call-level analysis data</td></tr>"
    top_workflow_rows = _render_hotspot_rows(pattern.get("hotspots", {}).get("topWorkflows", []), "key") if pattern.get("available") else "<tr><td colspan='4'>No call-level analysis data</td></tr>"

    prompt_stats = pattern.get("promptTokens", {}) if pattern.get("available") else {}
    completion_stats = pattern.get("completionTokens", {}) if pattern.get("available") else {}

    attribution = summary.get("costAttribution") if isinstance(summary.get("costAttribution"), dict) else {"available": False}
    recs = summary.get("optimizationRecommendations") if isinstance(summary.get("optimizationRecommendations"), dict) else {"available": False}
    unified_cloud_view = summary.get("unifiedCloudCostView") if isinstance(summary.get("unifiedCloudCostView"), dict) else {"available": False}
    unified_budget_alerts = summary.get("unifiedBudgetAlerts") if isinstance(summary.get("unifiedBudgetAlerts"), dict) else {"available": False}

    cloud_provider_rows = "<tr><td colspan='3'>No cloud provider cost data</td></tr>"
    if isinstance(unified_cloud_view.get("cloudProviders"), list) and unified_cloud_view.get("cloudProviders"):
        cloud_provider_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('provider'))}</td><td>{usd(float(x.get('totalCostUSD', 0.0)))}</td></tr>"
            for i, x in enumerate(unified_cloud_view.get("cloudProviders", [])[:8])
        ])

    cloud_service_rows = "<tr><td colspan='3'>No cloud service cost data</td></tr>"
    if isinstance(unified_cloud_view.get("cloudServices"), list) and unified_cloud_view.get("cloudServices"):
        cloud_service_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('providerService'))}</td><td>{usd(float(x.get('totalCostUSD', 0.0)))}</td></tr>"
            for i, x in enumerate(unified_cloud_view.get("cloudServices", [])[:10])
        ])

    cloud_tag_rows = "<tr><td colspan='3'>No cloud tag cost data</td></tr>"
    if isinstance(unified_cloud_view.get("cloudTags"), list) and unified_cloud_view.get("cloudTags"):
        cloud_tag_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('tag'))}</td><td>{usd(float(x.get('totalCostUSD', 0.0)))}</td></tr>"
            for i, x in enumerate(unified_cloud_view.get("cloudTags", [])[:10])
        ])

    unified_totals = unified_cloud_view.get("totals") if isinstance(unified_cloud_view.get("totals"), dict) else {}

    unified_budget_rows = "<tr><td colspan='7'>No unified budget alert triggered</td></tr>"
    if unified_budget_alerts.get("available") and isinstance(unified_budget_alerts.get("events"), list) and unified_budget_alerts.get("events"):
        unified_budget_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('id'))}</td><td>{esc(x.get('scope'))}</td><td>{usd(float(x.get('valueUSD', 0.0)))}</td><td>{usd(float(x.get('thresholdUSD', 0.0)))}</td><td>{_safe_float(x.get('usagePct', 0.0)):.1f}%</td><td>{esc(x.get('message'))}</td></tr>"
            for i, x in enumerate(unified_budget_alerts.get("events", [])[:12])
        ])

    def _render_attribution_rows(items: List[Dict[str, Any]]) -> str:
        if not items:
            return "<tr><td colspan='6'>No attribution data</td></tr>"
        return "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('key', 'unknown'))}</td><td>{usd(float(x.get('costUSD', 0.0)))}</td><td>{float(x.get('totalTokens', 0.0)):,.0f}</td><td>{int(x.get('count', 0))}</td><td>{_safe_float(x.get('sharePct', 0.0)):.1f}%</td></tr>"
            for i, x in enumerate(items[:8])
        ])

    attr_project_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("project", [])) if attribution.get("available") else "<tr><td colspan='6'>No call-level attribution data</td></tr>"
    attr_dept_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("department", [])) if attribution.get("available") else "<tr><td colspan='6'>No call-level attribution data</td></tr>"
    attr_user_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("user", [])) if attribution.get("available") else "<tr><td colspan='6'>No call-level attribution data</td></tr>"
    attr_app_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("application", [])) if attribution.get("available") else "<tr><td colspan='6'>No call-level attribution data</td></tr>"
    attr_business_line_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("businessLine", [])) if attribution.get("available") else "<tr><td colspan='6'>No call-level attribution data</td></tr>"

    attr_model_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("model", [])) if attribution.get("available") else "<tr><td colspan='6'>No detailed attribution data</td></tr>"
    attr_workflow_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("workflow", [])) if attribution.get("available") else "<tr><td colspan='6'>No detailed attribution data</td></tr>"
    attr_cloud_provider_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("cloudProvider", [])) if attribution.get("available") else "<tr><td colspan='6'>No cloud attribution data</td></tr>"
    attr_cloud_service_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("cloudService", [])) if attribution.get("available") else "<tr><td colspan='6'>No cloud attribution data</td></tr>"
    attr_cloud_tag_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("cloudTag", [])) if attribution.get("available") else "<tr><td colspan='6'>No cloud attribution data</td></tr>"
    attr_cloud_project_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("cloudProject", [])) if attribution.get("available") else "<tr><td colspan='6'>No cloud attribution data</td></tr>"
    attr_cloud_source_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("cloudSource", [])) if attribution.get("available") else "<tr><td colspan='6'>No cloud attribution data</td></tr>"
    attr_cloud_env_rows = _render_attribution_rows(attribution.get("dimensions", {}).get("cloudEnvironment", [])) if attribution.get("available") else "<tr><td colspan='6'>No cloud attribution data</td></tr>"

    rec_rows = "<tr><td colspan='6'>No recommendations</td></tr>"
    if recs.get("available") and isinstance(recs.get("recommendations"), list) and recs.get("recommendations"):
        rec_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(r.get('priority'))}</td><td>{esc(r.get('title'))}</td><td>{esc(r.get('rationale'))}</td><td>{_safe_float(r.get('estimatedSavingsPct', 0.0)):.1f}%</td><td>{esc('; '.join(r.get('actions', [])) if isinstance(r.get('actions'), list) else '')}</td></tr>"
            for i, r in enumerate(recs.get("recommendations", [])[:6])
        ])

    prompt_engine = summary.get("promptOptimizationEngine") if isinstance(summary.get("promptOptimizationEngine"), dict) else {"available": False}
    prompt_family_rows = "<tr><td colspan='8'>No prompt-level optimization data</td></tr>"
    if prompt_engine.get("available") and isinstance(prompt_engine.get("highConsumptionPrompts"), list) and prompt_engine.get("highConsumptionPrompts"):
        prompt_family_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('templateSignature'))}</td><td>{esc(x.get('topProject'))}</td><td>{esc(x.get('topModel'))}</td><td>{int(x.get('calls', 0))}</td><td>{usd(float(x.get('totalCostUSD', 0.0)))}</td><td>{float(x.get('avgPromptTokens', 0.0)):.1f}</td><td>{esc('; '.join([s.get('title', '') for s in x.get('suggestions', []) if isinstance(s, dict)]))}</td></tr>"
            for i, x in enumerate(prompt_engine.get("highConsumptionPrompts", [])[:8])
        ])

    ab_test_rows = "<tr><td colspan='6'>No A/B test plan generated</td></tr>"
    if prompt_engine.get("available") and isinstance(prompt_engine.get("abTests"), list) and prompt_engine.get("abTests"):
        ab_test_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('testId'))}</td><td>{esc(x.get('scope'))}</td><td>{esc('/'.join([v.get('name', '') for v in x.get('variants', []) if isinstance(v, dict)]))}</td><td>{esc(', '.join(x.get('metrics', [])) if isinstance(x.get('metrics'), list) else '')}</td><td>{esc(str(x.get('successCriteria')))}</td></tr>"
            for i, x in enumerate(prompt_engine.get("abTests", [])[:8])
        ])

    budget_eval = summary.get("budgetAllocation") if isinstance(summary.get("budgetAllocation"), dict) else {"available": False}
    budget_alloc_rows = "<tr><td colspan='8'>No budget allocation configured</td></tr>"
    if budget_eval.get("available") and isinstance(budget_eval.get("allocations"), list) and budget_eval.get("allocations"):
        budget_alloc_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('dimension'))}</td><td>{esc(x.get('key'))}</td><td>{usd(float(x.get('budgetUSD', 0.0)))}</td><td>{usd(float(x.get('actualCostUSD', 0.0)))}</td><td>{_safe_float(x.get('usagePct', 0.0)):.1f}%</td><td>{usd(float(x.get('remainingUSD', 0.0)))}</td><td>{esc(x.get('status'))}</td></tr>"
            for i, x in enumerate(budget_eval.get("allocations", [])[:12])
        ])

    perms = budget_eval.get("permissions") if isinstance(budget_eval.get("permissions"), dict) else {}
    role_rows = "<tr><td colspan='4'>No role permissions configured</td></tr>"
    if isinstance(perms.get("roles"), dict) and perms.get("roles"):
        role_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(role)}</td><td>{esc(', '.join([str(m) for m in cfg.get('allowedModels', [])]) if isinstance(cfg.get('allowedModels'), list) and cfg.get('allowedModels') else 'all')}</td><td>{esc(str(cfg.get('maxCostPerCallUSD')) if cfg.get('maxCostPerCallUSD') is not None else '—')}</td></tr>"
            for i, (role, cfg) in enumerate(sorted([(k, v) for k, v in perms.get("roles", {}).items() if isinstance(v, dict)], key=lambda x: x[0])[:20])
        ])

    user_perm_rows = "<tr><td colspan='5'>No user permission overrides configured</td></tr>"
    if isinstance(perms.get("users"), dict) and perms.get("users"):
        user_perm_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(user_name)}</td><td>{esc(str(cfg.get('role') or perms.get('defaultRole') or 'viewer'))}</td><td>{esc(', '.join([str(m) for m in cfg.get('allowedModels', [])]) if isinstance(cfg.get('allowedModels'), list) and cfg.get('allowedModels') else 'inherit')}</td><td>{esc(str(cfg.get('maxCostPerCallUSD')) if cfg.get('maxCostPerCallUSD') is not None else 'inherit')}</td></tr>"
            for i, (user_name, cfg) in enumerate(sorted([(k, v) for k, v in perms.get("users", {}).items() if isinstance(v, dict)], key=lambda x: x[0])[:20])
        ])

    permission_violation_rows = "<tr><td colspan='7'>No permission violations detected</td></tr>"
    if isinstance(perms.get("violations"), list) and perms.get("violations"):
        permission_violation_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(v.get('user'))}</td><td>{esc(v.get('role'))}</td><td>{esc(v.get('model'))}</td><td>{esc(v.get('violation'))}</td><td>{int(v.get('calls', 0))}</td><td>{usd(float(v.get('costUSD', 0.0)))}</td></tr>"
            for i, v in enumerate(perms.get("violations", [])[:20])
        ])

    overage = summary.get("overageBehaviors") if isinstance(summary.get("overageBehaviors"), dict) else {"available": False}
    overage_rows = "<tr><td colspan='7'>No overage events</td></tr>"
    if overage.get("available") and isinstance(overage.get("events"), list) and overage.get("events"):
        overage_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('dimension'))}</td><td>{esc(x.get('key'))}</td><td>{_safe_float(x.get('usagePct', 0.0)):.1f}%</td><td>{esc(x.get('action'))}</td><td>{esc(x.get('routeToModel') or '—')}</td><td>{esc(x.get('message'))}</td></tr>"
            for i, x in enumerate(overage.get("events", [])[:12])
        ])

    quota_policies = summary.get("quotaPolicies") if isinstance(summary.get("quotaPolicies"), dict) else {"available": False}
    quota_summary = quota_policies.get("summary") if isinstance(quota_policies.get("summary"), dict) else {}
    quota_alloc_rows = "<tr><td colspan='7'>No quota allocation policy</td></tr>"
    if quota_policies.get("available") and isinstance(quota_policies.get("allocations"), list) and quota_policies.get("allocations"):
        quota_alloc_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('policyId'))}</td><td>{esc(x.get('scope'))}</td><td>{usd(float(x.get('budgetUSD', 0.0)))}</td><td>{usd(float(x.get('usedUSD', 0.0)))}</td><td>{_safe_float(x.get('usagePct', 0.0)):.1f}%</td><td>{esc(x.get('status'))}</td></tr>"
            for i, x in enumerate(quota_policies.get("allocations", [])[:12])
        ])

    quota_enforcement_rows = "<tr><td colspan='6'>No auto-enforcement events</td></tr>"
    if quota_policies.get("available") and isinstance(quota_policies.get("enforcements"), list) and quota_policies.get("enforcements"):
        quota_enforcement_rows = "\n".join([
            f"<tr><td>{i+1}</td><td>{esc(x.get('allocationId'))}</td><td>{esc(x.get('dimension'))}:{esc(x.get('key'))}</td><td>{esc(x.get('action'))}</td><td>{esc(x.get('routeToModel') or '—')}</td><td>{esc(x.get('message'))}</td></tr>"
            for i, x in enumerate(quota_policies.get("enforcements", [])[:12])
        ])

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

    json_labels = json_for_script(labels)
    json_series = json_for_script(series)
    json_day_totals = json_for_script(day_totals)
    all_models = sorted({str(x["model"]) for day in day_breakdown_by_date.values() for x in day if isinstance(x, dict) and isinstance(x.get("model"), str)})

    json_spike_by_date = json_for_script(spike_by_date)
    json_day_breakdown_by_date = json_for_script(day_breakdown_by_date)
    json_day_total_by_date = json_for_script(day_total_by_date)
    json_all_models = json_for_script(all_models)
    json_policy = json_for_script(policy)

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>Token Usage Dashboard · {esc(provider)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; margin: 24px; color: #1f2937; }}
    .grid {{ display: grid; grid-template-columns: repeat(5, minmax(160px, 1fr)); gap: 12px; margin-bottom: 18px; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px 14px; background: #fff; }}
    .label {{ color: #6b7280; font-size: 12px; }}
    .value {{ font-size: 24px; font-weight: 700; margin-top: 4px; }}
    .chart-wrap {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px; margin: 12px 0 18px; position: relative; }}
    canvas {{ width: 100%; height: 360px; }}
    #tooltip {{ position: absolute; pointer-events: none; background: #111827; color: #fff; padding: 8px 10px; border-radius: 8px; font-size: 12px; opacity: 0; transform: translate(-50%, -110%); white-space: pre; }}
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
  <h2>Token Usage Dashboard · {esc(provider)}</h2>
  <div style="color:#6b7280;font-size:12px;margin-bottom:6px;">Role: <strong>{esc(role_name)}</strong></div>
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
  </div>

  <div class=\"chart-wrap\" id=\"chartWrap\">
    <canvas id=\"costChart\"></canvas>
    <div id=\"tooltip\"></div>
  </div>

  <h3>Multi-Provider Unified View</h3>
  {multi_provider_header}
  <table>
    <thead><tr><th>#</th><th>Provider</th><th>Total Cost</th></tr></thead>
    <tbody>{multi_provider_rows}</tbody>
  </table>
  <h4>Unified Top Models (cross-provider)</h4>
  <table>
    <thead><tr><th>#</th><th>Model</th><th>Total Cost</th></tr></thead>
    <tbody>{multi_provider_models_rows}</tbody>
  </table>

  <h3>Unified Cloud Cost View (LLM + AWS/GCP)</h3>
  <div style="font-size:12px;color:#6b7280;margin:-6px 0 8px;">Cloud integration: {esc('enabled' if unified_cloud_view.get('cloudIntegrationEnabled') else 'disabled')} · LLM {usd(float(unified_totals.get('llmCostUSD', 0.0)))} + Cloud Infra {usd(float(unified_totals.get('cloudInfraCostUSD', 0.0)))} = Unified {usd(float(unified_totals.get('totalUnifiedCostUSD', 0.0)))}</div>
  <table>
    <thead><tr><th>#</th><th>Cloud Provider</th><th>Total Cost</th></tr></thead>
    <tbody>{cloud_provider_rows}</tbody>
  </table>
  <h4>Top Cloud Services</h4>
  <table>
    <thead><tr><th>#</th><th>Provider:Service</th><th>Total Cost</th></tr></thead>
    <tbody>{cloud_service_rows}</tbody>
  </table>
  <h4>Top Cloud Cost Tags</h4>
  <table>
    <thead><tr><th>#</th><th>Tag</th><th>Total Cost</th></tr></thead>
    <tbody>{cloud_tag_rows}</tbody>
  </table>

  <h3>Cross-platform Unified Budget Alerts</h3>
  <div style="font-size:12px;color:#6b7280;margin:-6px 0 8px;">Single ruleset across LLM + cloud infrastructure spend (AWS/GCP/provider/service scope). Triggered: {int((unified_budget_alerts.get('summary') or {}).get('triggered', 0))} · bySeverity={esc(str((unified_budget_alerts.get('summary') or {}).get('bySeverity', {})))}.</div>
  <table>
    <thead><tr><th>#</th><th>Rule</th><th>Scope</th><th>Actual</th><th>Threshold</th><th>Usage</th><th>Message</th></tr></thead>
    <tbody>{unified_budget_rows}</tbody>
  </table>

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

  <h3>Cost Forecast & Anomaly Alerts</h3>
  <div style="font-size:12px;color:#6b7280;margin:-6px 0 8px;">Forecast windows: next 7/30 days + anomaly detection (z-score) + configurable alert rules.</div>
  <table>
    <thead><tr><th>Window</th><th>Predicted Total Cost</th><th>Predicted Avg Daily</th><th>Baseline Avg Daily</th><th>Trend / day</th></tr></thead>
    <tbody>
      <tr><td>Next 7 days</td><td>{usd(float(f7.get('predictedTotalCostUSD', 0.0)))}</td><td>{usd(float(f7.get('predictedAvgDailyCostUSD', 0.0)))}</td><td>{usd(float(f7.get('baselineAvgDailyCostUSD', 0.0)))}</td><td>{usd(float(f7.get('dailyTrendUSD', 0.0)))}</td></tr>
      <tr><td>Next 30 days</td><td>{usd(float(f30.get('predictedTotalCostUSD', 0.0)))}</td><td>{usd(float(f30.get('predictedAvgDailyCostUSD', 0.0)))}</td><td>{usd(float(f30.get('baselineAvgDailyCostUSD', 0.0)))}</td><td>{usd(float(f30.get('dailyTrendUSD', 0.0)))}</td></tr>
    </tbody>
  </table>
  <h4>Detected Cost Anomalies</h4>
  <table>
    <thead><tr><th>#</th><th>Date</th><th>Cost</th><th>Z-score</th><th>Severity</th></tr></thead>
    <tbody>{anomaly_rows}</tbody>
  </table>
  <h4>Triggered Alerts</h4>
  <div style="font-size:12px;color:#6b7280;margin:-6px 0 8px;">Notification channels: {esc(', '.join(alerts.get('notificationChannels', [])) if alerts.get('notificationChannels') else 'not configured')}</div>
  <table>
    <thead><tr><th>#</th><th>Rule</th><th>Severity</th><th>Message</th></tr></thead>
    <tbody>{alert_rows}</tbody>
  </table>

  <h3>Real-time Cost Control Strategy</h3>
  <div style="font-size:12px;color:#6b7280;margin:-6px 0 8px;">Multi-layer budget guardrails. Triggered actions can instruct router to degrade/switch/stop calls.</div>
  <table>
    <thead><tr><th>#</th><th>Layer</th><th>Scope</th><th>Metric</th><th>Value</th><th>Threshold</th><th>Triggered</th></tr></thead>
    <tbody>{cost_control_layer_rows}</tbody>
  </table>
  <h4>Triggered Control Actions</h4>
  <table>
    <thead><tr><th>#</th><th>Layer</th><th>Scope</th><th>Action</th><th>Route Model</th><th>Message</th></tr></thead>
    <tbody>{cost_control_action_rows}</tbody>
  </table>

  <h3>LLM Usage Pattern Deep Analysis</h3>
  <div style="font-size:12px;color:#6b7280;margin:-6px 0 8px;">Prompt/completion distribution, high-consumption hotspots, model efficiency, and anonymized prompt themes.</div>
  <table>
    <thead><tr><th>Metric</th><th>avg</th><th>p50</th><th>p95</th></tr></thead>
    <tbody>
      <tr><td>Prompt tokens</td><td>{float(prompt_stats.get('avg', 0.0)):.2f}</td><td>{float(prompt_stats.get('p50', 0.0)):.2f}</td><td>{float(prompt_stats.get('p95', 0.0)):.2f}</td></tr>
      <tr><td>Completion tokens</td><td>{float(completion_stats.get('avg', 0.0)):.2f}</td><td>{float(completion_stats.get('p50', 0.0)):.2f}</td><td>{float(completion_stats.get('p95', 0.0)):.2f}</td></tr>
    </tbody>
  </table>
  <table>
    <thead><tr><th>#</th><th>Model</th><th>Cost</th><th>Tokens</th><th>Calls</th></tr></thead>
    <tbody>{pattern_model_rows}</tbody>
  </table>
  <h4>By Model Type</h4>
  <table>
    <thead><tr><th>#</th><th>Model Type</th><th>Cost</th><th>Tokens</th><th>Calls</th></tr></thead>
    <tbody>{pattern_model_type_rows}</tbody>
  </table>
  <h4>By Project</h4>
  <table>
    <thead><tr><th>#</th><th>Project</th><th>Cost</th><th>Tokens</th><th>Calls</th></tr></thead>
    <tbody>{pattern_project_rows}</tbody>
  </table>
  <h4>By Task/Use Case</h4>
  <table>
    <thead><tr><th>#</th><th>Task</th><th>Cost</th><th>Tokens</th><th>Calls</th></tr></thead>
    <tbody>{pattern_task_rows}</tbody>
  </table>
  <h4>By User</h4>
  <table>
    <thead><tr><th>#</th><th>User</th><th>Cost</th><th>Tokens</th><th>Calls</th></tr></thead>
    <tbody>{pattern_user_rows}</tbody>
  </table>
  <h4>Hotspots · Top API Calls</h4>
  <table>
    <thead><tr><th>#</th><th>Model/Call</th><th>Cost</th><th>Tokens</th></tr></thead>
    <tbody>{top_api_rows}</tbody>
  </table>
  <h4>Hotspots · Top Sessions</h4>
  <table>
    <thead><tr><th>#</th><th>Session</th><th>Cost</th><th>Tokens</th></tr></thead>
    <tbody>{top_session_rows}</tbody>
  </table>
  <h4>Hotspots · Top Workflows</h4>
  <table>
    <thead><tr><th>#</th><th>Workflow</th><th>Cost</th><th>Tokens</th></tr></thead>
    <tbody>{top_workflow_rows}</tbody>
  </table>
  <h4>Model Efficiency (Cost/Token + Latency)</h4>
  <table>
    <thead><tr><th>#</th><th>Model</th><th>Cost</th><th>Tokens</th><th>Cost / 1K tokens (USD)</th><th>Avg Latency (ms)</th></tr></thead>
    <tbody>{pattern_eff_rows}</tbody>
  </table>
  <h4>Anonymized Prompt Keywords</h4>
  <table>
    <thead><tr><th>#</th><th>Keyword</th><th>Count</th></tr></thead>
    <tbody>{keyword_rows}</tbody>
  </table>

  <h3>Cost Attribution & Optimization Recommendations</h3>
  <div style="font-size:12px;color:#6b7280;margin:-6px 0 8px;">Attribute spend by project/department/user/application and provide actionable optimization suggestions. Cloud cost integration hooks are reserved for AWS Cost Explorer/GCP Billing.</div>
  <h4>Attribution by Project</h4>
  <table>
    <thead><tr><th>#</th><th>Project</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_project_rows}</tbody>
  </table>
  <h4>Attribution by Department</h4>
  <table>
    <thead><tr><th>#</th><th>Department</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_dept_rows}</tbody>
  </table>
  <h4>Attribution by User</h4>
  <table>
    <thead><tr><th>#</th><th>User</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_user_rows}</tbody>
  </table>
  <h4>Attribution by Application</h4>
  <table>
    <thead><tr><th>#</th><th>Application</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_app_rows}</tbody>
  </table>
  <h4>Attribution by Business Line</h4>
  <table>
    <thead><tr><th>#</th><th>Business Line</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_business_line_rows}</tbody>
  </table>
  <h4>Attribution by Model (Detailed)</h4>
  <table>
    <thead><tr><th>#</th><th>Model</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_model_rows}</tbody>
  </table>
  <h4>Attribution by Workflow (Detailed)</h4>
  <table>
    <thead><tr><th>#</th><th>Workflow</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_workflow_rows}</tbody>
  </table>
  <h4>Cloud Attribution by Provider</h4>
  <table>
    <thead><tr><th>#</th><th>Provider</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_cloud_provider_rows}</tbody>
  </table>
  <h4>Cloud Attribution by Service</h4>
  <table>
    <thead><tr><th>#</th><th>Provider:Service</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_cloud_service_rows}</tbody>
  </table>
  <h4>Cloud Attribution by Tag</h4>
  <table>
    <thead><tr><th>#</th><th>Tag</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_cloud_tag_rows}</tbody>
  </table>
  <h4>Cloud Attribution by Project</h4>
  <table>
    <thead><tr><th>#</th><th>Project</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_cloud_project_rows}</tbody>
  </table>
  <h4>Cloud Attribution by Source</h4>
  <table>
    <thead><tr><th>#</th><th>Source</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_cloud_source_rows}</tbody>
  </table>
  <h4>Cloud Attribution by Environment</h4>
  <table>
    <thead><tr><th>#</th><th>Environment</th><th>Cost</th><th>Tokens</th><th>Calls</th><th>Share</th></tr></thead>
    <tbody>{attr_cloud_env_rows}</tbody>
  </table>
  <h4>Optimization Recommendations</h4>
  <table>
    <thead><tr><th>#</th><th>Priority</th><th>Recommendation</th><th>Rationale</th><th>Est. Savings</th><th>Actions</th></tr></thead>
    <tbody>{rec_rows}</tbody>
  </table>

  <h4>Prompt 優化建議引擎 · High-Consumption Prompt Families</h4>
  <table>
    <thead><tr><th>#</th><th>Template Signature</th><th>Top Project</th><th>Top Model</th><th>Calls</th><th>Total Cost</th><th>Avg Prompt Tokens</th><th>Suggestions</th></tr></thead>
    <tbody>{prompt_family_rows}</tbody>
  </table>
  <h4>Prompt 優化建議引擎 · A/B Testing Plans</h4>
  <table>
    <thead><tr><th>#</th><th>Test ID</th><th>Scope</th><th>Variants</th><th>Metrics</th><th>Success Criteria</th></tr></thead>
    <tbody>{ab_test_rows}</tbody>
  </table>

  <h3>Budget Allocation & Permission Management</h3>
  <table>
    <thead><tr><th>#</th><th>Dimension</th><th>Key</th><th>Budget</th><th>Actual</th><th>Usage</th><th>Remaining</th><th>Status</th></tr></thead>
    <tbody>{budget_alloc_rows}</tbody>
  </table>
  <h4>Role Permission Matrix</h4>
  <table>
    <thead><tr><th>#</th><th>Role</th><th>Allowed Models</th><th>Max Cost / Call (USD)</th></tr></thead>
    <tbody>{role_rows}</tbody>
  </table>
  <h4>User Permission Overrides</h4>
  <table>
    <thead><tr><th>#</th><th>User</th><th>Role</th><th>Allowed Models</th><th>Max Cost / Call (USD)</th></tr></thead>
    <tbody>{user_perm_rows}</tbody>
  </table>
  <h4>Permission Violations (Detected from call logs)</h4>
  <table>
    <thead><tr><th>#</th><th>User</th><th>Role</th><th>Model</th><th>Violation</th><th>Calls</th><th>Cost</th></tr></thead>
    <tbody>{permission_violation_rows}</tbody>
  </table>

  <h3>Overage Handling</h3>
  <table>
    <thead><tr><th>#</th><th>Dimension</th><th>Key</th><th>Usage</th><th>Action</th><th>Route To Model</th><th>Message</th></tr></thead>
    <tbody>{overage_rows}</tbody>
  </table>

  <h3>Dashboard Policy View</h3>
  <div style="font-size:12px;color:#6b7280;margin:-6px 0 8px;">Quota policies={int(quota_summary.get('allocationPolicies', 0))} · Overage policies={int(quota_summary.get('overagePolicies', 0))} · Auto-enforced={int(quota_summary.get('autoHandledEvents', 0))} · Permission violations={int(quota_summary.get('permissionViolations', 0))}</div>
  <table>
    <thead><tr><th>#</th><th>Policy ID</th><th>Scope</th><th>Budget</th><th>Used</th><th>Usage</th><th>Status</th></tr></thead>
    <tbody>{quota_alloc_rows}</tbody>
  </table>
  <h4>Auto Enforcement Actions</h4>
  <table>
    <thead><tr><th>#</th><th>Allocation</th><th>Scope</th><th>Action</th><th>Route To Model</th><th>Message</th></tr></thead>
    <tbody>{quota_enforcement_rows}</tbody>
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

    function escapeHtml(value) {{
      return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

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
        return `<tr class="${{rowClass}}"><td>${{i + 1}}</td><td>${{escapeHtml(r.model)}}</td><td>$${{(r.costUSD || 0).toFixed(2)}}</td><td>${{share}}%</td><td class="${{dodClass}}">${{dodText}}</td><td class="${{dodPctClass}}">${{dodPctText}}</td></tr>`;
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
        return `<tr><td>${{escapeHtml(r.period)}}</td><td>${{total}}</td><td>${{active}}</td><td>${{avg}}</td></tr>`;
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
        modelFilters.innerHTML = allModels.map((m) => `<label style="display:block;font-size:12px;"><input type="checkbox" class="modelOpt" value="${{escapeHtml(m)}}"/> ${{escapeHtml(m)}}</label>`).join('');
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

    canvas.addEventListener('mousemove', (ev) => {{
      if (!labels.length) return;
      const i = nearestIndex(ev.clientX);
      const names = Object.keys(series);
      const rows = names.map(n => `${{n}}: $${{(series[n][i] || 0).toFixed(2)}}`).join('\n');
      const spike = spikeByDate[labels[i]];
      const spikeLine = spike
        ? `\n⚠ Spike: $${{(spike.costUSD || 0).toFixed(2)}} / baseline $${{(spike.baselineUSD || 0).toFixed(2)}} (${{(spike.ratio || 0).toFixed(2)}}x)`
        : '';
      tip.textContent = `${{labels[i]}}\nTotal: $${{(dayTotals[i] || 0).toFixed(2)}}${{spikeLine}}\n${{rows}}`;
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
    if (!accessPolicy.canViewPatternAnalysis) {{
      hideSectionByHeading('LLM Usage Pattern Deep Analysis', 'You do not have permission to view deep usage pattern analysis.');
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
    parser.add_argument("--aggregate-providers", help="Comma-separated providers for unified aggregation view (e.g. codex,claude)")
    parser.add_argument("--input", help="Path to codexbar JSON (or '-' for stdin)")
    parser.add_argument("--days", type=positive_int, help="Limit to last N days")
    parser.add_argument("--top-models", type=positive_int, default=6, help="Top models to chart")
    parser.add_argument("--spike-lookback-days", type=positive_int, default=7, help="Lookback window (days) for spike baseline")
    parser.add_argument("--spike-threshold-mult", type=positive_float, default=2.0, help="Spike threshold multiplier vs baseline")
    parser.add_argument("--output", default="token_usage_dashboard.html", help="Output HTML file path")
    parser.add_argument("--summary-json", help="Also write summary JSON to this path")
    parser.add_argument("--custom-report-json", help="Write custom report JSON to this path")
    parser.add_argument("--report-metrics", default="total_cost", help="Comma-separated: total_cost,active_models,avg_cost_per_model")
    parser.add_argument("--report-models", help="Comma-separated model filter (blank means all models)")
    parser.add_argument("--report-granularity", choices=["daily", "weekly", "monthly"], default="daily", help="Custom report time granularity")
    parser.add_argument("--max-table-rows", type=positive_int, default=120, help="Render at most N model rows in summary tables")
    parser.add_argument("--chart-max-points", type=positive_int, default=1200, help="Downsample chart to at most N points for large datasets")
    parser.add_argument("--alert-config", help="Path to alert rules/notification channels JSON")
    parser.add_argument("--cost-control-config", help="Path to real-time cost control policy JSON")
    parser.add_argument("--budget-config", help="Path to budget allocation / permission / overage policy JSON")
    parser.add_argument("--prompt-optimization-config", help="Path to prompt optimization engine JSON config")
    parser.add_argument("--cloud-cost-input", help="Path to cloud cost JSON (normalized records / AWS Cost Explorer / GCP billing export)")
    parser.add_argument("--cloud-tag-mapping-config", help="Path to cloud tag mapping JSON ({tagName: targetDimension})")
    parser.add_argument("--attribution-granularity", choices=["standard", "detailed"], default="standard", help="Cost attribution granularity")
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
    parser.add_argument("--report-scheduler-config", help="Path to report scheduler JSON config")
    parser.add_argument("--run-report-scheduler", action="store_true", help="Run report automation jobs and update report history center")
    parser.add_argument("--report-output-dir", default="report_center", help="Directory for generated scheduled reports/history")
    parser.add_argument("--scheduler-now", help="Override scheduler time (ISO8601, UTC recommended)")
    parser.add_argument("--run-event-monitor", action="store_true", help="Evaluate alerts/cost-controls/overage and dispatch webhook notifications")
    parser.add_argument("--event-output-json", help="Write event monitor result JSON to this path")
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

    if args.run_report_scheduler and not args.report_scheduler_config:
        eprint("--report-scheduler-config is required with --run-report-scheduler")
        return 8

    try:
        aggregate_providers = parse_provider_list(args.aggregate_providers, default=[])
        load_providers = [args.provider] + [p for p in aggregate_providers if p != args.provider]
        payload_bundle = load_payload_bundle(args.input, load_providers)
        payload = payload_bundle[args.provider]
    except Exception as exc:
        eprint(str(exc))
        return 1

    if args.run_report_scheduler:
        try:
            scheduler_config = json.loads(Path(args.report_scheduler_config).read_text(encoding="utf-8"))
            now_dt = datetime.fromisoformat(args.scheduler_now.replace("Z", "+00:00")) if args.scheduler_now else None
            scheduler_result = run_report_scheduler(
                payload=payload,
                provider=args.provider,
                config=scheduler_config if isinstance(scheduler_config, dict) else {},
                output_dir=Path(args.report_output_dir),
                tenant_config_path=args.tenant_config,
                now=now_dt,
            )
        except Exception as exc:
            eprint(f"Failed to run report scheduler: {exc}")
            return 9
        print(json.dumps(scheduler_result, ensure_ascii=False, indent=2))
        return 0

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
                allow_role_override=args.allow_role_override,
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

    try:
        alert_config = _load_alert_config(args.alert_config)
    except Exception as exc:
        eprint(f"Failed to load alert config: {exc}")
        return 7

    try:
        cost_control_config = _load_cost_control_config(args.cost_control_config)
    except Exception as exc:
        eprint(f"Failed to load cost control config: {exc}")
        return 10

    try:
        budget_config = _load_budget_config(args.budget_config)
    except Exception as exc:
        eprint(f"Failed to load budget config: {exc}")
        return 11

    try:
        prompt_optimization_config = _load_prompt_optimization_config(args.prompt_optimization_config)
    except Exception as exc:
        eprint(f"Failed to load prompt optimization config: {exc}")
        return 12

    try:
        cloud_tag_mapping = _load_cloud_tag_mapping_config(args.cloud_tag_mapping_config)
        cloud_cost_rows = load_cloud_cost_rows(args.cloud_cost_input, tag_mapping=cloud_tag_mapping)
    except Exception as exc:
        eprint(f"Failed to load cloud cost input: {exc}")
        return 13

    multi_provider_agg: Optional[Dict[str, Any]] = None
    if aggregate_providers:
        agg_rows: Dict[str, List[Dict[str, Any]]] = {}
        for p in aggregate_providers:
            provider_payload = payload_bundle.get(p)
            if not isinstance(provider_payload, dict):
                continue
            agg_rows[p] = filter_days(parse_daily(provider_payload), args.days)
        multi_provider_agg = build_multi_provider_aggregation(agg_rows)

    if args.run_event_monitor:
        summary = build_summary(
            args.provider,
            rows,
            spike_lookback_days=args.spike_lookback_days,
            spike_threshold_mult=args.spike_threshold_mult,
            alert_config=alert_config,
            cost_control_config=cost_control_config,
            budget_config=budget_config,
            prompt_optimization_config=prompt_optimization_config,
            cloud_cost_rows=cloud_cost_rows,
            attribution_granularity=args.attribution_granularity,
        )
        dispatch_cfg = alert_config.get("dispatch") if isinstance(alert_config.get("dispatch"), dict) else {}
        dispatch_timeout = max(1.0, _safe_float(dispatch_cfg.get("timeoutSeconds"), default=8.0))
        dispatch_retries = _safe_int(dispatch_cfg.get("retries"), default=0, minimum=0)
        event_result = dispatch_event_alerts(
            summary,
            alert_config=alert_config,
            timeout_seconds=dispatch_timeout,
            retries=dispatch_retries,
        )
        payload_out = {
            "provider": args.provider,
            "generatedAt": datetime.now(ZoneInfo("UTC")).isoformat(),
            "summary": {
                "startDate": summary.get("startDate"),
                "endDate": summary.get("endDate"),
                "totalCostUSD": summary.get("totalCostUSD"),
            },
            "alerts": summary.get("alerts"),
            "realTimeCostControls": summary.get("realTimeCostControls"),
            "overageBehaviors": summary.get("overageBehaviors"),
            "unifiedBudgetAlerts": summary.get("unifiedBudgetAlerts"),
            "dispatch": event_result,
        }
        if args.event_output_json:
            Path(args.event_output_json).write_text(json.dumps(payload_out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload_out, ensure_ascii=False, indent=2))
        return 0


    html = build_dashboard_html(
        args.provider,
        rows,
        top_models=args.top_models,
        spike_lookback_days=args.spike_lookback_days,
        spike_threshold_mult=args.spike_threshold_mult,
        max_table_rows=args.max_table_rows,
        chart_max_points=args.chart_max_points,
        role_name=role_name,
        policy=policy,
        alert_config=alert_config,
        cost_control_config=cost_control_config,
        multi_provider_agg=multi_provider_agg,
        budget_config=budget_config,
        prompt_optimization_config=prompt_optimization_config,
        cloud_cost_rows=cloud_cost_rows,
        attribution_granularity=args.attribution_granularity,
    )
    out = Path(args.output)
    out.write_text(html, encoding="utf-8")

    if args.summary_json:
        summary = build_summary(
            args.provider,
            rows,
            spike_lookback_days=args.spike_lookback_days,
            spike_threshold_mult=args.spike_threshold_mult,
            alert_config=alert_config,
            cost_control_config=cost_control_config,
            budget_config=budget_config,
            prompt_optimization_config=prompt_optimization_config,
            cloud_cost_rows=cloud_cost_rows,
            attribution_granularity=args.attribution_granularity,
        )
        summary["role"] = role_name
        summary["policy"] = policy
        if tenant_meta:
            summary["tenant"] = tenant_meta
        if multi_provider_agg:
            summary["multiProviderAggregation"] = multi_provider_agg
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

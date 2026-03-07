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
    breakdowns = row.get("modelBreakdowns")
    if not isinstance(breakdowns, list):
        return 0.0
    total = 0.0
    for b in breakdowns:
        if isinstance(b, dict) and isinstance(b.get("cost"), (int, float)):
            total += float(b["cost"])
    return total


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
    }


def build_dashboard_html(
    provider: str,
    rows: List[Dict[str, Any]],
    top_models: int,
    spike_lookback_days: int = 7,
    spike_threshold_mult: float = 2.0,
) -> str:
    totals = model_totals(rows)
    grand_total = sum(day_total_cost(r) for r in rows)
    labels, series, day_totals = prepare_chart_series(rows, top_models=top_models)
    latest_day = rows[-1]["date"] if rows else "—"
    last_7d = sum(day_totals[-7:]) if day_totals else 0.0
    prev_7d = sum(day_totals[-14:-7]) if len(day_totals) > 7 else 0.0
    if prev_7d > 0:
        trend = f"{((last_7d - prev_7d) / prev_7d * 100.0):+.1f}%"
    else:
        trend = "N/A"

    models_ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    table_rows = "\n".join(
        f"<tr><td>{idx}</td><td>{m}</td><td>{usd(c)}</td><td>{(c / grand_total * 100 if grand_total else 0):.1f}%</td></tr>"
        for idx, (m, c) in enumerate(models_ranked, start=1)
    )

    summary = build_summary(
        provider,
        rows,
        spike_lookback_days=spike_lookback_days,
        spike_threshold_mult=spike_threshold_mult,
    )
    spike_count = len(summary.get("spikes", []))

    movers_rows_parts: List[str] = []
    for idx, x in enumerate(summary.get("movers", [])[:8], start=1):
        pct = f"{x['deltaPct']:+.1f}%" if isinstance(x.get("deltaPct"), (int, float)) else "N/A"
        movers_rows_parts.append(
            f"<tr><td>{idx}</td><td>{x['model']}</td><td>{usd(float(x['last7dCostUSD']))}</td><td>{usd(float(x['prev7dCostUSD']))}</td><td>{usd(float(x['deltaCostUSD']))}</td><td>{pct}</td></tr>"
        )
    movers_rows = "\n".join(movers_rows_parts)

    spikes_rows_parts: List[str] = []
    for idx, x in enumerate(summary.get("spikes", [])[:10], start=1):
        date_key = str(x["date"])
        spikes_rows_parts.append(
            f"<tr id='spike-row-{date_key}'><td>{idx}</td><td>{x['date']}</td><td>{usd(float(x['costUSD']))}</td><td>{usd(float(x['baselineUSD']))}</td><td>{x['ratio']:.2f}x</td></tr>"
        )
    spikes_rows = "\n".join(spikes_rows_parts) if spikes_rows_parts else "<tr><td colspan='5'>No spikes detected</td></tr>"

    spike_by_date: Dict[str, Dict[str, float]] = {}
    for s in summary.get("spikes", []):
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
    json_spike_by_date = json.dumps(spike_by_date)
    json_day_breakdown_by_date = json.dumps(day_breakdown_by_date)
    json_day_total_by_date = json.dumps(day_total_by_date)

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>Token Usage Dashboard · {provider}</title>
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
  <h2>Token Usage Dashboard · {provider}</h2>
  <div style="color:#6b7280;font-size:12px;margin-bottom:6px;">Tips: click chart points/spikes to focus a day · use ←/→ or j/k to step dates · n/p jump next/prev spike · s toggle spike-only · r reset to latest · c copy link · ? help</div>
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap;">
    <label style="display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#374151;">
      <input type="checkbox" id="spikeOnlyToggle" />
      Spike-only navigation (shareable via URL hash)
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

  <h3>Model Breakdown</h3>
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

  <div id="kbdHelp" class="kbd-help">
    <div><strong>Keyboard shortcuts</strong></div>
    <div><code>←/→</code> or <code>j/k</code>: step day</div>
    <div><code>n/p</code>: next/prev spike</div>
    <div><code>s</code>: toggle spike-only mode</div>
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
    const copyLinkBtn = document.getElementById('copyLinkBtn');
    const copyLinkHint = document.getElementById('copyLinkHint');
    const kbdHelp = document.getElementById('kbdHelp');
    let selectedSpikeDate = null;
    let selectedDate = null;
    let spikeOnlyMode = false;
    let sortByDodMode = false;

    function renderSelectedDay(date) {{
      const baseRows = dayBreakdownByDate[date] || [];
      const idx = labels.indexOf(date);
      const prevDate = idx > 0 ? labels[idx - 1] : null;
      const prevRows = prevDate ? (dayBreakdownByDate[prevDate] || []) : [];
      const prevMap = Object.fromEntries(prevRows.map(r => [r.model, r.costUSD || 0]));
      const rows = baseRows.map(r => ({{
        ...r,
        dod: (r.costUSD || 0) - (prevMap[r.model] || 0),
      }}));
      if (sortByDodMode) rows.sort((a, b) => (b.dod || 0) - (a.dod || 0));
      const currTotal = dayTotalByDate[date] || baseRows.reduce((acc, r) => acc + (r.costUSD || 0), 0);
      const prevTotal = prevDate ? (dayTotalByDate[prevDate] || 0) : 0;
      const totalDelta = currTotal - prevTotal;
      const totalDeltaPct = prevTotal > 0 ? ((totalDelta / prevTotal) * 100) : null;
      const totalDeltaText = `${{totalDelta >= 0 ? '+' : ''}}$${{totalDelta.toFixed(2)}}`;
      const totalDeltaPctText = totalDeltaPct === null ? 'N/A' : `${{totalDeltaPct >= 0 ? '+' : ''}}${{totalDeltaPct.toFixed(1)}}%`;
      selectedDayTitle.textContent = `Selected Day Model Breakdown · ${{date}} · DoD ${{totalDeltaText}} (${{totalDeltaPctText}})`;
      if (selectedDayMeta) selectedDayMeta.textContent = `Day total: $${{currTotal.toFixed(2)}} · Previous day total: $${{prevTotal.toFixed(2)}}`;
      if (!rows.length) {{
        selectedDayBody.innerHTML = '<tr><td colspan="6">No model breakdown on this day</td></tr>';
        return;
      }}
      const total = rows.reduce((acc, r) => acc + (r.costUSD || 0), 0);
      selectedDayBody.innerHTML = rows.map((r, i) => {{
        const share = total > 0 ? (((r.costUSD || 0) / total) * 100).toFixed(1) : '0.0';
        const prev = prevMap[r.model] || 0;
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
      if (!raw) return {{ date: null, spikeOnly: false, sortDod: false }};
      const p = new URLSearchParams(raw);
      const date = p.get('date');
      const spikeOnly = p.get('spikeOnly') === '1';
      const sortDod = p.get('sortDod') === '1';
      return {{
        date: date && labels.includes(date) ? date : null,
        spikeOnly,
        sortDod,
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
    if (spikeOnlyToggle) spikeOnlyToggle.checked = spikeOnlyMode;
    if (sortByDodToggle) sortByDodToggle.checked = sortByDodMode;

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
      if (spikeOnlyToggle) spikeOnlyToggle.checked = false;
      focusDate(labels[labels.length - 1]);
    }}

    window.addEventListener('keydown', (ev) => {{
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
      if (ev.key === 's') {{
        ev.preventDefault();
        toggleSpikeOnlyMode();
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
    parser.add_argument("--output", default="token_usage_dashboard.html", help="Output HTML file path")
    parser.add_argument("--summary-json", help="Also write summary JSON to this path")
    parser.add_argument("--open", action="store_true", help="Open dashboard in default browser")
    args = parser.parse_args()

    try:
        payload = load_payload(args.input, args.provider)
    except Exception as exc:
        eprint(str(exc))
        return 1

    rows = filter_days(parse_daily(payload), args.days)
    if not rows:
        eprint("No daily rows found in payload.")
        return 2

    html = build_dashboard_html(
        args.provider,
        rows,
        top_models=args.top_models,
        spike_lookback_days=args.spike_lookback_days,
        spike_threshold_mult=args.spike_threshold_mult,
    )
    out = Path(args.output)
    out.write_text(html, encoding="utf-8")

    if args.summary_json:
        summary = build_summary(
            args.provider,
            rows,
            spike_lookback_days=args.spike_lookback_days,
            spike_threshold_mult=args.spike_threshold_mult,
        )
        Path(args.summary_json).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    resolved = str(out.resolve())
    print(resolved)

    if args.open:
        webbrowser.open(out.resolve().as_uri())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

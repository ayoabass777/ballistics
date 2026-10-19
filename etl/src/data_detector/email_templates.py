"""HTML + plain-text emails for the daily data flow check.

One email per day (when the daily report is on): the subject and header change
with the day's state -- all clear, warnings, failed, recovered -- and failures
lead the body. Built with inline styles and tables only, which is what Gmail,
Apple Mail and Outlook render consistently. Every value from the database is
HTML-escaped (task errors can contain anything).
"""
from __future__ import annotations

from datetime import date
from html import escape

from etl.src.data_detector.datasets import effective_severity

# Palette: Ballistics lime accent on neutral ink. Adjust here only.
INK = "#111418"
MUTED = "#5B6470"
LINE = "#E4E7EB"
PAGE = "#F4F5F7"
CARD = "#FFFFFF"
LIME = "#C6F135"
STATUS = {                     # (dot, soft background, label colour)
    "ok": ("#22A06B", "#E7F6EF", "#14653F"),
    "warning": ("#E2A400", "#FFF6DB", "#7A5900"),
    "failed": ("#E5484D", "#FDECEC", "#9B1C20"),
    "missing": ("#E5484D", "#FDECEC", "#9B1C20"),
    "blocked": ("#8B95A1", "#EEF0F2", "#4A535E"),
    "none": ("#D3D8DE", "#F4F5F7", "#8B95A1"),
}
LAYER_NAMES = {"source": "Source", "raw": "Raw", "dbt": "dbt", "engine": "Engine", "api": "API & assets"}
LAYER_ORDER = ["source", "raw", "dbt", "engine", "api"]
FONT = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"


def day_state(results: dict, registry, resolved: list[str]) -> str:
    bad = [k for k, v in results.items() if v["status"] in ("failed", "missing")]
    errors = [k for k in bad if effective_severity(k, results, registry) != "warning"]
    if errors:
        return "failed"
    if bad:
        return "warning"
    if resolved:
        return "recovered"
    return "ok"


HEADLINES = {
    "ok": ("All clear", "Data flowed through every stage."),
    "warning": ("Warnings", "Data flowed, with something to look at."),
    "failed": ("Data did not flow", "At least one stage failed or is missing."),
    "recovered": ("Recovered", "Earlier failures are flowing again."),
}


def subject_line(ds: date, state: str, results: dict, registry) -> str:
    bad = [k for k, v in results.items() if v["status"] in ("failed", "missing")]
    blocked = sum(v["status"] == "blocked" for v in results.values())
    warns = [k for k in bad if effective_severity(k, results, registry) == "warning"]
    if state == "failed":
        extra = f", {len(warns)} warning{'s' if len(warns) != 1 else ''}" if warns else ""
        return f"✗ Ballistics {ds}: {len(bad) - len(warns)} failed{extra}, {blocked} blocked"
    if state == "warning":
        return f"! Ballistics {ds}: {len(bad)} warning{'s' if len(bad) != 1 else ''}"
    if state == "recovered":
        return f"✓ Ballistics {ds}: recovered"
    return f"✓ Ballistics {ds}: all clear"


def _status_of(dataset_id: str, r: dict, registry) -> str:
    if r["status"] in ("failed", "missing") and (r.get("severity") or registry[dataset_id].check.get("severity")) == "warning":
        return "warning"
    return r["status"]


def _pill(text: str, status: str) -> str:
    dot, bg, fg = STATUS[status]
    return (f'<span style="display:inline-block;padding:3px 10px;border-radius:999px;'
            f'background:{bg};color:{fg};font-size:12px;font-weight:600;line-height:18px;">'
            f'<span style="color:{dot};">&#9679;</span>&nbsp;{escape(text)}</span>')


def _kpi(label: str, value: str, note: str = "") -> str:
    note_html = (f'<div style="font-size:12px;color:{MUTED};margin-top:2px;">{escape(note)}</div>'
                 if note else "")
    return (f'<td width="23%" valign="top" style="padding:14px 12px;border:1px solid {LINE};'
            f'border-radius:10px;background:{CARD};">'
            f'<div style="font-size:12px;color:{MUTED};">{escape(label)}</div>'
            f'<div style="font-size:22px;font-weight:700;color:{INK};margin-top:4px;">{escape(value)}</div>'
            f'{note_html}</td>')


def _fmt(n) -> str:
    return "—" if n is None else f"{int(n):,}"


def _history_strip(history: list[dict]) -> str:
    cells = []
    for h in history:
        status = h["status"]
        dot, bg, _ = STATUS["ok" if status == "ok" else status if status in STATUS else "none"]
        cells.append(
            f'<td align="center" style="padding:0 3px;">'
            f'<div style="width:34px;height:34px;border-radius:8px;background:{bg};'
            f'border:1px solid {dot};line-height:34px;font-size:14px;color:{dot};">'
            f'{"&#10003;" if status == "ok" else "&#9679;" if status != "none" else "&middot;"}</div>'
            f'<div style="font-size:11px;color:{MUTED};margin-top:4px;">{escape(h["label"])}</div></td>')
    return f'<table role="presentation" cellpadding="0" cellspacing="0"><tr>{"".join(cells)}</tr></table>'


def _issue_block(dataset_id: str, r: dict, registry, results: dict) -> str:
    d = registry[dataset_id]
    status = _status_of(dataset_id, r, registry)
    dot, bg, fg = STATUS[status]
    producer = f"{d.producer_dag}.{d.producer_task}" if d.producer_dag else "—"
    rows = "".join(
        f'<tr><td style="padding:2px 12px 2px 0;font-size:12px;color:{MUTED};white-space:nowrap;" valign="top">'
        f'{escape(str(k))}</td><td style="padding:2px 0;font-size:12px;color:{INK};'
        f'font-family:Menlo,Consolas,monospace;word-break:break-word;">{escape(str(v))}</td></tr>'
        for k, v in r["details"].items())
    blocked = [x for x in registry.downstream(dataset_id) if results.get(x, {}).get("status") == "blocked"]
    blocks_html = (f'<div style="font-size:12px;color:{MUTED};margin-top:8px;">Blocks: '
                   f'{escape(", ".join(blocked))}</div>' if blocked else "")
    return (f'<tr><td style="padding:0 0 12px 0;"><table role="presentation" width="100%" cellpadding="0" '
            f'cellspacing="0" style="background:{bg};border-left:4px solid {dot};border-radius:8px;">'
            f'<tr><td style="padding:12px 14px;">'
            f'<div style="font-size:14px;font-weight:700;color:{fg};">{escape(dataset_id)} &middot; '
            f'{escape("warning" if status == "warning" else r["status"])}</div>'
            f'<div style="font-size:12px;color:{MUTED};margin:2px 0 8px;">{escape(d.description)} '
            f'&middot; producer {escape(producer)}</div>'
            f'<table role="presentation" cellpadding="0" cellspacing="0">{rows}</table>{blocks_html}'
            f'</td></tr></table></td></tr>')


def _layer_rows(results: dict, registry) -> str:
    out = []
    for layer in LAYER_ORDER:
        ids = [k for k, v in results.items() if v["layer"] == layer]
        if not ids:
            continue
        statuses = [_status_of(k, results[k], registry) for k in ids]
        ok = statuses.count("ok")
        worst = next((s for s in ("failed", "missing", "warning", "blocked") if s in statuses), "ok")
        parts = [f"{statuses.count(s)} {s}" for s in ("failed", "missing", "warning", "blocked")
                 if statuses.count(s)]
        label = " · ".join([f"{ok}/{len(ids)} ok"] + parts)
        out.append(
            f'<tr><td style="padding:10px 0;border-bottom:1px solid {LINE};font-size:14px;color:{INK};">'
            f'{escape(LAYER_NAMES[layer])}</td><td align="right" style="padding:10px 0;'
            f'border-bottom:1px solid {LINE};">{_pill(label, worst)}</td></tr>')
    return "".join(out)


def _pct(x) -> str:
    return "—" if x is None else f"{x:.0%}"


def _watch_lines(watch: dict | None) -> list[str]:
    """Plain-text Model watch block (also the source for the HTML rows)."""
    if not watch:
        return []
    lines = ["", "Model watch (tracking, not live rules)"]
    o = watch.get("over_1_5")
    if o:
        lines.append(f"  Match Over 1.5, refit {o['refit']}:")
        for r in o["rows"]:
            prev = (f", was {r['previous_overconfidence']*100:+.1f}"
                    if r["previous_overconfidence"] is not None else "")
            lines.append(f"    {r['source']}: n={r['n']:,} claimed {_pct(r['predicted'])} hit {_pct(r['hit'])} "
                         f"({r['overconfidence']*100:+.1f} pts{prev})" + (f"  FLAG {r['flag']}" if r["flag"] else ""))
    else:
        lines.append("  Match Over 1.5: no refit logged")
    b = watch.get("big_dogs")
    if b:
        y, n = b["confirmed"], b["not_confirmed"]
        lines.append("  Big dogs, Dixon-Coles cross-check (DC >= 65% for the favourite):")
        lines.append(f"    confirmed: {y['won']}/{y['n']} won {_pct(y['rate'])} (range {_pct(y['low'])}-{_pct(y['high'])})")
        lines.append(f"    not confirmed: {n['won']}/{n['n']} won {_pct(n['rate'])} (range {_pct(n['low'])}-{_pct(n['high'])})")
        lines.append(f"    {b['verdict']}")
        for d in b["day"][:10]:
            lines.append(f"    {d['match']} {d['score']} ({d['result']}), DC {_pct(d['dc'])}, gap {d['gap']}")
    else:
        lines.append("  Big dogs cross-check: no log yet")
    return lines


def _watch_html(watch: dict | None) -> str:
    if not watch:
        return ""
    def row(label, value, status="ok"):
        dot = STATUS[status][0]
        return (f'<tr><td style="padding:8px 0;border-bottom:1px solid {LINE};font-size:13px;color:{INK};">'
                f'<span style="color:{dot};">&#9679;</span>&nbsp;{escape(label)}</td>'
                f'<td align="right" style="padding:8px 0;border-bottom:1px solid {LINE};font-size:13px;'
                f'color:{INK};">{escape(value)}</td></tr>')
    rows = []
    o = watch.get("over_1_5")
    if o:
        for r in o["rows"]:
            prev = (f" (was {r['previous_overconfidence']*100:+.1f})"
                    if r["previous_overconfidence"] is not None else "")
            rows.append(row(f"Over 1.5 · {r['source']}",
                            f"claimed {_pct(r['predicted'])} · hit {_pct(r['hit'])} · "
                            f"{r['overconfidence']*100:+.1f} pts{prev}",
                            "warning" if r["flag"] else "ok"))
    else:
        rows.append(row("Over 1.5", "no refit logged", "none"))
    b = watch.get("big_dogs")
    if b:
        y, n = b["confirmed"], b["not_confirmed"]
        rows.append(row("Big dogs · DC backs favourite ≥ 65%",
                        f"{y['won']}/{y['n']} won · {_pct(y['rate'])}"))
        rows.append(row("Big dogs · DC below 65%", f"{n['won']}/{n['n']} won · {_pct(n['rate'])}",
                        "blocked"))
        status = "warning" if b["verdict"].startswith("FLAG") else "none"
        rows.append(f'<tr><td colspan="2" style="padding:8px 0;font-size:12px;color:{STATUS[status][2]};">'
                    f'{escape(b["verdict"])}</td></tr>')
        for d in b["day"][:10]:
            st = "ok" if d["result"] == "won" else "failed"
            rows.append(row(f"{d['match']} {d['score']}", f"DC {_pct(d['dc'])} · gap {d['gap']}", st))
    else:
        rows.append(row("Big dogs cross-check", "no log yet", "none"))
    refit = f" · Over 1.5 refit {o['refit']}" if o else ""
    return (f'<tr><td style="padding:22px 28px 0;"><div style="font-size:13px;font-weight:700;'
            f'letter-spacing:.04em;text-transform:uppercase;color:{MUTED};margin-bottom:2px;">Model watch</div>'
            f'<div style="font-size:12px;color:{MUTED};margin-bottom:6px;">Tracking, not live rules{escape(refit)}</div>'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0">{"".join(rows)}</table>'
            f'</td></tr>')


def render(ds: date, results: dict, registry, *, metrics: dict, history: list[dict],
           resolved: list[str], watch: dict | None = None) -> tuple[str, str, str]:
    """(subject, plain text, html) for the day's email."""
    state = day_state(results, registry, resolved)
    subject = subject_line(ds, state, results, registry)
    title, sub = HEADLINES[state]
    pill_status = {"ok": "ok", "recovered": "ok", "warning": "warning", "failed": "failed"}[state]
    issues = [k for k, v in results.items() if v["status"] in ("failed", "missing")]
    total = len(results)
    ok = sum(v["status"] == "ok" for v in results.values())

    issues_html = ""
    if issues:
        issues_html = (f'<tr><td style="padding:24px 28px 4px;"><div style="font-size:13px;font-weight:700;'
                       f'letter-spacing:.04em;text-transform:uppercase;color:{MUTED};">Needs attention</div>'
                       f'</td></tr><tr><td style="padding:10px 28px 0;"><table role="presentation" width="100%" '
                       f'cellpadding="0" cellspacing="0">'
                       + "".join(_issue_block(k, results[k], registry, results) for k in issues)
                       + "</table></td></tr>")
    recovered_html = ""
    if resolved:
        recovered_html = (f'<tr><td style="padding:16px 28px 0;"><div style="font-size:13px;color:{STATUS["ok"][2]};'
                          f'background:{STATUS["ok"][1]};border-radius:8px;padding:10px 14px;">'
                          f'&#10003;&nbsp;Flowing again: {escape(", ".join(resolved))}</div></td></tr>')

    m = metrics
    gap = '<td width="2.66%" style="font-size:0;">&nbsp;</td>'
    kpis = gap.join([
        _kpi("Datasets ok", f"{ok}/{total}"),
        _kpi("Fixtures loaded", _fmt(m.get("fixtures_loaded")), "from API-Sports"),
        _kpi("Match pages", _fmt(m.get("match_pages")), m.get("engine_built") or ""),
        _kpi("Picks in pool", _fmt(m.get("pool_rows")), "consensus"),
    ])
    crest = m.get("crest_coverage")
    crest_line = (f'{crest[0]:,} of {crest[1]:,} upcoming teams have a crest' if crest else "Crest coverage unavailable")

    html = f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(subject)}</title></head>
<body style="margin:0;padding:0;background:{PAGE};">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:{PAGE};">
<tr><td align="center" style="padding:24px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:{CARD};border-radius:16px;overflow:hidden;font-family:{FONT};">
  <tr><td style="background:{INK};padding:18px 28px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="font-size:15px;font-weight:800;color:#FFFFFF;letter-spacing:.02em;">
        <span style="display:inline-block;width:10px;height:10px;border-radius:3px;background:{LIME};"></span>&nbsp; BALLISTICS</td>
      <td align="right" style="font-size:12px;color:#AEB6C0;">Daily data report</td>
    </tr></table></td></tr>
  <tr><td style="padding:26px 28px 6px;">
    {_pill(title, pill_status)}
    <div style="font-size:24px;font-weight:800;color:{INK};margin-top:12px;">{escape(str(ds))}</div>
    <div style="font-size:14px;color:{MUTED};margin-top:4px;">{escape(sub)} Pipeline day {escape(str(ds))} (Airflow logical date).</div>
  </td></tr>
  {recovered_html}
  {issues_html}
  <tr><td style="padding:22px 28px 0;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>{kpis}</tr></table>
  </td></tr>
  <tr><td style="padding:22px 28px 0;">
    <div style="font-size:13px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:{MUTED};margin-bottom:6px;">Pipeline</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0">{_layer_rows(results, registry)}</table>
  </td></tr>
  {_watch_html(watch)}
  <tr><td style="padding:22px 28px 0;">
    <div style="font-size:13px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:{MUTED};margin-bottom:10px;">Last 7 days</div>
    {_history_strip(history)}
  </td></tr>
  <tr><td style="padding:18px 28px 0;font-size:13px;color:{MUTED};">{escape(crest_line)}.</td></tr>
  <tr><td style="padding:24px 28px 26px;">
    <div style="border-top:1px solid {LINE};padding-top:14px;font-size:12px;color:{MUTED};line-height:18px;">
      Sent by the data flow check after the 09:00 UTC run. Details: <span style="font-family:Menlo,Consolas,monospace;">./scripts/detector_report.sh</span>
      &middot; history in <span style="font-family:Menlo,Consolas,monospace;">data_detector.daily_flow_checks</span>.</div>
  </td></tr>
</table></td></tr></table></body></html>"""

    lines = [subject, "", f"{title}: {sub}", ""]
    for k in issues:
        r, d = results[k], registry[k]
        producer = f"{d.producer_dag}.{d.producer_task}" if d.producer_dag else "-"
        lines.append(f"- {k}: {r['status']} (producer {producer})")
        lines += [f"    {kk}: {vv}" for kk, vv in r["details"].items()]
        blocked = [x for x in registry.downstream(k) if results.get(x, {}).get("status") == "blocked"]
        if blocked:
            lines.append(f"    blocks: {', '.join(blocked)}")
    if resolved:
        lines.append("Flowing again: " + ", ".join(resolved))
    lines += ["", f"Datasets ok: {ok}/{total}", f"Fixtures loaded: {_fmt(m.get('fixtures_loaded'))}",
              f"Match pages: {_fmt(m.get('match_pages'))}", f"Picks in pool: {_fmt(m.get('pool_rows'))}",
              crest_line,
              "Last 7 days: " + " ".join(f"{h['label']}:{h['status']}" for h in history)]
    lines += _watch_lines(watch)
    return subject, "\n".join(lines), html

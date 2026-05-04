#!/usr/bin/env python3
"""
Talkspirit Sales Dashboard — Generator
python generate.py  →  index.html
"""

import requests, json, os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
OWNER = "33612016"
TODAY = datetime.now(timezone.utc)
BASE  = "https://api.hubapi.com"
OUT   = Path(__file__).parent / "index.html"

def headers():
    return {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# ── Stage map ─────────────────────────────────────────────────────────────────
STAGE = {
    "3112501453": ("Discovery Scheduled",  10, "acq"),
    "3112501454": ("Discovery Complete",   20, "acq"),
    "3112501455": ("Solution Fit",         40, "acq"),
    "3112501456": ("Proposal Sent",        60, "acq"),
    "3112501457": ("Negotiation",          80, "acq"),
    "3112501458": ("Closed Won",          100, "won"),
    "3112501459": ("Closed Lost",           0, "lost"),
    "5137691842": ("No Decision",           0, "dead"),
    "462257366":  ("Upsell Identified",    20, "exp"),
    "462257390":  ("Negotiation AM",       60, "exp"),
    "462257370":  ("Contract Sent AM",     80, "exp"),
    "462257371":  ("Closed Won AM",       100, "won"),
    "462257372":  ("Closed Lost AM",        0, "lost"),
    "5134323928": ("Renewal Upcoming",     20, "exp"),
    "5134323929": ("Renewal Outreach",     40, "exp"),
    "5134323930": ("Renewal In Progress",  60, "exp"),
    "5134323931": ("Contract Sent",        80, "exp"),
    "5134323932": ("Renewed",             100, "won"),
    "5134323933": ("Churned",               0, "lost"),
    "5134323934": ("Downgraded",          100, "exp"),
}

ACQ_ORDER = ["3112501453","3112501454","3112501455","3112501456","3112501457"]
EXP_ORDER = ["462257366","462257390","462257370"]

PIPELINE_LABEL = {
    "2281936074": "New Biz",
    "284322551":  "AM",
    "3709248745": "Renewal",
}

# ── HubSpot fetchers ──────────────────────────────────────────────────────────
def fetch_deals():
    print("Fetching deals...")
    props = ["dealname","amount","dealstage","closedate","pipeline",
             "hubspot_owner_id","hs_lastmodifieddate","createdate"]
    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": OWNER}
        ]}],
        "properties": props,
        "sorts": [{"propertyName": "amount", "direction": "DESCENDING"}],
        "limit": 100
    }
    r = requests.post(f"{BASE}/crm/v3/objects/deals/search",
                      headers=headers(), json=payload)
    r.raise_for_status()
    deals = r.json().get("results", [])
    print(f"  → {len(deals)} deals found")
    return deals


def fetch_engagements(deal_id):
    """Returns (last_touch, next_step) for a deal via legacy engagements API."""
    try:
        r = requests.get(
            f"{BASE}/engagements/v1/engagements/associated/DEAL/{deal_id}/paged",
            headers=headers(),
            params={"count": 100}
        )
        if r.status_code != 200:
            return None, None
        items = r.json().get("results", [])

        last_email = last_call = None
        best_task  = None  # most recent non-completed task

        for item in items:
            eng   = item.get("engagement", {})
            meta  = item.get("metadata", {})
            etype = eng.get("type", "")
            ts    = eng.get("createdAt", 0)
            dt    = datetime.fromtimestamp(ts / 1000, timezone.utc) if ts else None

            if etype == "EMAIL" and dt:
                if last_email is None or dt > last_email["date"]:
                    subj = meta.get("subject") or meta.get("text", "")[:60] or "Email"
                    last_email = {"date": dt, "label": subj[:60], "channel": "email"}

            elif etype == "CALL" and dt:
                if last_call is None or dt > last_call["date"]:
                    dur_ms = meta.get("durationMilliseconds", 0) or 0
                    dur    = f"{int(dur_ms)//60000}min" if dur_ms else ""
                    label  = meta.get("title") or ("Appel " + dur if dur else "Appel")
                    last_call = {"date": dt, "label": label, "channel": "call"}

            elif etype == "TASK" and dt:
                status = meta.get("status", "NOT_STARTED")
                if status != "COMPLETED":
                    if best_task is None or dt > best_task["_dt"]:
                        due_ts  = eng.get("timestamp", 0)
                        due_dt  = datetime.fromtimestamp(due_ts / 1000, timezone.utc) if due_ts else None
                        best_task = {
                            "_dt":     dt,  # internal datetime for comparison
                            "subject":  meta.get("subject") or "Tâche",
                            "status":   status,
                            "due_date": due_dt.strftime("%d/%m") if due_dt else None,
                            "overdue":  (due_dt < TODAY) if due_dt else False,
                        }

        candidates = [x for x in [last_email, last_call] if x]
        last_touch = max(candidates, key=lambda x: x["date"]) if candidates else None
        if last_touch:
            last_touch["days_ago"] = (TODAY - last_touch["date"]).days
            last_touch["date"]     = last_touch["date"].strftime("%d/%m/%y")

        if best_task:
            best_task.pop("_dt", None)  # strip non-serializable datetime

        return last_touch, best_task

    except Exception as e:
        print(f"  Warning deal {deal_id}: {e}")
        return None, None


def enrich(deals):
    print(f"Enriching {len(deals)} deals (engagements + tasks)...")
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(fetch_engagements, d["id"]): d for d in deals}
        for fut in as_completed(futs):
            deal = futs[fut]
            lt, ns = fut.result()
            deal["last_touch"] = lt
            deal["next_step"]  = ns
    return deals


# ── Metrics ───────────────────────────────────────────────────────────────────
def compute_metrics(deals):
    acq = {s: {"count": 0, "mrr": 0} for s in ACQ_ORDER}
    exp = {s: {"count": 0, "mrr": 0} for s in EXP_ORDER}

    total_pipeline = total_weighted = 0
    alerts = {"fire": [], "warn": [], "no_step": [], "no_decision": []}

    for d in deals:
        sid    = d["properties"]["dealstage"]
        cat    = STAGE.get(sid, ("", 0, ""))[2]
        prob   = STAGE.get(sid, ("", 0, ""))[1]
        amount = float(d["properties"].get("amount") or 0)
        name   = d["properties"].get("dealname", "")
        did    = d["id"]

        if cat == "acq" and sid in acq:
            acq[sid]["count"] += 1
            acq[sid]["mrr"]   += amount
        elif cat == "exp" and sid in exp:
            exp[sid]["count"] += 1
            exp[sid]["mrr"]   += amount

        if cat in ("acq", "exp"):
            total_pipeline += amount
            total_weighted += amount * prob / 100

            lt = d.get("last_touch")
            days = lt["days_ago"] if lt else 999
            if days > 14:
                alerts["fire"].append({"name": name, "id": did, "amount": amount, "days": days})
            elif days > 7:
                alerts["warn"].append({"name": name, "id": did, "amount": amount, "days": days})

            if not d.get("next_step"):
                alerts["no_step"].append({"name": name, "id": did, "amount": amount})

            if sid == "5137691842":
                alerts["no_decision"].append({"name": name, "id": did, "amount": amount})

    return {
        "acq": acq,
        "exp": exp,
        "total_pipeline": round(total_pipeline),
        "total_weighted": round(total_weighted),
        "alerts": alerts,
        "generated_at": TODAY.strftime("%d/%m/%Y à %H:%M UTC"),
    }


# ── Deal serialisation ────────────────────────────────────────────────────────
def serialise_deals(deals):
    out = []
    for d in deals:
        sid    = d["properties"]["dealstage"]
        s_info = STAGE.get(sid, ("Unknown", 0, ""))
        cat    = s_info[2]
        if cat in ("won", "lost"):
            continue  # exclude closed from main table

        close_raw = d["properties"].get("closedate", "")
        close_dt  = None
        overdue   = False
        if close_raw:
            try:
                close_dt = datetime.fromisoformat(close_raw.replace("Z", "+00:00"))
                overdue  = close_dt < TODAY
            except:
                pass

        out.append({
            "id":       d["id"],
            "name":     d["properties"].get("dealname", ""),
            "amount":   float(d["properties"].get("amount") or 0),
            "stage":    s_info[0],
            "stage_id": sid,
            "prob":     s_info[1],
            "cat":      cat,
            "pipeline": PIPELINE_LABEL.get(d["properties"].get("pipeline", ""), ""),
            "close":    close_dt.strftime("%d/%m/%y") if close_dt else "",
            "overdue":  overdue,
            "hs_url":   f"https://app-eu1.hubspot.com/contacts/25761660/deal/{d['id']}",
            "last_touch": d.get("last_touch"),
            "next_step":  d.get("next_step"),
        })
    return out


# ── HTML template ─────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sales Dashboard — Eloi</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --brand:#F56220;--brand-light:#FFF5ED;--brand-100:#FEE9D6;
  --slate:#2E1F1C;--muted:#6A4B42;--light:#F0EBE4;--border:#E2D6CA;
  --cream:#F9F6F3;--white:#fff;
  --success:#25A384;--warning:#F99F07;--error:#FF442C;
  --r-sm:4px;--r-md:8px;--r-lg:12px;--r-xl:16px;
}
body{font-family:'DM Sans',sans-serif;background:var(--cream);color:var(--slate);font-size:13px;line-height:1.5}
/* ── Layout ── */
.topbar{background:var(--slate);color:var(--white);padding:12px 28px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.topbar-logo{display:flex;align-items:center;gap:10px;font-size:14px;font-weight:600;letter-spacing:-.3px}
.spark{width:28px;height:28px;background:var(--brand);border-radius:6px;display:flex;align-items:center;justify-content:center;color:white;font-size:15px;font-weight:700}
.topbar-meta{font-size:11px;color:#B09080;font-family:'DM Mono',monospace}
.container{max-width:1400px;margin:0 auto;padding:24px 28px}
/* ── KPIs ── */
.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.kpi{background:var(--white);border:1.5px solid var(--border);border-radius:var(--r-xl);padding:18px 20px}
.kpi-label{font-size:11px;font-weight:500;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px}
.kpi-value{font-size:26px;font-weight:600;color:var(--slate);letter-spacing:-.5px}
.kpi-value.brand{color:var(--brand)}
.kpi-sub{font-size:11px;color:var(--muted);margin-top:4px;font-family:'DM Mono',monospace}
/* ── Alerts ── */
.alerts{margin-bottom:24px;display:flex;flex-direction:column;gap:8px}
.alert-row{border-radius:var(--r-lg);padding:11px 16px;display:flex;align-items:center;gap:12px;font-size:12px;border:1.5px solid}
.alert-row.fire{background:#FFF0EE;border-color:#FFBAB0;color:#9B2100}
.alert-row.warn{background:#FFFBEE;border-color:#FFEAB0;color:#7A5200}
.alert-row.info{background:var(--brand-light);border-color:var(--brand-100);color:#7A2800}
.alert-title{font-weight:600;white-space:nowrap}
.alert-chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{display:inline-flex;align-items:center;gap:4px;background:rgba(255,255,255,.7);border:1px solid rgba(0,0,0,.1);border-radius:20px;padding:2px 8px;font-size:11px;cursor:pointer;text-decoration:none;color:inherit}
.chip:hover{background:white}
/* ── Bowtie ── */
.bowtie-section{background:var(--white);border:1.5px solid var(--border);border-radius:var(--r-xl);padding:20px 24px;margin-bottom:24px}
.section-title{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:16px}
.bowtie{display:flex;align-items:center;gap:0}
.bt-funnel{flex:1;display:flex;gap:4px}
.bt-funnel.right{flex-direction:row-reverse}
.bt-stage{flex:1;border-radius:var(--r-md);padding:10px 8px;text-align:center;border:1.5px solid var(--border);background:var(--cream);transition:all .15s}
.bt-stage:hover{border-color:var(--brand);transform:translateY(-2px);box-shadow:0 4px 12px rgba(245,98,32,.12)}
.bt-stage .count{font-size:18px;font-weight:600;color:var(--slate);letter-spacing:-.5px}
.bt-stage .label{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-top:2px;line-height:1.3}
.bt-stage .mrr{font-size:10px;font-family:'DM Mono',monospace;color:var(--brand);margin-top:4px;font-weight:500}
.bt-knot{padding:0 16px;text-align:center;flex-shrink:0}
.bt-knot-inner{background:var(--slate);color:white;border-radius:var(--r-xl);padding:12px 16px;min-width:100px}
.bt-knot-inner .k-val{font-size:18px;font-weight:600;letter-spacing:-.5px}
.bt-knot-inner .k-sub{font-size:9px;opacity:.6;text-transform:uppercase;letter-spacing:.6px;margin-top:2px}
.bt-arrow{font-size:18px;color:var(--border);align-self:center;padding:0 4px;flex-shrink:0}
/* ── Table ── */
.table-section{background:var(--white);border:1.5px solid var(--border);border-radius:var(--r-xl);overflow:hidden}
.table-header{padding:14px 20px;display:flex;align-items:center;justify-content:space-between;border-bottom:1.5px solid var(--border)}
.table-header h2{font-size:13px;font-weight:600;color:var(--slate)}
.filter-tabs{display:flex;gap:4px}
.tab{padding:5px 12px;border-radius:20px;font-size:11px;font-weight:500;cursor:pointer;border:1.5px solid var(--border);color:var(--muted);background:transparent;transition:all .15s}
.tab.active,.tab:hover{background:var(--brand-light);border-color:var(--brand);color:var(--brand)}
.table-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse}
thead th{padding:9px 14px;text-align:left;font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;background:var(--cream);border-bottom:1.5px solid var(--border);white-space:nowrap;cursor:pointer;user-select:none}
thead th:hover{color:var(--brand)}
thead th.sorted{color:var(--brand)}
thead th.sorted::after{content:' ↓'}
thead th.sorted.asc::after{content:' ↑'}
tbody tr{border-bottom:1px solid var(--border);transition:background .1s}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:var(--cream)}
tbody tr.highlight{background:var(--brand-light)}
td{padding:10px 14px;vertical-align:middle}
/* ── Cell styles ── */
.deal-name{font-weight:500;color:var(--slate);font-size:12.5px}
.deal-name a{color:inherit;text-decoration:none}
.deal-name a:hover{color:var(--brand)}
.deal-pipe{font-size:10px;color:var(--muted);margin-top:2px;font-family:'DM Mono',monospace}
.mrr{font-family:'DM Mono',monospace;font-size:12px;font-weight:500;white-space:nowrap}
.stage-badge{display:inline-block;padding:3px 8px;border-radius:20px;font-size:10px;font-weight:500;white-space:nowrap}
.stage-acq{background:#EFF6FF;color:#1D4ED8;border:1px solid #BFDBFE}
.stage-exp{background:#F0FDF4;color:#166534;border:1px solid #BBF7D0}
.stage-dead{background:#FEF2F2;color:#991B1B;border:1px solid #FECACA}
.close-date{font-family:'DM Mono',monospace;font-size:11px;white-space:nowrap}
.close-date.overdue{color:var(--error);font-weight:500}
.close-date.ok{color:var(--muted)}
.touch{font-size:11px;white-space:nowrap}
.touch-icon{font-size:13px}
.touch-days{font-family:'DM Mono',monospace;font-weight:500}
.touch.hot  .touch-days{color:var(--error)}
.touch.warm .touch-days{color:var(--warning)}
.touch.ok   .touch-days{color:var(--success)}
.touch.none .touch-days{color:var(--muted)}
.touch-label{font-size:10px;color:var(--muted);margin-top:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:160px}
.next-step{font-size:11px}
.ns-subject{color:var(--slate);font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:180px}
.ns-due{font-size:10px;font-family:'DM Mono',monospace;margin-top:1px}
.ns-due.overdue{color:var(--error)}
.ns-due.ok{color:var(--muted)}
.ns-empty{color:var(--border);font-style:italic;font-size:11px}
.hs-link{display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:6px;border:1.5px solid var(--border);color:var(--muted);font-size:12px;text-decoration:none;transition:all .15s}
.hs-link:hover{border-color:var(--brand);color:var(--brand)}
/* ── Footer ── */
.footer{text-align:center;font-size:11px;color:var(--muted);padding:20px;font-family:'DM Mono',monospace}
</style>
</head>
<body>

<div class="topbar">
  <div class="topbar-logo">
    <div class="spark">✦</div>
    <span>Sales Dashboard — Eloi Lefebvre</span>
  </div>
  <div class="topbar-meta">Mis à jour le __GENERATED_AT__</div>
</div>

<div class="container">

  <!-- KPIs -->
  <div class="kpi-row" id="kpi-row"></div>

  <!-- Alerts -->
  <div class="alerts" id="alerts"></div>

  <!-- Bowtie -->
  <div class="bowtie-section">
    <div class="section-title">Bowtie — Acquisition &amp; Expansion</div>
    <div class="bowtie" id="bowtie"></div>
  </div>

  <!-- Deal table -->
  <div class="table-section">
    <div class="table-header">
      <h2 id="table-title">Deals actifs</h2>
      <div class="filter-tabs">
        <button class="tab active" onclick="filterDeals('all')">Tous</button>
        <button class="tab" onclick="filterDeals('acq')">New Biz</button>
        <button class="tab" onclick="filterDeals('exp')">AM / Renewal</button>
        <button class="tab" onclick="filterDeals('fire')">🔴 Urgents</button>
      </div>
    </div>
    <div class="table-wrap">
      <table id="deal-table">
        <thead>
          <tr>
            <th onclick="sortBy('name')">Deal</th>
            <th onclick="sortBy('amount')" class="sorted">MRR</th>
            <th onclick="sortBy('stage')">Stage</th>
            <th onclick="sortBy('close')">Close</th>
            <th>Dernier touch</th>
            <th>Next step</th>
            <th></th>
          </tr>
        </thead>
        <tbody id="deal-tbody"></tbody>
      </table>
    </div>
  </div>

</div>

<div class="footer">Performance Starts With Clarity — talkspirit.com</div>

<script>
const DATA    = __DATA_JSON__;
const METRICS = __METRICS_JSON__;
const TODAY   = new Date();

// ── KPIs ──────────────────────────────────────────────────────────────────────
function renderKPIs() {
  const fire = METRICS.alerts.fire.length;
  const warn = METRICS.alerts.warn.length;
  const noStep = METRICS.alerts.no_step.length;
  const noDec  = METRICS.alerts.no_decision.length;
  const urgent = fire + noDec;
  document.getElementById('kpi-row').innerHTML = `
    <div class="kpi">
      <div class="kpi-label">Pipeline MRR</div>
      <div class="kpi-value brand">${fmt(METRICS.total_pipeline)} €</div>
      <div class="kpi-sub">/mois brut</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Forecast pondéré</div>
      <div class="kpi-value">${fmt(METRICS.total_weighted)} €</div>
      <div class="kpi-sub">${Math.round(METRICS.total_weighted/METRICS.total_pipeline*100)||0}% du pipeline</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Deals actifs</div>
      <div class="kpi-value">${DATA.length}</div>
      <div class="kpi-sub">${METRICS.alerts.no_step.length} sans next step</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Urgences</div>
      <div class="kpi-value ${urgent>0?'brand':''}">${urgent}</div>
      <div class="kpi-sub">${fire} inactifs &gt;14j · ${noDec} No Decision</div>
    </div>
  `;
}

// ── Alerts ────────────────────────────────────────────────────────────────────
function renderAlerts() {
  const el = document.getElementById('alerts');
  const rows = [];
  const {fire, warn, no_step, no_decision} = METRICS.alerts;

  if (fire.length) {
    rows.push(`<div class="alert-row fire">
      <span class="alert-title">🔴 Inactifs &gt;14 jours</span>
      <div class="alert-chips">${fire.map(d=>chipLink(d)).join('')}</div>
    </div>`);
  }
  if (warn.length) {
    rows.push(`<div class="alert-row warn">
      <span class="alert-title">🟡 Inactifs 7–14 jours</span>
      <div class="alert-chips">${warn.map(d=>chipLink(d)).join('')}</div>
    </div>`);
  }
  if (no_decision.length) {
    rows.push(`<div class="alert-row fire">
      <span class="alert-title">⚠️ No Decision — à traiter</span>
      <div class="alert-chips">${no_decision.map(d=>chipLink(d)).join('')}</div>
    </div>`);
  }
  if (no_step.length) {
    rows.push(`<div class="alert-row info">
      <span class="alert-title">📋 Sans next step</span>
      <div class="alert-chips">${no_step.map(d=>chipLink(d)).join('')}</div>
    </div>`);
  }
  el.innerHTML = rows.join('');
}

function chipLink(d) {
  return `<a class="chip" href="https://app-eu1.hubspot.com/contacts/25761660/deal/${d.id}" target="_blank">
    ${d.name.replace(/- New Deal|- Nouvel.+/i,'').trim()} <span style="color:var(--muted)">${fmt(d.amount)}€</span>
  </a>`;
}

// ── Bowtie ────────────────────────────────────────────────────────────────────
function renderBowtie() {
  const acqOrder = ["3112501453","3112501454","3112501455","3112501456","3112501457"];
  const expOrder = ["462257366","462257390","462257370"];
  const acqLabels = ["Disc. Sched.","Disc. Compl.","Solution Fit","Proposal","Negociation"];
  const expLabels = ["Upsell Id.","Negociation","Contract"];

  const acq = METRICS.acq;
  const exp = METRICS.exp;

  const acqHTML = acqOrder.map((id,i) => {
    const s = acq[id] || {count:0,mrr:0};
    return `<div class="bt-stage">
      <div class="count">${s.count}</div>
      <div class="label">${acqLabels[i]}</div>
      ${s.mrr ? `<div class="mrr">${fmt(Math.round(s.mrr))}€</div>` : ''}
    </div>`;
  }).join('<span class="bt-arrow">›</span>');

  const expHTML = expOrder.map((id,i) => {
    const s = exp[id] || {count:0,mrr:0};
    return `<div class="bt-stage">
      <div class="count">${s.count}</div>
      <div class="label">${expLabels[i]}</div>
      ${s.mrr ? `<div class="mrr">${fmt(Math.round(s.mrr))}€</div>` : ''}
    </div>`;
  }).join('<span class="bt-arrow">‹</span>');

  document.getElementById('bowtie').innerHTML = `
    <div class="bt-funnel">${acqHTML}</div>
    <div class="bt-knot">
      <div class="bt-knot-inner">
        <div class="k-val">${fmt(METRICS.total_weighted)}€</div>
        <div class="k-sub">Weighted</div>
      </div>
    </div>
    <div class="bt-funnel right">${expHTML}</div>
  `;
}

// ── Table ─────────────────────────────────────────────────────────────────────
let currentFilter = 'all';
let sortKey = 'amount';
let sortAsc  = false;
let fireIds  = new Set((METRICS.alerts.fire||[]).map(d=>d.id)
                .concat((METRICS.alerts.no_decision||[]).map(d=>d.id)));

function filterDeals(f) {
  currentFilter = f;
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  event.target.classList.add('active');
  renderTable();
}

function sortBy(key) {
  if (sortKey === key) sortAsc = !sortAsc;
  else { sortKey = key; sortAsc = false; }
  document.querySelectorAll('thead th').forEach(th => {
    th.classList.remove('sorted','asc');
    if(th.getAttribute('onclick') === `sortBy('${key}')`) {
      th.classList.add('sorted');
      if(sortAsc) th.classList.add('asc');
    }
  });
  renderTable();
}

function renderTable() {
  let rows = [...DATA];

  if (currentFilter === 'acq') rows = rows.filter(d=>d.cat==='acq');
  else if (currentFilter === 'exp') rows = rows.filter(d=>d.cat==='exp');
  else if (currentFilter === 'fire') rows = rows.filter(d=>fireIds.has(d.id)||
    (d.last_touch && d.last_touch.days_ago>7)||d.overdue||!d.next_step);

  rows.sort((a,b)=>{
    let av = a[sortKey]??'', bv = b[sortKey]??'';
    if(sortKey==='amount'){av=a.amount;bv=b.amount;}
    if(sortKey==='close'){av=a.close||'9999';bv=b.close||'9999';}
    const cmp = av < bv ? -1 : av > bv ? 1 : 0;
    return sortAsc ? cmp : -cmp;
  });

  document.getElementById('table-title').textContent = `Deals actifs (${rows.length})`;
  document.getElementById('deal-tbody').innerHTML = rows.map(renderRow).join('');
}

function renderRow(d) {
  const lt = d.last_touch;
  const ns = d.next_step;
  const days = lt ? lt.days_ago : 999;
  const touchClass = days > 14 ? 'hot' : days > 7 ? 'warm' : days <= 7 ? 'ok' : 'none';
  const icon = lt ? (lt.channel === 'email' ? '📧' : '📞') : '—';
  const daysLabel = days < 999 ? `${days}j` : 'jamais';
  const touchLabel = lt ? lt.label : '';
  const highlight = fireIds.has(d.id) ? ' class="highlight"' : '';

  const stageClass = d.cat==='exp' ? 'stage-exp' : d.cat==='dead' ? 'stage-dead' : 'stage-acq';
  const nameClean = d.name.replace(/- New Deal|- Nouvel.+|- Nouvel élément.+/i,'').trim();

  const nsHTML = ns
    ? `<div class="ns-subject">${ns.subject}</div>
       <div class="ns-due ${ns.overdue?'overdue':'ok'}">${ns.due_date ? (ns.overdue?'⚠ ':'')+ ns.due_date : 'Pas de date'}</div>`
    : `<div class="ns-empty">Aucun next step</div>`;

  return `<tr${highlight}>
    <td>
      <div class="deal-name"><a href="${d.hs_url}" target="_blank">${nameClean}</a></div>
      <div class="deal-pipe">${d.pipeline}</div>
    </td>
    <td><span class="mrr">${fmt(d.amount)} €</span></td>
    <td><span class="stage-badge ${stageClass}">${d.stage}</span></td>
    <td><span class="close-date ${d.overdue?'overdue':'ok'}">${d.close || '—'}${d.overdue?' ⚠':''}</span></td>
    <td>
      <div class="touch ${touchClass}">
        <div><span class="touch-icon">${icon}</span> <span class="touch-days">${daysLabel}</span></div>
        <div class="touch-label">${touchLabel}</div>
      </div>
    </td>
    <td><div class="next-step">${nsHTML}</div></td>
    <td><a class="hs-link" href="${d.hs_url}" target="_blank" title="Ouvrir HubSpot">↗</a></td>
  </tr>`;
}

// ── Utils ─────────────────────────────────────────────────────────────────────
function fmt(n) {
  return Number(n).toLocaleString('fr-FR');
}

// ── Init ──────────────────────────────────────────────────────────────────────
renderKPIs();
renderAlerts();
renderBowtie();
renderTable();
</script>
</body>
</html>
"""


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not TOKEN:
        print("ERROR: HUBSPOT_TOKEN not set")
        return

    deals   = fetch_deals()
    deals   = enrich(deals)
    metrics = compute_metrics(deals)
    rows    = serialise_deals(deals)

    html = HTML.replace("__GENERATED_AT__", metrics["generated_at"])
    html = html.replace("__DATA_JSON__",    json.dumps(rows,    ensure_ascii=False))
    html = html.replace("__METRICS_JSON__", json.dumps(metrics, ensure_ascii=False, default=str))

    OUT.write_text(html, encoding="utf-8")
    print(f"\n✓ Dashboard generated → {OUT}")
    print(f"  {len(rows)} active deals · Pipeline {metrics['total_pipeline']}€ · Weighted {metrics['total_weighted']}€")
    alerts = metrics["alerts"]
    if alerts["fire"]:
        print(f"  🔴 {len(alerts['fire'])} deals inactifs >14j")
    if alerts["no_decision"]:
        print(f"  ⚠️  {len(alerts['no_decision'])} No Decision")


if __name__ == "__main__":
    main()

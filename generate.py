#!/usr/bin/env python3
"""
Talkspirit Sales Dashboard — Generator
python generate.py  →  index.html
"""

import requests, json, os
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
OWNER = "33612016"
MONTHLY_QUOTA = 1000  # €/mois MRR — objectif new biz mensuel (à ajuster)
TODAY = datetime.now(timezone.utc)
BASE  = "https://api.hubapi.com"
OUT   = Path(__file__).parent / "index.html"

def headers():
    return {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

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
PIPELINE_LABEL = {"2281936074": "New Biz", "284322551": "AM", "3709248745": "Renewal"}

LEAD_STATUS_ORDER = ["NEW", "IN_PROGRESS", "CONNECTED"]
LEAD_STATUS_LABEL = {
    "NEW":         ("Nouveau",   "#1D4ED8", "#EFF6FF", "#BFDBFE"),
    "IN_PROGRESS": ("En cours",  "#7C3AED", "#F5F3FF", "#DDD6FE"),
    "CONNECTED":   ("Connecté",  "#B45309", "#FFFBEB", "#FDE68A"),
    "OPEN_DEAL":   ("Deal ouv.", "#166534", "#F0FDF4", "#BBF7D0"),
    "NVP":         ("No value",  "#991B1B", "#FEF2F2", "#FECACA"),
    "Nurturing":   ("Nurturing", "#6B7280", "#F9FAFB", "#E5E7EB"),
}

# ── Engagements ───────────────────────────────────────────────────────────────
def fetch_engagements(obj_type, obj_id):
    try:
        r = requests.get(
            f"{BASE}/engagements/v1/engagements/associated/{obj_type}/{obj_id}/paged",
            headers=headers(), params={"count": 100}
        )
        if r.status_code != 200:
            return None, None
        items = r.json().get("results", [])

        last_email = last_call = None
        best_task  = None

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
                if meta.get("status", "NOT_STARTED") != "COMPLETED":
                    if best_task is None or dt > best_task["_dt"]:
                        due_ts = eng.get("timestamp", 0)
                        due_dt = datetime.fromtimestamp(due_ts / 1000, timezone.utc) if due_ts else None
                        best_task = {
                            "_dt":      dt,
                            "subject":  meta.get("subject") or "Tâche",
                            "status":   meta.get("status", "NOT_STARTED"),
                            "due_date": due_dt.strftime("%d/%m") if due_dt else None,
                            "overdue":  (due_dt < TODAY) if due_dt else False,
                        }

        candidates = [x for x in [last_email, last_call] if x]
        last_touch = max(candidates, key=lambda x: x["date"]) if candidates else None
        if last_touch:
            last_touch["days_ago"] = (TODAY - last_touch["date"]).days
            last_touch["date"]     = last_touch["date"].strftime("%d/%m/%y")
        if best_task:
            best_task.pop("_dt", None)
        return last_touch, best_task

    except Exception as e:
        print(f"  Warning {obj_type} {obj_id}: {e}")
        return None, None


# ── Deals ─────────────────────────────────────────────────────────────────────
def fetch_deals():
    print("Fetching deals...")
    props = ["dealname","amount","dealstage","closedate","pipeline",
             "hubspot_owner_id","hs_lastmodifieddate","createdate","deal_archived"]
    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": OWNER},
            {"propertyName": "deal_archived", "operator": "NEQ", "value": "true"}
        ]}],
        "properties": props,
        "sorts": [{"propertyName": "amount", "direction": "DESCENDING"}],
        "limit": 100
    }
    deals = []
    after = None
    while True:
        if after:
            payload["after"] = after
        r = requests.post(f"{BASE}/crm/v3/objects/deals/search", headers=headers(), json=payload)
        r.raise_for_status()
        data = r.json()
        deals.extend(data.get("results", []))
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
    print(f"  → {len(deals)} deals")
    return deals


def fetch_deal_contact_map(deals):
    """Returns {deal_id: [contact_id, ...]} via batch associations API."""
    if not deals:
        return {}
    contact_map = {}
    for i in range(0, len(deals), 100):
        chunk = [{"id": d["id"]} for d in deals[i:i+100]]
        try:
            r = requests.post(
                f"{BASE}/crm/v4/associations/deals/contacts/batch/read",
                headers=headers(),
                json={"inputs": chunk}
            )
            if r.status_code not in (200, 207):
                continue
            for result in r.json().get("results", []):
                deal_id = str(result["from"]["id"])
                contact_ids = [str(t["toObjectId"]) for t in result.get("to", [])]
                if contact_ids:
                    contact_map[deal_id] = contact_ids
        except Exception as e:
            print(f"  Warning fetch_deal_contact_map: {e}")
    n_contacts = sum(len(v) for v in contact_map.values())
    print(f"  → {n_contacts} contacts associés à {len(contact_map)} deals")
    return contact_map


def _best_touch(lt1, lt2):
    """Return the most recent last_touch (smaller days_ago = more recent)."""
    if lt1 is None: return lt2
    if lt2 is None: return lt1
    return lt1 if lt1["days_ago"] <= lt2["days_ago"] else lt2


def enrich_deals(deals):
    print(f"Enriching {len(deals)} deals...")
    # Also check engagements on associated contacts (emails often logged there)
    contact_map = fetch_deal_contact_map(deals)

    def fetch_all_for_deal(deal):
        lt, ns = fetch_engagements("DEAL", deal["id"])
        for cid in contact_map.get(str(deal["id"]), []):
            lt_c, ns_c = fetch_engagements("CONTACT", cid)
            lt = _best_touch(lt, lt_c)
            if ns is None:
                ns = ns_c
        return lt, ns

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_all_for_deal, d): d for d in deals}
        for fut in as_completed(futs):
            deal = futs[fut]
            lt, ns = fut.result()
            deal["last_touch"] = lt
            deal["next_step"]  = ns
    return deals


# ── Leads ─────────────────────────────────────────────────────────────────────
def fetch_leads():
    print("Fetching leads...")
    props = ["firstname","lastname","email","company","jobtitle",
             "hs_lead_status","lifecyclestage","hubspot_owner_id",
             "createdate","hs_sales_email_last_replied","phone"]

    # Timestamp filters: created ≤ 90 days OR last modified ≤ 30 days
    ts_90d = int((TODAY - __import__('datetime').timedelta(days=90)).timestamp() * 1000)
    ts_30d = int((TODAY - __import__('datetime').timedelta(days=30)).timestamp() * 1000)

    base_filters = [
        {"propertyName": "hubspot_owner_id", "operator": "EQ",  "value": OWNER},
        {"propertyName": "hs_lead_status",   "operator": "IN",  "values": ["NEW","IN_PROGRESS","CONNECTED"]},
    ]
    payload = {
        # filterGroups = OR between groups
        "filterGroups": [
            {"filters": base_filters + [
                {"propertyName": "createdate", "operator": "GTE", "value": str(ts_90d)}
            ]},
            {"filters": base_filters + [
                {"propertyName": "hs_lastmodifieddate", "operator": "GTE", "value": str(ts_30d)}
            ]},
        ],
        "properties": props,
        "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
        "limit": 100
    }
    r = requests.post(f"{BASE}/crm/v3/objects/contacts/search", headers=headers(), json=payload)
    r.raise_for_status()
    leads = r.json().get("results", [])
    print(f"  → {len(leads)} leads (créés <90j OU modifiés <30j)")
    return leads


def enrich_leads(leads):
    print(f"Enriching {len(leads)} leads...")
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(fetch_engagements, "CONTACT", l["id"]): l for l in leads}
        for fut in as_completed(futs):
            lead = futs[fut]
            lt, ns = fut.result()
            lead["last_touch"] = lt
            lead["next_step"]  = ns
    return leads


def filter_leads_without_deals(leads):
    """Returns only leads that have NO associated deal in HubSpot — avoids lead/deal duplicates.
    Checks directly from the contact side so it works regardless of which deals we fetched."""
    if not leads:
        return leads
    contacts_with_deals = set()
    # Process in chunks of 100 (API limit per batch call)
    for i in range(0, len(leads), 100):
        chunk = [{"id": l["id"]} for l in leads[i:i+100]]
        try:
            r = requests.post(
                f"{BASE}/crm/v4/associations/contacts/deals/batch/read",
                headers=headers(),
                json={"inputs": chunk}
            )
            if r.status_code not in (200, 207):
                continue
            for result in r.json().get("results", []):
                if result.get("to"):  # contact has at least one deal
                    contacts_with_deals.add(str(result["from"]["id"]))
        except Exception as e:
            print(f"  Warning filter_leads_without_deals: {e}")
    before = len(leads)
    filtered = [l for l in leads if l["id"] not in contacts_with_deals]
    print(f"  → {len(filtered)} leads after dedup (removed {before - len(filtered)} contacts already in a deal)")
    return filtered


# ── WbD KPIs ─────────────────────────────────────────────────────────────────
def fetch_win_rate():
    cutoff_ms = int((TODAY - timedelta(days=90)).timestamp() * 1000)
    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "hubspot_owner_id", "operator": "EQ",  "value": OWNER},
            {"propertyName": "dealstage",        "operator": "IN",  "values": ["3112501458", "3112501459"]},
            {"propertyName": "closedate",        "operator": "GTE", "value": str(cutoff_ms)}
        ]}],
        "properties": ["dealstage", "amount"],
        "limit": 100
    }
    r = requests.post(f"{BASE}/crm/v3/objects/deals/search", headers=headers(), json=payload)
    if r.status_code != 200:
        return {"won": 0, "lost": 0, "rate": None, "total": 0}
    results = r.json().get("results", [])
    won   = sum(1 for d in results if d["properties"]["dealstage"] == "3112501458")
    lost  = sum(1 for d in results if d["properties"]["dealstage"] == "3112501459")
    total = won + lost
    rate  = round(won / total * 100) if total else None
    print(f"  Win Rate 90j: {won}W / {lost}L → {rate}%")
    return {"won": won, "lost": lost, "rate": rate, "total": total}


def fetch_new_pipeline_mtd():
    first_of_month = TODAY.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    cutoff_ms = int(first_of_month.timestamp() * 1000)
    won_lost = {"3112501458","3112501459","462257371","462257372","5134323932","5134323933","5137691842"}
    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "hubspot_owner_id", "operator": "EQ",  "value": OWNER},
            {"propertyName": "deal_archived",    "operator": "NEQ", "value": "true"},
            {"propertyName": "createdate",       "operator": "GTE", "value": str(cutoff_ms)}
        ]}],
        "properties": ["dealstage", "amount"],
        "limit": 100
    }
    r = requests.post(f"{BASE}/crm/v3/objects/deals/search", headers=headers(), json=payload)
    if r.status_code != 200:
        return {"mrr": 0, "count": 0}
    results = r.json().get("results", [])
    active = [d for d in results if d["properties"]["dealstage"] not in won_lost]
    mrr    = sum(float(d["properties"].get("amount") or 0) for d in active)
    print(f"  New Pipeline MTD: {len(active)} deals · {round(mrr)}€ MRR")
    return {"mrr": round(mrr), "count": len(active)}


# ── SPICED Scoring ────────────────────────────────────────────────────────────
def spiced_deal(d):
    p         = d["properties"]
    sid       = p.get("dealstage", "")
    prob      = STAGE.get(sid, ("", 0, ""))[1]
    amount    = float(p.get("amount") or 0)
    name      = p.get("dealname", "")
    lt        = d.get("last_touch")
    ns        = d.get("next_step")
    close_raw = p.get("closedate", "")
    close_dt  = None
    days_to_close = 999
    if close_raw:
        try:
            close_dt      = datetime.fromisoformat(close_raw.replace("Z", "+00:00"))
            days_to_close = (close_dt - TODAY).days
        except:
            pass

    generic = any(x in name.lower() for x in ["new deal","nouvel","- deal","élément"])

    S = 1 if (amount > 0 and not generic) else 0
    S_w = "Entreprise + montant identifiés" if S else "Nom générique ou montant absent"

    P = 1 if (prob >= 20 or (lt and lt.get("channel") == "call")) else 0
    P_w = "Discovery réalisée ou appel enregistré" if P else "Pas encore de discovery"

    I = 1 if (amount > 0 and prob >= 20) else 0
    I_w = f"Valeur qualifiée : {int(amount)} €/m" if I else "Impact non quantifié"

    C = 1 if (close_dt and 0 < days_to_close <= 45) else 0
    C_w = f"Échéance dans {days_to_close}j" if C else "Pas d'urgence (<45j)"

    E = 1 if close_dt else 0
    E_w = close_dt.strftime("%d/%m/%y") if close_dt else "Close date non définie"

    D = 1 if prob >= 40 else 0
    D_w = f"Stage {prob}% — critères de décision probablement connus" if D else "Stage < 40%"

    total = S + P + I + C + E + D

    recency = 1.0 if (lt and lt.get("days_ago", 999) <= 3)  else \
              0.8 if (lt and lt.get("days_ago", 999) <= 7)  else \
              0.5 if (lt and lt.get("days_ago", 999) <= 14) else 0.2
    urgency = 2.0 if days_to_close <= 7  else \
              1.5 if days_to_close <= 14 else \
              1.2 if days_to_close <= 30 else 1.0
    pub = is_public(name)
    hot = round((total / 6) * (prob / 100) * recency * urgency * 10 * (_PUBLIC_FACTOR if pub else 1.0), 1)

    return {"S": S, "P": P, "I": I, "C": C, "E": E, "D": D, "total": total, "hot": hot,
            "pub": pub,
            "why": {"S": S_w, "P": P_w, "I": I_w, "C": C_w, "E": E_w, "D": D_w}}


def spiced_lead(l):
    p      = l["properties"]
    status = p.get("hs_lead_status", "")
    lt     = l.get("last_touch")
    ns     = l.get("next_step")
    RANK   = {"NEW": 1, "IN_PROGRESS": 2, "CONNECTED": 3, "OPEN_DEAL": 4}
    rank   = RANK.get(status, 1)

    S = 1 if (p.get("company") and p.get("jobtitle")) else 0
    S_w = f"{p.get('company')} · {p.get('jobtitle')}" if S else "Entreprise ou titre manquant"

    P = 1 if (rank >= 3 or (lt and lt.get("channel") == "call")) else 0
    P_w = "CONNECTED ou appel enregistré" if P else "Pas encore de conversation substantielle"

    I = 1 if (lt and lt.get("channel") == "call") else 0
    I_w = "Appel réalisé — impact discuté" if I else "Aucun appel enregistré"

    C = 1 if (ns and ns.get("due_date") and not ns.get("overdue")) else 0
    C_w = f"RDV planifié : {ns.get('due_date')}" if C else "Pas de prochain RDV planifié"

    E = 1 if ns else 0
    E_w = ns.get("subject", "Tâche définie") if E else "Aucun next step défini"

    D = 1 if (lt and lt.get("days_ago", 999) <= 7) else 0
    D_w = f"Dernier touch il y a {lt['days_ago']}j" if (lt and D) else "Relation inactive (>7j)"

    total = S + P + I + C + E + D

    prob_map = {1: 0.05, 2: 0.15, 3: 0.30, 4: 0.45}
    prob    = prob_map.get(rank, 0.05)
    recency = 1.0 if (lt and lt.get("days_ago", 999) <= 3)  else \
              0.8 if (lt and lt.get("days_ago", 999) <= 7)  else \
              0.5 if (lt and lt.get("days_ago", 999) <= 14) else 0.2
    pub = is_public(p.get("company", "") or "")
    hot = round((total / 6) * prob * recency * 10 * (_PUBLIC_FACTOR if pub else 1.0), 1)

    return {"S": S, "P": P, "I": I, "C": C, "E": E, "D": D, "total": total, "hot": hot,
            "pub": pub,
            "why": {"S": S_w, "P": P_w, "I": I_w, "C": C_w, "E": E_w, "D": D_w}}


def days_open_from(raw):
    if not raw:
        return 999
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return (TODAY - dt).days
    except:
        return 999


# ── Secteur public ─────────────────────────────────────────────────────────────
_PUBLIC_KW = [
    "mairie", "commune ", "ville de ", "département", "departement",
    "région ", "region ", "préfecture", "sous-préfecture",
    "métropole", "metropole", "agglomération", "agglomeration",
    "communauté de communes", "communauté d'agglomération",
    "conseil régional", "conseil départemental", "conseil général",
    "sdis", "ccas", "collectivité",
    "centre hospitalier", " chu ", "hôpital", "hopital",
    "cpam", "caisse primaire", "académie de", "rectorat",
    "ministère", "ministere", "direction régionale", "direction générale",
    "cnrs", "inserm", "inria", "inrae",
    "office de tourisme", "chambre de commerce", "chambre des métiers",
]
_PUBLIC_FACTOR = 0.55   # cycle de décision ~2× plus long → probabilité semaine réduite

def is_public(text):
    t = (text or "").lower()
    return any(kw in t for kw in _PUBLIC_KW)


# ── Metrics ───────────────────────────────────────────────────────────────────
def compute_metrics(deals, leads):
    acq = {s: {"count": 0, "mrr": 0} for s in ACQ_ORDER}
    exp = {s: {"count": 0, "mrr": 0} for s in EXP_ORDER}
    lead_counts = {s: 0 for s in LEAD_STATUS_ORDER}
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
            lt   = d.get("last_touch")
            days = lt["days_ago"] if lt else 999
            if days > 14:
                alerts["fire"].append({"name": name, "id": did, "amount": amount, "days": days, "kind": "deal"})
            elif days > 7:
                alerts["warn"].append({"name": name, "id": did, "amount": amount, "days": days, "kind": "deal"})
            if not d.get("next_step"):
                alerts["no_step"].append({"name": name, "id": did, "amount": amount, "kind": "deal"})
            if sid == "5137691842":
                alerts["no_decision"].append({"name": name, "id": did, "amount": amount, "kind": "deal"})

    for l in leads:
        status = l["properties"].get("hs_lead_status", "")
        if status in lead_counts:
            lead_counts[status] += 1
        lt   = l.get("last_touch")
        days = lt["days_ago"] if lt else 999
        fn   = l["properties"].get("firstname", "")
        ln   = l["properties"].get("lastname", "")
        name = f"{fn} {ln}".strip() or l["properties"].get("email", "")
        lid  = l["id"]
        if days > 14:
            alerts["fire"].append({"name": name, "id": lid, "amount": 0, "days": days, "kind": "lead"})
        elif days > 7:
            alerts["warn"].append({"name": name, "id": lid, "amount": 0, "days": days, "kind": "lead"})
        if not l.get("next_step"):
            alerts["no_step"].append({"name": name, "id": lid, "amount": 0, "kind": "lead"})

    return {
        "acq": acq, "exp": exp, "lead_counts": lead_counts,
        "total_leads": len(leads),
        "total_pipeline": round(total_pipeline),
        "total_weighted": round(total_weighted),
        "alerts": alerts,
        "generated_at": TODAY.strftime("%d/%m/%Y à %H:%M UTC"),
    }


# ── Serialisation ─────────────────────────────────────────────────────────────
def serialise_deals(deals):
    out = []
    for d in deals:
        sid    = d["properties"]["dealstage"]
        s_info = STAGE.get(sid, ("Unknown", 0, ""))
        cat    = s_info[2]
        if cat in ("won", "lost"):
            continue

        close_raw = d["properties"].get("closedate", "")
        close_dt  = None
        overdue   = False
        if close_raw:
            try:
                close_dt = datetime.fromisoformat(close_raw.replace("Z", "+00:00"))
                overdue  = close_dt < TODAY
            except:
                pass

        sp = spiced_deal(d)

        out.append({
            "id":         d["id"],
            "kind":       "deal",
            "name":       d["properties"].get("dealname", ""),
            "amount":     float(d["properties"].get("amount") or 0),
            "stage":      s_info[0],
            "stage_id":   sid,
            "prob":       s_info[1],
            "cat":        cat,
            "pipeline":   PIPELINE_LABEL.get(d["properties"].get("pipeline", ""), ""),
            "close":      close_dt.strftime("%d/%m/%y") if close_dt else "",
            "overdue":    overdue,
            "days_open":  days_open_from(d["properties"].get("createdate", "")),
            "hs_url":     f"https://app-eu1.hubspot.com/contacts/25761660/deal/{d['id']}",
            "last_touch": d.get("last_touch"),
            "next_step":  d.get("next_step"),
            "spiced":     sp,
        })
    return out


def serialise_leads(leads):
    out = []
    for l in leads:
        p      = l["properties"]
        fn     = p.get("firstname", "")
        ln     = p.get("lastname", "")
        name   = f"{fn} {ln}".strip() or p.get("email", "")
        status = p.get("hs_lead_status", "")
        s_info = LEAD_STATUS_LABEL.get(status, (status, "#6B7280", "#F9FAFB", "#E5E7EB"))

        create_raw  = p.get("createdate", "")
        created_str = ""
        if create_raw:
            try:
                created_str = datetime.fromisoformat(
                    create_raw.replace("Z", "+00:00")).strftime("%d/%m/%y")
            except:
                pass

        sp = spiced_lead(l)

        out.append({
            "id":           l["id"],
            "kind":         "lead",
            "name":         name,
            "company":      p.get("company", "") or "",
            "email":        p.get("email", "") or "",
            "jobtitle":     p.get("jobtitle", "") or "",
            "status":       status,
            "status_label": s_info[0],
            "status_color": s_info[1],
            "status_bg":    s_info[2],
            "status_border":s_info[3],
            "created":      created_str,
            "days_open":    days_open_from(create_raw),
            "hs_url":       f"https://app-eu1.hubspot.com/contacts/25761660/contact/{l['id']}",
            "last_touch":   l.get("last_touch"),
            "next_step":    l.get("next_step"),
            "spiced":       sp,
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
.topbar{background:var(--slate);color:var(--white);padding:12px 28px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.topbar-logo{display:flex;align-items:center;gap:10px;font-size:14px;font-weight:600;letter-spacing:-.3px}
.spark{width:28px;height:28px;background:var(--brand);border-radius:6px;display:flex;align-items:center;justify-content:center;color:white;font-size:15px;font-weight:700}
.topbar-meta{font-size:11px;color:#B09080;font-family:'DM Mono',monospace}
.container{max-width:1500px;margin:0 auto;padding:24px 28px}
/* KPIs */
.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.kpi{background:var(--white);border:1.5px solid var(--border);border-radius:var(--r-xl);padding:18px 20px}
.kpi-label{font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px}
.kpi-value{font-size:24px;font-weight:600;color:var(--slate);letter-spacing:-.5px}
.kpi-value.brand{color:var(--brand)}
.kpi-sub{font-size:11px;color:var(--muted);margin-top:4px;font-family:'DM Mono',monospace}
/* Alerts */
.alerts{margin-bottom:24px;display:flex;flex-direction:column;gap:8px}
.alert-row{border-radius:var(--r-lg);padding:11px 16px;display:flex;align-items:center;gap:12px;font-size:12px;border:1.5px solid;flex-wrap:wrap}
.alert-row.fire{background:#FFF0EE;border-color:#FFBAB0;color:#9B2100}
.alert-row.warn{background:#FFFBEE;border-color:#FFEAB0;color:#7A5200}
.alert-row.info{background:var(--brand-light);border-color:var(--brand-100);color:#7A2800}
.alert-title{font-weight:600;white-space:nowrap;flex-shrink:0}
.alert-chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{display:inline-flex;align-items:center;gap:4px;background:rgba(255,255,255,.7);border:1px solid rgba(0,0,0,.1);border-radius:20px;padding:2px 8px;font-size:11px;cursor:pointer;text-decoration:none;color:inherit}
.chip:hover{background:white}
/* Bowtie */
.bowtie-section{background:var(--white);border:1.5px solid var(--border);border-radius:var(--r-xl);padding:20px 24px;margin-bottom:24px;overflow-x:auto}
.section-title{font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:16px}
.bowtie{display:flex;align-items:stretch;gap:0;min-width:900px}
.bt-group{display:flex;flex-direction:column;gap:4px;flex:1}
.bt-group-label{font-size:9px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;text-align:center;margin-bottom:4px;padding-bottom:6px;border-bottom:1px solid var(--border)}
.bt-stages{display:flex;gap:4px;flex:1}
.bt-stage{flex:1;border-radius:var(--r-md);padding:10px 6px;text-align:center;border:1.5px solid var(--border);background:var(--cream);transition:all .15s;cursor:pointer}
.bt-stage:hover{border-color:var(--brand);transform:translateY(-2px);box-shadow:0 4px 12px rgba(245,98,32,.1)}
.bt-stage.active-drill{border-color:var(--brand);background:var(--brand-light);transform:translateY(-2px)}
/* Bowtie drill-down panel */
.bt-drill{margin-top:16px;border:1.5px solid var(--border);border-radius:var(--r-lg);overflow:hidden;animation:fadeUp .2s ease}
@keyframes fadeUp{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.bt-drill-header{padding:10px 16px;background:var(--cream);border-bottom:1.5px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.bt-drill-title{font-size:12px;font-weight:600;color:var(--slate)}
.bt-drill-close{background:none;border:none;cursor:pointer;color:var(--muted);font-size:16px;line-height:1;padding:0 4px}
.bt-drill-close:hover{color:var(--slate)}
.bt-drill table{width:100%;border-collapse:collapse}
.bt-drill thead th{padding:7px 14px;text-align:left;font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;background:var(--cream);border-bottom:1.5px solid var(--border)}
.bt-drill tbody tr{border-bottom:1px solid var(--border)}
.bt-drill tbody tr:last-child{border-bottom:none}
.bt-drill tbody tr:hover{background:var(--cream)}
.bt-drill td{padding:8px 14px;font-size:12px;vertical-align:middle}
.bt-stage .count{font-size:20px;font-weight:600;color:var(--slate);letter-spacing:-.5px}
.bt-stage .label{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-top:2px;line-height:1.3}
.bt-stage .mrr{font-size:10px;font-family:'DM Mono',monospace;color:var(--brand);margin-top:4px;font-weight:500}
.bt-stage.lead-new{border-color:#BFDBFE;background:#EFF6FF}
.bt-stage.lead-new .count{color:#1D4ED8}
.bt-stage.lead-prog{border-color:#DDD6FE;background:#F5F3FF}
.bt-stage.lead-prog .count{color:#7C3AED}
.bt-stage.lead-conn{border-color:#FDE68A;background:#FFFBEB}
.bt-stage.lead-conn .count{color:#B45309}
.bt-divider{display:flex;align-items:center;padding:0 8px;color:var(--border);font-size:20px;flex-shrink:0;padding-top:28px}
.bt-knot{padding:0 12px;text-align:center;flex-shrink:0;display:flex;flex-direction:column;justify-content:flex-end;padding-top:28px}
.bt-knot-inner{background:var(--slate);color:white;border-radius:var(--r-xl);padding:14px 18px;min-width:110px}
.bt-knot-inner .k-val{font-size:20px;font-weight:600;letter-spacing:-.5px}
.bt-knot-inner .k-sub{font-size:9px;opacity:.6;text-transform:uppercase;letter-spacing:.6px;margin-top:2px}
/* View tabs */
.view-tabs{display:flex;gap:0}
.view-tab{padding:10px 20px;font-size:13px;font-weight:500;cursor:pointer;border:none;background:transparent;color:var(--muted);border-bottom:2px solid transparent;margin-bottom:-2px;transition:all .15s}
.view-tab.active{color:var(--brand);border-bottom-color:var(--brand)}
.view-tab:hover:not(.active){color:var(--slate)}
/* Tables */
.table-section{background:var(--white);border:1.5px solid var(--border);border-radius:var(--r-xl);overflow:hidden}
.table-header{padding:14px 20px;display:flex;align-items:center;justify-content:space-between;border-bottom:1.5px solid var(--border)}
.table-header h2{font-size:13px;font-weight:600;color:var(--slate)}
.filter-tabs{display:flex;gap:4px;flex-wrap:wrap}
.tab{padding:5px 12px;border-radius:20px;font-size:11px;font-weight:500;cursor:pointer;border:1.5px solid var(--border);color:var(--muted);background:transparent;transition:all .15s}
.tab.active,.tab:hover{background:var(--brand-light);border-color:var(--brand);color:var(--brand)}
.table-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse}
thead th{padding:9px 14px;text-align:left;font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;background:var(--cream);border-bottom:1.5px solid var(--border);white-space:nowrap;cursor:pointer;user-select:none}
thead th:hover{color:var(--brand)}
thead th.sorted::after{content:' ↓';color:var(--brand)}
thead th.sorted.asc::after{content:' ↑'}
tbody tr{border-bottom:1px solid var(--border);transition:background .1s}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:var(--cream)}
tbody tr.highlight{background:var(--brand-light)}
td{padding:10px 14px;vertical-align:middle}
/* Cell styles */
.deal-name{font-weight:500;color:var(--slate);font-size:12.5px}
.deal-name a{color:inherit;text-decoration:none}
.deal-name a:hover{color:var(--brand)}
.deal-sub{font-size:10px;color:var(--muted);margin-top:2px;font-family:'DM Mono',monospace}
.mrr{font-family:'DM Mono',monospace;font-size:12px;font-weight:500;white-space:nowrap}
.stage-badge{display:inline-block;padding:3px 8px;border-radius:20px;font-size:10px;font-weight:500;white-space:nowrap}
.stage-acq{background:#EFF6FF;color:#1D4ED8;border:1px solid #BFDBFE}
.stage-exp{background:#F0FDF4;color:#166534;border:1px solid #BBF7D0}
.stage-dead{background:#FEF2F2;color:#991B1B;border:1px solid #FECACA}
.close-date{font-family:'DM Mono',monospace;font-size:11px;white-space:nowrap}
.close-date.overdue{color:var(--error);font-weight:500}
.close-date.ok{color:var(--muted)}
.touch{font-size:11px}
.touch-days{font-family:'DM Mono',monospace;font-weight:500}
.touch.hot  .touch-days{color:var(--error)}
.touch.warm .touch-days{color:var(--warning)}
.touch.ok   .touch-days{color:var(--success)}
.touch.none .touch-days{color:var(--muted)}
.touch-label{font-size:10px;color:var(--muted);margin-top:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:150px}
.next-step{font-size:11px}
.ns-subject{color:var(--slate);font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:170px}
.ns-due{font-size:10px;font-family:'DM Mono',monospace;margin-top:1px}
.ns-due.overdue{color:var(--error)}
.ns-due.ok{color:var(--muted)}
.ns-empty{color:var(--border);font-style:italic;font-size:11px}
.hs-link{display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:6px;border:1.5px solid var(--border);color:var(--muted);font-size:12px;text-decoration:none;transition:all .15s}
.hs-link:hover{border-color:var(--brand);color:var(--brand)}
/* SPICED */
.spiced-dims{display:flex;align-items:center;gap:3px}
.sd{width:22px;height:22px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;cursor:help;transition:transform .15s;flex-shrink:0;position:relative}
.sd:hover{transform:scale(1.15)}
.sd.on{background:var(--brand);color:white}
.sd.off{background:var(--border);color:var(--muted)}
.sd-score{font-size:11px;font-family:'DM Mono',monospace;color:var(--muted);margin-left:5px;white-space:nowrap}
.sd-score.full{color:var(--success);font-weight:600}
/* Tooltip */
.sd[data-tip]{position:relative}
.sd[data-tip]:hover::after{content:attr(data-tip);position:absolute;bottom:calc(100% + 6px);left:50%;transform:translateX(-50%);background:var(--slate);color:white;font-size:10px;font-weight:400;white-space:nowrap;padding:4px 8px;border-radius:6px;z-index:999;pointer-events:none;max-width:220px;white-space:normal;text-align:center;line-height:1.4}
/* Hot score */
.hot-wrap{display:flex;flex-direction:column;gap:4px;min-width:100px}
.hot-bar-outer{background:var(--border);border-radius:4px;height:5px;width:100%;overflow:hidden}
.hot-bar-inner{height:100%;border-radius:4px;transition:width .3s}
.hot-meta{display:flex;justify-content:space-between;align-items:center}
.hot-val{font-family:'DM Mono',monospace;font-size:11px;font-weight:600}
.hot-lbl{font-size:10px;color:var(--muted)}
/* Type badge */
.type-badge{display:inline-block;padding:2px 7px;border-radius:20px;font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.6px}
.type-deal{background:#F0FDF4;color:#166534;border:1px solid #BBF7D0}
.type-lead{background:#F5F3FF;color:#7C3AED;border:1px solid #DDD6FE}
.hidden{display:none}
.footer{text-align:center;font-size:11px;color:var(--muted);padding:20px;font-family:'DM Mono',monospace}
/* SPICED Questions panel */
.q-row>td{padding:0;border:none}
.q-panel{padding:14px 20px 16px;background:#FAFAF9;border-top:1px dashed var(--border)}
.q-label{font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.7px;margin-bottom:8px}
.q-draft-box{background:var(--white);border:1.5px solid var(--border);border-radius:var(--r-lg);padding:12px 16px;font-size:12px;line-height:1.8;color:var(--slate);white-space:pre-wrap;font-family:'DM Sans',sans-serif;max-height:220px;overflow-y:auto}
.q-actions{display:flex;gap:8px;margin-top:10px}
.q-copy{padding:5px 16px;border-radius:6px;font-size:11px;font-weight:500;cursor:pointer;border:1.5px solid var(--brand);color:var(--brand);background:transparent;transition:all .15s}
.q-copy:hover{background:var(--brand);color:white}
.q-copy.ok{background:var(--success);border-color:var(--success);color:white}
.q-close-btn{padding:5px 12px;border-radius:6px;font-size:11px;cursor:pointer;border:1.5px solid var(--border);color:var(--muted);background:transparent}
.q-close-btn:hover{border-color:var(--slate);color:var(--slate)}
.q-toggle-btn{margin-left:6px;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:500;cursor:pointer;border:1px solid var(--border);color:var(--muted);background:transparent;white-space:nowrap;transition:all .15s}
.q-toggle-btn:hover{border-color:var(--brand);color:var(--brand)}
</style>
</head>
<body>

<div class="topbar">
  <div class="topbar-logo">
    <div class="spark">✦</div>
    <span>Sales Dashboard — Eloi Lefebvre</span>
  </div>
  <div style="display:flex;align-items:center;gap:12px">
    <div class="topbar-meta">Mis à jour le __GENERATED_AT__</div>
    <button onclick="hardRefresh()" id="refresh-btn" style="display:flex;align-items:center;gap:5px;background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.2);color:#fff;border-radius:6px;padding:5px 12px;font-size:11px;font-family:'DM Sans',sans-serif;font-weight:500;cursor:pointer;transition:background .15s" onmouseover="this.style.background='rgba(255,255,255,.2)'" onmouseout="this.style.background='rgba(255,255,255,.1)'">
      <svg id="refresh-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
      Actualiser
    </button>
  </div>
</div>

<div class="container">
  <div class="kpi-row" id="kpi-row"></div>
  <div class="alerts" id="alerts"></div>

  <div class="bowtie-section">
    <div class="section-title">Bowtie — Leads → Acquisition → Expansion</div>
    <div class="bowtie" id="bowtie"></div>
    <div id="bt-drill" class="hidden"></div>
  </div>

  <div class="table-section">
    <div style="padding:0 20px;border-bottom:2px solid var(--border)">
      <div class="view-tabs">
        <button class="view-tab active" onclick="switchView('deals',this)">Deals <span id="deals-count"></span></button>
        <button class="view-tab" onclick="switchView('leads',this)">Leads <span id="leads-count"></span></button>
        <button class="view-tab" onclick="switchView('spiced',this)">✦ SPICED — Récents <span id="spiced-count"></span></button>
      </div>
    </div>

    <!-- Deals -->
    <div id="view-deals">
      <div class="table-header">
        <h2 id="deals-title">Deals actifs</h2>
        <div class="filter-tabs">
          <button class="tab active" onclick="filterDeals('all',this)">Tous</button>
          <button class="tab" onclick="filterDeals('acq',this)">New Biz</button>
          <button class="tab" onclick="filterDeals('exp',this)">AM / Renewal</button>
          <button class="tab" onclick="filterDeals('fire',this)">🔴 Urgents</button>
          <button class="tab" onclick="filterDeals('spiced',this)">✦ SPICED ≥ 3</button>
        </div>
      </div>
      <div class="table-wrap">
        <table><thead><tr>
          <th onclick="sortDeals('name')">Deal</th>
          <th onclick="sortDeals('amount')" class="sorted">MRR</th>
          <th onclick="sortDeals('stage')">Stage</th>
          <th onclick="sortDeals('close')">Close</th>
          <th>Dernier touch</th>
          <th>Next step</th>
          <th onclick="sortDeals('spiced_total')">SPICED</th>
          <th></th>
        </tr></thead><tbody id="deals-tbody"></tbody></table>
      </div>
    </div>

    <!-- Leads -->
    <div id="view-leads" class="hidden">
      <div class="table-header">
        <h2 id="leads-title">Leads en qualification</h2>
        <div class="filter-tabs">
          <button class="tab active" onclick="filterLeads('all',this)">Tous</button>
          <button class="tab" onclick="filterLeads('NEW',this)">Nouveaux</button>
          <button class="tab" onclick="filterLeads('IN_PROGRESS',this)">En cours</button>
          <button class="tab" onclick="filterLeads('CONNECTED',this)">Connectés</button>
          <button class="tab" onclick="filterLeads('fire',this)">🔴 Urgents</button>
          <button class="tab" onclick="filterLeads('spiced',this)">✦ SPICED ≥ 3</button>
        </div>
      </div>
      <div class="table-wrap">
        <table><thead><tr>
          <th onclick="sortLeads('name')">Contact</th>
          <th onclick="sortLeads('company')">Entreprise</th>
          <th onclick="sortLeads('status')">Statut</th>
          <th onclick="sortLeads('created')">Entré</th>
          <th>Dernier touch</th>
          <th>Next step</th>
          <th onclick="sortLeads('spiced_total')">SPICED</th>
          <th></th>
        </tr></thead><tbody id="leads-tbody"></tbody></table>
      </div>
    </div>

    <!-- SPICED -->
    <div id="view-spiced" class="hidden">
      <div class="table-header">
        <h2 id="spiced-title">SPICED — Ouverts les 3 dernières semaines</h2>
        <div class="filter-tabs">
          <button class="tab active" onclick="filterSpiced('all',this)">Tous</button>
          <button class="tab" onclick="filterSpiced('deal',this)">Deals</button>
          <button class="tab" onclick="filterSpiced('lead',this)">Leads</button>
          <button class="tab" onclick="filterSpiced('hot',this)">🔥 Score ≥ 3</button>
        </div>
      </div>
      <div style="padding:10px 20px;background:var(--cream);border-bottom:1px solid var(--border);font-size:11px;color:var(--muted)">
        <strong style="color:var(--slate)">Score SPICED :</strong>
        <span style="margin:0 8px"><span style="display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border-radius:50%;background:var(--brand);color:white;font-size:8px;font-weight:700;margin-right:3px">S</span>Situation</span>
        <span style="margin:0 8px"><span style="display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border-radius:50%;background:var(--brand);color:white;font-size:8px;font-weight:700;margin-right:3px">P</span>Pain</span>
        <span style="margin:0 8px"><span style="display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border-radius:50%;background:var(--brand);color:white;font-size:8px;font-weight:700;margin-right:3px">I</span>Impact</span>
        <span style="margin:0 8px"><span style="display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border-radius:50%;background:var(--brand);color:white;font-size:8px;font-weight:700;margin-right:3px">C</span>Critical Event</span>
        <span style="margin:0 8px"><span style="display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border-radius:50%;background:var(--brand);color:white;font-size:8px;font-weight:700;margin-right:3px">E</span>Expected Date</span>
        <span style="margin:0 8px"><span style="display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border-radius:50%;background:var(--brand);color:white;font-size:8px;font-weight:700;margin-right:3px">D</span>Decision</span>
        &nbsp;·&nbsp; <strong style="color:var(--slate)">Indice de closing</strong> = richesse SPICED × probabilité stage × fraîcheur × urgence
      </div>
      <div class="table-wrap">
        <table><thead><tr>
          <th onclick="sortSpiced('name')">Nom</th>
          <th>Type</th>
          <th onclick="sortSpiced('amount')">MRR / Statut</th>
          <th onclick="sortSpiced('hot')" class="sorted">Indice closing</th>
          <th onclick="sortSpiced('spiced_total')">SPICED</th>
          <th onclick="sortSpiced('close')">Échéance</th>
          <th>Dernier touch</th>
          <th>Next step</th>
          <th></th>
        </tr></thead><tbody id="spiced-tbody"></tbody></table>
      </div>
    </div>

  </div>
</div>

<div class="footer">Performance Starts With Clarity — talkspirit.com</div>

<script>
const DEALS   = __DEALS_JSON__;
const LEADS   = __LEADS_JSON__;
const METRICS = __METRICS_JSON__;

let dealFilter='all', dealSort='amount', dealAsc=false;
let leadFilter='all', leadSort='name',   leadAsc=false;
let spicedFilter='all', spicedSort='hot', spicedAsc=false;

const fireIds = new Set(
  (METRICS.alerts.fire||[]).concat(METRICS.alerts.no_decision||[]).map(d=>d.id)
);

// ── KPIs ──────────────────────────────────────────────────────────────────────
function renderKPIs() {
  const fire  = METRICS.alerts.fire.length;
  const noDec = METRICS.alerts.no_decision.length;
  const urg   = fire + noDec;
  const lc = METRICS.lead_counts;
  const newCount = lc.NEW || 0;
  const reactivityLabel = newCount === 0 ? '100% inbounds traités' : `${newCount} en attente`;
  const reactivityColor = newCount === 0 ? 'var(--success)' : 'var(--warning)';

  // Win Rate 90j
  const wr = METRICS.win_rate || {};
  const wrRate = (wr.rate !== null && wr.rate !== undefined) ? wr.rate : null;
  const wrColor = wrRate === null ? 'var(--muted)' : wrRate >= 50 ? 'var(--success)' : wrRate >= 30 ? 'var(--warning)' : 'var(--error)';
  const wrVal = wrRate !== null ? wrRate + '%' : '—';
  const wrSub = wr.total ? `${wr.won} Won · ${wr.lost} Lost sur 90j` : 'Aucun deal clôturé (90j)';

  // Pipeline Coverage
  const quota = METRICS.monthly_quota || 1000;
  const pipe  = METRICS.total_pipeline || 0;
  const cov   = pipe / quota;
  const covStr   = cov.toFixed(1) + '×';
  const covColor = cov >= 3 ? 'var(--success)' : cov >= 2 ? 'var(--warning)' : 'var(--error)';

  // New Pipeline MTD
  const npm    = METRICS.new_pipe_mtd || {};
  const npmMrr = npm.mrr || 0;
  const npmCnt = npm.count || 0;
  const npmPct = quota > 0 ? Math.round(npmMrr / quota * 100) : 0;

  document.getElementById('kpi-row').innerHTML = `
    <div class="kpi"><div class="kpi-label">Win Rate (90j)</div><div class="kpi-value" style="color:${wrColor}">${wrVal}</div><div class="kpi-sub">${wrSub}</div></div>
    <div class="kpi"><div class="kpi-label">Pipeline Coverage</div><div class="kpi-value" style="color:${covColor}">${covStr}</div><div class="kpi-sub">vs quota ${fmt(quota)} €/m · cible ≥3×</div></div>
    <div class="kpi"><div class="kpi-label">New Pipeline MTD</div><div class="kpi-value">${fmt(npmMrr)} €</div><div class="kpi-sub">${npmCnt} deal${npmCnt>1?'s':''} créés ce mois · ${npmPct}% quota</div></div>
    <div class="kpi"><div class="kpi-label">Urgences</div><div class="kpi-value ${urg>0?'brand':''}">${urg}</div><div class="kpi-sub">${fire} inactifs >14j · ${noDec} No Decision</div></div>
    <div class="kpi"><div class="kpi-label">Leads actifs</div><div class="kpi-value brand">${METRICS.total_leads}</div><div class="kpi-sub">NEW · IN_PROGRESS · CONNECTED</div></div>
    <div class="kpi"><div class="kpi-label">Réactivité inbound</div><div class="kpi-value" style="color:${reactivityColor}">${newCount === 0 ? '✓ 0' : newCount} NEW</div><div class="kpi-sub">${reactivityLabel}</div></div>
    <div class="kpi"><div class="kpi-label">Pipeline MRR</div><div class="kpi-value">${fmt(METRICS.total_pipeline)} €</div><div class="kpi-sub">/mois brut deals actifs</div></div>
    <div class="kpi"><div class="kpi-label">Forecast pondéré</div><div class="kpi-value">${fmt(METRICS.total_weighted)} €</div><div class="kpi-sub">${METRICS.total_pipeline?Math.round(METRICS.total_weighted/METRICS.total_pipeline*100):0}% du pipeline</div></div>
  `;
}

// ── Alerts ────────────────────────────────────────────────────────────────────
function renderAlerts() {
  const {fire,warn,no_step,no_decision} = METRICS.alerts;
  const rows = [];
  if (fire.length) rows.push(`<div class="alert-row fire"><span class="alert-title">🔴 Inactifs >14j</span><div class="alert-chips">${fire.map(chip).join('')}</div></div>`);
  if (warn.length) rows.push(`<div class="alert-row warn"><span class="alert-title">🟡 Inactifs 7–14j</span><div class="alert-chips">${warn.map(chip).join('')}</div></div>`);
  if (no_decision.length) rows.push(`<div class="alert-row fire"><span class="alert-title">⚠️ No Decision</span><div class="alert-chips">${no_decision.map(chip).join('')}</div></div>`);
  if (no_step.length) rows.push(`<div class="alert-row info"><span class="alert-title">📋 Sans next step</span><div class="alert-chips">${no_step.map(chip).join('')}</div></div>`);
  document.getElementById('alerts').innerHTML = rows.join('');
}
function chip(d) {
  const url = d.kind==='lead'?`https://app-eu1.hubspot.com/contacts/25761660/contact/${d.id}`:`https://app-eu1.hubspot.com/contacts/25761660/deal/${d.id}`;
  return `<a class="chip" href="${url}" target="_blank">${d.kind==='lead'?'👤 ':''}${d.name.replace(/- New Deal|- Nouvel.+/i,'').trim()}${d.amount?` <span style="color:var(--muted)">${fmt(d.amount)}€</span>`:''}</a>`;
}

// ── Bowtie ────────────────────────────────────────────────────────────────────
function renderBowtie() {
  const lc=METRICS.lead_counts, acq=METRICS.acq, exp=METRICS.exp;
  const aO=["3112501453","3112501454","3112501455","3112501456","3112501457"];
  const aL=["Disc. Sch.","Disc. Compl.","Sol. Fit","Proposal","Negoc."];
  const eO=["462257366","462257390","462257370"];
  const eL=["Upsell Id.","Negoc. AM","Contract"];
  const acqS = aO.map((id,i)=>{const s=acq[id]||{count:0,mrr:0};return `<div class="bt-stage" onclick="drillBowtie('deal','${id}','${aL[i]}')"><div class="count">${s.count}</div><div class="label">${aL[i]}</div>${s.mrr?`<div class="mrr">${fmt(Math.round(s.mrr))}€</div>`:''}</div>`;}).join('<span style="align-self:center;color:var(--border);padding-top:28px">›</span>');
  const expS = eO.map((id,i)=>{const s=exp[id]||{count:0,mrr:0};return `<div class="bt-stage" onclick="drillBowtie('deal','${id}','${eL[i]}')"><div class="count">${s.count}</div><div class="label">${eL[i]}</div>${s.mrr?`<div class="mrr">${fmt(Math.round(s.mrr))}€</div>`:''}</div>`;}).join('<span style="align-self:center;color:var(--border);padding-top:28px">‹</span>');
  document.getElementById('bowtie').innerHTML = `
    <div class="bt-group"><div class="bt-group-label">Leads</div><div class="bt-stages">
      <div class="bt-stage lead-new" onclick="drillBowtie('lead','NEW','New')"><div class="count">${lc.NEW||0}</div><div class="label">NEW</div></div>
      <div class="bt-stage lead-prog" onclick="drillBowtie('lead','IN_PROGRESS','In Progress')"><div class="count">${lc.IN_PROGRESS||0}</div><div class="label">IN PROG.</div></div>
      <div class="bt-stage lead-conn" onclick="drillBowtie('lead','CONNECTED','Connected')"><div class="count">${lc.CONNECTED||0}</div><div class="label">CONNECTED</div></div>
    </div></div>
    <div class="bt-divider">→</div>
    <div class="bt-group"><div class="bt-group-label">Acquisition — New Biz</div><div class="bt-stages">${acqS}</div></div>
    <div class="bt-knot"><div class="bt-knot-inner"><div class="k-val">${fmt(METRICS.total_weighted)}€</div><div class="k-sub">Weighted</div></div></div>
    <div class="bt-divider">→</div>
    <div class="bt-group"><div class="bt-group-label">Expansion — AM</div><div class="bt-stages">${expS}</div></div>
  `;
}

let _drillActive = null;

function drillBowtie(kind, key, label) {
  const panel = document.getElementById('bt-drill');
  // Toggle off if same stage clicked again
  if(_drillActive === kind+key){
    panel.classList.add('hidden');
    panel.innerHTML='';
    _drillActive=null;
    document.querySelectorAll('.bt-stage').forEach(s=>s.classList.remove('active-drill'));
    return;
  }
  _drillActive = kind+key;
  document.querySelectorAll('.bt-stage').forEach(s=>s.classList.remove('active-drill'));
  event.currentTarget.classList.add('active-drill');

  let items=[];
  if(kind==='lead'){
    items = LEADS.filter(l=>l.status===key);
  } else {
    items = DEALS.filter(d=>d.stage_id===key);
  }

  if(!items.length){
    panel.innerHTML=`<div class="bt-drill"><div class="bt-drill-header"><span class="bt-drill-title">${label} — aucun élément</span><button class="bt-drill-close" onclick="drillBowtie('${kind}','${key}','${label}')">×</button></div></div>`;
    panel.classList.remove('hidden');
    return;
  }

  const rows = items.map(x=>{
    const lt = x.last_touch;
    const days = lt ? lt.days_ago : 999;
    const touchIcon = lt?(lt.channel==='email'?'📧':'📞'):'—';
    const touchLbl = lt ? `${touchIcon} ${days<999?days+'j':''}` : '—';
    const ns = x.next_step;
    const nsLbl = ns ? `<span style="font-size:11px;color:var(--slate)">${ns.subject.slice(0,45)}${ns.subject.length>45?'…':''}</span>${ns.due_date?`<span style="font-size:10px;color:${ns.overdue?'var(--error)':'var(--muted)'};margin-left:4px">${ns.due_date}</span>`:''}` : '<span style="color:var(--muted);font-size:11px">—</span>';
    const name = kind==='deal'?x.name.replace(/- New Deal|- Nouvel.+|- Nouvel élément.+/i,'').trim():x.name;
    const sub = kind==='deal'
      ? `<span style="font-family:'DM Mono',monospace;font-weight:600;color:var(--brand);font-size:12px">${fmt(x.amount)}€</span>`
      : `<span style="font-size:11px;color:var(--muted)">${x.company||x.jobtitle||''}</span>`;
    return `<tr>
      <td><div style="font-weight:500;color:var(--slate)">${name}</div></td>
      <td>${sub}</td>
      <td style="font-size:11px;color:var(--muted)">${touchLbl}<div style="font-size:10px;color:var(--muted)">${lt?lt.label.slice(0,30):''}</div></td>
      <td>${nsLbl}</td>
      <td><a class="hs-link" href="${x.hs_url}" target="_blank">↗</a></td>
    </tr>`;
  }).join('');

  const totalMrr = kind==='deal' ? items.reduce((s,x)=>s+x.amount,0) : 0;
  const mrrStr = totalMrr>0 ? ` — ${fmt(Math.round(totalMrr))}€ MRR` : '';

  panel.innerHTML=`<div class="bt-drill">
    <div class="bt-drill-header">
      <span class="bt-drill-title">${label} — ${items.length} élément${items.length>1?'s':''}${mrrStr}</span>
      <button class="bt-drill-close" onclick="drillBowtie('${kind}','${key}','${label}')">×</button>
    </div>
    <table><thead><tr>
      <th>Nom</th><th>${kind==='deal'?'MRR':'Entreprise'}</th><th>Dernier touch</th><th>Next step</th><th></th>
    </tr></thead><tbody>${rows}</tbody></table>
  </div>`;
  panel.classList.remove('hidden');
  panel.scrollIntoView({behavior:'smooth',block:'nearest'});
}

// ── View switch ───────────────────────────────────────────────────────────────
function switchView(view, btn) {
  document.querySelectorAll('.view-tab').forEach(t=>t.classList.remove('active'));
  btn.classList.add('active');
  ['deals','leads','spiced'].forEach(v=>document.getElementById('view-'+v).classList.toggle('hidden',v!==view));
}

// ── Deals ─────────────────────────────────────────────────────────────────────
function filterDeals(f,btn){dealFilter=f;document.querySelectorAll('#view-deals .tab').forEach(t=>t.classList.remove('active'));btn.classList.add('active');renderDeals();}
function sortDeals(k){if(dealSort===k)dealAsc=!dealAsc;else{dealSort=k;dealAsc=false;}document.querySelectorAll('#view-deals thead th').forEach(th=>{th.classList.remove('sorted','asc');if(th.getAttribute('onclick')===`sortDeals('${k}')`){th.classList.add('sorted');if(dealAsc)th.classList.add('asc');}});renderDeals();}
function renderDeals(){
  let rows=[...DEALS];
  if(dealFilter==='acq') rows=rows.filter(d=>d.cat==='acq');
  else if(dealFilter==='exp') rows=rows.filter(d=>d.cat==='exp');
  else if(dealFilter==='fire') rows=rows.filter(d=>fireIds.has(d.id)||(d.last_touch&&d.last_touch.days_ago>7)||d.overdue||!d.next_step);
  else if(dealFilter==='spiced') rows=rows.filter(d=>d.spiced.total>=3);
  rows.sort((a,b)=>{let av=a[dealSort]??'',bv=b[dealSort]??'';if(dealSort==='amount'){av=a.amount;bv=b.amount;}if(dealSort==='close'){av=a.close||'9999';bv=b.close||'9999';}if(dealSort==='spiced_total'){av=a.spiced.total;bv=b.spiced.total;}const c=av<bv?-1:av>bv?1:0;return dealAsc?c:-c;});
  document.getElementById('deals-title').textContent=`Deals actifs (${rows.length})`;
  document.getElementById('deals-count').textContent=`(${DEALS.length})`;
  document.getElementById('deals-tbody').innerHTML=rows.map(dealRow).join('');
}
function dealRow(d){
  const lt=d.last_touch,ns=d.next_step,days=lt?lt.days_ago:999;
  const tc=days>14?'hot':days>7?'warm':days<=7?'ok':'none';
  const icon=lt?(lt.channel==='email'?'📧':'📞'):'—';
  const sc=d.cat==='exp'?'stage-exp':d.cat==='dead'?'stage-dead':'stage-acq';
  const name=d.name.replace(/- New Deal|- Nouvel.+|- Nouvel élément.+/i,'').trim();
  const hi=fireIds.has(d.id)?' class="highlight"':'';
  const nsHTML=ns?`<div class="ns-subject">${ns.subject}</div><div class="ns-due ${ns.overdue?'overdue':'ok'}">${ns.due_date?(ns.overdue?'⚠ ':'')+ns.due_date:'Pas de date'}</div>`:`<div class="ns-empty">Aucun next step</div>`;
  const dSp=d.spiced;
  const dDims=['S','P','I','C','E','D'].map(k=>`<span class="sd ${dSp[k]?'on':'off'}" data-tip="${dSp.why[k]}">${k}</span>`).join('');
  const dSpHTML=`<div class="spiced-dims">${dDims}<span class="sd-score ${dSp.total>=5?'full':''}">${dSp.total}/6</span></div>`;
  return `<tr${hi}><td><div class="deal-name"><a href="${d.hs_url}" target="_blank">${name}</a></div><div class="deal-sub">${d.pipeline}</div></td><td><span class="mrr">${fmt(d.amount)} €</span></td><td><span class="stage-badge ${sc}">${d.stage}</span></td><td><span class="close-date ${d.overdue?'overdue':'ok'}">${d.close||'—'}${d.overdue?' ⚠':''}</span></td><td><div class="touch ${tc}"><div><span>${icon}</span> <span class="touch-days">${days<999?days+'j':'jamais'}</span></div><div class="touch-label">${lt?lt.label:''}</div></div></td><td><div class="next-step">${nsHTML}</div></td><td>${dSpHTML}</td><td><a class="hs-link" href="${d.hs_url}" target="_blank">↗</a></td></tr>`;
}

// ── Leads ─────────────────────────────────────────────────────────────────────
function filterLeads(f,btn){leadFilter=f;document.querySelectorAll('#view-leads .tab').forEach(t=>t.classList.remove('active'));btn.classList.add('active');renderLeads();}
function sortLeads(k){if(leadSort===k)leadAsc=!leadAsc;else{leadSort=k;leadAsc=false;}document.querySelectorAll('#view-leads thead th').forEach(th=>{th.classList.remove('sorted','asc');if(th.getAttribute('onclick')===`sortLeads('${k}')`){th.classList.add('sorted');if(leadAsc)th.classList.add('asc');}});renderLeads();}
function renderLeads(){
  let rows=[...LEADS];
  if(leadFilter==='fire') rows=rows.filter(l=>fireIds.has(l.id)||(l.last_touch&&l.last_touch.days_ago>7)||!l.next_step);
  else if(leadFilter==='spiced') rows=rows.filter(l=>l.spiced.total>=3);
  else if(leadFilter!=='all') rows=rows.filter(l=>l.status===leadFilter);
  rows.sort((a,b)=>{let av=a[leadSort]??'',bv=b[leadSort]??'';if(leadSort==='spiced_total'){av=a.spiced.total;bv=b.spiced.total;}const c=av<bv?-1:av>bv?1:0;return leadAsc?c:-c;});
  document.getElementById('leads-title').textContent=`Leads en qualification (${rows.length})`;
  document.getElementById('leads-count').textContent=`(${LEADS.length})`;
  document.getElementById('leads-tbody').innerHTML=rows.map(leadRow).join('');
}
function leadRow(l){
  const lt=l.last_touch,ns=l.next_step,days=lt?lt.days_ago:999;
  const tc=days>14?'hot':days>7?'warm':days<=7?'ok':'none';
  const icon=lt?(lt.channel==='email'?'📧':'📞'):'—';
  const hi=fireIds.has(l.id)?' class="highlight"':'';
  const nsHTML=ns?`<div class="ns-subject">${ns.subject}</div><div class="ns-due ${ns.overdue?'overdue':'ok'}">${ns.due_date?(ns.overdue?'⚠ ':'')+ns.due_date:'Pas de date'}</div>`:`<div class="ns-empty">Aucun next step</div>`;
  const lSp=l.spiced;
  const lDims=['S','P','I','C','E','D'].map(k=>`<span class="sd ${lSp[k]?'on':'off'}" data-tip="${lSp.why[k]}">${k}</span>`).join('');
  const lSpHTML=`<div class="spiced-dims">${lDims}<span class="sd-score ${lSp.total>=5?'full':''}">${lSp.total}/6</span></div>`;
  return `<tr${hi}><td><div class="deal-name"><a href="${l.hs_url}" target="_blank">${l.name}</a></div><div class="deal-sub">${l.jobtitle||''}</div></td><td><div style="font-weight:500;font-size:12px">${l.company||'—'}</div></td><td><span class="stage-badge" style="background:${l.status_bg};color:${l.status_color};border:1px solid ${l.status_border}">${l.status_label}</span></td><td><span class="close-date ok">${l.created||'—'}</span></td><td><div class="touch ${tc}"><div><span>${icon}</span> <span class="touch-days">${days<999?days+'j':'jamais'}</span></div><div class="touch-label">${lt?lt.label:''}</div></div></td><td><div class="next-step">${nsHTML}</div></td><td>${lSpHTML}</td><td><a class="hs-link" href="${l.hs_url}" target="_blank">↗</a></td></tr>`;
}

// ── SPICED ────────────────────────────────────────────────────────────────────
function filterSpiced(f,btn){spicedFilter=f;document.querySelectorAll('#view-spiced .tab').forEach(t=>t.classList.remove('active'));btn.classList.add('active');renderSpiced();}
function sortSpiced(k){if(spicedSort===k)spicedAsc=!spicedAsc;else{spicedSort=k;spicedAsc=false;}document.querySelectorAll('#view-spiced thead th').forEach(th=>{th.classList.remove('sorted','asc');if(th.getAttribute('onclick')===`sortSpiced('${k}')`){th.classList.add('sorted');if(spicedAsc)th.classList.add('asc');}});renderSpiced();}

function renderSpiced(){
  const recent21 = [...DEALS,...LEADS].filter(x=>x.days_open<=21);
  let rows=[...recent21];
  if(spicedFilter==='deal') rows=rows.filter(x=>x.kind==='deal');
  else if(spicedFilter==='lead') rows=rows.filter(x=>x.kind==='lead');
  else if(spicedFilter==='hot') rows=rows.filter(x=>x.spiced.hot>=3);

  rows.sort((a,b)=>{
    let av, bv;
    if(spicedSort==='hot'){av=a.spiced.hot;bv=b.spiced.hot;}
    else if(spicedSort==='spiced_total'){av=a.spiced.total;bv=b.spiced.total;}
    else if(spicedSort==='amount'){av=a.amount||0;bv=b.amount||0;}
    else if(spicedSort==='close'){av=a.close||a.created||'9999';bv=b.close||b.created||'9999';}
    else{av=a[spicedSort]??'';bv=b[spicedSort]??'';}
    const c=av<bv?-1:av>bv?1:0;
    return spicedAsc?c:-c;
  });

  document.getElementById('spiced-title').textContent=`SPICED — Ouverts dans les 3 dernières semaines (${rows.length})`;
  document.getElementById('spiced-count').textContent=`(${recent21.length})`;
  document.getElementById('spiced-tbody').innerHTML=rows.map(spicedRow).join('');
}

const SPICED_Q={
  S:{deal:"Pourriez-vous me donner quelques précisions sur votre contexte ? Combien d'utilisateurs seraient concernés, et quels outils utilisez-vous actuellement ?",
     lead:"Pourriez-vous me parler brièvement de votre organisation et de votre rôle dans ce projet ?"},
  P:{deal:"Quels sont les principaux défis que vous cherchez à résoudre ? Qu'est-ce qui ne fonctionne pas bien avec votre organisation ou vos outils actuels ?",
     lead:"Qu'est-ce qui vous a amené à chercher une solution comme Talkspirit ? Quels problèmes cherchez-vous à adresser ?"},
  I:{deal:"Si ce projet aboutit, quel serait l'impact concret pour votre organisation — gain de temps, réduction de friction, meilleure coordination entre équipes ?",
     lead:"Comment estimeriez-vous la valeur de ce projet pour votre organisation si vous le menez à bien ?"},
  C:{deal:"Y a-t-il un événement particulier qui donne de l'urgence à ce projet — renouvellement de contrat, lancement, réorganisation interne, ou contrainte réglementaire ?",
     lead:"Y a-t-il une échéance ou un déclencheur précis qui rend ce projet prioritaire pour vous en ce moment ?"},
  E:{deal:"À quel horizon envisagez-vous de prendre une décision ? Avez-vous une date de démarrage idéale en tête ?",
     lead:"À quel moment souhaiteriez-vous idéalement avancer sur ce projet ?"},
  D:{deal:"Qui sera impliqué dans la décision finale de votre côté ? Y a-t-il un comité ou un processus de validation particulier à anticiper ?",
     lead:"Qui d'autre sera impliqué dans ce projet ? Y a-t-il un décideur à intégrer dans nos échanges ?"}
};
const SPICED_LBL={S:'Situation',P:'Pain',I:'Impact',C:'Critical Event',E:'Expected Date',D:'Decision'};

function toggleQ(id){const r=document.getElementById('q-'+id);if(r)r.classList.toggle('hidden');}

function buildQDraft(x){
  const sp=x.spiced, kind=x.kind;
  const missing=['S','P','I','C','E','D'].filter(k=>sp[k]===0);
  if(!missing.length) return 'Toutes les dimensions SPICED sont renseignées.';
  const name=kind==='deal'?x.name.replace(/- New Deal|- Nouvel.+|- Nouvel élément.+/i,'').trim():x.name;
  const qs=missing.map(k=>`• ${SPICED_LBL[k]}\n  ${SPICED_Q[k][kind]}`).join('\n\n');
  return `Bonjour,\n\nAfin d'avancer au mieux sur votre projet, j'aurais quelques questions :\n\n${qs}\n\nMerci d'avance pour ces précisions.\n\nCordialement,\nEloi`;
}

function copyQDraft(id,xJson){
  const x=JSON.parse(decodeURIComponent(xJson));
  navigator.clipboard.writeText(buildQDraft(x)).then(()=>{
    const btn=document.getElementById('qcopy-'+id);
    if(btn){btn.textContent='✓ Copié';btn.classList.add('ok');setTimeout(()=>{btn.textContent='Copier le draft';btn.classList.remove('ok');},2500);}
  });
}

function spicedRow(x){
  const sp=x.spiced, lt=x.last_touch, ns=x.next_step, days=lt?lt.days_ago:999;
  const tc=days>14?'hot':days>7?'warm':days<=7?'ok':'none';
  const icon=lt?(lt.channel==='email'?'📧':'📞'):'—';
  const hi=fireIds.has(x.id)?' class="highlight"':'';
  const name=x.kind==='deal'?x.name.replace(/- New Deal|- Nouvel.+|- Nouvel élément.+/i,'').trim():x.name;

  // Type badge + public sector indicator
  const typeBadge=x.kind==='deal'?`<span class="type-badge type-deal">Deal</span>`:`<span class="type-badge type-lead">Lead</span>`;
  const pubBadge=sp.pub?`<span title="Secteur public — cycle de décision plus long (×0.55 sur l'indice de closing)" style="font-size:11px;margin-left:5px;cursor:default" aria-label="Secteur public">🏛</span>`:``;

  // MRR or status
  const mrrOrStatus = x.kind==='deal'
    ? `<span class="mrr">${fmt(x.amount)} €</span><div class="deal-sub" style="margin-top:3px"><span class="stage-badge ${x.cat==='exp'?'stage-exp':'stage-acq'}" style="font-size:9px">${x.stage}</span></div>`
    : `<span class="stage-badge" style="background:${x.status_bg};color:${x.status_color};border:1px solid ${x.status_border};font-size:9px">${x.status_label}</span>${x.company?`<div class="deal-sub" style="margin-top:3px">${x.company}</div>`:''}`;

  // SPICED dims
  const dims=['S','P','I','C','E','D'].map(k=>
    `<span class="sd ${sp[k]?'on':'off'}" data-tip="${sp.why[k]}">${k}</span>`
  ).join('');
  const scoreClass=sp.total>=5?'full':'';
  const spicedHTML=`<div class="spiced-dims">${dims}<span class="sd-score ${scoreClass}">${sp.total}/6</span></div>`;

  // Hot score bar
  const hotColor=sp.hot>=6?'#F56220':sp.hot>=3?'#F99F07':sp.hot>=1?'#AA7F65':'#CDB8A4';
  const hotLabel=sp.hot>=6?'🔥 Cette semaine':sp.hot>=3?'⚡ Bientôt':sp.hot>=1?'🌡 En cours':'❄ Froid';
  const hotPct=Math.min(100,Math.round(sp.hot*10));
  const hotHTML=`<div class="hot-wrap"><div class="hot-bar-outer"><div class="hot-bar-inner" style="width:${hotPct}%;background:${hotColor}"></div></div><div class="hot-meta"><span class="hot-val" style="color:${hotColor}">${sp.hot}</span><span class="hot-lbl">${hotLabel}</span></div></div>`;

  // Close / date
  const dateStr = x.close || x.created || '—';
  const dateClass = (x.overdue) ? 'overdue' : 'ok';

  // Next step
  const nsHTML=ns
    ?`<div class="ns-subject">${ns.subject}</div><div class="ns-due ${ns.overdue?'overdue':'ok'}">${ns.due_date?(ns.overdue?'⚠ ':'')+ns.due_date:'Pas de date'}</div>`
    :`<div class="ns-empty">Aucun next step</div>`;

  // Questions button — only if there are missing dimensions
  const missing=['S','P','I','C','E','D'].filter(k=>sp[k]===0);
  const qBtn=missing.length>0
    ? `<button class="q-toggle-btn" onclick="toggleQ('${x.id}')" title="Questions pour enrichir le SPICED">✉ ${missing.length} question${missing.length>1?'s':''}</button>`
    : '';

  // Questions panel (hidden by default)
  const xJson=encodeURIComponent(JSON.stringify(x));
  const draft=buildQDraft(x).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const qPanel=missing.length>0?`
<tr id="q-${x.id}" class="q-row hidden">
  <td colspan="9">
    <div class="q-panel">
      <div class="q-label">Questions à poser pour enrichir le SPICED — ${missing.map(k=>SPICED_LBL[k]).join(', ')}</div>
      <div class="q-draft-box">${draft}</div>
      <div class="q-actions">
        <button id="qcopy-${x.id}" class="q-copy" onclick="copyQDraft('${x.id}','${xJson}')">Copier le draft</button>
        <button class="q-close-btn" onclick="toggleQ('${x.id}')">Fermer</button>
      </div>
    </div>
  </td>
</tr>`:'';

  return `<tr${hi}>
    <td><div class="deal-name"><a href="${x.hs_url}" target="_blank">${name}</a>${pubBadge}</div><div class="deal-sub">${x.days_open}j</div></td>
    <td>${typeBadge}</td>
    <td>${mrrOrStatus}</td>
    <td>${hotHTML}</td>
    <td>${spicedHTML}</td>
    <td><span class="close-date ${dateClass}">${dateStr}${x.overdue?' ⚠':''}</span></td>
    <td><div class="touch ${tc}"><div><span>${icon}</span> <span class="touch-days">${days<999?days+'j':'jamais'}</span></div><div class="touch-label">${lt?lt.label:''}</div></div></td>
    <td><div class="next-step">${nsHTML}</div></td>
    <td style="white-space:nowrap"><a class="hs-link" href="${x.hs_url}" target="_blank">↗</a>${qBtn}</td>
  </tr>${qPanel}`;
}

function fmt(n){return Number(n).toLocaleString('fr-FR');}

function hardRefresh(){
  const btn=document.getElementById('refresh-btn');
  const icon=document.getElementById('refresh-icon');
  btn.disabled=true;
  icon.style.animation='spin .8s linear infinite';
  const style=document.createElement('style');
  style.textContent='@keyframes spin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}';
  document.head.appendChild(style);
  const url=new URL(window.location.href);
  url.searchParams.set('t', Date.now());
  window.location.replace(url.toString());
}

// ── Init ──────────────────────────────────────────────────────────────────────
renderKPIs();
renderAlerts();
renderBowtie();
renderDeals();
renderLeads();
renderSpiced();
</script>
</body>
</html>
"""


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not TOKEN:
        print("ERROR: HUBSPOT_TOKEN not set")
        return

    deals  = fetch_deals()
    leads  = fetch_leads()
    leads  = filter_leads_without_deals(leads)
    deals  = enrich_deals(deals)
    leads  = enrich_leads(leads)

    win_rate     = fetch_win_rate()
    new_pipe_mtd = fetch_new_pipeline_mtd()

    metrics = compute_metrics(deals, leads)
    metrics["win_rate"]      = win_rate
    metrics["new_pipe_mtd"]  = new_pipe_mtd
    metrics["monthly_quota"] = MONTHLY_QUOTA

    d_rows  = serialise_deals(deals)
    l_rows  = serialise_leads(leads)

    html = HTML.replace("__GENERATED_AT__", metrics["generated_at"])
    html = html.replace("__DEALS_JSON__",   json.dumps(d_rows,  ensure_ascii=False))
    html = html.replace("__LEADS_JSON__",   json.dumps(l_rows,  ensure_ascii=False))
    html = html.replace("__METRICS_JSON__", json.dumps(metrics, ensure_ascii=False, default=str))

    OUT.write_text(html, encoding="utf-8")
    print(f"\n✓ Dashboard → {OUT}")
    print(f"  Leads: {len(l_rows)} · Deals: {len(d_rows)}")
    print(f"  Pipeline {metrics['total_pipeline']}€ · Weighted {metrics['total_weighted']}€")
    recent = [x for x in d_rows + l_rows if x.get("days_open", 999) <= 21]
    hot    = [x for x in recent if x["spiced"]["hot"] >= 3]
    print(f"  Récents (21j): {len(recent)} · Score ≥3 (🔥⚡): {len(hot)}")


if __name__ == "__main__":
    main()

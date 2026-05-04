"""One-off gap-fill + accuracy validator for qualified hydroponics leads.

Runs after the main pipeline + enrich_all when the qualified set still has
rows missing location/phone/email or rows that look like articles/job
listings. For each row, asks claude (with WebSearch + WebFetch) to:
  1. Verify the row is a real buying entity (not an article, job posting,
     directory listing, or department-page-with-no-procurement).
  2. Fill in any missing location / phone / email from authoritative
     sources (the company's own website first, then verified third-party
     directories).

Rows that fail the buying-entity check are demoted to status='new' so
they don't ship as outreach. Rows that pass keep their status and gain
whatever verified contact data was found.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from market_validation.company_enrichment import (
    _run_ai_prompt,
    is_plausible_email,
)
from market_validation._helpers.contacts import (
    detect_country,
    is_valid_phone_intl,
    normalize_phone,
)

DB_PATH = "output/market-research.sqlite3"
HYDRO_RIDS = ("a368de47", "03c9e724", "2e710faa")


def build_prompt(company_name: str, website: str | None,
                 missing: list[str], existing: dict[str, str | None]) -> str:
    have_lines = [f"- {k}: {v}" for k, v in existing.items() if v]
    have_block = "\n".join(have_lines) if have_lines else "(nothing yet)"
    website_line = website or "(no website on file — search for one)"
    return f"""Research the entity below. Use WebSearch and WebFetch.

Name: {company_name}
Website on file: {website_line}

Already known:
{have_block}

Missing fields to fill: {", ".join(missing)}

Two tasks:

1. Decide if this is a real BUYING ENTITY for B2B outreach. A buying
   entity is a company / lab / nursery / facility that has procurement
   authority and could plausibly purchase capital equipment. NOT a
   buying entity:
     - news article, blog post, or press release
     - job posting page
     - directory / aggregator listing without an underlying business
     - "category" page like "Research | Department of X" that doesn't
       represent a single procuring lab
     - article/event page (e.g. "Urban Farms to Highlight Grow Local OC")

2. If it IS a buying entity, find the missing fields by visiting the
   company's own site first, then a verified secondary source (LinkedIn,
   Google Business, official directory). Do NOT pull contact info from
   review aggregators (birdeye, yelp, manta) — those leak third-party
   addresses.

Email rule: the email's domain must match the company's website domain
OR be a standard info-mailbox pattern (info@, contact@, sales@, hello@)
on the same domain. If the only email you can find is on a third-party
domain, return null for email rather than the wrong-domain address.

Return ONLY this JSON shape:
{{
  "is_buying_entity": true | false,
  "reason": "one short sentence",
  "location": "full street address or null",
  "phone": "phone number or null",
  "email": "email address or null",
  "website": "canonical website URL or null"
}}"""


def run_one(row: dict) -> dict:
    missing = []
    if not (row["location"] or "").strip(): missing.append("location")
    if not (row["phone"] or "").strip(): missing.append("phone")
    if not (row["email"] or "").strip(): missing.append("email")

    existing = {
        "location": row["location"],
        "phone": row["phone"],
        "email": row["email"],
        "website": row["website"],
    }

    prompt = build_prompt(row["company_name"], row["website"], missing, existing)
    try:
        raw = _run_ai_prompt(prompt, timeout=120)
    except Exception as exc:
        return {"id": row["id"], "error": f"subprocess: {exc}"}

    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        return {"id": row["id"], "error": f"no_json: {raw[:200]}"}
    try:
        data = json.loads(raw[start : end + 1])
    except json.JSONDecodeError as exc:
        return {"id": row["id"], "error": f"json_parse: {exc}"}

    return {"id": row["id"], "name": row["company_name"], "data": data,
            "website": row["website"]}


def website_domain(url: str | None) -> str | None:
    if not url:
        return None
    s = url.lower().split("//", 1)[-1].split("/", 1)[0]
    return s.removeprefix("www.")


_FREEMAIL_DOMAINS = frozenset({
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
    "aol.com", "icloud.com", "protonmail.com", "proton.me",
})


def email_domain_ok(email: str | None, website: str | None,
                    company_name: str | None = None) -> bool:
    """Per project_data_quality memory rule.

    Acceptable when one of:
      (a) email's root domain matches website's root domain (subdomains OK
          either side);
      (b) email is on a freemail domain AND the local-part contains tokens
          from the company name (small business mailbox pattern);
      (c) both email and website are on .edu domains (academic units publish
          on different institutional subdomains than their main address).
    """
    if not email or "@" not in email:
        return False
    edom = email.rsplit("@", 1)[1].lower()
    local = email.rsplit("@", 1)[0].lower()
    wdom = website_domain(website)
    if not wdom:
        return True  # no website to compare; trust the address
    # (a) subdomain relationship
    if edom == wdom or edom.endswith("." + wdom) or wdom.endswith("." + edom):
        return True
    # (b) freemail + name-token match
    if edom in _FREEMAIL_DOMAINS and company_name:
        name_tokens = [
            t for t in re.findall(r"[a-z0-9]+", company_name.lower())
            if len(t) >= 3
        ]
        local_norm = re.sub(r"[^a-z0-9]+", "", local)
        if any(tok in local_norm for tok in name_tokens):
            return True
    # (c) academic (.edu ↔ .edu)
    if edom.endswith(".edu") and wdom.endswith(".edu"):
        return True
    return False


def apply_result(conn: sqlite3.Connection, r: dict, geography_hint: str = "California"):
    if "error" in r:
        print(f"  ERR  {r['id']}  {r['error']}")
        return "error"

    cid = r["id"]
    name = r["name"]
    d = r["data"]

    if not d.get("is_buying_entity", True):
        conn.execute(
            "UPDATE companies SET status='new', notes = COALESCE(notes,'') || ' | demoted: ' || ? WHERE id=?",
            (d.get("reason", "not a buying entity")[:200], cid),
        )
        # Also reject any pending drafts for this company so they don't ship.
        conn.execute(
            "UPDATE emails SET status='rejected' WHERE company_id=? AND status='pending'",
            (cid,),
        )
        print(f"  DEMOTE  {cid}  {name[:50]}  reason={d.get('reason','')[:80]}")
        return "demoted"

    updates = []
    values = []
    if d.get("location"):
        updates.append("location = ?")
        values.append(d["location"])
    if d.get("phone"):
        ph = normalize_phone(d["phone"], country_hint=detect_country(d.get("location") or geography_hint))
        if ph and is_valid_phone_intl(ph):
            updates.append("phone = ?")
            values.append(ph)
    if d.get("email"):
        em = d["email"].strip()
        if is_plausible_email(em) and email_domain_ok(
            em, r.get("website") or d.get("website"), company_name=name
        ):
            updates.append("email = ?")
            values.append(em)
        else:
            print(f"  REJECT_EMAIL  {cid}  {name[:50]}  {em} (cross-domain or junk)")
    if d.get("website") and not r.get("website"):
        updates.append("website = ?")
        values.append(d["website"])

    if updates:
        values.append(cid)
        conn.execute(f"UPDATE companies SET {', '.join(updates)} WHERE id = ?", values)
        print(f"  FILL  {cid}  {name[:50]}  +{','.join(u.split('=')[0].strip() for u in updates)}")
        return "filled"
    print(f"  NOOP  {cid}  {name[:50]}  (nothing to apply)")
    return "noop"


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    placeholders = ",".join("?" * len(HYDRO_RIDS))
    rows = conn.execute(
        f"""SELECT id, research_id, company_name, website, location, phone, email
            FROM companies
            WHERE status='qualified' AND research_id IN ({placeholders})
              AND ((location IS NULL OR location='')
                OR (phone IS NULL OR phone='')
                OR (email IS NULL OR email=''))""",
        HYDRO_RIDS,
    ).fetchall()
    rows = [dict(r) for r in rows]
    print(f"Gap-filling {len(rows)} qualified rows")

    results = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(run_one, r): r for r in rows}
        for fut in as_completed(futures):
            results.append(fut.result())

    print()
    print("Applying updates:")
    counts = {"filled": 0, "demoted": 0, "noop": 0, "error": 0}
    for r in results:
        outcome = apply_result(conn, r)
        counts[outcome] = counts.get(outcome, 0) + 1
    conn.commit()
    print()
    print(f"Summary: {counts}")


if __name__ == "__main__":
    sys.exit(main() or 0)

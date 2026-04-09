#!/usr/bin/env python3
"""
apollo.py — Enrich CRM contacts with professional data via Apollo.io.

Uses the bulk_match API (10 per batch) for efficiency.

Fields enriched:
  - company, role (primary columns)
  - LinkedIn URL (merged into social_profiles JSON)
  - address (city/state/country, if empty)
  - emails (merged, deduplicated)
  - phones (only with --reveal-phones)

Supplementary data stored in notes (## Apollo Enrichment block):
  - headline, seniority, department
  - employment history
  - org industry, employee count, revenue, technologies, founded year

Idempotent: skips contacts with apollo_enriched_at set (unless --force).
Respects confidence hierarchy: never overwrites iCloud or Gmail-sourced fields.

Usage:
  python3 -m enrichment.apollo [--limit 500] [--heat hot,warm] [--force] [--reveal-phones] [--dry-run]
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "--break-system-packages", "requests"])
    import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, GENERIC_DOMAINS, get_conn, get_secret

APOLLO_BASE = "https://api.apollo.io/api/v1"
BATCH_SIZE = 10
RATE_LIMIT_SECONDS = 0.1  # 1000 req/min limit; 100ms is very safe
LOG_PATH = os.path.join(os.path.dirname(DB_PATH), "..", "logs", "crm-apollo.log")

# Sources that outrank Apollo in the confidence hierarchy
PROTECTED_SOURCES = {"icloud", "gmail"}

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_api_key():
    key = get_secret("APOLLO_API_KEY")
    if not key:
        log.error("No APOLLO_API_KEY found. Add it to .env or export it.")
        sys.exit(1)
    return key


def ensure_apollo_column(conn):
    """Add apollo_enriched_at column if it doesn't exist."""
    try:
        conn.execute("ALTER TABLE contacts ADD COLUMN apollo_enriched_at TEXT")
        conn.commit()
        log.info("Added apollo_enriched_at column.")
    except Exception:
        pass


# ── Contact selection ────────────────────────────────────────────────────────

def get_contacts_to_enrich(conn, limit, heat_tiers=None, force=False):
    """Get contacts needing Apollo enrichment, prioritized by heat and score."""
    conditions = []
    params = []

    if not force:
        conditions.append("(apollo_enriched_at IS NULL OR apollo_enriched_at = '')")

    # Must have at least one identifier Apollo can use
    conditions.append(
        "(emails != '[]' AND emails IS NOT NULL AND emails != '') "
        "OR (company IS NOT NULL AND company != '' AND name IS NOT NULL AND name != '')"
    )

    if heat_tiers:
        placeholders = ",".join("?" * len(heat_tiers))
        conditions.append(f"relationship_heat IN ({placeholders})")
        params.extend(heat_tiers)

    where = " AND ".join(f"({c})" for c in conditions)
    query = f"""
        SELECT id, name, emails, phones, company, role, address,
               social_profiles, personal_data_source, notes
        FROM contacts
        WHERE {where}
        ORDER BY
            CASE relationship_heat
                WHEN 'hot' THEN 1 WHEN 'warm' THEN 2
                WHEN 'cool' THEN 3 WHEN 'cold' THEN 4 ELSE 5
            END,
            relationship_score DESC NULLS LAST
        LIMIT ?
    """
    params.append(limit)
    return conn.execute(query, params).fetchall()


# ── Apollo API ───────────────────────────────────────────────────────────────

def build_match_detail(contact, reveal_phones=False):
    """Build an Apollo match detail dict for one contact. Returns None if unmatchable."""
    detail = {}

    # Parse emails
    try:
        emails = json.loads(contact["emails"] or "[]")
    except (json.JSONDecodeError, TypeError):
        emails = []

    # Prefer work email (non-generic domain)
    work_emails = [e for e in emails if e.split("@")[-1].lower() not in GENERIC_DOMAINS]
    if work_emails:
        detail["email"] = work_emails[0]
    elif emails:
        detail["email"] = emails[0]

    # Name split
    name = (contact["name"] or "").strip()
    if name:
        parts = name.split(" ", 1)
        detail["first_name"] = parts[0]
        detail["last_name"] = parts[1] if len(parts) > 1 else ""

    # Company for better matching
    company = (contact["company"] or "").strip()
    if company:
        detail["organization_name"] = company

    # Must have at least email or (name + company)
    has_email = "email" in detail
    has_name_company = "first_name" in detail and "organization_name" in detail
    if not has_email and not has_name_company:
        return None

    detail["reveal_personal_emails"] = False
    detail["reveal_phone_number"] = reveal_phones
    return detail


def bulk_match(api_key, details):
    """Call Apollo bulk_match. Returns list of person dicts (None for unmatched)."""
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
    }
    body = {"details": details}

    for attempt in range(3):
        try:
            resp = requests.post(
                f"{APOLLO_BASE}/people/bulk_match",
                headers=headers,
                json=body,
                timeout=60,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("matches", data.get("people", []))
            elif resp.status_code == 429:
                wait = (2 ** attempt) * 2
                log.warning("Rate limited (429). Backing off %ds...", wait)
                time.sleep(wait)
                continue
            elif resp.status_code == 401:
                log.error("Apollo API key invalid (401). Aborting.")
                sys.exit(1)
            else:
                log.error("Apollo API error %d: %s", resp.status_code, resp.text[:200])
                return [None] * len(details)
        except requests.exceptions.Timeout:
            log.warning("Request timeout (attempt %d/3)", attempt + 1)
            time.sleep(2)
            continue
        except requests.exceptions.RequestException as e:
            log.error("Request failed: %s", e)
            return [None] * len(details)

    log.error("Exhausted retries for bulk_match batch.")
    return [None] * len(details)


# ── Field mapping ────────────────────────────────────────────────────────────

def get_provenance(contact):
    """Parse personal_data_source JSON from contact row."""
    try:
        return json.loads(contact["personal_data_source"] or "{}")
    except (json.JSONDecodeError, TypeError):
        return {}


def should_update(field_name, provenance):
    """Check if Apollo should write a field, respecting confidence hierarchy."""
    current_source = provenance.get(field_name, "")
    return current_source.lower() not in PROTECTED_SOURCES


def map_apollo_to_updates(person, contact, reveal_phones=False):
    """
    Map Apollo response to CRM column updates, notes block, and provenance updates.
    Returns (column_updates, provenance_updates, notes_block) or (None, None, None) if no match.
    """
    if not person:
        return None, None, None

    provenance = get_provenance(contact)
    updates = {}
    prov_updates = {}

    # Company
    new_company = (person.get("organization", {}) or {}).get("name") or person.get("organization_name")
    current_company = (contact["company"] or "").strip()
    if new_company and not current_company and should_update("company", provenance):
        updates["company"] = new_company
        prov_updates["company"] = "apollo"

    # Role / Title
    new_role = person.get("title")
    current_role = (contact["role"] or "").strip()
    if new_role and not current_role and should_update("role", provenance):
        updates["role"] = new_role
        prov_updates["role"] = "apollo"

    # LinkedIn URL -> social_profiles
    linkedin = person.get("linkedin_url")
    if linkedin and "linkedin.com" in linkedin:
        try:
            socials = json.loads(contact["social_profiles"] or "{}")
        except (json.JSONDecodeError, TypeError):
            socials = {}
        if not socials.get("linkedin"):
            socials["linkedin"] = linkedin
            updates["social_profiles"] = json.dumps(socials)
            prov_updates["linkedin"] = "apollo"

    # Address (only if empty)
    current_address = (contact["address"] or "").strip()
    if not current_address and should_update("address", provenance):
        city = person.get("city") or ""
        state = person.get("state") or ""
        country = person.get("country") or ""
        parts = [p for p in [city, state, country] if p]
        if parts:
            updates["address"] = ", ".join(parts)
            prov_updates["address"] = "apollo"

    # Merge emails
    new_email = person.get("email")
    if new_email:
        try:
            existing_emails = json.loads(contact["emails"] or "[]")
        except (json.JSONDecodeError, TypeError):
            existing_emails = []
        lower_existing = {e.lower() for e in existing_emails}
        if new_email.lower() not in lower_existing:
            existing_emails.append(new_email)
            updates["emails"] = json.dumps(existing_emails)

    # Merge phones (only if reveal_phones)
    if reveal_phones:
        phone_numbers = person.get("phone_numbers") or []
        if phone_numbers:
            try:
                existing_phones = json.loads(contact["phones"] or "[]")
            except (json.JSONDecodeError, TypeError):
                existing_phones = []
            for pn in phone_numbers:
                num = pn.get("sanitized_number") or pn.get("number", "")
                if num and num not in existing_phones:
                    existing_phones.append(num)
            if len(existing_phones) > len(json.loads(contact["phones"] or "[]")):
                updates["phones"] = json.dumps(existing_phones)

    # Build supplementary notes block
    notes_lines = []
    org = person.get("organization") or {}

    if person.get("headline"):
        notes_lines.append(f"Headline: {person['headline']}")
    if person.get("seniority"):
        notes_lines.append(f"Seniority: {person['seniority']}")
    if person.get("departments"):
        notes_lines.append(f"Department: {', '.join(person['departments'][:3])}")

    # Employment history
    history = person.get("employment_history") or []
    if history:
        emp_lines = []
        for emp in history[:5]:
            title = emp.get("title", "")
            org_name = emp.get("organization_name", "")
            start = emp.get("start_date", "")
            current = emp.get("current", False)
            end = "present" if current else emp.get("end_date", "")
            if title or org_name:
                emp_lines.append(f"  {title} @ {org_name} ({start}–{end})")
        if emp_lines:
            notes_lines.append("Employment:")
            notes_lines.extend(emp_lines)

    if org.get("name"):
        notes_lines.append(f"Organization: {org['name']}")
    if org.get("industry"):
        notes_lines.append(f"Industry: {org['industry']}")
    if org.get("estimated_num_employees"):
        notes_lines.append(f"Employees: {org['estimated_num_employees']}")
    if org.get("annual_revenue_printed"):
        notes_lines.append(f"Revenue: {org['annual_revenue_printed']}")
    if org.get("founded_year"):
        notes_lines.append(f"Founded: {org['founded_year']}")

    techs = org.get("current_technologies") or []
    if techs:
        notes_lines.append(f"Technologies: {', '.join(techs[:10])}")

    notes_block = "\n".join(notes_lines) if notes_lines else ""

    return updates, prov_updates, notes_block


def build_apollo_notes(existing_notes, notes_block):
    """Merge Apollo enrichment block into contact notes. Idempotent."""
    existing = existing_notes or ""
    existing = re.sub(r"\n?## Apollo Enrichment\n.*?(?=\n##|\Z)", "", existing, flags=re.DOTALL).strip()

    if not notes_block:
        return existing

    block = "## Apollo Enrichment\n" + notes_block
    return (existing + "\n\n" + block).strip() if existing else block


# ── Database update ──────────────────────────────────────────────────────────

def apply_updates(conn, contact_id, column_updates, prov_updates, new_notes, dry_run=False):
    """Write enriched data back to the contact row."""
    now = datetime.now(timezone.utc).isoformat()

    if dry_run:
        log.info("  [DRY RUN] Would update: %s", json.dumps(column_updates, indent=2))
        return

    sets = []
    values = []

    for col, val in column_updates.items():
        sets.append(f"{col} = ?")
        values.append(val)

    if prov_updates:
        row = conn.execute("SELECT personal_data_source FROM contacts WHERE id=?", (contact_id,)).fetchone()
        try:
            prov = json.loads(row["personal_data_source"] or "{}")
        except (json.JSONDecodeError, TypeError):
            prov = {}
        prov.update(prov_updates)
        sets.append("personal_data_source = ?")
        values.append(json.dumps(prov))

    if new_notes is not None:
        sets.append("notes = ?")
        values.append(new_notes)

    sets.append("apollo_enriched_at = ?")
    values.append(now)
    sets.append("updated_at = ?")
    values.append(now)
    values.append(contact_id)

    conn.execute(f"UPDATE contacts SET {', '.join(sets)} WHERE id=?", values)
    conn.commit()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enrich CRM contacts via Apollo.io bulk_match API")
    parser.add_argument("--limit", type=int, default=500, help="Max contacts to enrich (default: 500)")
    parser.add_argument("--heat", default=None, help="Comma-separated heat tiers (default: all)")
    parser.add_argument("--force", action="store_true", help="Re-enrich already-enriched contacts")
    parser.add_argument("--reveal-phones", action="store_true", help="Request phone numbers (costs 8x more)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without writing")
    args = parser.parse_args()

    heat_tiers = [h.strip() for h in args.heat.split(",")] if args.heat else None
    heat_label = args.heat or "all"
    mode = "DRY RUN" if args.dry_run else ("FORCE" if args.force else "STANDARD")
    log.info("=== Apollo Enrich Starting (limit=%d, heat=%s, mode=%s, phones=%s) ===",
             args.limit, heat_label, mode, args.reveal_phones)

    api_key = get_api_key()
    conn = get_conn()
    ensure_apollo_column(conn)

    contacts = get_contacts_to_enrich(conn, args.limit, heat_tiers=heat_tiers, force=args.force)
    log.info("Contacts to enrich: %d", len(contacts))

    if not contacts:
        log.info("No contacts need Apollo enrichment.")
        conn.close()
        return

    matchable = []
    unmatchable = 0
    for c in contacts:
        detail = build_match_detail(c, reveal_phones=args.reveal_phones)
        if detail:
            matchable.append((c, detail))
        else:
            unmatchable += 1

    log.info("Matchable: %d, Unmatchable (no email or company): %d", len(matchable), unmatchable)

    enriched = 0
    matched = 0
    no_match = 0
    errors = 0

    for batch_start in range(0, len(matchable), BATCH_SIZE):
        batch = matchable[batch_start:batch_start + BATCH_SIZE]
        details = [d for _, d in batch]

        if args.dry_run:
            log.info("  [DRY RUN] Batch %d-%d: %d contacts",
                     batch_start + 1, batch_start + len(batch), len(batch))
            for contact, detail in batch:
                log.info("    %s (%s)", contact["name"], detail.get("email", "name-only"))
            matched += len(batch)
            continue

        results = bulk_match(api_key, details)

        for i, (contact, detail) in enumerate(batch):
            person = results[i] if i < len(results) else None
            name = contact["name"] or "(unnamed)"

            if not person:
                log.info("  %s: no Apollo match", name)
                no_match += 1
                apply_updates(conn, contact["id"], {}, {}, None, dry_run=False)
                continue

            try:
                col_updates, prov_updates, notes_block = map_apollo_to_updates(
                    person, contact, reveal_phones=args.reveal_phones
                )

                if col_updates is None:
                    log.info("  %s: no usable data from Apollo", name)
                    no_match += 1
                    apply_updates(conn, contact["id"], {}, {}, None, dry_run=False)
                    continue

                existing_notes = contact["notes"] or ""
                new_notes = build_apollo_notes(existing_notes, notes_block)
                apply_updates(conn, contact["id"], col_updates, prov_updates, new_notes, dry_run=False)
                matched += 1

                title = person.get("title", "")
                org = (person.get("organization") or {}).get("name", "")
                fields_changed = list(col_updates.keys())
                log.info("  %s: %s @ %s | updated: %s", name, title, org, ", ".join(fields_changed) or "notes only")
                enriched += 1

            except Exception as e:
                log.error("  %s: ERROR %s", name, e)
                errors += 1

        time.sleep(RATE_LIMIT_SECONDS)

    conn.close()

    log.info("=== Apollo Enrich Done ===")
    log.info("  Processed:   %d", len(matchable))
    log.info("  Matched:     %d", matched)
    log.info("  Enriched:    %d (columns updated)", enriched)
    log.info("  No match:    %d", no_match)
    log.info("  Unmatchable: %d", unmatchable)
    log.info("  Errors:      %d", errors)
    if not args.reveal_phones:
        log.info("  Credits est: ~%d (email-only @ 1 credit each)", matched)
    else:
        log.info("  Credits est: ~%d (email+phone @ 9 credits each)", matched * 9)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
enrich.py — Enrich CRM contacts with professional data via Tavily + LLM.

Extracts:
  - company, role (primary fields)
  - industry / sector
  - LinkedIn URL
  - Twitter/X handle
  - education background (brief)
  - recent news mention (brief)
  - location/city

Stores enriched fields back in contacts.company/role + structured notes block.
Idempotent: skips contacts with enriched_at set (unless --force).

Usage:
  python3 -m enrichment.enrich [--limit 50] [--force] [--min-data]

--min-data: focus run on contacts with no company AND no role (highest value targets)
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    DB_PATH, LOG_DIR, ENRICHMENT_MODEL, RATE_LIMIT_SECONDS,
    TAVILY_SCRIPT, GENERIC_DOMAINS, get_secret,
)

import openai

LOG_PATH = os.path.join(LOG_DIR, "enrichment.log")
DEEPENING_LOG = os.path.join(LOG_DIR, "autonomous-deepening.log")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


def dlog(msg):
    """Also log to deepening log."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(DEEPENING_LOG, "a") as f:
        f.write(f"[{ts}] [enrich] {msg}\n")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_enriched_column(conn):
    """Add enriched_at column to contacts if it doesn't exist."""
    try:
        conn.execute("ALTER TABLE contacts ADD COLUMN enriched_at TEXT")
        conn.commit()
        log.info("Added enriched_at column to contacts table.")
    except Exception:
        pass


def get_contacts_to_enrich(conn, limit, force=False, min_data=False):
    if force:
        query = """
            SELECT id, name, emails, company, role, notes FROM contacts
            ORDER BY last_contact_date DESC NULLS LAST LIMIT ?
        """
        return conn.execute(query, (limit,)).fetchall()

    if min_data:
        # Prioritize contacts with no company AND no role
        query = """
            SELECT id, name, emails, company, role, notes FROM contacts
            WHERE (company IS NULL OR company = '')
              AND (role IS NULL OR role = '')
              AND enriched_at IS NULL
            ORDER BY last_contact_date DESC NULLS LAST LIMIT ?
        """
    else:
        query = """
            SELECT id, name, emails, company, role, notes FROM contacts
            WHERE (company IS NULL OR company = '' OR role IS NULL OR role = '')
              AND enriched_at IS NULL
            ORDER BY last_contact_date DESC NULLS LAST LIMIT ?
        """
    return conn.execute(query, (limit,)).fetchall()


def get_email_domain(emails_json):
    try:
        emails = json.loads(emails_json or "[]")
        if emails:
            return emails[0].split("@")[-1].lower()
    except Exception:
        pass
    return None


def get_all_emails(emails_json):
    try:
        return json.loads(emails_json or "[]")
    except Exception:
        return []


def build_search_queries(name, domain, emails):
    """Build 1-3 search queries with fallback strategies."""
    queries = []
    name_clean = name.strip()

    # Strategy 1: name + domain (if non-generic)
    if domain and domain not in GENERIC_DOMAINS:
        queries.append(f"{name_clean} {domain}")

    # Strategy 2: name + "LinkedIn" for professional context
    queries.append(f'"{name_clean}" LinkedIn professional')

    # Strategy 3: for ambiguous names, add email address itself
    non_generic_emails = [e for e in emails if e.split("@")[-1] not in GENERIC_DOMAINS]
    if non_generic_emails and len(queries) < 3:
        queries.append(f"{name_clean} {non_generic_emails[0]}")

    # Return up to 2 unique queries
    seen = set()
    unique = []
    for q in queries:
        if q not in seen:
            seen.add(q)
            unique.append(q)
    return unique[:2]


def tavily_search(query):
    """Run Tavily search and return raw output."""
    if not TAVILY_SCRIPT:
        log.warning("TAVILY_SCRIPT not configured — skipping search")
        return ""
    try:
        result = subprocess.run(
            ["bash", TAVILY_SCRIPT, query],
            capture_output=True, text=True, timeout=30,
        )
        return result.stdout.strip()
    except Exception as e:
        log.error("Tavily search failed: %s", e)
        return ""


def extract_with_gpt(client, name, search_results_combined):
    """
    Use LLM to extract enrichment data from combined search results.
    Returns dict with keys: company, role, industry, linkedin, twitter, education, news, location.
    """
    if not search_results_combined or len(search_results_combined.strip()) < 20:
        return {}

    prompt = (
        f"Extract professional information about '{name}' from these search results.\n\n"
        f"Search results:\n{search_results_combined[:3000]}\n\n"
        "Return a JSON object with ONLY these exact fields (null if not found or uncertain):\n"
        "{\n"
        '  "company": "current employer or null",\n'
        '  "role": "current job title or null",\n'
        '  "industry": "industry/sector (e.g. Finance, Healthcare, Tech, Legal) or null",\n'
        '  "linkedin": "full LinkedIn profile URL or null",\n'
        '  "twitter": "Twitter/X handle starting with @ or null",\n'
        '  "education": "highest degree + school, very brief (e.g. MBA Harvard) or null",\n'
        '  "news": "one recent notable mention or achievement, <100 chars, or null",\n'
        '  "location": "city/state or null"\n'
        "}\n\n"
        "Rules: Only include data you are CONFIDENT about from the search results. "
        "Do not guess. Return null for anything uncertain. "
        "company and role must match this specific person, not a namesake."
    )

    try:
        resp = client.chat.completions.create(
            model=ENRICHMENT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=250,
            temperature=0,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content)

        # Sanity checks
        for field in ("company", "role", "industry", "education", "news", "location"):
            val = data.get(field)
            if val and len(str(val)) > 150:
                data[field] = None

        # Validate LinkedIn URL format
        linkedin = data.get("linkedin")
        if linkedin and "linkedin.com" not in str(linkedin).lower():
            data["linkedin"] = None

        # Validate Twitter handle
        twitter = data.get("twitter")
        if twitter and not str(twitter).startswith("@"):
            if re.match(r"^[A-Za-z0-9_]+$", str(twitter)):
                data["twitter"] = "@" + twitter
            else:
                data["twitter"] = None

        return data
    except Exception as e:
        log.error("LLM extraction failed for %s: %s", name, e)
        return {}


def build_enrichment_notes(existing_notes, data):
    """
    Merge new enrichment data into existing notes.
    Stores a structured block: ## Enrichment\nKey: Value
    Idempotent: replaces the enrichment block if it already exists.
    """
    existing = existing_notes or ""

    # Strip any prior enrichment block
    existing = re.sub(r"\n?## Enrichment\n.*?(?=\n##|\Z)", "", existing, flags=re.DOTALL).strip()

    fields = [
        ("Industry", data.get("industry")),
        ("Education", data.get("education")),
        ("Location", data.get("location")),
        ("Twitter", data.get("twitter")),
        ("News", data.get("news")),
    ]

    # Add LinkedIn if not already in notes and not already in a LinkedIn line
    linkedin = data.get("linkedin")
    if linkedin and "linkedin.com" not in existing.lower():
        fields.insert(0, ("LinkedIn", linkedin))

    enrichment_lines = [f"{k}: {v}" for k, v in fields if v]

    if not enrichment_lines:
        return existing

    block = "## Enrichment\n" + "\n".join(enrichment_lines)
    return (existing + "\n\n" + block).strip() if existing else block


def enrich_contact(conn, client, contact):
    """
    Search and apply enrichment for one contact.
    Returns dict of what was found (empty dict if nothing).
    """
    name = contact["name"]
    emails = get_all_emails(contact["emails"])
    domain = get_email_domain(contact["emails"])

    queries = build_search_queries(name, domain, emails)
    log.info("Enriching: %s (queries: %d)", name, len(queries))

    # Run searches and combine results
    combined_results = ""
    for q in queries:
        result = tavily_search(q)
        if result:
            combined_results += f"\n--- Query: {q} ---\n{result}\n"
        time.sleep(0.5)

    data = extract_with_gpt(client, name, combined_results)

    now = datetime.utcnow().isoformat()
    updates = ["enriched_at = ?"]
    values = [now]

    # Apply company/role only if contact is missing them
    if data.get("company") and not (contact["company"] and contact["company"].strip()):
        updates.append("company = ?")
        values.append(data["company"])

    if data.get("role") and not (contact["role"] and contact["role"].strip()):
        updates.append("role = ?")
        values.append(data["role"])

    # Build enriched notes
    existing_notes = conn.execute(
        "SELECT notes FROM contacts WHERE id=?", (contact["id"],)
    ).fetchone()["notes"]

    new_notes = build_enrichment_notes(existing_notes, data)
    if new_notes != (existing_notes or ""):
        updates.append("notes = ?")
        values.append(new_notes)

    updates.append("updated_at = ?")
    values.append(now)
    values.append(contact["id"])

    conn.execute(f"UPDATE contacts SET {', '.join(updates)} WHERE id=?", values)
    conn.commit()

    return data


def main():
    parser = argparse.ArgumentParser(description="Enrich CRM contacts via Tavily + LLM")
    parser.add_argument("--limit", type=int, default=50, help="Max contacts to enrich per run")
    parser.add_argument("--force", action="store_true", help="Re-enrich already-enriched contacts")
    parser.add_argument("--min-data", action="store_true", help="Focus on contacts with no company AND no role")
    args = parser.parse_args()

    mode = "FORCE" if args.force else ("MIN-DATA" if args.min_data else "STANDARD")
    log.info("=== CRM Enrich Starting (limit=%d, mode=%s) ===", args.limit, mode)
    dlog(f"=== Enrich run: limit={args.limit} mode={mode} ===")

    api_key = get_secret("OPENAI_API_KEY")
    if not api_key:
        log.error("OPENAI_API_KEY not set.")
        sys.exit(1)

    client = openai.OpenAI(api_key=api_key)
    conn = get_conn()
    ensure_enriched_column(conn)

    contacts = get_contacts_to_enrich(conn, args.limit, force=args.force, min_data=args.min_data)
    log.info("Contacts to enrich: %d", len(contacts))

    if not contacts:
        log.info("No contacts need enrichment.")
        conn.close()
        return

    enriched = 0
    no_data = 0
    partial = 0

    for contact in contacts:
        try:
            data = enrich_contact(conn, client, contact)
            has_primary = bool(data.get("company") or data.get("role"))
            has_supplemental = bool(data.get("industry") or data.get("linkedin") or
                                    data.get("twitter") or data.get("education"))

            if has_primary:
                log.info("  -> %s: company=%s, role=%s", contact["name"],
                         data.get("company"), data.get("role"))
                enriched += 1
            elif has_supplemental:
                log.info("  -> %s: supplemental only (industry=%s)", contact["name"],
                         data.get("industry"))
                partial += 1
            else:
                log.info("  -> %s: no data found", contact["name"])
                no_data += 1
        except Exception as e:
            log.error("  -> %s: ERROR %s", contact["name"], e)
            no_data += 1

        time.sleep(RATE_LIMIT_SECONDS)

    conn.close()
    summary = (f"Enrich done: {enriched} full, {partial} partial, {no_data} no data "
               f"(of {len(contacts)} total)")
    log.info("=== %s ===", summary)
    dlog(summary)


if __name__ == "__main__":
    main()

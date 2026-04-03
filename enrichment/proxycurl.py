#!/usr/bin/env python3
"""
proxycurl.py — Enrich hot/warm CRM contacts with LinkedIn data via Proxycurl.

For each hot/warm contact:
  1. If LinkedIn URL already in notes -> use it directly
  2. Otherwise -> resolve via name + company (Proxycurl profile search)
  3. Pull full LinkedIn profile -> store structured data back into contact notes

Fields added:
  - linkedin_url
  - linkedin_headline
  - linkedin_summary
  - linkedin_current_company
  - linkedin_current_role
  - linkedin_location
  - linkedin_education (most recent)
  - linkedin_connections (approximate)
  - linkedin_enriched_at

Usage:
  python3 -m enrichment.proxycurl [--limit 50] [--dry-run] [--heat hot,warm]

Cost: ~$0.02/lookup (profile) + ~$0.02/resolve (if no LinkedIn URL)
Idempotent: skips contacts with linkedin_enriched_at set (unless --force)
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, RATE_LIMIT_SECONDS, get_secret

import requests

PROXYCURL_BASE = "https://nubela.co/proxycurl/api"


def get_api_key():
    key = get_secret("PROXYCURL_API_KEY")
    if not key:
        print("ERROR: PROXYCURL_API_KEY not set. Add it to .env or export it.")
        sys.exit(1)
    return key


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_linkedin_columns(conn):
    """Add LinkedIn columns to contacts table if missing."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(contacts)")}
    columns = [
        ("linkedin_url", "TEXT"),
        ("linkedin_headline", "TEXT"),
        ("linkedin_current_company", "TEXT"),
        ("linkedin_current_role", "TEXT"),
        ("linkedin_location", "TEXT"),
        ("linkedin_education", "TEXT"),
        ("linkedin_connections", "INTEGER"),
        ("linkedin_enriched_at", "TEXT"),
    ]
    for col_name, col_type in columns:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE contacts ADD COLUMN {col_name} {col_type}")
    conn.commit()


def extract_linkedin_url_from_notes(notes: str) -> str | None:
    """Pull LinkedIn URL if enrich.py already found one."""
    if not notes:
        return None
    match = re.search(r'https?://(?:www\.)?linkedin\.com/in/[\w\-]+/?', notes)
    return match.group(0) if match else None


def resolve_linkedin_url(api_key: str, name: str, company: str, email: str = None) -> str | None:
    """Use Proxycurl Person Search to find LinkedIn URL from name + company."""
    parts = name.strip().split(" ", 1)
    first = parts[0] if parts else ""
    last = parts[1] if len(parts) > 1 else ""

    params = {
        "first_name": first,
        "last_name": last,
        "similarity_checks": "include",
        "enrich_profile": "skip",
    }
    if company:
        params["company_domain"] = company  # Proxycurl also accepts company name
        params["current_company_name"] = company
    if email:
        params["work_email"] = email

    headers = {"Authorization": f"Bearer {api_key}"}
    resp = requests.get(
        f"{PROXYCURL_BASE}/linkedin/profile/resolve",
        params=params,
        headers=headers,
        timeout=30,
    )
    if resp.status_code == 200:
        data = resp.json()
        return data.get("url")
    elif resp.status_code == 404:
        return None
    else:
        print(f"  Resolve error {resp.status_code}: {resp.text[:200]}")
        return None


def fetch_linkedin_profile(api_key: str, linkedin_url: str) -> dict | None:
    """Fetch full LinkedIn profile from URL."""
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "url": linkedin_url,
        "fallback_to_cache": "on-error",
        "use_cache": "if-present",
        "skills": "exclude",
        "inferred_salary": "exclude",
        "personal_email": "exclude",
        "personal_contact_number": "exclude",
        "twitter_profile_id": "include",
    }
    resp = requests.get(
        f"{PROXYCURL_BASE}/v2/linkedin",
        params=params,
        headers=headers,
        timeout=30,
    )
    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 404:
        return None
    else:
        print(f"  Profile fetch error {resp.status_code}: {resp.text[:200]}")
        return None


def parse_profile(profile: dict) -> dict:
    """Extract the fields we care about from a Proxycurl profile response."""
    result = {}

    result["linkedin_headline"] = profile.get("headline") or ""
    result["linkedin_location"] = profile.get("city") or profile.get("country_full_name") or ""
    result["linkedin_connections"] = profile.get("connections") or None

    # Current experience
    experiences = profile.get("experiences") or []
    if experiences:
        current = experiences[0]
        result["linkedin_current_company"] = current.get("company") or ""
        result["linkedin_current_role"] = current.get("title") or ""

    # Most recent education
    education = profile.get("education") or []
    if education:
        school = education[0]
        school_name = school.get("school") or ""
        degree = school.get("degree_name") or ""
        field = school.get("field_of_study") or ""
        result["linkedin_education"] = ", ".join(filter(None, [degree, field, school_name]))

    return result


def update_contact(conn, contact_id: str, linkedin_url: str, profile_fields: dict, dry_run: bool):
    """Write enriched LinkedIn data back to the contact record."""
    if dry_run:
        print(f"  [DRY RUN] Would update {contact_id} with: {json.dumps(profile_fields, indent=2)}")
        return

    fields = {**profile_fields, "linkedin_url": linkedin_url, "linkedin_enriched_at": datetime.now(timezone.utc).isoformat()}
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [contact_id]
    conn.execute(f"UPDATE contacts SET {set_clause} WHERE id = ?", values)
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Enrich hot/warm contacts with LinkedIn data via Proxycurl")
    parser.add_argument("--limit", type=int, default=50, help="Max contacts to process (default: 50)")
    parser.add_argument("--heat", default="hot,warm", help="Comma-separated heat tiers to target (default: hot,warm)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without writing")
    parser.add_argument("--force", action="store_true", help="Re-enrich contacts already enriched via LinkedIn")
    args = parser.parse_args()

    api_key = get_api_key()
    heat_tiers = [h.strip() for h in args.heat.split(",")]

    conn = get_conn()
    ensure_linkedin_columns(conn)

    # Query contacts to enrich — check if relationship_heat column exists
    cols = {r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()}
    has_heat = "relationship_heat" in cols

    placeholders = ",".join("?" * len(heat_tiers))
    if has_heat and args.force:
        query = f"""
            SELECT id, name, emails, company, notes
            FROM contacts
            WHERE relationship_heat IN ({placeholders})
            ORDER BY COALESCE(relationship_score, 0) DESC
            LIMIT ?
        """
        rows = conn.execute(query, heat_tiers + [args.limit]).fetchall()
    elif has_heat:
        query = f"""
            SELECT id, name, emails, company, notes
            FROM contacts
            WHERE relationship_heat IN ({placeholders})
              AND (linkedin_enriched_at IS NULL OR linkedin_enriched_at = '')
            ORDER BY COALESCE(relationship_score, 0) DESC
            LIMIT ?
        """
        rows = conn.execute(query, heat_tiers + [args.limit]).fetchall()
    else:
        # relationship_heat column doesn't exist yet — process all contacts
        if args.force:
            query = """
                SELECT id, name, emails, company, notes
                FROM contacts
                ORDER BY last_contact_date DESC
                LIMIT ?
            """
            rows = conn.execute(query, [args.limit]).fetchall()
        else:
            query = """
                SELECT id, name, emails, company, notes
                FROM contacts
                WHERE (linkedin_enriched_at IS NULL OR linkedin_enriched_at = '')
                ORDER BY last_contact_date DESC
                LIMIT ?
            """
            rows = conn.execute(query, [args.limit]).fetchall()

    print(f"Proxycurl enrichment -- {len(rows)} contacts | heat: {args.heat} | dry_run: {args.dry_run}")
    print(f"Estimated cost: ${len(rows) * 0.04:.2f} (resolve + profile per contact)\n")

    success = 0
    skipped = 0
    failed = 0

    for i, row in enumerate(rows):
        contact_id = row["id"]
        name = row["name"] or ""
        company = row["company"] or ""
        notes = row["notes"] or ""
        emails_raw = row["emails"] or "[]"

        try:
            emails = json.loads(emails_raw)
        except Exception:
            emails = []

        primary_email = emails[0] if emails else None

        print(f"[{i+1}/{len(rows)}] {name} ({company or 'no company'})")

        # Step 1: Find LinkedIn URL
        linkedin_url = extract_linkedin_url_from_notes(notes)
        if linkedin_url:
            print(f"  Found LinkedIn URL from notes: {linkedin_url}")
        else:
            print(f"  Resolving via name+company...")
            if not args.dry_run:
                linkedin_url = resolve_linkedin_url(api_key, name, company, primary_email)
                time.sleep(RATE_LIMIT_SECONDS)

            if linkedin_url:
                print(f"  Resolved: {linkedin_url}")
            else:
                print(f"  Could not resolve LinkedIn URL -- skipping")
                skipped += 1
                continue

        # Step 2: Fetch full profile
        print(f"  Fetching profile...")
        if not args.dry_run:
            profile = fetch_linkedin_profile(api_key, linkedin_url)
            time.sleep(RATE_LIMIT_SECONDS)
        else:
            profile = {"headline": "DRY RUN", "experiences": [{"company": company, "title": "Unknown"}]}

        if not profile:
            print(f"  Profile not found")
            skipped += 1
            continue

        # Step 3: Parse and store
        profile_fields = parse_profile(profile)
        update_contact(conn, contact_id, linkedin_url, profile_fields, args.dry_run)

        headline = profile_fields.get("linkedin_headline", "")
        role = profile_fields.get("linkedin_current_role", "")
        co = profile_fields.get("linkedin_current_company", "")
        print(f"  Enriched: {role} @ {co} | {headline[:60]}")
        success += 1

    print(f"\n-- Done ------------------------------------")
    print(f"  Enriched: {success}")
    print(f"  Skipped (no LinkedIn found): {skipped}")
    print(f"  Failed: {failed}")
    print(f"  Estimated cost: ${success * 0.04:.2f}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
find_dupes.py — Duplicate contact detection for the CRM.
Usage:
  python3 tools/find_dupes.py
  python3 tools/find_dupes.py --limit 10   # first N results (for query.py)
  python3 tools/find_dupes.py --json        # machine-readable output

Detects duplicates via:
  1. Fuzzy name match (difflib, threshold 85%)
  2. Shared email address
  3. Shared phone number
"""

import argparse
import difflib
import json
import re
import sqlite3
import sys
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH

NAME_THRESHOLD = 0.85


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _normalize_phone(p):
    return re.sub(r'\D', '', p or '')


def _normalize_name(n):
    return re.sub(r'\s+', ' ', (n or '').lower().strip())


def find_all_dupes(conn):
    """
    Returns list of dicts:
      {match_type, id_a, name_a, company_a, last_contact_a,
                   id_b, name_b, company_b, last_contact_b, similarity, shared_value}
    """
    contacts = conn.execute(
        "SELECT id, name, company, emails, phones, last_contact_date FROM contacts ORDER BY name"
    ).fetchall()

    found = {}  # key=(min_id, max_id) -> result dict, dedup

    def add(match_type, a, b, similarity, shared=None):
        key = (min(a["id"], b["id"]), max(a["id"], b["id"]))
        if key not in found:
            found[key] = {
                "match_type": match_type,
                "id_a": a["id"], "name_a": a["name"],
                "company_a": a["company"] or "",
                "last_contact_a": a["last_contact_date"] or "",
                "id_b": b["id"], "name_b": b["name"],
                "company_b": b["company"] or "",
                "last_contact_b": b["last_contact_date"] or "",
                "similarity": similarity,
                "shared_value": shared or "",
            }
        else:
            # Upgrade to name match if we already have one via email/phone
            if match_type == "Name match" and found[key]["match_type"] != "Name match":
                found[key]["match_type"] = "Name match"
                found[key]["similarity"] = similarity

    # Build lookup indexes for email / phone
    email_index = {}   # email -> list of contacts
    phone_index = {}   # normalized phone -> list of contacts

    for c in contacts:
        try:
            emails = json.loads(c["emails"] or "[]")
        except Exception:
            emails = []
        seen_emails = set()
        for e in emails:
            e = e.strip().lower()
            if e and e not in seen_emails:
                seen_emails.add(e)
                email_index.setdefault(e, []).append(c)

        try:
            phones = json.loads(c["phones"] or "[]")
        except Exception:
            phones = []
        seen_phones = set()
        for p in phones:
            norm = _normalize_phone(p)
            if len(norm) >= 7 and norm not in seen_phones:
                seen_phones.add(norm)
                phone_index.setdefault(norm, []).append(c)

    # --- Email matches ---
    for email, group in email_index.items():
        if len(group) > 1:
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    add("Email match", group[i], group[j], 1.0, email)

    # --- Phone matches ---
    for phone, group in phone_index.items():
        if len(group) > 1:
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    add("Phone match", group[i], group[j], 1.0, phone)

    # --- Name fuzzy matches ---
    for i in range(len(contacts)):
        for j in range(i + 1, len(contacts)):
            a, b = contacts[i], contacts[j]
            na = _normalize_name(a["name"])
            nb = _normalize_name(b["name"])
            if not na or not nb:
                continue
            ratio = difflib.SequenceMatcher(None, na, nb).ratio()
            if ratio >= NAME_THRESHOLD:
                add("Name match", a, b, ratio)

    results = sorted(found.values(), key=lambda x: (-x["similarity"], x["name_a"]))
    return results


def format_report(results, limit=None):
    if not results:
        return "No duplicate candidates found."

    display = results[:limit] if limit else results
    total = len(results)

    lines = [f"DUPLICATE CANDIDATES ({total} found)\n"]
    for r in display:
        sim_pct = f"{r['similarity']*100:.0f}%"
        lines.append(
            f"[{r['match_type']}] {r['name_a']} (id:{r['id_a'][:8]}) vs {r['name_b']} (id:{r['id_b'][:8]}) — similarity {sim_pct}"
        )
        if r["shared_value"]:
            lines.append(f"  Shared: {r['shared_value']}")
        ca = r["company_a"] or "—"
        cb = r["company_b"] or "—"
        if ca != cb:
            lines.append(f"  Company: {ca} vs {cb}")
        la = r["last_contact_a"][:10] if r["last_contact_a"] else "never"
        lb = r["last_contact_b"][:10] if r["last_contact_b"] else "never"
        lines.append(f"  Last contact: {la} vs {lb}")
        lines.append("")

    if limit and total > limit:
        lines.append(f"(showing {limit} of {total} — run find_dupes.py without --limit to see all)")

    return "\n".join(lines).strip()


def main():
    parser = argparse.ArgumentParser(description="Find duplicate CRM contacts")
    parser.add_argument("--limit", type=int, default=None, help="Max results to show")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    conn = get_conn()
    results = find_all_dupes(conn)
    conn.close()

    if args.json:
        print(json.dumps(results[:args.limit] if args.limit else results, indent=2))
    else:
        print(format_report(results, limit=args.limit))


if __name__ == "__main__":
    main()

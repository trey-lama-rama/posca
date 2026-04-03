#!/usr/bin/env python3
"""
gmail_personal.py — Mine Gmail bodies for personal contact data.

Extracts: birthday, address, anniversary, website, phone.
Incremental: tracks gmail_mined_at per contact, skips already-mined.
Respects iCloud as higher-confidence source (never overwrites).

Usage:
  python3 seeds/gmail_personal.py [--limit 20] [--force] [--heat hot,warm]
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, ACCOUNT_EMAILS, GOG_BIN, ENRICHMENT_MODEL, RATE_LIMIT_SECONDS

import openai

MODEL = ENRICHMENT_MODEL
GOG_TIMEOUT = 30
MAX_MESSAGES_PER_CONTACT = 10
MAX_BODY_CHARS = 2000
MAX_BODIES_TO_LLM = 3

# Domains to skip — marketing, automated, noreply
NOISE_DOMAINS = {
    "noreply", "no-reply", "notifications", "marketing", "mailer",
    "updates", "donotreply", "info@", "support@", "hello@",
}

stats = {"mined": 0, "found_data": 0, "no_data": 0, "skipped": 0, "errors": 0}


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_contacts_to_mine(conn, limit, force=False, heat="hot,warm"):
    heat_list = [h.strip() for h in heat.split(",")]
    placeholders = ",".join("?" for _ in heat_list)

    if force:
        query = f"""
            SELECT id, name, emails, birthday, address, anniversary, website,
                   phones, personal_data_source
            FROM contacts
            WHERE relationship_heat IN ({placeholders})
              AND emails != '[]'
            ORDER BY COALESCE(relationship_score, 0) DESC
            LIMIT ?
        """
        return conn.execute(query, heat_list + [limit]).fetchall()

    query = f"""
        SELECT id, name, emails, birthday, address, anniversary, website,
               phones, personal_data_source
        FROM contacts
        WHERE gmail_mined_at IS NULL
          AND (birthday IS NULL OR birthday = ''
               OR address IS NULL OR address = '')
          AND relationship_heat IN ({placeholders})
          AND emails != '[]'
        ORDER BY COALESCE(relationship_score, 0) DESC
        LIMIT ?
    """
    return conn.execute(query, heat_list + [limit]).fetchall()


def build_search_queries(name, emails):
    """Build Gmail search queries for personal data extraction."""
    queries = []
    for email in emails[:2]:  # max 2 emails per contact
        # Birthday
        queries.append(f'from:{email} (birthday OR "born on" OR "my birthday" OR "bday")')
        queries.append(f'to:{email} ("happy birthday")')
        # Address
        queries.append(f'from:{email} ("my address" OR "mailing address" OR "send it to" OR "street")')
        # Anniversary
        queries.append(f'from:{email} (anniversary OR "wedding anniversary")')

    # Name-based queries (less precise, use sparingly)
    if name:
        first_name = name.split()[0] if name.split() else name
        queries.append(f'"happy birthday" "{first_name}"')

    return queries


def search_gmail(query, account):
    """Search Gmail via GOG CLI, return list of message dicts."""
    try:
        result = subprocess.run(
            [GOG_BIN, "gmail", "messages", "search", query,
             "-a", account, "--max", "5", "-j", "--results-only"],
            capture_output=True, text=True, timeout=GOG_TIMEOUT,
        )
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
        messages = data if isinstance(data, list) else data.get("messages", [])
        return messages
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception):
        return []


def get_message_body(message_id, account):
    """Fetch full message body via GOG CLI."""
    try:
        result = subprocess.run(
            [GOG_BIN, "gmail", "get", message_id,
             "-a", account, "-j", "--format=full"],
            capture_output=True, text=True, timeout=GOG_TIMEOUT,
        )
        if result.returncode != 0:
            return ""
        data = json.loads(result.stdout)
        body = data.get("body", "")
        return body[:MAX_BODY_CHARS] if body else ""
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception):
        return ""


def is_noise_message(msg):
    """Filter out marketing/automated messages."""
    from_addr = msg.get("from", "").lower()
    for noise in NOISE_DOMAINS:
        if noise in from_addr:
            return True
    labels = msg.get("labels", [])
    if "CATEGORY_PROMOTIONS" in labels:
        return True
    return False


def extract_personal_data(name, email, bodies):
    """Use LLM to extract personal data from email bodies."""
    combined = "\n\n---\n\n".join(bodies[:MAX_BODIES_TO_LLM])

    prompt = f"""Extract personal contact information about {name} ({email}) from these email messages.

{combined}

Return a JSON object with ONLY these fields (null if not found or uncertain):
{{
  "birthday": "YYYY-MM-DD or null (use --MM-DD if year unknown)",
  "address": "full mailing address or null",
  "anniversary": "YYYY-MM-DD or null",
  "website": "personal or professional URL or null",
  "phone": "phone number if found, or null"
}}

Rules:
- Only extract data explicitly stated in the emails, do not infer or guess
- Birthday must be an actual date, not vague references like "around March"
- Address must be a physical/mailing address, not just a city name
- Ignore marketing emails, only extract data about {name} specifically
- Return null for anything uncertain"""

    try:
        client = openai.OpenAI()
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=200,
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"    [LLM ERROR] {e}", flush=True)
        return {}


def update_contact(conn, contact_id, extracted, sources):
    """Update contact with extracted data, respecting source confidence."""
    current_sources = json.loads(sources or "{}")
    updates = {}
    new_sources = dict(current_sources)

    for field in ("birthday", "address", "anniversary", "website"):
        new_val = extracted.get(field)
        if not new_val:
            continue
        # Never overwrite icloud-sourced data
        if current_sources.get(field) == "icloud":
            continue
        updates[field] = new_val
        new_sources[field] = "gmail"

    # Phone — merge into existing JSON array
    new_phone = extracted.get("phone")
    if new_phone:
        row = conn.execute("SELECT phones FROM contacts WHERE id=?", (contact_id,)).fetchone()
        existing_phones = set(json.loads(row["phones"] or "[]"))
        if new_phone not in existing_phones:
            existing_phones.add(new_phone)
            updates["phones"] = json.dumps(list(existing_phones))

    if not updates:
        return False

    set_clauses = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values())

    conn.execute(f"""
        UPDATE contacts SET
            {set_clauses},
            personal_data_source = ?,
            gmail_mined_at = ?,
            updated_at = ?
        WHERE id = ?
    """, values + [json.dumps(new_sources), datetime.utcnow().isoformat(),
                   datetime.utcnow().isoformat(), contact_id])
    return True


def mine_contact(conn, contact):
    """Mine Gmail for personal data about one contact."""
    name = contact["name"]
    emails = json.loads(contact["emails"] or "[]")
    sources = contact["personal_data_source"]

    if not emails:
        return False

    print(f"  Mining: {name} ({emails[0]})", flush=True)

    # Collect unique messages across all queries and accounts
    seen_ids = set()
    messages = []

    queries = build_search_queries(name, emails)

    for account in ACCOUNT_EMAILS:
        for query in queries:
            if len(messages) >= MAX_MESSAGES_PER_CONTACT:
                break
            results = search_gmail(query, account)
            for msg in results:
                msg_id = msg.get("id", "")
                if msg_id and msg_id not in seen_ids and not is_noise_message(msg):
                    seen_ids.add(msg_id)
                    msg["_account"] = account
                    messages.append(msg)
                    if len(messages) >= MAX_MESSAGES_PER_CONTACT:
                        break
            time.sleep(0.3)  # Brief pause between queries

    if not messages:
        print(f"    No relevant messages found", flush=True)
        # Mark as mined even with no results to avoid re-scanning
        conn.execute(
            "UPDATE contacts SET gmail_mined_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), contact["id"]),
        )
        return False

    print(f"    Found {len(messages)} candidate messages, fetching bodies...", flush=True)

    # Fetch bodies for top messages
    bodies = []
    cache_entries = []
    for msg in messages[:MAX_BODIES_TO_LLM + 2]:  # fetch a few extra in case some are empty
        body = get_message_body(msg["id"], msg["_account"])
        if body and len(body.strip()) > 50:
            bodies.append(body)
            cache_entries.append(msg["id"])
        if len(bodies) >= MAX_BODIES_TO_LLM:
            break
        time.sleep(0.3)

    if not bodies:
        print(f"    No usable message bodies", flush=True)
        conn.execute(
            "UPDATE contacts SET gmail_mined_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), contact["id"]),
        )
        return False

    print(f"    Extracting from {len(bodies)} message(s) via {MODEL}...", flush=True)
    extracted = extract_personal_data(name, emails[0], bodies)

    if not extracted or all(v is None for v in extracted.values()):
        print(f"    No personal data extracted", flush=True)
        conn.execute(
            "UPDATE contacts SET gmail_mined_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), contact["id"]),
        )
        return False

    # Log what was found
    found = {k: v for k, v in extracted.items() if v}
    print(f"    Found: {json.dumps(found)}", flush=True)

    # Update contact
    updated = update_contact(conn, contact["id"], extracted, sources)

    # Cache which messages we processed
    for msg_id in cache_entries:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO gmail_mining_cache (message_id, contact_id, extracted_data) VALUES (?, ?, ?)",
                (msg_id, contact["id"], json.dumps(extracted)),
            )
        except Exception:
            pass

    if not updated:
        # Mark as mined even if nothing was written (e.g. icloud data already present)
        conn.execute(
            "UPDATE contacts SET gmail_mined_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), contact["id"]),
        )

    return updated


def main():
    parser = argparse.ArgumentParser(description="Mine Gmail for personal contact data")
    parser.add_argument("--limit", type=int, default=20, help="Max contacts to mine (default: 20)")
    parser.add_argument("--force", action="store_true", help="Re-mine already-mined contacts")
    parser.add_argument("--heat", default="hot,warm", help="Relationship heat filter (default: hot,warm)")
    args = parser.parse_args()

    print("=== Gmail Personal Data Mining ===", flush=True)

    conn = get_conn()
    contacts = get_contacts_to_mine(conn, args.limit, args.force, args.heat)
    print(f"  {len(contacts)} contacts to mine (limit={args.limit}, heat={args.heat})", flush=True)

    for contact in contacts:
        try:
            found = mine_contact(conn, contact)
            stats["mined"] += 1
            if found:
                stats["found_data"] += 1
            else:
                stats["no_data"] += 1
        except Exception as e:
            stats["errors"] += 1
            print(f"  [ERROR] {contact['name']}: {e}", flush=True)

        conn.commit()
        time.sleep(RATE_LIMIT_SECONDS)

    conn.close()

    print(f"\n  Done: {stats['mined']} mined, {stats['found_data']} with data, "
          f"{stats['no_data']} no data, {stats['errors']} errors", flush=True)


if __name__ == "__main__":
    main()

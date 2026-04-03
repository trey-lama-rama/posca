#!/usr/bin/env python3
"""
calendar.py — Pull calendar events from configured accounts (last 2 years).
Extracts attendees, upserts into contacts + interactions tables.
Idempotent — skips interactions already logged by calendar_event_id.
"""

import json
import os
import re
import sqlite3
import subprocess
import sys
import uuid
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, ACCOUNTS, ACCOUNT_EMAILS, GOG_BIN

# Use the first account as the default calendar account
ACCOUNT = ACCOUNTS[0]["address"] if ACCOUNTS else ""
SELF_EMAILS = {e.lower() for e in ACCOUNT_EMAILS}

# Calendar noise — skip events where we're the only attendee or it's a calendar block
SKIP_PATTERNS = re.compile(
    r'\b(focus time|lunch|dinner|block|hold|personal|ooo|out of office|vacation|travel|flight|drive)\b',
    re.IGNORECASE
)

stats = {"new_contacts": 0, "updated_contacts": 0, "new_interactions": 0, "events_processed": 0, "errors": 0}


def run_gog(args):
    cmd = [GOG_BIN] + args
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"GOG calendar failed: {result.stderr[:300]}")
    return json.loads(result.stdout)


def name_similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def find_existing_contact(conn, email, name):
    if email:
        rows = conn.execute("SELECT id FROM contacts WHERE emails LIKE ?", (f'%{email}%',)).fetchall()
        for row in rows:
            full = conn.execute("SELECT emails FROM contacts WHERE id=?", (row[0],)).fetchone()
            try:
                if email.lower() in [e.lower() for e in json.loads(full[0] or "[]")]:
                    return row[0]
            except Exception:
                pass

    if name and len(name) > 2:
        rows = conn.execute("SELECT id, name FROM contacts").fetchall()
        for row in rows:
            if name_similarity(name, row[1]) >= 0.85:
                return row[0]

    return None


def upsert_contact_calendar(conn, name, email, event_date, accepted):
    now = datetime.utcnow().isoformat()
    rel_type = "warm" if accepted else "cold-inbound"
    existing_id = find_existing_contact(conn, email, name)

    if existing_id:
        row = conn.execute("SELECT last_contact_date, emails FROM contacts WHERE id=?", (existing_id,)).fetchone()
        last_date = row[0] or ""
        existing_emails = set(json.loads(row[1] or "[]"))
        if email:
            existing_emails.add(email.lower())

        if event_date and event_date > last_date:
            conn.execute("""
                UPDATE contacts SET emails=?, last_contact_date=?, last_contact_channel='calendar', updated_at=? WHERE id=?
            """, (json.dumps(list(existing_emails)), event_date, now, existing_id))
        else:
            conn.execute("UPDATE contacts SET emails=?, updated_at=? WHERE id=?",
                         (json.dumps(list(existing_emails)), now, existing_id))
        stats["updated_contacts"] += 1
        return existing_id
    else:
        new_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO contacts (id, name, emails, phones, company, role,
                relationship_type, source_account, first_seen_date,
                last_contact_date, last_contact_channel, created_at, updated_at)
            VALUES (?, ?, ?, '[]', NULL, NULL, ?, 'calendar', ?, ?, 'calendar', ?, ?)
        """, (
            new_id, name, json.dumps([email.lower()] if email else []),
            rel_type,
            event_date[:10] if event_date else now[:10],
            event_date[:10] if event_date else now[:10],
            now, now,
        ))
        stats["new_contacts"] += 1
        return new_id


def upsert_interaction_calendar(conn, contact_id, event_date, subject, event_id, description=None, location=None):
    """Insert interaction if not already logged for this calendar_event_id + contact."""
    if event_id:
        existing = conn.execute(
            "SELECT id FROM interactions WHERE calendar_event_id=? AND contact_id=?",
            (event_id, contact_id)
        ).fetchone()
        if existing:
            return False

    # Build a brief summary from description and location
    summary_parts = []
    if description:
        desc_clean = re.sub(r"<[^>]+>", "", description).strip()[:200]
        if desc_clean:
            summary_parts.append(desc_clean)
    if location:
        summary_parts.append(f"@ {location.strip()[:80]}")
    summary = " | ".join(summary_parts) if summary_parts else None

    now = datetime.utcnow().isoformat()
    new_id = str(uuid.uuid4())

    # Tag interaction type from event title (fast keyword match, no API call)
    itype = None
    cols = [r[1] for r in conn.execute("PRAGMA table_info(interactions)").fetchall()]
    if "interaction_type" in cols:
        title_lower = (subject or "").lower()
        if any(w in title_lower for w in ("intro", "introduction", "meet", "first call")):
            itype = "introduction"
        elif any(w in title_lower for w in ("follow", "check-in", "sync", "catch up", "catch-up")):
            itype = "follow_up"
        elif any(w in title_lower for w in ("deal", "term", "due diligence", "valuation", "closing")):
            itype = "deal_discussion"
        else:
            itype = "meeting"

    if itype:
        conn.execute("""
            INSERT INTO interactions (id, contact_id, date, channel, direction, subject, summary, calendar_event_id, interaction_type, created_at)
            VALUES (?, ?, ?, 'calendar', 'attended', ?, ?, ?, ?, ?)
        """, (new_id, contact_id, event_date or now[:10], subject, summary, event_id, itype, now))
    else:
        conn.execute("""
            INSERT INTO interactions (id, contact_id, date, channel, direction, subject, summary, calendar_event_id, created_at)
            VALUES (?, ?, ?, 'calendar', 'attended', ?, ?, ?, ?)
        """, (new_id, contact_id, event_date or now[:10], subject, summary, event_id, now))
    stats["new_interactions"] += 1
    return True


def process_event(conn, event):
    title = event.get("title") or event.get("summary", "")
    event_id = event.get("id") or event.get("eventId", "")
    start = event.get("start") or event.get("startTime", "")
    if isinstance(start, dict):
        start = start.get("dateTime") or start.get("date", "")
    event_date = start[:10] if start else ""
    description = event.get("description") or event.get("notes") or ""
    location = event.get("location") or ""

    attendees = event.get("attendees", [])
    if not attendees:
        return

    # Skip solo or noise events — filter out all self emails
    real_attendees = [
        a for a in attendees
        if a.get("email", "").lower() not in SELF_EMAILS
    ]
    if not real_attendees:
        return

    stats["events_processed"] += 1

    for attendee in real_attendees:
        email = attendee.get("email", "").strip().lower()
        name = attendee.get("displayName") or attendee.get("name") or ""
        if not name and email:
            name = email.split("@")[0].replace(".", " ").replace("_", " ").title()
        status = attendee.get("responseStatus", "")
        accepted = status in ("accepted", "")

        # Skip resource calendars / rooms
        if "resource.calendar.google.com" in email:
            continue
        if not email or "@" not in email:
            continue

        try:
            contact_id = upsert_contact_calendar(conn, name, email, event_date, accepted)
            upsert_interaction_calendar(conn, contact_id, event_date, title, event_id, description, location)
        except Exception as e:
            stats["errors"] += 1
            print(f"    [ERROR] event attendee: {e}", flush=True)


def main():
    print("=== Calendar Seed ===", flush=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    print(f"  Fetching calendar events from {ACCOUNT}...", flush=True)
    try:
        result = run_gog(["calendar", "list", "-a", ACCOUNT, "--limit=500", "-j"])
    except Exception as e:
        print(f"  [ERROR] Calendar list failed: {e}", flush=True)
        conn.close()
        return stats

    events = result.get("events", [])
    print(f"  Found {len(events)} events", flush=True)

    for event in events:
        try:
            process_event(conn, event)
        except Exception as e:
            stats["errors"] += 1
            print(f"  [ERROR] processing event: {e}", flush=True)

    conn.commit()
    conn.close()

    print(f"  Events processed: {stats['events_processed']}", flush=True)
    print(f"  New contacts: {stats['new_contacts']} | Updated: {stats['updated_contacts']}", flush=True)
    print(f"  Interactions logged: {stats['new_interactions']} | Errors: {stats['errors']}", flush=True)
    return stats


if __name__ == "__main__":
    main()

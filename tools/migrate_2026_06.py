#!/usr/bin/env python3
"""
migrate_2026_06.py — June 2026 performance migration.
Idempotent: safe to run repeatedly on new or existing databases.

Changes:
  1. contacts.primary_email TEXT — denormalized first email (lowercased),
     backfilled from the emails JSON array.
  2. Index on contacts(primary_email).
  3. Compound index on interactions(contact_id, date DESC) for the
     "recent interactions per contact" query pattern.
"""

import json
import sqlite3
import sys
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH


def column_exists(conn, table, column):
    return any(r[1] == column for r in conn.execute(f"PRAGMA table_info({table})"))


def main(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # 1. primary_email column
    if not column_exists(conn, "contacts", "primary_email"):
        conn.execute("ALTER TABLE contacts ADD COLUMN primary_email TEXT")
        print("  Added contacts.primary_email")
    else:
        print("  contacts.primary_email already exists")

    # Backfill from emails JSON (first element, lowercased)
    backfilled = 0
    rows = conn.execute(
        "SELECT id, emails FROM contacts WHERE primary_email IS NULL OR primary_email = ''"
    ).fetchall()
    for contact_id, emails_json in rows:
        try:
            emails = json.loads(emails_json or "[]")
        except Exception:
            emails = []
        first = next((str(e).strip().lower() for e in emails if e), None)
        if first:
            conn.execute(
                "UPDATE contacts SET primary_email = ? WHERE id = ?",
                (first, contact_id),
            )
            backfilled += 1
    if backfilled:
        print(f"  Backfilled primary_email for {backfilled} contact(s)")
    else:
        print("  primary_email backfill: nothing to do")

    # 2 + 3. Indexes
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_contacts_primary_email ON contacts(primary_email)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_interactions_contact_date "
        "ON interactions(contact_id, date DESC)"
    )
    print("  Indexes OK (idx_contacts_primary_email, idx_interactions_contact_date)")

    conn.commit()
    conn.close()
    print("  Migration 2026-06 complete.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
migrate_personal_fields.py — Add personal data columns to CRM contacts table.
Idempotent: uses ALTER TABLE ADD COLUMN with try/except (SQLite raises on dupes).

New columns:
  - address, birthday, anniversary, website (personal data)
  - social_profiles (JSON: linkedin, twitter, etc.)
  - personal_data_source (JSON provenance: which source provided each field)
  - gmail_mined_at (timestamp of last Gmail body mining pass)

Also creates gmail_mining_cache table for dedup/audit.
"""

import sqlite3
import sys
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH

COLUMNS = [
    ("contacts", "address", "TEXT"),
    ("contacts", "birthday", "TEXT"),
    ("contacts", "anniversary", "TEXT"),
    ("contacts", "website", "TEXT"),
    ("contacts", "social_profiles", "TEXT DEFAULT '{}'"),
    ("contacts", "personal_data_source", "TEXT DEFAULT '{}'"),
    ("contacts", "gmail_mined_at", "TEXT"),
]

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    added = 0
    for table, col, coltype in COLUMNS:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
            added += 1
            print(f"  Added {table}.{col} ({coltype})")
        except Exception:
            pass  # column already exists

    # Gmail mining cache for dedup/audit
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gmail_mining_cache (
            message_id TEXT PRIMARY KEY,
            contact_id TEXT,
            query_used TEXT,
            extracted_data TEXT,
            mined_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mining_cache_contact ON gmail_mining_cache(contact_id)")

    conn.commit()
    conn.close()

    if added:
        print(f"  Migration complete: {added} new column(s) added.")
    else:
        print("  Migration: all columns already exist.")

if __name__ == "__main__":
    main()

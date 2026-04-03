#!/usr/bin/env python3
"""
migrate_all.py — Run ALL migrations in order, idempotently.

Consolidates scattered ensure_column() / ALTER TABLE calls from:
  - migrations/migrate_personal_fields.py (personal data columns)
  - migrations/migrate_zoom_schema.py (zoom channel, pending_approval, owner)
  - enrichment/enrich.py (enriched_at)
  - enrichment/relationship_score.py (relationship_score, relationship_heat)
  - enrichment/proxycurl.py (linkedin_* columns)
  - enrichment/contact_summary.py (ai_summary_at)
  - enrichment/categorize.py (interaction_type on interactions)
  - tools/query.py (snoozed_until on action_items)

Safe to run repeatedly — all operations are idempotent.
"""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH


def _add_column(conn, table, column, coltype):
    """Add a column if it doesn't exist. Idempotent."""
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
        print(f"  + {table}.{column} ({coltype})")
        return True
    except Exception:
        return False


def migrate_personal_fields(conn):
    """Personal data columns on contacts + gmail mining cache table."""
    print("\n[1] Personal fields migration", flush=True)
    columns = [
        ("contacts", "address", "TEXT"),
        ("contacts", "birthday", "TEXT"),
        ("contacts", "anniversary", "TEXT"),
        ("contacts", "website", "TEXT"),
        ("contacts", "social_profiles", "TEXT DEFAULT '{}'"),
        ("contacts", "personal_data_source", "TEXT DEFAULT '{}'"),
        ("contacts", "gmail_mined_at", "TEXT"),
    ]
    for table, col, coltype in columns:
        _add_column(conn, table, col, coltype)

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
    print("  Personal fields: done.", flush=True)


def migrate_zoom_schema(conn):
    """Zoom channel support, pending_approval, owner columns."""
    print("\n[2] Zoom schema migration", flush=True)
    from migrations.migrate_zoom_schema import migrate_schema
    migrate_schema(conn)


def migrate_enrichment_columns(conn):
    """Columns added by various enrichment scripts."""
    print("\n[3] Enrichment columns", flush=True)

    # enrichment/enrich.py
    _add_column(conn, "contacts", "enriched_at", "TEXT")

    # enrichment/relationship_score.py
    _add_column(conn, "contacts", "relationship_score", "INTEGER")
    _add_column(conn, "contacts", "relationship_heat", "TEXT")

    # enrichment/proxycurl.py — LinkedIn columns
    linkedin_cols = [
        ("linkedin_url", "TEXT"),
        ("linkedin_headline", "TEXT"),
        ("linkedin_current_company", "TEXT"),
        ("linkedin_current_role", "TEXT"),
        ("linkedin_location", "TEXT"),
        ("linkedin_education", "TEXT"),
        ("linkedin_connections", "INTEGER"),
        ("linkedin_enriched_at", "TEXT"),
    ]
    for col_name, col_type in linkedin_cols:
        _add_column(conn, "contacts", col_name, col_type)

    # enrichment/contact_summary.py
    _add_column(conn, "contacts", "ai_summary_at", "TEXT")

    # enrichment/categorize.py
    _add_column(conn, "interactions", "interaction_type", "TEXT")

    # tools/query.py — action items snooze
    _add_column(conn, "action_items", "snoozed_until", "TEXT")

    conn.commit()
    print("  Enrichment columns: done.", flush=True)


def main():
    print("=== Running All Migrations ===", flush=True)
    print(f"  Database: {DB_PATH}", flush=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    migrate_personal_fields(conn)
    migrate_zoom_schema(conn)
    migrate_enrichment_columns(conn)

    conn.close()
    print("\n=== All migrations complete ===", flush=True)


if __name__ == "__main__":
    main()

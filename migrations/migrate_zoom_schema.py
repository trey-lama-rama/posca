#!/usr/bin/env python3
"""
migrate_zoom_schema.py — Expand CHECK constraints to support zoom channel,
pending_approval status, owner column.

Extracted from seeds/zoom.py for reuse via migrate_all.py.
Idempotent.
"""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH


def migrate_schema(conn):
    """Expand CHECK constraints to support zoom channel, pending_approval status, owner column."""
    conn.execute("PRAGMA foreign_keys=OFF")

    # Clean up any leftover backup tables from a previously-failed migration run
    for bak in ("_interactions_bak", "_contacts_bak", "_action_items_bak"):
        conn.execute(f"DROP TABLE IF EXISTS {bak}")

    # If interactions has a stale FK reference (REFERENCES "_contacts_bak") from a
    # partial migration, recreate it with the correct FK pointing to contacts.
    ix_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='interactions'"
    ).fetchone()
    if ix_row and '"_contacts_bak"' in (ix_row[0] or ""):
        print("  [migration] Fixing interactions FK (stale REFERENCES _contacts_bak -> contacts)", flush=True)
        conn.execute("ALTER TABLE interactions RENAME TO _interactions_bak")
        conn.execute("""
            CREATE TABLE interactions (
                id TEXT PRIMARY KEY,
                contact_id TEXT NOT NULL REFERENCES contacts(id),
                date TEXT NOT NULL,
                channel TEXT NOT NULL
                    CHECK (channel IN ('email','calendar','telegram','zoom')),
                direction TEXT NOT NULL
                    CHECK (direction IN ('inbound','outbound','attended')),
                subject TEXT,
                summary TEXT,
                gmail_message_id TEXT,
                calendar_event_id TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("INSERT INTO interactions SELECT * FROM _interactions_bak")
        conn.execute("DROP TABLE _interactions_bak")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_interactions_contact ON interactions(contact_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_interactions_date ON interactions(date)")
        conn.execute("DROP TRIGGER IF EXISTS interactions_ai")
        conn.execute("DROP TRIGGER IF EXISTS interactions_au")
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS interactions_ai AFTER INSERT ON interactions BEGIN
                INSERT INTO interactions_fts(rowid, subject, summary)
                VALUES (new.rowid, new.subject, new.summary);
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS interactions_au AFTER UPDATE ON interactions BEGIN
                INSERT INTO interactions_fts(interactions_fts, rowid, subject, summary)
                VALUES ('delete', old.rowid, old.subject, old.summary);
                INSERT INTO interactions_fts(rowid, subject, summary)
                VALUES (new.rowid, new.subject, new.summary);
            END
        """)
        conn.execute("INSERT INTO interactions_fts(interactions_fts) VALUES('rebuild')")

    # If contacts_fts points to a backup table (SQLite auto-renamed it), recreate it
    fts_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='contacts_fts'"
    ).fetchone()
    if fts_row and "_bak" in (fts_row[0] or ""):
        print("  [migration] Rebuilding contacts_fts (stale content reference)", flush=True)
        conn.execute("DROP TABLE IF EXISTS contacts_fts")
        conn.execute("""
            CREATE VIRTUAL TABLE contacts_fts USING fts5(
                name, company, role, notes,
                content='contacts', content_rowid='rowid'
            )
        """)
        conn.execute("INSERT INTO contacts_fts(contacts_fts) VALUES('rebuild')")
        conn.execute("DROP TRIGGER IF EXISTS contacts_ai")
        conn.execute("DROP TRIGGER IF EXISTS contacts_au")
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS contacts_ai AFTER INSERT ON contacts BEGIN
                INSERT INTO contacts_fts(rowid, name, company, role, notes)
                VALUES (new.rowid, new.name, new.company, new.role, new.notes);
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS contacts_au AFTER UPDATE ON contacts BEGIN
                INSERT INTO contacts_fts(contacts_fts, rowid, name, company, role, notes)
                VALUES ('delete', old.rowid, old.name, old.company, old.role, old.notes);
                INSERT INTO contacts_fts(rowid, name, company, role, notes)
                VALUES (new.rowid, new.name, new.company, new.role, new.notes);
            END
        """)
        conn.execute("INSERT INTO contacts_fts(contacts_fts) VALUES('rebuild')")

    # --- interactions: add 'zoom' to channel CHECK ---
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='interactions'"
    ).fetchone()
    if row and "'zoom'" not in row[0]:
        print("  [migration] Upgrading interactions.channel -> adding 'zoom'", flush=True)
        conn.execute("ALTER TABLE interactions RENAME TO _interactions_bak")
        conn.execute("""
            CREATE TABLE interactions (
                id TEXT PRIMARY KEY,
                contact_id TEXT NOT NULL REFERENCES contacts(id),
                date TEXT NOT NULL,
                channel TEXT NOT NULL
                    CHECK (channel IN ('email','calendar','telegram','zoom')),
                direction TEXT NOT NULL
                    CHECK (direction IN ('inbound','outbound','attended')),
                subject TEXT,
                summary TEXT,
                gmail_message_id TEXT,
                calendar_event_id TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("INSERT INTO interactions SELECT * FROM _interactions_bak")
        conn.execute("DROP TABLE _interactions_bak")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_interactions_contact ON interactions(contact_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_interactions_date ON interactions(date)")
        conn.execute("DROP TRIGGER IF EXISTS interactions_ai")
        conn.execute("DROP TRIGGER IF EXISTS interactions_au")
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS interactions_ai AFTER INSERT ON interactions BEGIN
                INSERT INTO interactions_fts(rowid, subject, summary)
                VALUES (new.rowid, new.subject, new.summary);
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS interactions_au AFTER UPDATE ON interactions BEGIN
                INSERT INTO interactions_fts(interactions_fts, rowid, subject, summary)
                VALUES ('delete', old.rowid, old.subject, old.summary);
                INSERT INTO interactions_fts(rowid, subject, summary)
                VALUES (new.rowid, new.subject, new.summary);
            END
        """)
        conn.execute("INSERT INTO interactions_fts(interactions_fts) VALUES('rebuild')")

    # --- contacts: add 'zoom' to last_contact_channel CHECK ---
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='contacts'"
    ).fetchone()
    if row and "'zoom'" not in row[0]:
        print("  [migration] Upgrading contacts.last_contact_channel -> adding 'zoom'", flush=True)
        conn.execute("ALTER TABLE contacts RENAME TO _contacts_bak")
        conn.execute("""
            CREATE TABLE contacts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                emails TEXT DEFAULT '[]',
                phones TEXT DEFAULT '[]',
                company TEXT,
                role TEXT,
                relationship_type TEXT DEFAULT 'warm'
                    CHECK (relationship_type IN ('warm','cold-inbound','vendor','investor','political','personal')),
                source_account TEXT,
                first_seen_date TEXT,
                last_contact_date TEXT,
                last_contact_channel TEXT
                    CHECK (last_contact_channel IN ('email','calendar','telegram','zoom') OR last_contact_channel IS NULL),
                stale_flag INTEGER DEFAULT 0 CHECK (stale_flag IN (0,1)),
                notes TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("INSERT INTO contacts SELECT * FROM _contacts_bak")
        conn.execute("DROP TABLE _contacts_bak")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_contacts_last_contact ON contacts(last_contact_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_contacts_relationship ON contacts(relationship_type)")
        conn.execute("DROP TRIGGER IF EXISTS contacts_ai")
        conn.execute("DROP TRIGGER IF EXISTS contacts_au")
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS contacts_ai AFTER INSERT ON contacts BEGIN
                INSERT INTO contacts_fts(rowid, name, company, role, notes)
                VALUES (new.rowid, new.name, new.company, new.role, new.notes);
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS contacts_au AFTER UPDATE ON contacts BEGIN
                INSERT INTO contacts_fts(contacts_fts, rowid, name, company, role, notes)
                VALUES ('delete', old.rowid, old.name, old.company, old.role, old.notes);
                INSERT INTO contacts_fts(rowid, name, company, role, notes)
                VALUES (new.rowid, new.name, new.company, new.role, new.notes);
            END
        """)
        conn.execute("INSERT INTO contacts_fts(contacts_fts) VALUES('rebuild')")

    # --- action_items: add 'pending_approval' status + owner + source_meeting_id ---
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='action_items'"
    ).fetchone()
    if row and "'pending_approval'" not in row[0]:
        print("  [migration] Upgrading action_items -> adding pending_approval, owner, source_meeting_id", flush=True)
        conn.execute("ALTER TABLE action_items RENAME TO _action_items_bak")
        conn.execute("""
            CREATE TABLE action_items (
                id TEXT PRIMARY KEY,
                contact_id TEXT NOT NULL REFERENCES contacts(id),
                description TEXT NOT NULL,
                due_date TEXT,
                status TEXT DEFAULT 'open'
                    CHECK (status IN ('open','done','waiting_them','pending_approval')),
                owner TEXT CHECK (owner IN ('mine','theirs') OR owner IS NULL),
                source_meeting_id TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                completed_at TEXT
            )
        """)
        conn.execute("""
            INSERT INTO action_items (id, contact_id, description, due_date, status, created_at, completed_at)
            SELECT id, contact_id, description, due_date, status, created_at, completed_at
            FROM _action_items_bak
        """)
        conn.execute("DROP TABLE _action_items_bak")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_action_items_contact ON action_items(contact_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_action_items_status ON action_items(status)")
    else:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(action_items)").fetchall()]
        if 'owner' not in cols:
            conn.execute("ALTER TABLE action_items ADD COLUMN owner TEXT CHECK (owner IN ('mine','theirs') OR owner IS NULL)")
        if 'source_meeting_id' not in cols:
            conn.execute("ALTER TABLE action_items ADD COLUMN source_meeting_id TEXT")

    conn.commit()
    conn.execute("PRAGMA foreign_keys=ON")
    print("  [migration] Schema ready.", flush=True)


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    migrate_schema(conn)
    conn.close()
    print("Zoom schema migration complete.")


if __name__ == "__main__":
    main()

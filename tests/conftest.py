"""Shared test fixtures. Ensures the repo root is importable."""

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Minimal contacts table covering every column the seed upserts write.
CONTACTS_DDL = """
CREATE TABLE contacts (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    emails TEXT DEFAULT '[]',
    primary_email TEXT,
    phones TEXT DEFAULT '[]',
    company TEXT,
    role TEXT,
    relationship_type TEXT DEFAULT 'warm',
    source_account TEXT,
    first_seen_date TEXT,
    last_contact_date TEXT,
    last_contact_channel TEXT,
    stale_flag INTEGER DEFAULT 0,
    notes TEXT,
    created_at TEXT,
    updated_at TEXT
)
"""


@pytest.fixture
def conn(tmp_path):
    """Temp on-disk SQLite DB with a contacts table."""
    db_path = tmp_path / "test-crm.db"
    connection = sqlite3.connect(str(db_path))
    connection.execute(CONTACTS_DDL)
    connection.commit()
    yield connection
    connection.close()


def insert_contact(conn, contact_id, name, emails_json):
    conn.execute(
        "INSERT INTO contacts (id, name, emails) VALUES (?, ?, ?)",
        (contact_id, name, emails_json),
    )
    conn.commit()

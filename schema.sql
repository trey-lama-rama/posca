-- Headless CRM Schema
-- SQLite with FTS5
-- Idempotent: all CREATE statements use IF NOT EXISTS

CREATE TABLE IF NOT EXISTS contacts (
    id TEXT PRIMARY KEY,                          -- UUID
    name TEXT NOT NULL,
    emails TEXT DEFAULT '[]',                     -- JSON array of strings
    phones TEXT DEFAULT '[]',                     -- JSON array of strings
    company TEXT,
    role TEXT,
    relationship_type TEXT DEFAULT 'warm'
        CHECK (relationship_type IN ('warm','cold-inbound','vendor','investor','political','personal','unknown')),
    source_account TEXT,
    first_seen_date TEXT,
    last_contact_date TEXT,
    last_contact_channel TEXT
        CHECK (last_contact_channel IN ('email','calendar','telegram','zoom') OR last_contact_channel IS NULL),
    stale_flag INTEGER DEFAULT 0 CHECK (stale_flag IN (0,1)),
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),

    -- Enrichment fields
    enriched_at TEXT,
    relationship_score INTEGER,
    relationship_heat TEXT,
    ai_summary_at TEXT,

    -- Personal data
    address TEXT,
    birthday TEXT,
    anniversary TEXT,
    website TEXT,
    social_profiles TEXT DEFAULT '{}',
    personal_data_source TEXT DEFAULT '{}',
    gmail_mined_at TEXT,

    -- LinkedIn enrichment
    linkedin_url TEXT,
    linkedin_headline TEXT,
    linkedin_current_company TEXT,
    linkedin_current_role TEXT,
    linkedin_location TEXT,
    linkedin_education TEXT,
    linkedin_connections INTEGER,
    linkedin_enriched_at TEXT
);

CREATE TABLE IF NOT EXISTS interactions (
    id TEXT PRIMARY KEY,                          -- UUID
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
    interaction_type TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS action_items (
    id TEXT PRIMARY KEY,                          -- UUID
    contact_id TEXT NOT NULL REFERENCES contacts(id),
    description TEXT NOT NULL,
    due_date TEXT,
    status TEXT DEFAULT 'open'
        CHECK (status IN ('open','done','waiting_them','pending_approval')),
    owner TEXT CHECK (owner IN ('mine','theirs') OR owner IS NULL),
    source_meeting_id TEXT,
    snoozed_until TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS gmail_mining_cache (
    message_id TEXT PRIMARY KEY,
    contact_id TEXT,
    query_used TEXT,
    extracted_data TEXT,
    mined_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS embeddings (
    contact_id TEXT PRIMARY KEY,
    embedding BLOB,
    embedded_text TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS filter_cache (
    email TEXT PRIMARY KEY,
    decision TEXT NOT NULL CHECK(decision IN ('KEEP', 'SKIP')),
    reason TEXT,
    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(name);
CREATE INDEX IF NOT EXISTS idx_contacts_last_contact ON contacts(last_contact_date);
CREATE INDEX IF NOT EXISTS idx_contacts_relationship ON contacts(relationship_type);
CREATE INDEX IF NOT EXISTS idx_contacts_heat ON contacts(relationship_heat);
CREATE INDEX IF NOT EXISTS idx_interactions_contact ON interactions(contact_id);
CREATE INDEX IF NOT EXISTS idx_interactions_date ON interactions(date);
CREATE INDEX IF NOT EXISTS idx_action_items_contact ON action_items(contact_id);
CREATE INDEX IF NOT EXISTS idx_action_items_status ON action_items(status);
CREATE INDEX IF NOT EXISTS idx_mining_cache_contact ON gmail_mining_cache(contact_id);

-- FTS5 virtual table over contacts
CREATE VIRTUAL TABLE IF NOT EXISTS contacts_fts USING fts5(
    name,
    company,
    role,
    notes,
    content='contacts',
    content_rowid='rowid'
);

-- FTS5 virtual table over interactions
CREATE VIRTUAL TABLE IF NOT EXISTS interactions_fts USING fts5(
    subject,
    summary,
    content='interactions',
    content_rowid='rowid'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS contacts_ai AFTER INSERT ON contacts BEGIN
    INSERT INTO contacts_fts(rowid, name, company, role, notes)
    VALUES (new.rowid, new.name, new.company, new.role, new.notes);
END;

CREATE TRIGGER IF NOT EXISTS contacts_au AFTER UPDATE ON contacts BEGIN
    INSERT INTO contacts_fts(contacts_fts, rowid, name, company, role, notes)
    VALUES ('delete', old.rowid, old.name, old.company, old.role, old.notes);
    INSERT INTO contacts_fts(rowid, name, company, role, notes)
    VALUES (new.rowid, new.name, new.company, new.role, new.notes);
END;

CREATE TRIGGER IF NOT EXISTS interactions_ai AFTER INSERT ON interactions BEGIN
    INSERT INTO interactions_fts(rowid, subject, summary)
    VALUES (new.rowid, new.subject, new.summary);
END;

CREATE TRIGGER IF NOT EXISTS interactions_au AFTER UPDATE ON interactions BEGIN
    INSERT INTO interactions_fts(interactions_fts, rowid, subject, summary)
    VALUES ('delete', old.rowid, old.subject, old.summary);
    INSERT INTO interactions_fts(rowid, subject, summary)
    VALUES (new.rowid, new.subject, new.summary);
END;

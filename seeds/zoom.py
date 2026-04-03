#!/usr/bin/env python3
"""
zoom.py — Pull Zoom meeting transcripts and seed CRM contacts, interactions, action items.

- Fetches all recordings from last 90 days for the configured Zoom user
- Downloads VTT transcripts, parses to plain text
- Matches attendees to CRM contacts by name/email
- Logs interactions with channel='zoom'
- Uses LLM to extract action items (mine vs theirs)
- Saves action items with status='pending_approval'

Idempotent — skips meetings already logged by calendar_event_id (used as zoom meeting uuid).
"""

import base64
import json
import os
import re
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta, date
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    DB_PATH, ACCOUNT_EMAILS,
    ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET, ZOOM_USER_EMAIL,
    ENRICHMENT_MODEL,
)

import requests

DAYS_BACK = 90

# Build self-name filters from configured account emails
# e.g. "user@domain.com" -> {"user", "domain.com"}
_SELF_EMAIL_PARTS = set()
for _e in ACCOUNT_EMAILS:
    _local, _domain = _e.split("@", 1)
    _SELF_EMAIL_PARTS.add(_local.lower())
    _SELF_EMAIL_PARTS.add(_domain.lower())

stats = {
    "recordings_found": 0,
    "transcripts_found": 0,
    "transcripts_downloaded": 0,
    "contacts_matched": 0,
    "contacts_new": 0,
    "interactions_new": 0,
    "action_items_extracted": 0,
    "meetings_skipped_duplicate": 0,
    "errors": 0,
}


# -- Schema Migration (extracted to migrations/migrate_zoom_schema.py) ---------
from migrations.migrate_zoom_schema import migrate_schema  # noqa: E402

def _migrate_schema_original(conn):
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


# -- Zoom Auth -----------------------------------------------------------------
def get_zoom_token():
    """Obtain a Server-to-Server OAuth bearer token."""
    creds = base64.b64encode(f"{ZOOM_CLIENT_ID}:{ZOOM_CLIENT_SECRET}".encode()).decode()
    resp = requests.post(
        "https://zoom.us/oauth/token",
        params={"grant_type": "account_credentials", "account_id": ZOOM_ACCOUNT_ID},
        headers={"Authorization": f"Basic {creds}", "Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in Zoom response: {data}")
    return token


def zoom_get(token, path, params=None):
    """GET from Zoom API v2."""
    resp = requests.get(
        f"https://api.zoom.us/v2{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params or {},
        timeout=30,
    )
    if resp.status_code == 204:
        return {}
    resp.raise_for_status()
    return resp.json()


# -- Recordings Fetch ----------------------------------------------------------
def fetch_recordings(token):
    """Fetch all recordings for last DAYS_BACK days, across pages."""
    date_to   = date.today()
    date_from = date_to - timedelta(days=DAYS_BACK)
    recordings = []
    page_token = None

    while True:
        params = {
            "from":      date_from.strftime("%Y-%m-%d"),
            "to":        date_to.strftime("%Y-%m-%d"),
            "page_size": 30,
        }
        if page_token:
            params["next_page_token"] = page_token

        data = zoom_get(token, f"/users/{ZOOM_USER_EMAIL}/recordings", params)
        meetings = data.get("meetings", [])
        recordings.extend(meetings)

        page_token = data.get("next_page_token", "")
        if not page_token:
            break

    return recordings


# -- VTT Parsing ---------------------------------------------------------------
_TIMESTAMP_RE = re.compile(r"^\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}")

def parse_vtt(vtt_text):
    """
    Parse WebVTT to plain text, preserving speaker names.
    Returns (plain_text, list_of_speakers).
    """
    lines = []
    speakers = set()
    speaker_tag_re = re.compile(r"^<v\s+([^>]+)>(.*)$")

    for line in vtt_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("WEBVTT") or line.startswith("NOTE") or line.startswith("STYLE"):
            continue
        if _TIMESTAMP_RE.match(line):
            continue
        if re.match(r"^\d+$", line):  # cue number
            continue

        # Speaker tag format: <v Name>text
        m = speaker_tag_re.match(line)
        if m:
            speaker, text = m.group(1).strip(), m.group(2).strip()
            speakers.add(speaker)
            if text:
                lines.append(f"{speaker}: {text}")
            continue

        line = re.sub(r"<[^>]+>", "", line)  # strip any remaining HTML tags
        if ":" in line:
            potential_speaker = line.split(":")[0].strip()
            if len(potential_speaker.split()) <= 4 and not re.search(r"\d", potential_speaker):
                speakers.add(potential_speaker)
        lines.append(line)

    return "\n".join(lines), sorted(speakers)


# -- Participant Fetch ---------------------------------------------------------
def fetch_participants(token, meeting_id):
    """Fetch past meeting participants. Returns list of {name, email} dicts."""
    try:
        data = zoom_get(token, f"/past_meetings/{meeting_id}/participants", {"page_size": 100})
        participants = data.get("participants", [])
        return [{"name": p.get("name", ""), "email": p.get("user_email", "").lower()} for p in participants if p.get("name")]
    except requests.HTTPError as e:
        if e.response.status_code in (400, 403, 404):
            return []
        raise


# -- Contact Matching ----------------------------------------------------------
def name_similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def find_existing_contact(conn, email, name):
    """Find contact by email then name similarity."""
    if email:
        email = email.lower()
        rows = conn.execute("SELECT id FROM contacts WHERE emails LIKE ?", (f"%{email}%",)).fetchall()
        for row in rows:
            full = conn.execute("SELECT emails FROM contacts WHERE id=?", (row[0],)).fetchone()
            try:
                if email in [e.lower() for e in json.loads(full[0] or "[]")]:
                    return row[0]
            except Exception:
                pass

    if name and len(name) > 2:
        rows = conn.execute("SELECT id, name FROM contacts").fetchall()
        for row in rows:
            if name_similarity(name, row[1]) >= 0.82:
                return row[0]

    return None


def upsert_contact(conn, name, email, meeting_date):
    """Upsert contact from Zoom attendee. Returns (contact_id, is_new)."""
    now = datetime.utcnow().isoformat()
    existing_id = find_existing_contact(conn, email, name)

    if existing_id:
        row = conn.execute("SELECT last_contact_date, emails FROM contacts WHERE id=?", (existing_id,)).fetchone()
        last_date = row[0] or ""
        existing_emails = set(json.loads(row[1] or "[]"))
        if email:
            existing_emails.add(email.lower())

        if meeting_date and meeting_date > last_date:
            conn.execute(
                "UPDATE contacts SET emails=?, last_contact_date=?, last_contact_channel='zoom', updated_at=? WHERE id=?",
                (json.dumps(list(existing_emails)), meeting_date, now, existing_id),
            )
        else:
            conn.execute(
                "UPDATE contacts SET emails=?, updated_at=? WHERE id=?",
                (json.dumps(list(existing_emails)), now, existing_id),
            )
        return existing_id, False
    else:
        new_id = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO contacts (id, name, emails, phones, company, role,
                relationship_type, source_account, first_seen_date,
                last_contact_date, last_contact_channel, created_at, updated_at)
               VALUES (?, ?, ?, '[]', NULL, NULL, 'warm', 'zoom', ?, ?, 'zoom', ?, ?)""",
            (
                new_id, name,
                json.dumps([email.lower()] if email else []),
                meeting_date[:10] if meeting_date else now[:10],
                meeting_date[:10] if meeting_date else now[:10],
                now, now,
            ),
        )
        return new_id, True


def upsert_interaction(conn, contact_id, meeting_date, subject, summary, meeting_uuid):
    """Insert interaction if not already logged for this meeting."""
    existing = conn.execute(
        "SELECT id FROM interactions WHERE calendar_event_id=? AND contact_id=?",
        (meeting_uuid, contact_id),
    ).fetchone()
    if existing:
        return False

    now = datetime.utcnow().isoformat()
    conn.execute(
        """INSERT INTO interactions (id, contact_id, date, channel, direction, subject, summary, calendar_event_id, created_at)
           VALUES (?, ?, ?, 'zoom', 'attended', ?, ?, ?, ?)""",
        (str(uuid.uuid4()), contact_id, meeting_date or now[:10], subject, summary, meeting_uuid, now),
    )
    return True


# -- GPT Action Item Extraction ------------------------------------------------
def extract_action_items(transcript_text, meeting_topic, host_name="Host"):
    """
    Use LLM to extract action items from transcript.
    Returns list of {description, owner ('mine'|'theirs'), contact_hint}.

    host_name: the name of the CRM owner / meeting host (used to assign 'mine' vs 'theirs').
               Falls back to "Host" if not provided.
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("  [warn] OPENAI_API_KEY not set -- skipping action item extraction", flush=True)
        return []

    try:
        import openai
        client = openai.OpenAI(api_key=api_key)

        prompt = f"""You are analyzing a Zoom meeting transcript to extract action items.
Meeting: {meeting_topic}
The host is {host_name}.

Transcript (first 6000 chars):
{transcript_text[:6000]}

Extract all action items -- tasks someone committed to doing. For each:
- description: clear, actionable task
- owner: "mine" if {host_name} committed to do it, "theirs" if the other person committed
- contact_hint: name of the person responsible (if "theirs")

Return ONLY valid JSON array, no other text:
[{{"description": "...", "owner": "mine"|"theirs", "contact_hint": "name or empty string"}}]

If no clear action items exist, return [].
"""
        resp = client.chat.completions.create(
            model=ENRICHMENT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=800,
        )
        content = resp.choices[0].message.content.strip()
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)
        items = json.loads(content)
        return items if isinstance(items, list) else []
    except Exception as e:
        print(f"  [warn] GPT action item extraction failed: {e}", flush=True)
        return []


# -- Main ----------------------------------------------------------------------
def main():
    print("=== Zoom Transcript Seed ===", flush=True)
    print(f"  Window: last {DAYS_BACK} days ({(date.today() - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%d')} -> {date.today()})", flush=True)

    # DB
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    # Migrations
    print("\n[1] Schema migrations", flush=True)
    migrate_schema(conn)

    # Zoom token
    print("\n[2] Authenticating with Zoom", flush=True)
    try:
        token = get_zoom_token()
        print("  Token obtained.", flush=True)
    except Exception as e:
        print(f"  [ERROR] Zoom auth failed: {e}", flush=True)
        conn.close()
        return

    # Fetch recordings
    print("\n[3] Fetching recordings", flush=True)
    try:
        recordings = fetch_recordings(token)
    except Exception as e:
        print(f"  [ERROR] Fetch recordings failed: {e}", flush=True)
        conn.close()
        return

    stats["recordings_found"] = len(recordings)
    print(f"  Found {len(recordings)} recording(s)", flush=True)

    if not recordings:
        print("  No recordings in window -- nothing to do.", flush=True)
        conn.close()
        return

    # Process each recording
    print("\n[4] Processing recordings", flush=True)
    for rec in recordings:
        meeting_uuid  = rec.get("uuid", "")
        meeting_id    = rec.get("id", "")
        topic         = rec.get("topic", "Zoom Meeting")
        start_time    = rec.get("start_time", "")
        meeting_date  = start_time[:10] if start_time else date.today().strftime("%Y-%m-%d")
        files         = rec.get("recording_files", [])

        print(f"\n  > [{meeting_date}] {topic} (id={meeting_id})", flush=True)

        # Find transcript file
        transcript_file = next(
            (f for f in files if f.get("file_type") == "TRANSCRIPT" and f.get("status") == "completed"),
            None,
        )
        if not transcript_file:
            transcript_file = next((f for f in files if f.get("file_type") == "TRANSCRIPT"), None)

        if not transcript_file:
            print(f"    No transcript -- skipping", flush=True)
            continue

        stats["transcripts_found"] += 1

        # Download transcript
        download_url = transcript_file.get("download_url", "")
        if not download_url:
            print(f"    No download_url -- skipping", flush=True)
            continue

        try:
            vtt_resp = requests.get(
                download_url,
                params={"access_token": token},
                timeout=30,
            )
            vtt_resp.raise_for_status()
            vtt_text = vtt_resp.text
            stats["transcripts_downloaded"] += 1
            print(f"    Transcript downloaded ({len(vtt_text):,} chars)", flush=True)
        except Exception as e:
            print(f"    [ERROR] Download failed: {e}", flush=True)
            stats["errors"] += 1
            continue

        # Parse VTT
        plain_text, speakers_in_transcript = parse_vtt(vtt_text)
        if not plain_text.strip():
            print(f"    Transcript empty after parsing -- skipping", flush=True)
            continue

        summary = plain_text[:500]
        print(f"    Speakers in transcript: {speakers_in_transcript or '(unknown)'}", flush=True)

        # Get participants
        participants = fetch_participants(token, meeting_id)
        if participants:
            print(f"    Participants API: {[p['name'] for p in participants]}", flush=True)
        else:
            # Fall back to speakers from transcript, filtering out self
            participants = [
                {"name": s, "email": ""}
                for s in speakers_in_transcript
                if not any(part in s.lower() for part in _SELF_EMAIL_PARTS if len(part) > 2)
                and s.lower() not in ("host",)
            ]
            if participants:
                print(f"    Participants (transcript fallback): {[p['name'] for p in participants]}", flush=True)

        # Filter out self by email
        self_emails = {e.lower() for e in ACCOUNT_EMAILS}
        self_domains = {e.split("@")[1] for e in ACCOUNT_EMAILS}
        participants = [
            p for p in participants
            if (p.get("email") or "") not in self_emails
            and not any(d in (p.get("email") or "") for d in self_domains)
        ]

        if not participants:
            print(f"    No external participants to match -- skipping contact/interaction update", flush=True)
            continue

        # Extract action items (once per meeting) -- skip if already seeded
        existing_items = conn.execute(
            "SELECT COUNT(*) FROM action_items WHERE source_meeting_id=?", (meeting_uuid,)
        ).fetchone()[0]
        if existing_items:
            print(f"    Action items already seeded ({existing_items} exist) -- skipping extraction", flush=True)
            action_items = []
        else:
            action_items = extract_action_items(plain_text, topic)
        if action_items:
            print(f"    Action items extracted: {len(action_items)}", flush=True)
        else:
            print(f"    No action items found", flush=True)

        # Match / upsert contacts and interactions
        matched_contacts = []
        for p in participants:
            pname  = p.get("name", "").strip()
            pemail = p.get("email", "").strip().lower()
            if not pname:
                continue

            try:
                contact_id, is_new = upsert_contact(conn, pname, pemail, meeting_date)
                if is_new:
                    stats["contacts_new"] += 1
                    print(f"    + New contact: {pname}", flush=True)
                else:
                    stats["contacts_matched"] += 1

                inserted = upsert_interaction(conn, contact_id, meeting_date, topic, summary, meeting_uuid)
                if inserted:
                    stats["interactions_new"] += 1
                else:
                    stats["meetings_skipped_duplicate"] += 1

                matched_contacts.append((contact_id, pname))
            except Exception as e:
                print(f"    [ERROR] upsert {pname}: {e}", flush=True)
                stats["errors"] += 1

        # Save action items
        if action_items and matched_contacts:
            now = datetime.utcnow().isoformat()
            for item in action_items:
                desc         = item.get("description", "").strip()
                owner        = item.get("owner", "mine")
                contact_hint = item.get("contact_hint", "").strip().lower()
                if not desc:
                    continue

                if owner == "theirs" and contact_hint:
                    target = next(
                        ((cid, cn) for cid, cn in matched_contacts if contact_hint in cn.lower()),
                        matched_contacts[0],
                    )
                else:
                    target = matched_contacts[0]

                contact_id = target[0]

                existing = conn.execute(
                    "SELECT id FROM action_items WHERE source_meeting_id=? AND description=?",
                    (meeting_uuid, desc),
                ).fetchone()
                if existing:
                    continue

                conn.execute(
                    """INSERT INTO action_items (id, contact_id, description, status, owner, source_meeting_id, created_at)
                       VALUES (?, ?, ?, 'pending_approval', ?, ?, ?)""",
                    (str(uuid.uuid4()), contact_id, desc, owner, meeting_uuid, now),
                )
                stats["action_items_extracted"] += 1

        conn.commit()

    # Summary
    conn.close()
    print("\n" + "="*50, flush=True)
    print("=== Zoom Seed Complete ===", flush=True)
    print(f"  Recordings found:        {stats['recordings_found']}", flush=True)
    print(f"  Transcripts found:       {stats['transcripts_found']}", flush=True)
    print(f"  Transcripts downloaded:  {stats['transcripts_downloaded']}", flush=True)
    print(f"  Contacts matched:        {stats['contacts_matched']}", flush=True)
    print(f"  New contacts:            {stats['contacts_new']}", flush=True)
    print(f"  New interactions:        {stats['interactions_new']}", flush=True)
    print(f"  Duplicate meetings skip: {stats['meetings_skipped_duplicate']}", flush=True)
    print(f"  Action items extracted:  {stats['action_items_extracted']}", flush=True)
    print(f"  Errors:                  {stats['errors']}", flush=True)


if __name__ == "__main__":
    main()

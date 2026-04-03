#!/usr/bin/env python3
"""
roam.py — Pull meeting transcripts from ro.am and upsert participants
           into the CRM contacts + interactions tables.

API base: https://api.ro.am/v1/
Endpoints:
  recording.list   -> list all recordings (needs `recordings:read` scope)
  transcript.info  -> get transcript for a recording (needs `transcripts:read` scope)

Idempotent — skips recordings already logged by calendar_event_id or
the ro.am recording ID stored in interactions.calendar_event_id.
"""

import json
import os
import re
import sqlite3
import sys
import uuid
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, ROAM_API_KEY

import requests

API_BASE = "https://api.ro.am/v1"

stats = {
    "recordings_fetched": 0,
    "transcripts_fetched": 0,
    "new_contacts": 0,
    "updated_contacts": 0,
    "new_interactions": 0,
    "scope_errors": 0,
    "errors": 0,
}

_session = None


def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Authorization": f"Bearer {ROAM_API_KEY}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
    return _session


class ScopeMissingError(Exception):
    """Raised when the API returns a scope error."""
    pass


class ApiError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(f"HTTP {status_code}: {message}")


def api_get(path, params=None):
    """GET from ro.am API. Raises ScopeMissingError or ApiError on failure."""
    url = f"{API_BASE}/{path}"
    resp = get_session().get(url, params=params, timeout=30)

    if resp.status_code == 200:
        return resp.json()

    try:
        body = resp.json()
    except Exception:
        body = {"error": resp.text}

    err_msg = body.get("error", "") or body.get("message", "") or resp.text

    if resp.status_code == 401 and "missing scope" in err_msg.lower():
        raise ScopeMissingError(err_msg)

    if resp.status_code == 404:
        raise ApiError(404, f"{path} not found")

    raise ApiError(resp.status_code, err_msg)


# -- Contact helpers -----------------------------------------------------------

def name_similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def find_existing_contact(conn, email, name):
    if email:
        rows = conn.execute("SELECT id FROM contacts WHERE emails LIKE ?", (f"%{email}%",)).fetchall()
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


def upsert_contact(conn, name, email, event_date):
    now = datetime.utcnow().isoformat()
    existing_id = find_existing_contact(conn, email, name)

    if existing_id:
        row = conn.execute("SELECT last_contact_date, emails FROM contacts WHERE id=?", (existing_id,)).fetchone()
        last_date = row[0] or ""
        existing_emails = set(json.loads(row[1] or "[]"))
        if email:
            existing_emails.add(email.lower())
        if event_date and event_date > last_date:
            conn.execute("""
                UPDATE contacts
                SET emails=?, last_contact_date=?, last_contact_channel='calendar', updated_at=?
                WHERE id=?
            """, (json.dumps(list(existing_emails)), event_date, now, existing_id))
        else:
            conn.execute("UPDATE contacts SET emails=?, updated_at=? WHERE id=?",
                         (json.dumps(list(existing_emails)), now, existing_id))
        stats["updated_contacts"] += 1
        return existing_id
    else:
        new_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO contacts
                (id, name, emails, phones, company, role,
                 relationship_type, source_account, first_seen_date,
                 last_contact_date, last_contact_channel, created_at, updated_at)
            VALUES (?, ?, ?, '[]', NULL, NULL, 'warm', 'roam',
                    ?, ?, 'calendar', ?, ?)
        """, (
            new_id, name,
            json.dumps([email.lower()] if email else []),
            event_date[:10] if event_date else now[:10],
            event_date[:10] if event_date else now[:10],
            now, now,
        ))
        stats["new_contacts"] += 1
        return new_id


def upsert_interaction(conn, contact_id, event_date, title, roam_recording_id, summary=None):
    """Use calendar_event_id column to store ro.am recording ID for dedup."""
    existing = conn.execute(
        "SELECT id FROM interactions WHERE calendar_event_id=? AND contact_id=?",
        (roam_recording_id, contact_id)
    ).fetchone()
    if existing:
        return False

    now = datetime.utcnow().isoformat()
    new_id = str(uuid.uuid4())
    conn.execute("""
        INSERT INTO interactions
            (id, contact_id, date, channel, direction, subject, summary, calendar_event_id, created_at)
        VALUES (?, ?, ?, 'calendar', 'attended', ?, ?, ?, ?)
    """, (new_id, contact_id, event_date or now[:10], title, summary, roam_recording_id, now))
    stats["new_interactions"] += 1
    return True


# -- ro.am data extraction -----------------------------------------------------

def extract_participants_from_recording(recording):
    """Pull participant list from a recording object."""
    participants = []

    for field in ["participants", "attendees", "members", "users"]:
        raw = recording.get(field, [])
        if raw:
            for p in raw:
                name = p.get("name") or p.get("displayName") or p.get("username") or ""
                email = (p.get("email") or p.get("emailAddress") or "").strip().lower()
                if name or email:
                    if not name and email:
                        name = email.split("@")[0].replace(".", " ").replace("_", " ").title()
                    participants.append({"name": name.strip(), "email": email})
            break

    return participants


def extract_participants_from_transcript(transcript, recording_id):
    """Pull speaker names from a transcript."""
    participants = []
    seen = set()

    for field in ["speakers", "participants"]:
        raw = transcript.get(field, [])
        for p in raw:
            name = p.get("name") or p.get("displayName") or ""
            email = (p.get("email") or "").strip().lower()
            key = email or name.lower()
            if key and key not in seen:
                seen.add(key)
                participants.append({"name": name.strip(), "email": email})

    # Fallback: extract unique speaker names from segments
    if not participants:
        segments = transcript.get("segments") or transcript.get("utterances") or []
        for seg in segments:
            speaker = seg.get("speaker") or seg.get("speakerName") or ""
            if speaker and speaker.lower() not in seen:
                seen.add(speaker.lower())
                participants.append({"name": speaker.strip(), "email": ""})

    return participants


def build_summary_from_transcript(transcript):
    """Extract a short plain-text summary from transcript segments."""
    segments = transcript.get("segments") or transcript.get("utterances") or []
    if not segments:
        return None

    lines = []
    for seg in segments[:5]:
        speaker = seg.get("speaker") or seg.get("speakerName") or "?"
        text = seg.get("text") or seg.get("transcript") or ""
        if text.strip():
            lines.append(f"{speaker}: {text.strip()[:100]}")
    if not lines:
        return None
    return " | ".join(lines[:3])


# -- Main pipeline -------------------------------------------------------------

def fetch_recordings():
    """Fetch all recordings from ro.am recording.list."""
    print("  Calling recording.list...", flush=True)
    data = api_get("recording.list")

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for field in ["recordings", "items", "data", "results"]:
            if field in data and isinstance(data[field], list):
                return data[field]
        if "id" in data:
            return [data]
    return []


def fetch_transcript(recording_id):
    """Fetch transcript for a specific recording."""
    return api_get("transcript.info", params={"recordingId": recording_id})


def process_recordings(conn, recordings):
    """Process each recording: extract participants, upsert into CRM."""
    print(f"  Processing {len(recordings)} recordings...", flush=True)
    stats["recordings_fetched"] = len(recordings)

    for recording in recordings:
        rec_id = recording.get("id") or recording.get("recordingId") or ""
        title = recording.get("title") or recording.get("name") or recording.get("subject") or "ro.am Meeting"
        started_at = (
            recording.get("startedAt") or recording.get("startTime") or
            recording.get("date") or recording.get("createdAt") or ""
        )
        event_date = started_at[:10] if started_at else ""

        participants = extract_participants_from_recording(recording)
        summary = None

        if rec_id:
            try:
                transcript = fetch_transcript(rec_id)
                stats["transcripts_fetched"] += 1
                transcript_participants = extract_participants_from_transcript(transcript, rec_id)
                if transcript_participants:
                    existing_emails = {p["email"] for p in participants if p["email"]}
                    for tp in transcript_participants:
                        if tp["email"] not in existing_emails:
                            participants.append(tp)
                summary = build_summary_from_transcript(transcript)
            except ScopeMissingError:
                pass
            except ApiError as e:
                if e.status_code != 404:
                    stats["errors"] += 1

        if not participants:
            continue

        for person in participants:
            name = person["name"]
            email = person["email"]
            if not name and not email:
                continue

            try:
                contact_id = upsert_contact(conn, name, email, event_date)
                upsert_interaction(conn, contact_id, event_date, title,
                                   rec_id or f"roam-{title[:20]}", summary)
            except Exception as e:
                stats["errors"] += 1
                print(f"    [ERROR] {name}: {e}", flush=True)


def main():
    print("=== ro.am Transcript Seed ===", flush=True)
    print(f"  API base: {API_BASE}", flush=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        recordings = fetch_recordings()
        if recordings:
            process_recordings(conn, recordings)
            conn.commit()
        else:
            print("  [INFO] recording.list returned 0 recordings.", flush=True)

    except ScopeMissingError as e:
        stats["scope_errors"] += 1
        print("", flush=True)
        print("  +---------------------------------------------------------+", flush=True)
        print("  |  ro.am SCOPE MISSING -- action required                  |", flush=True)
        print("  |                                                          |", flush=True)
        print("  |  API returned: \"Client missing scope\"                    |", flush=True)
        print("  |                                                          |", flush=True)
        print("  |  To activate: ro.am Admin -> Developer -> your API key   |", flush=True)
        print("  |  Add scopes:                                             |", flush=True)
        print("  |    * recordings:read                                     |", flush=True)
        print("  |    * transcripts:read                                    |", flush=True)
        print("  |                                                          |", flush=True)
        print("  |  Then re-run: python3 seeds/roam.py                      |", flush=True)
        print("  +---------------------------------------------------------+", flush=True)
        print("", flush=True)
        print("  Skipping ro.am seed -- CRM seeded from other sources.", flush=True)

    except ApiError as e:
        stats["errors"] += 1
        print(f"  [ERROR] recording.list: HTTP {e.status_code} -- {e.message}", flush=True)

    except Exception as e:
        stats["errors"] += 1
        print(f"  [ERROR] Unexpected: {e}", flush=True)

    conn.close()
    print(f"  Recordings: {stats['recordings_fetched']} | Transcripts: {stats['transcripts_fetched']}", flush=True)
    print(f"  New contacts: {stats['new_contacts']} | Updated: {stats['updated_contacts']}", flush=True)
    print(f"  Interactions: {stats['new_interactions']} | Errors: {stats['errors']}", flush=True)
    return stats


if __name__ == "__main__":
    main()

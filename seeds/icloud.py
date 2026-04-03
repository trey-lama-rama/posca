#!/usr/bin/env python3
"""
icloud.py — Pull contacts from iCloud CardDAV and upsert into CRM.
Idempotent. Matches on email first, then name similarity.
"""

import html
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
from config import DB_PATH, ICLOUD_CARDDAV_BASE, ICLOUD_USER, ICLOUD_PASS

import requests
import vobject

stats = {"new": 0, "updated": 0, "skipped": 0, "errors": 0}


def fetch_all_vcards():
    """REPORT request to get all vCards from iCloud."""
    # Use addressbook-query REPORT to get all vcards at once
    query_body = """<?xml version="1.0" encoding="UTF-8"?>
<C:addressbook-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:prop>
    <D:getetag/>
    <C:address-data/>
  </D:prop>
  <C:filter>
    <C:prop-filter name="FN"/>
  </C:filter>
</C:addressbook-query>"""

    auth = (ICLOUD_USER, ICLOUD_PASS)
    headers = {
        "Depth": "1",
        "Content-Type": "application/xml; charset=utf-8",
    }

    resp = requests.request(
        "REPORT",
        ICLOUD_CARDDAV_BASE,
        auth=auth,
        headers=headers,
        data=query_body.encode("utf-8"),
        timeout=60,
    )

    if resp.status_code not in (207, 200):
        print(f"  [iCloud] REPORT failed: HTTP {resp.status_code}", flush=True)
        print(f"  [iCloud] Response: {resp.text[:500]}", flush=True)
        return []

    # Parse vCard data from multistat XML response
    raw = resp.text
    vcards = []

    # Extract ADDRESS-DATA sections from XML
    patterns = [
        r'<[^>]*address-data[^>]*>(.*?)</[^>]*address-data>',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, raw, re.DOTALL | re.IGNORECASE)
        if matches:
            for m in matches:
                vcard_text = html.unescape(m).strip()
                if vcard_text.startswith("BEGIN:VCARD"):
                    vcards.append(vcard_text)
            break

    print(f"  [iCloud] Retrieved {len(vcards)} vCards", flush=True)
    return vcards


def parse_vcard(vcard_text):
    """Parse a vCard string and return a contact dict."""
    try:
        card = vobject.readOne(vcard_text)
    except Exception as e:
        # Try manual fallback
        return parse_vcard_manual(vcard_text)

    name = ""
    emails = []
    phones = []
    company = ""
    role = ""
    notes = ""
    address = ""
    birthday = ""
    anniversary = ""
    website = ""

    try:
        if hasattr(card, 'fn'):
            name = str(card.fn.value).strip()
        elif hasattr(card, 'n'):
            n = card.n.value
            parts = [n.prefix or "", n.given or "", n.additional or "", n.family or "", n.suffix or ""]
            name = " ".join(p for p in parts if p).strip()
    except Exception:
        pass

    try:
        for email_obj in card.contents.get('email', []):
            email_val = str(email_obj.value).strip().lower()
            if email_val and '@' in email_val:
                emails.append(email_val)
    except Exception:
        pass

    try:
        for tel_obj in card.contents.get('tel', []):
            phone_val = str(tel_obj.value).strip()
            if phone_val:
                phones.append(phone_val)
    except Exception:
        pass

    try:
        if hasattr(card, 'org'):
            org = card.org.value
            if isinstance(org, (list, tuple)):
                company = " ".join(str(o) for o in org if o).strip()
            else:
                company = str(org).strip()
    except Exception:
        pass

    try:
        if hasattr(card, 'title'):
            role = str(card.title.value).strip()
    except Exception:
        pass

    try:
        if hasattr(card, 'note'):
            notes = str(card.note.value).strip()
    except Exception:
        pass

    # Address (ADR)
    try:
        for adr_obj in card.contents.get('adr', []):
            adr = adr_obj.value
            parts = [adr.street or "", adr.city or "", adr.region or "",
                     adr.code or "", adr.country or ""]
            addr_str = ", ".join(p.strip() for p in parts if p and p.strip())
            if addr_str:
                address = addr_str
                break
    except Exception:
        pass

    # Birthday (BDAY)
    try:
        for bday_obj in card.contents.get('bday', []):
            bday_val = str(bday_obj.value).strip()
            if bday_val:
                birthday = bday_val[:10]  # YYYY-MM-DD or --MM-DD
                break
    except Exception:
        pass

    # Anniversary
    try:
        for ann_obj in card.contents.get('anniversary', []):
            ann_val = str(ann_obj.value).strip()
            if ann_val:
                anniversary = ann_val[:10]
                break
    except Exception:
        pass

    # Website (URL)
    try:
        for url_obj in card.contents.get('url', []):
            url_val = str(url_obj.value).strip()
            if url_val:
                website = url_val
                break
    except Exception:
        pass

    if not name:
        return None

    return {
        "name": name,
        "emails": emails,
        "phones": phones,
        "company": company,
        "role": role,
        "notes": notes,
        "address": address,
        "birthday": birthday,
        "anniversary": anniversary,
        "website": website,
    }


def parse_vcard_manual(vcard_text):
    """Fallback manual vCard parser."""
    lines = vcard_text.splitlines()
    name = ""
    emails = []
    phones = []
    company = ""
    role = ""
    notes_lines = []
    address = ""
    birthday = ""
    anniversary = ""
    website = ""

    for line in lines:
        line = line.strip()
        if line.startswith("FN:"):
            name = line[3:].strip()
        elif line.startswith("EMAIL") and ":" in line:
            val = line.split(":", 1)[1].strip().lower()
            if "@" in val:
                emails.append(val)
        elif line.startswith("TEL") and ":" in line:
            val = line.split(":", 1)[1].strip()
            if val:
                phones.append(val)
        elif line.startswith("ORG:"):
            company = line[4:].replace(";", " ").strip()
        elif line.startswith("TITLE:"):
            role = line[6:].strip()
        elif line.startswith("NOTE:"):
            notes_lines.append(line[5:].strip())
        elif line.startswith("ADR") and ":" in line:
            val = line.split(":", 1)[1]
            parts = [p.strip() for p in val.split(";") if p.strip()]
            if parts:
                address = ", ".join(parts)
        elif line.startswith("BDAY:"):
            birthday = line[5:].strip()[:10]
        elif line.startswith("ANNIVERSARY:"):
            anniversary = line[12:].strip()[:10]
        elif line.startswith("URL") and ":" in line:
            val = line.split(":", 1)[1].strip()
            if not website and val:
                website = val

    if not name:
        return None

    return {
        "name": name,
        "emails": emails,
        "phones": phones,
        "company": company,
        "role": role,
        "notes": " ".join(notes_lines),
        "address": address,
        "birthday": birthday,
        "anniversary": anniversary,
        "website": website,
    }


def name_similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def find_existing_contact(conn, contact):
    """Find existing contact by email first, then name similarity."""
    # Email match
    for email in contact["emails"]:
        rows = conn.execute("SELECT id, name, emails FROM contacts WHERE emails LIKE ?", (f'%{email}%',)).fetchall()
        for row in rows:
            existing_emails = json.loads(row[2] or "[]")
            if email in [e.lower() for e in existing_emails]:
                return row[0]

    # Name similarity match (threshold 0.85)
    if contact["name"]:
        rows = conn.execute("SELECT id, name FROM contacts").fetchall()
        for row in rows:
            if name_similarity(contact["name"], row[1]) >= 0.85:
                return row[0]

    return None


def upsert_contact(conn, contact):
    now = datetime.utcnow().isoformat()
    existing_id = find_existing_contact(conn, contact)

    if existing_id:
        # Merge emails and phones with existing
        row = conn.execute(
            "SELECT emails, phones, company, role, notes, address, birthday, anniversary, website, personal_data_source FROM contacts WHERE id=?",
            (existing_id,)
        ).fetchone()
        existing_emails = set(json.loads(row[0] or "[]"))
        existing_phones = set(json.loads(row[1] or "[]"))
        merged_emails = list(existing_emails | set(contact["emails"]))
        merged_phones = list(existing_phones | set(contact["phones"]))

        # Build provenance updates — iCloud is highest confidence, always wins
        sources = json.loads(row[9] or "{}")
        personal_fields = {}
        for field in ("address", "birthday", "anniversary", "website"):
            new_val = contact.get(field, "")
            if new_val:
                personal_fields[field] = new_val
                sources[field] = "icloud"

        conn.execute("""
            UPDATE contacts SET
                emails = ?,
                phones = ?,
                company = COALESCE(NULLIF(?, ''), company),
                role = COALESCE(NULLIF(?, ''), role),
                notes = COALESCE(NULLIF(?, ''), notes),
                address = COALESCE(NULLIF(?, ''), address),
                birthday = COALESCE(NULLIF(?, ''), birthday),
                anniversary = COALESCE(NULLIF(?, ''), anniversary),
                website = COALESCE(NULLIF(?, ''), website),
                personal_data_source = ?,
                updated_at = ?
            WHERE id = ?
        """, (
            json.dumps(merged_emails),
            json.dumps(merged_phones),
            contact["company"],
            contact["role"],
            contact["notes"],
            contact.get("address", ""),
            contact.get("birthday", ""),
            contact.get("anniversary", ""),
            contact.get("website", ""),
            json.dumps(sources),
            now,
            existing_id,
        ))
        stats["updated"] += 1
        return existing_id
    else:
        new_id = str(uuid.uuid4())
        sources = {}
        for field in ("address", "birthday", "anniversary", "website"):
            if contact.get(field):
                sources[field] = "icloud"

        conn.execute("""
            INSERT INTO contacts (id, name, emails, phones, company, role,
                address, birthday, anniversary, website, personal_data_source,
                relationship_type, source_account, first_seen_date, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'warm', 'icloud', ?, ?, ?)
        """, (
            new_id,
            contact["name"],
            json.dumps(contact["emails"]),
            json.dumps(contact["phones"]),
            contact["company"],
            contact["role"],
            contact.get("address", ""),
            contact.get("birthday", ""),
            contact.get("anniversary", ""),
            contact.get("website", ""),
            json.dumps(sources),
            now[:10],
            now,
            now,
        ))
        stats["new"] += 1
        return new_id


def main():
    print("=== iCloud Contacts Seed ===", flush=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    print("  Fetching vCards from iCloud CardDAV...", flush=True)
    vcards = fetch_all_vcards()

    if not vcards:
        print("  [WARNING] No vCards retrieved. Check CardDAV credentials/URL.", flush=True)
        conn.close()
        return stats

    print(f"  Parsing and upserting {len(vcards)} contacts...", flush=True)
    for vcard_text in vcards:
        try:
            contact = parse_vcard(vcard_text)
            if not contact or not contact["name"]:
                stats["skipped"] += 1
                continue
            upsert_contact(conn, contact)
        except Exception as e:
            stats["errors"] += 1
            print(f"  [ERROR] {e}", flush=True)

    conn.commit()
    conn.close()

    print(f"  Done: {stats['new']} new, {stats['updated']} updated, {stats['skipped']} skipped, {stats['errors']} errors", flush=True)
    return stats


if __name__ == "__main__":
    main()

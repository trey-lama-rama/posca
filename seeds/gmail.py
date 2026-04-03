#!/usr/bin/env python3
"""
gmail.py — Pull contact data from Gmail accounts via GOG CLI.
Idempotent. Noise-filtered. Upserts into contacts + interactions tables.
"""

import json
import os
import re
import sqlite3
import subprocess
import sys
import uuid
from datetime import datetime, date
from difflib import SequenceMatcher
from email.utils import parseaddr, getaddresses
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, ACCOUNTS, GOG_BIN

# LLM filter — lazy import; gracefully degrades if unavailable
def _llm_filter_available():
    try:
        import importlib
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import llm_filter  # noqa: F401
        return True
    except Exception:
        return False

_LLM_FILTER_ENABLED = _llm_filter_available()

# Noise filter: skip messages from these domain patterns
NOISE_DOMAINS = {
    "linkedin.com", "calendly.com", "docusign.com", "google.com", "googleapis.com",
    "googlemail.com", "microsoft.com", "apple.com", "substack.com", "mailchimp.com",
    "constantcontact.com", "klaviyo.com", "sendgrid.net", "mailgun.org",
    "hubspotemail.net", "salesforce.com", "marketo.com", "pardot.com",
    "zoom.us", "zoominfo.com", "smartsheet.com",
    "notifications.google.com", "mail.linkedin.com", "bounce.linkedin.com",
    "em.getrevue.co", "e.getrevue.co",
}

NOISE_PREFIXES = {
    "no-reply", "noreply", "notifications", "notification", "info",
    "admin", "support", "do-not-reply", "donotreply", "newsletter",
    "marketing", "hello", "team", "updates", "alerts", "bounce",
    "mailer-daemon", "postmaster",
}

NOISE_SUBJECT_PATTERNS = [
    r'unsubscribe',
    r"you'?re receiving this",
    r'newsletter',
    r'automated message',
    r'do not reply',
    r'mailing list',
    r'manage preferences',
    r'view in browser',
    r'email preferences',
    r'update your preferences',
]
NOISE_SUBJECT_RE = re.compile("|".join(NOISE_SUBJECT_PATTERNS), re.IGNORECASE)


def _build_stats():
    """Build stats dict with per-account keys derived from ACCOUNTS config."""
    s = {"skipped_noise": 0, "errors": 0}
    for i, acct in enumerate(ACCOUNTS):
        prefix = f"acct{i}"
        s[f"{prefix}_processed"] = 0
        s[f"{prefix}_contacts"] = 0
        s[f"{prefix}_interactions"] = 0
    return s

stats = _build_stats()


def _account_stat_prefix(addr):
    """Return the stats key prefix for a given account address."""
    for i, acct in enumerate(ACCOUNTS):
        if acct["address"] == addr:
            return f"acct{i}"
    return "acct0"


def run_gog(args):
    """Run GOG CLI and return parsed JSON."""
    cmd = [GOG_BIN] + args
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"GOG failed: {result.stderr[:200]}")
    return json.loads(result.stdout)


def is_noise_email(email_addr):
    """Return True if this email should be filtered out."""
    if not email_addr or '@' not in email_addr:
        return True

    local, domain = email_addr.lower().rsplit('@', 1)

    # Check domain
    domain_root = '.'.join(domain.split('.')[-2:])
    if domain_root in NOISE_DOMAINS or domain in NOISE_DOMAINS:
        return True

    # Check local part prefix
    for prefix in NOISE_PREFIXES:
        if local == prefix or local.startswith(prefix + '+') or local.startswith(prefix + '-') or local.startswith(prefix + '.') or local.startswith(prefix + '_'):
            return True

    # Unsubscribe pattern in address
    if 'unsubscribe' in local or 'newsletter' in local:
        return True

    return False


def is_noise_subject(subject):
    """Return True if subject indicates automated/marketing message."""
    if not subject:
        return False
    return bool(NOISE_SUBJECT_RE.search(subject))


def extract_people_from_headers(from_h, to_h, cc_h, account_address, direction):
    """Extract non-self, non-noise people from email headers."""
    people = []

    all_addrs = []
    if from_h:
        all_addrs.append(("from", from_h))
    if to_h:
        all_addrs.append(("to", to_h))
    if cc_h:
        all_addrs.append(("cc", cc_h))

    for field, header_val in all_addrs:
        parsed = getaddresses([header_val])
        for display_name, email_addr in parsed:
            email_addr = email_addr.strip().lower()
            if not email_addr or '@' not in email_addr:
                continue
            # Skip self
            if email_addr == account_address.lower():
                continue
            # Skip noise
            if is_noise_email(email_addr):
                continue

            name = display_name.strip() if display_name.strip() else email_addr.split('@')[0].replace('.', ' ').title()
            people.append({"name": name, "email": email_addr})

    # Deduplicate by email
    seen = set()
    unique = []
    for p in people:
        if p["email"] not in seen:
            seen.add(p["email"])
            unique.append(p)

    return unique


def name_similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def find_existing_contact(conn, email, name):
    """Find existing contact by email, then name similarity."""
    if email:
        rows = conn.execute("SELECT id FROM contacts WHERE emails LIKE ?", (f'%{email}%',)).fetchall()
        for row in rows:
            # Verify email is actually in JSON array
            full = conn.execute("SELECT emails FROM contacts WHERE id=?", (row[0],)).fetchone()
            try:
                if email in [e.lower() for e in json.loads(full[0] or "[]")]:
                    return row[0]
            except Exception:
                pass

    if name and len(name) > 2:
        rows = conn.execute("SELECT id, name FROM contacts").fetchall()
        for row in rows:
            if name_similarity(name, row[1]) >= 0.85:
                return row[0]

    return None


def upsert_contact(conn, name, email, default_rel_type, source_account, msg_date, msg_direction):
    """Upsert contact. Returns (contact_id, is_new)."""
    now = datetime.utcnow().isoformat()
    existing_id = find_existing_contact(conn, email, name)

    if existing_id:
        # Update last_contact if this message is more recent
        row = conn.execute("SELECT last_contact_date, emails FROM contacts WHERE id=?", (existing_id,)).fetchone()
        last_date = row[0] or ""
        should_update_last = msg_date > last_date if msg_date else False

        existing_emails = set(json.loads(row[1] or "[]"))
        if email:
            existing_emails.add(email)

        update_kwargs = [json.dumps(list(existing_emails)), now]
        if should_update_last and msg_date:
            conn.execute("""
                UPDATE contacts SET emails=?, last_contact_date=?, last_contact_channel='email', updated_at=? WHERE id=?
            """, (json.dumps(list(existing_emails)), msg_date, now, existing_id))
        else:
            conn.execute("UPDATE contacts SET emails=?, updated_at=? WHERE id=?",
                         (json.dumps(list(existing_emails)), now, existing_id))
        return existing_id, False
    else:
        new_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO contacts (id, name, emails, phones, company, role,
                relationship_type, source_account, first_seen_date,
                last_contact_date, last_contact_channel, created_at, updated_at)
            VALUES (?, ?, ?, '[]', NULL, NULL, ?, ?, ?, ?, 'email', ?, ?)
        """, (
            new_id, name, json.dumps([email] if email else []),
            default_rel_type, source_account,
            msg_date[:10] if msg_date else now[:10],
            msg_date[:10] if msg_date else now[:10],
            now, now,
        ))
        return new_id, True


_DEAL_RE = re.compile(r'\b(term sheet|valuation|equity|due diligence|cap table|investment|fund|loi|letter of intent|deal|closing|diligence|acquisition|m&a|series [abcde]|exit)\b', re.I)
_DEALFLOW_RE = re.compile(r'\b(pitch|deck|startup|intro|introduction|referred|warm intro|pre.?seed|angel|raise)\b', re.I)
_FOLLOWUP_RE = re.compile(r'\b(follow.?up|following up|checking in|check.?in|touching base|circle back|per our|as discussed)\b', re.I)
_VENDOR_RE = re.compile(r'\b(invoice|payment|contract|renewal|subscription|service agreement|billing|retainer|sow)\b', re.I)
_PERSONAL_RE = re.compile(r'\b(birthday|wedding|graduation|holiday|vacation|family|personal|congrats|happy new year|thank you|thanks for)\b', re.I)


def _classify_subject(subject, direction):
    """Fast keyword classification of interaction type from email subject."""
    text = (subject or "").lower()
    if _DEAL_RE.search(text):
        return "deal_discussion"
    if _DEALFLOW_RE.search(text):
        return "deal_flow" if direction == "inbound" else "introduction"
    if _FOLLOWUP_RE.search(text):
        return "follow_up"
    if _VENDOR_RE.search(text):
        return "vendor"
    if _PERSONAL_RE.search(text):
        return "personal"
    return "meeting"


def upsert_interaction(conn, contact_id, msg_date, direction, subject, gmail_msg_id, snippet=None):
    """Insert interaction if not already logged for this gmail_message_id."""
    if gmail_msg_id:
        existing = conn.execute("SELECT id FROM interactions WHERE gmail_message_id=?", (gmail_msg_id,)).fetchone()
        if existing:
            return False

    now = datetime.utcnow().isoformat()
    new_id = str(uuid.uuid4())
    # Clean snippet: strip whitespace, truncate to 300 chars
    summary = None
    if snippet:
        summary = snippet.strip()[:300] if snippet.strip() else None

    # Check if interaction_type column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(interactions)").fetchall()]
    itype = _classify_subject(subject, direction) if "interaction_type" in cols else None

    if itype:
        conn.execute("""
            INSERT INTO interactions (id, contact_id, date, channel, direction, subject, summary, gmail_message_id, interaction_type, created_at)
            VALUES (?, ?, ?, 'email', ?, ?, ?, ?, ?, ?)
        """, (new_id, contact_id, msg_date or now[:10], direction, subject, summary, gmail_msg_id, itype, now))
    else:
        conn.execute("""
            INSERT INTO interactions (id, contact_id, date, channel, direction, subject, summary, gmail_message_id, created_at)
            VALUES (?, ?, ?, 'email', ?, ?, ?, ?, ?)
        """, (new_id, contact_id, msg_date or now[:10], direction, subject, summary, gmail_msg_id, now))
    return True


def get_message_detail(msg_id, account):
    """Fetch full message details via GOG."""
    data = run_gog(["gmail", "get", msg_id, "-a", account, "-j"])
    return data


def process_account(conn, account_cfg):
    """Process one Gmail account: sent + inbox."""
    addr = account_cfg["address"]
    label = account_cfg["label"]
    default_rel = account_cfg["default_rel_type"]
    stat_prefix = _account_stat_prefix(addr)

    print(f"\n  --- {label} ({addr}) ---", flush=True)

    queries = [
        ("in:sent after:2024/01/01", "outbound"),
        ("in:inbox -category:promotions -category:updates after:2024/01/01", "inbound"),
    ]

    for query, direction in queries:
        print(f"    Querying [{direction}]: {query}", flush=True)
        try:
            result = run_gog(["gmail", "list", query, "-a", addr, "--limit=500", "-j"])
        except Exception as e:
            print(f"    [ERROR] list failed: {e}", flush=True)
            stats["errors"] += 1
            continue

        threads = result.get("threads", [])
        print(f"    Found {len(threads)} threads", flush=True)

        for thread in threads:
            thread_id = thread.get("id")
            subject = thread.get("subject", "")
            msg_date = thread.get("date", "")[:10] if thread.get("date") else ""
            from_h = thread.get("from", "")

            stats[f"{stat_prefix}_processed"] += 1

            # Noise filter on subject
            if is_noise_subject(subject):
                stats["skipped_noise"] += 1
                continue

            # Noise filter on from address
            _, from_addr = parseaddr(from_h)
            if from_addr and is_noise_email(from_addr.lower()):
                stats["skipped_noise"] += 1
                continue

            # Get full message for To/Cc headers
            try:
                msg_data = get_message_detail(thread_id, addr)
            except Exception as e:
                stats["errors"] += 1
                continue

            headers = msg_data.get("headers", {}) if isinstance(msg_data, dict) else {}
            if not headers:
                # Try messages list
                messages = msg_data.get("messages", []) if isinstance(msg_data, dict) else []
                if messages:
                    headers = messages[0].get("headers", {})

            from_header = headers.get("From", from_h)
            to_header = headers.get("To", "")
            cc_header = headers.get("Cc", "")

            # Some gog versions return flat fields
            if not from_header:
                from_header = msg_data.get("from", from_h) if isinstance(msg_data, dict) else from_h
            if not to_header:
                to_header = msg_data.get("to", "") if isinstance(msg_data, dict) else ""
            if not cc_header:
                cc_header = msg_data.get("cc", "") if isinstance(msg_data, dict) else ""

            people = extract_people_from_headers(from_header, to_header, cc_header, addr, direction)

            if not people:
                stats["skipped_noise"] += 1
                continue

            # LLM borderline filter — second-pass on the primary sender only
            # Keyword pre-filter already ran above; LLM catches ambiguous cases
            if _LLM_FILTER_ENABLED and people:
                try:
                    import llm_filter
                    primary = people[0]
                    snippet = msg_data.get("snippet", "") if isinstance(msg_data, dict) else ""
                    keep, reason = llm_filter.is_real_contact(
                        primary["name"], primary["email"], subject, snippet
                    )
                    if not keep:
                        stats["skipped_noise"] += 1
                        continue
                except Exception:
                    pass  # LLM filter failure is non-fatal; proceed

            # Extract snippet for summary
            snippet = msg_data.get("snippet", "") if isinstance(msg_data, dict) else ""

            for person in people:
                try:
                    contact_id, is_new = upsert_contact(
                        conn, person["name"], person["email"],
                        default_rel, addr, msg_date, direction
                    )
                    if is_new:
                        stats[f"{stat_prefix}_contacts"] += 1

                    inserted = upsert_interaction(conn, contact_id, msg_date, direction, subject, thread_id, snippet)
                    if inserted:
                        stats[f"{stat_prefix}_interactions"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    print(f"    [ERROR] contact upsert: {e}", flush=True)


def main():
    print("=== Gmail Contacts Seed ===", flush=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    for acct in ACCOUNTS:
        try:
            process_account(conn, acct)
            conn.commit()
        except Exception as e:
            print(f"  [ERROR] Account {acct['address']}: {e}", flush=True)
            stats["errors"] += 1

    conn.close()
    for i, acct in enumerate(ACCOUNTS):
        prefix = f"acct{i}"
        print(f"  {acct['label']}: {stats[f'{prefix}_contacts']} new contacts, {stats[f'{prefix}_interactions']} interactions", flush=True)
    print(f"  Noise filtered: {stats['skipped_noise']} | Errors: {stats['errors']}", flush=True)
    return stats


if __name__ == "__main__":
    main()

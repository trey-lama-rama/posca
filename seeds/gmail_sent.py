#!/usr/bin/env python3
"""
gmail_sent.py — Scan sent mail from Gmail accounts for contacts not in CRM.
Idempotent. Only inserts; skips addresses already in contacts.emails JSON array.
"""

import json
import os
import re
import sqlite3
import subprocess
import sys
import argparse
from datetime import datetime
from email.utils import getaddresses, parseaddr
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, ACCOUNTS, ACCOUNT_EMAILS, GOG_BIN

# Domains belonging to the sending accounts — derived from config
OWN_DOMAINS = {addr.split("@")[1] for addr in ACCOUNT_EMAILS}

# Noise address prefixes to skip
NOISE_PREFIXES = {
    "noreply", "no-reply", "no_reply", "mailer-daemon", "postmaster",
    "notifications", "notification", "bounce", "bounces", "donotreply",
    "do-not-reply", "do_not_reply", "unsubscribe", "newsletter",
    "admin", "support", "info", "hello", "team", "updates", "alerts",
    "marketing", "automated",
}

# Noise domain patterns to skip
NOISE_DOMAINS = {
    "linkedin.com", "calendly.com", "docusign.com", "google.com", "googleapis.com",
    "googlemail.com", "microsoft.com", "apple.com", "substack.com", "mailchimp.com",
    "constantcontact.com", "klaviyo.com", "sendgrid.net", "mailgun.org",
    "hubspotemail.net", "salesforce.com", "marketo.com", "pardot.com",
    "zoom.us", "zoominfo.com", "smartsheet.com",
    "notifications.google.com", "mail.linkedin.com", "bounce.linkedin.com",
    "amazonses.com", "sendgrid.com", "mailjet.com",
}

stats = {
    "messages_scanned": 0,
    "new_contacts": 0,
    "skipped_crm": 0,
    "skipped_filtered": 0,
    "errors": 0,
}


def run_gog(args, timeout=120):
    """Run GOG CLI and return stdout text."""
    cmd = [GOG_BIN] + args
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(f"GOG failed (rc={result.returncode}): {result.stderr[:300]}")
    return result.stdout


def is_noise_email(email_addr):
    """Return True if this email address should be filtered out."""
    if not email_addr or '@' not in email_addr:
        return True

    local, domain = email_addr.lower().rsplit('@', 1)

    # Own domains
    if domain in OWN_DOMAINS:
        return True

    # Noise domain (check root domain too)
    domain_root = '.'.join(domain.split('.')[-2:])
    if domain in NOISE_DOMAINS or domain_root in NOISE_DOMAINS:
        return True

    # Noise local-part prefixes
    for prefix in NOISE_PREFIXES:
        if (local == prefix
                or local.startswith(prefix + '+')
                or local.startswith(prefix + '-')
                or local.startswith(prefix + '.')
                or local.startswith(prefix + '_')):
            return True

    # Pattern-based noise in local part
    if 'unsubscribe' in local or 'newsletter' in local or 'bounce' in local:
        return True

    return False


def email_in_crm(conn, email_addr):
    """Return True if email_addr already exists in any contact's emails JSON array."""
    email_lower = email_addr.lower()
    rows = conn.execute(
        "SELECT emails FROM contacts WHERE emails LIKE ?",
        (f'%{email_lower}%',)
    ).fetchall()
    for (emails_json,) in rows:
        try:
            arr = json.loads(emails_json or "[]")
            if email_lower in [e.lower() for e in arr]:
                return True
        except Exception:
            pass
    return False


def generate_id(conn):
    """Generate a random hex ID consistent with existing contacts style."""
    row = conn.execute("SELECT lower(hex(randomblob(8)))").fetchone()
    return row[0]


def insert_contact(conn, name, email, source_account, msg_date):
    """Insert a new contact. Returns the new contact id."""
    now = datetime.utcnow().isoformat()
    date_str = msg_date[:10] if msg_date and len(msg_date) >= 10 else now[:10]
    new_id = generate_id(conn)

    conn.execute("""
        INSERT INTO contacts (
            id, name, emails, phones, company, role,
            relationship_type, relationship_heat,
            source_account,
            first_seen_date, last_contact_date, last_contact_channel,
            stale_flag, notes,
            created_at, updated_at
        ) VALUES (
            ?, ?, ?, '[]', NULL, NULL,
            'unknown', 'cold',
            ?,
            ?, ?, 'email',
            0, 'Auto-discovered from sent mail scan -- needs review',
            ?, ?
        )
    """, (
        new_id, name, json.dumps([email]),
        source_account,
        date_str, date_str,
        now, now,
    ))
    return new_id


def parse_recipients_from_thread(thread_output, account_address):
    """
    Parse To/CC recipient email+name pairs from `gog gmail threads show` output.
    The output is text with lines like:
        To: Name <email>, Name2 <email2>
        Cc: ...
    Returns list of {"name": ..., "email": ...} dicts (filtered, deduplicated).
    """
    recipients = []
    seen = set()

    # Match header lines: "To:", "Cc:", "CC:" (possibly indented or prefixed)
    header_re = re.compile(r'^\s*(?:To|Cc|CC)\s*:\s*(.+)$', re.IGNORECASE | re.MULTILINE)

    for match in header_re.finditer(thread_output):
        header_val = match.group(1).strip()
        parsed = getaddresses([header_val])
        for display_name, email_addr in parsed:
            email_addr = email_addr.strip().lower()
            if not email_addr or '@' not in email_addr:
                continue
            if email_addr == account_address.lower():
                continue
            if is_noise_email(email_addr):
                continue
            if email_addr in seen:
                continue
            seen.add(email_addr)

            name = display_name.strip()
            if not name:
                name = email_addr.split('@')[0].replace('.', ' ').replace('_', ' ').replace('-', ' ').title()

            recipients.append({"name": name, "email": email_addr})

    return recipients


def scan_account(conn, account_cfg, max_messages, insert_limit, insert_count):
    """
    Scan sent mail for one account and insert new contacts.
    Returns updated insert_count.
    """
    addr = account_cfg["address"]
    label = account_cfg["label"]

    print(f"\n  --- {label} ({addr}) ---", flush=True)
    print(f"    Fetching sent mail (max {max_messages})...", flush=True)

    try:
        raw = run_gog([
            "gmail", "messages", "search", "in:sent",
            "--account", addr,
            "--max", str(max_messages),
        ], timeout=180)
    except Exception as e:
        print(f"    [ERROR] messages search failed: {e}", flush=True)
        stats["errors"] += 1
        return insert_count

    # Parse tab-separated output: ID\tTHREAD\tDATE\tFROM\tSUBJECT\tLABELS
    lines = [ln for ln in raw.strip().splitlines() if ln.strip()]
    print(f"    Found {len(lines)} sent messages", flush=True)

    # Collect unique thread IDs with their dates/subjects for processing
    seen_threads = {}  # thread_id -> earliest/best date
    msg_meta = {}      # thread_id -> {"date": ..., "subject": ...}

    for line in lines:
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        msg_id = parts[0].strip()
        thread_id = parts[1].strip() if len(parts) > 1 else msg_id
        date_str = parts[2].strip() if len(parts) > 2 else ""
        subject = parts[4].strip() if len(parts) > 4 else ""

        if thread_id and thread_id not in seen_threads:
            seen_threads[thread_id] = date_str
            msg_meta[thread_id] = {"date": date_str, "subject": subject}

    print(f"    Processing {len(seen_threads)} unique threads...", flush=True)

    thread_list = list(seen_threads.items())

    for i, (thread_id, date_str) in enumerate(thread_list):
        stats["messages_scanned"] += 1

        if insert_limit is not None and insert_count >= insert_limit:
            print(f"    Insert limit ({insert_limit}) reached, stopping.", flush=True)
            break

        if i > 0 and i % 50 == 0:
            print(f"    ... processed {i}/{len(thread_list)} threads", flush=True)
            conn.commit()

        try:
            thread_output = run_gog([
                "gmail", "threads", "show", thread_id,
                "--account", addr,
            ], timeout=60)
        except Exception as e:
            stats["errors"] += 1
            continue

        recipients = parse_recipients_from_thread(thread_output, addr)

        if not recipients:
            stats["skipped_filtered"] += 1
            continue

        meta = msg_meta.get(thread_id, {})
        msg_date = meta.get("date", "")

        for person in recipients:
            email = person["email"]
            name = person["name"]

            if is_noise_email(email):
                stats["skipped_filtered"] += 1
                continue

            if email_in_crm(conn, email):
                stats["skipped_crm"] += 1
                continue

            if insert_limit is not None and insert_count >= insert_limit:
                break

            try:
                new_id = insert_contact(conn, name, email, addr, msg_date)
                stats["new_contacts"] += 1
                insert_count += 1
                print(f"    [NEW] {name} <{email}> (from thread {thread_id[:12]}...)", flush=True)
            except Exception as e:
                stats["errors"] += 1
                print(f"    [ERROR] insert failed for {email}: {e}", flush=True)

    conn.commit()
    return insert_count


def main():
    parser = argparse.ArgumentParser(description="Seed CRM from sent mail in Gmail accounts.")
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Maximum number of new contacts to insert (default: no limit)"
    )
    parser.add_argument(
        "--max-messages", type=int, default=500,
        help="Maximum sent messages to scan per account (default: 500)"
    )
    args = parser.parse_args()

    print("=== Sent Mail Contacts Seed ===", flush=True)
    print(f"  Accounts: {', '.join(a['address'] for a in ACCOUNTS)}", flush=True)
    print(f"  Max messages per account: {args.max_messages}", flush=True)
    if args.limit is not None:
        print(f"  Insert limit: {args.limit}", flush=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    insert_count = 0

    for acct in ACCOUNTS:
        if args.limit is not None and insert_count >= args.limit:
            print(f"\n  Insert limit reached after first account, skipping remaining.", flush=True)
            break
        try:
            insert_count = scan_account(conn, acct, args.max_messages, args.limit, insert_count)
        except Exception as e:
            print(f"  [ERROR] Account {acct['address']}: {e}", flush=True)
            stats["errors"] += 1
            conn.rollback()

    conn.close()

    print(f"\n  ===== Summary =====", flush=True)
    print(f"  Messages/threads scanned : {stats['messages_scanned']}", flush=True)
    print(f"  New contacts added       : {stats['new_contacts']}", flush=True)
    print(f"  Skipped (already in CRM) : {stats['skipped_crm']}", flush=True)
    print(f"  Skipped (filtered/noise) : {stats['skipped_filtered']}", flush=True)
    print(f"  Errors                   : {stats['errors']}", flush=True)

    return stats


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
llm_filter.py — LLM-based contact quality filter for CRM.
Classifies email senders as real contacts (KEEP) or noise (SKIP).

Caches results in filter_cache table — never re-classifies the same sender email.
Used by seed_gmail.py for borderline cases that pass keyword pre-filtering.

Usage:
  from llm_filter import is_real_contact
  keep, reason = is_real_contact(from_name, from_email, subject, snippet)
"""

import json
import logging
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import openai

# ── Config ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, LOG_DIR, ENRICHMENT_MODEL

LOG_PATH = os.path.join(LOG_DIR, "llm-filter.log")
MODEL = ENRICHMENT_MODEL

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_filter_cache_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS filter_cache (
            email TEXT PRIMARY KEY,
            decision TEXT NOT NULL CHECK(decision IN ('KEEP', 'SKIP')),
            reason TEXT,
            cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def _get_cached(conn, email):
    row = conn.execute(
        "SELECT decision, reason FROM filter_cache WHERE email = ?",
        (email.lower(),),
    ).fetchone()
    if row:
        return row["decision"], row["reason"]
    return None, None


def _cache_decision(conn, email, decision, reason):
    conn.execute("""
        INSERT OR REPLACE INTO filter_cache (email, decision, reason, cached_at)
        VALUES (?, ?, ?, ?)
    """, (email.lower(), decision, reason, datetime.utcnow().isoformat()))
    conn.commit()


def is_real_contact(from_name, from_email, subject, snippet):
    """
    Returns (bool, reason_str).
    True  = real person, worth saving as CRM contact.
    False = noise/automated/marketing, skip.

    Checks SQLite cache first; calls LLM only for uncached emails.
    """
    if not from_email or "@" not in from_email:
        return False, "invalid_email"

    conn = get_conn()
    ensure_filter_cache_table(conn)

    # Cache hit
    decision, reason = _get_cached(conn, from_email)
    if decision is not None:
        conn.close()
        return decision == "KEEP", reason

    # LLM classification
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        conn.close()
        log.warning("OPENAI_API_KEY not set — skipping LLM filter, defaulting KEEP")
        return True, "no_api_key"

    client = openai.OpenAI(api_key=api_key)

    prompt = (
        "You are a CRM filter. Decide if this email sender is a real person worth "
        "saving as a contact.\n\n"
        f"From name: {from_name or 'unknown'}\n"
        f"From email: {from_email}\n"
        f"Subject: {subject or 'none'}\n"
        f"Snippet: {(snippet or '')[:200]}\n\n"
        "Rules:\n"
        "- KEEP: real humans (colleagues, clients, investors, partners, friends, "
        "individual cold outreach)\n"
        "- SKIP: automated systems, marketing, newsletters, notifications, "
        "no-reply addresses, bots, SaaS platforms\n\n"
        "Reply with exactly: KEEP <one_word_reason> or SKIP <one_word_reason>\n"
        "Examples: KEEP colleague, SKIP newsletter, SKIP automated, KEEP coldoutreach"
    )

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=10,
            temperature=0,
        )
        text = resp.choices[0].message.content.strip().upper()
        parts = text.split()
        decision = parts[0] if parts else "SKIP"
        reason = parts[1] if len(parts) > 1 else "unknown"
        if decision not in ("KEEP", "SKIP"):
            decision = "SKIP"
            reason = "parse_error"
    except Exception as e:
        log.error("LLM filter error for %s: %s", from_email, e)
        decision = "KEEP"
        reason = "api_error_default_keep"

    _cache_decision(conn, from_email, decision, reason)
    conn.close()

    return decision == "KEEP", reason


if __name__ == "__main__":
    # Quick smoke test
    tests = [
        ("John Smith", "john.smith@acmecorp.com", "Following up on our call", "Great talking with you..."),
        ("Mailchimp", "newsletter@mailchimp.com", "Your weekly digest", "Unsubscribe here..."),
        ("", "no-reply@docusign.com", "Please sign", "Document ready..."),
    ]
    for name, email, subj, snip in tests:
        keep, reason = is_real_contact(name, email, subj, snip)
        print(f"{'KEEP' if keep else 'SKIP'} [{reason}] — {email}")

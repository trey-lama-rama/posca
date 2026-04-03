#!/usr/bin/env python3
"""
contact_summary.py — Generate AI-powered summaries for high-value CRM contacts.

For each hot/warm contact without a summary, uses the configured LLM to generate
a 2-3 sentence briefing: who they are, how they connect to the CRM owner,
and what's the latest touch point.

Stored in contacts.notes under a "## Summary" block (replaces prior block).
Idempotent: skips contacts with a recent summary (unless --force).

Usage:
  python3 -m enrichment.contact_summary [--limit 30] [--heat hot,warm] [--force]
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, LOG_DIR, ENRICHMENT_MODEL, RATE_LIMIT_SECONDS, get_secret

import openai

LOG_PATH = os.path.join(LOG_DIR, "enrichment.log")

# Override this via config.yaml or environment to customize summary prompts
OWNER_CONTEXT = os.environ.get(
    "CRM_OWNER_CONTEXT",
    "CRM owner — professional contact",
)


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [summary] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _ensure_col(conn):
    """Add ai_summary_at column to track when summary was generated."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()]
    if "ai_summary_at" not in cols:
        conn.execute("ALTER TABLE contacts ADD COLUMN ai_summary_at TEXT")
        conn.commit()
        log("Added ai_summary_at column")


def get_recent_interactions(conn, contact_id, limit=5):
    """Get most recent interactions for context."""
    rows = conn.execute("""
        SELECT date, channel, direction, subject, summary
        FROM interactions WHERE contact_id=?
        ORDER BY date DESC LIMIT ?
    """, (contact_id, limit)).fetchall()
    return rows


def build_contact_context(c, interactions):
    """Build context string for LLM."""
    parts = [f"Name: {c['name']}"]
    if c["company"]:
        parts.append(f"Company: {c['company']}")
    if c["role"]:
        parts.append(f"Role: {c['role']}")
    if c["relationship_type"]:
        parts.append(f"Relationship type: {c['relationship_type']}")
    if c["last_contact_date"]:
        parts.append(f"Last contacted: {c['last_contact_date'][:10]}")

    # Extract notes without existing AI summary block
    notes = c["notes"] or ""
    # Pull just the enrichment block if present
    enrichment_match = re.search(r"## Enrichment\n(.*?)(?=\n##|\Z)", notes, re.DOTALL)
    if enrichment_match:
        parts.append(f"Enrichment: {enrichment_match.group(1).strip()[:300]}")

    if interactions:
        ix_parts = []
        for ix in interactions[:3]:
            subj = ix["subject"] or ""
            summ = (ix["summary"] or "")[:100]
            date = ix["date"][:10] if ix["date"] else ""
            ix_parts.append(f"{date} via {ix['channel']}: {subj}" + (f" -- {summ}" if summ else ""))
        parts.append("Recent interactions:\n  " + "\n  ".join(ix_parts))

    return "\n".join(parts)


def generate_summary(client, contact_context):
    """Use LLM to generate a 2-3 sentence contact summary."""
    prompt = (
        f"You are writing a concise briefing for: {OWNER_CONTEXT}.\n\n"
        f"Contact information:\n{contact_context}\n\n"
        "Write 2-3 sentences that capture:\n"
        "1. Who this person is professionally\n"
        "2. The nature/quality of the relationship\n"
        "3. Most recent relevant context (if any interactions exist)\n\n"
        "Keep it tight, factual, and useful for pre-meeting prep. "
        "Do not speculate beyond the provided data. "
        "If insufficient data, write what you know and note 'Limited data available.'"
    )

    try:
        resp = client.chat.completions.create(
            model=ENRICHMENT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=150,
            temperature=0.3,
        )
        summary = resp.choices[0].message.content.strip()
        # Sanity: cap length
        if len(summary) > 400:
            summary = summary[:400] + "..."
        return summary
    except Exception as e:
        log(f"LLM summary failed: {e}")
        return None


def inject_summary_into_notes(existing_notes, summary):
    """Inject or replace the ## Summary block in notes."""
    clean = re.sub(r"\n?## Summary\n.*?(?=\n##|\Z)", "", (existing_notes or ""), flags=re.DOTALL).strip()
    block = f"## Summary\n{summary}"
    return (clean + "\n\n" + block).strip() if clean else block


def main():
    parser = argparse.ArgumentParser(description="Generate AI summaries for high-value contacts")
    parser.add_argument("--limit", type=int, default=30, help="Max contacts to summarize")
    parser.add_argument("--heat", default="hot,warm", help="Comma-separated heat levels to target")
    parser.add_argument("--force", action="store_true", help="Re-generate existing summaries")
    args = parser.parse_args()

    heat_list = [h.strip() for h in args.heat.split(",")]
    log(f"=== Contact Summary Generation Starting (limit={args.limit}, heat={heat_list}) ===")

    api_key = get_secret("OPENAI_API_KEY")
    if not api_key:
        log("ERROR: OPENAI_API_KEY not set")
        sys.exit(1)

    client = openai.OpenAI(api_key=api_key)
    conn = get_conn()
    _ensure_col(conn)

    # Check if relationship_heat column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()]
    has_heat = "relationship_heat" in cols

    if has_heat and not args.force:
        placeholders = ",".join("?" * len(heat_list))
        contacts = conn.execute(f"""
            SELECT * FROM contacts
            WHERE relationship_heat IN ({placeholders})
              AND (ai_summary_at IS NULL)
            ORDER BY COALESCE(relationship_score, 0) DESC
            LIMIT ?
        """, heat_list + [args.limit]).fetchall()
    elif has_heat:
        placeholders = ",".join("?" * len(heat_list))
        contacts = conn.execute(f"""
            SELECT * FROM contacts
            WHERE relationship_heat IN ({placeholders})
            ORDER BY COALESCE(relationship_score, 0) DESC
            LIMIT ?
        """, heat_list + [args.limit]).fetchall()
    else:
        # Fallback: last-contacted contacts
        contacts = conn.execute("""
            SELECT * FROM contacts
            WHERE ai_summary_at IS NULL
            ORDER BY COALESCE(last_contact_date, '') DESC
            LIMIT ?
        """, (args.limit,)).fetchall()

    total = len(contacts)
    log(f"Contacts to summarize: {total}")

    if not total:
        log("No contacts need summarization.")
        conn.close()
        return

    generated = 0
    now = datetime.utcnow().isoformat()

    for c in contacts:
        interactions = get_recent_interactions(conn, c["id"])
        context = build_contact_context(c, interactions)
        summary = generate_summary(client, context)

        if summary:
            new_notes = inject_summary_into_notes(c["notes"], summary)
            conn.execute(
                "UPDATE contacts SET notes=?, ai_summary_at=?, updated_at=? WHERE id=?",
                (new_notes, now, now, c["id"])
            )
            conn.commit()
            log(f"  Summarized: {c['name']} ({c.get('relationship_heat', '?') or '?'})")
            generated += 1
        else:
            log(f"  Skipped (no summary): {c['name']}")

        time.sleep(RATE_LIMIT_SECONDS)

    conn.close()
    log(f"=== Summary generation complete: {generated}/{total} contacts ===")


if __name__ == "__main__":
    main()

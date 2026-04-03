#!/usr/bin/env python3
"""
categorize.py — Classify CRM interactions by type using LLM.

Adds interaction_type column to interactions table (idempotent ALTER TABLE).

Categories:
  deal_discussion  -- investment, M&A, deal terms, valuations
  deal_flow        -- inbound pitch, intro to a deal
  follow_up        -- scheduled follow-up, check-in, catch-up
  introduction     -- first meeting, intro, referral
  vendor           -- service, contract, billing, operations
  personal         -- social, family, non-business
  meeting          -- general business meeting, call
  other            -- doesn't fit above categories

Classification uses subject + summary snippet. Falls back to keyword heuristics
if no OpenAI key or API fails.

Usage:
  python3 -m enrichment.categorize [--limit 200] [--force]
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
from config import DB_PATH, LOG_DIR, ENRICHMENT_MODEL, get_secret

import openai

LOG_PATH = os.path.join(LOG_DIR, "enrichment.log")

VALID_TYPES = {
    "deal_discussion", "deal_flow", "follow_up", "introduction",
    "vendor", "personal", "meeting", "other",
}


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [categorize] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_column(conn):
    """Add interaction_type column to interactions if not present."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(interactions)").fetchall()]
    if "interaction_type" not in cols:
        conn.execute("ALTER TABLE interactions ADD COLUMN interaction_type TEXT")
        conn.commit()
        log("Added interaction_type column to interactions table")


# -- Keyword heuristic fallback ------------------------------------------------

KEYWORD_RULES = [
    ("deal_discussion", re.compile(
        r"\b(term sheet|valuation|equity|due diligence|cap table|investment|investor|fundrais|round|series [abcde]|exit|acquisition|m&a|loi|letter of intent|deal|closing|diligence)\b",
        re.I
    )),
    ("deal_flow", re.compile(
        r"\b(pitch|deck|startup|intro.*to|introducing|referred|warm intro|fund.*raise|raise.*fund|seed|pre-seed|angel)\b",
        re.I
    )),
    ("introduction", re.compile(
        r"\b(introduction|meet.*for the first|nice to meet|first time|intro|referred by|connecting you with)\b",
        re.I
    )),
    ("follow_up", re.compile(
        r"\b(follow.?up|following up|checking in|check.?in|touching base|circle back|reconnect|per our|as discussed|as promised)\b",
        re.I
    )),
    ("vendor", re.compile(
        r"\b(invoice|payment|contract|renewal|subscription|vendor|service agreement|statement of work|sow|billing|retainer)\b",
        re.I
    )),
    ("personal", re.compile(
        r"\b(birthday|wedding|graduation|holiday|vacation|family|dinner|lunch|coffee|catch.?up|personal|congrats|congrats|happy new year)\b",
        re.I
    )),
]


def classify_keyword(subject, summary):
    text = f"{subject or ''} {summary or ''}".strip()
    for itype, pattern in KEYWORD_RULES:
        if pattern.search(text):
            return itype
    return "meeting"  # default for business interactions


# -- LLM batch classification -------------------------------------------------

def classify_batch_gpt(client, interactions):
    """
    Classify a batch of interactions using the configured LLM.
    interactions: list of {id, subject, summary, channel}
    Returns dict: {id: interaction_type}
    """
    if not interactions:
        return {}

    # Build compact input
    items = []
    for i, ix in enumerate(interactions):
        text = f"{ix['subject'] or ''} | {(ix['summary'] or '')[:200]}".strip(" |")
        items.append(f"{i}: [{ix['channel']}] {text}")

    prompt = (
        "Classify each of these CRM interactions into exactly one category.\n\n"
        "Categories:\n"
        "  deal_discussion  -- investment terms, M&A, valuations, cap table\n"
        "  deal_flow        -- inbound pitch, intro to a deal, startup outreach\n"
        "  follow_up        -- checking in, following up, per our last conversation\n"
        "  introduction     -- first meeting, being introduced, referral\n"
        "  vendor           -- invoice, contract, billing, service agreement\n"
        "  personal         -- social, family, birthday, non-business chat\n"
        "  meeting          -- general business call or meeting\n"
        "  other            -- doesn't fit above\n\n"
        "Interactions:\n"
        + "\n".join(items)
        + "\n\n"
        'Return a JSON object mapping index (as string) to category, e.g. {"0":"meeting","1":"follow_up"}'
    )

    try:
        resp = client.chat.completions.create(
            model=ENRICHMENT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0,
            response_format={"type": "json_object"},
        )
        result = json.loads(resp.choices[0].message.content)
        # Map index back to interaction id
        return {
            interactions[int(k)]["id"]: v
            for k, v in result.items()
            if k.isdigit() and int(k) < len(interactions) and v in VALID_TYPES
        }
    except Exception as e:
        log(f"LLM batch classification failed: {e}")
        return {}


def main():
    parser = argparse.ArgumentParser(description="Classify CRM interactions by type")
    parser.add_argument("--limit", type=int, default=500, help="Max interactions to classify")
    parser.add_argument("--force", action="store_true", help="Re-classify already-classified interactions")
    args = parser.parse_args()

    log(f"=== Interaction Categorization Starting (limit={args.limit}, force={args.force}) ===")

    conn = get_conn()
    ensure_column(conn)

    # Fetch interactions to classify
    if args.force:
        rows = conn.execute("""
            SELECT id, subject, summary, channel FROM interactions
            ORDER BY date DESC LIMIT ?
        """, (args.limit,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT id, subject, summary, channel FROM interactions
            WHERE interaction_type IS NULL
            ORDER BY date DESC LIMIT ?
        """, (args.limit,)).fetchall()

    total = len(rows)
    log(f"Interactions to classify: {total}")

    if not total:
        log("Nothing to classify.")
        conn.close()
        return

    api_key = get_secret("OPENAI_API_KEY")
    use_gpt = bool(api_key)
    if use_gpt:
        client = openai.OpenAI(api_key=api_key)
        log("Using LLM for classification")
    else:
        log("No OPENAI_API_KEY -- using keyword heuristics only")

    classified = 0
    batch_size = 20

    for batch_start in range(0, total, batch_size):
        batch = [dict(r) for r in rows[batch_start:batch_start + batch_size]]

        if use_gpt:
            gpt_results = classify_batch_gpt(client, batch)
        else:
            gpt_results = {}

        updates = []
        for ix in batch:
            itype = gpt_results.get(ix["id"])
            if not itype:
                # Fall back to keyword heuristic
                itype = classify_keyword(ix["subject"], ix["summary"])
            updates.append((itype, ix["id"]))

        conn.executemany(
            "UPDATE interactions SET interaction_type=? WHERE id=?", updates
        )
        conn.commit()
        classified += len(batch)

        if use_gpt and batch_start + batch_size < total:
            time.sleep(0.5)

    # Distribution summary
    dist = conn.execute("""
        SELECT interaction_type, COUNT(*) as cnt FROM interactions
        WHERE interaction_type IS NOT NULL
        GROUP BY interaction_type ORDER BY cnt DESC
    """).fetchall()

    conn.close()

    log(f"Classification complete: {classified} interactions")
    for row in dist:
        log(f"  {row['interaction_type']}: {row['cnt']}")
    log("=== Categorization Done ===")


if __name__ == "__main__":
    main()

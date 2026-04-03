#!/usr/bin/env python3
"""
relationship_score.py — Compute relationship health scores for all CRM contacts.

Scoring factors (0-100):
  - Recency of last_contact_date:  up to +/-30 pts
  - Interaction frequency:         up to +15 pts
  - relationship_type:             -5 to +10 pts
  - enrichment completeness:       0 to +5 pts
  - stale_flag penalty:            -20 pts

Heat mapping:
  70+ -> hot | 50-69 -> warm | 30-49 -> cool | 10-29 -> cold | 0-9 -> ghost

Adds columns relationship_score (INTEGER) and relationship_heat (TEXT) to contacts
via idempotent ALTER TABLE. Safe to re-run: recomputes all scores each time.

Usage:
  python3 -m enrichment.relationship_score
"""

import os
import sqlite3
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, LOG_DIR

LOG_PATH = os.path.join(LOG_DIR, "enrichment.log")


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [score] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_columns(conn):
    """Add relationship_score and relationship_heat columns if not present."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()]
    added = []
    if "relationship_score" not in cols:
        conn.execute("ALTER TABLE contacts ADD COLUMN relationship_score INTEGER")
        added.append("relationship_score")
    if "relationship_heat" not in cols:
        conn.execute("ALTER TABLE contacts ADD COLUMN relationship_heat TEXT")
        added.append("relationship_heat")
    if added:
        conn.commit()
        log(f"Added columns: {', '.join(added)}")


def compute_score(contact, interaction_count):
    """Compute 0-100 relationship health score for a contact."""
    score = 50  # baseline

    # -- Recency ---------------------------------------------------------------
    last_contact = contact["last_contact_date"]
    if last_contact:
        try:
            last_dt = date.fromisoformat(last_contact[:10])
            days_ago = (date.today() - last_dt).days
            if days_ago <= 14:
                score += 30
            elif days_ago <= 30:
                score += 25
            elif days_ago <= 90:
                score += 15
            elif days_ago <= 180:
                score += 5
            elif days_ago <= 365:
                score -= 5
            else:
                score -= 15
        except Exception:
            score -= 5
    else:
        # No last_contact_date -- never touched
        score -= 15

    # -- Interaction frequency -------------------------------------------------
    if interaction_count >= 15:
        score += 15
    elif interaction_count >= 10:
        score += 12
    elif interaction_count >= 5:
        score += 8
    elif interaction_count >= 3:
        score += 5
    elif interaction_count >= 1:
        score += 2

    # -- Relationship type -----------------------------------------------------
    rel = contact["relationship_type"] or "warm"
    rel_bonus = {
        "personal": 10,
        "investor": 7,
        "warm": 5,
        "political": 3,
        "vendor": 0,
        "cold-inbound": -5,
    }
    score += rel_bonus.get(rel, 0)

    # -- Enrichment completeness -----------------------------------------------
    has_company = bool(contact["company"] and str(contact["company"]).strip())
    has_role = bool(contact["role"] and str(contact["role"]).strip())
    if has_company and has_role:
        score += 5
    elif has_company or has_role:
        score += 2

    # -- Stale flag penalty ----------------------------------------------------
    if contact["stale_flag"]:
        score -= 20

    return max(0, min(100, score))


def score_to_heat(score):
    if score >= 70:
        return "hot"
    elif score >= 50:
        return "warm"
    elif score >= 30:
        return "cool"
    elif score >= 10:
        return "cold"
    else:
        return "ghost"


def main():
    log("=== Relationship Score Computation Starting ===")
    conn = get_conn()
    ensure_columns(conn)

    contacts = conn.execute("""
        SELECT id, name, last_contact_date, relationship_type,
               company, role, stale_flag
        FROM contacts
    """).fetchall()

    total = len(contacts)
    log(f"Scoring {total} contacts...")

    heat_counts = {"hot": 0, "warm": 0, "cool": 0, "cold": 0, "ghost": 0}
    updates = []

    for c in contacts:
        interaction_count = conn.execute(
            "SELECT COUNT(*) FROM interactions WHERE contact_id=?", (c["id"],)
        ).fetchone()[0]

        score = compute_score(c, interaction_count)
        heat = score_to_heat(score)
        heat_counts[heat] += 1
        updates.append((score, heat, c["id"]))

    # Batch update
    conn.executemany(
        "UPDATE contacts SET relationship_score=?, relationship_heat=? WHERE id=?",
        updates
    )
    conn.commit()
    conn.close()

    pct = {k: f"{v}/{total} ({100*v//total}%)" for k, v in heat_counts.items()}
    log(f"Scoring complete: {total} contacts")
    log(f"  hot:   {pct['hot']}")
    log(f"  warm:  {pct['warm']}")
    log(f"  cool:  {pct['cool']}")
    log(f"  cold:  {pct['cold']}")
    log(f"  ghost: {pct['ghost']}")
    log("=== Relationship Score Computation Done ===")
    return heat_counts


if __name__ == "__main__":
    main()

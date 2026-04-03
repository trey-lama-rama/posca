#!/usr/bin/env python3
"""
auto_complete_actions.py — Auto-complete action items when an interaction is found.

Logic:
  - Scans open action_items where description LIKE '%follow up%' OR '%email%' OR '%send%'
  - For each, checks if an interaction exists for that contact AFTER the action_item's created_at
  - If yes -> sets status='done', completed_at=interaction.date, notes the auto-complete
  - Logs results

Idempotent: skips items already done or previously auto-completed.
"""

import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, LOG_DIR

LOG_PATH = os.path.join(LOG_DIR, "crm-autocomplete.log")


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def run():
    conn = get_conn()

    # Find open action items that are follow-up / email / send related
    # Exclude items that already have "Auto-completed" in their description (re-run safety)
    candidates = conn.execute("""
        SELECT ai.id, ai.contact_id, ai.description, ai.created_at, c.name as contact_name
        FROM action_items ai
        JOIN contacts c ON c.id = ai.contact_id
        WHERE ai.status IN ('open', 'waiting_them', 'pending_approval')
          AND (
            ai.description LIKE '%follow up%'
            OR ai.description LIKE '%follow-up%'
            OR ai.description LIKE '%email%'
            OR ai.description LIKE '%send%'
          )
          AND ai.description NOT LIKE '%Auto-completed%'
        ORDER BY ai.created_at ASC
    """).fetchall()

    log(f"Scanning {len(candidates)} open follow-up/email/send action items...")

    completed_count = 0
    for item in candidates:
        # Look for any interaction after this action item was created
        interaction = conn.execute("""
            SELECT id, date, channel, subject FROM interactions
            WHERE contact_id = ?
              AND date > ?
            ORDER BY date ASC
            LIMIT 1
        """, (item["contact_id"], item["created_at"])).fetchone()

        if interaction:
            note_suffix = f" [Auto-completed: interaction found on {interaction['date'][:10]}]"
            new_desc = item["description"] + note_suffix
            conn.execute("""
                UPDATE action_items
                SET status = 'done',
                    completed_at = ?,
                    description = ?
                WHERE id = ?
            """, (interaction["date"], new_desc, item["id"]))
            log(
                f"  Auto-completed: [{item['contact_name']}] \"{item['description'][:60]}\" "
                f"(interaction {interaction['date'][:10]} via {interaction['channel']})"
            )
            completed_count += 1

    conn.commit()
    conn.close()

    log(f"Auto-complete done. {completed_count} of {len(candidates)} items completed.")
    return completed_count


if __name__ == "__main__":
    count = run()
    sys.exit(0)

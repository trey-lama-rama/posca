#!/usr/bin/env python3
"""
crm_report.py — Daily CRM health report.

Outputs a Telegram-formatted summary covering:
  - Relationship health distribution
  - Overdue / due-today follow-ups
  - Top hot contacts with stale risk (warm contacts not touched in 60+ days)
  - Recent interactions (last 7 days)
  - Pending action items for approval
  - Enrichment progress

Usage:
  python3 tools/crm_report.py
  python3 tools/crm_report.py --brief   # Single-paragraph summary only
"""

import argparse
import sqlite3
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def format_date(d):
    if not d:
        return "unknown"
    try:
        return datetime.fromisoformat(d[:10]).strftime("%b %d")
    except Exception:
        return d[:10]


def _col_exists(conn, table, col):
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return col in cols


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brief", action="store_true", help="Single-paragraph summary")
    args = parser.parse_args()

    conn = get_conn()
    today = date.today().isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    sixty_days_ago = (date.today() - timedelta(days=60)).isoformat()

    has_heat = _col_exists(conn, "contacts", "relationship_heat")
    has_score = _col_exists(conn, "contacts", "relationship_score")
    has_snoozed = _col_exists(conn, "action_items", "snoozed_until")

    lines = []

    # ── Header ────────────────────────────────────────────────────────────────
    lines.append(f"*CRM Health Report — {date.today().strftime('%B %d, %Y')}*\n")

    # ── Relationship health summary ───────────────────────────────────────────
    total = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    heat_counts = {}
    if has_heat:
        heat_counts = dict(conn.execute(
            "SELECT relationship_heat, COUNT(*) FROM contacts WHERE relationship_heat IS NOT NULL GROUP BY relationship_heat"
        ).fetchall())
        hot = heat_counts.get("hot", 0)
        warm = heat_counts.get("warm", 0)
        cool = heat_counts.get("cool", 0)
        cold = heat_counts.get("cold", 0)
        ghost = heat_counts.get("ghost", 0)
        lines.append(
            f"*Relationship Health* ({total} contacts)\n"
            f"  Hot: {hot}  Warm: {warm}  Cool: {cool}  Cold: {cold}  Ghost: {ghost}"
        )
    else:
        stale = conn.execute("SELECT COUNT(*) FROM contacts WHERE stale_flag=1").fetchone()[0]
        lines.append(f"*Contacts:* {total} total | {stale} stale (>180 days)")

    # ── Overdue / due today ───────────────────────────────────────────────────
    snooze_clause = "AND (snoozed_until IS NULL OR snoozed_until <= ?)" if has_snoozed else ""
    snooze_params = [today] if has_snoozed else []

    overdue_q = f"""
        SELECT ai.*, c.name as contact_name FROM action_items ai
        JOIN contacts c ON c.id = ai.contact_id
        WHERE ai.status IN ('open','waiting_them')
          AND ai.due_date < ?
          {snooze_clause}
        ORDER BY ai.due_date ASC LIMIT 5
    """
    overdue = conn.execute(overdue_q, [today] + snooze_params).fetchall()

    due_today_q = f"""
        SELECT ai.*, c.name as contact_name FROM action_items ai
        JOIN contacts c ON c.id = ai.contact_id
        WHERE ai.status IN ('open','waiting_them')
          AND ai.due_date = ?
          {snooze_clause}
        ORDER BY ai.created_at ASC LIMIT 5
    """
    due_today = conn.execute(due_today_q, [today] + snooze_params).fetchall()

    pending_approval = conn.execute("""
        SELECT ai.*, c.name as contact_name FROM action_items ai
        JOIN contacts c ON c.id = ai.contact_id
        WHERE ai.status = 'pending_approval'
        ORDER BY ai.created_at DESC LIMIT 5
    """).fetchall()

    if overdue or due_today:
        lines.append("\n*Follow-ups:*")
        for r in overdue:
            lines.append(f"  OVERDUE: [{r['contact_name']}] {r['description'][:60]} (was {format_date(r['due_date'])})")
        for r in due_today:
            lines.append(f"  TODAY: [{r['contact_name']}] {r['description'][:60]}")

    if pending_approval:
        lines.append(f"\n*Pending approval ({len(pending_approval)} action items):*")
        for r in pending_approval[:3]:
            own = "-> me" if r["owner"] == "mine" else "-> them"
            lines.append(f"  [{r['contact_name']}] {r['description'][:70]} {own}")
        if len(pending_approval) > 3:
            lines.append(f"  ...and {len(pending_approval)-3} more")

    # ── Hot contacts at risk (warm -> not touched in 60+ days) ────────────────
    if has_heat:
        at_risk = conn.execute("""
            SELECT name, company, last_contact_date, relationship_score
            FROM contacts
            WHERE relationship_heat IN ('hot', 'warm')
              AND (last_contact_date IS NULL OR last_contact_date < ?)
            ORDER BY relationship_score DESC
            LIMIT 5
        """, (sixty_days_ago,)).fetchall()

        if at_risk:
            lines.append("\n*Hot/Warm contacts going stale (60+ days):*")
            for c in at_risk:
                co = f" | {c['company']}" if c["company"] else ""
                lines.append(f"  {c['name']}{co} — last {format_date(c['last_contact_date'])}")

    # ── Recent interactions (last 7 days) ─────────────────────────────────────
    recent = conn.execute("""
        SELECT i.date, i.channel, i.subject, c.name as contact_name
        FROM interactions i
        JOIN contacts c ON c.id = i.contact_id
        WHERE i.date >= ?
        ORDER BY i.date DESC LIMIT 8
    """, (week_ago,)).fetchall()

    if recent:
        lines.append(f"\n*Recent interactions (last 7 days, {len(recent)}):*")
        for r in recent:
            ch = {"email": "email", "calendar": "cal", "zoom": "zoom", "telegram": "tg"}.get(r["channel"], r["channel"])
            subj = (r["subject"] or "")[:50]
            lines.append(f"  {ch} {format_date(r['date'])} [{r['contact_name']}] {subj}")
    else:
        lines.append("\n*No interactions logged in the last 7 days.*")

    # ── Enrichment progress ───────────────────────────────────────────────────
    enriched = conn.execute("SELECT COUNT(*) FROM contacts WHERE enriched_at IS NOT NULL").fetchone()[0]
    needs = conn.execute("SELECT COUNT(*) FROM contacts WHERE (company IS NULL OR company='') AND enriched_at IS NULL").fetchone()[0]
    lines.append(f"\n*Enrichment:* {enriched} contacts enriched | {needs} remaining")

    conn.close()

    report = "\n".join(lines)

    if args.brief:
        hot_n = heat_counts.get("hot", "?") if has_heat else "?"
        warm_n = heat_counts.get("warm", "?") if has_heat else "?"
        overdue_n = len(overdue)
        brief = (
            f"CRM: {total} contacts | {hot_n} hot {warm_n} warm | "
            f"{overdue_n} overdue follow-ups | {enriched} enriched"
        )
        print(brief)
    else:
        print(report)

    return report


if __name__ == "__main__":
    main()

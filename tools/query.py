#!/usr/bin/env python3
"""
query.py — Natural language query interface for the CRM.
Usage: python3 tools/query.py "what did I last talk to Josh about?"
Returns clean human-readable output formatted for Telegram.
"""

import json
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
import uuid

# ── Config ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, ROOT

MAX_RESULTS = 10

# Semantic search — lazy import so query.py works even without openai installed
def _try_semantic_search(query, top_k=10):
    """Try semantic search via embed.py; returns list of (contact_id, score) or []."""
    try:
        tools_dir = str(Path(__file__).resolve().parent)
        if tools_dir not in sys.path:
            sys.path.insert(0, tools_dir)
        from embed import search_semantic
        return search_semantic(query, top_k=top_k)
    except Exception as e:
        return []

# Patterns that indicate a conceptual/semantic query rather than a name/company lookup
_SEMANTIC_TRIGGERS = re.compile(
    r'^(?:who |find me |show me |list |give me |do i know anyone|anyone )'
    r'(?:works?|is |are |in |from |with |who |that |knows?|invest|fund|run|manag|own|lead|found|built|build)',
    re.I
)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def format_date(d):
    if not d:
        return "unknown"
    try:
        dt = datetime.fromisoformat(d[:10])
        return dt.strftime("%b %d, %Y")
    except Exception:
        return d[:10]


def format_contact_line(row, last_interaction=None, show_score=False):
    """Format: Name | Company | Last contact: date via channel | Summary"""
    name = row["name"] or "Unknown"
    company = row["company"] or ""
    last_date = format_date(row["last_contact_date"])
    channel = row["last_contact_channel"] or ""
    rel = row["relationship_type"] or ""

    # Heat indicator (if column present)
    heat_emoji = {"hot": "🔥", "warm": "✅", "cool": "🌤", "cold": "❄️", "ghost": "👻"}
    heat = None
    try:
        heat = row["relationship_heat"]
    except (IndexError, KeyError):
        pass

    line = f"*{name}*"
    if heat and show_score:
        score = None
        try:
            score = row["relationship_score"]
        except (IndexError, KeyError):
            pass
        line += f" {heat_emoji.get(heat, '')} ({score})" if score is not None else f" {heat_emoji.get(heat, '')}"
    if company:
        line += f" | {company}"
    if rel:
        line += f" | [{rel}]"
    line += f"\n  Last contact: {last_date}"
    if channel:
        line += f" via {channel}"

    if last_interaction:
        subj = last_interaction["subject"] or ""
        summary = last_interaction["summary"] or ""
        if subj:
            line += f"\n  Last: {subj}"
        if summary:
            line += f"\n  Note: {summary[:120]}"

    return line


# ── Intent parsing ────────────────────────────────────────────────────────────

def parse_intent(query):
    """
    Parse NL query into (intent, args).
    Intents:
      - last_interaction: name -> what did I talk to X about
      - find_by_name: name -> who is X
      - find_by_company: company -> who do I know at X
      - list_by_type: rel_type -> show me all warm contacts, investors, etc.
      - action_items: open action items [for contact]
      - stale: who haven't I talked to
      - recent: recent contacts
      - search: fallback FTS search
    """
    q = query.strip().lower().rstrip("?!. ")

    # "what did I (last) talk/discuss/say to/with [name] about"
    m = re.search(r'(?:what did i (?:last )?(?:talk|discuss|say|chat|email|send)|last (?:talk|discussion|email|interaction) (?:with|to)) (.+?)(?:\s+about)?$', q)
    if m:
        name = m.group(1).strip().rstrip("?").strip()
        # Strip leading prepositions: "to josh" -> "josh"
        name = re.sub(r'^(?:to|with|from)\s+', '', name).strip()
        return ("last_interaction", name)

    # "when did I last (talk/hear from/email) [name]"
    m = re.search(r'when did i last (?:talk|email|hear from|contact|speak|meet with|meet) (.+)', q)
    if m:
        return ("last_interaction", m.group(1).strip().rstrip("?").strip())

    # "last [interaction/contact/email] with [name]"
    m = re.search(r'last (?:interaction|contact|email|message|conversation|meeting) (?:with|from) (.+)', q)
    if m:
        return ("last_interaction", m.group(1).strip().rstrip("?").strip())

    # "who do I know at [company]" / "contacts at [company]"
    m = re.search(r'(?:who (?:do i know|have i met) at|contacts? at|people at|anyone at) (.+)', q)
    if m:
        return ("find_by_company", m.group(1).strip().rstrip("?!. ").strip())

    # "show me all [type] contacts" / "list [investors|warm|cold|vendor|personal]"
    type_map = {
        "investor": "investor", "investors": "investor",
        "warm": "warm",
        "cold": "cold-inbound", "cold-inbound": "cold-inbound", "cold inbound": "cold-inbound",
        "vendor": "vendor", "vendors": "vendor",
        "political": "political",
        "personal": "personal",
    }
    for keyword, rel_type in type_map.items():
        if keyword in q:
            return ("list_by_type", rel_type)

    # "remind me to follow up with [name] in [N] days/weeks"
    m = re.search(
        r'remind(?:er)?\s+(?:me\s+)?(?:to\s+)?(?:follow\s*up|call|email|check\s*in|reach\s*out|contact)\s+(?:with\s+|on\s+)?(.+?)\s+in\s+(\d+)\s*(day|week|month)s?',
        q
    )
    if m:
        return ("create_reminder", {"name": m.group(1).strip(), "amount": int(m.group(2)), "unit": m.group(3)})

    # "show my reminders" / "what's due" / "what is due"
    if any(kw in q for kw in ["show my reminder", "what's due", "whats due", "what is due", "my reminders", "upcoming reminder", "what do i have due"]):
        return ("show_reminders", None)

    # "snooze [name] reminder [N] days"
    m = re.search(r'snooze\s+(.+?)\s+(?:reminder\s+)?(?:for\s+)?(\d+)\s*(day|week)s?', q)
    if m:
        return ("snooze_reminder", {"name": m.group(1).strip(), "amount": int(m.group(2)), "unit": m.group(3)})

    # "done with [name]" / "mark [name] done"
    m = re.search(r'(?:done with|mark done for|complete(?:d)? (?:reminder )?(?:with|for))\s+(.+)', q)
    if m:
        return ("mark_done", m.group(1).strip().rstrip("?").strip())
    m = re.search(r'mark\s+(.+?)\s+(?:done|complete)', q)
    if m:
        return ("mark_done", m.group(1).strip().rstrip("?").strip())

    # "show me hot contacts" / "who are my warm contacts" / "relationship health"
    heat_map = {
        "hot": "hot",
        "warm": "warm",
        "cool": "cool", "cold": "cold",
        "ghost": "ghost", "dormant": "ghost",
    }
    for keyword, heat in heat_map.items():
        if keyword in q and any(kw in q for kw in ["contact", "who", "show", "list", "give", "heat", "health", keyword]):
            return ("list_by_heat", heat)

    # "relationship health" / "show scores" / "relationship scores"
    if any(kw in q for kw in ["relationship health", "relationship score", "show score", "contact score", "heat map", "heatmap"]):
        return ("relationship_health", None)

    # "who should I reconnect with" / "who to reach out to" / "reconnect"
    if any(kw in q for kw in ["reconnect", "reach out", "who should i call", "who to contact", "who should i email", "who should i follow", "outreach"]):
        return ("reconnect", None)

    # "top contacts" / "best contacts" / "most important contacts"
    if any(kw in q for kw in ["top contact", "best contact", "most important", "highest score", "vip"]):
        return ("top_contacts", None)

    # "find duplicates" / "show dupes"
    if any(kw in q for kw in ["find dup", "show dup", "duplicate", "dupes", "find dupes", "show dupes"]):
        return ("find_dupes", None)

    # "action items" / "follow ups" / "open items"
    if any(kw in q for kw in ["action item", "follow up", "follow-up", "todo", "to do", "open item", "waiting"]):
        # Check if a name is mentioned
        m = re.search(r'(?:action items?|follow.?ups?|todos?) (?:for|with|re) (.+)', q)
        if m:
            return ("action_items_for", m.group(1).strip().rstrip("?").strip())
        return ("action_items", None)

    # "show me deal discussions" / "contacts from introductions"
    itype_map = {
        "deal discussion": "deal_discussion",
        "deal flow": "deal_flow",
        "deals": "deal_discussion",
        "introduction": "introduction",
        "intro": "introduction",
        "follow up": "follow_up",
        "follow-up": "follow_up",
        "vendor interaction": "vendor",
        "personal interaction": "personal",
    }
    for keyword, itype in itype_map.items():
        if keyword in q and any(kw in q for kw in ["show", "list", "find", "who", "contacts", "give me"]):
            return ("interactions_by_type", itype)

    # "crm stats" / "crm summary" / "crm report"
    if any(kw in q for kw in ["crm stat", "crm summar", "crm report", "crm health", "database stat"]):
        return ("crm_stats", None)

    # "stale" / "haven't talked to" / "not heard from"
    if any(kw in q for kw in ["stale", "haven't talked", "haven't i talked", "haven't heard", "lost touch", "not contacted", "overdue", "in a while", "a long time", "ages"]):
        return ("stale", None)

    # "recent" / "latest contacts" / "new contacts"
    if any(kw in q for kw in ["recent", "latest", "new contact", "recently", "added"]):
        return ("recent", None)

    # "who is [name]" / "find [name]" / "tell me about [name]"
    m = re.search(r'(?:who is|who\'s|find|tell me about|show me|info on|details on|profile) (.+)', q)
    if m:
        return ("find_by_name", m.group(1).strip().rstrip("?").strip())

    # Fallback: FTS search on everything
    return ("search", query.strip())


# ── Query handlers ────────────────────────────────────────────────────────────

def find_contacts_by_name(conn, name):
    """FTS search on contacts by name, fallback to LIKE."""
    try:
        rows = conn.execute("""
            SELECT c.* FROM contacts c
            JOIN contacts_fts fts ON c.rowid = fts.rowid
            WHERE contacts_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """, (name, MAX_RESULTS)).fetchall()
        if rows:
            return rows
    except Exception:
        pass

    # LIKE fallback
    like = f"%{name}%"
    rows = conn.execute("""
        SELECT * FROM contacts WHERE name LIKE ? ORDER BY last_contact_date DESC LIMIT ?
    """, (like, MAX_RESULTS)).fetchall()
    return rows


def get_last_interaction(conn, contact_id):
    return conn.execute("""
        SELECT * FROM interactions WHERE contact_id=?
        ORDER BY date DESC LIMIT 1
    """, (contact_id,)).fetchone()


def handle_last_interaction(conn, name):
    contacts = find_contacts_by_name(conn, name)
    if not contacts:
        return f"No contact found matching '{name}'."

    if len(contacts) == 1:
        # Single match: show full profile with interaction history
        return format_full_profile(conn, contacts[0])

    results = []
    for c in contacts[:3]:
        last = get_last_interaction(conn, c["id"])
        results.append(format_contact_line(c, last))

    header = f"Last interaction — {name.title()}:"
    return header + "\n\n" + "\n\n".join(results)


def format_full_profile(conn, c):
    """Detailed profile for a single contact, including notes/enrichment/summary."""
    name = c["name"] or "Unknown"
    company = c["company"] or ""
    role = c["role"] or ""
    rel = c["relationship_type"] or ""
    last_date = format_date(c["last_contact_date"])
    channel = c["last_contact_channel"] or ""
    notes = c["notes"] or ""

    heat_emoji = {"hot": "🔥", "warm": "✅", "cool": "🌤", "cold": "❄️", "ghost": "👻"}
    heat = None
    score = None
    try:
        heat = c["relationship_heat"]
        score = c["relationship_score"]
    except (IndexError, KeyError):
        pass

    lines = []
    # Header
    heat_str = f" {heat_emoji.get(heat, '')} {score}" if heat and score is not None else ""
    lines.append(f"*{name}*{heat_str}")
    if company and role:
        lines.append(f"  {role} at {company}")
    elif company:
        lines.append(f"  {company}")
    elif role:
        lines.append(f"  {role}")

    lines.append(f"  Relationship: {rel} | Last: {last_date}" + (f" via {channel}" if channel else ""))

    # Email(s)
    try:
        emails = json.loads(c["emails"] or "[]")
        if emails:
            lines.append(f"  Email: {', '.join(emails[:2])}")
    except Exception:
        pass

    # AI Summary block
    import re as _re
    summary_match = _re.search(r"## Summary\n(.*?)(?=\n##|\Z)", notes, _re.DOTALL)
    if summary_match:
        lines.append(f"\n  📝 {summary_match.group(1).strip()}")

    # Enrichment block
    enrich_match = _re.search(r"## Enrichment\n(.*?)(?=\n##|\Z)", notes, _re.DOTALL)
    if enrich_match:
        enrich_text = enrich_match.group(1).strip()
        # Show select fields
        for line in enrich_text.splitlines()[:5]:
            if line.strip():
                lines.append(f"  {line}")

    # Recent interactions
    recent = conn.execute("""
        SELECT date, channel, subject, interaction_type FROM interactions
        WHERE contact_id=? ORDER BY date DESC LIMIT 3
    """, (c["id"],)).fetchall()
    if recent:
        lines.append("\n  *Recent interactions:*")
        ch_icons = {"email": "✉️", "calendar": "📅", "zoom": "🎥", "telegram": "💬"}
        for ix in recent:
            icon = ch_icons.get(ix["channel"] or "", "•")
            subj = (ix["subject"] or "")[:60]
            itype = f" [{ix['interaction_type']}]" if ix.get("interaction_type") else ""
            lines.append(f"    {icon} {format_date(ix['date'])}{itype} {subj}")

    # Open action items
    actions = conn.execute("""
        SELECT description, due_date, status FROM action_items
        WHERE contact_id=? AND status != 'done' LIMIT 3
    """, (c["id"],)).fetchall()
    if actions:
        lines.append("\n  *Open items:*")
        for a in actions:
            due = f" (due {format_date(a['due_date'])})" if a["due_date"] else ""
            status_icon = "⏳" if a["status"] == "waiting_them" else "◻"
            lines.append(f"    {status_icon} {a['description'][:70]}{due}")

    return "\n".join(lines)


def handle_find_by_name(conn, name):
    contacts = find_contacts_by_name(conn, name)
    if not contacts:
        return f"No contact found matching '{name}'."

    if len(contacts) == 1:
        # Single match: show full profile
        return format_full_profile(conn, contacts[0])

    results = []
    for c in contacts[:MAX_RESULTS]:
        last = get_last_interaction(conn, c["id"])
        results.append(format_contact_line(c, last))

    header = f"Contacts matching '{name}':"
    return header + "\n\n" + "\n\n".join(results)


def handle_find_by_company(conn, company):
    rows = conn.execute("""
        SELECT * FROM contacts WHERE company LIKE ? ORDER BY last_contact_date DESC LIMIT ?
    """, (f"%{company}%", MAX_RESULTS)).fetchall()

    if not rows:
        # Try FTS
        try:
            rows = conn.execute("""
                SELECT c.* FROM contacts c
                JOIN contacts_fts fts ON c.rowid = fts.rowid
                WHERE contacts_fts MATCH ?
                ORDER BY rank LIMIT ?
            """, (company, MAX_RESULTS)).fetchall()
        except Exception:
            pass

    if not rows:
        # Try searching email domain (e.g. "eqb strategy" -> "eqbstrategy.com")
        domain_guess = company.lower().replace(" ", "").replace("-", "")
        rows = conn.execute("""
            SELECT * FROM contacts WHERE emails LIKE ? ORDER BY last_contact_date DESC LIMIT ?
        """, (f"%@{domain_guess}%", MAX_RESULTS)).fetchall()

    if not rows:
        return f"No contacts found at '{company}'."

    results = []
    for c in rows:
        last = get_last_interaction(conn, c["id"])
        results.append(format_contact_line(c, last))

    return f"Contacts at {company.title()} ({len(results)}):\n\n" + "\n\n".join(results)


def handle_list_by_type(conn, rel_type):
    rows = conn.execute("""
        SELECT * FROM contacts WHERE relationship_type=?
        ORDER BY last_contact_date DESC LIMIT ?
    """, (rel_type, MAX_RESULTS)).fetchall()

    if not rows:
        return f"No {rel_type} contacts found."

    total = conn.execute("SELECT COUNT(*) FROM contacts WHERE relationship_type=?", (rel_type,)).fetchone()[0]
    results = []
    for c in rows:
        last = get_last_interaction(conn, c["id"])
        results.append(format_contact_line(c, last))

    header = f"{rel_type.title()} contacts (showing {len(results)} of {total}):"
    return header + "\n\n" + "\n\n".join(results)


def handle_action_items(conn, contact_name=None):
    if contact_name:
        contacts = find_contacts_by_name(conn, contact_name)
        if not contacts:
            return f"No contact found matching '{contact_name}'."
        contact_id = contacts[0]["id"]
        rows = conn.execute("""
            SELECT ai.*, c.name as contact_name FROM action_items ai
            JOIN contacts c ON c.id = ai.contact_id
            WHERE ai.contact_id=? AND ai.status != 'done'
            ORDER BY ai.due_date ASC LIMIT ?
        """, (contact_id, MAX_RESULTS)).fetchall()
    else:
        rows = conn.execute("""
            SELECT ai.*, c.name as contact_name FROM action_items ai
            JOIN contacts c ON c.id = ai.contact_id
            WHERE ai.status != 'done'
            ORDER BY ai.due_date ASC LIMIT ?
        """, (MAX_RESULTS,)).fetchall()

    if not rows:
        return "No open action items." if not contact_name else f"No open action items for {contact_name}."

    lines = []
    for r in rows:
        due = f" (due {format_date(r['due_date'])})" if r['due_date'] else ""
        status = "⏳ waiting" if r['status'] == 'waiting_them' else "◻"
        lines.append(f"{status} [{r['contact_name']}] {r['description']}{due}")

    header = "Open action items:"
    if contact_name:
        header = f"Action items for {contact_name.title()}:"
    return header + "\n" + "\n".join(lines)


def handle_stale(conn):
    rows = conn.execute("""
        SELECT * FROM contacts WHERE stale_flag=1
        ORDER BY last_contact_date ASC LIMIT ?
    """, (MAX_RESULTS,)).fetchall()

    total = conn.execute("SELECT COUNT(*) FROM contacts WHERE stale_flag=1").fetchone()[0]

    if not rows:
        return "No stale contacts. All relationships active within 180 days."

    results = []
    for c in rows:
        results.append(format_contact_line(c))

    return f"Stale contacts (>180 days, {total} total — showing {len(results)}):\n\n" + "\n\n".join(results)


def handle_recent(conn):
    rows = conn.execute("""
        SELECT * FROM contacts ORDER BY created_at DESC LIMIT ?
    """, (MAX_RESULTS,)).fetchall()

    if not rows:
        return "No contacts in CRM yet."

    results = []
    for c in rows:
        last = get_last_interaction(conn, c["id"])
        results.append(format_contact_line(c, last))

    return f"Most recently added contacts:\n\n" + "\n\n".join(results)


def handle_search(conn, query):
    """FTS search across contacts and interactions, with semantic routing for conceptual queries."""
    results = []

    # Route conceptual/natural-language queries directly to semantic search
    if _SEMANTIC_TRIGGERS.match(query):
        hits = _try_semantic_search(query, top_k=MAX_RESULTS)
        if hits:
            for contact_id, score in hits:
                c = conn.execute("SELECT * FROM contacts WHERE id=?", (contact_id,)).fetchone()
                if c:
                    last = get_last_interaction(conn, c["id"])
                    results.append(format_contact_line(c, last))
        if results:
            return f"Results for '{query}':\n\n" + "\n\n".join(results[:MAX_RESULTS])
        return f"No results found for '{query}'."

    # Search contacts FTS
    try:
        rows = conn.execute("""
            SELECT c.* FROM contacts c
            JOIN contacts_fts fts ON c.rowid = fts.rowid
            WHERE contacts_fts MATCH ?
            ORDER BY rank LIMIT ?
        """, (query, MAX_RESULTS)).fetchall()
        for c in rows:
            last = get_last_interaction(conn, c["id"])
            results.append(format_contact_line(c, last))
    except Exception:
        pass

    # Search interactions FTS if no results yet
    if not results:
        try:
            rows = conn.execute("""
                SELECT DISTINCT c.* FROM contacts c
                JOIN interactions i ON i.contact_id = c.id
                JOIN interactions_fts fts ON i.rowid = fts.rowid
                WHERE interactions_fts MATCH ?
                ORDER BY i.date DESC LIMIT ?
            """, (query, MAX_RESULTS)).fetchall()
            for c in rows:
                last = get_last_interaction(conn, c["id"])
                results.append(format_contact_line(c, last))
        except Exception:
            pass

    if not results:
        # LIKE fallback
        like = f"%{query}%"
        rows = conn.execute("""
            SELECT * FROM contacts WHERE name LIKE ? OR company LIKE ? OR notes LIKE ?
            ORDER BY last_contact_date DESC LIMIT ?
        """, (like, like, like, MAX_RESULTS)).fetchall()
        for c in rows:
            last = get_last_interaction(conn, c["id"])
            results.append(format_contact_line(c, last))

    if not results:
        # Semantic search fallback for anything not caught above
        try:
            hits = _try_semantic_search(query, top_k=MAX_RESULTS)
            if hits:
                for contact_id, score in hits:
                    c = conn.execute("SELECT * FROM contacts WHERE id=?", (contact_id,)).fetchone()
                    if c:
                        last = get_last_interaction(conn, c["id"])
                        results.append(format_contact_line(c, last))
                if results:
                    return f"Results for '{query}':\n\n" + "\n\n".join(results[:MAX_RESULTS])
        except Exception:
            pass

    if not results:
        return f"No results found for '{query}'."

    return f"Search results for '{query}':\n\n" + "\n\n".join(results[:MAX_RESULTS])


# ── Reminder handlers ─────────────────────────────────────────────────────────

def _ensure_snoozed_until(conn):
    """Add snoozed_until column if not present — idempotent."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(action_items)").fetchall()]
    if "snoozed_until" not in cols:
        conn.execute("ALTER TABLE action_items ADD COLUMN snoozed_until TEXT")
        conn.commit()


def _calc_due_date(amount, unit):
    today = date.today()
    if unit == "day":
        return (today + timedelta(days=amount)).isoformat()
    elif unit == "week":
        return (today + timedelta(weeks=amount)).isoformat()
    elif unit == "month":
        return (today + timedelta(days=amount * 30)).isoformat()
    return today.isoformat()


def handle_create_reminder(conn, name, amount, unit):
    _ensure_snoozed_until(conn)
    contacts = find_contacts_by_name(conn, name)
    if not contacts:
        return f"No contact found matching '{name}'."
    c = contacts[0]
    due = _calc_due_date(amount, unit)
    unit_label = f"{amount} {unit}{'s' if amount != 1 else ''}"
    desc = f"Follow up with {c['name']}"
    item_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO action_items (id, contact_id, description, due_date, status, created_at) VALUES (?,?,?,?,'open',datetime('now'))",
        (item_id, c["id"], desc, due)
    )
    conn.commit()
    return f"Reminder set: follow up with *{c['name']}* in {unit_label} (due {format_date(due)})."


def handle_show_reminders(conn):
    _ensure_snoozed_until(conn)
    today = date.today().isoformat()
    rows = conn.execute("""
        SELECT ai.*, c.name as contact_name FROM action_items ai
        JOIN contacts c ON c.id = ai.contact_id
        WHERE ai.status IN ('open','waiting_them')
          AND (ai.snoozed_until IS NULL OR ai.snoozed_until <= ?)
        ORDER BY ai.due_date ASC NULLS LAST
        LIMIT 30
    """, (today,)).fetchall()

    if not rows:
        return "No open reminders."

    overdue, due_today, upcoming = [], [], []
    for r in rows:
        d = r["due_date"]
        if d and d < today:
            overdue.append(r)
        elif d == today:
            due_today.append(r)
        else:
            upcoming.append(r)

    lines = []
    if overdue:
        lines.append("*OVERDUE*")
        for r in overdue:
            lines.append(f"  ⚠ [{r['contact_name']}] {r['description']} — was due {format_date(r['due_date'])}")
    if due_today:
        lines.append("*TODAY*")
        for r in due_today:
            lines.append(f"  • [{r['contact_name']}] {r['description']}")
    if upcoming:
        lines.append("*UPCOMING*")
        for r in upcoming:
            due_str = f" (due {format_date(r['due_date'])})" if r["due_date"] else ""
            lines.append(f"  ◻ [{r['contact_name']}] {r['description']}{due_str}")

    total = len(overdue) + len(due_today) + len(upcoming)
    return f"Reminders ({total}):\n" + "\n".join(lines)


def handle_snooze_reminder(conn, name, amount, unit):
    _ensure_snoozed_until(conn)
    contacts = find_contacts_by_name(conn, name)
    if not contacts:
        return f"No contact found matching '{name}'."
    c = contacts[0]
    until = _calc_due_date(amount, unit)
    unit_label = f"{amount} {unit}{'s' if amount != 1 else ''}"
    rows = conn.execute("""
        SELECT id FROM action_items
        WHERE contact_id=? AND status IN ('open','waiting_them')
        ORDER BY due_date ASC LIMIT 1
    """, (c["id"],)).fetchall()
    if not rows:
        return f"No open reminder found for *{c['name']}*."
    conn.execute(
        "UPDATE action_items SET status='waiting_them', snoozed_until=? WHERE id=?",
        (until, rows[0]["id"])
    )
    conn.commit()
    return f"Snoozed *{c['name']}* reminder for {unit_label} — resurfaces {format_date(until)}."


def handle_mark_done(conn, name):
    contacts = find_contacts_by_name(conn, name)
    if not contacts:
        return f"No contact found matching '{name}'."
    c = contacts[0]
    rows = conn.execute("""
        SELECT id FROM action_items
        WHERE contact_id=? AND status IN ('open','waiting_them')
        ORDER BY due_date ASC LIMIT 1
    """, (c["id"],)).fetchall()
    if not rows:
        return f"No open reminder found for *{c['name']}*."
    conn.execute(
        "UPDATE action_items SET status='done', completed_at=datetime('now') WHERE id=?",
        (rows[0]["id"],)
    )
    conn.commit()
    return f"Marked done: *{c['name']}* follow-up complete."


def handle_list_by_heat(conn, heat):
    """Show contacts filtered by relationship_heat."""
    # Check if column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()]
    if "relationship_heat" not in cols:
        return "Relationship scores not yet computed. Run relationship_score.py first."

    heat_emoji = {"hot": "🔥", "warm": "✅", "cool": "🌤", "cold": "❄️", "ghost": "👻"}
    rows = conn.execute("""
        SELECT * FROM contacts WHERE relationship_heat=?
        ORDER BY relationship_score DESC, last_contact_date DESC LIMIT ?
    """, (heat, MAX_RESULTS)).fetchall()

    total = conn.execute(
        "SELECT COUNT(*) FROM contacts WHERE relationship_heat=?", (heat,)
    ).fetchone()[0]

    if not rows:
        return f"No {heat} contacts found."

    results = []
    for c in rows:
        last = get_last_interaction(conn, c["id"])
        results.append(format_contact_line(c, last, show_score=True))

    label = f"{heat_emoji.get(heat, '')} {heat.title()} contacts"
    return f"{label} ({total} total, showing {len(results)}):\n\n" + "\n\n".join(results)


def handle_relationship_health(conn):
    """Show relationship health distribution across all contacts."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()]
    if "relationship_heat" not in cols:
        return "Relationship scores not yet computed. Run relationship_score.py first."

    counts = conn.execute("""
        SELECT relationship_heat, COUNT(*) as cnt,
               ROUND(AVG(relationship_score), 1) as avg_score
        FROM contacts
        WHERE relationship_heat IS NOT NULL
        GROUP BY relationship_heat
        ORDER BY avg_score DESC
    """).fetchall()

    total = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    heat_emoji = {"hot": "🔥", "warm": "✅", "cool": "🌤", "cold": "❄️", "ghost": "👻"}

    lines = ["*Relationship Health Summary:*\n"]
    for row in counts:
        h = row["relationship_heat"] or "?"
        pct = int(100 * row["cnt"] / total) if total else 0
        bar = "█" * (pct // 5)
        lines.append(
            f"{heat_emoji.get(h, '')} *{h.title()}*: {row['cnt']} contacts ({pct}%) "
            f"avg score {row['avg_score']} {bar}"
        )

    # Top 3 hot contacts
    top_hot = conn.execute("""
        SELECT name, company, relationship_score FROM contacts
        WHERE relationship_heat='hot'
        ORDER BY relationship_score DESC LIMIT 3
    """).fetchall()
    if top_hot:
        lines.append("\n*Top hot contacts:*")
        for c in top_hot:
            co = f" | {c['company']}" if c["company"] else ""
            lines.append(f"  🔥 {c['name']}{co} (score {c['relationship_score']})")

    return "\n".join(lines)


def handle_reconnect(conn):
    """Show cool/cold contacts worth reconnecting with — sorted by score desc."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()]
    if "relationship_heat" not in cols:
        return "Relationship scores not yet computed. Run relationship_score.py first."

    # Warm+cool contacts that are getting stale — good reconnect candidates
    rows = conn.execute("""
        SELECT * FROM contacts
        WHERE relationship_heat IN ('cool', 'cold')
          AND relationship_type IN ('warm', 'investor', 'personal', 'political')
        ORDER BY relationship_score DESC
        LIMIT ?
    """, (MAX_RESULTS,)).fetchall()

    if not rows:
        # Fallback: stale contacts
        rows = conn.execute("""
            SELECT * FROM contacts WHERE stale_flag=1
            ORDER BY last_contact_date DESC LIMIT ?
        """, (MAX_RESULTS,)).fetchall()

    if not rows:
        return "No reconnect candidates found — all relationships look healthy."

    results = []
    for c in rows:
        last = get_last_interaction(conn, c["id"])
        results.append(format_contact_line(c, last, show_score=True))

    return f"Reconnect candidates ({len(results)}):\n\n" + "\n\n".join(results)


def handle_top_contacts(conn):
    """Show top contacts by relationship score."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()]
    if "relationship_score" not in cols:
        return "Relationship scores not yet computed. Run relationship_score.py first."

    rows = conn.execute("""
        SELECT * FROM contacts
        WHERE relationship_score IS NOT NULL
        ORDER BY relationship_score DESC
        LIMIT ?
    """, (MAX_RESULTS,)).fetchall()

    if not rows:
        return "No contacts with scores found."

    results = []
    for c in rows:
        last = get_last_interaction(conn, c["id"])
        results.append(format_contact_line(c, last, show_score=True))

    return f"Top contacts by relationship score:\n\n" + "\n\n".join(results)


def handle_interactions_by_type(conn, itype):
    """Show contacts who have interactions of a given type."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(interactions)").fetchall()]
    if "interaction_type" not in cols:
        return "Interactions not yet categorized. Run categorize_interactions.py first."

    rows = conn.execute("""
        SELECT DISTINCT c.*,
               MAX(i.date) as last_typed_date,
               i.subject as typed_subject
        FROM contacts c
        JOIN interactions i ON i.contact_id = c.id
        WHERE i.interaction_type = ?
        GROUP BY c.id
        ORDER BY last_typed_date DESC
        LIMIT ?
    """, (itype, MAX_RESULTS)).fetchall()

    if not rows:
        return f"No contacts with '{itype}' interactions found."

    results = []
    for c in rows:
        last = get_last_interaction(conn, c["id"])
        results.append(format_contact_line(c, last))

    label = itype.replace("_", " ").title()
    return f"Contacts with {label} interactions ({len(results)}):\n\n" + "\n\n".join(results)


def handle_crm_stats(conn):
    """Show CRM database statistics."""
    total = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    total_i = conn.execute("SELECT COUNT(*) FROM interactions").fetchone()[0]
    total_a = conn.execute("SELECT COUNT(*) FROM action_items").fetchone()[0]
    stale = conn.execute("SELECT COUNT(*) FROM contacts WHERE stale_flag=1").fetchone()[0]
    enriched = conn.execute("SELECT COUNT(*) FROM contacts WHERE enriched_at IS NOT NULL").fetchone()[0]

    lines = [f"*CRM Database Stats:*"]
    lines.append(f"  Contacts: {total:,} | Stale: {stale} | Enriched: {enriched}")
    lines.append(f"  Interactions: {total_i:,} | Action items: {total_a}")

    # By relationship type
    by_type = conn.execute("""
        SELECT relationship_type, COUNT(*) as cnt FROM contacts
        GROUP BY relationship_type ORDER BY cnt DESC
    """).fetchall()
    lines.append(f"\n*By type:*")
    for r in by_type:
        lines.append(f"  {r['relationship_type'] or 'unset'}: {r['cnt']}")

    # By channel
    by_channel = conn.execute("""
        SELECT channel, COUNT(*) as cnt FROM interactions
        GROUP BY channel ORDER BY cnt DESC
    """).fetchall()
    if by_channel:
        lines.append(f"\n*Interactions by channel:*")
        for r in by_channel:
            lines.append(f"  {r['channel']}: {r['cnt']}")

    # Heat distribution
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()]
    if "relationship_heat" in cols:
        heat_counts = conn.execute("""
            SELECT relationship_heat, COUNT(*) as cnt FROM contacts
            WHERE relationship_heat IS NOT NULL
            GROUP BY relationship_heat ORDER BY cnt DESC
        """).fetchall()
        if heat_counts:
            heat_emoji = {"hot": "🔥", "warm": "✅", "cool": "🌤", "cold": "❄️", "ghost": "👻"}
            lines.append(f"\n*Relationship heat:*")
            for r in heat_counts:
                h = r["relationship_heat"]
                lines.append(f"  {heat_emoji.get(h, '')} {h}: {r['cnt']}")

    return "\n".join(lines)


def handle_find_dupes(conn):
    """Run find_dupes and return first 10 results."""
    try:
        find_dupes_path = str(Path(__file__).resolve().parent / "find_dupes.py")
        result = subprocess.run(
            ["python3", find_dupes_path, "--limit", "10"],
            capture_output=True, text=True, timeout=30
        )
        output = result.stdout.strip()
        if not output:
            return "No duplicate candidates found."
        return output
    except subprocess.TimeoutExpired:
        return "Duplicate check timed out."
    except Exception as e:
        return f"Error running duplicate check: {e}"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 query.py \"your question here\"")
        sys.exit(1)

    query = " ".join(sys.argv[1:])
    conn = get_conn()

    intent, args = parse_intent(query)

    if intent == "last_interaction":
        result = handle_last_interaction(conn, args)
    elif intent == "find_by_name":
        result = handle_find_by_name(conn, args)
    elif intent == "find_by_company":
        result = handle_find_by_company(conn, args)
    elif intent == "list_by_type":
        result = handle_list_by_type(conn, args)
    elif intent == "action_items":
        result = handle_action_items(conn)
    elif intent == "action_items_for":
        result = handle_action_items(conn, args)
    elif intent == "stale":
        result = handle_stale(conn)
    elif intent == "recent":
        result = handle_recent(conn)
    elif intent == "create_reminder":
        result = handle_create_reminder(conn, args["name"], args["amount"], args["unit"])
    elif intent == "show_reminders":
        result = handle_show_reminders(conn)
    elif intent == "snooze_reminder":
        result = handle_snooze_reminder(conn, args["name"], args["amount"], args["unit"])
    elif intent == "mark_done":
        result = handle_mark_done(conn, args)
    elif intent == "find_dupes":
        result = handle_find_dupes(conn)
    elif intent == "list_by_heat":
        result = handle_list_by_heat(conn, args)
    elif intent == "relationship_health":
        result = handle_relationship_health(conn)
    elif intent == "reconnect":
        result = handle_reconnect(conn)
    elif intent == "top_contacts":
        result = handle_top_contacts(conn)
    elif intent == "interactions_by_type":
        result = handle_interactions_by_type(conn, args)
    elif intent == "crm_stats":
        result = handle_crm_stats(conn)
    else:
        result = handle_search(conn, args)

    conn.close()
    print(result)


if __name__ == "__main__":
    main()

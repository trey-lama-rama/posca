# Posca

*Named after the servant of Julius Caesar — always on hand to remember the names, birthdays, and debts of Roman society so Caesar didn't have to.*

A headless personal CRM that runs entirely from scripts and cron jobs. No browser, no GUI, no SaaS dashboard. Posca watches your email, calendar, and contacts, and quietly builds a complete picture of everyone you know — so when you need to remember, you just ask.

## Why headless?

Every CRM fails the same way: you stop opening it. The app sits there waiting for you to log in, type notes, update contacts, and categorize deals. You do it for a week, maybe two, then you stop. The data goes stale. Six months later you're back to searching your inbox.

Posca inverts the model. **You never open it because there's nothing to open.** It reads the systems you already use — email, calendar, contacts, meeting recordings — and builds your relationship database automatically. No data entry. No forms. No tabs to remember to check.

The database updates itself every night. Contacts appear when you email someone. Relationships get scored by how recently and frequently you interact. Birthdays and addresses get mined from your email history. AI fills in job titles and company names. Stale relationships get flagged.

When you need something, you query it:

```bash
sqlite3 data/crm.db "SELECT name, birthday FROM contacts WHERE birthday LIKE '%-04-%' ORDER BY birthday;"
```

Or pipe it into whatever tool you want — a dashboard, a script, an AI agent. The database is a single SQLite file. It doesn't care how you read it.

**The best CRM is the one that works when you forget about it.**

## What it does

- **Automatically builds your contact database** from Gmail, Google Calendar, iCloud Contacts, Zoom recordings, and ro.am transcripts
- **Enriches contacts** with professional data via web search (Tavily) and LinkedIn (Proxycurl)
- **Mines your email bodies** for personal details — birthdays, addresses, phone numbers, websites
- **Scores relationships** by recency and frequency, flags stale contacts
- **Generates AI summaries** of your relationship with each person
- **Categorizes every interaction** (deal flow, follow-up, introduction, etc.)
- **Syncs to Supabase** for optional cloud access and dashboards
- **Runs nightly on autopilot** via a single `sync.sh` pipeline

## What makes it different

- **Zero UI** — the database *is* the product. Query it however you want.
- **Runs anywhere** — headless server, laptop, Raspberry Pi
- **Pluggable sources** — each data source is a standalone seed script
- **AI enrichment** — incremental, rate-limited, cost-controlled (~$0.04/night)
- **Confidence tracking** — knows which source provided each fact, never lets lower-confidence data overwrite higher
- **Single-file database** — SQLite with full-text search, portable and backupable

## Quick start

```bash
git clone https://github.com/yourusername/posca.git
cd posca
bash setup.sh
```

Edit `config.yaml` with your email accounts and `/.env` with your API keys, then:

```bash
bash sync.sh
```

## Requirements

- Python 3.10+
- [GOG CLI](https://github.com/thedataflows/gog) — for Gmail and Google Calendar access
- SQLite 3
- An OpenAI API key (for AI enrichment, ~$0.04/night)

### Optional

- Supabase account (for cloud sync)
- Proxycurl API key (for LinkedIn enrichment)
- ro.am API key (for meeting transcript ingestion)
- Zoom Server-to-Server OAuth app (for Zoom recording ingestion)
- Voyage AI API key (for contact embeddings)

## Configuration

### config.yaml

Defines your email accounts, iCloud settings, and sync parameters:

```yaml
accounts:
  - address: you@example.com
    label: Primary
    default_rel_type: warm
  - address: you@work.com
    label: Work
    default_rel_type: cold-inbound

icloud:
  carddav_base: https://pXX-contacts.icloud.com/XXXXXXX/carddavhome/card/
  user: your-apple-id@icloud.com

sync:
  stale_threshold_days: 180
  enrich_limit: 30
  gmail_mine_limit: 20
```

### .env

Stores secrets (never committed):

```
GOG_KEYRING_PASSWORD=your-gog-password
OPENAI_API_KEY=sk-...
ICLOUD_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx
```

## Pipeline

`sync.sh` runs a 13-step pipeline:

| Step | Script | What it does |
|------|--------|-------------|
| 1 | `schema.sql` | Apply database schema (idempotent) |
| 1b | `migrations/` | Add new columns (idempotent) |
| 2 | `seeds/icloud.py` | Pull contacts from iCloud CardDAV |
| 2b | `seeds/gmail_sent.py` | Discover contacts from sent mail |
| 3 | `seeds/gmail.py` | Pull contacts from Gmail inbox |
| 4 | `seeds/calendar.py` | Extract attendees from calendar events |
| 5 | `seeds/roam.py` | Ingest ro.am meeting transcripts |
| 5b | `seeds/gmail_personal.py` | Mine email bodies for birthdays, addresses |
| 6 | SQL | Flag stale contacts (180-day threshold) |
| 7 | `tools/auto_complete_actions.py` | Auto-close action items with recent interaction |
| 8 | `enrichment/relationship_score.py` | Compute relationship health scores |
| 9 | `enrichment/categorize.py` | Categorize interactions by type |
| 10 | `enrichment/enrich.py` | Enrich via Tavily + GPT-4o-mini |
| 10b | `enrichment/proxycurl.py` | LinkedIn enrichment (optional) |
| 11 | `enrichment/contact_summary.py` | Generate AI relationship summaries |
| 12 | `enrichment/embed.py` | Generate contact embeddings (optional) |
| 13 | `tools/push_supabase.py` | Push to Supabase (optional) |

## Querying

The database is plain SQLite. Query it directly:

```bash
# Find hot contacts
sqlite3 data/crm.db "SELECT name, company, relationship_heat FROM contacts WHERE relationship_heat='hot' ORDER BY last_contact_date DESC LIMIT 20;"

# Contacts with upcoming birthdays
sqlite3 data/crm.db "SELECT name, birthday FROM contacts WHERE birthday IS NOT NULL ORDER BY substr(birthday, 6) LIMIT 20;"

# Full-text search
sqlite3 data/crm.db "SELECT name, company FROM contacts WHERE rowid IN (SELECT rowid FROM contacts_fts WHERE contacts_fts MATCH 'venture capital');"

# Stale relationships
sqlite3 data/crm.db "SELECT name, last_contact_date FROM contacts WHERE stale_flag=1 AND relationship_heat IN ('hot','warm') ORDER BY last_contact_date;"
```

Or use the interactive query tool:

```bash
python3 tools/query.py "venture capital contacts"
```

## Talk to it through your AI agent

The real interface isn't SQL — it's conversation. If you run an AI agent (Claude Code, OpenClaw, a custom GPT, or anything with tool access), point it at the database and just ask questions in natural language:

> **"When is Sarah's birthday?"**
> March 15th.

> **"Who haven't I talked to in 6 months that I should?"**
> 12 hot/warm contacts are stale. The most overdue are...

> **"Draft a happy birthday email to everyone with a birthday this week."**
> Found 3 contacts with birthdays this week: ...

> **"What's James Chen's company and how do I reach him?"**
> James Chen — Apex Partners, 555-234-5678, www.apexpartners.com

> **"Who did I meet with last Tuesday?"**
> You had calendar events with 4 people: ...

> **"Add a follow-up reminder for Rachel next Thursday."**
> Created action item: Follow up with Rachel Torres, due next Thursday.

Your agent just runs `sqlite3` queries against `data/crm.db` under the hood. No API, no SDK, no integration work. If the agent can run shell commands, it can use the CRM.

This is the point of headless: the database is the API, and any tool that can read SQLite is a client — including the AI assistant you're already talking to.

## Nightly automation

Add to your crontab:

```bash
crontab -e
# Add:
0 2 * * * cd /path/to/posca && bash sync.sh >> logs/cron.log 2>&1
```

## Architecture

```
config.yaml + .env
       │
       ▼
   config.py ──────── shared by all scripts
       │
       ▼
   sync.sh ────────── orchestrates the pipeline
       │
       ├── seeds/     ── pull contacts from data sources
       ├── enrichment/ ── add professional data via AI
       ├── tools/     ── query, sync, maintain
       └── data/crm.db ── single-file SQLite database
```

### Data sources and confidence

Each contact field tracks its provenance in `personal_data_source`:

```json
{"address": "icloud", "birthday": "gmail", "website": "gmail"}
```

**Confidence hierarchy:** iCloud > Gmail > Enrichment. Higher-confidence sources never get overwritten by lower ones.

## License

MIT

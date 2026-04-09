#!/bin/bash
# sync.sh — Master CRM sync pipeline. Idempotent.
# Usage: bash sync.sh
# Logs to logs/crm-sync.log

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load secrets from .env
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

LOG="logs/crm-sync.log"
DB="data/crm.db"
mkdir -p logs data

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"
}

log "========================================="
log "CRM Sync Starting"
log "========================================="

# ── Step 1: Create / migrate schema (idempotent) ─────────────────────────────
log "Step 1: Applying schema..."
sqlite3 "$DB" < schema.sql 2>&1 | tee -a "$LOG"
log "Schema OK."

# ── Step 1b: Migrate personal data columns (idempotent) ─────────────────────
log "Step 1b: Migrating personal data columns..."
python3 migrations/migrate_personal_fields.py 2>&1 | tee -a "$LOG"
log "Personal data columns OK."

# ── Step 2: iCloud Contacts seed ─────────────────────────────────────────────
log "Step 2: Seeding iCloud contacts..."
python3 seeds/icloud.py 2>&1 | tee -a "$LOG"
log "iCloud seed complete."

# ── Step 2b: Sent mail seed ──────────────────────────────────────────────────
log "Step 2b: Seeding contacts from sent mail..."
python3 seeds/gmail_sent.py --limit 100 2>&1 | tee -a "$LOG"
log "Sent mail seed complete."

# ── Step 3: Gmail seed ───────────────────────────────────────────────────────
log "Step 3: Seeding Gmail contacts..."
python3 seeds/gmail.py 2>&1 | tee -a "$LOG"
log "Gmail seed complete."

# ── Step 4: Calendar seed ────────────────────────────────────────────────────
log "Step 4: Seeding calendar attendees..."
python3 seeds/calendar.py 2>&1 | tee -a "$LOG"
log "Calendar seed complete."

# ── Step 5: ro.am transcript seed ────────────────────────────────────────────
if [ -n "${ROAM_API_KEY:-}" ]; then
    log "Step 5: Seeding ro.am meeting transcripts..."
    python3 seeds/roam.py 2>&1 | tee -a "$LOG"
    log "ro.am seed complete."
else
    log "Step 5: Skipping ro.am seed (ROAM_API_KEY not set)."
fi

# ── Step 5b: Mine Gmail bodies for personal data ────────────────────────────
log "Step 5b: Mining Gmail bodies for personal contact data..."
python3 seeds/gmail_personal.py --limit 20 --heat hot,warm 2>&1 | tee -a "$LOG"
log "Gmail personal data mining done."

# ── Step 6: Stale flag update ────────────────────────────────────────────────
STALE_DAYS="${STALE_THRESHOLD_DAYS:-180}"
log "Step 6: Updating stale flags (${STALE_DAYS}-day threshold)..."
sqlite3 "$DB" "
UPDATE contacts
SET stale_flag = CASE
    WHEN last_contact_date IS NULL THEN 0
    WHEN julianday('now') - julianday(last_contact_date) > ${STALE_DAYS} THEN 1
    ELSE 0
END,
updated_at = datetime('now');
" 2>&1 | tee -a "$LOG"
log "Stale flags updated."

# ── Step 7: Auto-complete action items ───────────────────────────────────────
log "Step 7: Auto-completing action items where interactions found..."
python3 tools/auto_complete_actions.py 2>&1 | tee -a "$LOG"
log "Auto-complete done."

# ── Step 8: Relationship health scoring ──────────────────────────────────────
log "Step 8: Computing relationship health scores..."
python3 enrichment/relationship_score.py 2>&1 | tee -a "$LOG"
log "Relationship scoring done."

# ── Step 9: Categorize interactions ──────────────────────────────────────────
log "Step 9: Categorizing interactions by type..."
python3 enrichment/categorize.py --limit 500 2>&1 | tee -a "$LOG"
log "Interaction categorization done."

# ── Step 9b: Apollo.io enrichment ────────────────────────────────────────────
if [ -n "${APOLLO_API_KEY:-}" ]; then
    log "Step 9b: Apollo.io enrichment (limit 500)..."
    python3 enrichment/apollo.py --limit 500 2>&1 | tee -a "$LOG"
    log "Apollo enrichment done."
else
    log "Step 9b: Skipping Apollo enrichment (APOLLO_API_KEY not set)."
fi

# ── Step 10: Enrich contacts ─────────────────────────────────────────────────
log "Step 10: Enriching contacts (limit 30)..."
python3 enrichment/enrich.py --limit 30 --min-data 2>&1 | tee -a "$LOG"
log "Enrichment done."

# ── Step 10b: LinkedIn enrichment via Proxycurl ──────────────────────────────
if [ -n "${PROXYCURL_API_KEY:-}" ]; then
    log "Step 10b: LinkedIn enrichment via Proxycurl (hot/warm, limit 25)..."
    python3 enrichment/proxycurl.py --limit 25 --heat hot,warm 2>&1 | tee -a "$LOG"
    log "Proxycurl enrichment done."
else
    log "Step 10b: Skipping Proxycurl (PROXYCURL_API_KEY not set)."
fi

# ── Step 11: AI summaries ────────────────────────────────────────────────────
log "Step 11: Generating AI summaries for hot/warm contacts..."
python3 enrichment/contact_summary.py --limit 20 --heat hot,warm 2>&1 | tee -a "$LOG"
log "AI summaries done."

# ── Step 12: Embed contacts ─────────────────────────────────────────────────
if [ -n "${VOYAGE_API_KEY:-}" ]; then
    log "Step 12: Embedding contacts..."
    python3 enrichment/embed.py 2>&1 | tee -a "$LOG"
    log "Embed complete."
else
    log "Step 12: Skipping embeddings (VOYAGE_API_KEY not set)."
fi

# ── Summary ──────────────────────────────────────────────────────────────────
log "========================================="
log "SYNC SUMMARY"
log "========================================="

TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM contacts;" 2>/dev/null || echo "?")
STALE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM contacts WHERE stale_flag=1;" 2>/dev/null || echo "?")
INTERACTIONS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM interactions;" 2>/dev/null || echo "?")
ACTIONS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM action_items WHERE status='open';" 2>/dev/null || echo "?")
ENRICHED=$(sqlite3 "$DB" "SELECT COUNT(*) FROM contacts WHERE enriched_at IS NOT NULL;" 2>/dev/null || echo "?")
APOLLO_ENRICHED=$(sqlite3 "$DB" "SELECT COUNT(*) FROM contacts WHERE apollo_enriched_at IS NOT NULL;" 2>/dev/null || echo "?")
HOT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM contacts WHERE relationship_heat='hot';" 2>/dev/null || echo "?")
WARM=$(sqlite3 "$DB" "SELECT COUNT(*) FROM contacts WHERE relationship_heat='warm';" 2>/dev/null || echo "?")
WITH_BIRTHDAY=$(sqlite3 "$DB" "SELECT COUNT(*) FROM contacts WHERE birthday IS NOT NULL AND birthday <> '';" 2>/dev/null || echo "?")
WITH_ADDRESS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM contacts WHERE address IS NOT NULL AND address <> '';" 2>/dev/null || echo "?")
GMAIL_MINED=$(sqlite3 "$DB" "SELECT COUNT(*) FROM contacts WHERE gmail_mined_at IS NOT NULL;" 2>/dev/null || echo "?")

log "Total contacts:      $TOTAL"
log "Stale (>$STALE_DAYS days): $STALE"
log "Enriched contacts:   $ENRICHED"
log "Apollo-enriched:     $APOLLO_ENRICHED"
log "Hot contacts:        $HOT"
log "Warm contacts:       $WARM"
log "With birthday:       $WITH_BIRTHDAY"
log "With address:        $WITH_ADDRESS"
log "Gmail-mined:         $GMAIL_MINED"
log "Interactions logged: $INTERACTIONS"
log "Open action items:   $ACTIONS"

# ── Step 13: Push to Supabase ────────────────────────────────────────────────
if [ -n "${SUPABASE_URL:-}" ] && [ -n "${SUPABASE_KEY:-}" ]; then
    log "Step 13: Pushing to Supabase..."
    python3 tools/push_supabase.py 2>&1 | tee -a "$LOG"
    log "Supabase push complete."
else
    log "Step 13: Skipping Supabase push (credentials not set)."
fi

log "========================================="
log "Sync complete."

echo ""
echo "===== CRM SYNC COMPLETE ====="
echo "Total contacts:      $TOTAL"
echo "Enriched:            $ENRICHED"
echo "Hot: $HOT | Warm: $WARM"
echo "With birthday: $WITH_BIRTHDAY | With address: $WITH_ADDRESS"
echo "Log: $LOG"

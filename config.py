"""
Centralized configuration for Posca.
Reads settings from config.yaml and secrets from .env.
All paths are relative to the repo root.
"""

import os
import sys
from pathlib import Path

# Repo root = directory containing this file
ROOT = Path(__file__).resolve().parent

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

# Load config.yaml
try:
    import yaml
    with open(ROOT / "config.yaml") as f:
        _cfg = yaml.safe_load(f) or {}
except FileNotFoundError:
    print("ERROR: config.yaml not found. Run: cp config.yaml.example config.yaml", file=sys.stderr)
    sys.exit(1)
except ImportError:
    print("ERROR: pyyaml not installed. Run: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


def get_secret(name, required=False):
    """Get a secret from environment variables."""
    val = os.environ.get(name, "")
    if required and not val:
        print(f"ERROR: {name} not set. Add it to .env or export it.", file=sys.stderr)
        sys.exit(1)
    return val


# ── Validation helpers ─────────────────────────────────────────────────────────
# Invalid config values log a clear warning and fall back to the default
# instead of crashing the pipeline.

def _warn_invalid(name, value, default, why):
    print(
        f"WARNING: config value {name}={value!r} is invalid ({why}); "
        f"using default {default!r}",
        file=sys.stderr,
    )


def _valid_number(value, default, name, minimum=None, integer=False):
    """Validate a numeric config value (type + range). Falls back to default."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _warn_invalid(name, value, default, "not a number")
        return default
    if integer and not isinstance(value, int):
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        else:
            _warn_invalid(name, value, default, "not an integer")
            return default
    if minimum is not None and value < minimum:
        _warn_invalid(name, value, default, f"must be >= {minimum}")
        return default
    return value


def _valid_string(value, default, name):
    """Validate a non-empty string config value. Falls back to default."""
    if not isinstance(value, str) or not value.strip():
        _warn_invalid(name, value, default, "must be a non-empty string")
        return default
    return value.strip()


# ── Paths ──────────────────────────────────────────────────────────────────────

DB_PATH = str(ROOT / _cfg.get("database", "data/crm.db"))
LOG_DIR = str(ROOT / _cfg.get("log_dir", "logs"))
SYNC_LOG = os.path.join(LOG_DIR, "crm-sync.log")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ── Accounts ───────────────────────────────────────────────────────────────────

ACCOUNTS = _cfg.get("accounts", [])
ACCOUNT_EMAILS = [a["address"] for a in ACCOUNTS]

# ── iCloud ─────────────────────────────────────────────────────────────────────

_icloud = _cfg.get("icloud", {})
ICLOUD_CARDDAV_BASE = _icloud.get("carddav_base", "")
ICLOUD_USER = _icloud.get("user", "")
ICLOUD_PASS = get_secret("ICLOUD_APP_PASSWORD")

# ── Zoom ───────────────────────────────────────────────────────────────────────

_zoom = _cfg.get("zoom", {})
ZOOM_USER_EMAIL = _zoom.get("user_email", "")
ZOOM_ACCOUNT_ID = get_secret("ZOOM_ACCOUNT_ID")
ZOOM_CLIENT_ID = get_secret("ZOOM_CLIENT_ID")
ZOOM_CLIENT_SECRET = get_secret("ZOOM_CLIENT_SECRET")

# ── Enrichment ─────────────────────────────────────────────────────────────────

_enrich = _cfg.get("enrichment", {})
ENRICHMENT_MODEL = _valid_string(
    _enrich.get("model", "gpt-4o-mini"), "gpt-4o-mini", "enrichment.model")
TAVILY_SCRIPT = _enrich.get("tavily_script")
RATE_LIMIT_SECONDS = _valid_number(
    _enrich.get("rate_limit_seconds", 1.2), 1.2, "enrichment.rate_limit_seconds", minimum=0)
ENRICHMENT_BUDGET_USD = _valid_number(
    _enrich.get("budget_usd", 5.0), 5.0, "enrichment.budget_usd", minimum=0)

# ── Sync settings ──────────────────────────────────────────────────────────────

_sync = _cfg.get("sync", {})
STALE_THRESHOLD_DAYS = _valid_number(
    _sync.get("stale_threshold_days", 180), 180, "sync.stale_threshold_days",
    minimum=1, integer=True)
ENRICH_LIMIT = _valid_number(
    _sync.get("enrich_limit", 30), 30, "sync.enrich_limit", minimum=1, integer=True)
GMAIL_MINE_LIMIT = _valid_number(
    _sync.get("gmail_mine_limit", 20), 20, "sync.gmail_mine_limit", minimum=1, integer=True)
SUMMARY_LIMIT = _valid_number(
    _sync.get("summary_limit", 20), 20, "sync.summary_limit", minimum=1, integer=True)

# ── External tools ─────────────────────────────────────────────────────────────

import shutil
GOG_BIN = shutil.which("gog") or "gog"

# Set GOG keyring password if provided
_gog_pw = get_secret("GOG_KEYRING_PASSWORD")
if _gog_pw:
    os.environ["GOG_KEYRING_PASSWORD"] = _gog_pw

# ── Supabase ───────────────────────────────────────────────────────────────────

SUPABASE_URL = get_secret("SUPABASE_URL")
SUPABASE_KEY = get_secret("SUPABASE_KEY")

# ── ro.am ──────────────────────────────────────────────────────────────────────

ROAM_API_KEY = get_secret("ROAM_API_KEY")

# ── Generic email domains (for enrichment filtering) ──────────────────────────

GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "me.com", "mac.com", "msn.com", "live.com", "aol.com", "protonmail.com",
    "pm.me", "fastmail.com", "hey.com",
}


def get_conn():
    """Get a configured SQLite connection."""
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


if __name__ == "__main__":
    print("=== Posca Config ===")
    print(f"  ROOT:      {ROOT}")
    print(f"  DB_PATH:   {DB_PATH}")
    print(f"  LOG_DIR:   {LOG_DIR}")
    print(f"  Accounts:  {len(ACCOUNTS)}")
    for a in ACCOUNTS:
        print(f"    - {a['label']} ({a['address']})")
    print(f"  GOG_BIN:   {GOG_BIN}")
    print(f"  Model:     {ENRICHMENT_MODEL}")
    print(f"  Stale days: {STALE_THRESHOLD_DAYS}")
    print(f"  iCloud:    {'configured' if ICLOUD_USER else 'not configured'}")
    print(f"  Zoom:      {'configured' if ZOOM_ACCOUNT_ID else 'not configured'}")
    print(f"  Supabase:  {'configured' if SUPABASE_URL else 'not configured'}")

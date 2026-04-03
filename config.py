"""
Centralized configuration for Headless CRM.
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
ENRICHMENT_MODEL = _enrich.get("model", "gpt-4o-mini")
TAVILY_SCRIPT = _enrich.get("tavily_script")
RATE_LIMIT_SECONDS = _enrich.get("rate_limit_seconds", 1.2)

# ── Sync settings ──────────────────────────────────────────────────────────────

_sync = _cfg.get("sync", {})
STALE_THRESHOLD_DAYS = _sync.get("stale_threshold_days", 180)
ENRICH_LIMIT = _sync.get("enrich_limit", 30)
GMAIL_MINE_LIMIT = _sync.get("gmail_mine_limit", 20)
SUMMARY_LIMIT = _sync.get("summary_limit", 20)

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

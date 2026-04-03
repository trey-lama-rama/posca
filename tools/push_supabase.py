#!/usr/bin/env python3
"""push_supabase.py — Read all records from local SQLite CRM DB and upsert to Supabase."""

import json
import logging
import sys
import sqlite3
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, SYNC_LOG, SUPABASE_URL, SUPABASE_KEY

BATCH_SIZE = 250

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(SYNC_LOG, mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── SQLite helpers ────────────────────────────────────────────────────────────
def fetch_table(conn, table):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return cols, [dict(zip(cols, row)) for row in rows]

def get_supabase_columns(base_url, key, table):
    """Fetch existing column names from Supabase by requesting 0 rows."""
    url = f"{base_url}/rest/v1/{table}?limit=0"
    headers = {
        "Authorization": f"Bearer {key}",
        "apikey": key,
        "Accept": "application/json",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.ok:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                return set(data[0].keys())
            # Empty result — try to get schema via head request with count
            resp2 = requests.get(f"{base_url}/rest/v1/{table}?limit=1", headers=headers, timeout=15)
            if resp2.ok:
                data2 = resp2.json()
                if isinstance(data2, list) and len(data2) > 0:
                    return set(data2[0].keys())
    except Exception as e:
        log.warning(f"Could not fetch Supabase columns for {table}: {e}")
    return None

def filter_row(row, allowed_cols):
    return {k: v for k, v in row.items() if k in allowed_cols}

# ── Supabase upsert ───────────────────────────────────────────────────────────
def upsert_batch(base_url, key, table, batch):
    url = f"{base_url}/rest/v1/{table}"
    headers = {
        "Authorization": f"Bearer {key}",
        "apikey": key,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    resp = requests.post(url, headers=headers, json=batch, timeout=30)
    return resp

def push_table(base_url, key, table, rows, supabase_cols=None):
    if supabase_cols:
        rows = [filter_row(r, supabase_cols) for r in rows]
    total = len(rows)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    log.info(f"  {table}: {total} rows, {batches} batch(es)")
    success = 0
    errors = 0
    for i in range(batches):
        batch = rows[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
        batch_num = i + 1
        try:
            resp = upsert_batch(base_url, key, table, batch)
            if resp.ok:
                success += len(batch)
                log.info(f"  {table} batch {batch_num}/{batches}: OK ({len(batch)} rows)")
            else:
                # Retry row-by-row to isolate bad records
                log.warning(
                    f"  {table} batch {batch_num}/{batches}: HTTP {resp.status_code} — retrying row-by-row"
                )
                for row in batch:
                    try:
                        r2 = upsert_batch(base_url, key, table, [row])
                        if r2.ok:
                            success += 1
                        else:
                            errors += 1
                            log.error(f"  {table} row skip: {r2.status_code} — {r2.text[:200]}")
                    except Exception as re:
                        errors += 1
                        log.error(f"  {table} row exception: {re}")
        except Exception as e:
            errors += len(batch)
            log.error(f"  {table} batch {batch_num}/{batches}: exception — {e}")
    return success, errors

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("push_supabase.py starting")

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL or SUPABASE_KEY not set — aborting.")
        sys.exit(1)

    log.info(f"Supabase URL: {SUPABASE_URL}")

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        log.error(f"Cannot open SQLite DB at {DB_PATH}: {e}")
        sys.exit(1)

    tables = ["contacts", "interactions", "action_items"]
    summary = {}

    for table in tables:
        log.info(f"Reading {table} from SQLite...")
        try:
            sqlite_cols, rows = fetch_table(conn, table)
        except Exception as e:
            log.error(f"Failed to read {table}: {e}")
            summary[table] = (0, 0)
            continue
        supabase_cols = get_supabase_columns(SUPABASE_URL, SUPABASE_KEY, table)
        if supabase_cols:
            new_cols = set(sqlite_cols) - supabase_cols
            if new_cols:
                log.info(f"  {table}: skipping {len(new_cols)} columns not in Supabase: {sorted(new_cols)}")
        ok, err = push_table(SUPABASE_URL, SUPABASE_KEY, table, rows, supabase_cols)
        summary[table] = (ok, err)

    conn.close()

    total_errors = sum(e for _, e in summary.values())
    log.info("========================================")
    log.info("Supabase push summary:")
    for table in tables:
        ok, err = summary.get(table, (0, 0))
        log.info(f"  {table}: {ok} pushed, {err} errors")
    log.info(f"  Total errors: {total_errors}")
    log.info("========================================")

    print("")
    print("===== SUPABASE PUSH COMPLETE =====")
    for table in tables:
        ok, err = summary.get(table, (0, 0))
        print(f"  {table}: {ok} pushed, {err} errors")

if __name__ == "__main__":
    main()

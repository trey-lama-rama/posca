#!/usr/bin/env python3
"""
embed.py — Vector embeddings for CRM contacts using Voyage AI.
Idempotent: only re-embeds contacts that are new or updated since last embed.

Usage:
  python3 -m enrichment.embed            # embed all pending contacts
  python3 -m enrichment.embed --force    # re-embed everything

Exposes: search_semantic(query, top_k=10) -> list[(contact_id, score)]
"""

import json
import logging
import math
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DB_PATH, LOG_DIR, get_secret

import voyageai

LOG_PATH = os.path.join(LOG_DIR, "enrichment.log")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "voyage-finance-2")
BATCH_SIZE = 100

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_embeddings_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS embeddings (
            contact_id TEXT PRIMARY KEY,
            embedding BLOB,
            embedded_text TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def build_embedding_text(contact, recent_subjects):
    """
    Build rich embedding text for a contact.
    Includes: name, company, role, relationship type, email domain, notes
    (AI summary + enrichment), and recent interaction subjects.
    Better coverage improves semantic search quality.
    """
    import re as _re, json as _json
    parts = [contact["name"] or ""]
    if contact["company"]:
        parts.append(contact["company"])
    if contact["role"]:
        parts.append(contact["role"])

    rel = (contact["relationship_type"] or "") if "relationship_type" in contact.keys() else ""
    if rel:
        parts.append(f"relationship: {rel}")

    # Email domain (company signal for generic-email contacts)
    generic_domains = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com", "me.com"}
    try:
        emails = _json.loads(contact["emails"] or "[]")
        for e in emails[:1]:
            domain = e.split("@")[-1].lower() if "@" in e else ""
            if domain and domain not in generic_domains:
                parts.append(f"domain: {domain}")
    except Exception:
        pass

    # Notes: parse structured blocks first, fall back to plain
    notes = contact["notes"] or ""
    if notes:
        summary_m = _re.search(r"## Summary\n(.*?)(?=\n##|\Z)", notes, _re.DOTALL)
        enrich_m = _re.search(r"## Enrichment\n(.*?)(?=\n##|\Z)", notes, _re.DOTALL)
        if summary_m:
            parts.append(summary_m.group(1).strip()[:300])
        if enrich_m:
            for line in enrich_m.group(1).strip().splitlines():
                if any(line.startswith(k) for k in ("Industry:", "Education:", "Location:")):
                    parts.append(line.strip())
        if not summary_m and not enrich_m:
            parts.append(notes.strip()[:400])

    if recent_subjects:
        parts.append("recent: " + "; ".join(recent_subjects))

    return " | ".join(p for p in parts if p and p.strip())


def get_contacts_to_embed(conn, force=False):
    """Return contacts that need (re-)embedding."""
    if force:
        rows = conn.execute(
            "SELECT id, name, company, role, notes, updated_at FROM contacts"
        ).fetchall()
    else:
        rows = conn.execute("""
            SELECT c.id, c.name, c.company, c.role, c.notes, c.updated_at
            FROM contacts c
            LEFT JOIN embeddings e ON c.id = e.contact_id
            WHERE e.contact_id IS NULL
               OR c.updated_at > e.updated_at
        """).fetchall()
    return rows


def get_recent_subjects(conn, contact_id, limit=3):
    rows = conn.execute("""
        SELECT subject FROM interactions
        WHERE contact_id = ? AND subject IS NOT NULL AND subject != ''
        ORDER BY date DESC LIMIT ?
    """, (contact_id, limit)).fetchall()
    return [r["subject"] for r in rows]


def cosine_similarity(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def search_semantic(query, top_k=10):
    """
    Embed query and return top_k contact_ids ranked by cosine similarity.
    Used by query modules for natural language fallback search.
    """
    api_key = get_secret("VOYAGE_API_KEY")
    client = voyageai.Client(api_key=api_key)
    result = client.embed([query], model=EMBEDDING_MODEL)
    query_vec = result.embeddings[0]

    conn = get_conn()
    rows = conn.execute("SELECT contact_id, embedding FROM embeddings").fetchall()
    conn.close()

    scored = []
    for row in rows:
        try:
            vec = json.loads(row["embedding"])
            sim = cosine_similarity(query_vec, vec)
            scored.append((row["contact_id"], sim))
        except Exception:
            pass

    scored.sort(key=lambda x: x[1], reverse=True)
    return [(cid, score) for cid, score in scored[:top_k]]


def _embed_batch(client, texts):
    result = client.embed(texts, model=EMBEDDING_MODEL)
    return result.embeddings


def main():
    force = "--force" in sys.argv
    log.info("=== CRM Embed Starting%s ===", " (FORCE)" if force else "")

    api_key = get_secret("VOYAGE_API_KEY")
    if not api_key:
        log.error("VOYAGE_API_KEY not set.")
        sys.exit(1)

    client = voyageai.Client(api_key=api_key)
    conn = get_conn()
    ensure_embeddings_table(conn)

    contacts = get_contacts_to_embed(conn, force=force)
    log.info("Contacts needing embedding: %d", len(contacts))

    if not contacts:
        log.info("All contacts are up to date. Nothing to embed.")
        conn.close()
        return

    # Build (id, text) pairs
    records = []
    for c in contacts:
        subjects = get_recent_subjects(conn, c["id"])
        text = build_embedding_text(c, subjects)
        if not text.strip():
            text = c["name"] or "unknown"
        records.append((c["id"], text))

    total = len(records)
    embedded = 0
    errors = 0
    now = datetime.utcnow().isoformat()

    for i in range(0, total, BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        ids = [r[0] for r in batch]
        texts = [r[1] for r in batch]

        try:
            embeddings = _embed_batch(client, texts)
        except Exception as e:
            log.error("Batch %d-%d failed: %s", i, i + BATCH_SIZE, e)
            errors += len(batch)
            continue

        for cid, text, vec in zip(ids, texts, embeddings):
            conn.execute("""
                INSERT OR REPLACE INTO embeddings (contact_id, embedding, embedded_text, updated_at)
                VALUES (?, ?, ?, ?)
            """, (cid, json.dumps(vec), text, now))

        conn.commit()
        embedded += len(batch)
        log.info("Progress: %d/%d embedded...", embedded, total)

    conn.close()
    log.info(
        "=== Embed complete. %d embedded, %d errors. ===",
        embedded, errors,
    )
    return embedded, errors


if __name__ == "__main__":
    main()

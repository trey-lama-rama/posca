"""
contact_lookup.py — In-memory contact matching index for the seed scripts.

Replaces the per-contact `SELECT ... WHERE emails LIKE ?` queries and the
full-table name scans with a single load at the start of a run.

Matching semantics are identical to the old per-query code:
  1. Exact email match (case-insensitive) against the contacts.emails JSON array.
     First matching row (table order) wins.
  2. Name similarity via difflib.SequenceMatcher; first contact (table order)
     with ratio >= threshold wins.

The index must be kept up to date during the run:
  - call add() after inserting a new contact
  - call add_email() after merging a new email into an existing contact
"""

import json
from difflib import SequenceMatcher

DEFAULT_NAME_THRESHOLD = 0.85


def name_similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


class ContactLookup:
    """email -> contact_id and normalized-name -> contact_id index over contacts."""

    def __init__(self, conn):
        self._by_email = {}   # lowercased email -> contact id (first row wins)
        self._by_name = {}    # lowercased name  -> contact id (first row wins)
        self.has_primary_email = table_has_column(conn, "contacts", "primary_email")
        for row in conn.execute("SELECT id, name, emails FROM contacts"):
            self._index_row(row[0], row[1], row[2])

    def _index_row(self, contact_id, name, emails_json):
        try:
            emails = json.loads(emails_json or "[]")
        except Exception:
            emails = []
        for email in emails:
            if email:
                self._by_email.setdefault(str(email).strip().lower(), contact_id)
        if name:
            self._by_name.setdefault(str(name).lower(), contact_id)

    # -- updates during the run ------------------------------------------------

    def add(self, contact_id, name, emails):
        """Register a newly inserted contact."""
        for email in emails or []:
            if email:
                self._by_email.setdefault(str(email).strip().lower(), contact_id)
        if name:
            self._by_name.setdefault(str(name).lower(), contact_id)

    def add_email(self, contact_id, email):
        """Register an email merged into an existing contact."""
        if email:
            self._by_email.setdefault(str(email).strip().lower(), contact_id)

    # -- lookups -----------------------------------------------------------------

    def find_by_email(self, email):
        if not email:
            return None
        return self._by_email.get(str(email).strip().lower())

    def find_by_name(self, name, threshold=DEFAULT_NAME_THRESHOLD):
        if not name:
            return None
        lowered = str(name).lower()
        for existing_name, contact_id in self._by_name.items():
            # ratio() <= quick_ratio() <= real_quick_ratio() (documented upper
            # bounds), so gating on the cheap bounds skips only rows that could
            # never reach the threshold — results are identical, just faster.
            matcher = SequenceMatcher(None, lowered, existing_name)
            if (matcher.real_quick_ratio() >= threshold
                    and matcher.quick_ratio() >= threshold
                    and matcher.ratio() >= threshold):
                return contact_id
        return None

    def find(self, email, name, threshold=DEFAULT_NAME_THRESHOLD, min_name_len=3):
        """Email match first, then name similarity — same precedence as before."""
        contact_id = self.find_by_email(email)
        if contact_id:
            return contact_id
        if name and len(name) >= min_name_len:
            return self.find_by_name(name, threshold)
        return None


def table_has_column(conn, table, column):
    return any(r[1] == column for r in conn.execute(f"PRAGMA table_info({table})"))


def set_primary_email(conn, contact_id, emails, enabled=True):
    """
    Maintain the denormalized contacts.primary_email column.
    Sets it from the first email if it is currently NULL/empty. No-op when
    the column doesn't exist yet (enabled=False) or there are no emails.
    """
    if not enabled or not emails:
        return
    first = next((str(e).strip().lower() for e in emails if e), None)
    if not first:
        return
    conn.execute(
        "UPDATE contacts SET primary_email = COALESCE(NULLIF(primary_email, ''), ?) WHERE id = ?",
        (first, contact_id),
    )

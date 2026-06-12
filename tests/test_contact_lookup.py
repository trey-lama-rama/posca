"""Tests for the in-memory contact matching index (contact_lookup.py)
and the gmail seed's find_existing_contact / upsert_contact built on it."""

import json

from contact_lookup import ContactLookup
from tests.conftest import insert_contact


# ── ContactLookup core semantics ──────────────────────────────────────────────

class TestEmailMatch:
    def test_exact_email_match(self, conn):
        insert_contact(conn, "c1", "Jane Doe", '["jane@example.com"]')
        lookup = ContactLookup(conn)
        assert lookup.find_by_email("jane@example.com") == "c1"

    def test_email_match_is_case_insensitive(self, conn):
        insert_contact(conn, "c1", "Jane Doe", '["Jane@Example.COM"]')
        lookup = ContactLookup(conn)
        assert lookup.find_by_email("JANE@example.com") == "c1"

    def test_email_match_any_position_in_json_array(self, conn):
        insert_contact(conn, "c1", "Jane Doe", '["jane@example.com", "jd@corp.com"]')
        lookup = ContactLookup(conn)
        assert lookup.find_by_email("jd@corp.com") == "c1"

    def test_email_takes_precedence_over_name(self, conn):
        insert_contact(conn, "c1", "Jane Doe", '["jane@example.com"]')
        insert_contact(conn, "c2", "Janet Doe", '["janet@example.com"]')
        lookup = ContactLookup(conn)
        # "Janet Doe" is similar to "Jane Doe", but the email pins it to c2
        assert lookup.find("janet@example.com", "Janet Doe") == "c2"

    def test_malformed_emails_json_is_skipped(self, conn):
        insert_contact(conn, "c1", "Broken Row", "not-json")
        insert_contact(conn, "c2", "Good Row", '["good@example.com"]')
        lookup = ContactLookup(conn)
        assert lookup.find_by_email("good@example.com") == "c2"


class TestNameSimilarityMatch:
    def test_similar_name_matches_at_default_threshold(self, conn):
        insert_contact(conn, "c1", "Jonathan Smith", '["jon@x.com"]')
        lookup = ContactLookup(conn)
        # close-but-not-exact name, no email
        assert lookup.find_by_name("Jonathon Smith") == "c1"

    def test_dissimilar_name_does_not_match(self, conn):
        insert_contact(conn, "c1", "Jonathan Smith", '["jon@x.com"]')
        lookup = ContactLookup(conn)
        assert lookup.find_by_name("Alice Wong") is None

    def test_threshold_is_respected(self, conn):
        insert_contact(conn, "c1", "Bob Marley", "[]")
        lookup = ContactLookup(conn)
        # "Rob Marley" vs "Bob Marley": high similarity, passes 0.82 and 0.85
        assert lookup.find_by_name("Rob Marley", threshold=0.85) == "c1"
        # An impossible threshold rejects everything except exact-ish matches
        assert lookup.find_by_name("Rob Harley", threshold=0.99) is None


class TestNoMatch:
    def test_empty_db(self, conn):
        lookup = ContactLookup(conn)
        assert lookup.find("a@b.com", "Anyone At All") is None

    def test_no_email_no_name(self, conn):
        insert_contact(conn, "c1", "Jane Doe", '["jane@example.com"]')
        lookup = ContactLookup(conn)
        assert lookup.find(None, None) is None
        assert lookup.find("", "") is None


class TestMidRunUpdates:
    def test_new_contact_added_mid_run_is_found(self, conn):
        lookup = ContactLookup(conn)  # loaded while DB empty
        assert lookup.find_by_email("new@x.com") is None
        lookup.add("c9", "New Person", ["new@x.com"])
        assert lookup.find_by_email("new@x.com") == "c9"
        assert lookup.find_by_name("New Person") == "c9"

    def test_email_merged_mid_run_is_found(self, conn):
        insert_contact(conn, "c1", "Jane Doe", '["jane@example.com"]')
        lookup = ContactLookup(conn)
        assert lookup.find_by_email("jane.doe@work.com") is None
        lookup.add_email("c1", "jane.doe@work.com")
        assert lookup.find_by_email("jane.doe@work.com") == "c1"


# ── seeds.gmail find_existing_contact / upsert_contact on a temp DB ──────────

class TestGmailSeedIntegration:
    def _import_gmail(self):
        from seeds import gmail
        return gmail

    def test_find_existing_email_match(self, conn):
        gmail = self._import_gmail()
        insert_contact(conn, "c1", "Jane Doe", '["jane@example.com"]')
        lookup = ContactLookup(conn)
        assert gmail.find_existing_contact(lookup, "jane@example.com", "Someone Else") == "c1"

    def test_find_existing_name_similarity_match(self, conn):
        gmail = self._import_gmail()
        insert_contact(conn, "c1", "Jonathan Smith", '["jon@x.com"]')
        lookup = ContactLookup(conn)
        assert gmail.find_existing_contact(lookup, "different@y.com", "Jonathon Smith") == "c1"

    def test_find_existing_no_match(self, conn):
        gmail = self._import_gmail()
        insert_contact(conn, "c1", "Jane Doe", '["jane@example.com"]')
        lookup = ContactLookup(conn)
        assert gmail.find_existing_contact(lookup, "x@y.com", "Zelda Quux") is None

    def test_short_names_never_name_match(self, conn):
        gmail = self._import_gmail()
        insert_contact(conn, "c1", "Al", "[]")
        lookup = ContactLookup(conn)
        # names of length <= 2 skip the similarity path entirely
        assert gmail.find_existing_contact(lookup, None, "Al") is None

    def test_upsert_inserts_then_matches_mid_run(self, conn):
        gmail = self._import_gmail()
        lookup = ContactLookup(conn)

        cid1, is_new1 = gmail.upsert_contact(
            conn, lookup, "Fresh Person", "fresh@x.com",
            "warm", "test@acct.com", "2026-06-01", "inbound")
        assert is_new1

        # Same email again in the same run: must match, not duplicate
        cid2, is_new2 = gmail.upsert_contact(
            conn, lookup, "Fresh Person", "fresh@x.com",
            "warm", "test@acct.com", "2026-06-02", "inbound")
        assert not is_new2
        assert cid2 == cid1

        count = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
        assert count == 1

        # primary_email is maintained on insert
        primary = conn.execute(
            "SELECT primary_email FROM contacts WHERE id=?", (cid1,)).fetchone()[0]
        assert primary == "fresh@x.com"

    def test_upsert_merges_new_email_and_registers_it(self, conn):
        gmail = self._import_gmail()
        insert_contact(conn, "c1", "Jonathan Smith", '["jon@x.com"]')
        lookup = ContactLookup(conn)

        # Matched by name similarity, brings a second email along
        cid, is_new = gmail.upsert_contact(
            conn, lookup, "Jonathon Smith", "jsmith@work.com",
            "warm", "test@acct.com", "2026-06-01", "inbound")
        assert not is_new
        assert cid == "c1"

        emails = json.loads(
            conn.execute("SELECT emails FROM contacts WHERE id='c1'").fetchone()[0])
        assert set(emails) == {"jon@x.com", "jsmith@work.com"}

        # The merged email is immediately findable in the same run
        assert lookup.find_by_email("jsmith@work.com") == "c1"

"""Tests for the pure scoring functions in enrichment/relationship_score.py."""

from datetime import date, timedelta

from enrichment.relationship_score import compute_score, score_to_heat


def make_contact(**overrides):
    contact = {
        "last_contact_date": None,
        "relationship_type": "warm",
        "company": None,
        "role": None,
        "stale_flag": 0,
    }
    contact.update(overrides)
    return contact


def days_ago(n):
    return (date.today() - timedelta(days=n)).isoformat()


class TestComputeScore:
    def test_hot_contact_clamps_at_100(self):
        # 50 base +30 recency +15 frequency +10 personal +5 enrichment = 110 -> 100
        c = make_contact(
            last_contact_date=days_ago(1),
            relationship_type="personal",
            company="Acme", role="CEO",
        )
        assert compute_score(c, interaction_count=20) == 100

    def test_never_contacted_cold_inbound(self):
        # 50 base -15 no-date -5 cold-inbound -20 stale = 10
        c = make_contact(relationship_type="cold-inbound", stale_flag=1)
        assert compute_score(c, interaction_count=0) == 10

    def test_recency_tiers(self):
        recent = make_contact(last_contact_date=days_ago(10))
        old = make_contact(last_contact_date=days_ago(400))
        assert compute_score(recent, 0) > compute_score(old, 0)
        # exact values: 50+30+5(warm)=85 vs 50-15+5(warm)=40
        assert compute_score(recent, 0) == 85
        assert compute_score(old, 0) == 40

    def test_interaction_frequency_adds_points(self):
        c = make_contact(last_contact_date=days_ago(60))
        assert compute_score(c, 15) - compute_score(c, 0) == 15
        assert compute_score(c, 5) - compute_score(c, 0) == 8

    def test_enrichment_completeness(self):
        base = make_contact(last_contact_date=days_ago(60))
        both = make_contact(last_contact_date=days_ago(60), company="Acme", role="CTO")
        one = make_contact(last_contact_date=days_ago(60), company="Acme")
        assert compute_score(both, 0) - compute_score(base, 0) == 5
        assert compute_score(one, 0) - compute_score(base, 0) == 2

    def test_score_never_negative(self):
        c = make_contact(relationship_type="cold-inbound", stale_flag=1,
                         last_contact_date=days_ago(1000))
        assert compute_score(c, 0) >= 0


class TestScoreToHeat:
    def test_boundaries(self):
        assert score_to_heat(70) == "hot"
        assert score_to_heat(69) == "warm"
        assert score_to_heat(50) == "warm"
        assert score_to_heat(49) == "cool"
        assert score_to_heat(30) == "cool"
        assert score_to_heat(29) == "cold"
        assert score_to_heat(10) == "cold"
        assert score_to_heat(9) == "ghost"
        assert score_to_heat(0) == "ghost"

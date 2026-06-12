"""Tests for the per-run LLM budget guard (costguard.py)."""

import pytest

from costguard import BudgetExceeded, CostGuard


def test_charges_accumulate_and_raise_at_cap():
    guard = CostGuard(budget_usd=0.001)
    guard.charge("gpt-4o-mini", "x" * 4000, max_output_tokens=250)
    with pytest.raises(BudgetExceeded):
        for _ in range(10_000):
            guard.charge("gpt-4o-mini", "x" * 4000, max_output_tokens=250)


def test_zero_budget_blocks_first_call():
    guard = CostGuard(budget_usd=0)
    with pytest.raises(BudgetExceeded):
        guard.charge("gpt-4o-mini", "hello", max_output_tokens=10)


def test_normal_budget_allows_typical_run():
    guard = CostGuard(budget_usd=5.0)
    # 100 typical enrichment calls should be far below $5 on gpt-4o-mini
    for _ in range(100):
        guard.charge("gpt-4o-mini", "x" * 3500, max_output_tokens=250)
    assert guard.spent_usd < 1.0


def test_unknown_model_uses_conservative_fallback():
    cheap = CostGuard.estimate_cost("gpt-4o-mini", 4000, 250)
    unknown = CostGuard.estimate_cost("some-future-model", 4000, 250)
    assert unknown > cheap

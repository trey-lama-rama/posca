"""
costguard.py — Per-run LLM spend guard.

Tracks estimated spend for a run and raises BudgetExceeded once the
configured cap (config.yaml -> enrichment.budget_usd, default 5.0) would
be exceeded. Callers treat BudgetExceeded as a SOFT stop: log it, finish
the step cleanly, exit 0 — never crash the pipeline.

Estimates are intentionally conservative: input tokens ~= chars/4 and the
full max_tokens allowance is assumed to be generated as output.

Usage:
    from costguard import CostGuard, BudgetExceeded

    guard = CostGuard()                  # cap from config.yaml
    guard.charge(model, prompt, 250)     # before each LLM call
"""

DEFAULT_BUDGET_USD = 5.0

# $ per 1M tokens (input, output). Unknown models use the conservative fallback.
MODEL_PRICES = {
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4o": (2.50, 10.00),
    "gpt-4.1-nano": (0.10, 0.40),
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4.1": (2.00, 8.00),
    "gpt-5-nano": (0.05, 0.40),
    "gpt-5-mini": (0.25, 2.00),
    "gpt-5": (1.25, 10.00),
}
DEFAULT_PRICE = (2.50, 10.00)


class BudgetExceeded(Exception):
    """Estimated LLM spend for this run has hit the configured cap."""


class CostGuard:
    def __init__(self, budget_usd=None):
        if budget_usd is None:
            try:
                from config import ENRICHMENT_BUDGET_USD
                budget_usd = ENRICHMENT_BUDGET_USD
            except Exception:
                budget_usd = DEFAULT_BUDGET_USD
        self.budget_usd = max(0.0, float(budget_usd))
        self.spent_usd = 0.0
        self.calls = 0

    @staticmethod
    def estimate_cost(model, prompt_chars=0, max_output_tokens=0):
        """Estimated $ cost of one chat call (chars/4 input tokens, full output)."""
        in_price, out_price = MODEL_PRICES.get(model, DEFAULT_PRICE)
        input_tokens = prompt_chars / 4.0
        return (input_tokens * in_price + max_output_tokens * out_price) / 1_000_000

    def charge(self, model, prompt="", max_output_tokens=0):
        """
        Record one upcoming LLM call. Raises BudgetExceeded once the
        estimated run total passes the cap (a 0 budget allows no LLM spend).
        Returns the estimated cost of this call.
        """
        cost = self.estimate_cost(model, len(prompt or ""), max_output_tokens)
        self.spent_usd += cost
        self.calls += 1
        if self.spent_usd > self.budget_usd:
            raise BudgetExceeded(
                f"Estimated LLM spend ${self.spent_usd:.4f} exceeds budget "
                f"${self.budget_usd:.2f} after {self.calls} call(s)"
            )
        return cost

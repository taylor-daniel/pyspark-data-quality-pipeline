"""Data-quality checks, run at each stage rather than once at the end.

Every check returns a Check: a name, a pass/fail, the measured value and a short
explanation. A failing check does not silently continue — `run_all` returns the
report and the caller decides, which is what makes the pipeline safe to schedule.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class Check:
    name: str
    passed: bool
    value: str
    detail: str = ""

    def __str__(self) -> str:
        return f"[{'PASS' if self.passed else 'FAIL'}] {self.name:<34} {self.value}"


@dataclass
class Report:
    stage: str
    checks: list[Check] = field(default_factory=list)
    advisory: bool = False  # advisory reports describe the input; they do not fail the run

    def add(self, check: Check) -> Check:
        self.checks.append(check)
        return check

    @property
    def passed(self) -> bool:
        return True if self.advisory else all(c.passed for c in self.checks)

    def to_markdown(self) -> str:
        head = f"### {self.stage}\n\n| Check | Result | Value |\n|---|---|---|\n"
        rows = "\n".join(
            f"| {c.name} | {'✅ pass' if c.passed else '❌ fail'} | {c.value} |"
            for c in self.checks
        )
        return head + rows + "\n"

    def __str__(self) -> str:
        return f"{self.stage}\n" + "\n".join(f"  {c}" for c in self.checks)


# ── individual checks ─────────────────────────────────────────────────────────
def check_not_null(df: DataFrame, column: str) -> Check:
    nulls = df.filter(F.col(column).isNull()).count()
    return Check(
        name=f"no nulls in {column}",
        passed=nulls == 0,
        value=f"{nulls} null(s)",
        detail="A null in a key means the row cannot be attributed.",
    )


def check_unique(df: DataFrame, column: str) -> Check:
    total, distinct = df.count(), df.select(column).distinct().count()
    return Check(
        name=f"{column} is unique",
        passed=total == distinct,
        value=f"{total - distinct} duplicate(s)",
        detail="Duplicates double-count revenue.",
    )


def check_referential(child: DataFrame, parent: DataFrame, key: str) -> Check:
    orphans = child.join(parent.select(key).distinct(), key, "left_anti").count()
    return Check(
        name=f"every {key} exists in the parent",
        passed=orphans == 0,
        value=f"{orphans} unmatched",
        detail="Unmatched keys are revenue the report cannot place.",
    )


def check_row_count(df: DataFrame, minimum: int) -> Check:
    rows = df.count()
    return Check(
        name="row count above the floor",
        passed=rows >= minimum,
        value=f"{rows:,} rows",
        detail="A collapse in row count usually means an upstream extract failed.",
    )


def check_date_order(df: DataFrame, earlier: str, later: str) -> Check:
    wrong = df.filter(F.col(later) < F.col(earlier)).count()
    return Check(
        name=f"{later} is not before {earlier}",
        passed=wrong == 0,
        value=f"{wrong} row(s) out of order",
        detail="Transaction dates trailing the issue date are expected; the reverse is not.",
    )


def reconcile_premium(fact: DataFrame, transactions: DataFrame, tolerance: Decimal = Decimal("0.01")) -> Check:
    """The check that earns adoption: does the model agree with the source of truth?"""
    modelled = fact.agg(F.sum("premium")).collect()[0][0] or Decimal(0)
    booked = (
        transactions.join(fact.select("policy_no").distinct(), "policy_no", "inner")
        .agg(F.sum("premium"))
        .collect()[0][0]
        or Decimal(0)
    )
    difference = abs(Decimal(modelled) - Decimal(booked))
    return Check(
        name="premium reconciles to the transaction system",
        passed=difference <= tolerance,
        value=f"difference {difference:,.2f}",
        detail="Reported premium must equal what the transaction system booked.",
    )


# ── staged suites ─────────────────────────────────────────────────────────────
def check_raw(quotes, policies, transactions) -> Report:
    """What the data looks like before anything is cleaned.

    This stage is expected to fail: that is the point. It measures the mess the
    pipeline has to absorb, so the same checks passing at stage 1 mean something.
    """
    report = Report("Stage 0 — raw extracts, before standardisation", advisory=True)
    report.add(check_unique(quotes, "quote_id"))
    report.add(check_unique(policies, "policy_no"))
    report.add(check_referential(transactions, policies, "policy_no"))
    padded = quotes.filter(F.col("product_code") != F.trim(F.col("product_code"))).count()
    report.add(
        Check(
            "product_code free of padding",
            padded == 0,
            f"{padded} padded value(s)",
            "Padded keys silently break joins.",
        )
    )
    mixed = quotes.filter(F.col("product_code") != F.upper(F.col("product_code"))).count()
    report.add(
        Check(
            "product_code consistently cased",
            mixed == 0,
            f"{mixed} mixed-case value(s)",
            "Case differences split one product into several.",
        )
    )
    return report



def check_sources(quotes: DataFrame, policies: DataFrame, products: DataFrame, transactions: DataFrame) -> Report:
    report = Report("Stage 1 — source data, after standardisation")
    report.add(check_row_count(quotes, minimum=100))
    report.add(check_unique(quotes, "quote_id"))
    report.add(check_unique(policies, "policy_no"))
    report.add(check_unique(products, "product_code"))
    report.add(check_not_null(policies, "product_code"))
    report.add(check_referential(policies, quotes, "quote_id"))
    return report


def check_fact(fact: DataFrame, transactions: DataFrame) -> Report:
    report = Report("Stage 2 — the joined fact table")
    report.add(check_unique(fact, "policy_no"))
    report.add(check_not_null(fact, "product_name"))
    report.add(check_not_null(fact, "channel"))
    report.add(check_date_order(fact, "issued_on", "booked_on"))
    report.add(reconcile_premium(fact, transactions))
    return report


def check_outputs(by_product: DataFrame, by_month: DataFrame, fact: DataFrame) -> Report:
    report = Report("Stage 3 — the aggregated outputs")
    total_fact = fact.agg(F.sum("premium")).collect()[0][0] or Decimal(0)
    total_product = by_product.agg(F.sum("premium")).collect()[0][0] or Decimal(0)
    total_month = by_month.agg(F.sum("premium")).collect()[0][0] or Decimal(0)
    report.add(
        Check(
            "aggregate by product ties to the fact",
            abs(Decimal(total_fact) - Decimal(total_product)) <= Decimal("0.01"),
            f"difference {abs(Decimal(total_fact) - Decimal(total_product)):,.2f}",
        )
    )
    report.add(
        Check(
            "aggregate by month ties to the fact",
            abs(Decimal(total_fact) - Decimal(total_month)) <= Decimal("0.01"),
            f"difference {abs(Decimal(total_fact) - Decimal(total_month)):,.2f}",
        )
    )
    return report

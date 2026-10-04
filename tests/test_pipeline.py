"""The transformation rules, tested on tiny hand-written frames."""
from decimal import Decimal

from src.pipeline import (
    build_fact,
    classify,
    conversion_by_channel,
    issued_only,
    revenue_by_month,
    revenue_by_product_and_channel,
    standardise_policies,
    standardise_products,
    standardise_quotes,
    standardise_transactions,
)


def _sources(spark):
    quotes = spark.createDataFrame(
        [
            ("  q1 ", " mtr-01 ", "intermediary portal", "2025-01-10", 100.00),
            ("Q2", "TRV-11", "Customer app", "2025-01-20", 200.00),
            ("Q2", "TRV-11", "Customer app", "2025-01-20", 200.00),  # duplicate row
            ("Q3", "MTR-01", "Customer app", "2025-02-02", 300.00),
        ],
        ["quote_id", "product_code", "channel", "quoted_on", "quoted_premium"],
    )
    policies = spark.createDataFrame(
        [
            ("mtr/ip/000001", "Q1", "MTR-01", "issued", "2025-01-12"),
            ("TRV/CA/000002", "Q2", "trv-11 ", "ISSUED", "2025-01-25"),
            ("MTR/CA/000003", "Q3", "MTR-01", "DRAFT", "2025-02-04"),
        ],
        ["policy_no", "quote_id", "product_code", "status", "issued_on"],
    )
    products = spark.createDataFrame(
        [("MTR-01", "MTR", "Motor"), ("TRV-11", "TRV", "Travel")],
        ["product_code", "product_group", "product_name"],
    )
    transactions = spark.createDataFrame(
        [
            ("T1", "MTR/IP/000001", "2025-01-15", 100.00),
            ("T2", "TRV/CA/000002", "2025-01-27", 200.00),
            ("T3", "MTR/CA/000003", "2025-02-06", 300.00),  # a draft policy: must not count
        ],
        ["txn_id", "policy_no", "booked_on", "premium"],
    )
    return (
        standardise_quotes(quotes),
        standardise_policies(policies),
        standardise_products(products),
        standardise_transactions(transactions),
    )


def test_standardisation_trims_pads_and_deduplicates(spark):
    quotes, policies, _, _ = _sources(spark)
    assert quotes.count() == 3, "the duplicated quote row should be collapsed"
    assert quotes.filter("quote_id = 'Q1'").count() == 1, "padded keys should be trimmed"
    assert policies.filter("policy_no = 'MTR/IP/000001'").count() == 1, "keys should be upper-cased"


def test_classification_reads_group_and_channel_from_the_policy_number(spark):
    quotes, policies, products, transactions = _sources(spark)
    fact = classify(build_fact(quotes, policies, products, transactions))
    row = fact.filter("policy_no = 'TRV/CA/000002'").collect()[0]
    assert row["policy_group"] == "TRV"
    assert row["channel_derived"] == "Customer app"
    assert row["channel_matches_source"] is True


def test_draft_policies_never_reach_reporting(spark):
    quotes, policies, products, transactions = _sources(spark)
    fact = issued_only(build_fact(quotes, policies, products, transactions))
    assert fact.count() == 2
    assert "MTR/CA/000003" not in [r["policy_no"] for r in fact.collect()]


def test_aggregates_tie_back_to_the_fact_table(spark):
    quotes, policies, products, transactions = _sources(spark)
    fact = issued_only(build_fact(quotes, policies, products, transactions))
    total = fact.agg({"premium": "sum"}).collect()[0][0]
    by_product = revenue_by_product_and_channel(fact).agg({"premium": "sum"}).collect()[0][0]
    by_month = revenue_by_month(fact).agg({"premium": "sum"}).collect()[0][0]
    assert total == by_product == by_month == Decimal("300.00")


def test_conversion_uses_quotes_as_the_denominator(spark):
    quotes, policies, products, transactions = _sources(spark)
    fact = issued_only(build_fact(quotes, policies, products, transactions))
    rows = {r["channel"]: r for r in conversion_by_channel(quotes, fact).collect()}
    assert rows["Intermediary Portal"]["conversion_pct"] == 100.0
    assert rows["Customer App"]["policies"] == 1  # the draft does not convert

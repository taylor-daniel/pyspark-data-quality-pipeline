"""The checks themselves: each one must fail when it should."""
from src import quality
from src.pipeline import build_fact, issued_only, standardise_policies, standardise_quotes


def test_duplicate_check_catches_a_repeated_key(spark):
    df = spark.createDataFrame([("A",), ("A",), ("B",)], ["policy_no"])
    check = quality.check_unique(df, "policy_no")
    assert not check.passed and "1 duplicate" in check.value


def test_null_check_catches_a_missing_value(spark):
    df = spark.createDataFrame([("A",), (None,)], ["product_code"])
    assert not quality.check_not_null(df, "product_code").passed


def test_referential_check_catches_an_orphan(spark):
    child = spark.createDataFrame([("P1",), ("P9",)], ["policy_no"])
    parent = spark.createDataFrame([("P1",)], ["policy_no"])
    check = quality.check_referential(child, parent, "policy_no")
    assert not check.passed and "1 unmatched" in check.value


def test_date_order_check_allows_a_later_booking_but_not_an_earlier_one(spark):
    df = spark.createDataFrame(
        [("2025-01-01", "2025-01-05"), ("2025-02-01", "2025-01-20")],
        ["issued_on", "booked_on"],
    )
    assert not quality.check_date_order(df, "issued_on", "booked_on").passed


def test_reconciliation_fails_when_the_model_disagrees_with_the_source(spark):
    quotes = standardise_quotes(
        spark.createDataFrame([("Q1", "MTR-01", "Customer app", "2025-01-01", 100.0)],
                              ["quote_id", "product_code", "channel", "quoted_on", "quoted_premium"]))
    policies = standardise_policies(
        spark.createDataFrame([("MTR/CA/000001", "Q1", "MTR-01", "ISSUED", "2025-01-02")],
                              ["policy_no", "quote_id", "product_code", "status", "issued_on"]))
    products = spark.createDataFrame([("MTR-01", "MTR", "Motor")],
                                     ["product_code", "product_group", "product_name"])
    good = spark.createDataFrame([("T1", "MTR/CA/000001", "2025-01-03", 100.0)],
                                 ["txn_id", "policy_no", "booked_on", "premium"])
    fact = issued_only(build_fact(quotes, policies, products, good))
    assert quality.reconcile_premium(fact, good).passed

    # the same fact table, checked against a source that booked a different amount
    other = spark.createDataFrame([("T1", "MTR/CA/000001", "2025-01-03", 180.0)],
                                  ["txn_id", "policy_no", "booked_on", "premium"])
    assert not quality.reconcile_premium(fact, other).passed


def test_a_report_is_only_green_when_every_check_passes():
    report = quality.Report("demo")
    report.add(quality.Check("a", True, "ok"))
    assert report.passed
    report.add(quality.Check("b", False, "nope"))
    assert not report.passed


def test_advisory_reports_do_not_fail_the_run():
    report = quality.Report("raw", advisory=True)
    report.add(quality.Check("a", False, "expected"))
    assert report.passed

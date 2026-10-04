"""Run the whole thing: generate (optional) → standardise → check → build → check → aggregate → check → write."""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from . import quality
from .generate_data import generate
from .pipeline import (
    build_fact,
    conversion_by_channel,
    get_spark,
    issued_only,
    revenue_by_month,
    revenue_by_product_and_channel,
    standardise_policies,
    standardise_products,
    standardise_quotes,
    standardise_transactions,
)


def read_csv(spark, path: Path):
    return spark.read.option("header", True).option("inferSchema", True).csv(str(path))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", default="data/raw", help="folder holding the source CSVs")
    parser.add_argument("--out", default="data/out", help="where the outputs are written")
    parser.add_argument("--report", default="docs/quality-report.md", help="markdown report path")
    parser.add_argument("--generate", action="store_true", help="generate the synthetic sources first")
    parser.add_argument("--quotes", type=int, default=4000)
    args = parser.parse_args()

    raw, out = Path(args.raw), Path(args.out)
    if args.generate:
        generate(raw, n_quotes=args.quotes)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # 0. read raw and measure the mess (advisory: expected to fail)
    quotes_raw = read_csv(spark, raw / "quotes.csv")
    policies_raw = read_csv(spark, raw / "policies.csv")
    products_raw = read_csv(spark, raw / "products.csv")
    transactions_raw = read_csv(spark, raw / "transactions.csv")
    reports = [quality.check_raw(quotes_raw, policies_raw, transactions_raw)]

    # 1. standardise, then run the same checks again
    quotes = standardise_quotes(quotes_raw)
    policies = standardise_policies(policies_raw)
    products = standardise_products(products_raw)
    transactions = standardise_transactions(transactions_raw)

    reports.append(quality.check_sources(quotes, policies, products, transactions))

    # 2. integrate, classify, filter
    fact_all = build_fact(quotes, policies, products, transactions)
    fact = issued_only(fact_all).filter(F.col("premium").isNotNull())
    removed = fact_all.count() - fact.count()
    reports.append(quality.check_fact(fact, transactions))

    # 3. aggregate
    by_product = revenue_by_product_and_channel(fact)
    by_month = revenue_by_month(fact)
    conversion = conversion_by_channel(quotes, fact)
    reports.append(quality.check_outputs(by_product, by_month, fact))

    # 4. write
    out.mkdir(parents=True, exist_ok=True)
    for name, df in [
        ("fact_policies", fact),
        ("revenue_by_product_and_channel", by_product),
        ("revenue_by_month", by_month),
        ("conversion_by_channel", conversion),
    ]:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out / name))

    # 5. report
    lines = [
        "# Data quality report",
        "",
        f"Source rows: {policies.count():,} policies · {quotes.count():,} quotes · "
        f"{transactions.count():,} transactions",
        f"Rows excluded from reporting (draft, lapsed or unbooked): {removed:,}",
        "",
    ]
    for report in reports:
        print(report, "\n")
        lines.append(report.to_markdown())
    verdict = all(r.passed for r in reports)
    lines.append(f"\n**Overall: {'all checks passed' if verdict else 'checks failed — outputs not trusted'}**\n")
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text("\n".join(lines), encoding="utf-8")

    print(f"outputs  -> {out}")
    print(f"report   -> {args.report}")
    spark.stop()
    return 0 if verdict else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""The transformation itself: standardise, join, filter, classify, aggregate.

The order matters. Keys are standardised *before* any join, because a trailing space
is the most common reason two correct datasets refuse to match. Filtering happens
after the join so that the rows removed can be counted and reported, rather than
silently disappearing.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

CHANNEL_FROM_POLICY_MARKER = {"IP": "Intermediary portal", "CA": "Customer app"}


def get_spark(app_name: str = "digital-revenue-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ── 1. standardise ────────────────────────────────────────────────────────────
def clean_key(col: str):
    """Trim, collapse inner whitespace and upper-case a join key."""
    return F.upper(F.regexp_replace(F.trim(F.col(col)), r"\s+", " "))


def clean_label(col: str):
    """Trim and collapse whitespace on a label, keeping its original casing."""
    return F.regexp_replace(F.trim(F.col(col)), r"\s+", " ")


def standardise_quotes(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("quote_id", clean_key("quote_id"))
        .withColumn("product_code", clean_key("product_code"))
        .withColumn("channel", F.initcap(clean_label("channel")))
        .withColumn("quoted_on", F.to_date("quoted_on"))
        .withColumn("quoted_premium", F.col("quoted_premium").cast(DecimalType(18, 2)))
        .dropDuplicates(["quote_id"])
    )


def standardise_policies(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("policy_no", clean_key("policy_no"))
        .withColumn("quote_id", clean_key("quote_id"))
        .withColumn("product_code", clean_key("product_code"))
        .withColumn("status", clean_key("status"))
        .withColumn("issued_on", F.to_date("issued_on"))
        .dropDuplicates(["policy_no"])
    )


def standardise_products(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("product_code", clean_key("product_code"))
        .withColumn("product_group", clean_key("product_group"))
        .withColumn("product_name", clean_label("product_name"))
        .dropDuplicates(["product_code"])
    )


def standardise_transactions(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("txn_id", clean_key("txn_id"))
        .withColumn("policy_no", clean_key("policy_no"))
        .withColumn("booked_on", F.to_date("booked_on"))
        .withColumn("premium", F.col("premium").cast(DecimalType(18, 2)))
        .dropDuplicates(["txn_id"])
    )


# ── 2. classify ───────────────────────────────────────────────────────────────
def classify(df: DataFrame) -> DataFrame:
    """Derive reporting dimensions from the policy number and the product reference.

    Policy numbers follow <GROUP>/<CHANNEL MARKER>/<SEQUENCE>, so the group and the
    channel can be recovered from the key itself rather than trusting a free-text field.
    """
    marker = F.split(F.col("policy_no"), "/").getItem(1)
    channel_expr = F.create_map(
        *[x for k, v in CHANNEL_FROM_POLICY_MARKER.items() for x in (F.lit(k), F.lit(v))]
    )[marker]
    return (
        df.withColumn("policy_group", F.split(F.col("policy_no"), "/").getItem(0))
        .withColumn("channel_derived", channel_expr)
        .withColumn(
            # the two sides are compared case-insensitively: the policy number is a key,
            # the channel field is a label, and they are cased by different systems
            "channel_matches_source",
            F.upper(F.col("channel_derived")) == F.upper(F.col("channel")),
        )
    )


# ── 3. integrate and filter ───────────────────────────────────────────────────
def build_fact(
    quotes: DataFrame, policies: DataFrame, products: DataFrame, transactions: DataFrame
) -> DataFrame:
    """One row per issued policy, with its quote, product and booked premium."""
    joined = (
        policies.join(quotes.select("quote_id", "channel", "quoted_on", "quoted_premium"), "quote_id", "left")
        .join(products.select("product_code", "product_group", "product_name"), "product_code", "left")
        .join(transactions.select("policy_no", "booked_on", "premium"), "policy_no", "left")
    )
    return classify(joined)


def issued_only(fact: DataFrame) -> DataFrame:
    """Draft and lapsed policies never reach reporting."""
    return fact.filter(F.col("status") == "ISSUED")


# ── 4. aggregate ──────────────────────────────────────────────────────────────
def revenue_by_product_and_channel(fact: DataFrame) -> DataFrame:
    return (
        fact.groupBy("product_name", "channel")
        .agg(
            F.countDistinct("policy_no").alias("policies"),
            F.sum("premium").alias("premium"),
        )
        .orderBy("product_name", "channel")
    )


def revenue_by_month(fact: DataFrame) -> DataFrame:
    """Booked on the transaction date, not the issue date: the two differ by days."""
    return (
        fact.withColumn("month", F.date_format("booked_on", "yyyy-MM"))
        .groupBy("month")
        .agg(
            F.countDistinct("policy_no").alias("policies"),
            F.sum("premium").alias("premium"),
        )
        .orderBy("month")
    )


def conversion_by_channel(quotes: DataFrame, fact: DataFrame) -> DataFrame:
    quoted = quotes.groupBy("channel").agg(F.countDistinct("quote_id").alias("quotes"))
    converted = fact.groupBy("channel").agg(F.countDistinct("policy_no").alias("policies"))
    return (
        quoted.join(converted, "channel", "left")
        .withColumn("policies", F.coalesce("policies", F.lit(0)))
        .withColumn(
            "conversion_pct",
            F.round(F.col("policies") / F.col("quotes") * 100, 2),
        )
        .orderBy("channel")
    )

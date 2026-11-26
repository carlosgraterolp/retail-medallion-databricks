# Databricks notebook source
# MAGIC %md
# MAGIC # silver_rules: shared cleaning and validation logic
# MAGIC
# MAGIC Pure DataFrame functions, used by `02_silver` (incrementally, on each new bronze micro-batch)
# MAGIC and by `04_quality_checks` (on the whole bronze table, to prove that the incremental silver
# MAGIC equals a full recomputation). Keeping the rules in one place is what makes that proof valid.
# MAGIC
# MAGIC | Step | What | Why |
# MAGIC |---|---|---|
# MAGIC | Types | `try_cast` / `try_to_timestamp` instead of `cast` | ANSI mode is on in serverless: a bad value in `cast` fails the whole job; `try_` returns NULL so a rule can catch it |
# MAGIC | Text | trim, collapse spaces, upper-case invoice and stock code | `85123a` and `85123A` are the same product |
# MAGIC | Flags | `is_cancelled`, `is_guest`, `is_non_merchandise` | cancellations and guest checkouts are business facts, so they are flagged, not removed |
# MAGIC | Rules | see `RULES` | invalid rows go to quarantine with the reasons |
# MAGIC | Dedup | exact duplicates on all business columns (`line_id`) | the two source sheets overlap (2010-12-01 to 2010-12-09) and the source repeats some lines |

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# Stock codes that are services, fees or adjustments rather than merchandise (found by profiling).
NON_MERCHANDISE_CODES = [
    "POST", "DOT", "M", "C2", "D", "S", "B", "BANK CHARGES", "ADJUST", "ADJUST2",
    "AMAZONFEE", "PADS", "CRUK", "TEST001", "TEST002",
]

# A row that matches any rule goes to quarantine. Missing customer_id is NOT a rule: those are
# guest sales, kept with is_guest = true and mapped to the "Unknown customer" member in gold.
RULES = {
    "missing_invoice_no": "invoice_no IS NULL",
    "invalid_invoice_no": "invoice_no NOT RLIKE '^C?[0-9]{6}$'",  # e.g. 'A' bad-debt adjustments
    "missing_stock_code": "stock_code IS NULL",
    "invalid_invoice_date": "invoice_ts IS NULL",
    "invalid_quantity": "quantity IS NULL OR quantity = 0",
    "invalid_price": "unit_price IS NULL OR unit_price < 0",
    "zero_price": "unit_price = 0",  # free items / stock adjustments, not sales
    "invalid_customer_id": "_raw_customer_id IS NOT NULL AND customer_id IS NULL",
    "missing_country": "country IS NULL OR country = ''",
    "negative_qty_not_cancelled": "quantity < 0 AND NOT is_cancelled",  # stock write-offs
    "positive_qty_cancelled": "quantity > 0 AND is_cancelled",
}

BUSINESS_COLS = [
    "invoice_no", "stock_code", "description", "quantity",
    "invoice_ts", "unit_price", "customer_id", "country",
]

SILVER_COLS = [
    "line_id", "invoice_no", "invoice_ts", "invoice_date", "stock_code", "description",
    "quantity", "unit_price", "line_amount", "customer_id", "country",
    "is_cancelled", "is_guest", "is_non_merchandise", "_source_file", "_ingested_at",
]


def validate(bronze_df):
    """Type, clean and flag bronze rows; add `failed_rules` (empty array = valid row)."""
    typed = bronze_df.select(
        F.upper(F.trim("invoice")).alias("invoice_no"),
        F.upper(F.trim("stock_code")).alias("stock_code"),
        F.expr("nullif(trim(regexp_replace(description, '\\\\s+', ' ')), '')").alias("description"),
        F.expr("try_cast(trim(quantity) AS INT)").alias("quantity"),
        F.expr("try_to_timestamp(trim(invoice_date), 'yyyy-MM-dd HH:mm:ss')").alias("invoice_ts"),
        F.expr("try_cast(trim(price) AS DECIMAL(12,3))").alias("unit_price"),
        F.expr("try_cast(trim(customer_id) AS INT)").alias("customer_id"),
        F.expr("nullif(trim(customer_id), '')").alias("_raw_customer_id"),
        F.trim("country").alias("country"),
        "_source_file",
        "_ingested_at",
    )
    flagged = (
        typed.withColumn("is_cancelled", F.col("invoice_no").startswith("C"))
        .withColumn("is_guest", F.col("customer_id").isNull())
        .withColumn(
            "is_non_merchandise",
            F.col("stock_code").isin(NON_MERCHANDISE_CODES) | F.col("stock_code").startswith("GIFT_"),
        )
        .withColumn("line_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,3)"))
    )
    failed = F.array_compact(F.array(*[F.when(F.expr(cond), F.lit(name)) for name, cond in RULES.items()]))
    return flagged.withColumn("failed_rules", failed)


def to_silver(valid_df):
    """Add the deterministic line_id (SHA-256 of all business columns) and the silver columns."""
    return (
        valid_df.withColumn(
            "line_id",
            F.sha2(
                F.concat_ws(
                    "||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in BUSINESS_COLS]
                ),
                256,
            ),
        )
        .withColumn("invoice_date", F.to_date("invoice_ts"))
        .select(*SILVER_COLS)
    )


def dedup(silver_df):
    """Keep one row per line_id: the copy from the earliest file (ties broken by ingestion time)."""
    w = Window.partitionBy("line_id").orderBy("_source_file", "_ingested_at")
    return silver_df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

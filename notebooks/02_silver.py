# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver: clean, conform, validate, deduplicate
# MAGIC
# MAGIC Input: `retail_bronze.online_retail_raw` (all strings). Outputs:
# MAGIC - `retail_silver.sales_lines`: typed, valid, deduplicated invoice lines (one row per invoice line)
# MAGIC - `retail_silver.sales_lines_quarantine`: rows that failed at least one rule, with the list of failed rules
# MAGIC
# MAGIC Nothing is silently dropped: every bronze row ends up in exactly one of
# MAGIC **silver**, **quarantine** or the **duplicate** count, and the notebook asserts that.
# MAGIC
# MAGIC | Step | What | Why |
# MAGIC |---|---|---|
# MAGIC | Types | `try_cast` / `try_to_timestamp` instead of `cast` | ANSI mode is on in serverless: a bad value in `cast` fails the whole job; `try_` returns NULL so a rule can catch it |
# MAGIC | Text | trim, collapse spaces, upper-case invoice and stock code | `85123a` and `85123A` are the same product |
# MAGIC | Flags | `is_cancelled`, `is_guest`, `is_non_merchandise` | cancellations and guest checkouts are business facts, so they are flagged, not removed |
# MAGIC | Rules | see `RULES` below | invalid rows go to quarantine with the reasons |
# MAGIC | Dedup | exact duplicates on all business columns | the two source sheets overlap (2010-12-01 to 2010-12-09) and the source repeats some lines |

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# Stock codes that are services, fees or adjustments rather than merchandise (found by profiling).
NON_MERCHANDISE_CODES = [
    "POST", "DOT", "M", "C2", "D", "S", "B", "BANK CHARGES", "ADJUST", "ADJUST2",
    "AMAZONFEE", "PADS", "CRUK", "TEST001", "TEST002",
]

bronze = spark.table(T_BRONZE)

typed = bronze.select(
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation rules
# MAGIC A row that matches any rule goes to quarantine. Missing `customer_id` is **not** a rule:
# MAGIC those are guest sales (about 22% of silver lines) and stay in silver with `is_guest = true`.
# MAGIC In gold they map to the "Unknown customer" member.

# COMMAND ----------

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

failed_rules = F.array_compact(
    F.array(*[F.when(F.expr(cond), F.lit(name)) for name, cond in RULES.items()])
)
checked = flagged.withColumn("failed_rules", failed_rules)

valid = checked.filter(F.size("failed_rules") == 0).drop("failed_rules", "_raw_customer_id")
quarantine = checked.filter(F.size("failed_rules") > 0).withColumn(
    "_quarantined_at", F.current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication and line id
# MAGIC Exact duplicates on all business columns are removed, keeping the copy from the earliest file.
# MAGIC Assumption (documented in the README): the source has no line number, so two identical lines
# MAGIC on the same invoice at the same second are treated as one line.

# COMMAND ----------

BUSINESS_COLS = [
    "invoice_no", "stock_code", "description", "quantity",
    "invoice_ts", "unit_price", "customer_id", "country",
]

w = Window.partitionBy(*BUSINESS_COLS).orderBy("_source_file")
deduped = (
    valid.withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
    .withColumn(
        "line_id",  # deterministic natural key of the line, unique after dedup
        F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in BUSINESS_COLS]),
            256,
        ),
    )
    .withColumn("invoice_date", F.to_date("invoice_ts"))
)

silver = deduped.select(
    "line_id", "invoice_no", "invoice_ts", "invoice_date", "stock_code", "description",
    "quantity", "unit_price", "line_amount", "customer_id", "country",
    "is_cancelled", "is_guest", "is_non_merchandise", "_source_file", "_ingested_at",
)

silver.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(T_SILVER)
quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(T_QUARANTINE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation: bronze = silver + quarantine + duplicates removed

# COMMAND ----------

n_bronze = spark.table(T_BRONZE).count()
n_silver = spark.table(T_SILVER).count()
n_quarantine = spark.table(T_QUARANTINE).count()
n_valid = n_bronze - n_quarantine
n_duplicates = n_valid - n_silver

rule_counts = (
    spark.table(T_QUARANTINE)
    .select(F.explode("failed_rules").alias("rule"))
    .groupBy("rule").count().orderBy(F.desc("count"))
)
display(rule_counts)

flags = spark.table(T_SILVER).agg(
    F.sum(F.col("is_cancelled").cast("int")).alias("cancelled"),
    F.sum(F.col("is_guest").cast("int")).alias("guest"),
    F.sum(F.col("is_non_merchandise").cast("int")).alias("non_merchandise"),
).first()

assert n_silver + n_quarantine + n_duplicates == n_bronze, "Row reconciliation failed"
assert spark.table(T_SILVER).select("line_id").distinct().count() == n_silver, "line_id not unique"

rules_txt = ",".join(f"{r['rule']}:{r['count']}" for r in rule_counts.collect())
result = (
    f"bronze={n_bronze} silver={n_silver} quarantine={n_quarantine} duplicates_removed={n_duplicates} "
    f"cancelled={flags['cancelled']} guest={flags['guest']} non_merchandise={flags['non_merchandise']} "
    f"rules[{rules_txt}]"
)
print(result)
dbutils.notebook.exit(result)

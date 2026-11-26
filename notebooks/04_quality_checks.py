# Databricks notebook source
# MAGIC %md
# MAGIC # 04_quality_checks: fail loudly
# MAGIC
# MAGIC Each check is a SQL query that returns one number (`observed`), compared to an `expected`
# MAGIC value. All results are appended to `retail_gold.dq_check_results` (an audit trail per run).
# MAGIC If any check fails, the notebook raises an exception, so the job task and the run are marked
# MAGIC **FAILED**. A wrong dashboard is worse than a late one.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

import datetime
from decimal import Decimal

from pyspark.sql import Row

U = UNKNOWN_KEY


def scalar(sql):
    return spark.sql(sql).first()[0]


n_bronze = scalar(f"SELECT count(*) FROM {T_BRONZE}")
n_silver = scalar(f"SELECT count(*) FROM {T_SILVER}")
n_quarantine = scalar(f"SELECT count(*) FROM {T_QUARANTINE}")
silver_revenue = scalar(f"SELECT sum(line_amount) FROM {T_SILVER}")

# (check_name, layer, sql returning one value, expected value)
CHECKS = [
    # --- row counts per layer
    ("bronze_not_empty", "bronze", f"SELECT count(*) > 0 FROM {T_BRONZE}", True),
    ("silver_not_empty", "silver", f"SELECT count(*) > 0 FROM {T_SILVER}", True),
    ("silver_plus_quarantine_le_bronze", "silver", f"SELECT {n_silver} + {n_quarantine} <= {n_bronze}", True),
    ("fact_rows_equal_silver_rows", "gold", f"SELECT count(*) FROM {T_FACT}", n_silver),
    # --- silver rules really enforced
    ("silver_no_zero_or_negative_price", "silver", f"SELECT count(*) FROM {T_SILVER} WHERE unit_price <= 0", 0),
    ("silver_no_zero_quantity", "silver", f"SELECT count(*) FROM {T_SILVER} WHERE quantity = 0", 0),
    ("silver_cancelled_have_negative_qty", "silver",
     f"SELECT count(*) FROM {T_SILVER} WHERE is_cancelled AND quantity > 0", 0),
    ("silver_line_id_unique", "silver",
     f"SELECT count(*) - count(DISTINCT line_id) FROM {T_SILVER}", 0),
    # --- null checks on fact keys
    *[
        (f"fact_{c}_not_null", "gold", f"SELECT count(*) FROM {T_FACT} WHERE {c} IS NULL", 0)
        for c in ("line_id", "invoice_no", "date_key", "product_key", "customer_key", "country_key")
    ],
    # --- duplicate checks on keys
    ("fact_line_id_unique", "gold", f"SELECT count(*) - count(DISTINCT line_id) FROM {T_FACT}", 0),
    *[
        (f"{t.split('.')[-1]}_{k}_unique", "gold", f"SELECT count(*) - count(DISTINCT {k}) FROM {t}", 0)
        for t, k in (
            (T_DIM_PRODUCT, "product_key"), (T_DIM_PRODUCT, "stock_code"),
            (T_DIM_CUSTOMER, "customer_key"), (T_DIM_COUNTRY, "country_key"),
            (T_DIM_COUNTRY, "country_name"), (T_DIM_DATE, "date_key"),
        )
    ],
    # count(DISTINCT) ignores NULL, so customer_id uniqueness is checked on non-unknown rows
    ("dim_customer_customer_id_unique", "gold",
     f"SELECT count(*) - count(DISTINCT customer_id) FROM {T_DIM_CUSTOMER} WHERE customer_key <> {U}", 0),
    # --- unknown members present
    *[
        (f"{t.split('.')[-1]}_has_unknown_member", "gold", f"SELECT count(*) FROM {t} WHERE {k} = {U}", 1)
        for t, k in ((T_DIM_PRODUCT, "product_key"), (T_DIM_CUSTOMER, "customer_key"), (T_DIM_COUNTRY, "country_key"))
    ],
    # --- referential integrity fact -> dims (orphans)
    *[
        (f"ri_fact_to_{t.split('.')[-1]}", "gold",
         f"SELECT count(*) FROM {T_FACT} f LEFT ANTI JOIN {t} d ON f.{k} = d.{k}", 0)
        for t, k in ((T_DIM_DATE, "date_key"), (T_DIM_PRODUCT, "product_key"),
                     (T_DIM_CUSTOMER, "customer_key"), (T_DIM_COUNTRY, "country_key"))
    ],
    # only guest lines may point to the unknown customer
    ("fact_unknown_customer_only_for_guests", "gold",
     f"SELECT count(*) FROM {T_FACT} f JOIN {T_SILVER} s USING (line_id) "
     f"WHERE (f.customer_key = {U}) <> s.is_guest", 0),
    # --- revenue reconciliation
    ("revenue_silver_equals_fact", "gold", f"SELECT sum(line_amount) FROM {T_FACT}", silver_revenue),
    ("revenue_fact_equals_agg_country", "gold",
     f"SELECT sum(net_revenue) FROM {T_AGG_MONTHLY_COUNTRY}", silver_revenue),
    ("revenue_fact_equals_agg_cancellation_rate", "gold",
     f"SELECT sum(gross_sales) - sum(cancelled_amount) FROM {T_AGG_CANCELLATION_RATE}", silver_revenue),
]

# COMMAND ----------

run_ts = datetime.datetime.now(datetime.timezone.utc)
results = []
for name, layer, sql, expected in CHECKS:
    observed = scalar(sql)
    passed = observed == expected
    results.append(Row(run_ts=run_ts, check_name=name, layer=layer,
                       observed=str(observed), expected=str(expected), passed=passed))

results_df = spark.createDataFrame(results)
results_df.write.format("delta").mode("append").saveAsTable(T_DQ_RESULTS)
display(results_df)

failed = [r for r in results if not r.passed]
n_pass = len(results) - len(failed)
summary = (f"checks={len(results)} passed={n_pass} failed={len(failed)} "
           f"bronze={n_bronze} silver={n_silver} quarantine={n_quarantine} "
           f"revenue={Decimal(silver_revenue):.2f}")
print(summary)

if failed:
    details = "; ".join(f"{r.check_name}: observed={r.observed} expected={r.expected}" for r in failed)
    raise AssertionError(f"DATA QUALITY FAILED ({len(failed)}/{len(results)}): {details}")

dbutils.notebook.exit(summary)

# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup: configuration and Unity Catalog objects
# MAGIC
# MAGIC Single place for names and paths. Every other notebook starts with `%run ./00_setup`.
# MAGIC
# MAGIC The schemas and the landing volume are created by the bundle (`databricks bundle deploy`).
# MAGIC The `CREATE ... IF NOT EXISTS` statements below are idempotent, so the notebooks also work
# MAGIC when imported and run by hand in the UI without the bundle.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")

BRONZE = f"{CATALOG}.retail_bronze"
SILVER = f"{CATALOG}.retail_silver"
GOLD = f"{CATALOG}.retail_gold"

LANDING_VOLUME = f"{BRONZE}.landing"
RAW_PATH = f"/Volumes/{CATALOG}/retail_bronze/landing/online_retail_ii/"

# Tables
T_BRONZE = f"{BRONZE}.online_retail_raw"
T_SILVER = f"{SILVER}.sales_lines"
T_QUARANTINE = f"{SILVER}.sales_lines_quarantine"
T_DIM_DATE = f"{GOLD}.dim_date"
T_DIM_PRODUCT = f"{GOLD}.dim_product"
T_DIM_CUSTOMER = f"{GOLD}.dim_customer"
T_DIM_COUNTRY = f"{GOLD}.dim_country"
T_FACT = f"{GOLD}.fact_sales"
T_AGG_MONTHLY_COUNTRY = f"{GOLD}.agg_monthly_revenue_by_country"
T_AGG_TOP_PRODUCTS = f"{GOLD}.agg_top_products"
T_AGG_CANCELLATION_RATE = f"{GOLD}.agg_monthly_cancellation_rate"
T_DQ_RESULTS = f"{GOLD}.dq_check_results"

# Surrogate key used for the "unknown" member of each dimension (e.g. guest checkouts).
UNKNOWN_KEY = -1

# COMMAND ----------

for schema in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {LANDING_VOLUME}")
print(f"catalog={CATALOG} raw_path={RAW_PATH}")

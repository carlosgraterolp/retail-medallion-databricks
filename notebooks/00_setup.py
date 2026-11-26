# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup: configuration and platform check
# MAGIC
# MAGIC Single place for names and paths. Every other notebook starts with `%run ./00_setup`.
# MAGIC
# MAGIC The schemas and the landing volume are platform resources owned by Terraform (`terraform/`).
# MAGIC The notebooks do not create them (the CI service principal has no CREATE SCHEMA right);
# MAGIC they only check that they exist and fail with a clear message otherwise.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")  # "prod" (CI deployment) or "dev" (developer deployment)
CATALOG = dbutils.widgets.get("catalog")
ENV = dbutils.widgets.get("env")
assert ENV in ("prod", "dev"), f"env must be prod or dev, got {ENV!r}"
SUFFIX = "" if ENV == "prod" else "_dev"  # dev writes to its own *_dev schemas (Terraform)

BRONZE = f"{CATALOG}.retail_bronze{SUFFIX}"
SILVER = f"{CATALOG}.retail_silver{SUFFIX}"
GOLD = f"{CATALOG}.retail_gold{SUFFIX}"

# Raw files land once, in the prod landing volume; both environments only read them.
LANDING_VOLUME = f"{CATALOG}.retail_bronze.landing"
LANDING = f"/Volumes/{CATALOG}/retail_bronze/landing"
SALES_LANDING = f"{LANDING}/online_retail_ii_weekly/"
CATALOG_LANDING = f"{LANDING}/product_catalog/"
# Streaming checkpoints and Auto Loader schema locations: one folder per stream.
CHECKPOINTS_VOLUME = f"{BRONZE}.checkpoints"
CHECKPOINTS = f"/Volumes/{CATALOG}/retail_bronze{SUFFIX}/checkpoints"

# Tables
T_BRONZE = f"{BRONZE}.online_retail_raw"
T_BRONZE_CATALOG = f"{BRONZE}.product_catalog_raw"
T_SILVER = f"{SILVER}.sales_lines"
T_QUARANTINE = f"{SILVER}.sales_lines_quarantine"
T_SILVER_CATALOG = f"{SILVER}.product_catalog"
T_SILVER_PRICES = f"{SILVER}.product_price_history"
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

existing_schemas = {r[0] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()}
missing = [s for s in (BRONZE, SILVER, GOLD) if s.split(".")[1] not in existing_schemas]
if missing:
    raise RuntimeError(f"Missing schemas {missing}: create them with Terraform (terraform/) first.")
for volume in (LANDING_VOLUME, CHECKPOINTS_VOLUME):
    schema_name, volume_name = volume.split(".")[1:]
    if volume_name not in {r[1] for r in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{schema_name}").collect()}:
        raise RuntimeError(f"Missing volume {volume}: create it with Terraform (terraform/) first.")
print(f"env={ENV} catalog={CATALOG} bronze={BRONZE} silver={SILVER} gold={GOLD}")

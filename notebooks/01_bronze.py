# Databricks notebook source
# MAGIC %md
# MAGIC # 01_bronze: incremental ingestion with Auto Loader
# MAGIC
# MAGIC Two Auto Loader (`cloudFiles`) streams, each with its own checkpoint in the checkpoints volume.
# MAGIC Both run with `trigger(availableNow=True)`: process every file that arrived since the last run,
# MAGIC then stop (serverless supports only this trigger). A rerun with no new files ingests 0 rows.
# MAGIC
# MAGIC | Stream | Source | Schema | Target |
# MAGIC |---|---|---|---|
# MAGIC | Sales | weekly CSV files in `landing/online_retail_ii_weekly/` | explicit, every column a string (values kept as-is); anything that does not fit goes to `_rescued_data` | `online_retail_raw` |
# MAGIC | Product catalog | JSON Lines in `landing/product_catalog/` (semi-structured, nested) | inferred, stored in the schema location; `addNewColumns` evolution | `product_catalog_raw` |
# MAGIC
# MAGIC **Schema evolution:** when a catalog file brings a new field, Auto Loader records it in the schema
# MAGIC location and deliberately stops the stream with `UnknownFieldException`. The job retries the task
# MAGIC once (`max_retries: 1` in the bundle) and the retry ingests the file with the new column.
# MAGIC
# MAGIC Only the column *names* of the CSV are changed (`Customer ID` -> `customer_id`): Delta column
# MAGIC names cannot contain spaces without column mapping. Values are untouched.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Source header names -> bronze column names. Auto Loader checks the CSV header against the schema
# when a rescued-data column is set, so the schema must use the header names exactly; columns are
# renamed (snake_case) afterwards, values untouched.
SOURCE_COLUMNS = {
    "Invoice": "invoice",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "price",
    "Customer ID": "customer_id",
    "Country": "country",
}
SALES_SCHEMA = StructType([StructField(name, StringType()) for name in SOURCE_COLUMNS])


def table_count(table):
    return spark.table(table).count() if spark.catalog.tableExists(table) else 0


def has_files(path):
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:  # the folder does not exist yet: nothing has landed
        return False


# COMMAND ----------

# MAGIC %md ## Sales: weekly CSV files

# COMMAND ----------

before = table_count(T_BRONZE)
if has_files(SALES_LANDING):
    sales = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')  # pandas writes embedded quotes as ""
        .option("rescuedDataColumn", "_rescued_data")
        .schema(SALES_SCHEMA)
        .load(SALES_LANDING)
        .select(
            *[F.col(f"`{src}`").alias(dst) for src, dst in SOURCE_COLUMNS.items()],
            "_rescued_data",
            F.col("_metadata.file_name").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
    (
        sales.writeStream.option("checkpointLocation", f"{CHECKPOINTS}/bronze_sales")
        .trigger(availableNow=True)
        .toTable(T_BRONZE)
        .awaitTermination()
    )
sales_new = table_count(T_BRONZE) - before
print(f"sales: {sales_new:,} new rows, {before + sales_new:,} in {T_BRONZE}")

# COMMAND ----------

# MAGIC %md ## Product catalog: nested JSON with schema inference and evolution

# COMMAND ----------

before_catalog = table_count(T_BRONZE_CATALOG)
if has_files(CATALOG_LANDING):
    catalog = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/bronze_catalog")
        .option("cloudFiles.inferColumnTypes", True)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(CATALOG_LANDING)
        .select(
            "*",
            F.col("_metadata.file_name").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
    (
        catalog.writeStream.option("checkpointLocation", f"{CHECKPOINTS}/bronze_catalog")
        .option("mergeSchema", True)  # let the Delta table take the new columns Auto Loader adds
        .trigger(availableNow=True)
        .toTable(T_BRONZE_CATALOG)
        .awaitTermination()
    )
catalog_new = table_count(T_BRONZE_CATALOG) - before_catalog
print(f"catalog: {catalog_new:,} new documents, {before_catalog + catalog_new:,} in {T_BRONZE_CATALOG}")

# COMMAND ----------

rescued = (
    spark.table(T_BRONZE).filter("_rescued_data IS NOT NULL").count() if spark.catalog.tableExists(T_BRONZE) else 0
)
result = (
    f"sales_new={sales_new} bronze_rows={before + sales_new} rescued_rows={rescued} "
    f"catalog_new={catalog_new} catalog_docs={before_catalog + catalog_new}"
)
print(result)
dbutils.notebook.exit(result)

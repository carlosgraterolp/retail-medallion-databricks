# Databricks notebook source
# MAGIC %md
# MAGIC # 01_bronze: raw ingestion
# MAGIC
# MAGIC Reads the CSV files from the landing volume **as-is** into a Delta table:
# MAGIC - every column is kept as a string (no type inference, no cleaning, no filtering);
# MAGIC - two metadata columns are added: `_source_file` and `_ingested_at`.
# MAGIC
# MAGIC The only change is to column *names* (`Customer ID` -> `customer_id`), because Delta column
# MAGIC names cannot contain spaces without column mapping. Values are untouched.
# MAGIC
# MAGIC Load strategy: full overwrite, so re-running the job is idempotent.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

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

raw = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", False)  # bronze keeps source values as text
    .option("quote", '"')
    .option("escape", '"')  # pandas writes embedded quotes as ""
    .load(RAW_PATH)
)

missing = set(SOURCE_COLUMNS) - set(raw.columns)
assert not missing, f"Source schema changed, missing columns: {missing}"

bronze = raw.select(
    *[F.col(f"`{src}`").alias(dst) for src, dst in SOURCE_COLUMNS.items()],
    F.col("_metadata.file_name").alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)

(
    bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(T_BRONZE)
)

# COMMAND ----------

# Writing above was the action that executed the plan; reading back the table gives the counts.
summary = spark.table(T_BRONZE).groupBy("_source_file").count().orderBy("_source_file")
display(summary)
total = spark.table(T_BRONZE).count()
print(f"BRONZE {T_BRONZE}: {total:,} rows")
dbutils.notebook.exit(f"bronze_rows={total}")

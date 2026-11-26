# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver: incremental cleaning, validation, dedup and quarantine
# MAGIC
# MAGIC Bronze is append-only, so silver reads it as a **Delta streaming source**: each stream's
# MAGIC checkpoint records which bronze versions it has processed, and `trigger(availableNow=True)`
# MAGIC processes only what is new. The cleaning and validation rules live in `silver_rules`.
# MAGIC
# MAGIC | Stream | Sink | How reruns stay idempotent |
# MAGIC |---|---|---|
# MAGIC | Valid sales lines | `sales_lines` via `foreachBatch` | dedup inside the micro-batch, then an **insert-only `MERGE` on `line_id`**: a replayed batch inserts nothing twice, and a line already loaded from an earlier batch (the overlapping sheets) is skipped |
# MAGIC | Rows failing a rule | `sales_lines_quarantine` (direct Delta sink) | the Delta sink commits each micro-batch once per checkpoint (exactly-once) |
# MAGIC | Product catalog (JSON) | `product_catalog` and `product_price_history` via `foreachBatch` | `MERGE` on `stock_code` (latest document wins, SCD Type 1) and on `(stock_code, valid_from)` |
# MAGIC
# MAGIC `foreachBatch` only guarantees at-least-once delivery, which is why its writes are `MERGE`s
# MAGIC keyed on natural keys rather than appends.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %run ./silver_rules

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F


def table_count(table):
    return spark.table(table).count() if spark.catalog.tableExists(table) else 0


def ensure_table(df, table):
    """Create an empty Delta table with the DataFrame's schema if it does not exist yet."""
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)


counts_before = {t: table_count(t) for t in (T_SILVER, T_QUARANTINE, T_SILVER_CATALOG, T_SILVER_PRICES)}

# COMMAND ----------

# MAGIC %md ## Sales lines

# COMMAND ----------

if spark.catalog.tableExists(T_BRONZE):
    ensure_table(to_silver(validate(spark.table(T_BRONZE))), T_SILVER)
    checked = validate(spark.readStream.table(T_BRONZE))

    def merge_valid(batch_df, batch_id):
        batch = dedup(to_silver(batch_df.filter(F.size("failed_rules") == 0)))
        batch.createOrReplaceTempView("silver_batch")
        batch.sparkSession.sql(f"""
            MERGE INTO {T_SILVER} t
            USING silver_batch s
            ON t.line_id = s.line_id
            WHEN NOT MATCHED THEN INSERT *
        """)

    (
        checked.writeStream.foreachBatch(merge_valid)
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_sales")
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
    (
        checked.filter(F.size("failed_rules") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
        .writeStream.option("checkpointLocation", f"{CHECKPOINTS}/silver_quarantine")
        .trigger(availableNow=True)
        .toTable(T_QUARANTINE)
        .awaitTermination()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product catalog: flatten the nested JSON
# MAGIC Structs become columns (`attributes.category` -> `category`); the `price_history` array is exploded
# MAGIC into one row per price. `category`, `is_seasonal` and `tags` are **synthetic** (keyword rules on
# MAGIC the description, see `scripts/make_landing_batches.py`); `tags` only exists from catalog batch 3
# MAGIC on, so it is NULL for earlier products.


# COMMAND ----------


def flatten_catalog(df):
    tags = F.col("tags") if "tags" in df.columns else F.lit(None).cast("array<string>")
    return df.select(
        F.upper(F.trim("stock_code")).alias("stock_code"),
        F.col("name").alias("product_name"),
        F.col("attributes.category").alias("category"),
        F.col("attributes.is_seasonal").cast("boolean").alias("is_seasonal"),
        tags.alias("tags"),
        F.size("aliases").alias("alias_count"),
        F.to_date("first_sold_date").alias("first_sold_date"),
        F.size("price_history").alias("price_points"),
        "_source_file",
        "_ingested_at",
    )


def explode_prices(df):
    return df.select(
        F.upper(F.trim("stock_code")).alias("stock_code"), F.explode("price_history").alias("p"), "_ingested_at"
    ).select(
        "stock_code",
        F.to_date("p.valid_from").alias("valid_from"),
        F.col("p.unit_price").cast("decimal(12,3)").alias("unit_price"),
        "_ingested_at",
    )


if spark.catalog.tableExists(T_BRONZE_CATALOG):
    bronze_catalog = spark.table(T_BRONZE_CATALOG)
    ensure_table(flatten_catalog(bronze_catalog), T_SILVER_CATALOG)
    ensure_table(explode_prices(bronze_catalog), T_SILVER_PRICES)

    def merge_catalog(batch_df, batch_id):
        session = batch_df.sparkSession
        latest = (
            flatten_catalog(batch_df)
            .withColumn(
                "_rn",
                F.row_number().over(
                    Window.partitionBy("stock_code").orderBy(F.desc("_ingested_at"), F.desc("_source_file"))
                ),
            )
            .filter("_rn = 1")
            .drop("_rn")
        )
        latest.createOrReplaceTempView("catalog_batch")
        session.sql(f"""
            MERGE INTO {T_SILVER_CATALOG} t USING catalog_batch s ON t.stock_code = s.stock_code
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        """)
        explode_prices(batch_df).dropDuplicates(["stock_code", "valid_from"]).createOrReplaceTempView("price_batch")
        session.sql(f"""
            MERGE INTO {T_SILVER_PRICES} t USING price_batch s
            ON t.stock_code = s.stock_code AND t.valid_from = s.valid_from
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        """)

    (
        spark.readStream.table(T_BRONZE_CATALOG)
        .writeStream.foreachBatch(merge_catalog)
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_catalog")
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )

# COMMAND ----------

counts_after = {t: table_count(t) for t in counts_before}
new = {t.split(".")[-1]: counts_after[t] - counts_before[t] for t in counts_before}
n_bronze = table_count(T_BRONZE)
result = (
    f"bronze={n_bronze} silver={counts_after[T_SILVER]} (+{new['sales_lines']}) "
    f"quarantine={counts_after[T_QUARANTINE]} (+{new['sales_lines_quarantine']}) "
    f"duplicates_removed={n_bronze - counts_after[T_SILVER] - counts_after[T_QUARANTINE]} "
    f"catalog={counts_after[T_SILVER_CATALOG]} (+{new['product_catalog']}) "
    f"price_points={counts_after[T_SILVER_PRICES]} (+{new['product_price_history']})"
)
print(result)
dbutils.notebook.exit(result)

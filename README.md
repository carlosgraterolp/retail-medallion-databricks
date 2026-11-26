# Retail Medallion Pipeline on Databricks

An end-to-end batch pipeline on **Databricks Free Edition** that takes ~1.07M real e-commerce
invoice lines through a **Medallion architecture** (bronze → silver → gold), models the gold layer
as a **Kimball star schema**, validates it with automated **data quality checks**, and serves a small
**AI/BI dashboard**. Infrastructure, the job and the dashboard are defined as code with a
**Declarative Automation Bundle** (`databricks.yml`, formerly "Databricks Asset Bundles").

![Online Retail Sales dashboard](screenshots/dashboard.png)

Stack: PySpark, Spark SQL, Delta Lake, Unity Catalog, Lakeflow Jobs (serverless), Databricks CLI,
Declarative Automation Bundles, Databricks AI/BI dashboards.

## Problem

The UCI *Online Retail II* dataset holds every transaction of a UK-based online gift retailer between
2009-12-01 and 2011-12-09. The raw data has the problems real sources have: two files that overlap
in time, exact duplicate lines, cancellations mixed with sales, 23% of lines without a customer,
stock adjustments and bad-debt write-offs that are not sales, inconsistent casing and several
descriptions per product code. The goal is a trustworthy, query-ready model of sales, where every
rejected row can be accounted for.

## Architecture

```mermaid
flowchart LR
    A["UCI Online Retail II<br/>(.xlsx, 2 sheets)"] -->|scripts/prepare_data.py<br/>xlsx → CSV, values as text| B["UC Volume<br/>retail_bronze.landing"]
    B -->|01_bronze| C["Bronze<br/>online_retail_raw<br/>(strings + metadata)"]
    C -->|02_silver| D["Silver<br/>sales_lines"]
    C -->|02_silver| Q["Silver<br/>sales_lines_quarantine"]
    D -->|03_gold| E["Gold: star schema<br/>fact_sales + 4 dims"]
    E -->|03_gold| F["Gold aggregates"]
    E -->|04_quality_checks| G["dq_check_results<br/>(fails the job on error)"]
    F --> H["AI/BI dashboard"]
```

All of it runs as one Lakeflow Job with four serverless notebook tasks:
`bronze → silver → gold → quality_checks`.

## Results of the last full run

| Layer | Table | Rows |
|---|---|---|
| Bronze | `retail_bronze.online_retail_raw` | 1,067,371 (525,461 + 541,910 from the two sheets) |
| Silver | `retail_silver.sales_lines` | 1,027,015 |
| Silver | `retail_silver.sales_lines_quarantine` | 6,209 |
| (removed) | exact duplicates | 34,147 |
| Gold | `fact_sales` | 1,027,015 |
| Gold | `dim_product` / `dim_customer` / `dim_country` / `dim_date` | 4,760 / 5,940 / 44 / 1,095 |

`bronze = silver + quarantine + duplicates` (1,027,015 + 6,209 + 34,147 = 1,067,371) is asserted on
every run. **33 of 33** quality checks passed. End-to-end run time: about 2 minutes on serverless
compute (task durations: bronze 13 s, silver 20 s, gold 46 s, checks 31 s).

## Tables

All tables are Delta tables in Unity Catalog, catalog `workspace` (the default catalog in Free Edition).

### Bronze: `retail_bronze`
| Table | Grain | Key | Description |
|---|---|---|---|
| `online_retail_raw` | one row per source CSV row | none | All 8 source columns as **strings**, plus `_source_file` and `_ingested_at`. Only the column names are changed (snake_case), because Delta column names cannot contain spaces without column mapping. Full overwrite on every run. |

### Silver: `retail_silver`
| Table | Grain | Key | Description |
|---|---|---|---|
| `sales_lines` | one invoice line | `line_id` (SHA-256 of all business columns) | Typed, trimmed, upper-cased codes, valid and deduplicated. Flags: `is_cancelled`, `is_guest`, `is_non_merchandise`. `line_amount = quantity × unit_price` (negative for cancellations). |
| `sales_lines_quarantine` | one rejected source row | none | Every row that failed at least one rule, with the array `failed_rules` and `_quarantined_at`. |

### Gold: `retail_gold` (star schema)
| Table | Grain | Keys | Description |
|---|---|---|---|
| `fact_sales` | **one invoice line** | `line_id`; FKs `date_key`, `product_key`, `customer_key`, `country_key` | Measures: `quantity`, `unit_price`, `line_amount`. `invoice_no` is a degenerate dimension. Liquid clustering on `date_key`. |
| `dim_product` | one stock code | SK `product_key` (identity), NK `stock_code` | Latest description, `is_non_merchandise`, first/last sold date. SCD Type 1. |
| `dim_customer` | one customer | SK `customer_key` (identity), NK `customer_id` | Latest country, first/last purchase date. SCD Type 1. `-1` = guest/unknown. |
| `dim_country` | one country | SK `country_key` (identity), NK `country_name` | Country of the invoice (works for guest sales too). |
| `dim_date` | one calendar day | `date_key` (`yyyymmdd`) | Generated calendar 2009-01-01 to 2011-12-31. |
| `agg_monthly_revenue_by_country` | month × country | `year_month`, `country_name` | Gross sales, cancelled amount, net revenue, invoices, customers. |
| `agg_top_products` | one merchandise product | `product_key` | Net revenue, net units, invoices, `revenue_rank`. Postage, fees and adjustments are excluded. |
| `agg_monthly_cancellation_rate` | month | `year_month` | Gross sales, cancelled amount, `cancellation_rate = cancelled / gross`. |
| `dq_check_results` | check × run | `run_ts`, `check_name` | Audit trail of every quality check run (appended). |

Every dimension has an **unknown member** with key `-1`, so fact foreign keys are never NULL.

## Data quality rules

### Silver validation (a failing row goes to quarantine, with every rule it failed)
| Rule | Condition | Rows (last run) | Why |
|---|---|---|---|
| `zero_price` | `unit_price = 0` | 6,202 | Free items and stock adjustments, not sales |
| `negative_qty_not_cancelled` | `quantity < 0` on a non-`C` invoice | 3,457 | Stock write-offs ("damaged", "lost"…) |
| `invalid_invoice_no` | not `^C?[0-9]{6}$` | 6 | "A" invoices = bad-debt adjustments |
| `invalid_price` | NULL or `< 0` | 5 | Same bad-debt adjustments |
| `positive_qty_cancelled` | `quantity > 0` on a `C` invoice | 1 | Contradictory record |
| `missing_invoice_no`, `missing_stock_code`, `invalid_invoice_date`, `invalid_quantity`, `invalid_customer_id`, `missing_country` | NULL or unparseable | 0 | Type and completeness guards |

The counts overlap (one row can fail several rules); 6,209 distinct rows are quarantined.

### Handled explicitly, not rejected
- **Cancellations** (invoice starting with `C`, 19,103 lines in silver): kept and flagged with `is_cancelled`. They carry negative quantities, so net revenue nets them out and the cancellation rate measures them.
- **Missing Customer ID** (229,200 silver lines, 22%): kept as guest sales (`is_guest`), mapped to the `-1` "Unknown" customer in gold.
- **Duplicates** (34,147): exact duplicates on all business columns, mostly from the two source sheets overlapping on 2010-12-01 to 2010-12-09. *Assumption*: the source has no line number, so two identical lines on the same invoice at the same second are treated as one.
- **Non-merchandise codes** (POST, DOT, M, BANK CHARGES, AMAZONFEE, gift vouchers…): kept as revenue, flagged, and excluded from the top-products ranking.
- **Several descriptions per stock code**: `dim_product` keeps the latest non-empty one (SCD1).

### Quality checks (`04_quality_checks`, 33 checks)
- Row counts: bronze and silver not empty; silver + quarantine ≤ bronze; fact rows = silver rows.
- Silver rules enforced: no zero/negative prices, no zero quantity, no cancelled line with positive quantity, `line_id` unique.
- Null checks on all fact keys; uniqueness of every surrogate and natural key; unknown member present.
- Referential integrity: no orphan fact rows against any of the four dimensions; only guest lines point to the unknown customer.
- Revenue reconciliation: silver = fact = monthly-by-country aggregate = cancellation-rate aggregate.

Results are appended to `dq_check_results`. If any check fails, the notebook raises an
`AssertionError` and the job run fails. This was tested by inserting one orphan row into
`fact_sales`: 3 of 33 checks failed (row count, referential integrity, revenue) and the run was
marked FAILED. The next full run rebuilt the fact and all checks passed.

## How to deploy and run

Requirements: a Databricks Free Edition workspace, Databricks CLI v1.28.0 (the version this was built and tested with), Python 3
(only to convert the source file).

```bash
# 1. Authenticate (OAuth in the browser; credentials stay in ~/.databrickscfg)
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile retail

# 2. Get the data and convert the two Excel sheets to CSV (values kept as text)
mkdir -p data/raw
curl -L -o data/raw/online_retail_ii.zip "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
unzip -o data/raw/online_retail_ii.zip -d data/raw
python3 -m venv .venv && .venv/bin/pip install -r scripts/requirements.txt
.venv/bin/python scripts/prepare_data.py data/raw/online_retail_II.xlsx data/raw

# 3. Deploy: creates the 3 schemas, the landing volume, the job and the dashboard
databricks bundle deploy -p retail

# 4. Upload the CSVs to the volume (give the full target file name)
databricks fs mkdir dbfs:/Volumes/workspace/retail_bronze/landing/online_retail_ii -p retail
for f in data/raw/online_retail_ii_*.csv; do
  databricks fs cp "$f" "dbfs:/Volumes/workspace/retail_bronze/landing/online_retail_ii/$(basename "$f")" -p retail
done

# 5. Run the whole pipeline (or one task: --only silver)
databricks bundle run retail_medallion_pipeline -p retail

# 6. Open the dashboard (deployed and published in step 3)
databricks bundle open retail_sales -p retail
```

**Manual alternative (UI only):** import the `notebooks/` folder into the workspace, run
`00_setup`, upload the two CSVs to `workspace.retail_bronze.landing/online_retail_ii/` in Catalog
Explorer, then run `01_bronze` → `04_quality_checks` in order (each one runs `00_setup` first).

## Dashboard

The AI/BI dashboard is code: [`dashboards/retail_sales.lvdash.json`](dashboards/retail_sales.lvdash.json)
is declared as a `dashboards` resource in `databricks.yml`, so `databricks bundle deploy` creates (or
updates) and publishes it. Its queries use bare table names; the bundle supplies the catalog and
schema (`dataset_catalog`, `dataset_schema`) and looks up the SQL warehouse by name
(`Serverless Starter Warehouse`, the one Free Edition provides), so no IDs are committed.

It shows 3 counters for the whole period (net revenue in GBP, invoices, cancellation rate),
the monthly net revenue trend, the monthly cancellation rate, the top 10 products and the top 15
countries by net revenue. Details and the standalone SQL are in [`dashboards/README.md`](dashboards/README.md);
the screenshot is [`screenshots/dashboard.png`](screenshots/dashboard.png).

**Repo safety:** `git config core.hooksPath .githooks` enables a pre-commit hook
(`scripts/check_repo_safety.sh`) that blocks raw data files, local config, files over 1 MB and
common credential patterns. `.gitignore` excludes `data/`, `*.csv`, `*.xlsx` and `.databricks/`.

## Design decisions

- **Why Medallion.** Each layer has one job: bronze is a replayable copy of the source, silver is
  the single cleaned version of the truth at transaction level, gold is shaped for consumption. When
  a rule changes, silver and gold are rebuilt from bronze without touching the source.
- **Why Medallion *and* Kimball.** They answer different questions: Medallion organises *how data
  flows and gets refined*; Kimball decides *how the consumption layer is modelled*. Here the gold
  layer is the star schema.
- **Why this grain (one invoice line).** It is the most atomic level the source offers. Any
  aggregate (per invoice, per day, per customer) can be derived from it, but not the reverse.
- **Why quarantine instead of delete.** Silently dropped rows make data loss invisible. Quarantine
  keeps every rejected row with the reason, makes the loss measurable (6,209 rows, by rule) and lets
  a corrected rule replay them. The row reconciliation is asserted on every run.
- **Why surrogate keys.** They decouple the model from source codes, give a compact integer join key,
  let the model represent "unknown" (`-1`) instead of NULL foreign keys, and are the foundation for
  SCD Type 2 if history is needed later. Identity columns + `MERGE` keep them stable across reruns
  (verified: the key fingerprint of `dim_product` was identical before and after a full rerun).
- **Why SCD Type 1.** The source has no reliable effective dates for attribute changes, and the
  analysis needs current descriptions. SCD2 is listed as a next step.
- **Why a `dim_country`.** 22% of lines have no customer, so country cannot be reached through
  `dim_customer` for those sales; the invoice's country is its own dimension.
- **Why liquid clustering, not partitioning.** At ~1M rows, Hive-style partitions by date would
  create many small files. Clustering on `date_key` gives data skipping for date filters without
  that cost.
- **Why `try_cast`.** Serverless compute runs with ANSI mode on: a plain `cast` of one bad value
  fails the whole job. `try_cast` returns NULL so a validation rule can catch and quarantine it.
- **Why a Declarative Automation Bundle.** Schemas, the volume, the job (tasks, dependencies,
  parameters) and the dashboard live in Git next to the code. One `bundle deploy` recreates the setup in any
  workspace, and changes are reviewed like code.
- **Why convert xlsx → CSV locally.** Serverless Spark has no native Excel reader, and outbound
  internet from Free Edition compute is restricted. The conversion keeps every value as text.

## Limitations (what this project is not)

- Batch, full-refresh pipeline: bronze, silver and the fact are rebuilt on every run (no incremental
  loads or streaming).
- Dimensions are SCD Type 1 only; no attribute history.
- One `dev` bundle target; no CI/CD yet.
- `cancellation_rate` is the share of gross sales on cancellation invoices (`C…`). The source does
  not say why an invoice was cancelled, so it is not presented as a return rate.

## Dataset and license

Chen, D. (2012). *Online Retail II* [Dataset]. UCI Machine Learning Repository.
https://doi.org/10.24432/C5CG6D. Licensed under CC BY 4.0. Prices are in pounds sterling. The data
is not included in this repository.

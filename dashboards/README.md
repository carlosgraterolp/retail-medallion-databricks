# Dashboard: Online Retail Sales

Four widgets plus KPI counters, built in the Databricks UI (AI/BI dashboards) on the gold layer.
Each `.sql` file is one dataset. All queries were tested on the Free Edition serverless SQL warehouse.

| File | Dataset name | Widget |
|---|---|---|
| `01_kpis.sql` | `kpis` | Counters: net revenue, invoices, identified customers, cancellation rate |
| `02_monthly_revenue_trend.sql` | `monthly_revenue` | Line chart: x `month`, y `net_revenue` and `gross_sales` |
| `03_top_10_products.sql` | `top_products` | Horizontal bar: y `description`, x `net_revenue`, sorted by `revenue_rank` |
| `04_revenue_by_country.sql` | `revenue_by_country` | Bar chart: x `country_name`, y `net_revenue` (or a table with `revenue_share`) |
| `05_cancellation_rate.sql` | `cancellation_rate` | Line chart: x `month`, y `cancellation_rate` (format as percent) |

## Build it in the UI (about 10 minutes)

1. Run the pipeline first, so the gold tables exist.
2. In the workspace sidebar, open **Dashboards** and choose **Create dashboard**.
3. Open the **Data** tab. For each `.sql` file, choose **Create from SQL**, paste the query, run it,
   and rename the dataset to the name in the table above.
4. Open the canvas tab and add widgets:
   - **Counter** ×4 from `kpis`: `net_revenue`, `invoices`, `identified_customers`, `cancellation_rate`.
   - **Line** from `monthly_revenue`: X = `month` (monthly), Y = `net_revenue` and `gross_sales`.
   - **Bar** from `top_products`: Y = `description`, X = `net_revenue`.
   - **Bar** from `revenue_by_country`: X = `country_name`, Y = `net_revenue`.
     The UK is about 85% of revenue, so a second widget filtered to `country_name <> 'United Kingdom'`
     makes the other countries readable.
   - **Line** from `cancellation_rate`: X = `month`, Y = `cancellation_rate`.
5. Add a text box: "Source: UCI Online Retail II (CC BY 4.0). December 2011 is a partial month (data ends 2011-12-09)."
6. Choose **Publish**. The dashboard uses the Serverless Starter Warehouse.
7. Take screenshots and save them in `../screenshots/` (e.g. `dashboard.png`).

Values from the last run, to sanity-check the widgets: net revenue £19,002,774.21, gross sales
£20,465,198.39, 40,076 invoices, 5,939 identified customers, cancellation rate 7.15%,
top product "REGENCY CAKESTAND 3 TIER" (£314,045.02).

# Dashboard: Online Retail Sales

The dashboard is defined as code in [`retail_sales.lvdash.json`](retail_sales.lvdash.json) and
deployed with the bundle (`dashboards.retail_sales` in [`../databricks.yml`](../databricks.yml)). The
deploy workflow creates or updates and publishes it on every push to `main` (target `prod`, as the
`github-ci` service principal). A copy owned by you can be deployed by hand with
`databricks bundle deploy -t dev -p <profile>` and opened with
`databricks bundle open retail_sales -t dev -p <profile>`.

- Warehouse: looked up by name (`Serverless Starter Warehouse`, the SQL warehouse Free Edition provides).
- Catalog and schema: supplied by the bundle (`dataset_catalog: workspace`, `dataset_schema: retail_gold`),
  so the queries in the JSON use bare table names.
- To change it: edit the JSON (or edit a `dev` copy in the UI and pull it back with
  `databricks bundle generate dashboard --resource retail_sales --force -t dev -p <profile>`), then
  open a pull request; the merge deploys it. A UI edit that is not pulled back into the JSON is
  overwritten by the next `bundle deploy`.

## Layout

| Row | Widget | Type | Dataset | Notes |
|---|---|---|---|---|
| 1 | Title and source note | text | | Amounts in GBP; December 2011 is a partial month |
| 2 | Net revenue | counter | `ds_sales` | Whole period, GBP, compact |
| 2 | Invoices | counter | `ds_sales` | Whole period; sales invoices, cancellations excluded |
| 2 | Cancellation rate | counter | `ds_sales` | Dataset measure `-SUM(cancelled_amount) / SUM(gross_sales)` |
| 3 | Monthly net revenue | line | `ds_sales` | |
| 3 | Monthly cancellation rate | line | `ds_sales` | Same measure, per month; the Jan 2011 and Dec 2011 spikes are single bulk orders that were cancelled |
| 4 | Top 10 products by net revenue | horizontal bar | `ds_top_products` | Merchandise only; labels prefixed with the rank so the bars sort by revenue |
| 4 | Net revenue by country (top 15) | horizontal bar | `ds_countries` | Country on the invoice |

`ds_sales` (month × country from `agg_monthly_revenue_by_country`) backs the counters and both
trend charts, so they share one dataset. The counters have no sparkline on purpose: with a
sparkline, an AI/BI counter shows the latest month instead of the total. The cancellation rate is a dataset-level measure, so it is
computed as a ratio of sums at every level instead of averaging monthly ratios.

## Standalone SQL

The `.sql` files are the same metrics as fully qualified queries, for the SQL editor or another BI
tool (each was run on the Free Edition SQL warehouse):

| File | Returns |
|---|---|
| `01_kpis.sql` | Net revenue, gross sales, invoices, identified customers, cancellation rate |
| `02_monthly_revenue_trend.sql` | Gross sales and net revenue per month |
| `03_top_10_products.sql` | Top 10 merchandise products by net revenue |
| `04_revenue_by_country.sql` | Net revenue and revenue share per country |
| `05_cancellation_rate.sql` | Gross sales, cancelled amount and cancellation rate per month |

Values from the last run, to sanity-check the widgets: net revenue £19,002,774.21, 40,076 invoices,
cancellation rate 7.15%, top product "REGENCY CAKESTAND 3 TIER" (£314,045.02), United Kingdom
about 85% of net revenue.

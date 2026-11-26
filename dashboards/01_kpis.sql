-- KPI counters (whole period). Dataset: "kpis"
SELECT
  sum(line_amount)                                                   AS net_revenue,
  sum(CASE WHEN NOT is_cancelled THEN line_amount END)               AS gross_sales,
  count(DISTINCT CASE WHEN NOT is_cancelled THEN invoice_no END)     AS invoices,
  count(DISTINCT CASE WHEN customer_key <> -1 THEN customer_key END) AS identified_customers,
  round(-sum(CASE WHEN is_cancelled THEN line_amount END)
        / sum(CASE WHEN NOT is_cancelled THEN line_amount END), 4)   AS cancellation_rate
FROM workspace.retail_gold.fact_sales;

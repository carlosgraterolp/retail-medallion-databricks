-- Monthly cancellation rate = cancelled amount / gross sales.
-- Dataset: "cancellation_rate". Line or bar chart: x = month, y = cancellation_rate
SELECT
  to_date(year_month || '-01') AS month,
  gross_sales,
  cancelled_amount,
  cancellation_rate
FROM workspace.retail_gold.agg_monthly_cancellation_rate
ORDER BY month;

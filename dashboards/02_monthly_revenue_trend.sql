-- Monthly revenue trend. Dataset: "monthly_revenue". Line chart: x = month, y = net_revenue, gross_sales
-- Note: the source ends on 2011-12-09, so December 2011 is a partial month.
SELECT
  to_date(year_month || '-01') AS month,
  sum(gross_sales)             AS gross_sales,
  sum(net_revenue)             AS net_revenue
FROM workspace.retail_gold.agg_monthly_revenue_by_country
GROUP BY year_month
ORDER BY month;

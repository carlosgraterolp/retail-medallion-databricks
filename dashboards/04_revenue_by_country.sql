-- Net revenue by country (country on the invoice, so guest sales are included).
-- Dataset: "revenue_by_country". Bar chart (or table): x = country_name, y = net_revenue
SELECT
  country_name,
  sum(net_revenue)                                          AS net_revenue,
  round(sum(net_revenue) / sum(sum(net_revenue)) OVER (), 4) AS revenue_share
FROM workspace.retail_gold.agg_monthly_revenue_by_country
GROUP BY country_name
ORDER BY net_revenue DESC;

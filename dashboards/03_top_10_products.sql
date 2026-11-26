-- Top 10 merchandise products by net revenue (postage, fees and adjustments excluded).
-- Dataset: "top_products". Bar chart: y = description, x = net_revenue
SELECT revenue_rank, stock_code, description, net_revenue, net_units, invoices
FROM workspace.retail_gold.agg_top_products
WHERE revenue_rank <= 10
ORDER BY revenue_rank;

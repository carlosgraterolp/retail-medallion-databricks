-- Revenue by synthetic category must add up to the merchandise revenue of the product mart.
with c as (select sum(net_revenue) as r from {{ ref('mart_category_performance') }}),
     p as (select sum(net_revenue) as r from {{ ref('mart_product_performance') }})
select c.r as category_revenue, p.r as product_revenue
from c cross join p
where abs(c.r - p.r) >= 0.01

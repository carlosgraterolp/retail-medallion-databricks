-- Revenue shares must add up to 100%, and the cumulative share on the last product of the
-- ranking must be 100%. (The maximum can be above 100%: products with negative net revenue,
-- more cancelled than sold, sit at the bottom and pull the running total back down.)
with last_row as (
    select cumulative_revenue_share
    from {{ ref('mart_product_performance') }}
    order by net_revenue asc, stock_code desc
    limit 1
)
select sum(p.revenue_share) as total_share, max(l.cumulative_revenue_share) as final_cumulative
from {{ ref('mart_product_performance') }} p
cross join last_row l
having abs(sum(p.revenue_share) - 1) > 1e-6 or abs(max(l.cumulative_revenue_share) - 1) > 1e-6

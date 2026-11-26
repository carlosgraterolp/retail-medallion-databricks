-- One row per merchandise product that appears in sales (postage, fees and adjustments excluded).
with product_sales as (
    select
        s.product_key,
        sum(s.gross_amount)                                            as gross_sales,
        sum(s.cancelled_amount)                                        as cancelled_amount,
        sum(s.net_amount)                                              as net_revenue,
        sum(s.quantity)                                                as net_units,
        count(distinct case when not s.is_cancelled then s.invoice_no end) as invoices
    from {{ ref('stg_sales') }} s
    group by s.product_key
),

merchandise as (
    select p.product_key, p.stock_code, p.description, ps.*
    except (product_key)
    from product_sales ps
    join {{ source('gold', 'dim_product') }} p using (product_key)
    where not p.is_non_merchandise and p.product_key <> -1
)

select
    product_key,
    stock_code,
    description,
    gross_sales,
    cancelled_amount,
    net_revenue,
    net_units,
    invoices,
    cancelled_amount / nullif(gross_sales, 0)                          as cancellation_rate,
    rank() over (order by net_revenue desc)                            as revenue_rank,
    net_revenue / sum(net_revenue) over ()                             as revenue_share,
    sum(net_revenue) over (order by net_revenue desc, stock_code
        rows between unbounded preceding and current row)
        / sum(net_revenue) over ()                                     as cumulative_revenue_share,
    -- Pareto flag: the product is needed to reach 80% of revenue (the share before it is < 80%).
    sum(net_revenue) over (order by net_revenue desc, stock_code
        rows between unbounded preceding and 1 preceding)
        / sum(net_revenue) over () < 0.8
        or row_number() over (order by net_revenue desc, stock_code) = 1 as is_top_80pct_revenue
from merchandise

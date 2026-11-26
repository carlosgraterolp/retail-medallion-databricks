-- One row per SYNTHETIC product category (keyword rules on the description, not real
-- merchandising data). Demonstrates joining the semi-structured catalog to the star schema;
-- the figures are not business findings.
with lines as (
    select
        coalesce(c.synthetic_category, 'Not in catalog') as synthetic_category,
        p.stock_code,
        s.gross_amount,
        s.cancelled_amount,
        s.net_amount,
        s.quantity
    from {{ ref('stg_sales') }} s
    join {{ source('gold', 'dim_product') }} p using (product_key)
    left join {{ ref('stg_product_catalog') }} c on c.stock_code = p.stock_code
    where not p.is_non_merchandise and p.product_key <> -1
)

select
    synthetic_category,
    count(distinct stock_code)                         as products,
    sum(gross_amount)                                  as gross_sales,
    sum(cancelled_amount)                              as cancelled_amount,
    sum(net_amount)                                    as net_revenue,
    sum(quantity)                                      as net_units,
    sum(cancelled_amount) / nullif(sum(gross_amount), 0) as cancellation_rate,
    sum(net_amount) / sum(sum(net_amount)) over ()     as revenue_share
from lines
group by synthetic_category

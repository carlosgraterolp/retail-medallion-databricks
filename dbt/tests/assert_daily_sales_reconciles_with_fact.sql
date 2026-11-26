-- Net revenue in mart_daily_sales must equal net revenue in fact_sales (to the penny),
-- over the same date range (the cutoff var limits both).
with mart as (
    select coalesce(sum(net_revenue), 0) as revenue from {{ ref('mart_daily_sales') }}
),
fact as (
    select coalesce(sum(line_amount), 0) as revenue
    from {{ source('gold', 'fact_sales') }}
    {% if var('cutoff_date') %}
    where to_date(invoice_ts) <= date'{{ var("cutoff_date") }}'
    {% endif %}
)
select mart.revenue as mart_revenue, fact.revenue as fact_revenue
from mart cross join fact
where abs(mart.revenue - fact.revenue) >= 0.01

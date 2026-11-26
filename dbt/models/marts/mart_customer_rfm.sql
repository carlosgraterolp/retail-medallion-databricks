-- One row per identified customer (guests excluded). See docs: rfm_rules.
with customer_activity as (
    select
        customer_key,
        max(case when not is_cancelled then sale_date end)         as last_purchase_date,
        count(distinct case when not is_cancelled then invoice_no end) as frequency,
        sum(net_amount)                                            as monetary,
        sum(gross_amount)                                          as gross_sales,
        sum(cancelled_amount)                                      as cancelled_amount
    from {{ ref('stg_sales') }}
    where not is_guest
    group by customer_key
),

with_recency as (
    select
        *,
        datediff(date'{{ var("rfm_snapshot_date") }}', last_purchase_date) as recency_days,
        monetary > 0                                                     as is_scored
    from customer_activity
),

-- percent_rank() gives tied values the same rank, so equal R, F or M values always get
-- the same score (NTILE would split a tie across buckets arbitrarily).
-- Only customers with positive net value are ranked.
ranked as (
    select
        *,
        case when is_scored then percent_rank() over (
            partition by is_scored order by recency_days desc) end as r_pct,
        case when is_scored then percent_rank() over (
            partition by is_scored order by frequency) end         as f_pct,
        case when is_scored then percent_rank() over (
            partition by is_scored order by monetary) end          as m_pct
    from with_recency
),

scored as (
    select
        *,
        -- Spark's least() skips NULLs (least(5, NULL) = 5), so unscored customers need the CASE.
        case when is_scored then least(5, 1 + floor(r_pct * 5)) end as r_score,
        case when is_scored then least(5, 1 + floor(f_pct * 5)) end as f_score,
        case when is_scored then least(5, 1 + floor(m_pct * 5)) end as m_score
    from ranked
)

select
    s.customer_key,
    c.customer_id,
    c.customer_country,
    s.last_purchase_date,
    s.recency_days,
    s.frequency,
    s.monetary,
    s.gross_sales,
    s.cancelled_amount,
    cast(s.r_score as int) as r_score,
    cast(s.f_score as int) as f_score,
    cast(s.m_score as int) as m_score,
    case
        when not s.is_scored                       then 'No net value'
        when s.r_score >= 4 and s.f_score >= 4     then 'Champions'
        when s.r_score >= 3 and s.f_score >= 3     then 'Loyal'
        when s.r_score >= 4                        then 'New'
        when s.r_score = 3                         then 'Potential loyalist'
        when s.f_score >= 3                        then 'At risk'
        else 'Lost'
    end as segment
from scored s
join {{ ref('stg_customers') }} c using (customer_key)

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['sale_date', 'country'],
        on_schema_change='fail'
    )
}}
-- One row per day and invoice country.
-- Incremental: each run reprocesses every day from (last loaded date - lookback_days) through the
-- newest date, inclusive. With lookback_days = 3 that is 4 already-loaded days (the last loaded
-- date and the 3 days before it) plus any new days, MERGEd on (sale_date, country).
-- var('cutoff_date') simulates data arriving later.
select
    s.sale_date,
    g.country_name                                                     as country,
    concat(cast(s.sale_date as string), '|', g.country_name)           as daily_key,
    sum(s.gross_amount)                                                as gross_sales,
    sum(s.cancelled_amount)                                            as cancelled_amount,
    sum(s.net_amount)                                                  as net_revenue,
    count(distinct case when not s.is_cancelled then s.invoice_no end) as invoices,
    count(distinct case when not s.is_guest then s.customer_key end)   as customers,
    count(*)                                                           as lines,
    current_timestamp()                                                as _processed_at
from {{ ref('stg_sales') }} s
join {{ source('gold', 'dim_country') }} g using (country_key)
where 1 = 1
{% if var('cutoff_date') %}
  and s.sale_date <= date'{{ var("cutoff_date") }}'
{% endif %}
{% if is_incremental() %}
  and s.sale_date >= (
      select date_sub(max(sale_date), {{ var('lookback_days') }}) from {{ this }}
  )
{% endif %}
group by s.sale_date, g.country_name

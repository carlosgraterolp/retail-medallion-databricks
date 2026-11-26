-- One row per invoice line, with business names and the sale date.
-- Adds value over the raw fact: friendlier names, a DATE column for daily grains,
-- and explicit gross / cancelled amounts so every mart uses the same definitions.
select
    line_id,
    invoice_no,
    to_date(invoice_ts)                                    as sale_date,
    invoice_ts                                             as invoiced_at,
    product_key,
    customer_key,
    country_key,
    customer_key = -1                                      as is_guest,
    is_cancelled,
    quantity,
    unit_price,
    line_amount                                            as net_amount,
    case when not is_cancelled then line_amount else 0 end as gross_amount,
    case when is_cancelled then -line_amount else 0 end    as cancelled_amount
from {{ source('gold', 'fact_sales') }}

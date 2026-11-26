-- Every identified (non-guest) customer that appears in fact_sales must have a row in the RFM mart.
select distinct f.customer_key
from {{ source('gold', 'fact_sales') }} f
left anti join {{ ref('mart_customer_rfm') }} r on f.customer_key = r.customer_key
where f.customer_key <> -1

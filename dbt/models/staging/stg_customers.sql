-- Identified customers only: drops the -1 "guest/unknown" member, which is a modelling
-- device of the star schema and not a customer.
select
    customer_key,
    customer_id,
    country as customer_country
from {{ source('gold', 'dim_customer') }}
where customer_key <> -1

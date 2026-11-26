-- One row per merchandise product from the JSON catalog feed.
-- category, is_seasonal and tags are SYNTHETIC: derived with keyword rules on the product
-- description (scripts/make_landing_batches.py). They exist to demonstrate semi-structured
-- ingestion and must not be read as real merchandising data.
select
    stock_code,
    product_name,
    category       as synthetic_category,
    is_seasonal    as synthetic_is_seasonal,
    tags           as synthetic_tags,
    price_points,
    first_sold_date
from {{ source('silver', 'product_catalog') }}

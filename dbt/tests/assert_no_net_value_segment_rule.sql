-- 'No net value' must be exactly the customers with monetary <= 0, and they must not be scored.
select customer_key, monetary, segment, r_score
from {{ ref('mart_customer_rfm') }}
where (segment = 'No net value') <> (monetary <= 0)
   or (segment = 'No net value' and coalesce(r_score, f_score, m_score) is not null)

{% docs rfm_rules %}
**RFM rules** (customer level, identified customers only; guests have no customer id).

- **Recency**: days from the purchase date of the last non-cancelled invoice to the analysis date
  `rfm_snapshot_date` (2011-12-10, the day after the last invoice). Lower is better.
- **Frequency**: number of distinct non-cancelled invoices.
- **Monetary**: net revenue in GBP (sales minus cancellations).
- **Scores 1-5**: `score = least(5, 1 + floor(percent_rank * 5))`, ranking recency descending (most
  recent = 5) and frequency / monetary ascending (highest = 5). `percent_rank` gives tied values the
  same rank, so customers with equal values always get the same score. NTILE(5) would put
  customers with the same frequency (for example the many one-invoice customers) into different
  buckets arbitrarily. Consequence: score groups are not equal-sized.
- **Customers with monetary <= 0** (only cancellations, or purchases fully reversed) are not
  scored: their scores are NULL and their segment is `No net value`. They are kept so every
  identified customer of fact_sales appears in the mart.
- **Segments** (first rule that matches, on R and F; M is reported for prioritisation and is
  strongly correlated with F):

| Segment | Rule |
|---|---|
| No net value | monetary <= 0 |
| Champions | R >= 4 and F >= 4 |
| Loyal | R >= 3 and F >= 3 |
| New | R >= 4 (and F <= 2) |
| Potential loyalist | R = 3 (and F <= 2) |
| At risk | R <= 2 and F >= 3 |
| Lost | R <= 2 and F <= 2 |
{% enddocs %}

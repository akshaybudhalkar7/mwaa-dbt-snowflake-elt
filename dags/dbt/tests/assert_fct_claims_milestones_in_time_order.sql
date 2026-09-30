-- Accumulating snapshot sanity (ERROR): a milestone can't happen before the claim was
-- reported, and a loss can't be reported before it happened. Any row here = broken data.
select
    claim_id,
    loss_date_key,
    reported_date_key,
    approved_date_key,
    rejected_date_key,
    paid_date_key
from {{ ref('fct_claims') }}
where loss_date_key     > reported_date_key
   or approved_date_key < reported_date_key
   or rejected_date_key < reported_date_key
   or paid_date_key     < reported_date_key
   or (approved_date_key is not null and rejected_date_key is not null)

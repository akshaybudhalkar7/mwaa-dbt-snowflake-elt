{{ config(severity='warn') }}
-- Business rule (WARN): a claim should only be PAID after it was APPROVED.
-- Known source issue: the policy admin system sends payments for rejected / undecided claims
-- (9 + 14 on 2026-09-30). Reported on every run for the source team, but not blocking:
-- the fact faithfully records what the source says.
select
    claim_id,
    current_status,
    approved_date_key,
    rejected_date_key,
    paid_date_key
from {{ ref('fct_claims') }}
where paid_date_key is not null
  and (approved_date_key is null or rejected_date_key is not null)

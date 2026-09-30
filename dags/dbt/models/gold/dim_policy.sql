{#
    Policy dimension, SCD Type 1: one row per policy, current attributes only.
    FULL REFRESH (gold default). Policies don't change after issue in this source, so
    there is no history to keep.
    Descriptive attributes only - the premium is a MEASURE and lives in fct_policies_written.
#}
select
    md5(policy_id)                                   as policy_sk,
    policy_id,
    product_line,
    policy_status,
    effective_date,
    expiration_date,
    datediff(day, effective_date, expiration_date)   as term_days
from {{ ref('slv_policies') }}

union all

-- Unknown member: claims whose policy isn't loaded point here instead of being dropped
select '-1', 'UNKNOWN', 'Unknown', 'Unknown', null, null, null

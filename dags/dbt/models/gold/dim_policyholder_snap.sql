{#
    Policyholder dimension, SCD Type 2, built from the dbt SNAPSHOT (snap_api_policyholders).
    Same columns as dim_policyholder, so the two SCD2 approaches can be compared 1:1.

    FULL REFRESH is safe: the history lives in the snapshot table, and dbt never
    rebuilds snapshots (--full-refresh ignores them).
#}
select
    dbt_scd_id                                   as policyholder_sk,   -- dbt's hash of key + updated_at
    policyholder_id,
    first_name,
    last_name,
    date_of_birth,
    city,
    state,
    risk_tier,
    dbt_valid_from                               as valid_from,
    dbt_valid_to                                 as valid_to,          -- 9999-12-31 = current (snapshot config)
    dbt_valid_to = '9999-12-31'::timestamp_ntz   as is_current
from {{ ref('snap_api_policyholders') }}

union all

-- Unknown member (same as dim_policyholder)
select
    '-1', -1, 'Unknown', 'Unknown', null, 'Unknown', 'Unknown', 'Unknown',
    '1900-01-01'::timestamp_ntz, '9999-12-31'::timestamp_ntz, true

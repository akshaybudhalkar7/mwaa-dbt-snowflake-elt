{#
    Current state of every policyholder: the latest version from the version log.
    Grain: policyholder_id. Input for the dbt snapshot (snap_api_policyholders), which
    compares it with its last run to record changes as SCD Type 2.
#}
{{ config(materialized='view') }}

select
    policyholder_id,
    first_name,
    last_name,
    date_of_birth,
    city,
    state,
    risk_tier,
    updated_at
from {{ ref('slv_policyholder_versions') }}
qualify row_number() over (partition by policyholder_id order by updated_at desc) = 1

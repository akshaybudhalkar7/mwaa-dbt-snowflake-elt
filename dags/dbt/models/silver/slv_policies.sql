{#
    One row per policy. Policies don't change after issue in this source, so the grain is
    policy_id; buffer/retry duplicates collapse to one row. If the source ever did change a
    policy (e.g. CANCELLED), the MERGE on policy_id keeps only the latest -> SCD Type 1.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='policy_id',
        on_schema_change='fail',
    )
}}

with source as (

    select * from {{ ref('brz_api__policies') }}

    {% if is_incremental() %}
    -- Only rows Snowpipe loaded since the last build (+3h lookback, see slv_policyholder_versions)
    where loaded_at > (select dateadd(hour, -3, max(loaded_at)) from {{ this }})
    {% endif %}

),

typed as (

    select
        record:policy_id::string              as policy_id,
        record:policyholder_id::integer       as policyholder_id,
        record:product_line::string           as product_line,
        record:annual_premium::number(12, 2)  as annual_premium,
        record:effective_date::date           as effective_date,
        record:expiration_date::date          as expiration_date,
        record:status::string                 as policy_status,
        record:issued_at::timestamp_ntz       as issued_at,
        run_id,
        source_file,
        loaded_at
    from source

)

select * from typed
-- One row per policy: latest version wins, deterministic tie-break on the load
qualify row_number() over (
    partition by policy_id
    order by issued_at desc, loaded_at desc, source_file desc
) = 1

{#
    Every status change of every claim: OPEN -> APPROVED/REJECTED -> PAID.
    Grain: one row per (claim_id, updated_at). fct_claims (gold) builds the claim's
    milestone dates from these events.

    Partition by claim_id (NOT policy_id): one policy can have several claims.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['claim_id', 'updated_at'],
        on_schema_change='fail',
    )
}}

with source as (

    select * from {{ ref('brz_api__claims') }}

    {% if is_incremental() %}
    -- Only rows Snowpipe loaded since the last build (+3h lookback, see slv_policyholder_versions)
    where loaded_at > (select dateadd(hour, -3, max(loaded_at)) from {{ this }})
    {% endif %}

),

typed as (

    select
        record:claim_id::string             as claim_id,
        record:policy_id::string            as policy_id,
        record:loss_date::date              as loss_date,
        record:reported_date::date          as reported_date,
        record:claim_amount::number(12, 2)  as claim_amount,
        record:status::string               as claim_status,
        record:updated_at::timestamp_ntz    as updated_at,
        run_id,
        source_file,
        loaded_at
    from source

)

select * from typed
-- Same event delivered twice (buffer / retry) -> keep the latest load
qualify row_number() over (
    partition by claim_id, updated_at
    order by loaded_at desc, source_file desc
) = 1

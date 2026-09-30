{#
    Every version of every policyholder, typed and deduplicated.
    Grain: one row per (policyholder_id, updated_at).

    Duplicates come from the 2h watermark buffer (the same version is pulled twice) and from
    retried extractor attempts. Real changes have a new updated_at, so they are kept.
#}

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['policyholder_id','updated_at'],
        on_schema_change = 'fail'

    )
}}


with source as 
(
    select * from {{ref('brz_api__policyholders')}}
    {% if is_incremental() %}
    where loaded_at > (select dateadd(hour, -3, max(loaded_at)) from {{ this }})
    {% endif %}

),

typed as (

        select
        record:policyholder_id::integer    as policyholder_id,
        record:first_name::string          as first_name,
        record:last_name::string           as last_name,
        record:date_of_birth::date         as date_of_birth,
        record:city::string                as city,
        record:state::string               as state,
        record:risk_tier::string           as risk_tier,
        record:updated_at::timestamp_ntz   as updated_at,
        run_id,
        source_file,
        loaded_at
    from source

)

select * from typed
-- Same version delivered twice -> keep the latest load (deterministic tie-break on the file)
qualify row_number() over (
    partition by policyholder_id, updated_at
    order by loaded_at desc, source_file desc
) = 1





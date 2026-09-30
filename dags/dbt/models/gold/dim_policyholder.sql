{#
    Policyholder dimension, SCD Type 2, built from the version log (slv_policyholder_versions).
    Grain: one row per customer VERSION. A version is valid in [valid_from, valid_to);
    the current version has valid_to = 9999-12-31.

    FULL REFRESH is safe: the history lives in the version log, so every rebuild recomputes
    the same windows (plus any new versions). Same inputs -> same md5 keys.
#}
with versions as (

    select * from {{ ref('slv_policyholder_versions') }}

),

windowed as (

    select
        -- Surrogate key: one per VERSION. to_char fixes the timestamp format, so the hash never
        -- depends on the session's display settings.
        md5(policyholder_id || '~' || to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS')) as policyholder_sk,
        policyholder_id,
        first_name,
        last_name,
        date_of_birth,
        city,
        state,
        risk_tier,
        updated_at as valid_from,
        -- valid until the NEXT version of the same customer starts; the last one never ends
        coalesce(
            lead(updated_at) over (partition by policyholder_id order by updated_at),
            '9999-12-31'::timestamp_ntz
        ) as valid_to
    from versions

)

select
    policyholder_sk,
    policyholder_id,
    first_name,
    last_name,
    date_of_birth,
    city,
    state,
    risk_tier,
    valid_from,
    valid_to,
    valid_to = '9999-12-31'::timestamp_ntz as is_current
from windowed

union all

-- Unknown member: facts whose policyholder isn't loaded point here instead of being dropped
select
    '-1', -1, 'Unknown', 'Unknown', null, 'Unknown', 'Unknown', 'Unknown',
    '1900-01-01'::timestamp_ntz, '9999-12-31'::timestamp_ntz, true

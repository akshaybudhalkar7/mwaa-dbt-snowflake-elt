with source as (

    select * from {{ source('policy_admin', 'policyholders') }}

),

renamed as (

    select
        record:policyholder_id::integer   as policyholder_id,
        record:first_name::string         as first_name,
        record:last_name::string          as last_name,
        record:date_of_birth::date        as date_of_birth,
        record:city::string               as city,
        record:state::string              as state,
        record:risk_tier::string          as risk_tier,
        record:updated_at::timestamp_ntz  as updated_at,
        business_date,
        source_file,
        loaded_at

    from source

)

select * from renamed
qualify row_number() over (partition by policyholder_id, business_date order by loaded_at desc) = 1

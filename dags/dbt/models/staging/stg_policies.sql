with source as (

    select * from {{ source('policy_admin', 'policies') }}

),

renamed as (

    select
        record:policy_id::string              as policy_id,
        record:policyholder_id::integer       as policyholder_id,
        record:product_line::string           as product_line,
        record:annual_premium::number(12, 2)  as annual_premium,
        record:effective_date::date           as effective_date,
        record:expiration_date::date          as expiration_date,
        record:status::string                 as policy_status,
        record:issued_at::timestamp_ntz       as issued_at,
        business_date,
        source_file,
        loaded_at

    from source

)

select * from renamed
-- A policy is issued once -> one row per policy (drops Snowpipe duplicate loads)
qualify row_number() over (partition by policy_id order by loaded_at desc) = 1

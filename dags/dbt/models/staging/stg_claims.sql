with source as (
    select * from {{ source('policy_admin','claims') }}
),

renamed as (
    select
        record:claim_id::string            as claim_id,
        record:policy_id::string           as policy_id,
        record:loss_date::date             as loss_date,
        record:reported_date::date         as reported_date,
        record:claim_amount::number(12, 2) as claim_amount,
        record:status::string              as claim_status,
        record:updated_at::timestamp_ntz   as updated_at,
        business_date,
        source_file,
        loaded_at

    from source

)

select * from renamed
qualify row_number() over (partition by policy_id order by loaded_at desc) = 1


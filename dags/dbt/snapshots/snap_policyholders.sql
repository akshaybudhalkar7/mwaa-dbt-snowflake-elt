{#
    SCD Type 2 history of policyholders. The source only sends the CURRENT state (a daily full
    snapshot), so this snapshot records every change with dbt_valid_from / dbt_valid_to.
    Airflow passes business_date (= ds) so backfills build history day by day, in order.
#}
{% snapshot snap_policyholders %}

{{
    config(
        unique_key='policyholder_id',
        strategy='timestamp',
        updated_at='updated_at',
        dbt_valid_to_current="to_date('9999-12-31')",
    )
}}

select
    policyholder_id,
    first_name,
    last_name,
    date_of_birth,
    city,
    state,
    risk_tier,
    updated_at
from {{ ref('stg_policyholders') }}
where business_date =
    {% if var('business_date', none) %}
        '{{ var("business_date") }}'
    {% else %}
        (select max(business_date) from {{ ref('stg_policyholders') }})
    {% endif %}

{% endsnapshot %}

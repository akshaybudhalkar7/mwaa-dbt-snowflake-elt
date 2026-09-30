{#
    SCD Type 2 of policyholders via dbt snapshot (v2 pipeline).
    Each run compares the CURRENT state (slv_policyholders) with the snapshot's current rows:
    a newer updated_at closes the old row (dbt_valid_to) and inserts the new one.

    Trade-off vs dim_policyholder (built from the version log): a snapshot only sees the
    state at the moment it runs. History starts at the first snapshot run, and if one run
    brings several changes of one customer, only the last becomes a row.
#}
{% snapshot snap_api_policyholders %}

{{
    config(
        unique_key='policyholder_id',
        strategy='timestamp',
        updated_at='updated_at',
        dbt_valid_to_current="to_timestamp_ntz('9999-12-31')",
    )
}}

select * from {{ ref('slv_policyholders') }}

{% endsnapshot %}

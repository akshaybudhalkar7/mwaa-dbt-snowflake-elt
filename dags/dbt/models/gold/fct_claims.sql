{#
    Fact: claims as an ACCUMULATING SNAPSHOT. Grain: one row per claim.
    The row is UPDATED as the claim moves OPEN -> APPROVED/REJECTED -> PAID: milestone
    dates fill in over time, like a checklist.

    INCREMENTAL MERGE on claim_id: each run rebuilds only the claims that got new events,
    from ALL of their events (so earlier milestone dates are never lost).
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='claim_id',
        on_schema_change='fail',
    )
}}

with events as (

    select * from {{ ref('slv_claim_events') }}

),

{% if is_incremental() %}
-- Claims that got a new event since the last build (3h lookback)
changed_claims as (

    select distinct claim_id
    from events
    where loaded_at > (select dateadd(hour, -3, max(last_loaded_at)) from {{ this }})

),
{% endif %}

claim_events as (

    -- ALL events of the changed claims (first run: all claims)
    select events.*
    from events
    {% if is_incremental() %}
    inner join changed_claims on changed_claims.claim_id = events.claim_id
    {% endif %}

),

claims as (

    -- Pivot: many status events -> one row per claim with a timestamp per milestone
    select
        claim_id,
        any_value(policy_id)                                          as policy_id,
        any_value(loss_date)                                          as loss_date,
        any_value(reported_date)                                      as reported_date,
        max_by(claim_amount, updated_at)                              as claim_amount,    -- latest
        max_by(claim_status, updated_at)                              as current_status,  -- latest
        min(case when claim_status = 'APPROVED' then updated_at end)  as approved_at,
        min(case when claim_status = 'REJECTED' then updated_at end)  as rejected_at,
        min(case when claim_status = 'PAID'     then updated_at end)  as paid_at,
        max(updated_at)                                               as last_updated_at,
        max(loaded_at)                                                as last_loaded_at
    from claim_events
    group by claim_id

)

select
    c.claim_id,                                                            -- degenerate dimension
    coalesce(dp.policy_sk, '-1')                            as policy_sk,
    coalesce(ph.policyholder_sk, '-1')                      as policyholder_sk,
    -- dim_date used 5 times ("role-playing"); NULL = milestone not reached yet
    to_number(to_char(c.loss_date, 'YYYYMMDD'))             as loss_date_key,
    to_number(to_char(c.reported_date, 'YYYYMMDD'))         as reported_date_key,
    to_number(to_char(c.approved_at::date, 'YYYYMMDD'))     as approved_date_key,
    to_number(to_char(c.rejected_at::date, 'YYYYMMDD'))     as rejected_date_key,
    to_number(to_char(c.paid_at::date, 'YYYYMMDD'))         as paid_date_key,
    c.current_status,
    c.claim_amount,                                                        -- measure
    -- explicit type: datediff returns NUMBER(9,0), the contract table has NUMBER(38,0); without the
    -- cast every INCREMENTAL run sees a "type change" and on_schema_change='fail' stops it
    datediff(day, c.reported_date, c.paid_at::date)::number(38, 0)  as days_to_settle,   -- measure, NULL until paid
    c.last_updated_at,
    c.last_loaded_at                                                       -- drives the incremental filter
from claims as c
left join {{ ref('slv_policies') }} as p
    on p.policy_id = c.policy_id
left join {{ ref('dim_policy') }} as dp
    on dp.policy_id = c.policy_id
-- Point-in-time: the customer version valid on the LOSS date
left join {{ ref('dim_policyholder') }} as ph
    on  ph.policyholder_id = p.policyholder_id
    and c.loss_date::timestamp_ntz >= ph.valid_from
    and c.loss_date::timestamp_ntz <  ph.valid_to

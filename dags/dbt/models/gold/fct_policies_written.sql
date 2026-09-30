{#
    Fact: policies written (new business). Grain: one row per policy issued.
    FULL REFRESH (gold default): small, and a policy never changes after issue.

    policyholder_sk = the customer VERSION valid when the policy was issued (point-in-time):
    "premium by city" counts the city the customer lived in at the time of sale.
#}
with policies as (

    select * from {{ ref('slv_policies') }}

),

dim_policy as (

    select policy_sk, policy_id from {{ ref('dim_policy') }}

),

dim_policyholder as (

    select policyholder_sk, policyholder_id, valid_from, valid_to from {{ ref('dim_policyholder') }}

)

select
    p.policy_id,                                                     -- degenerate dimension (drill-through)
    dp.policy_sk,
    coalesce(ph.policyholder_sk, '-1')                   as policyholder_sk,
    to_number(to_char(p.issued_at::date, 'YYYYMMDD'))    as issue_date_key,
    to_number(to_char(p.effective_date, 'YYYYMMDD'))     as effective_date_key,
    p.annual_premium,                                                -- the measure
    p.issued_at
from policies as p
inner join dim_policy as dp
    on dp.policy_id = p.policy_id
-- Point-in-time lookup: the version whose [valid_from, valid_to) contains the issue time
left join dim_policyholder as ph
    on  ph.policyholder_id = p.policyholder_id
    and p.issued_at >= ph.valid_from
    and p.issued_at <  ph.valid_to

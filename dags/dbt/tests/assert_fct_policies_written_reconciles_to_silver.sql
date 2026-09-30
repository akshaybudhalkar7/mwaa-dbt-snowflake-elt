-- Gold must neither lose nor duplicate business value: policy count and total premium in
-- the fact equal silver. Catches fan-out from a bad join (e.g. overlapping SCD2 windows)
-- and rows dropped by an inner join.
with gold as (

    select count(*) as policies, sum(annual_premium) as premium
    from {{ ref('fct_policies_written') }}

),

silver as (

    select count(*) as policies, sum(annual_premium) as premium
    from {{ ref('slv_policies') }}

)

select gold.policies as gold_policies, silver.policies as silver_policies,
       gold.premium  as gold_premium,  silver.premium  as silver_premium
from gold cross join silver
where gold.policies <> silver.policies
   or gold.premium  <> silver.premium

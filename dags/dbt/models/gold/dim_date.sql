{#
    Calendar dimension: one row per day, 2026-01-01 .. 2028-12-31.
    FULL REFRESH: gold's default materialization (table) -> dropped and rebuilt every run.
#}
with days as (

    -- GENERATOR makes N empty rows; ROW_NUMBER (not SEQ4) because SEQ4 can have gaps
    select dateadd(day, row_number() over (order by seq4()) - 1, '2026-01-01'::date) as date_day
    from table(generator(rowcount => 1096))

)

select
    to_number(to_char(date_day, 'YYYYMMDD'))  as date_key,        -- 20260930: readable surrogate key
    date_day,
    year(date_day)                            as calendar_year,
    quarter(date_day)                         as calendar_quarter,
    month(date_day)                           as calendar_month,
    monthname(date_day)                       as month_name,
    day(date_day)                             as day_of_month,
    dayofweekiso(date_day)                    as day_of_week,     -- 1 = Monday ... 7 = Sunday
    dayname(date_day)                         as day_name,
    dayofweekiso(date_day) in (6, 7)          as is_weekend,
    date_trunc('month', date_day)             as month_start_date
from days

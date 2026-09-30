{#
    SCD2 windows of one key must chain perfectly: each version ends exactly where the next
    one starts (no gap, no overlap), and every window is non-empty.
    An overlap would make a point-in-time join match two versions -> duplicated fact rows.
#}
{% test scd2_contiguous_windows(model, key, valid_from='valid_from', valid_to='valid_to') %}

with windows as (

    select
        {{ key }}                                                              as scd_key,
        {{ valid_from }}                                                       as window_start,
        {{ valid_to }}                                                         as window_end,
        lead({{ valid_from }}) over (partition by {{ key }} order by {{ valid_from }}) as next_start
    from {{ model }}

)

select *
from windows
where window_start >= window_end                              -- empty or reversed window
   or (next_start is not null and window_end <> next_start)   -- gap or overlap

{% endtest %}

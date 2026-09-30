{#
    Every key of an SCD2 dimension must have exactly ONE current version.
    0 -> the entity disappears from "current state" reports; 2+ -> it is counted twice.
#}
{% test scd2_one_current(model, key, is_current='is_current') %}

select {{ key }}, count_if({{ is_current }}) as current_versions
from {{ model }}
group by {{ key }}
having count_if({{ is_current }}) <> 1

{% endtest %}

{#
    Fails for every combination of `columns` that appears more than once.
    dbt's built-in `unique` only takes one column; our silver version tables are unique on
    (business key, updated_at).
#}
{% test unique_combination(model, columns) %}

select {{ columns | join(', ') }}, count(*) as occurrences
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}

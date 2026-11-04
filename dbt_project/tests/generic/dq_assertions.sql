{% test dq_condition(model, condition, column_name=none) %}
select * from {{ model }} where not coalesce(({{ condition }}), false)
{% endtest %}

{% test dq_unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as occurrence_count
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

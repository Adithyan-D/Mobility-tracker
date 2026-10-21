{% test accepted_range(model, column_name, min_value=none, max_value=none, where=none) %}

with validation as (
    select {{ column_name }} as value
    from {{ model }}
    {% if where is not none %}
    where {{ where }}
    {% endif %}
),

failures as (
    select value
    from validation
    where
    {% if min_value is not none and max_value is not none %}
        value < {{ min_value }} or value > {{ max_value }}
    {% elif min_value is not none %}
        value < {{ min_value }}
    {% elif max_value is not none %}
        value > {{ max_value }}
    {% else %}
        1 = 0
    {% endif %}
)

select * from failures

{% endtest %}

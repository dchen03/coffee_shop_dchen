
{% test is_not_gmail(model, column_name) %}
    select distinct {{ column_name }}
    from {{ model }}
    where not {{column_name}} like '%@gmail.com'
{% endtest %}



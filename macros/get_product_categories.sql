{% macro get_product_categories() %}

{% set product_categories_query %}
select distinct
product_category
from {{ ref('product_price_on_orders') }}
order by 1
{% endset %}



{% set results = run_query(product_categories_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}
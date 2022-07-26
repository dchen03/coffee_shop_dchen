{%- set product_categories = dbt_utils.get_column_values(
    table=ref('product_price_on_orders'),
    column='product_category'
) -%}

select
  date_trunc(order_created_at, month) as date_month,
  {%- for product_category in product_categories %}
 -- sum(case when product_category = '{{ product_category }}' then product_price end) as {{ product_category | replace(" ", "_") + "_amount" }}
  sum(case when product_category = '{{ product_category }}' then product_price end) as {{ product_category | replace(" ", "_") }}_amount
  {%- if not loop.last %},{% endif -%}
  {% endfor %}

from {{ ref('product_price_on_orders') }} 
group by 1

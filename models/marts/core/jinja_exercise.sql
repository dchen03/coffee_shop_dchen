{%- set product_categories = ['coffee beans', 'merch', 'brewing supplies'] -%}

select
  date_trunc(order_created_at, month) as date_month,
  {%- for product_category in product_categories %}
  sum(case when product_category = '{{ product_category }}' then product_price end) as {{ product_category | replace(" ", "_") + "_amount" }}
  {%- if not loop.last %},{% endif -%}
  {% endfor %}

from {{ ref('product_price_on_orders') }} 
group by 1

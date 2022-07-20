with customer_orders as (
    select
          customer_id, 
          count(*) as n_orders, 
          min(order_created_at) as first_order_at
    from {{ ref('stg_coffee_shop__orders') }} as orders
    group by customer_id
)

select 
     customers.customer_id,
     customers.customer_name,
     customers.customer_email,
     coalesce(customer_orders.n_orders, 0) as n_orders,
     customer_orders.first_order_at 
--- use customers as base to get customers with no orders
from {{ ref('stg_coffee_shop__customers') }} as customers
left join  customer_orders
  on  customers.customer_id = customer_orders.customer_id 
order by customer_orders.first_order_at 
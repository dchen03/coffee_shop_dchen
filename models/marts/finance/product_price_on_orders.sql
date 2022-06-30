with 

orders as (
    select * from {{ ref('stg_coffee_shop__orders') }}
),

order_items as (
    select * from {{ ref('stg_coffee_shop__order_items') }}
),

products as (
    select * from {{ ref('stg_coffee_shop__products') }}
),

product_prices as (
    select * from {{ ref('stg_coffee_shop__product_prices') }}
),
 
final as (
    select 

        orders.orders_id, --for merge
        order_items.product_id,--for merge
        orders.order_created_at,
        products.product_category,
        product_prices.product_price,
        product_prices.product_price_created_at,
        product_prices.product_price_ended_at,

        -- TODO: find out how to get customer_type
        cast(null as string) as customer_type -- (new, returning),

        -- excluded 
        --- product_id,

    from orders 

    left join order_items 
        on orders.orders_id = order_items.order_id

    left join products
        on order_items.product_id = products.product_id

    left join product_prices
        on order_items.product_id = product_prices.product_id
        and orders.order_created_at between product_prices.product_price_created_at and  product_prices.product_price_ended_at

)

select * from final
with source as (

    select * from {{ source('coffee_shop', 'orders') }}

),

renamed as (

    select
        id as orders_id,
        customer_id,
        total,
        address,
        state,
        zip,

        -- timestamps
        created_at as order_created_at

    from source

)

select * from renamed
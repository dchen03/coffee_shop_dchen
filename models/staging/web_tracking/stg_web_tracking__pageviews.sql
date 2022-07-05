with source as (

    select * from {{ source('web_tracking', 'pageviews') }}

),

renamed as (

    select
        id as pageviews_id,
        visitor_id,
        customer_id,
        device_type,
        page,

    -- timestamp
        timestamp as pageview_at,

    from source

)

select * from renamed
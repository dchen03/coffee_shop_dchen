with 

pageviews as (
    select * from {{ ref('stg_web_tracking__pageviews') }}
),

final as (
    select
        pageviews_id,
        visitor_id,
        customer_id,
        device_type,
        page,
        pageview_at,
        case
        when customer_id is not null then concat('a', dense_rank() over (order by customer_id))
        else null
        end as new_visitor_id
    from pageviews
    order by
        customer_id

)

select * from final
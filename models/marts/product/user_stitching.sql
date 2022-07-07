with 

pageviews as (
    select * from {{ ref('stg_web_tracking__pageviews') }}
),

-- customer id and visitor id crosswalk 
customer_visitor as ( 
    select distinct 
        customer_id, 
        visitor_id
    from pageviews
    where customer_id is not null
    order by customer_id
),

--bring in customer_id for every known visitor as customer_visitor_id
user_stitching as (
    select
        pageviews_id,
        pageviews.visitor_id,
        pageviews.customer_id,
        customer_visitor.customer_id as customer_visitor_id,
        device_type,
        page,
        pageview_at,
    from pageviews
    left join customer_visitor
        on pageviews.visitor_id = customer_visitor.visitor_id
)

select * from user_stitching


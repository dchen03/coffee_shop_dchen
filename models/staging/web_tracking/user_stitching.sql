with 

pageviews as (
    select * from {{ ref('stg_web_tracking__pageviews') }}
),

/* User stitching */

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
        customer_visitor.customer_id as filled_in_customer_id,
        device_type,
        page,
        coalesce(
          customer_visitor.customer_id,
          pageviews.visitor_id
        ) as blended_id,
        pageview_at
    from pageviews
    left join customer_visitor
        on pageviews.visitor_id = customer_visitor.visitor_id
)

select * from user_stitching 

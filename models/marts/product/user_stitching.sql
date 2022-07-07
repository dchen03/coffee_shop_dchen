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
        customer_visitor.customer_id as customer_visitor_id,
        device_type,
        page,
        pageview_at,
    from pageviews
    left join customer_visitor
        on pageviews.visitor_id = customer_visitor.visitor_id
),

/* Sessionization */

col_after_30 as (
    select *,
        timestamp_diff(pageview_at, lag(pageview_at) over (partition by customer_id order by pageview_at), MINUTE) > 30 as after_30
    from user_stitching
    order by customer_id, pageview_at
),

col_new_session as (
    select *, 
        case 
            when after_30 is null then 1 
            when after_30 then 1
            else 0
        end as is_new_session
    from col_after_30
),

final as (
    select *,
        sum(is_new_session) over (order by customer_id, pageview_at) as global_session_id,
        sum(is_new_session) over (partition by customer_id order by pageview_at) as user_session_id
    from col_new_session
)

select * from final 



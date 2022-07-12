with 

user_stitching as (
    select * from {{ ref('user_stitching') }}
),

/* Sessionization */

col_after_30 as (
    select *,
        timestamp_diff(pageview_at, lag(pageview_at) over (partition by blended_id order by pageview_at), MINUTE) > 30 as after_30
    from user_stitching
    order by blended_id, pageview_at
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
        sum(is_new_session) over (order by blended_id, pageview_at) as global_session_id,
        sum(is_new_session) over (partition by blended_id order by pageview_at) as user_session_id
    from col_new_session
),

session_time as (
    select
        pageviews_id,
        visitor_id,
        filled_in_customer_id as customer_id,
        blended_id,
        device_type,
        page,
        pageview_at, 
        is_new_session,
        global_session_id,
        user_session_id, 
        min(pageview_at) over (partition by blended_id, user_session_id) as session_start_at,
        max(pageview_at) over (partition by blended_id, user_session_id) as session_end_at
    from final
)

select * from session_time 




{{ config(
    materialized='table'
) }}


with 

sessionization as (
    select * from {{ ref('sessionization') }}
),

orders as (
    select * from {{ ref('stg_coffee_shop__orders') }}
),

session_length as (
  select *,
    timestamp_diff(session_end_at, session_start_at, SECOND) as session_length_seconds
  from sessionization
),

pagevisits as (
  select *, 
      count (distinct page) over (partition by blended_id, user_session_id) as n_page_visits
    from session_length
    order by blended_id, pageview_at, user_session_id
),

join_purchases as (
  select pagevisits.*,
         orders.order_created_at
  from pagevisits
  left join orders
    on pagevisits.blended_id = orders.customer_id

),

final as (
  select distinct customer_id, blended_id, user_session_id, session_start_at, session_end_at, session_length_seconds, n_page_visits, order_created_at,
    case 
     when order_created_at is not null then 1
     else 0
    end as order_created 
    from join_purchases
)

select * from final
with orders as (
    select * from {{ ref('stg_coffee_shop__orders') }}
),

customer_weeks as (
    select 
        customer_id,
        date_trunc(order_created_at, week) as date_week,
        sum(total) as weekly_spend
    from orders
    group by 1,2
),

customer_first_week as (
    select 
        customer_id,
        min(date_week) as first_week
    from customer_weeks
    group by 1

),

cohort_sizes as (
  select 
    first_week as cohort_identifier,
    count(*) as cohort_size
  from customer_first_week
  group by 1

)

select *
from cohort_sizes
order by 1
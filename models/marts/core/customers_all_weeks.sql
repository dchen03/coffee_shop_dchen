with 

orders as (
    select * from {{ ref('stg_coffee_shop__orders') }}
),

all_weeks as (
    select * from {{ ref('all_weeks') }}
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

customer_all_weeks as (
    select 
        *,
        date_diff(
            cast(all_weeks.date_week as datetime),
            cast(customer_first_week.first_week as datetime),
            week 
        ) as week_number
    from customer_first_week
    inner join all_weeks 
        on customer_first_week.first_week <= all_weeks.date_week

),

joined as (
    select 
        * except(weekly_spend),
        coalesce(weekly_spend, 0) as weekly_spend,
        sum(weekly_spend) over (
                partition by customer_id
                order by date_week
                rows between unbounded preceding and current row
            ) as cumulative_total
    from customer_all_weeks
    left join customer_weeks
        using (customer_id, date_week)
       --on customer_weeks.customer_id = customer_all_weeks.customer_id
        --and customer_weeks.date_week = customer_all_weeks.date_week

)

select *
from joined
order by 1,2,3
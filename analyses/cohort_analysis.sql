-- group customers into cohorts and calculate ltv, wk 4 exc 3

with 

customers_all_weeks as (
  select * from {{ ref('customers_all_weeks') }}
),

cohort_sizes as (
  select * from {{ ref('cohort_sizes') }}
),

cohort_weeks as (
  select 
    first_week as cohort_identifier,
    week_number,
    sum(weekly_spend) as cohort_total_spend,
    sum(cumulative_total) as cohort_cumulative_total
  from customers_all_weeks
  group by 1, 2
  order by 1, 2
),


normalized_cohort_weeks as (
  select
    *,
    cohort_total_spend / cohort_size as avg_spend,
    cohort_cumulative_total / cohort_size as avg_ltv
  from cohort_weeks
  left join cohort_sizes using (cohort_identifier)
)


select * 
from normalized_cohort_weeks
order by 1, 2

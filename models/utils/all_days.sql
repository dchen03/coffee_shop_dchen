with generated as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2021-11-14' as date)",
        end_date="cast('2022-08-02' as date)"
    ) }}
)

select
    cast(date_day as timestamp) as date_day
from generated
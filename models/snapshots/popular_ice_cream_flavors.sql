with 
    ---FROM `aec-students.dbt_dchen_snapshots.favorite_ice_creams` 
    -- NOTE: FIGURE OUT HOW TO PULL THE SNAPSHOT TABLE REFERENCE CORRECTLY
favorite_ice_creams as (
    select * from {{ ref('favorite_ice_creams') }}
),

set_end_date as (
    select 
        github_username, 
        favorite_ice_cream_flavor, 
        dbt_valid_from, 
        dbt_valid_to,
        case 
        -- NOTE: adjust date when null to current_date()
            when cast(dbt_valid_to as datetime) is null then date("2022-08-01") 
            else cast(dbt_valid_to as datetime)
            end as dbt_valid_to2
    from favorite_ice_creams
    order by dbt_valid_from desc, github_username
),

date_truncate as (
    select *,
        date_trunc(dbt_valid_from, day) as start_day,
        date_trunc(dbt_valid_to2, day) as end_day 
    from set_end_date 
),

all_days as (
    select * from {{ ref('all_days') }} 
),

ice_cream_all_days as (
    select 
        github_username,
        favorite_ice_cream_flavor,
        --start_day,
        --end_day,
        date_day
    from date_truncate
    inner join all_days 
        on date_truncate.start_day <= all_days.date_day and
           cast(date_truncate.end_day as datetime) >= cast(all_days.date_day as datetime)
    order by start_day desc, date_day desc, github_username 
)

select * from ice_cream_all_days

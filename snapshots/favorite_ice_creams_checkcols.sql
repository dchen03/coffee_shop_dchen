{% snapshot favorite_ice_creams_checkcols %}

{{
    config(
      target_schema='dbt_dchen_snapshots',
      unique_key='github_username',

      strategy='check',
      check_cols=['favorite_ice_cream_flavor'],
    )
}}

select 
* 
from {{ source('advanced_dbt_examples', 'favorite_ice_cream_flavors') }}


{% endsnapshot %}
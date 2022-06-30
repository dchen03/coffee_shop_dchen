with source as (
    select * from {{ source('github', 'issue_merged') }}
),

renamed as (
    select 
        issue_id, -- primary key first
        actor_id as merge_user_id, -- foreign key

        commit_sha,

        -- timestamps
        merged_at,	

        -- excluded columns
        -- _fivetran_synced,	
    from source

)

select * from renamed 
with 

pull_requests

repositories

issues

issues_merged



final as (

    select 
        pull_requests.pull_request_id,
        repository.name as repo_name, 
        issues.number as pull_request_number,

        -- TODO: find out how to label these PRs , get state
        cast(null as string) as type, -- (bug, eng, feature),

        case 
            when pull_requests.is_draft then 'draft'
            when issue_merged.merged_at is not null then 'merged'
            when issue_merged.closed_at is not null then 'closed_without_merge'
            else 'open'
        end as state,
        
        issues.created_at as opened_at,
        issue_merged.merged_at,
        date_diff(issues.created_at, issue_merged.merged_at, hour)/ 24.0 as days_open_to_merge


    from pull_requests
    left join repositories
        on pull_requests.head_repo_id = repositories.repository_id

    left join issues 
        on pull_requests.issue_id = issues.issue_id

    left join issue_merged
        on pull_requests.issue_id = issue_merged.issue_id


)

select * from final
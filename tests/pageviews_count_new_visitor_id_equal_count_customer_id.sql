

--- there should not be any customers with multiple visitor IDs
--- therefore, return records where this isn't true to make the test fail

select 
  count(distinct new_visitor_id) as n_new_visitor_id,
  count(distinct customer_id) as n_customer_id
from {{ ref('user_stitching') }}
group by customer_id
having not n_new_visitor_id = n_customer_id
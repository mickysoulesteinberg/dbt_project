with tbd as (
    select *
    from {{ ref('top_billed_by_year') }}
)
select *
from tbd
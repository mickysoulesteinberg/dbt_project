{{ config(
    materialized='table',
    partition_by={
        'field': 'year',
        'granularity':'year',
        'data_type': 'date'
    }
)}}
with names_raw as (
    select name, sex as ssa_code, num_births, year_of_birth as year
    from {{ ref('stg__ssa__names' )}}
),
gender_codes as (
    select ssa_code, gender 
    from {{ ref('gender_codes')}}
)
select
    name, gender, year,
    num_births,
    rank() over (partition by gender, year order by num_births desc) as name_rank
from names_raw
left join gender_codes using (ssa_code)

{{ config(
    materialized='table',
    partition_by={
        'field': 'year',
        'granularity':'year',
        'data_type': 'date'
    }
)}}

{%- set popularity_cap = 10 -%}

with names_raw as (
    select name, sex as ssa_code, num_births, year_of_birth as year
    from {{ ref('stg__ssa__names' )}}
),
gender_codes as (
    select ssa_code, gender 
    from {{ ref('gender_codes')}}
),
ranked_names as (
    select
        name, gender, year,
        num_births,
        rank() over (partition by gender, year order by num_births desc) as name_rank
    from names_raw
    left join gender_codes using (ssa_code)
),
popular_names as (
    select
        name, gender, year, num_births, name_rank,
        min(name_rank) over (partition by name, gender) as highest_name_rank,
        name_rank <= {{ popularity_cap }} as is_popular
    from ranked_names
    qualify min(name_rank) over (partition by name) <= {{ popularity_cap }}
),
tbd as (
    select *,
        min(case when is_popular then year end) over (partition by name, gender) as first_popular_year
    from popular_names
)
select *
from tbd


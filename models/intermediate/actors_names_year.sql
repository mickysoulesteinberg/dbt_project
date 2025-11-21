{{ config(
    materialized='table',
    partition_by={
        'field': 'year',
        'granularity':'year',
        'data_type': 'date'
    }
)}}

with top_actors_by_year as (
    select
        movie_title,
        release_year,
        actor_name as raw_name,
        actor_first_name as name,
        character_name as character,
        character_first_name,
        billing_order, gender, is_adult
    from {{ ref('top_billed_by_year') }}
),
top_first_names_by_year as (
    select release_year as year, gender, name,
        array_agg(struct(movie_title, raw_name, character, billing_order, is_adult)) as movies
    from top_actors_by_year
    group by release_year, gender, name
),
ssa_names as (
    select name, gender, year, num_births, name_rank,
        min(name_rank) over (partition by name, gender) as highest_name_rank
    from {{ ref('top_names_ssa_by_year') }}
    qualify min(name_rank) over (partition by name, gender) < 10
),
tbd as (
    select *,
        name_rank = highest_name_rank as is_most_popular_year,
        min(case when name_rank < 10 then year end) over (partition by name, gender) as first_popular_year
    from ssa_names
    left join top_first_names_by_year using (name, year, gender)
)
select *
from tbd
{{ config(
    materialized='table',
    partition_by={
        'field': 'year',
        'granularity':'year',
        'data_type': 'date'
    }
)}}

with top_names as (
    select
        name, gender, year,
        num_births,
        name_rank, highest_name_rank,
        is_popular, first_popular_year
    from {{ ref('top_names_ssa_by_year') }}
),
top_actors as (
    select
        movie_title, release_year,
        actor_name, actor_first_name,
        character_name, character_first_name,
        is_adult, billing_order,
        gender as actor_gender
    from {{ ref('top_billed_by_year') }}
)
select
    name, gender, year, num_births,
    name_rank, highest_name_rank, is_popular, first_popular_year,
    array_agg(struct(
        movie_title, release_year,
        if(actor_first_name = name, 'Actor', 'Character') as actor_or_character,
        actor_name, actor_first_name,
        character_name, character_first_name,
        is_adult, billing_order,
        if(actor_gender = gender, true, false) as is_gender_match
    )) as movies
from top_names
left join top_actors
on top_names.name = top_actors.actor_first_name
or top_names.name = top_actors.character_first_name
group by 1, 2, 3, 4, 5, 6, 7, 8

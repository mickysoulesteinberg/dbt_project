{{ config(
    materialized='table',
    partition_by={
        'field': 'release_year',
        'granularity':'year',
        'data_type': 'date()'
    }
)}}

{%- set billing_cap = 10 -%}

with gender_codes as (
    select tmdb_code, gender
    from {{ ref('gender_codes')}}
),
movies as (
    select
        id as movie_id,
        title,
        release_date
    from {{ ref('stg__tmdb__top_movies')}}
),
actors as (
    select
        movie_id,
        name, character,
        gender as tmdb_code,
        billing_order,
        is_adult
    from {{ ref('stg__tmdb__top_movies_credits')}}
    where billing_order < {{ billing_cap }}
),
tbd as (
    select
        movies.title as movie_title,
        date(date_trunc(movies.release_date,year)) as release_year,
        actors.name as actor_name,
        split(actors.name, ' ')[(safe_offset(0))] as actor_first_name,
        actors.character as character_name,
        split(actors.character, ' ')[(safe_offset(0))] as character_first_name,
        actors.is_adult,
        actors.billing_order,
        gender_codes.gender
    from movies
    inner join actors using (movie_id)
    left join gender_codes using (tmdb_code)
)
select *
from tbd
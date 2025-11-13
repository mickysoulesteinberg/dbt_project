{{ config(
    materialized='table',
    partition_by={
        'field': 'release_year',
        'granularity':'year',
        'data_type': 'date()'
    }
)}}

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
        billing_order,
        gender as tmdb_code,
        is_adult
    from {{ ref('stg__tmdb__top_movies_credits')}}
),
tbd as (
    select
        movies.title as movie_title,
        date(date_trunc(movies.release_date,year)) as release_year,
        actors.name,
        actors.character,
        actors.billing_order,
        actors.is_adult,
        gender_codes.gender
    from movies
    inner join actors using (movie_id)
    left join gender_codes using (tmdb_code)
)
select *
from tbd
select
    id, title, release_date,
    popularity, vote_average, vote_count
from {{ source('airflow_tmdb', 'top_movies')}}
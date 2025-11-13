with raw_source as (
    select
        id, `cast` as cast_raw, crew
    from {{ source('airflow_tmdb', 'top_movies_credits') }}
)
SELECT
    id as movie_id,
      CAST(JSON_VALUE(cast_raw, '$.id') AS INT64)     AS person_id,
      JSON_VALUE(cast_raw, '$.name')                  AS name,
      JSON_VALUE(cast_raw, '$.character')             AS character,
      CAST(JSON_VALUE(cast_raw, '$.order') AS INT64)  AS billing_order,
      JSON_VALUE(cast_raw, '$.credit_id')             AS credit_id,
      JSON_VALUE(cast_raw, '$.original_name')         AS original_name,
      CAST(JSON_VALUE(cast_raw, '$.gender') AS INT64) AS gender,
      CAST(JSON_VALUE(cast_raw, '$.popularity') AS FLOAT64) AS popularity,
      JSON_VALUE(cast_raw, '$.profile_path')          AS profile_path,
      CAST(JSON_VALUE(cast_raw, '$.adult') AS BOOL)   AS is_adult
FROM raw_source, UNNEST(JSON_QUERY_ARRAY(CAST(cast_raw AS JSON), '$')) AS cast_raw


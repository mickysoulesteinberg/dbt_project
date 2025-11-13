{{ config(
    materialized='table',
    partition_by={
        'field': 'year_of_birth',
        'granularity':'year',
        'data_type': 'date'
    }
)}}
select
    name, sex, num_births,
    date(cast(regexp_extract(FILE_NAME, r'(\d{4})') as integer), 1, 1) as year_of_birth

from {{ source('airflow_ssa', 'names')}}
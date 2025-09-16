select
    territory,
    sex,
    year,
    name,
    num_births
from {{ source('ssa_data', 'territory_names') }}
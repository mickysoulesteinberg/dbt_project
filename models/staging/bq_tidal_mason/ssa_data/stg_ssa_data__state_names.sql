select
    state,
    sex,
    year,
    name,
    num_births
from {{ source('ssa_data', 'state_names') }}
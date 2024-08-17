select 
    *
from {{ source('raw', 'rosters') }}
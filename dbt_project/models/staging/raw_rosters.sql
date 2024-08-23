select 
    *
from {{ source('teams', 'rosters') }}
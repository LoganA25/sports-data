select 
    *
from {{ ref('stats') }}
where team = 'ARI'    


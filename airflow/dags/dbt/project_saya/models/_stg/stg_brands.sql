select 
    CAST(brand_id AS INT64) as brand_id,
    name as brand_name
from {{source('my_data', 'raw_brands')}}



select
    CAST(product_id AS INT64) as product_id,
    CAST(brand_id AS INT64) as brand_id,
    name as product_name,
    price as product_price
from {{source('my_data', 'raw_products')}}

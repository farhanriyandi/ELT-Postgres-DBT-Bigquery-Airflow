select
products.product_id
, brands.brand_id
, brands.brand_name
, products.product_name
, products.product_price
from {{ ref('stg_products') }} as products
left join {{ ref('stg_brands') }} as brands
on products.brand_id = brands.brand_id

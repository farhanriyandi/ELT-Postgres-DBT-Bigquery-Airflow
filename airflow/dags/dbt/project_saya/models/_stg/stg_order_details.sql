select
    CAST(order_detail_id AS INT64) as order_detail_id,
    CAST(order_id AS INT64) as order_id,
    CAST(product_id AS INT64) as product_id,
    quantity as order_qty,
    price as unit_sales
from {{source('my_data', 'raw_order_details')}}

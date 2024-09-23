select 
    CAST(order_id AS INT64) as order_id,
    CAST(order_date AS TIMESTAMP) as order_at,
    customer_phone
from {{source('my_data', 'raw_orders')}}

select
    order_id,
    order_at,
    country,
    brand_id,
    brand_name,
    product_name,
    order_qty,
    unit_sales as daily_unit_sales
from {{ ref('int_order_details') }}
select *
from {{ ref('fct_orders') }}
where mart_flaging = 'marketing'
select
    customer_id as id,
    count_lifetime_orders
from {{ ref('customers') }}


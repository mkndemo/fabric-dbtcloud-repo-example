WITH customers_lakehouse AS (
    select id as customer_id, name from {{ source('bronze_lakehouse', 'raw_customers') }}
),

orders_dwh AS (
    SELECT 
        id AS order_id,
        customer,
        ordered_at,
        store_id
    FROM {{ ref('stg_taxi__Orders') }}
)

-- Join the two tables on the customer id
SELECT 
    o.order_id,
    c.name AS customer_name,
    o.ordered_at,
    o.store_id
FROM orders_dwh o
LEFT JOIN customers_lakehouse c
    ON o.customer = c.customer_id

{{
    config(
        materialized='view'
    )
}}

SELECT customer_id as id, customer_name as name 
FROM {{ ref('stg_customers') }}
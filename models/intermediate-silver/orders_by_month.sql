{{
    config(
        materialized='view'
    )
}}

SELECT order_id as id, location_id as loc FROM {{ ref('orders') }}
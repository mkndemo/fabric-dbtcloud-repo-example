{{
    config(
        materialized='table'
    )
}}

SELECT order_id as id, location_id as loc FROM {{ ref('orders') }}
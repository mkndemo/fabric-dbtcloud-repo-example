{{
    config(
        materialized='table'
    )
}}

select date_id from {{ ref('stg_taxi__Trip') }}
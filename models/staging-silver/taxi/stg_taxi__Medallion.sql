{{
    config(
        materialized='table'
    )
}}

with 

source as (

    select * from {{ source('taxi', 'Medallion') }}

),

renamed as (

    select
        MedallionID as medallion_id,
        MedallionBKey as medallion_bkey
    from source

)

select * from renamed

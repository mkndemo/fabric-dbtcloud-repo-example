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
        MedallionID,
        MedallionBKey
    from source

)

select * from renamed

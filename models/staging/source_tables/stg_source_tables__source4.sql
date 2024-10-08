{{ config( materialized='view' ) }}

with 

source as (

    select * from {{ source('source_tables', 'source4') }}

),

renamed as (

    select
        CAST(PK as INTEGER) as pk,
        Column_5,
        Column_200,
        updated_at

    from source

)

select * from renamed

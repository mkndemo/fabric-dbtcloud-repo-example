{{ config( materialized='view' ) }}

with 

source as (

    select * from {{ source('source_tables', 'source3') }}

),

renamed as (

    select
        CAST(PK as INTEGER) as pk,
        Column_2,
        Column_4,
        updated_at
    from source

)

select * from renamed

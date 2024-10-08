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
        updated_at,
        CONVERT(varchar(32), HASHBYTES('MD5', 
            CONCAT_WS('|', 
                CAST(PK as varchar(MAX)),
                CAST(Column_2 AS VARCHAR(MAX)),
                CAST(Column_4 AS VARCHAR(MAX))
            )
        ), 2) AS hash
    from source

)

select * from renamed

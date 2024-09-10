with 

source as (

    select * from {{ source('bronze_lakehouse', 'raw_customers') }}

),

renamed as (

    select * 
    from source

)

select * from renamed

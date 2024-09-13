with 

source as (

    select * from {{ source('taxi', 'Orders') }}

),

renamed as (

    select
        id,
        customer,
        ordered_at,
        store_id,
        subtotal,
        tax_paid,
        order_total

    from source

)

select * from renamed

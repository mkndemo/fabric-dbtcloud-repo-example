with

source as (

    select * from {{ source('ecom', 'raw_products') }}

),

renamed as (

    select
        sku as product_id,
        name as product_name,
        type as product_type,
        description as product_description,
        
        case when type = 'jaffle' then 1 else 0 end as is_food_item,
        case when type = 'beverage' then 1 else 0 end as is_drink_item,
        
        {{ cents_to_dollars('price') }} as product_price
    from source

)

select * from renamed
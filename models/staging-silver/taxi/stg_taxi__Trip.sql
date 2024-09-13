with 

source as (

    select * from {{ source('taxi', 'Trip') }}

),

renamed as (

    select
        t.DateID as date_id,
        t.MedallionID as medallion_id,
        t.HackneyLicenseID as hackney_license_id,
        t.PickupTimeID as pickup_time_id,
        t.TollsAmount as tolls_amount,
        t.TotalAmount as total_amount
    from source t

)

select * from renamed

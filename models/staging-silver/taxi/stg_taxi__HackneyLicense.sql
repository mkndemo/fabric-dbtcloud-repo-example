with 

source as (

    select * from {{ source('taxi', 'HackneyLicense') }}

),

renamed as (

    select
        HackneyLicenseID as hackney_license_id,
        HackneyLicenseBKey as hackney_license_bkey,
        HackneyLicenseCode as hackney_license_code

    from source

)

select * from renamed

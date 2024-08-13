WITH 

Trips AS (
    SELECT * FROM {{ source('taxi', 'Trip') }}
),

HackneyLicenses AS (
    SELECT * FROM {{ source('taxi', 'HackneyLicense') }}
),

renamed AS (
    SELECT
        t.DateID,
        t.HackneyLicenseID,
        t.PickupTimeID,
        h.HackneyLicenseCode
    FROM Trips t
    JOIN HackneyLicenses h ON h.HackneyLicenseID = t.HackneyLicenseID
)

SELECT * FROM renamed
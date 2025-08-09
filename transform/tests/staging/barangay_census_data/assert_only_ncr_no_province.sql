
SELECT *
FROM {{ ref('barangay_census_data') }}
WHERE region != 'NCR' and province is null
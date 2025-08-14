
SELECT *
FROM {{ ref('psa_barangay_census_data') }}
WHERE region != 'NCR' and province is null
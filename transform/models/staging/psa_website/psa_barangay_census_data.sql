SELECT 
    UPPER(barangay) as barangay, 
    population::integer as population,
    TRIM(REGEXP_REPLACE(city_municipality, '[0-9]', '')) as city_municipality,
    TRIM(REGEXP_REPLACE(province, '[^A-Za-z-\s]', '', 'g')) as province,
    TRIM(REGEXP_REPLACE(region, '[0-9]', '')) as region
FROM {{ source('bronze', 'psa_barangay_census_data') }}
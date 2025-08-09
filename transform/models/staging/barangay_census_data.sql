SELECT 
    UPPER(barangay) as barangay, 
    population::numeric as population,
    TRIM(REGEXP_REPLACE(city_municipality, '[^a-zA-Z ]', '', 'g')) as city_municipality,
    TRIM(REGEXP_REPLACE(province, '[^a-zA-Z ]', '', 'g')) as province,
    TRIM(REGEXP_REPLACE(region, '[^a-zA-Z ]', '', 'g')) as region
FROM {{ source('bronze', 'barangay_census_data') }}
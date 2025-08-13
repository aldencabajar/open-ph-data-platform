SELECT 
    UPPER(barangay) as barangay, 
    population::numeric as population,
    TRIM(REGEXP_REPLACE(city_municipality, '[0-9]', '')) as city_municipality,
    TRIM(REGEXP_REPLACE(province, '[0-9]', '')) as province,
    TRIM(REGEXP_REPLACE(region, '[0-9]', '')) as region
FROM {{ source('bronze', 'barangay_census_data') }}
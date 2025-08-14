WITH preproc AS (
    SELECT
        UPPER(barangay) as barangay, 
        population::integer as population,
        TRIM(REGEXP_REPLACE(city_municipality, '[0-9]', '')) as city_municipality,
        TRIM(REGEXP_REPLACE(province, '[^A-Za-z-\s]', '', 'g')) as province,
        TRIM(REGEXP_REPLACE(region, '[0-9]', '')) as region,
        source_timestamp_utc,
        source_uri,
        census_year,
        NOW() AS updated_date_time_utc
    FROM {{ source('raw', 'psa_barangay_census_data') }} 

)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['barangay', 'census_year']) }} as id,
    preproc.*
FROM preproc


{{
    config(
    materialized = 'incremental',
    )
}}

WITH preproc AS (
    SELECT
        UPPER(barangay) as barangay, 
        population::integer as population,
        TRIM(REGEXP_REPLACE(city_municipality, '[0-9]', '')) as city_municipality,
        TRIM(REGEXP_REPLACE(province, '[^A-Za-z-\s]', '', 'g')) as province,
        TRIM(REGEXP_REPLACE(region, '[0-9]', '')) as region,
        source_timestamp_utc,
        source_uri,
        NOW() AS updated_date_time_utc
    FROM {{ source('bronze', 'psa_barangay_census_data') }} 

    {% if is_incremental() %}
    WHERE load_datetime_utc >= coalesce((select max(updated_date_time_utc) from {{ this }}), '1900-01-01')

    {% endif %}

)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['barangay', 'source_uri']) }} as id,
    preproc.*
FROM preproc


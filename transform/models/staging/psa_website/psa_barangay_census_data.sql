WITH preproc AS (
    SELECT
        population::integer AS population,
        source_timestamp_utc,
        source_uri,
        census_year,
        UPPER(barangay) AS barangay,
        TRIM(REGEXP_REPLACE(city_municipality, '[0-9]', ''))
            AS city_municipality,
        TRIM(REGEXP_REPLACE(province, '[^A-Za-z-\s]', '', 'g')) AS province,
        TRIM(REGEXP_REPLACE(region, '[0-9]', '')) AS region,
        NOW() AS updated_date_time_utc
    FROM {{ source('raw', 'psa_barangay_census_data') }}

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['barangay', 'city_municipality', 'province', 'census_year']) }} AS id, --noqa: LT05
    *
FROM preproc

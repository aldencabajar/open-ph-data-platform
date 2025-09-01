WITH cleaned as (
    SELECT 
        UPPER(TRIM(REGEXP_REPLACE(city_or_municipality, '[^a-zA-z-0-9-\s]', '', 'g'))) as city_municipality_name,
        REGEXP_REPLACE(area_km2, '\[.*\]|,', '', 'g')::numeric as area_in_sqm,
        UPPER(TRIM(class)) as class,
        UPPER(TRIM(province)) as province_name,
        source_uri,
        source_timestamp_utc,
        load_datetime_utc
    FROM {{ source('raw', 'wikipedia_city_municipality') }}
)

SELECT * EXCLUDE (province_name),
    -- we set province_name to null for HUCs since they do not belong to any province
    CASE WHEN class = 'HUC' THEN NULL ELSE province_name END as province_name
FROM cleaned







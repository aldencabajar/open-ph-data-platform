
SELECT 
    UPPER(TRIM(REGEXP_REPLACE(city_or_municipality, '[^a-zA-z-0-9-\s]', '', 'g'))) as city_municipality_name,
    REGEXP_REPLACE(area_km2, '\[.*\]|,', '', 'g')::numeric as area_in_sqm,
    UPPER(TRIM(class)) as class,
    UPPER(TRIM(province)) as province_name,
    source_uri,
    source_timestamp_utc,
    load_datetime_utc
FROM {{ source('raw', 'wikipedia_city_municipality') }}






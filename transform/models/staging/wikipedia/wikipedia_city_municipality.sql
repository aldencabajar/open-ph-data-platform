WITH cleaned AS (
    SELECT
        -- includes enye (ñ) since a lot of city/municipality names have it
        REGEXP_REPLACE(area_km2, '\[.*\]|,', '', 'g')::numeric AS area_in_sqm,
        source_uri,
        source_timestamp_utc,
        load_datetime_utc,
        UPPER(
            TRIM(
                REGEXP_REPLACE(
                    city_or_municipality, '[^A-Za-zÑñ0-9\s-\-\.'']', '', 'g'
                )
            )
        ) AS city_municipality_name,
        UPPER(TRIM(class)) AS class,
        UPPER(TRIM(province)) AS province_name
    FROM {{ source('raw', 'wikipedia_city_municipality') }}
)

SELECT
    * EXCLUDE (province_name),
    /* We set province_name to null for HUCs since they do not
    belong to any province */
    CASE WHEN class = 'HUC' THEN NULL ELSE province_name END AS province_name
FROM cleaned
